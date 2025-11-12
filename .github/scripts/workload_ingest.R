#!/usr/bin/env Rscript

# ──────────────────────────────────────────────────────────────────────────────
# WORKLOAD INGEST — XLSX/CSV from OneDrive/SharePoint (public link)
# Uses post-cleaned field names:
#   roster_name, date, distance_yd, high_speed_distance, mechanical_load
# Builds daily features and MERGE-upserts into analytics.workload_daily
# ──────────────────────────────────────────────────────────────────────────────

# ===== Packages =====
tryCatch({
  suppressPackageStartupMessages({
    library(bigrquery); library(DBI)
    library(dplyr); library(tidyr); library(readr); library(readxl)
    library(stringr); library(tibble); library(lubridate)
    library(slider); library(janitor)
    library(httr); library(gargle); library(glue)
  })
}, error = function(e) { cat("Error loading packages:", e$message, "\n"); quit(status=1) })

options(bigrquery.use_bqstorage = FALSE)
Sys.setenv(BIGRQUERY_USE_BQ_STORAGE = "false")

# ===== Config =====
project    <- Sys.getenv("GCP_PROJECT", "sac-vald-hub")
dataset    <- Sys.getenv("BQ_DATASET",  "analytics")
location   <- Sys.getenv("BQ_LOCATION", "US")
table_out  <- Sys.getenv("BQ_TABLE",    "workload_daily")
write_mode <- toupper(Sys.getenv("BQ_WRITE_MODE", "MERGE"))  # MERGE or TRUNCATE

public_url <- Sys.getenv("ONEDRIVE_PUBLIC_URL", "")
local_file <- Sys.getenv("LOCAL_FILE_PATH", "")

cat("GCP Project:", project, "\n")
cat("BQ Dataset:", dataset, "\n")
cat("BQ Location:", location, "\n")
cat("BQ Table:", table_out, "\n")
cat("Write mode:", write_mode, "\n")

# ===== Logging =====
log <- function(msg, lvl="INFO"){
  ts <- format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz="UTC")
  cat(sprintf("[%s] [%s] %s\n", ts, lvl, msg))
}
script_start <- Sys.time()
log("=== WORKLOAD INGEST START ===","START")

# ===== Auth (gcloud → gargle token) =====
GLOBAL_ACCESS_TOKEN <- NULL
tryCatch({
  log("Authenticating to BigQuery via gcloud")
  tok <- system("gcloud auth print-access-token", intern = TRUE)
  GLOBAL_ACCESS_TOKEN <<- tok[1]; stopifnot(nzchar(GLOBAL_ACCESS_TOKEN))
  bq_auth(token = gargle::gargle2.0_token(
    scope='https://www.googleapis.com/auth/bigquery',
    client=gargle::gargle_client(),
    credentials=list(access_token=GLOBAL_ACCESS_TOKEN)
  ))
}, error=function(e){ log(paste("BigQuery auth failed:", e$message), "ERROR"); quit(status=1) })

refresh_token <- function(){
  tok <- system("gcloud auth print-access-token", intern = TRUE)
  if (nzchar(tok[1]) && tok[1] != GLOBAL_ACCESS_TOKEN) {
    GLOBAL_ACCESS_TOKEN <<- tok[1]
    bq_auth(token = gargle::gargle2.0_token(
      scope='https://www.googleapis.com/auth/bigquery',
      client=gargle::gargle_client(),
      credentials=list(access_token=GLOBAL_ACCESS_TOKEN)
    ))
    log("Token refreshed")
  }
}

# ===== Helpers =====
parse_date_robust <- function(x){
  if (inherits(x,"Date"))   return(x)
  if (inherits(x,"POSIXt")) return(as.Date(x))
  if (is.numeric(x))        return(as.Date(x, origin="1899-12-30"))
  v <- as.character(x)
  d <- suppressWarnings(as.Date(v, "%Y-%m-%d"))
  d[is.na(d)] <- suppressWarnings(as.Date(v[is.na(d)], "%m/%d/%Y"))
  d[is.na(d)] <- suppressWarnings(as.Date(v[is.na(d)], "%m-%d-%Y"))
  d[is.na(d)] <- suppressWarnings(as.Date(v[is.na(d)], "%d/%m/%Y"))
  d[is.na(d)] <- suppressWarnings(as.Date(v[is.na(d)], "%d-%m-%Y"))
  d
}

monotony_roll <- function(x, idx, k){
  slider::slide_index_dbl(
    x, idx,
    .f = ~ {
      m <- mean(.x, na.rm = TRUE); s <- sd(.x, na.rm = TRUE)
      if (length(na.omit(.x)) < 2 || is.na(s) || s == 0) NA_real_ else m/s
    },
    .before = lubridate::days(k-1), .complete = FALSE
  )
}

ensure_table <- function(tbl, data, partition_field="date", cluster_fields=c("roster_name")){
  if (bq_table_exists(tbl)) return(invisible(TRUE))
  tp <- if (partition_field %in% names(data)) list(type="DAY", field=partition_field) else NULL
  cl <- intersect(cluster_fields, names(data)); if (length(cl)==0) cl <- NULL
  bq_table_create(tbl, fields = as_bq_fields(data), time_partitioning = tp, clustering_fields = cl)
  log(glue("Created {tbl$table} (partition={partition_field}; cluster={paste(cl %||% '', collapse=',')})"))
}

# Download file from OneDrive/SharePoint (force download, follow redirects)
download_public_file <- function(url) {
  if (!nzchar(url)) stop("ONEDRIVE_PUBLIC_URL is empty")
  tf <- tempfile(fileext = "")  # we'll infer type
  final <- if (grepl("\\?", url)) paste0(url,"&download=1") else paste0(url,"?download=1")
  r <- httr::GET(final, httr::write_disk(tf, overwrite = TRUE), httr::timeout(180), httr::add_headers(Accept="*/*"))
  if (httr::http_error(r)) {
    # try original
    r2 <- httr::GET(url, httr::write_disk(tf, overwrite = TRUE), httr::timeout(180), httr::add_headers(Accept="*/*"))
    httr::stop_for_status(r2)
  }
  ctype <- httr::headers(r)[["content-type"]]
  if (is.null(ctype)) ctype <- ""
  sig <- tryCatch(readBin(tf, what="raw", n=4), error=function(e) raw(0))

  list(path=tf, content_type=ctype, signature=sig)
}

# Try reading XLSX/CSV; auto-pick the sheet with roster_name + date
read_any_tabular <- function(fetch) {
  is_html <- grepl("text/html", fetch$content_type, ignore.case = TRUE)
  # XLSX if content-type says so or ZIP signature "PK"
  is_xlsx <- grepl("spreadsheetml|application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                   fetch$content_type, ignore.case=TRUE) ||
             (length(fetch$signature) >= 2 && rawToChar(fetch$signature[1:2]) == "PK")

  if (is_html) {
    log("Got HTML (viewer page) instead of a file. Ensure the link is public and has ?download=1", "ERROR")
    stop("HTML response from OneDrive link.")
  }

  if (nzchar(local_file)) {
    # local file path short-circuit handled outside; keeping for clarity
    log(glue("Reading local file: {local_file}"))
  }

  if (is_xlsx || grepl("\\.xlsx$", fetch$path, ignore.case=TRUE)) {
    log("Detected XLSX file; scanning sheets to find roster_name/date")
    sh <- readxl::excel_sheets(fetch$path)
    if (length(sh) == 0) stop("No sheets in XLSX")
    for (s in sh) {
      df <- tryCatch(readxl::read_excel(fetch$path, sheet=s), error=function(e) NULL)
      if (is.null(df)) next
      dfc <- janitor::clean_names(df)
      if (all(c("roster_name") %in% names(dfc)) && any(c("date") %in% names(dfc))) {
        log(glue("Using sheet: {s}"))
        return(dfc)
      }
    }
    # Fallback to first sheet if not found; caller will error with column list
    log("Could not find a sheet with roster_name/date; using first sheet", "WARN")
    return(janitor::clean_names(readxl::read_excel(fetch$path, sheet = 1)))
  }

  log("Detected CSV or unknown; reading with readr::read_csv")
  return(janitor::clean_names(readr::read_csv(fetch$path, show_col_types = FALSE)))
}

# ===== Read data (local overrides URL) =====
raw_std <- NULL
if (nzchar(local_file)) {
  if (grepl("\\.xlsx$", local_file, ignore.case=TRUE)) {
    log(glue("Reading local XLSX: {local_file}"))
    tmp <- readxl::read_excel(local_file, sheet = 1)
    raw_std <- janitor::clean_names(tmp)
  } else {
    log(glue("Reading local CSV: {local_file}"))
    tmp <- readr::read_csv(local_file, show_col_types = FALSE)
    raw_std <- janitor::clean_names(tmp)
  }
} else if (nzchar(public_url)) {
  log("Downloading from OneDrive/SharePoint (public link)…")
  fetched <- download_public_file(public_url)
  raw_std <- read_any_tabular(fetched)
} else {
  stop("Provide ONEDRIVE_PUBLIC_URL or LOCAL_FILE_PATH")
}

log(glue("Columns (cleaned): {paste(names(raw_std), collapse=', ')}"))

# ===== Validate required columns =====
required <- c("roster_name", "date")
missing  <- setdiff(required, names(raw_std))
if (length(missing) > 0) {
  log(glue("Missing required columns: {paste(missing, collapse=', ')}"), "ERROR")
  quit(status=1)
}

# Metrics (present in your export; still guarded)
distance_col <- if ("distance_yd" %in% names(raw_std)) "distance_yd" else NULL
hsd_col      <- if ("high_speed_distance" %in% names(raw_std)) "high_speed_distance" else NULL
ml_col       <- if ("mechanical_load" %in% names(raw_std)) "mechanical_load" else NULL
if (is.null(distance_col)) log("distance_yd not found; defaulting to 0s", "WARN")
if (is.null(hsd_col))      log("high_speed_distance not found; defaulting to 0s", "WARN")
if (is.null(ml_col))       log("mechanical_load not found; defaulting to 0s", "WARN")

# ===== Transform → features
work_data0 <- raw_std %>%
  transmute(
    roster_name = .data[["roster_name"]],
    date        = parse_date_robust(.data[["date"]]),
    distance    = suppressWarnings(as.numeric(if (!is.null(distance_col)) .data[[distance_col]] else 0)),
    hsd         = suppressWarnings(as.numeric(if (!is.null(hsd_col))      .data[[hsd_col]]      else 0)),
    ml          = suppressWarnings(as.numeric(if (!is.null(ml_col))       .data[[ml_col]]       else 0))
  ) %>%
  filter(!is.na(roster_name), !is.na(date))

if (nrow(work_data0) == 0) { log("No usable rows after sanitize (need roster_name + date).", "ERROR"); quit(status=1) }

daily_sum <- work_data0 %>%
  group_by(roster_name, date) %>%
  summarise(distance_sum = sum(distance, na.rm=TRUE),
            hsd_sum      = sum(hsd,      na.rm=TRUE),
            ml_sum       = sum(ml,       na.rm=TRUE),
            .groups = "drop") %>%
  group_by(roster_name) %>%
  tidyr::complete(date = seq(min(date), max(date), by="1 day"),
                  fill = list(distance_sum=0, hsd_sum=0, ml_sum=0)) %>%
  ungroup()

roll_features <- daily_sum %>%
  group_by(roster_name) %>%
  arrange(date, .by_group = TRUE) %>%
  mutate(
    distance_prev_day = dplyr::lag(distance_sum, 1),
    hsd_prev_day      = dplyr::lag(hsd_sum, 1),
    ml_prev_day       = dplyr::lag(ml_sum, 1),

    distance_7d  = slider::slide_index_dbl(distance_sum, date, ~sum(.x, na.rm=TRUE), .before=days(6),  .complete=TRUE),
    distance_28d = slider::slide_index_dbl(distance_sum, date, ~sum(.x, na.rm=TRUE), .before=days(27), .complete=TRUE),

    hsd_7d  = slider::slide_index_dbl(hsd_sum, date, ~sum(.x, na.rm=TRUE), .before=days(6),  .complete=TRUE),
    hsd_28d = slider::slide_index_dbl(hsd_sum, date, ~sum(.x, na.rm=TRUE), .before=days(27), .complete=TRUE),

    ml_7d  = slider::slide_index_dbl(ml_sum, date, ~sum(.x, na.rm=TRUE), .before=days(6),  .complete=TRUE),
    ml_28d = slider::slide_index_dbl(ml_sum, date, ~sum(.x, na.rm=TRUE), .before=days(27), .complete=TRUE),

    distance_monotony_7d = monotony_roll(distance_sum, date, 7),
    ml_monotony_7d       = monotony_roll(ml_sum,       date, 7)
  ) %>%
  ungroup()

work_data <- roll_features %>%
  filter(distance_sum > 0, !is.na(distance_monotony_7d), !is.na(ml_monotony_7d)) %>%
  transmute(
    roster_name,
    date,
    distance            = distance_prev_day,
    high_speed_distance = hsd_prev_day,
    mechanical_load     = ml_prev_day,
    distance_7d, distance_28d, distance_monotony_7d,
    hsd_7d, hsd_28d,
    ml_7d, ml_28d, ml_monotony_7d
  ) %>%
  arrange(roster_name, date)

log(glue("Rows after transform: {nrow(work_data)}"))
if (nrow(work_data) > 0) {
  cat("Date range:", as.character(min(work_data$date, na.rm=TRUE)), "→", as.character(max(work_data$date, na.rm=TRUE)), "\n")
}

# ===== Upload (MERGE by roster_name+date via truncate-rewrite) =====
tryCatch({
  refresh_token()
  ds  <- bq_dataset(project, dataset)
  tbl <- bq_table(ds, table_out)

  ensure_table(tbl, work_data, partition_field="date", cluster_fields=c("roster_name"))

  if (write_mode == "TRUNCATE") {
    bq_table_upload(tbl, work_data, write_disposition = "WRITE_TRUNCATE")
    log("TRUNCATE upload complete.")
  } else {
    existing <- if (bq_table_exists(tbl)) tryCatch(bq_table_download(tbl), error=function(e) tibble()) else tibble()
    if (nrow(existing) > 0) {
      existing <- existing %>% mutate(date = as.Date(date))
      merged <- existing %>%
        anti_join(work_data %>% select(roster_name, date), by=c("roster_name","date")) %>%
        bind_rows(work_data)
      bq_table_upload(tbl, merged, write_disposition = "WRITE_TRUNCATE")
      log(glue("MERGE complete; rows now: {nrow(merged)}"))
    } else {
      bq_table_upload(tbl, work_data, write_disposition = "WRITE_TRUNCATE")
      log("Initial upload complete.")
    }
  }
}, error=function(e){
  log(paste("Upload failed:", e$message), "ERROR"); quit(status=1)
})

dur <- round(as.numeric(difftime(Sys.time(), script_start, units="mins")), 2)
log(glue("Total execution time: {dur} minutes"))
log("=== WORKLOAD INGEST END ===", "END")
cat("Script completed successfully\n")
