#!/usr/bin/env Rscript

# ===== Packages =====
tryCatch({
  suppressPackageStartupMessages({
    library(bigrquery); library(DBI)
    library(dplyr); library(tidyr); library(readr); library(readxl)
    library(stringr); library(purrr); library(tibble); library(data.table)
    library(hms); library(lubridate)
    library(httr); library(jsonlite); library(curl)
    library(gargle); library(glue); library(slider); library(janitor)
  })
}, error = function(e) { cat("Error loading packages:", e$message, "\n"); quit(status=1) })

# ===== Options =====
options(bigrquery.use_bqstorage = FALSE)
Sys.setenv(BIGRQUERY_USE_BQ_STORAGE = "false")

# ===== Config =====
project     <- Sys.getenv("GCP_PROJECT", "sac-vald-hub")
dataset     <- Sys.getenv("BQ_DATASET",  "analytics")
location    <- Sys.getenv("BQ_LOCATION","US")
table_out   <- Sys.getenv("BQ_TABLE",    "workload_daily")
write_mode  <- toupper(Sys.getenv("BQ_WRITE_MODE", "MERGE"))   # MERGE or TRUNCATE
public_url  <- Sys.getenv("ONEDRIVE_PUBLIC_URL", "")
local_file  <- Sys.getenv("LOCAL_FILE_PATH", "")

cat("GCP Project:", project, "\n")
cat("BQ Dataset:", dataset, "\n")
cat("BQ Location:", location, "\n")
cat("BQ Table:", table_out, "\n")
cat("Write mode:", write_mode, "\n")

# ===== Logging =====
log_entries <- tibble(
  timestamp = as.POSIXct(character(0)),
  level = character(0),
  message = character(0),
  run_id = character(0),
  repository = character(0)
)
create_log_entry <- function(message, level="INFO") {
  ts <- Sys.time()
  cat(sprintf("[%s] [%s] %s\n", format(ts, "%Y-%m-%d %H:%M:%S", tz="UTC"), level, message))
  log_entries <<- bind_rows(log_entries, tibble(
    timestamp = ts, level = level, message = message,
    run_id = Sys.getenv("GITHUB_RUN_ID", "manual"),
    repository = Sys.getenv("GITHUB_REPOSITORY", "unknown")
  ))
}
upload_logs_to_bigquery <- function() {
  if (nrow(log_entries)==0) return(invisible(TRUE))
  tryCatch({
    ds <- bq_dataset(project, dataset)
    if (!bq_dataset_exists(ds)) bq_dataset_create(ds, location = location)
    log_tbl <- bq_table(ds, "workload_ingest_log")
    if (!bq_table_exists(log_tbl)) bq_table_create(log_tbl, fields = as_bq_fields(log_entries))
    bq_table_upload(log_tbl, log_entries, write_disposition = "WRITE_APPEND")
    TRUE
  }, error=function(e){ cat("Log upload failed:", e$message, "\n"); FALSE })
}
script_start <- Sys.time()
create_log_entry("=== WORKLOAD INGEST START ===", "START")

# ===== Auth =====
GLOBAL_ACCESS_TOKEN <- NULL
tryCatch({
  create_log_entry("Authenticating to BigQuery via gcloud")
  tok <- system("gcloud auth print-access-token", intern = TRUE)
  GLOBAL_ACCESS_TOKEN <<- tok[1]
  stopifnot(nzchar(GLOBAL_ACCESS_TOKEN))
  bq_auth(token = gargle::gargle2.0_token(
    scope = 'https://www.googleapis.com/auth/bigquery',
    client = gargle::gargle_client(),
    credentials = list(access_token = GLOBAL_ACCESS_TOKEN)
  ))
  ds <- bq_dataset(project, dataset)
  if (!bq_dataset_exists(ds)) { bq_dataset_create(ds, location = location); create_log_entry(glue("Created dataset {dataset}")) }
}, error = function(e) {
  create_log_entry(paste("BigQuery auth failed:", e$message), "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
})

refresh_token_if_needed <- function() {
  tryCatch({
    tok <- system("gcloud auth print-access-token", intern = TRUE)
    if (nzchar(tok[1]) && tok[1] != GLOBAL_ACCESS_TOKEN) {
      GLOBAL_ACCESS_TOKEN <<- tok[1]
      bq_auth(token = gargle::gargle2.0_token(
        scope = 'https://www.googleapis.com/auth/bigquery',
        client = gargle::gargle_client(),
        credentials = list(access_token = GLOBAL_ACCESS_TOKEN)
      ))
      create_log_entry("Token refreshed", "INFO")
    }
  }, error = function(e) {
    create_log_entry(paste("Token refresh warning:", e$message), "WARN")
  })
}

# ===== File readers =====
read_any_tabular <- function(path) {
  ext <- tolower(tools::file_ext(path))
  if (ext %in% c("xlsx", "xls")) {
    create_log_entry(glue("Reading Excel: {path}"))
    df <- readxl::read_excel(path, guess_max = 200000) |> as.data.frame()
    return(df)
  } else if (ext %in% c("csv", "txt")) {
    create_log_entry(glue("Reading CSV: {path}"))
    return(readr::read_csv(path, show_col_types = FALSE))
  } else {
    stop(glue("Unsupported file extension: .{ext}"))
  }
}

# ===== Date helpers =====
parse_date_robust <- function(vec) {
  if (is.function(vec)) stop("parse_date_robust() received a function; pass the column vector instead.")
  if (inherits(vec, "Date"))   return(vec)
  if (inherits(vec, "POSIXt")) return(as.Date(vec))
  if (is.numeric(vec)) return(as.Date(vec, origin = "1899-12-30"))
  if (is.character(vec)) {
    v <- trimws(vec); v[nchar(v) == 0] <- NA_character_
    suppressWarnings({
      # Prefer American format first since source uses M/D/YYYY
      d <- as.Date(v, format = "%m/%d/%Y")
      d[is.na(d)] <- as.Date(v[is.na(d)], format = "%Y-%m-%d")
      d[is.na(d)] <- as.Date(v[is.na(d)], format = "%m-%d-%Y")
      d[is.na(d)] <- as.Date(v[is.na(d)], format = "%d/%m/%Y")
      d[is.na(d)] <- as.Date(v[is.na(d)], format = "%d-%m-%Y")
    })
    return(d)
  }
  rep(as.Date(NA), length(vec))
}

monotony_roll <- function(x, idx, window_days, complete = TRUE) {
  slider::slide_index_dbl(
    x, idx,
    .f = ~ {
      v <- as.numeric(.x); v <- v[is.finite(v)]
      if (length(v) < 2) return(NA_real_)
      s <- stats::sd(v); m <- mean(v)
      if (!is.finite(s) || s == 0) return(NA_real_)
      m / s
    },
    .before = lubridate::days(window_days - 1),
    .complete = complete
  )
}

# ===== Load file (LOCAL_FILE_PATH only) =====
fpath <- local_file
tryCatch({
  if (!nzchar(fpath)) stop("LOCAL_FILE_PATH is empty")
  if (!file.exists(fpath)) stop(glue("LOCAL_FILE_PATH not found: {fpath}"))
  create_log_entry("Loading local file (repo)")
}, error = function(e) {
  create_log_entry(paste("File fetch failed:", e$message), "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
})

raw <- tryCatch({
  read_any_tabular(fpath)
}, error = function(e) {
  create_log_entry(paste("File parse error:", e$message), "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
})

# ===== Transform =====
# Clean names first and work only with cleaned names
cleaned_names <- janitor::make_clean_names(names(raw))
if (!all(c("roster_name","date") %in% cleaned_names)) {
  create_log_entry(glue("CSV/XLSX columns (raw): {paste(names(raw), collapse=', ')}"))
  create_log_entry(glue("CSV/XLSX columns (cleaned): {paste(cleaned_names, collapse=', ')}"))
  create_log_entry("Missing required columns: roster_name and/or date (after cleaning).", "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
}

required_cols_clean <- c(
  "name","date","roster_name",
  "distance_yd","mechanical_load","high_speed_distance","decel_4"
)

# Safe date parser wrapper (returns NA on any error)
safe_parse_date <- purrr::possibly(parse_date_robust, otherwise = as.Date(NA))

work_data0 <- raw %>%
  janitor::clean_names() %>%
  dplyr::select(any_of(required_cols_clean)) %>%
  mutate(
    date = safe_parse_date(.data[["date"]])
  ) %>%
  # Ensure numeric
  mutate(
    distance_yd         = suppressWarnings(as.numeric(.data[["distance_yd"]] %||% NA)),
    high_speed_distance = suppressWarnings(as.numeric(.data[["high_speed_distance"]] %||% NA)),
    mechanical_load     = suppressWarnings(as.numeric(.data[["mechanical_load"]] %||% NA))
  ) %>%
  { tmp <- .; bad <- sum(is.na(tmp$date)); if (bad > 0) create_log_entry(glue("Date parse: {bad} invalid entries (NA) detected; rows will be dropped"), "WARN"); tmp } %>%
  filter(!is.na(roster_name), !is.na(date))


daily_sum <- work_data0 %>%
  group_by(roster_name, date) %>%
  summarise(
    distance_sum = sum(distance_yd,         na.rm = TRUE),
    hsd_sum      = sum(high_speed_distance, na.rm = TRUE),
    ml_sum       = sum(mechanical_load,     na.rm = TRUE),
    .groups = "drop"
  ) %>%
  group_by(roster_name) %>%
  tidyr::complete(
    date = seq(min(date), max(date), by = "1 day"),
    fill = list(distance_sum = 0, hsd_sum = 0, ml_sum = 0)
  ) %>%
  ungroup()

roll_features <- daily_sum %>%
  group_by(roster_name) %>%
  arrange(date, .by_group = TRUE) %>%
  mutate(
    distance_prev_day = dplyr::lag(distance_sum, 1),
    hsd_prev_day      = dplyr::lag(hsd_sum, 1),
    ml_prev_day       = dplyr::lag(ml_sum, 1),

    distance_7d  = slider::slide_index_dbl(distance_sum, date, ~sum(.x, na.rm=TRUE), .before = days(6),  .complete = TRUE),
    distance_28d = slider::slide_index_dbl(distance_sum, date, ~sum(.x, na.rm=TRUE), .before = days(27), .complete = FALSE),

    hsd_7d  = slider::slide_index_dbl(hsd_sum, date, ~sum(.x, na.rm=TRUE), .before = days(6),  .complete = TRUE),
    hsd_28d = slider::slide_index_dbl(hsd_sum, date, ~sum(.x, na.rm=TRUE), .before = days(27), .complete = FALSE),

    ml_7d  = slider::slide_index_dbl(ml_sum,  date, ~sum(.x, na.rm=TRUE), .before = days(6),  .complete = TRUE),
    ml_28d = slider::slide_index_dbl(ml_sum,  date, ~sum(.x, na.rm=TRUE), .before = days(27), .complete = FALSE),

    distance_monotony_7d = monotony_roll(distance_sum, date, 7),
    hsd_monotony_7d      = monotony_roll(hsd_sum,      date, 7),
    ml_monotony_7d       = monotony_roll(ml_sum,       date, 7),
    
    is_rest_day = (distance_sum == 0 & hsd_sum == 0 & ml_sum == 0)
  ) %>%
  ungroup()

work_data <- roll_features %>%
  filter(!is.na(distance_7d) & !is.na(hsd_7d) & !is.na(ml_7d)) %>%
  transmute(
    roster_name,
    date,
    distance            = distance_prev_day,
    high_speed_distance = hsd_prev_day,
    mechanical_load     = ml_prev_day,
    distance_7d, distance_28d, distance_monotony_7d,
    hsd_7d, hsd_28d, hsd_monotony_7d,
    ml_7d, ml_28d, ml_monotony_7d,
    is_rest_day
  ) %>%
  arrange(roster_name, date)

create_log_entry(glue("Rows after 7d filter (drop only first 6 days per athlete): {nrow(work_data)}"))

# ===== Upsert helpers =====
read_bq_table_rest <- function(tbl) {
  refresh_token_if_needed()
  if (!bq_table_exists(tbl)) return(tibble())
  meta <- bq_table_meta(tbl); fields <- meta$schema$fields
  out <- list(); pageToken <- NULL; i <- 0
  repeat {
    url <- sprintf("https://bigquery.googleapis.com/bigquery/v2/projects/%s/datasets/%s/tables/%s/data",
                   tbl$project, tbl$dataset, tbl$table)
    resp <- httr::GET(url,
      httr::add_headers(Authorization=paste("Bearer", GLOBAL_ACCESS_TOKEN)),
      query = list(maxResults=10000, pageToken=pageToken)
    )
    httr::stop_for_status(resp)
    ctt <- httr::content(resp, "parsed")
    if (is.null(ctt$rows)) break
    page <- lapply(ctt$rows, function(r) {
      vals <- lapply(seq_along(fields), function(j) {
        v <- r$f[[j]]$v; f <- fields[[j]]
        if (is.null(v)) return(NA)
        if (f$type %in% c("INTEGER","INT64")) return(as.integer(v))
        if (f$type %in% c("FLOAT","FLOAT64")) return(as.numeric(v))
        if (f$type == "DATE") return(as.Date(v))
        if (f$type == "TIMESTAMP") return(as.POSIXct(as.numeric(as.character(v)), origin="1970-01-01", tz="UTC"))
        if (f$type == "TIME") return(hms::as_hms(v))
        as.character(v)
      })
      names(vals) <- vapply(fields, function(f) f$name, character(1))
      as_tibble(vals)
    })
    out[[length(out)+1]] <- bind_rows(page)
    pageToken <- ctt$nextPageToken; i <- i + 1
    if (is.null(pageToken) || i > 200) break
  }
  bind_rows(out)
}

ensure_table <- function(tbl, data, partition_field="date", cluster_fields=c("roster_name")) {
  if (bq_table_exists(tbl)) return(invisible(TRUE))
  tp <- if (!is.null(partition_field) && partition_field %in% names(data))
    list(type="DAY", field=partition_field) else NULL
  cl <- intersect(cluster_fields, names(data)); if (length(cl)==0) cl <- NULL
  bq_table_create(tbl, fields = as_bq_fields(data), time_partitioning = tp, clustering_fields = cl)
  create_log_entry(glue("Created {tbl$table} (partition={partition_field}; cluster={paste(cl, collapse=',')} )"))
}

validate_against_schema <- function(data, tbl) {
  if (!bq_table_exists(tbl)) return(data)
  ex <- bq_table_meta(tbl)
  sch <- vapply(ex$schema$fields, function(f) f$name, character(1))
  drop <- setdiff(names(data), sch)
  if (length(drop)>0) {
    create_log_entry(paste("Dropping", length(drop), "cols not in BQ schema:", paste(head(drop, 12), collapse=", ")), "WARN")
    data <- dplyr::select(data, -any_of(drop))
  }
  data
}
bq_upsert <- function(df, table_name, mode=c("MERGE","TRUNCATE")) {
  mode <- match.arg(mode)
  ds <- bq_dataset(project, dataset); tbl <- bq_table(ds, table_name)
  if (nrow(df)==0) { create_log_entry(glue("No rows to upload for {table_name}")); return(TRUE) }

  ensure_table(tbl, df, partition_field="date", cluster_fields=c("roster_name"))
  df <- validate_against_schema(df, tbl)

  if (mode == "TRUNCATE" || !bq_table_exists(tbl) || isTRUE(bq_table_meta(tbl)$numRows == "0")) {
    refresh_token_if_needed()
    bq_table_upload(tbl, df, write_disposition = "WRITE_TRUNCATE")
    nr <- tryCatch(bq_table_meta(tbl)$numRows, error=function(e) NA)
    create_log_entry(glue("TRUNCATE upload complete; {table_name} rows: {nr}"))
    return(TRUE)
  }

  existing <- read_bq_table_rest(tbl)
  if (nrow(existing)==0) {
    refresh_token_if_needed()
    bq_table_upload(tbl, df, write_disposition = "WRITE_TRUNCATE")
    create_log_entry(glue("Initial upload to {table_name}"))
    return(TRUE)
  }

  # Use df (schema-validated) for MERGE logic
  df <- df %>% mutate(pk = paste0(roster_name, "|", as.character(date)))
  existing <- existing %>% mutate(pk = paste0(roster_name, "|", as.character(date)))
  df$pk <- as.character(df$pk); existing$pk <- as.character(existing$pk)
  allc <- union(names(existing), names(df))
  for (c in setdiff(allc, names(existing))) existing[[c]] <- NA
  for (c in setdiff(allc, names(df)))       df[[c]]       <- NA
  existing <- existing[, allc, drop=FALSE]
  df       <- df[, allc, drop=FALSE]

  combined <- bind_rows(existing, df) %>%
    mutate(.row_id = row_number()) %>%
    arrange(.row_id) %>%
    group_by(pk) %>%
    slice_tail(n=1) %>%
    ungroup() %>%
    select(-.row_id, -pk)

  refresh_token_if_needed()
  bq_table_upload(tbl, combined, write_disposition = "WRITE_TRUNCATE")
  nr <- tryCatch(bq_table_meta(tbl)$numRows, error=function(e) NA)
  create_log_entry(glue("MERGE complete; {table_name} rows: {nr}"))
  TRUE
}

# ===== Upload =====
tryCatch({
  out <- work_data %>%
    select(roster_name, date, distance, high_speed_distance, mechanical_load,
           distance_7d, distance_28d, distance_monotony_7d,
           hsd_7d, hsd_28d, hsd_monotony_7d,
           ml_7d, ml_28d, ml_monotony_7d,
           is_rest_day)

  bq_upsert(out, table_out, mode = ifelse(write_mode %in% c("MERGE","TRUNCATE"), write_mode, "MERGE"))
  create_log_entry("Ingest complete.")
}, error=function(e){
  create_log_entry(paste("Upload failed:", e$message), "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
})

# ===== Wrap up =====
dur <- round(as.numeric(difftime(Sys.time(), script_start, units="mins")), 2)
create_log_entry(glue("Total execution time: {dur} minutes"))
create_log_entry("=== WORKLOAD INGEST END ===", "END")
upload_logs_to_bigquery()
cat("Script completed successfully\n")
