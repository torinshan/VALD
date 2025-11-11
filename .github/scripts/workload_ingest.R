#!/usr/bin/env Rscript

# ===== Packages =====
tryCatch({
  suppressPackageStartupMessages({
    library(bigrquery); library(DBI)
    library(dplyr); library(tidyr); library(readr); library(stringr)
    library(purrr); library(tibble); library(data.table)
    library(hms); library(lubridate)
    library(httr); library(jsonlite); library(curl)
    library(gargle); library(glue); library(slider); library(janitor); library(rlang)
  })
}, error = function(e) { cat("Error loading packages:", e$message, "\n"); quit(status=1) })

# Disable BigQuery Storage API
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

# ===== BigQuery auth via gcloud token =====
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

# ===== OneDrive public-link download =====
download_public_onedrive <- function(url) {
  if (!nzchar(url)) stop("ONEDRIVE_PUBLIC_URL is empty")
  dl1 <- if (grepl("\\?", url)) paste0(url, "&download=1") else paste0(url, "?download=1")
  tf <- tempfile(fileext = ".csv")
  r1 <- httr::GET(dl1, httr::write_disk(tf, overwrite = TRUE), httr::timeout(180))
  if (httr::http_error(r1)) stop(httr::http_status(r1)$message)
  if (file.info(tf)$size < 50) {
    r2 <- httr::GET(url, httr::write_disk(tf, overwrite = TRUE), httr::timeout(180))
    if (httr::http_error(r2)) stop(httr::http_status(r2)$message)
  }
  tf
}

# ===== Helpers =====
monotony_roll <- function(x, idx, window_days) {
  slider::slide_index_dbl(
    x, idx,
    .f = ~ {
      m <- mean(.x, na.rm = TRUE)
      s <- sd(.x,  na.rm = TRUE)
      if (length(na.omit(.x)) < 2 || is.na(s) || s == 0) NA_real_ else m / s
    },
    .before = lubridate::days(window_days - 1),
    .complete = FALSE
  )
}

# Robust, deterministic date parser (single definition)
parse_date_robust <- function(vec) {
  if (is.function(vec)) stop("parse_date_robust() received a function instead of a vector. Use .data[['date']].")
  if (inherits(vec, "Date"))    return(vec)
  if (inherits(vec, "POSIXt"))  return(as.Date(vec))
  if (is.numeric(vec)) return(as.Date(vec, origin = "1899-12-30"))
  if (is.character(vec)) {
    v <- trimws(vec); v[nchar(v)==0] <- NA_character_
    suppressWarnings({
      d <- as.Date(v, "%Y-%m-%d")
      d[is.na(d)] <- as.Date(v[is.na(d)], "%m/%d/%Y")
      d[is.na(d)] <- as.Date(v[is.na(d)], "%m-%d-%Y")
      d[is.na(d)] <- as.Date(v[is.na(d)], "%d/%m/%Y")
      d[is.na(d)] <- as.Date(v[is.na(d)], "%d-%m-%Y")
    })
    return(d)
  }
  rep(as.Date(NA), length(vec))
}

# Pick the first existing column name from candidates
pick_col <- function(df, candidates) {
  nm <- candidates[candidates %in% names(df)]
  if (length(nm) == 0) return(NULL)
  nm[[1]]
}

# ===== Read CSV (force stable types) =====
csv_path <- NULL
tryCatch({
  if (nzchar(public_url)) {
    create_log_entry("Downloading CSV via public OneDrive/SharePoint link (no auth)")
    csv_path <- download_public_onedrive(public_url)
  } else if (nzchar(local_file)) {
    create_log_entry(glue("Reading local file: {local_file}"))
    csv_path <- local_file
  } else {
    stop("Provide ONEDRIVE_PUBLIC_URL (or LOCAL_FILE_PATH).")
  }
}, error = function(e) {
  create_log_entry(paste("CSV fetch failed:", e$message), "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
})

raw <- tryCatch({
  readr::read_csv(
    csv_path,
    col_types = cols(.default = col_character()),  # keep control
    show_col_types = FALSE
  )
}, error=function(e){
  create_log_entry(paste("CSV parse error:", e$message), "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
})

# ===== Transform =====
# Standardize names first
raw_std <- raw %>% janitor::clean_names()

# Map dynamic metric columns (tolerate trailing underscores or alternate names)
distance_nm <- pick_col(raw_std, c("distance_yd", "distance_yd_", "distance_yards", "distance"))
hsd_nm      <- pick_col(raw_std, c("high_speed_distance", "hsd", "hsd_yards"))
ml_nm       <- pick_col(raw_std, c("mechanical_load", "mech_load", "ml"))

# Basic presence checks (warn if any missing; we’ll treat them as zeros later)
if (is.null(distance_nm)) create_log_entry("Distance column not found; treating as 0s", "WARN")
if (is.null(hsd_nm))      create_log_entry("High speed distance column not found; treating as 0s", "WARN")
if (is.null(ml_nm))       create_log_entry("Mechanical load column not found; treating as 0s", "WARN")

# Build a minimal frame with stable column names
work_data0 <- raw_std %>%
  transmute(
    roster_name = coalesce(.data[["roster_name"]], .data[["name"]]),
    date_raw    = .data[["date"]],
    distance_src = if (!is.null(distance_nm)) .data[[distance_nm]] else NA_character_,
    hsd_src      = if (!is.null(hsd_nm))      .data[[hsd_nm]]      else NA_character_,
    ml_src       = if (!is.null(ml_nm))       .data[[ml_nm]]       else NA_character_
  ) %>%
  mutate(
    # Parse date from the explicit vector to avoid base::date() collision
    date = parse_date_robust(.data[['date_raw']]),
    # Coerce numeric safely
    distance_src = suppressWarnings(as.numeric(distance_src)),
    hsd_src      = suppressWarnings(as.numeric(hsd_src)),
    ml_src       = suppressWarnings(as.numeric(ml_src))
  ) %>%
  select(-date_raw)

if (all(is.na(work_data0$date))) {
  create_log_entry("After parsing, all 'date' values are NA. Sample raw values:", "ERROR")
  print(utils::head(unique(raw_std[["date"]]), 10))
  upload_logs_to_bigquery(); quit(status=1)
}

# Drop rows with no roster_name or date
work_data0 <- work_data0 %>% filter(!is.na(roster_name), !is.na(date))

create_log_entry(glue("Rows after initial sanitize: {nrow(work_data0)}"))

# Daily aggregate (sum) and complete date continuity per athlete
daily_sum <- work_data0 %>%
  group_by(roster_name, date) %>%
  summarise(
    distance_sum = sum(distance_src, na.rm = TRUE),
    hsd_sum      = sum(hsd_src,      na.rm = TRUE),
    ml_sum       = sum(ml_src,       na.rm = TRUE),
    .groups = "drop"
  ) %>%
  group_by(roster_name) %>%
  tidyr::complete(
    date = seq(min(date), max(date), by="1 day"),
    fill = list(distance_sum=0, hsd_sum=0, ml_sum=0)
  ) %>%
  ungroup()

# Rolling windows + monotony; lag previous day for features
roll_features <- daily_sum %>%
  group_by(roster_name) %>%
  arrange(date, .by_group = TRUE) %>%
  mutate(
    distance_prev_day = dplyr::lag(distance_sum, 1),
    hsd_prev_day      = dplyr::lag(hsd_sum, 1),
    ml_prev_day       = dplyr::lag(ml_sum, 1),

    distance_7d  = slider::slide_index_dbl(distance_sum, date, ~sum(.x, na.rm=TRUE),
                                           .before = lubridate::days(6),  .complete = TRUE),
    distance_28d = slider::slide_index_dbl(distance_sum, date, ~sum(.x, na.rm=TRUE),
                                           .before = lubridate::days(27), .complete = TRUE),

    hsd_7d  = slider::slide_index_dbl(hsd_sum, date, ~sum(.x, na.rm=TRUE),
                                      .before = lubridate::days(6),  .complete = TRUE),
    hsd_28d = slider::slide_index_dbl(hsd_sum, date, ~sum(.x, na.rm=TRUE),
                                      .before = lubridate::days(27), .complete = TRUE),

    ml_7d  = slider::slide_index_dbl(ml_sum, date, ~sum(.x, na.rm=TRUE),
                                     .before = lubridate::days(6),  .complete = TRUE),
    ml_28d = slider::slide_index_dbl(ml_sum, date, ~sum(.x, na.rm=TRUE),
                                     .before = lubridate::days(27), .complete = TRUE),

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

create_log_entry(glue("Rows after transform: {nrow(work_data)}"))
if (nrow(work_data) > 0) {
  cat("Date range:", as.character(min(work_data$date, na.rm=TRUE)), "→",
      as.character(max(work_data$date, na.rm=TRUE)), "\n")
}

# ===== BQ helpers (DML-free MERGE) =====
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

  # Create pk for merge logic
  df <- df %>% mutate(pk = paste0(roster_name, "|", as.character(date)))

  # Ensure table exists BEFORE adding pk (to keep schema clean)
  df_no_pk <- df %>% select(-pk)
  ensure_table(tbl, df_no_pk, partition_field="date", cluster_fields=c("roster_name"))

  # Validate against schema (without pk)
  df_no_pk <- validate_against_schema(df_no_pk, tbl)

  if (mode == "TRUNCATE" || !bq_table_exists(tbl) || isTRUE(bq_table_meta(tbl)$numRows == "0")) {
    refresh_token_if_needed()
    bq_table_upload(tbl, df_no_pk, write_disposition = "WRITE_TRUNCATE")
    nr <- tryCatch(bq_table_meta(tbl)$numRows, error=function(e) NA)
    create_log_entry(glue("TRUNCATE upload complete; {table_name} rows: {nr}"))
    return(TRUE)
  }

  existing <- read_bq_table_rest(tbl)
  if (nrow(existing)==0) {
    refresh_token_if_needed()
    bq_table_upload(tbl, df_no_pk, write_disposition = "WRITE_TRUNCATE")
    create_log_entry(glue("Initial upload to {table_name}"))
    return(TRUE)
  }

  # Add pk to existing for merge, align schemas
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

# ===== Upload to BigQuery =====
tryCatch({
  out <- work_data %>%
    select(roster_name, date, distance, high_speed_distance, mechanical_load,
           distance_7d, distance_28d, distance_monotony_7d,
           hsd_7d, hsd_28d,
           ml_7d, ml_28d, ml_monotony_7d)

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
