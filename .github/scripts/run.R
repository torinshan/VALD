#!/usr/bin/env Rscript

# ============================
# VALD → BigQuery pipeline
# Edited per 2025-09-25 request:
# - Start date = latest_date_current - 1 day overlap (single incremental path)
# - CMJ table stays WRITE_TRUNCATE (needs full history for metrics)
# - Other tables use MERGE upserts on test_ID
# - Per-section "entry" gates: only process if new data for that test_type exists
# - Partition+cluster on table creation; verify uploads via metadata (no COUNT(*))
# - Rolling metrics with slider; bodyweight join via data.table rolling join
# - standardize_data_types() tightened (explicit char/date/time; rest numeric with NA handling)
# - glue + slider added to deps; fix_rsi_data_type avoids CREATE OR REPLACE
# - Reduce schema-drift warnings; drop all-NA cols before upload
# ============================

# ----------------------------
# Library load (do not change auth)
# ----------------------------
tryCatch({
  suppressPackageStartupMessages({
    library(bigrquery)
    library(DBI)
    library(dplyr)
    library(tidyr)
    library(readr)
    library(stringr)
    library(purrr)
    library(tibble)
    library(data.table)
    library(hms)
    library(lubridate)
    library(httr)
    library(jsonlite)
    library(xml2)
    library(curl)
    library(valdr)
    library(gargle)
    library(rlang)
    library(lifecycle)
    library(glue)
    library(slider)
  })
}, error = function(e) {
  cat("Error loading required packages:", e$message, "\n")
  quit(status = 1)
})

# ----------------------------
# Config
# ----------------------------
project  <- Sys.getenv("GCP_PROJECT",  "sac-vald-hub")
dataset  <- Sys.getenv("BQ_DATASET",   "analytics")
location <- Sys.getenv("BQ_LOCATION",  "US")
cat("GCP Project:", project, "\n")
cat("BQ Dataset:", dataset, "\n")
cat("BQ Location:", location, "\n")

# ----------------------------
# Authenticate (unchanged per instruction)
# ----------------------------
tryCatch({
  cat("=== Authenticating to BigQuery ===\n")
  access_token_result <- system("gcloud auth print-access-token", intern = TRUE)
  access_token <- access_token_result[1]
  if (nchar(access_token) > 0) {
    cat("Access token obtained from gcloud\n")
    token <- gargle::gargle2.0_token(
      scope = 'https://www.googleapis.com/auth/bigquery',
      client = gargle::gargle_client(),
      credentials = list(access_token = access_token)
    )
    bigrquery::bq_auth(token = token)
    cat("BigQuery authentication successful\n")
    # test
    test_con <- DBI::dbConnect(bigrquery::bigquery(), project = project)
    invisible(DBI::dbGetQuery(test_con, "SELECT 1 as test"))
    DBI::dbDisconnect(test_con)
  } else {
    stop("Could not obtain access token from gcloud")
  }
}, error = function(e) {
  cat("BigQuery authentication failed:", e$message, "\n")
  quit(status = 1)
})

con <- DBI::dbConnect(bigrquery::bigquery(), project = project)
ds <- bq_dataset(project, dataset)
if (!bq_dataset_exists(ds)) {
  bq_dataset_create(ds, location = location)
  cat("Created BigQuery dataset:", dataset, "in", location, "\n")
}

# ----------------------------
# Logging
# ----------------------------
log_entries <- tibble::tibble(
  timestamp  = as.POSIXct(character(0)),
  level      = character(0),
  message    = character(0),
  run_id     = character(0),
  repository = character(0)
)

create_log_entry <- function(message, level = "INFO") {
  ts <- Sys.time()
  cat(sprintf("[%s] [%s] %s\n", format(ts, "%Y-%m-%d %H:%M:%S", tz = "UTC"), level, message))
  log_entries <<- dplyr::bind_rows(log_entries, tibble::tibble(
    timestamp  = ts, level = level, message = message,
    run_id     = Sys.getenv("GITHUB_RUN_ID", "manual"),
    repository = Sys.getenv("GITHUB_REPOSITORY", "unknown")
  ))
}

upload_logs_to_bigquery <- function() {
  if (nrow(log_entries) == 0) return(invisible(TRUE))
  tryCatch({
    log_tbl <- bq_table(ds, "vald_processing_log")
    if (!bq_table_exists(log_tbl)) {
      bq_table_create(log_tbl, fields = bigrquery::as_bq_fields(log_entries))
    }
    bq_table_upload(log_tbl, log_entries, write_disposition = "WRITE_APPEND")
    TRUE
  }, error = function(e) {
    cat("Failed to upload logs:", e$message, "\n")
    FALSE
  })
}

script_start_time <- Sys.time()
create_log_entry("=== VALD DATA PROCESSING SCRIPT STARTED ===", "START")

# ----------------------------
# Helpers: schema & upload
# ----------------------------

# Ensure table exists with partition+cluster if possible
ensure_table <- function(tbl, data, partition_field = "date", cluster_fields = character()) {
  if (bq_table_exists(tbl)) return(invisible(TRUE))
  tp <- NULL
  if (partition_field %in% names(data)) {
    tp <- bq_time_partition("DAY", field = partition_field)
  }
  cl <- intersect(cluster_fields, names(data))
  bq_table_create(tbl, fields = as_bq_fields(data),
                  time_partitioning = tp, clustering = cl)
  create_log_entry(sprintf("Created table %s with partition=%s cluster=%s",
                           tbl$table, partition_field, paste(cl, collapse=",")))
  invisible(TRUE)
}

# Safer RSI type fix without CREATE OR REPLACE
fix_rsi_data_type <- function() {
  create_log_entry("Checking body_weight_lbs type in vald_fd_rsi")
  q <- sprintf("SELECT data_type FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS`
               WHERE table_name='vald_fd_rsi' AND column_name='body_weight_lbs'",
               project, dataset)
  tryCatch({
    t <- DBI::dbGetQuery(con, q)
    if (nrow(t) && identical(t$data_type[1], "STRING")) {
      create_log_entry("Converting body_weight_lbs STRING -> FLOAT64 via temp column")
      DBI::dbExecute(con, sprintf("ALTER TABLE `%s.%s.vald_fd_rsi` ADD COLUMN body_weight_lbs__tmp FLOAT64", project, dataset))
      DBI::dbExecute(con, sprintf("UPDATE `%s.%s.vald_fd_rsi` SET body_weight_lbs__tmp = SAFE_CAST(body_weight_lbs AS FLOAT64)", project, dataset))
      DBI::dbExecute(con, sprintf("ALTER TABLE `%s.%s.vald_fd_rsi` DROP COLUMN body_weight_lbs", project, dataset))
      DBI::dbExecute(con, sprintf("ALTER TABLE `%s.%s.vald_fd_rsi` RENAME COLUMN body_weight_lbs__tmp TO body_weight_lbs", project, dataset))
      create_log_entry("Converted body_weight_lbs to FLOAT64 in vald_fd_rsi")
    } else {
      create_log_entry("body_weight_lbs is already FLOAT64 or column missing")
    }
  }, error = function(e) {
    create_log_entry(paste("RSI type check/fix error:", e$message), "WARN")
  })
}

# Map columns: character/date/time; all others numeric (double) with NA for non-numeric
is_character_col <- function(colname) {
  colname %in% c("name","full_name","given_name","family_name","email","team",
                 "position","position_class","sport","sex","test_type","triallimb",
                 "vald_id","test_ID","external_id","unit_of_measure","unit_of_measure_symbol",
                 "definition","metric","r_name","group")
}
is_date_col <- function(colname) colname %in% c("date","date_of_birth")
is_time_col <- function(colname) colname %in% c("time")
is_integer_col <- function(colname) colname %in% c("height","reps_left","reps_right")

standardize_data_types <- function(data, table_name) {
  create_log_entry(paste("Standardizing data types for", table_name))
  if (nrow(data) == 0) return(data)

  # Drop all-NA columns to avoid schema drift
  data <- data[, names(data)[!vapply(data, function(x) all(is.na(x)), logical(1))], drop = FALSE]

  for (cn in names(data)) {
    x <- data[[cn]]
    if (is_character_col(cn)) {
      data[[cn]] <- as.character(x)
    } else if (is_date_col(cn)) {
      data[[cn]] <- as.Date(x)
    } else if (is_time_col(cn)) {
      data[[cn]] <- hms::as_hms(x)
    } else if (is_integer_col(cn)) {
      if (is.character(x)) {
        suppressWarnings(data[[cn]] <- as.integer(as.numeric(x)))
      } else {
        data[[cn]] <- as.integer(x)
      }
    } else {
      # default numeric/double; non-convertible -> NA_real_
      if (is.numeric(x)) {
        data[[cn]] <- as.numeric(x)
      } else {
        suppressWarnings(data[[cn]] <- as.numeric(x))
      }
      data[[cn]][is.infinite(data[[cn]]) | is.nan(data[[cn]])] <- NA_real_
    }
  }
  data
}

# Upload: MERGE (default) or TRUNCATE for CMJ; use metadata for verification
bq_upsert <- function(data, table_name, key = "test_ID",
                      mode = c("MERGE","TRUNCATE"),
                      partition_field = "date",
                      cluster_fields = c("team","test_type","vald_id")) {
  mode <- match.arg(mode)
  data <- standardize_data_types(data, table_name)
  tbl <- bq_table(ds, table_name)

  if (nrow(data) == 0) {
    create_log_entry(paste("No rows to upload for", table_name))
    return(TRUE)
  }

  if (!bq_table_exists(tbl)) {
    ensure_table(tbl, data, partition_field, cluster_fields)
    bq_table_upload(tbl, data, write_disposition = "WRITE_TRUNCATE")
    meta <- bq_table_meta(tbl)
    create_log_entry(glue("Uploaded {meta$numRows %||% NA} rows to new table {table_name}"))
    return(TRUE)
  }

  if (mode == "TRUNCATE") {
    bq_table_upload(tbl, data, write_disposition = "WRITE_TRUNCATE")
    meta <- bq_table_meta(tbl)
    create_log_entry(glue("Truncated+uploaded; table {table_name} now has {meta$numRows %||% NA} rows"))
    return(TRUE)
  }

  # MERGE upsert
  stage <- bq_table(ds, paste0(table_name, "_stage"))
  if (bq_table_exists(stage)) bq_table_delete(stage)
  bq_table_create(stage, fields = as_bq_fields(data))
  bq_table_upload(stage, data, write_disposition = "WRITE_TRUNCATE")

  # Build MERGE statement
  cols <- names(data)
  if (!key %in% cols) stop(glue("Key {key} not in data for {table_name}"))
  up_cols <- setdiff(cols, key)
  if (length(up_cols) == 0) stop(glue("No updatable columns for {table_name}"))

  set_clause <- paste(sprintf("T.`%s` = S.`%s`", up_cols, up_cols), collapse = ", ")
  insert_cols <- paste(sprintf("`%s`", cols), collapse = ", ")
  insert_vals <- paste(sprintf("S.`%s`", cols), collapse = ", ")

  sql <- glue("
    MERGE `{project}.{dataset}.{table_name}` T
    USING `{project}.{dataset}.{table_name}_stage` S
    ON T.`{key}` = S.`{key}`
    WHEN MATCHED THEN UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
  ")
  DBI::dbExecute(con, sql)
  bq_table_delete(stage)

  meta <- bq_table_meta(tbl)
  create_log_entry(glue("MERGE complete; table {table_name} now has {meta$numRows %||% NA} rows"))
  TRUE
}

# ----------------------------
# Read helpers
# ----------------------------
read_bq_table <- function(table_name) {
  tbl <- bq_table(ds, table_name)
  if (!bq_table_exists(tbl)) return(dplyr::tibble())
  q <- sprintf("SELECT * FROM `%s.%s.%s`", project, dataset, table_name)
  tryCatch({
    DBI::dbGetQuery(con, q)
  }, error = function(e) {
    create_log_entry(paste("Read error for", table_name, ":", e$message), "WARN")
    dplyr::tibble()
  })
}

# ----------------------------
# VALD API setup
# ----------------------------
tryCatch({
  client_id <- Sys.getenv("VALD_CLIENT_ID")
  client_secret <- Sys.getenv("VALD_CLIENT_SECRET")
  tenant_id <- Sys.getenv("VALD_TENANT_ID")
  region <- Sys.getenv("VALD_REGION", "use")
  if (client_id == "" || client_secret == "" || tenant_id == "") stop("Missing VALD creds")
  set_credentials(client_id, client_secret, tenant_id, region)
  create_log_entry("VALD API credentials configured")
}, error = function(e) {
  create_log_entry(paste("VALD setup failed:", e$message), "ERROR")
  quit(status = 1)
})

# ----------------------------
# Current BQ state
# ----------------------------
create_log_entry("=== READING CURRENT DATA STATE FROM BIGQUERY ===")
current_dates <- read_bq_table("dates") %>%
  dplyr::select(date) %>% dplyr::distinct() %>% dplyr::mutate(date = as.Date(date))
if (nrow(current_dates) == 0) current_dates <- tibble::tibble(date = as.Date(character(0)))

tests_tbl <- read_bq_table("tests") %>% dplyr::mutate(test_ID = as.character(test_ID)) %>% dplyr::distinct()

latest_date_current <- if (nrow(current_dates) > 0) max(current_dates$date, na.rm = TRUE) else as.Date("1900-01-01")
count_tests_current <- nrow(tests_tbl)
create_log_entry(glue("Current state - Latest date: {latest_date_current} Test count: {count_tests_current}"))

# ----------------------------
# Overlapped incremental window (1 day)
# ----------------------------
overlap_days <- 1L
start_dt <- if (is.finite(latest_date_current)) latest_date_current - lubridate::days(overlap_days) else as.Date("2024-01-01")
set_start_date(sprintf("%sT00:00:00Z", start_dt))
create_log_entry(glue("Using overlap window start: {start_dt}T00:00:00Z"))

# ----------------------------
# Fetch ForceDecks (single pull), then gate per test_type
# ----------------------------
create_log_entry("Fetching ForceDecks data from VALD API...")
injest_fd <- get_forcedecks_data()

# Basic frames
profiles <- as.data.table(injest_fd$profiles)
definitions <- as.data.table(injest_fd$result_definitions)
tests <- as.data.table(injest_fd$tests)
trials <- as.data.table(injest_fd$trials)

# Roster (names only; team/position will be joined from external roster if present)
roster <- as_tibble(profiles)[, c("profileId","givenName","familyName")] %>%
  mutate(full_name = paste(trimws(givenName), trimws(familyName)),
         vald_id = as.character(profileId)) %>%
  dplyr::select(vald_id, full_name)

# Process tests
tests_processed <- as_tibble(tests) %>%
  mutate(
    vald_id = as.character(profileId),
    test_type = testType,
    test_ID = as.character(testId),
    recordedDateUtc_parsed = lubridate::ymd_hms(recordedDateUtc, tz = "UTC", quiet = TRUE),
    recordedDateUtc_local = dplyr::if_else(
      recordedDateTimezone %in% c("Pacific Standard Time","Pacific Daylight Time","Pacific Time"),
      lubridate::with_tz(recordedDateUtc_parsed, "America/Los_Angeles"),
      recordedDateUtc_parsed
    ),
    date = as.Date(recordedDateUtc_local),
    time = hms::as_hms(recordedDateUtc_local)
  ) %>%
  dplyr::select(-testId, -tenantId, -profileId, -testType, -weight,
                -analysedDateUtc, -analysedDateOffset, -analysedDateTimezone,
                -recordedDateUtc_parsed, -recordedDateUtc_local, -recordedDateUtc,
                -recordedDateOffset, -recordedDateTimezone, -recordingId)

new_test_types <- sort(unique(tests_processed$test_type))

create_log_entry(glue("New ForceDecks test types present: {paste(new_test_types, collapse=', ')}"))

# Trials wide (restrict to needed numeric defs after pivot to keep code closer to original)
trials_wider <- as_tibble(trials) %>%
  tidyr::pivot_wider(
    id_cols = c(testId, trialId, athleteId, recordedUTC, recordedTimezone, trialLimb),
    names_from = definition_result, values_from = value, values_fn = dplyr::first
  ) %>%
  mutate(
    recordedUTC_parsed = ymd_hms(recordedUTC, tz="UTC", quiet=TRUE),
    recordedUTC_local = dplyr::if_else(
      recordedTimezone %in% c("Pacific Standard Time","Pacific Daylight Time","Pacific Time"),
      lubridate::with_tz(recordedUTC_parsed, "America/Los_Angeles"),
      recordedUTC_parsed
    ),
    date = as.Date(recordedUTC_local),
    time = hms::as_hms(recordedUTC_local)
  ) %>%
  dplyr::select(-recordedUTC_parsed, -recordedUTC_local) %>%
  dplyr::rename_with(tolower)

# Mergeable trials (mean by test)
mergable_trials <- trials_wider %>%
  mutate(test_ID = as.character(testid)) %>%
  group_by(test_ID) %>%
  summarise(across(where(is.numeric), ~mean(.x, na.rm = TRUE)),
            triallimb = dplyr::first(triallimb),
            date = dplyr::first(date), time = dplyr::first(time),
            athleteid = dplyr::first(athleteid), .groups="drop") %>%
  mutate(vald_id = as.character(athleteid)) %>%
  dplyr::select(-athleteid)

# External roster (optional)
Vald_roster <- read_bq_table("vald_roster")
if (nrow(Vald_roster) == 0) {
  create_log_entry("No vald_roster in BQ; proceeding without team/position", "WARN")
}
mergable_roster <- roster %>%
  left_join(Vald_roster %>% dplyr::select(vald_id, position, sport, team), by="vald_id")

# ForceDecks raw
forcedecks_raw <- mergable_trials %>%
  left_join(tests_processed %>% dplyr::select(test_ID, test_type), by="test_ID") %>%
  left_join(mergable_roster, by="vald_id") %>%
  mutate(date = as.Date(date), time = hms::as_hms(time), test_ID = as.character(test_ID))

# Body weight snapshot per day from ForceDecks
bw <- forcedecks_raw %>%
  dplyr::select(vald_id, date, body_weight_lbs) %>%
  filter(!is.na(body_weight_lbs)) %>%
  group_by(vald_id, date) %>%
  summarise(body_weight_lbs = mean(body_weight_lbs, na.rm = TRUE), .groups="drop") %>%
  arrange(vald_id, date)

# ----------------------------
# Gated processing per test_type
# ----------------------------

# Helper: attach BW by rolling join (data.table)
attach_bw <- function(df) {
  if (!all(c("vald_id","date") %in% names(df))) return(df)
  if (nrow(bw) == 0) return(df)
  dt <- as.data.table(df)
  bw_dt <- as.data.table(bw)
  setkey(bw_dt, vald_id, date)
  setkey(dt, vald_id, date)
  dt[bw_dt, body_weight_lbs := i.body_weight_lbs, roll = Inf, on = .(vald_id, date)]
  as_tibble(dt)
}

# ========== CMJ / LCMJ / SJ / ABCMJ (TRUNCATE) ==========
if (any(new_test_types %in% c("CMJ","LCMJ","SJ","ABCMJ"))) {
  create_log_entry("Processing CMJ/LCMJ/SJ/ABCMJ")

  cmj_temp <- forcedecks_raw %>% filter(test_type %in% c("CMJ","LCMJ","SJ","ABCMJ"))
  cmj_new <- cmj_temp %>%
    dplyr::select(any_of(c(
      "test_ID","vald_id","full_name","position","team","test_type","date","time","body_weight_lbs",
      "countermovement_depth","jump_height_inches_imp_mom","bodymass_relative_takeoff_power",
      "mean_landing_power","mean_eccentric_force","mean_takeoff_acceleration","mean_ecc_con_ratio",
      "mean_takeoff_velocity","peak_landing_velocity","peak_takeoff_force","peak_takeoff_velocity",
      "concentric_rfd_100","start_to_peak_force_time","contraction_time","concentric_duration",
      "eccentric_concentric_duration_ratio","flight_eccentric_time_ratio","displacement_at_takeoff",
      "rsi_modified_imp_mom","positive_takeoff_impulse","positive_impulse","concentric_impulse",
      "eccentric_braking_impulse","total_work","relative_peak_landing_force",
      "relative_peak_concentric_force","relative_peak_eccentric_force","bm_rel_force_at_zero_velocity",
      "landing_impulse","force_at_zero_velocity","cmj_stiffness","braking_phase_duration",
      "takeoff_velocity","eccentric_time","peak_landing_acceleration","peak_takeoff_acceleration",
      "concentric_rfd_200","eccentric_peak_power"
    ))) %>%
    filter(!is.na(jump_height_inches_imp_mom)) %>%
    arrange(full_name, test_type, date)

  # Need full history to compute metrics — read existing only if present
  cmj_existing <- read_bq_table("vald_fd_jumps") %>%
    mutate(test_ID = as.character(test_ID))

  cmj_all <- bind_rows(cmj_existing, cmj_new) %>%
    dplyr::distinct(test_ID, .keep_all = TRUE) %>%
    arrange(full_name, test_type, date)

  # Windowed metrics with slider
  fd <- cmj_all %>% arrange(full_name, test_type, date) %>% group_by(full_name, test_type) %>%
    mutate(
      mean_30d_jh = slide_index_dbl(jump_height_inches_imp_mom, date, mean, .before = days(30), .complete = FALSE, .na_rm = TRUE),
      sd_30d_jh   = slide_index_dbl(jump_height_inches_imp_mom, date, sd,   .before = days(30), .complete = FALSE, .na_rm = TRUE),
      zscore_jump_height_inches_imp_mom = if_else(sd_30d_jh > 0, (jump_height_inches_imp_mom - mean_30d_jh)/sd_30d_jh, NA_real_),
      mean_30d_rpcf = slide_index_dbl(relative_peak_concentric_force, date, mean, .before = days(30), .complete = FALSE, .na_rm = TRUE),
      sd_30d_rpcf   = slide_index_dbl(relative_peak_concentric_force, date, sd,   .before = days(30), .complete = FALSE, .na_rm = TRUE),
      zscore_relative_peak_concentric_force = if_else(sd_30d_rpcf > 0, (relative_peak_concentric_force - mean_30d_rpcf)/sd_30d_rpcf, NA_real_),
      mean_30d_rsi = slide_index_dbl(rsi_modified_imp_mom, date, mean, .before = days(30), .complete = FALSE, .na_rm = TRUE),
      sd_30d_rsi   = slide_index_dbl(rsi_modified_imp_mom, date, sd,   .before = days(30), .complete = FALSE, .na_rm = TRUE),
      zscore_rsi_modified_imp_mom = if_else(sd_30d_rsi > 0, (rsi_modified_imp_mom - mean_30d_rsi)/sd_30d_rsi, NA_real_)
    ) %>% ungroup() %>%
    group_by(full_name) %>%
    mutate(
      # readiness vs CMJ baseline only
      cmj_mask = (test_type == "CMJ"),
      jh_cmj_mean_30d = slide_index_dbl(if_else(cmj_mask, jump_height_inches_imp_mom, NA_real_), date, ~mean(.x, na.rm=TRUE), .before = days(30)),
      rsi_cmj_mean_30d = slide_index_dbl(if_else(cmj_mask, rsi_modified_imp_mom, NA_real_), date, ~mean(.x, na.rm=TRUE), .before = days(30)),
      epf_cmj_mean_30d = slide_index_dbl(if_else(cmj_mask, relative_peak_eccentric_force, NA_real_), date, ~mean(.x, na.rm=TRUE), .before = days(30)),
      jump_height_readiness = if_else(!is.na(jump_height_inches_imp_mom) & !is.na(jh_cmj_mean_30d) & jh_cmj_mean_30d != 0,
                                      (jump_height_inches_imp_mom - jh_cmj_mean_30d)/jh_cmj_mean_30d, NA_real_),
      rsi_readiness = if_else(!is.na(rsi_modified_imp_mom) & !is.na(rsi_cmj_mean_30d) & rsi_cmj_mean_30d != 0,
                              (rsi_modified_imp_mom - rsi_cmj_mean_30d)/rsi_cmj_mean_30d, NA_real_),
      epf_readiness = if_else(!is.na(relative_peak_eccentric_force) & !is.na(epf_cmj_mean_30d) & epf_cmj_mean_30d != 0,
                              (relative_peak_eccentric_force - epf_cmj_mean_30d)/epf_cmj_mean_30d, NA_real_)
    ) %>%
    ungroup() %>%
    # outlier filters and cleanup (unchanged semantics)
    filter(between(jump_height_inches_imp_mom, 5, 28)) %>%
    select(-starts_with("mean_30d_"), -starts_with("sd_30d_"), -cmj_mask,
           -starts_with("jh_cmj_"), -starts_with("rsi_cmj_"), -starts_with("epf_cmj_"))

  # Performance scores (same as prior semantics)
  if (all(c("jump_height_inches_imp_mom", "relative_peak_eccentric_force",
            "bodymass_relative_takeoff_power", "rsi_modified_imp_mom") %in% names(fd))) {
    fd <- fd %>%
      mutate(calc_performance_score =
               percent_rank(jump_height_inches_imp_mom) * 100 +
               percent_rank(relative_peak_eccentric_force) * 100 +
               percent_rank(bodymass_relative_takeoff_power) * 100 +
               percent_rank(rsi_modified_imp_mom) * 100,
             performance_score = percent_rank(calc_performance_score) * 100) %>%
      group_by(team) %>%
      mutate(team_performance_score = percent_rank(calc_performance_score) * 100) %>%
      ungroup() %>%
      select(-calc_performance_score)
  }

  # TRUNCATE upload for CMJ
  bq_upsert(fd, "vald_fd_jumps", key = "test_ID", mode = "TRUNCATE",
            partition_field = "date", cluster_fields = c("team","test_type","vald_id"))
} else {
  create_log_entry("No new CMJ-family tests — skipping CMJ section")
}

# ========== DJ (MERGE) ==========
if ("DJ" %in% new_test_types) {
  create_log_entry("Processing DJ")
  dj_temp <- forcedecks_raw %>% filter(test_type == "DJ")
  dj_new <- dj_temp %>%
    dplyr::select(any_of(c(
      "test_ID","vald_id","full_name","position","team","date","time","body_weight_lbs",
      "peak_takeoff_velocity","peak_landing_velocity","countermovement_depth","peak_landing_force",
      "eccentric_time","jump_height_inches_imp_mom","bm_rel_force_at_zero_velocity","contact_time",
      "contact_velocity","active_stiffness","passive_stiffness","reactive_strength_index",
      "positive_takeoff_impulse","coefficient_of_restitution","active_stiffness_index",
      "passive_stiffness_index","peak_impact_force","peak_driveoff_force"
    ))) %>%
    filter(!is.na(jump_height_inches_imp_mom),
           jump_height_inches_imp_mom > 2, jump_height_inches_imp_mom < 30) %>%
    mutate(
      velocity_ratio = if_else(!is.na(peak_takeoff_velocity) & !is.na(peak_landing_velocity) & peak_landing_velocity != 0,
                               peak_takeoff_velocity/peak_landing_velocity, NA_real_),
      force_ratio    = if_else(!is.na(peak_impact_force) & !is.na(peak_driveoff_force) & peak_driveoff_force != 0,
                               peak_impact_force/peak_driveoff_force, NA_real_),
      stiffness_ratio = if_else(!is.na(active_stiffness) & !is.na(passive_stiffness) & passive_stiffness != 0,
                                active_stiffness/passive_stiffness, NA_real_)
    )
  bq_upsert(dj_new, "vald_fd_dj", key = "test_ID", mode = "MERGE",
            partition_field = "date", cluster_fields = c("team","vald_id"))
} else {
  create_log_entry("No new DJ tests — skipping DJ section")
}

# ========== RSI (MERGE) ==========
if (any(new_test_types %in% c("RSAIP","RSHIP","RSKIP"))) {
  create_log_entry("Processing RSI")
  rsi_temp <- trials_wider %>%
    mutate(test_ID = as.character(testid), vald_id = athleteid) %>%
    left_join(tests_processed %>% dplyr::select(test_ID, test_type), by = "test_ID") %>%
    filter(test_type %in% c("RSAIP","RSHIP","RSKIP"))
  rsi_new <- rsi_temp %>%
    dplyr::select(-testid, -athleteid) %>%
    left_join(mergable_roster, by = "vald_id") %>%
    mutate(date = as.Date(date), time = hms::as_hms(time)) %>%
    dplyr::select(any_of(c(
      "triallimb","test_ID","vald_id","full_name","position","team","date","time","body_weight_lbs",
      "start_to_peak_force","peak_vertical_force","rfd_at_100ms","rfd_at_250ms",
      "iso_bm_rel_force_peak","iso_bm_rel_force_100","iso_bm_rel_force_200","iso_abs_impulse_100"
    ))) %>%
    group_by(test_ID, vald_id, triallimb) %>%
    summarise(across(any_of(c("full_name","position","team")), first),
              across(any_of(c("date","time","body_weight_lbs")), first),
              across(where(is.numeric), ~mean(.x, na.rm = TRUE)), .groups = "drop") %>%
    tidyr::pivot_wider(
      id_cols = any_of(c("test_ID","vald_id","full_name","position","team","date","time","body_weight_lbs")),
      names_from = triallimb,
      values_from = -c(test_ID, vald_id, full_name, position, team, date, time, body_weight_lbs),
      names_sep = "_"
    ) %>%
    rename_with(~str_replace(.x, "_Left$", "_left")) %>%
    rename_with(~str_replace(.x, "_Right$", "_right"))
  bq_upsert(rsi_new, "vald_fd_rsi", key = "test_ID", mode = "MERGE",
            partition_field = "date", cluster_fields = c("team","vald_id"))
} else {
  create_log_entry("No new RSI tests — skipping RSI section")
}

# ========== Rebound (CMRJ / SLCMRJ) (MERGE) ==========
if (any(new_test_types %in% c("CMRJ","SLCMRJ"))) {
  create_log_entry("Processing Rebound")
  rebound_temp <- trials_wider %>%
    mutate(test_ID = as.character(testid), vald_id = athleteid) %>%
    left_join(tests_processed %>% dplyr::select(test_ID, test_type), by = "test_ID") %>%
    filter(test_type %in% c("CMRJ","SLCMRJ"))
  rebound_new <- rebound_temp %>%
    dplyr::select(-testid, -athleteid) %>%
    left_join(mergable_roster, by = "vald_id") %>%
    mutate(date = as.Date(date), time = hms::as_hms(time)) %>%
    dplyr::select(any_of(c(
      "triallimb","test_ID","test_type","vald_id","full_name","position","team","date","time","body_weight_lbs",
      "cmrj_takeoff_bm_rel_peak_force","cmrj_takeoff_countermovement_depth",
      "cmrj_takeoff_concentric_peak_force","cmrj_takeoff_contraction_time","cmrj_takeoff_ecc_decel_impulse",
      "cmrj_takeoff_ecc_duration","cmrj_takeoff_bm_rel_ecc_peak_force","cmrj_takeoff_jump_height_imp_mom_inches",
      "cmrj_takeoff_rsi_modified_imp_mom","cmrj_rebound_active_stiffness","cmrj_rebound_active_stiffness_index",
      "cmrj_rebound_contact_time","cmrj_rebound_countermovement_depth","cmrj_rebound_passive_stiffness",
      "cmrj_rebound_passive_stiffness_index","cmrj_rebound_jump_height_imp_mom_inches"
    ))) %>%
    group_by(test_ID, vald_id, test_type, triallimb) %>%
    summarise(across(any_of(c("full_name","position","team")), first),
              across(any_of(c("date","time","body_weight_lbs")), first),
              across(where(is.numeric), ~mean(.x, na.rm = TRUE)), .groups = "drop") %>%
    mutate(limb_suffix = case_when(
      test_type == "CMRJ" ~ "bilateral",
      test_type == "SLCMRJ" & triallimb == "Left" ~ "left",
      test_type == "SLCMRJ" & triallimb == "Right" ~ "right",
      TRUE ~ "bilateral"
    )) %>%
    tidyr::pivot_wider(
      id_cols = any_of(c("test_ID","vald_id","test_type","full_name","position","team","date","time","body_weight_lbs")),
      names_from = limb_suffix, values_from = -c(test_ID, vald_id, test_type, full_name, position, team, date, time, body_weight_lbs),
      names_sep = "_"
    )
  bq_upsert(rebound_new, "vald_fd_rebound", key = "test_ID", mode = "MERGE",
            partition_field = "date", cluster_fields = c("team","vald_id"))
} else {
  create_log_entry("No new Rebound tests — skipping Rebound section")
}

# ========== SLJ (MERGE) ==========
if ("SLJ" %in% new_test_types) {
  create_log_entry("Processing SLJ")
  slj_temp <- trials_wider %>%
    mutate(test_ID = as.character(testid), vald_id = athleteid) %>%
    left_join(tests_processed %>% dplyr::select(test_ID, test_type), by = "test_ID") %>%
    filter(test_type == "SLJ")
  slj_new <- slj_temp %>%
    dplyr::select(-testid, -athleteid) %>%
    left_join(mergable_roster, by = "vald_id") %>%
    mutate(date = as.Date(date), time = hms::as_hms(time)) %>%
    dplyr::select(any_of(c(
      "triallimb","test_ID","vald_id","full_name","position","team","date","time","body_weight_lbs",
      "peak_landing_force","peak_landing_velocity","peak_takeoff_velocity","time_to_peak_force",
      "weight_relative_peak_takeoff_force","weight_relative_peak_landing_force",
      "relative_peak_concentric_force","relative_peak_eccentric_force","lower_limb_stiffness",
      "rsi_modified_imp_mom"
    ))) %>%
    group_by(test_ID, vald_id, triallimb) %>%
    summarise(across(any_of(c("full_name","position","team")), first),
              across(any_of(c("date","time","body_weight_lbs")), first),
              across(where(is.numeric), ~mean(.x, na.rm = TRUE)), .groups = "drop") %>%
    tidyr::pivot_wider(
      id_cols = any_of(c("test_ID","vald_id","full_name","position","team","date","time","body_weight_lbs")),
      names_from = triallimb, values_from = -c(test_ID, vald_id, full_name, position, team, date, time, body_weight_lbs),
      names_sep = "_"
    ) %>%
    rename_with(~str_replace(.x, "_Left$", "_left")) %>%
    rename_with(~str_replace(.x, "_Right$", "_right"))
  bq_upsert(slj_new, "vald_fd_sl_jumps", key = "test_ID", mode = "MERGE",
            partition_field = "date", cluster_fields = c("team","vald_id"))
} else {
  create_log_entry("No new SLJ tests — skipping SLJ section")
}

# ========== IMTP (MERGE) ==========
if ("IMTP" %in% new_test_types) {
  create_log_entry("Processing IMTP")
  imtp_temp <- forcedecks_raw %>% filter(test_type == "IMTP")
  imtp_new <- imtp_temp %>%
    dplyr::select(any_of(c(
      "test_ID","vald_id","full_name","position","team","date","time",
      "start_to_peak_force","rfd_at_100ms","rfd_at_200ms","force_at_100ms",
      "iso_bm_rel_force_peak","peak_vertical_force","force_at_200ms"
    ))) %>%
    filter(!is.na(peak_vertical_force)) %>%
    arrange(full_name, date)

  # Performance scores (same semantics)
  if (all(c("peak_vertical_force","start_to_peak_force","rfd_at_100ms") %in% names(imtp_new))) {
    imtp_new <- imtp_new %>%
      mutate(calc_performance_score = percent_rank(peak_vertical_force) * 200 +
               (100 - percent_rank(start_to_peak_force) * 100) +
               percent_rank(rfd_at_100ms) * 100,
             performance_score = percent_rank(calc_performance_score) * 100) %>%
      group_by(team) %>%
      mutate(team_performance_score = percent_rank(calc_performance_score) * 100) %>%
      ungroup() %>%
      select(-calc_performance_score)
  }

  bq_upsert(imtp_new, "vald_fd_imtp", key = "test_ID", mode = "MERGE",
            partition_field = "date", cluster_fields = c("team","vald_id"))
} else {
  create_log_entry("No new IMTP tests — skipping IMTP section")
}

# ========== Nordbord (MERGE, conditional pull by presence) ==========
create_log_entry("Fetching Nordbord data from VALD API...")
injest_nord <- get_nordbord_data()
nord_tests <- injest_nord$tests
if (nrow(nord_tests) > 0) {
  create_log_entry(glue("Processing Nordbord ({nrow(nord_tests)} tests)"))
  nb <- as_tibble(nord_tests) %>%
    dplyr::select(-any_of(c("device","notes","testTypeId"))) %>%
    mutate(
      modifiedDateUtc_chr = as.character(modifiedDateUtc),
      modifiedDateUtc_parsed = coalesce(
        ymd_hms(modifiedDateUtc_chr, tz="UTC", quiet=TRUE),
        ymd_hm(modifiedDateUtc_chr,  tz="UTC", quiet=TRUE),
        ymd_h(modifiedDateUtc_chr,   tz="UTC", quiet=TRUE),
        ymd(modifiedDateUtc_chr,     tz="UTC")
      ),
      modifiedDateUtc_local = with_tz(modifiedDateUtc_parsed, "America/Los_Angeles"),
      date = as.Date(modifiedDateUtc_local),
      time = hms::as_hms(modifiedDateUtc_local),
      vald_id = athleteId,
      test_ID = as.character(testId),
      test_type = testTypeName,
      impulse_left  = if_else(leftRepetitions==0, NA_real_, if_else(leftCalibration==0, leftImpulse, NA_real_)),
      avg_force_left = if_else(leftRepetitions==0, NA_real_, leftAvgForce - leftCalibration),
      max_force_left = if_else(leftRepetitions==0, NA_real_, leftMaxForce - leftCalibration),
      impulse_right  = if_else(rightRepetitions==0, NA_real_, if_else(rightCalibration==0, rightImpulse, NA_real_)),
      avg_force_right = if_else(rightRepetitions==0, NA_real_, rightAvgForce - rightCalibration),
      max_force_right = if_else(rightRepetitions==0, NA_real_, rightMaxForce - rightCalibration),
      reps_left  = leftRepetitions,
      reps_right = rightRepetitions,
      avg_force_bilateral = rowMeans(cbind(avg_force_left, avg_force_right), na.rm = TRUE),
      max_force_bilateral = rowMeans(cbind(max_force_left, max_force_right), na.rm = TRUE),
      avg_force_asymmetry = case_when(
        is.na(avg_force_left) | is.na(avg_force_right) ~ NA_real_,
        avg_force_left == 0 & avg_force_right == 0 ~ NA_real_,
        avg_force_left >= avg_force_right ~ ((avg_force_left - avg_force_right) / avg_force_left),
        TRUE ~ ((avg_force_right - avg_force_left) / avg_force_right)
      ),
      max_force_asymmetry = case_when(
        is.na(max_force_left) | is.na(max_force_right) ~ NA_real_,
        max_force_left == 0 & max_force_right == 0 ~ NA_real_,
        max_force_left >= max_force_right ~ ((max_force_left - max_force_right) / max_force_left),
        TRUE ~ ((max_force_right - max_force_left) / max_force_right)
      ),
      impulse_asymmetry = case_when(
        is.na(impulse_left) | is.na(impulse_right) ~ NA_real_,
        impulse_left == 0 & impulse_right == 0 ~ NA_real_,
        impulse_left >= impulse_right ~ ((impulse_left - impulse_right) / impulse_left),
        TRUE ~ ((impulse_right - impulse_left) / impulse_right)
      )
    ) %>%
    dplyr::select(-any_of(c("modifiedDateUtc_chr","modifiedDateUtc_parsed","modifiedDateUtc_local",
                     "modifiedDateUtc","testDateUtc","athleteId","testId",
                     "leftTorque","rightTorque","leftMaxForce","rightMaxForce",
                     "leftRepetitions","rightRepetitions","testTypeName"))) %>%
    left_join(mergable_roster, by="vald_id") %>%
    attach_bw()

  # relative to BW
  if ("body_weight_lbs" %in% names(nb)) {
    nb <- nb %>%
      mutate(
        max_force_relative_bw = if_else(!is.na(body_weight_lbs) & body_weight_lbs != 0,
                                        max_force_bilateral / body_weight_lbs, NA_real_),
        max_force_left_relative_bw  = if_else(!is.na(body_weight_lbs) & body_weight_lbs != 0,
                                              max_force_left / body_weight_lbs, NA_real_),
        max_force_right_relative_bw = if_else(!is.na(body_weight_lbs) & body_weight_lbs != 0,
                                              max_force_right / body_weight_lbs, NA_real_)
      ) %>%
      dplyr::select(-any_of("body_weight_lbs"))
  }

  bq_upsert(nb, "vald_nord_all", key = "test_ID", mode = "MERGE",
            partition_field = "date", cluster_fields = c("team","vald_id"))
} else {
  create_log_entry("No Nordbord tests — skipping Nordbord section")
}

# ========== Dates & Tests (MERGE) ==========
dates_delta <- forcedecks_raw %>% dplyr::select(date) %>% dplyr::distinct()
tests_delta <- forcedecks_raw %>% dplyr::select(test_ID) %>% dplyr::distinct()
bq_upsert(dates_delta, "dates", key = "date", mode = "MERGE", partition_field = "date", cluster_fields = character())
bq_upsert(tests_delta, "tests", key = "test_ID", mode = "MERGE", partition_field = NULL, cluster_fields = character())

# ----------------------------
# RSI type fix (non-destructive)
# ----------------------------
fix_rsi_data_type()

# ----------------------------
# Wrap up
# ----------------------------
create_log_entry("=== SCRIPT EXECUTION SUMMARY ===")
create_log_entry(sprintf("Total execution time: %s minutes", round(difftime(Sys.time(), script_start_time, units="mins"),2)))
create_log_entry("=== VALD DATA PROCESSING SCRIPT ENDED ===", "END")

upload_logs_to_bigquery()
try(DBI::dbDisconnect(con), silent = TRUE)
cat("Script completed successfully\n")
