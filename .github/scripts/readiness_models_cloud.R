#!/usr/bin/env Rscript
# ===== Packages =====
tryCatch({
  suppressPackageStartupMessages({
    library(bigrquery); library(DBI)
    library(dplyr); library(tidyr); library(data.table)
    library(lubridate); library(slider); library(stringr)
    library(gargle); library(glue); library(digest)
    library(googleCloudStorageR)
    library(glmnet); library(bnlearn); library(reticulate)
    library(jsonlite)
  })
}, error = function(e) { 
  cat("Error loading packages:", e$message, "\n"); quit(status=1)
})

# Disable BigQuery Storage API
options(bigrquery.use_bqstorage = FALSE)
Sys.setenv(BIGRQUERY_USE_BQ_STORAGE = "false")

################################################################################
# FIX 4.2: CONFIGURATION CONSTANTS (extracted magic numbers)
################################################################################
project    <- Sys.getenv("GCP_PROJECT", "sac-vald-hub")
dataset    <- Sys.getenv("BQ_DATASET",  "analytics")
location   <- Sys.getenv("BQ_LOCATION", "US")
gcs_bucket <- Sys.getenv("GCS_BUCKET",  "")
team_name  <- Sys.getenv("TEAM_NAME",   "sacstate-football")

# Table names
workload_table    <- Sys.getenv("WORKLOAD_TABLE",    "workload_daily")
readiness_table   <- Sys.getenv("READINESS_TABLE",   "vald_fd_jumps")
roster_table      <- Sys.getenv("ROSTER_TABLE",      "roster_mapping")
predictions_table <- Sys.getenv("PREDICTIONS_TABLE", "readiness_predictions_byname")

# Model training config
cfg_start_date <- as.Date(Sys.getenv("START_DATE", "2025-06-01"))
cfg_end_date   <- as.Date(Sys.getenv("END_DATE",   "2025-12-31"))
cfg_frac_train <- as.numeric(Sys.getenv("TRAIN_FRACTION", "0.80"))

# Training thresholds (FIX 4.2)
MIN_TRAIN_OBS         <- as.integer(Sys.getenv("MIN_TRAIN_OBS", "3"))
MIN_TEST_OBS_FOR_EVAL <- as.integer(Sys.getenv("MIN_TEST_OBS", "5"))
MIN_TRAIN_RATIO       <- as.numeric(Sys.getenv("MIN_TRAIN_RATIO", "0.7"))
MIN_PREDICTOR_VARIANCE <- as.numeric(Sys.getenv("MIN_PRED_VARIANCE", "0.0"))

# Cross-validation config (FIX 4.2)
MAX_CV_FOLDS <- as.integer(Sys.getenv("MAX_CV_FOLDS", "5"))
MIN_CV_FOLDS <- as.integer(Sys.getenv("MIN_CV_FOLDS", "2"))

# Model selection (FIX 4.2)
LAMBDA_GRID_SIZE_FULL <- as.integer(Sys.getenv("LAMBDA_GRID_SIZE", "100"))
LAMBDA_GRID_SIZE_EN   <- as.integer(Sys.getenv("LAMBDA_GRID_SIZE_EN", "50"))

# Retry config (FIX 4.2)
MAX_RETRY_ATTEMPTS <- as.integer(Sys.getenv("MAX_RETRY_ATTEMPTS", "3"))
RETRY_WAIT_SECONDS <- as.integer(Sys.getenv("RETRY_WAIT_SECONDS", "5"))

# Workflow fork controls
skip_training       <- tolower(Sys.getenv("SKIP_TRAINING", "false")) %in% c("1","true","yes")
has_matches_hint    <- Sys.getenv("HAS_MATCHES", "")
match_lookback_days <- as.integer(Sys.getenv("MATCH_LOOKBACK_DAYS", "7"))

cat("=== CONFIGURATION ===\n")
cat("GCP Project:", project, "\n")
cat("BQ Dataset:", dataset, "\n")
cat("GCS Bucket:", ifelse(nzchar(gcs_bucket), gcs_bucket, "(none)"), "\n")
cat("Team:", team_name, "\n")
cat("Workload table:", workload_table, "\n")
cat("Readiness table:", readiness_table, "\n")
cat("Roster table:", roster_table, "\n")
cat("Predictions table:", predictions_table, "\n")
cat("Date range:", format(cfg_start_date, "%m/%d/%Y"), "to", format(cfg_end_date, "%m/%d/%Y"), "\n")
cat("Train fraction:", cfg_frac_train, "\n")
cat("Min train obs:", MIN_TRAIN_OBS, "\n")
cat("Skip training:", skip_training, "\n")
cat("HAS_MATCHES hint:", ifelse(nzchar(has_matches_hint), has_matches_hint, "not provided"), "\n")
cat("Match lookback days:", match_lookback_days, "\n\n")

################################################################################
# LOGGING
################################################################################
log_entries <- tibble(
  timestamp = as.POSIXct(character(0)),
  level = character(0),
  message = character(0),
  run_id = character(0),
  repository = character(0),
  reason = character(0)
)

create_log_entry <- function(message, level="INFO", reason="") {
  ts <- Sys.time()
  cat(sprintf("[%s] [%s] %s%s\n",
              format(ts, "%m/%d/%Y %H:%M:%S", tz="America/Los_Angeles"),
              level, message,
              ifelse(nzchar(reason), paste0(" (reason=", reason, ")"), "")))
  log_entries <<- bind_rows(log_entries, tibble(
    timestamp = ts, level = level, message = message,
    run_id = Sys.getenv("GITHUB_RUN_ID", "manual"),
    repository = Sys.getenv("GITHUB_REPOSITORY", "unknown"),
    reason = reason
  ))
}

upload_logs_to_bigquery <- function() {
  if (nrow(log_entries)==0) return(invisible(TRUE))
  tryCatch({
    ds <- bq_dataset(project, dataset)
    log_tbl <- bq_table(ds, "model_training_log")
    if (!bq_table_exists(log_tbl)) {
      bq_table_create(log_tbl, fields = as_bq_fields(log_entries))
    }
    bq_table_upload(log_tbl, log_entries, write_disposition = "WRITE_APPEND")
    TRUE
  }, error=function(e){ cat("Log upload failed:", e$message, "\n"); FALSE })
}

script_start <- Sys.time()
create_log_entry("=== READINESS MODEL TRAINING START ===", "START")

################################################################################
# AUTHENTICATION
################################################################################
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
  if (nzchar(gcs_bucket)) {
    gcs_auth(token = gargle::gargle2.0_token(
      scope = 'https://www.googleapis.com/auth/devstorage.full_control',
      client = gargle::gargle_client(),
      credentials = list(access_token = GLOBAL_ACCESS_TOKEN)
    ))
    gcs_global_bucket(gcs_bucket)
  }
  create_log_entry("Authentication successful")
}, error = function(e) {
  create_log_entry(paste("Authentication failed:", e$message), "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
})

################################################################################
# HELPER FUNCTIONS
################################################################################

# FIX 2.6: Consistent normalization function
normalize_athlete_id <- function(x) {
  # Consistent with SQL: TRIM(CAST(x AS STRING))
  # No lowercase or special char removal to match SQL exactly
  stringr::str_trim(as.character(x))
}

# FIX 2.7: Date parsing with MM/DD/YYYY preference
parse_date_mmddyyyy <- function(x) {
  if (inherits(x, "Date")) return(x)
  out <- rep(as.Date(NA), length(x))
  
  # Try MM/DD/YYYY first (American format)
  suppressWarnings({
    parsed <- lubridate::mdy(as.character(x))
    good <- !is.na(parsed)
    if (any(good)) out[good] <- parsed[good]
  })
  
  # Then try YYYY-MM-DD (ISO)
  bad <- is.na(out)
  if (any(bad)) {
    suppressWarnings({
      parsed <- lubridate::ymd(as.character(x[bad]))
      good <- !is.na(parsed)
      if (any(good)) out[bad][good] <- parsed[good]
    })
  }
  
  # Finally try DD/MM/YYYY
  bad <- is.na(out)
  if (any(bad)) {
    suppressWarnings({
      parsed <- lubridate::dmy(as.character(x[bad]))
      good <- !is.na(parsed)
      if (any(good)) out[bad][good] <- parsed[good]
    })
  }
  
  # Try Excel numeric dates
  bad <- is.na(out)
  if (any(bad)) {
    suppressWarnings({
      as_num <- as.numeric(x[bad])
      good <- !is.na(as_num) & as_num > 30000 & as_num < 50000
      if (any(good)) {
        out[bad][good] <- as.Date(as_num[good], origin = "1899-12-30")
      }
    })
  }
  
  out
}

# FIX 2.7: Date validation function
validate_dates <- function(df, date_col, context) {
  if (!date_col %in% names(df)) {
    create_log_entry(sprintf("ERROR: Date column '%s' not found in %s", date_col, context), "ERROR")
    upload_logs_to_bigquery(); quit(status=1)
  }
  
  if (!inherits(df[[date_col]], "Date")) {
    create_log_entry(sprintf("WARN: %s date column is %s, attempting conversion", 
                             context, class(df[[date_col]])[1]), "WARN")
    df[[date_col]] <- parse_date_mmddyyyy(df[[date_col]])
  }
  
  # Check for invalid dates
  invalid_count <- sum(is.na(df[[date_col]]))
  if (invalid_count > 0) {
    create_log_entry(sprintf("ERROR: %d invalid dates in %s", invalid_count, context), "ERROR")
    upload_logs_to_bigquery(); quit(status=1)
  }
  
  df
}

# Retry helper
retry_operation <- function(expr, max_attempts = MAX_RETRY_ATTEMPTS, 
                           wait_seconds = RETRY_WAIT_SECONDS, 
                           operation_name = "operation") {
  for (attempt in 1:max_attempts) {
    result <- tryCatch({
      force(expr)
      return(list(success = TRUE, result = expr))
    }, error = function(e) {
      if (attempt < max_attempts) {
        create_log_entry(
          sprintf("%s failed (attempt %d/%d): %s. Retrying in %d seconds...", 
                  operation_name, attempt, max_attempts, e$message, wait_seconds),
          "WARN",
          reason = "retry_transient_error"
        )
        Sys.sleep(wait_seconds)
        NULL
      } else {
        create_log_entry(
          sprintf("%s failed after %d attempts: %s", 
                  operation_name, max_attempts, e$message),
          "ERROR",
          reason = "retry_exhausted"
        )
        return(list(success = FALSE, error = e$message))
      }
    })
    if (!is.null(result)) return(result)
  }
}

# Existing helpers
rmse <- function(obs, pred) sqrt(mean((obs - pred)^2, na.rm=TRUE))
to_numeric_df <- function(df, cols) { for (c in cols) df[[c]] <- as.numeric(df[[c]]); df }
enough_rows <- function(n, min_n = 2) { !is.na(n) && is.finite(n) && n >= min_n }
choose_nfolds <- function(n) { 
  if (is.na(n) || n < MIN_CV_FOLDS) MIN_CV_FOLDS 
  else pmin(MAX_CV_FOLDS, n) 
}

impute_train_test <- function(train_df, test_df, preds) {
  tr <- train_df; te <- test_df
  meds <- vapply(preds, function(p) stats::median(tr[[p]], na.rm = TRUE), numeric(1))
  meds[!is.finite(meds)] <- 0
  for (p in preds) {
    tr[[p]][is.na(tr[[p]])] <- meds[[p]]
    if (nrow(te) > 0) te[[p]][is.na(te[[p]])] <- meds[[p]]
  }
  list(train = tr, test = te, medians = meds, cols = preds)
}

safe_scale_fit <- function(df, cols) {
  stopifnot(all(cols %in% names(df)))
  X <- as.matrix(df[, cols, drop = FALSE])
  Xs <- scale(X)
  list(X = Xs,
       center = as.numeric(attr(Xs, "scaled:center")),
       scale  = as.numeric(attr(Xs, "scaled:scale")),
       cols   = cols)
}

safe_scale_apply <- function(df, fit) {
  stopifnot(all(fit$cols %in% names(df)))
  X <- as.matrix(df[, fit$cols, drop = FALSE])
  s <- fit$scale; s[is.na(s) | s == 0] <- 1
  scale(X, center = fit$center, scale = s)
}

safe_runner <- function(name, fun, ...) {
  tryCatch(
    fun(...),
    error = function(e) {
      list(model_type = name,
           train_rmse = Inf, cv_rmse = Inf, test_rmse = Inf, primary_rmse = Inf,
           hyper = paste0("ERROR: ", conditionMessage(e)),
           fitted = list(type = "error", msg = conditionMessage(e)))
    }
  )
}

parse_date_robust <- function(vec) {
  # If this gets a function by accident, fail fast with a clear message
  if (is.function(vec)) stop("parse_date_robust() received a function instead of a vector. Pass the column explicitly, e.g. .data[['date']].")

  # already date-like?
  if (inherits(vec, "Date"))    return(vec)
  if (inherits(vec, "POSIXt"))  return(as.Date(vec))

  # numeric: often Excel serial days (Windows origin)
  if (is.numeric(vec)) {
    # Typical Excel origin for OneDrive/Windows: 1899-12-30
    out <- as.Date(vec, origin = "1899-12-30")
    return(out)
  }

  # character: try multiple formats
  if (is.character(vec)) {
    v <- trimws(vec)
    v[nchar(v) == 0] <- NA_character_

    suppressWarnings({
      # try ISO first
      d <- as.Date(v, format = "%Y-%m-%d")
      # fallback: m/d/Y or m-d-Y
      d[is.na(d)] <- as.Date(v[is.na(d)], format = "%m/%d/%Y")
      d[is.na(d)] <- as.Date(v[is.na(d)], format = "%m-%d-%Y")
      # fallback: d/m/Y or d-m-Y
      d[is.na(d)] <- as.Date(v[is.na(d)], format = "%d/%m/%Y")
      d[is.na(d)] <- as.Date(v[is.na(d)], format = "%d-%m-%Y")
    })
    return(d)
  }

  # anything else -> NA
  rep(as.Date(NA), length(vec))
}
                 
################################################################################
# FIX 3.3: CONFIGURATION AUDIT TRAIL
################################################################################
create_log_entry("Saving configuration snapshot")

config_snapshot <- tibble(
  run_id = Sys.getenv("GITHUB_RUN_ID", "manual"),
  run_timestamp = Sys.time(),
  git_sha = Sys.getenv("GITHUB_SHA", "unknown"),
  git_ref = Sys.getenv("GITHUB_REF", "unknown"),
  
  # Environment config
  gcp_project = project,
  bq_dataset = dataset,
  gcs_bucket = ifelse(nzchar(gcs_bucket), gcs_bucket, "(none)"),
  team_name = team_name,
  
  # Table names
  workload_table = workload_table,
  readiness_table = readiness_table,
  roster_table = roster_table,
  predictions_table = predictions_table,
  
  # Training config
  start_date = cfg_start_date,
  end_date = cfg_end_date,
  train_fraction = cfg_frac_train,
  min_train_obs = MIN_TRAIN_OBS,
  match_lookback_days = match_lookback_days,
  
  # Flags
  skip_training = skip_training,
  has_matches_hint = ifelse(nzchar(has_matches_hint), has_matches_hint, "(not provided)")
)

tryCatch({
  config_tbl <- bq_table(project, dataset, "pipeline_config_history")
  if (!bq_table_exists(config_tbl)) {
    bq_table_create(config_tbl, 
                    fields = as_bq_fields(config_snapshot),
                    time_partitioning = list(type="DAY", field="run_timestamp"))
  }
  bq_table_upload(config_tbl, config_snapshot, write_disposition = "WRITE_APPEND")
  create_log_entry("Configuration snapshot saved")
}, error = function(e) {
  create_log_entry(paste("Config snapshot save failed (non-fatal):", e$message), "WARN")
})

################################################################################
# WORKFLOW FORK — honor skip flag + readiness match gate
################################################################################

# 1) Manual skip from workflow_dispatch
if (skip_training) {
  create_log_entry("Training skipped by request (workflow input skip_training=true)", "INFO", reason="manual_skip")
  upload_logs_to_bigquery()
  cat("⏭️  TRAINING SKIPPED (manual)\n"); quit(status=0)
}

# 2) If Job 2 passed a definitive answer, honor it
if (nzchar(has_matches_hint)) {
  if (tolower(has_matches_hint) %in% c("false","0","no")) {
    create_log_entry("No matching readiness data (from Job 2 output)", "INFO", reason="no_readiness_match")
    upload_logs_to_bigquery()
    cat("⏭️  TRAINING SKIPPED (no matches)\n"); quit(status=0)
  }
}

# 3) Self-check for matches if not provided (FIX 4.1: using consolidated SQL)
if (!nzchar(has_matches_hint)) {
  create_log_entry("No HAS_MATCHES hint; performing readiness match check")
  
  # Build SQL from template
  match_sql <- glue("
    WITH workload AS (
      SELECT DISTINCT
        TRIM(CAST(roster_name AS STRING)) AS official_id,
        DATE(date) AS date
      FROM `{project}.{dataset}.{workload_table}`
      WHERE date BETWEEN '{cfg_start_date}' AND '{cfg_end_date}'
        AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL {match_lookback_days} DAY)
    ),
    readiness_raw AS (
      SELECT
        DATE(date) AS date,
        LOWER(TRIM(full_name)) AS vald_full_name_norm,
        SAFE_DIVIDE(
          IFNULL(CAST(jump_height_readiness AS FLOAT64), 0) +
          IFNULL(CAST(epf_readiness          AS FLOAT64), 0) +
          IFNULL(CAST(rsi_readiness          AS FLOAT64), 0),
          NULLIF(
            IF(jump_height_readiness IS NULL, 0, 1) +
            IF(epf_readiness          IS NULL, 0, 1) +
            IF(rsi_readiness          IS NULL, 0, 1),
            0
          )
        ) AS readiness
      FROM `{project}.{dataset}.{readiness_table}`
      WHERE date BETWEEN '{cfg_start_date}' AND '{cfg_end_date}'
        AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL {match_lookback_days} DAY)
    ),
    roster_norm AS (
      SELECT
        TRIM(CAST(official_id AS STRING)) AS official_id,
        LOWER(TRIM(vald_name))         AS vald_full_name_norm
      FROM `{project}.{dataset}.{roster_table}`
    ),
    readiness_by_id AS (
      SELECT
        rmap.official_id,
        rr.date
      FROM readiness_raw rr
      JOIN roster_norm rmap
        ON rmap.vald_full_name_norm = rr.vald_full_name_norm
    )
    SELECT 
      COUNT(DISTINCT w.official_id) AS athletes_with_matches,
      COUNT(*) AS total_matches
    FROM workload w
    JOIN readiness_by_id rd
      ON rd.official_id = w.official_id
     AND rd.date       = w.date
  ")
  
  mres <- tryCatch(
    bq_table_download(bq_project_query(project, match_sql)),
    error = function(e) { 
      create_log_entry(paste("Readiness match query failed:", e$message), "WARN", reason="query_failed")
      NULL 
    }
  )
  
  if (is.null(mres) || nrow(mres)==0 || is.na(mres$total_matches[1]) || mres$total_matches[1] == 0) {
    create_log_entry("No matching readiness data found in lookback window", "INFO", reason="no_readiness_match")
    upload_logs_to_bigquery()
    cat("⏭️  TRAINING SKIPPED (no matches)\n"); quit(status=0)
  } else {
    create_log_entry(glue("Matches found: {mres$total_matches[1]} across {mres$athletes_with_matches[1]} athletes"))
  }
}

################################################################################
# FIX 1.3: INIT REGISTRY TABLES (with primary keys and partitioning)
################################################################################
create_log_entry("Initializing model registry tables")
tryCatch({
  ds <- bq_dataset(project, dataset)
  if (!bq_dataset_exists(ds)) bq_dataset_create(ds, location = location)
  
  # Registry Models
  bq_project_query(project, glue("
    CREATE TABLE IF NOT EXISTS `{project}.{dataset}.registry_models` (
      model_id STRING NOT NULL,
      model_name STRING NOT NULL,
      team STRING,
      owner STRING,
      description STRING,
      created_at TIMESTAMP NOT NULL,
      PRIMARY KEY (model_id) NOT ENFORCED
    )
    PARTITION BY DATE(created_at)
    CLUSTER BY team, model_id
  "))
  create_log_entry("Registry table verified: registry_models")
  
  # Registry Versions
  bq_project_query(project, glue("
    CREATE TABLE IF NOT EXISTS `{project}.{dataset}.registry_versions` (
      model_id STRING NOT NULL,
      version_id STRING NOT NULL,
      created_at TIMESTAMP NOT NULL,
      created_by STRING,
      artifact_uri STRING,
      artifact_sha256 STRING,
      framework STRING,
      r_version STRING,
      package_info STRING,
      training_data_start_date DATE,
      training_data_end_date DATE,
      n_training_samples INT64,
      n_test_samples INT64,
      validation_method STRING,
      predictor_list STRING,
      hyperparameters STRING,
      train_rmse FLOAT64,
      cv_rmse FLOAT64,
      test_rmse FLOAT64,
      pipeline_run_id STRING,
      git_commit_sha STRING,
      notes STRING,
      PRIMARY KEY (model_id, version_id) NOT ENFORCED
    )
    PARTITION BY DATE(created_at)
    CLUSTER BY model_id, version_id
  "))
  create_log_entry("Registry table verified: registry_versions")
  
  # Registry Metrics
  bq_project_query(project, glue("
    CREATE TABLE IF NOT EXISTS `{project}.{dataset}.registry_metrics` (
      model_id STRING NOT NULL,
      version_id STRING NOT NULL,
      metric_name STRING NOT NULL,
      metric_value FLOAT64,
      split STRING NOT NULL,
      logged_at TIMESTAMP NOT NULL,
      PRIMARY KEY (model_id, version_id, metric_name, split) NOT ENFORCED
    )
    PARTITION BY DATE(logged_at)
    CLUSTER BY model_id, version_id, metric_name
  "))
  create_log_entry("Registry table verified: registry_metrics")
  
  # Registry Stages
  bq_project_query(project, glue("
    CREATE TABLE IF NOT EXISTS `{project}.{dataset}.registry_stages` (
      model_id STRING NOT NULL,
      version_id STRING NOT NULL,
      stage STRING NOT NULL,
      set_at TIMESTAMP NOT NULL,
      set_by STRING,
      reason STRING,
      PRIMARY KEY (model_id, version_id, stage) NOT ENFORCED
    )
    PARTITION BY DATE(set_at)
    CLUSTER BY model_id, version_id, stage
  "))
  create_log_entry("Registry table verified: registry_stages")
  
}, error = function(e) {
  create_log_entry(paste("Registry initialization failed:", e$message), "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
})

################################################################################
# LOAD DATA (MATCHING LOCAL IMPLEMENTATION)
################################################################################

# Load workload_daily from BigQuery
create_log_entry("Loading workload data from BigQuery")
workload_daily_result <- retry_operation(
  {
    sql <- glue("
      SELECT 
        roster_name,
        DATE(date) AS date,
        distance, high_speed_distance, mechanical_load,
        distance_7d, distance_28d, distance_monotony_7d,
        hsd_7d, hsd_28d, ml_7d, ml_28d, ml_monotony_7d
      FROM `{project}.{dataset}.{workload_table}`
      WHERE date BETWEEN '{cfg_start_date}' AND '{cfg_end_date}'
    ")
    bq_table_download(bq_project_query(project, sql))
  },
  max_attempts = MAX_RETRY_ATTEMPTS,
  wait_seconds = RETRY_WAIT_SECONDS,
  operation_name = "workload data load"
)

if (!workload_daily_result$success) {
  create_log_entry("Workload data load failed after retries", "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
}
workload_daily <- workload_daily_result$result

# FIX 2.7: Validate and parse dates
workload_daily <- validate_dates(workload_daily, "date", "workload")
create_log_entry(glue("Workload data loaded: {nrow(workload_daily)} rows"))

# Load vald_fd_jumps from BigQuery
create_log_entry("Loading VALD FD jumps data from BigQuery")
vald_fd_jumps_result <- retry_operation(
  {
    sql <- glue("
      SELECT
        full_name,
        DATE(date) AS date,
        jump_height_readiness,
        epf_readiness,
        rsi_readiness
      FROM `{project}.{dataset}.{readiness_table}`
      WHERE date BETWEEN '{cfg_start_date}' AND '{cfg_end_date}'
    ")
    bq_table_download(bq_project_query(project, sql))
  },
  max_attempts = MAX_RETRY_ATTEMPTS,
  wait_seconds = RETRY_WAIT_SECONDS,
  operation_name = "VALD FD jumps data load"
)

if (!vald_fd_jumps_result$success) {
  create_log_entry("VALD FD jumps data load failed after retries", "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
}
vald_fd_jumps <- vald_fd_jumps_result$result

# FIX 2.7: Validate and parse dates
vald_fd_jumps <- validate_dates(vald_fd_jumps, "date", "VALD FD jumps")
create_log_entry(glue("VALD FD jumps data loaded: {nrow(vald_fd_jumps)} rows"))

# Load roster_mapping from BigQuery
create_log_entry("Loading roster mapping from BigQuery")
roster_mapping_result <- retry_operation(
  {
    sql <- glue("
      SELECT
        official_id,
        vald_name
      FROM `{project}.{dataset}.{roster_table}`
    ")
    bq_table_download(bq_project_query(project, sql))
  },
  max_attempts = MAX_RETRY_ATTEMPTS,
  wait_seconds = RETRY_WAIT_SECONDS,
  operation_name = "roster mapping load"
)

if (!roster_mapping_result$success) {
  create_log_entry("Roster mapping load failed after retries", "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
}
roster_mapping <- roster_mapping_result$result
create_log_entry(glue("Roster mapping loaded: {nrow(roster_mapping)} rows"))

################################################################################
# CONNECT DATA (AS PER USER'S LOCAL IMPLEMENTATION)
################################################################################

# Step 1: workload_with_off_id = workload_daily %>% mutate(official_id = roster_name) %>% select(-roster_name)
create_log_entry("Creating workload_with_off_id")
workload_with_off_id <- workload_daily %>% 
  mutate(official_id = roster_name) %>%
  select(-roster_name)

# Validate required columns
required_workload_cols <- c("official_id", "date", "distance", "high_speed_distance", "mechanical_load")
missing_cols <- setdiff(required_workload_cols, names(workload_with_off_id))
if (length(missing_cols) > 0) {
  create_log_entry(paste("ERROR: Missing columns in workload data:", paste(missing_cols, collapse=", ")), "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
}

# Check for null athlete IDs
null_id_count <- sum(is.na(workload_with_off_id$official_id) | workload_with_off_id$official_id == "")
if (null_id_count > 0) {
  create_log_entry(sprintf("WARN: %d workload rows with missing/empty official_id (will be filtered)", 
                           null_id_count), "WARN")
  workload_with_off_id <- workload_with_off_id %>% filter(!is.na(official_id) & nzchar(official_id))
}

# Step 2: vald_fd_jumps_with_off_id = vald_fd_jumps %>% 
#         mutate(vald_name = full_name) %>% select(-full_name) %>% 
#         left_join(roster_mapping, by = "vald_name") %>% filter(!is.na(official_id))
create_log_entry("Creating vald_fd_jumps_with_off_id via roster mapping")
vald_fd_jumps_with_off_id <- vald_fd_jumps %>% 
  mutate(vald_name = full_name) %>%
  select(-full_name) %>% 
  left_join(roster_mapping, by = "vald_name") %>% 
  filter(!is.na(official_id))

create_log_entry(glue("VALD FD jumps with official_id: {nrow(vald_fd_jumps_with_off_id)} rows"))

# Step 3: Calculate readiness from individual components using SAFE_DIVIDE logic
#         readiness = vald_fd_jumps_with_off_id %>% 
#         select(date,official_id,jump_height_readiness,epf_readiness,rsi_readiness) %>% 
#         mutate(readiness = (jump_height_readiness + epf_readiness + rsi_readiness) / 3 ) %>% 
#         select(-jump_height_readiness,-epf_readiness,-rsi_readiness)
create_log_entry("Calculating readiness scores")
readiness <- vald_fd_jumps_with_off_id %>% 
  select(date, official_id, jump_height_readiness, epf_readiness, rsi_readiness) %>% 
  mutate(
    # Use SAFE_DIVIDE logic: sum non-NULL values, divide by count of non-NULL values
    readiness_sum = coalesce(jump_height_readiness, 0) + coalesce(epf_readiness, 0) + coalesce(rsi_readiness, 0),
    readiness_count = (!is.na(jump_height_readiness)) + (!is.na(epf_readiness)) + (!is.na(rsi_readiness)),
    readiness = if_else(readiness_count > 0, readiness_sum / readiness_count, NA_real_)
  ) %>% 
  select(date, official_id, readiness)

create_log_entry(glue("Readiness data calculated: {nrow(readiness)} rows"))

# Check readiness value bounds
readiness_range <- range(readiness$readiness, na.rm=TRUE)
if (readiness_range[1] < 0 || readiness_range[2] > 100) {
  create_log_entry(sprintf("WARN: Readiness values outside expected 0-100 range: [%.2f, %.2f]", 
                           readiness_range[1], readiness_range[2]), "WARN")
}

################################################################################
# MERGE WORKLOAD + READINESS BY (official_id, date)
################################################################################

# Step 4: merged_data = readiness %>% 
#         left_join(workload_with_off_id, by = c("official_id" = "official_id", "date" = "date")) %>% 
#         filter(!is.na(distance))
create_log_entry("Merging readiness and workload data by official_id and date")
merged_data <- readiness %>% 
  left_join(workload_with_off_id, by = c("official_id" = "official_id", "date" = "date")) %>% 
  filter(!is.na(distance))

create_log_entry(glue("Merged data: {nrow(merged_data)} rows with both readiness and workload"))
if (nrow(merged_data) == 0) {
  create_log_entry("No matched workload-readiness rows after join", "INFO", reason="no_readiness_match")
  upload_logs_to_bigquery()
  cat("⏭️  TRAINING SKIPPED (no matches after join)\n"); quit(status=0)
}

# Use merged_data for modeling (rename to data_joined for compatibility with rest of script)
data_joined <- merged_data

################################################################################
# SPLIT, PREP, MODELING
################################################################################
create_log_entry("Creating train/test splits")
assign_split <- function(df, frac_train = 0.80) {
  df <- df %>% arrange(date)
  n <- nrow(df); if (n == 0) { df$is_test <- NA_integer_; return(df) }
  k <- max(1L, floor(n * frac_train))
  df$is_test <- c(rep(1L, k), rep(2L, n - k))
  df
}
data_split <- data_joined %>%
  group_by(official_id) %>% group_modify(~assign_split(.x, cfg_frac_train)) %>% ungroup()

# Prepare modeling table
response_var <- "readiness"
data_model <- data_split %>%
  transmute(
    official_id,
    date,
    is_test,
    distance              = distance,
    hsd                   = high_speed_distance,
    ml                    = mechanical_load,
    distance_7_day        = distance_7d,
    hsd_7_day             = hsd_7d,
    ml_7_day              = ml_7d,
    distance_28_day       = distance_28d,
    hsd_28_day            = hsd_28d,
    ml_28_day             = ml_28d,
    distance_montony      = distance_monotony_7d,
    ml_montony            = ml_monotony_7d,
    readiness             = readiness
  )
all_predictors <- c(
  "distance","hsd","ml",
  "distance_7_day","hsd_7_day","ml_7_day",
  "distance_28_day","hsd_28_day","ml_28_day",
  "distance_montony","ml_montony"
)
data_clean <- as.data.table(data_model)
create_log_entry(glue("Model input: {nrow(data_clean)} rows, {length(unique(data_clean$official_id))} athletes"))

# Hyperparams (FIX 4.2: using constants)
lambda_grid    <- exp(seq(log(1e-4), log(100), length.out = LAMBDA_GRID_SIZE_FULL))
lambda_grid_en <- exp(seq(log(1e-4), log(100), length.out = LAMBDA_GRID_SIZE_EN))

# ---- Training functions (same as before but using constants) ----
run_glmnet_cvfit <- function(train_df, preds, alpha_glmnet, lambda_grid, fit_intercept, normalization) {
  mm_cols <- c(response_var, preds)
  tr_cc <- train_df[stats::complete.cases(train_df[, mm_cols, drop = FALSE]), , drop = FALSE]
  if (nrow(tr_cc) < 2) stop("Insufficient complete cases")
  if (normalization) {
    fit_sc <- safe_scale_fit(tr_cc, preds); Xtr <- fit_sc$X; sc <- fit_sc
  } else { Xtr <- as.matrix(tr_cc[, preds, drop = FALSE]); sc <- list(center=NULL, scale=NULL, cols=preds) }
  y <- as.numeric(tr_cc[[response_var]])
  nfolds_used <- choose_nfolds(nrow(tr_cc))
  cv <- suppressWarnings(glmnet::cv.glmnet(
    Xtr, y, alpha = alpha_glmnet, lambda = lambda_grid,
    intercept = fit_intercept, standardize = FALSE,
    nfolds = nfolds_used, grouped = FALSE
  ))
  list(cv=cv, scaler=sc, Xtr=Xtr, y=y, train_cc=tr_cc)
}

run_glmnet_predict <- function(cvfit, newX, s = "lambda.min") as.numeric(predict(cvfit, newX, s = s))

run_linear <- function(train_df, test_df, preds, val_method) {
  best <- NULL
  for (fi in c(TRUE, FALSE)) for (sc in c(TRUE, FALSE)) {
    rhs <- paste(preds, collapse = " + ")
    form <- as.formula(paste(response_var, "~", if (fi) rhs else paste("0 +", rhs)))
    tr_df <- to_numeric_df(train_df, preds)
    te_df <- if (nrow(test_df) > 0) to_numeric_df(test_df, preds) else test_df
    mm_cols <- c(response_var, preds)
    tr_cc <- tr_df[stats::complete.cases(tr_df[, mm_cols, drop = FALSE]), , drop = FALSE]
    if (!enough_rows(nrow(tr_cc), 2L)) next
    if (sc) { 
      fit_sc <- safe_scale_fit(tr_cc, preds)
      tr_fit <- tr_cc
      tr_fit[, preds] <- fit_sc$X
      m <- stats::lm(form, data=tr_fit, singular.ok=TRUE)
    } else { 
      fit_sc <- NULL
      m <- stats::lm(form, data=tr_cc)
      tr_fit <- tr_cc
    }
    train_rmse_val <- rmse(tr_fit[[response_var]], stats::predict(m, tr_fit))
    k <- choose_nfolds(nrow(tr_cc))
    folds <- sample(rep(1:k, length.out=nrow(tr_cc)))
    cv_vals <- numeric(k)
    for (f in seq_len(k)) {
      tr_ix <- folds != f; te_ix <- folds == f
      if (sum(tr_ix) < 2 || sum(te_ix) < 1) { cv_vals[f] <- NA_real_; next }
      m_cv <- stats::lm(form, data = tr_cc[tr_ix, , drop=FALSE])
      cv_vals[f] <- rmse(tr_cc[[response_var]][te_ix], stats::predict(m_cv, tr_cc[te_ix, , drop=FALSE]))
    }
    cv_val <- mean(cv_vals, na.rm=TRUE)
    if (val_method == "test_set" && nrow(te_df) > 0) {
      te_cc <- te_df[stats::complete.cases(te_df[, preds, drop = FALSE]), , drop = FALSE]
      if (nrow(te_cc) > 0) {
        if (sc) {
          te_scaled <- safe_scale_apply(te_cc, fit_sc)
          te_fit <- te_cc
          te_fit[, preds] <- te_scaled
          test_rmse_val <- rmse(te_fit[[response_var]], stats::predict(m, te_fit))
        } else {
          test_rmse_val <- rmse(te_cc[[response_var]], stats::predict(m, te_cc))
        }
      } else test_rmse_val <- NA_real_
      primary <- test_rmse_val
    } else { test_rmse_val <- NA_real_; primary <- cv_val }
    cand <- list(
      model_type="linear", 
      train_rmse=train_rmse_val, 
      cv_rmse=cv_val, 
      test_rmse=test_rmse_val, 
      primary_rmse=primary,
      hyper=sprintf("fit_intercept=%s; normalization=%s", fi, sc),
      fitted=list(
        type="lm", 
        model=m, 
        predictors=preds,
        preprocessing=list(
          normalization=sc, 
          center=if (sc) fit_sc$center else NULL,
          scale=if (sc) fit_sc$scale else NULL, 
          cols=if (sc) fit_sc$cols else preds,
          fit_intercept=fi
        )
      )
    )
    if (is.null(best) || (is.finite(cand$primary_rmse) && cand$primary_rmse < best$primary_rmse)) best <- cand
  }
  best
}

run_ridge <- function(train_df, test_df, preds, val_method) {
  best <- NULL
  for (fi in c(TRUE,FALSE)) for (sc_norm in c(TRUE,FALSE)) {
    fit <- run_glmnet_cvfit(train_df, preds, 0, lambda_grid, fi, sc_norm)
    train_rmse_val <- rmse(fit$y, run_glmnet_predict(fit$cv, fit$Xtr, "lambda.min"))
    cv_rmse_val <- sqrt(min(fit$cv$cvm))
    if (val_method == "test_set" && nrow(test_df) > 0) {
      te_df <- to_numeric_df(test_df, preds)
      te_cc <- te_df[stats::complete.cases(te_df[, preds, drop = FALSE]), , drop = FALSE]
      if (nrow(te_cc) > 0) {
        Xn <- if (sc_norm) safe_scale_apply(te_cc, fit$scaler) else as.matrix(te_cc[, preds, drop = FALSE])
        test_rmse_val <- rmse(te_cc[[response_var]], run_glmnet_predict(fit$cv, Xn, "lambda.min"))
      } else test_rmse_val <- NA_real_
      primary <- test_rmse_val
    } else { test_rmse_val <- NA_real_; primary <- cv_rmse_val }
    cand <- list(
      model_type="ridge", 
      train_rmse=train_rmse_val, 
      cv_rmse=cv_rmse_val, 
      test_rmse=test_rmse_val, 
      primary_rmse=primary,
      hyper=sprintf("lambda=%.6f; fit_intercept=%s; normalization=%s", fit$cv$lambda.min, fi, sc_norm),
      fitted=list(
        type="glmnet", 
        model=fit$cv, 
        predictors=preds,
        preprocessing=list(
          normalization=sc_norm, 
          center=if (sc_norm) fit$scaler$center else NULL,
          scale=if (sc_norm) fit$scaler$scale else NULL, 
          cols=fit$scaler$cols,
          fit_intercept=fi
        ),
        lambda_min=fit$cv$lambda.min, 
        alpha=0
      )
    )
    if (is.null(best) || (is.finite(cand$primary_rmse) && cand$primary_rmse < best$primary_rmse)) best <- cand
  }
  best
}

run_lasso <- function(train_df, test_df, preds, val_method) {
  best <- NULL
  for (fi in c(TRUE,FALSE)) for (sc_norm in c(TRUE,FALSE)) {
    fit <- run_glmnet_cvfit(train_df, preds, 1, lambda_grid, fi, sc_norm)
    train_rmse_val <- rmse(fit$y, run_glmnet_predict(fit$cv, fit$Xtr, "lambda.min"))
    cv_rmse_val <- sqrt(min(fit$cv$cvm))
    if (val_method == "test_set" && nrow(test_df) > 0) {
      te_df <- to_numeric_df(test_df, preds)
      te_cc <- te_df[stats::complete.cases(te_df[, preds, drop = FALSE]), , drop = FALSE]
      if (nrow(te_cc) > 0) {
        Xn <- if (sc_norm) safe_scale_apply(te_cc, fit$scaler) else as.matrix(te_cc[, preds, drop = FALSE])
        test_rmse_val <- rmse(te_cc[[response_var]], run_glmnet_predict(fit$cv, Xn, "lambda.min"))
      } else test_rmse_val <- NA_real_
      primary <- test_rmse_val
    } else { test_rmse_val <- NA_real_; primary <- cv_rmse_val }
    cand <- list(
      model_type="lasso", 
      train_rmse=train_rmse_val, 
      cv_rmse=cv_rmse_val, 
      test_rmse=test_rmse_val, 
      primary_rmse=primary,
      hyper=sprintf("lambda=%.6f; fit_intercept=%s; normalization=%s", fit$cv$lambda.min, fi, sc_norm),
      fitted=list(
        type="glmnet", 
        model=fit$cv, 
        predictors=preds,
        preprocessing=list(
          normalization=sc_norm, 
          center=if (sc_norm) fit$scaler$center else NULL,
          scale=if (sc_norm) fit$scaler$scale else NULL, 
          cols=fit$scaler$cols,
          fit_intercept=fi
        ),
        lambda_min=fit$cv$lambda.min, 
        alpha=1
      )
    )
    if (is.null(best) || (is.finite(cand$primary_rmse) && cand$primary_rmse < best$primary_rmse)) best <- cand
  }
  best
}

run_elastic <- function(train_df, test_df, preds, val_method) {
  best <- NULL
  for (ai in seq(0.1,0.9,by=0.2)) for (fi in c(TRUE,FALSE)) for (sc_norm in c(TRUE,FALSE)) {
    fit <- run_glmnet_cvfit(train_df, preds, ai, lambda_grid_en, fi, sc_norm)
    train_rmse_val <- rmse(fit$y, run_glmnet_predict(fit$cv, fit$Xtr, "lambda.min"))
    cv_rmse_val <- sqrt(min(fit$cv$cvm))
    if (val_method == "test_set" && nrow(test_df) > 0) {
      te_df <- to_numeric_df(test_df, preds)
      te_cc <- te_df[stats::complete.cases(te_df[, preds, drop = FALSE]), , drop = FALSE]
      if (nrow(te_cc) > 0) {
        Xn <- if (sc_norm) safe_scale_apply(te_cc, fit$scaler) else as.matrix(te_cc[, preds, drop = FALSE])
        test_rmse_val <- rmse(te_cc[[response_var]], run_glmnet_predict(fit$cv, Xn, "lambda.min"))
      } else test_rmse_val <- NA_real_
      primary <- test_rmse_val
    } else { test_rmse_val <- NA_real_; primary <- cv_rmse_val }
    cand <- list(
      model_type="elastic_net", 
      train_rmse=train_rmse_val, 
      cv_rmse=cv_rmse_val, 
      test_rmse=test_rmse_val, 
      primary_rmse=primary,
      hyper=sprintf("lambda=%.6f; l1_ratio=%.1f; fit_intercept=%s; normalization=%s", fit$cv$lambda.min, ai, fi, sc_norm),
      fitted=list(
        type="glmnet", 
        model=fit$cv, 
        predictors=preds,
        preprocessing=list(
          normalization=sc_norm, 
          center=if (sc_norm) fit$scaler$center else NULL,
          scale=if (sc_norm) fit$scaler$scale else NULL, 
          cols=fit$scaler$cols,
          fit_intercept=fi
        ),
        lambda_min=fit$cv$lambda.min, 
        alpha=ai
      )
    )
    if (is.null(best) || (is.finite(cand$primary_rmse) && cand$primary_rmse < best$primary_rmse)) best <- cand
  }
  best
}

run_bayes_reg <- function(train_df, test_df, preds, val_method) {
  if (!reticulate::py_module_available("sklearn")) {
    return(list(
      model_type="bayesian_regression", 
      train_rmse=Inf, 
      cv_rmse=Inf, 
      test_rmse=Inf, 
      primary_rmse=Inf,
      hyper="sklearn not available", 
      fitted=list(type="error")
    ))
  }
  sk <- reticulate::import("sklearn.linear_model", delay_load = TRUE)
  BayesRidge <- sk$BayesianRidge
  best <- NULL
  for (a1 in c(1e-6, 1e-4)) for (l1 in c(1e-6, 1e-4)) {
    tr_df <- to_numeric_df(train_df, preds)
    mm_cols <- c(response_var, preds)
    tr_cc <- tr_df[stats::complete.cases(tr_df[, mm_cols, drop = FALSE]), , drop = FALSE]
    if (!enough_rows(nrow(tr_cc), 2L)) next
    fit_sc <- safe_scale_fit(tr_cc, preds)
    Xtr <- fit_sc$X; y <- as.numeric(tr_cc[[response_var]])
    m <- BayesRidge(alpha_1=a1, alpha_2=a1, lambda_1=l1, lambda_2=l1, fit_intercept=TRUE)
    m$fit(Xtr, y)
    train_rmse_val <- rmse(y, as.numeric(m$predict(Xtr)))
    k <- choose_nfolds(nrow(tr_cc))
    folds <- sample(rep(seq_len(k), length.out=nrow(tr_cc)))
    cv_vals <- numeric(k)
    for (f in seq_len(k)) {
      tr_ix <- folds != f; te_ix <- folds == f
      if (sum(tr_ix) < 2 || sum(te_ix) < 1) { cv_vals[f] <- NA_real_; next }
      sc_cv <- safe_scale_fit(tr_cc[tr_ix, , drop=FALSE], preds)
      m_cv <- BayesRidge(alpha_1=a1, alpha_2=a1, lambda_1=l1, lambda_2=l1)
      m_cv$fit(sc_cv$X, as.numeric(tr_cc[[response_var]][tr_ix]))
      Xte_cv <- safe_scale_apply(tr_cc[te_ix, , drop=FALSE], sc_cv)
      cv_vals[f] <- rmse(tr_cc[[response_var]][te_ix], as.numeric(m_cv$predict(Xte_cv)))
    }
    cv_rmse_val <- mean(cv_vals, na.rm=TRUE)
    if (val_method == "test_set" && nrow(test_df) > 0) {
      te_df <- to_numeric_df(test_df, preds)
      te_cc <- te_df[stats::complete.cases(te_df[, preds, drop = FALSE]), , drop = FALSE]
      if (nrow(te_cc) > 0) {
        Xte <- safe_scale_apply(te_cc, fit_sc)
        test_rmse_val <- rmse(te_cc[[response_var]], as.numeric(m$predict(Xte)))
      } else test_rmse_val <- NA_real_
      primary <- test_rmse_val
    } else { test_rmse_val <- NA_real_; primary <- cv_rmse_val }
    coef_vec <- tryCatch(as.numeric(reticulate::py_to_r(m$coef_)), error=function(e) rep(NA_real_, length(preds)))
    intercept_val <- tryCatch(as.numeric(reticulate::py_to_r(m$intercept_)), error=function(e) NA_real_)
    cand <- list(
      model_type="bayesian_regression", 
      train_rmse=train_rmse_val, 
      cv_rmse=cv_rmse_val, 
      test_rmse=test_rmse_val, 
      primary_rmse=primary,
      hyper=sprintf("alpha_1=%.0e; lambda_1=%.0e", a1, l1),
      fitted=list(
        type="bayesridge_params", 
        predictors=preds,
        preprocessing=list(
          normalization=TRUE, 
          center=fit_sc$center, 
          scale=fit_sc$scale, 
          cols=fit_sc$cols, 
          fit_intercept=TRUE
        ),
        coefficients=coef_vec, 
        intercept=intercept_val,
        priors=list(alpha_1=a1, alpha_2=a1, lambda_1=l1, lambda_2=l1)
      )
    )
    if (is.null(best) || (is.finite(cand$primary_rmse) && cand$primary_rmse < best$primary_rmse)) best <- cand
  }
  best
}

run_bayes_net <- function(train_df, test_df, preds, val_method) {
  cols <- c(response_var, preds)
  tr_df <- train_df[, cols, drop = FALSE]
  for (cl in cols) if (!is.numeric(tr_df[[cl]])) tr_df[[cl]] <- as.numeric(tr_df[[cl]])
  tr_df <- tr_df[stats::complete.cases(tr_df), , drop = FALSE]
  if (nrow(tr_df) < 3) {
    return(list(
      model_type="bayesian_network", 
      train_rmse=Inf, 
      cv_rmse=Inf, 
      test_rmse=Inf, 
      primary_rmse=Inf,
      hyper="insufficient data", 
      fitted=list(type="error")
    ))
  }
  if (nrow(test_df) > 0) {
    te_df <- test_df[, cols, drop = FALSE]
    for (cl in cols) if (!is.numeric(te_df[[cl]])) te_df[[cl]] <- as.numeric(te_df[[cl]])
    te_df <- te_df[stats::complete.cases(te_df), , drop = FALSE]
  } else te_df <- data.frame()
  
  best <- NULL
  for (alg in c("hc","tabu")) {
    net <- if (alg == "hc") bnlearn::hc(tr_df, score="bic-g") else bnlearn::tabu(tr_df, score="bic-g")
    fit <- bnlearn::bn.fit(net, data=tr_df, method="mle-g")
    pr_tr <- as.numeric(predict(fit, node=response_var, data=tr_df[, preds, drop = FALSE]))
    train_rmse_val <- rmse(tr_df[[response_var]], pr_tr)
    k <- choose_nfolds(nrow(tr_df))
    folds <- sample(rep(seq_len(k), length.out=nrow(tr_df)))
    cv_vals <- numeric(k)
    for (f in seq_len(k)) {
      tr_ix <- folds != f; te_ix <- folds == f
      if (sum(tr_ix) < 3 || sum(te_ix) < 1) { cv_vals[f] <- NA_real_; next }
      net_cv <- if (alg == "hc") bnlearn::hc(tr_df[tr_ix,], score="bic-g") else bnlearn::tabu(tr_df[tr_ix,], score="bic-g")
      fit_cv <- bnlearn::bn.fit(net_cv, data=tr_df[tr_ix,], method="mle-g")
      pr_cv  <- as.numeric(predict(fit_cv, node=response_var, data=tr_df[te_ix, preds, drop = FALSE]))
      cv_vals[f] <- rmse(tr_df[[response_var]][te_ix], pr_cv)
    }
    cv_rmse_val <- mean(cv_vals, na.rm=TRUE)
    if (nrow(te_df) > 0 && val_method=="test_set") {
      pr_te <- as.numeric(predict(fit, node=response_var, data=te_df[, preds, drop = FALSE]))
      test_rmse_val <- rmse(te_df[[response_var]], pr_te)
      primary <- test_rmse_val
    } else { test_rmse_val <- NA_real_; primary <- cv_rmse_val }
    cand <- list(
      model_type="bayesian_network", 
      train_rmse=train_rmse_val, 
      cv_rmse=cv_rmse_val, 
      test_rmse=test_rmse_val, 
      primary_rmse=primary,
      hyper=sprintf("structure=%s; param=mle", alg),
      fitted=list(
        type="bnlearn", 
        model=fit, 
        predictors=preds,
        preprocessing=list(
          normalization=FALSE, 
          center=NULL, 
          scale=NULL, 
          cols=preds
        ),
        structure=alg, 
        param_method="mle"
      )
    )
    if (is.null(best) || (is.finite(cand$primary_rmse) && cand$primary_rmse < best$primary_rmse)) best <- cand
  }
  best
}

################################################################################
# TRAIN MODELS FOR ALL ATHLETES
################################################################################
create_log_entry("Starting model training loop")
athletes <- unique(na.omit(data_clean$official_id))
n_athletes <- length(athletes)
create_log_entry(glue("Training models for {n_athletes} athletes"))

# FIX 3.7: Semantic versioning
generate_version_id <- function() {
  date_range <- paste0(
    format(cfg_start_date, "%Y%m%d"),
    "-",
    format(cfg_end_date, "%Y%m%d")
  )
  
  git_sha <- Sys.getenv("GITHUB_SHA", "unknown")
  short_sha <- if (nzchar(git_sha) && git_sha != "unknown") substr(git_sha, 1, 7) else "manual"
  
  run_id <- Sys.getenv("GITHUB_RUN_ID", format(Sys.time(), "%Y%m%d_%H%M%S"))
  
  sprintf("v1_%s_run%s_%s", date_range, run_id, short_sha)
}

version_id <- generate_version_id()
create_log_entry(glue("Generated version ID: {version_id}"))

all_results <- list()
all_predictions <- list()
success_count <- 0
fail_count <- 0

for (i in seq_len(n_athletes)) {
  athlete_id <- athletes[i]
  
  # Progress indicator every 10 athletes
  if (i %% 10 == 0 || i == n_athletes) {
    pct_complete <- round((i / n_athletes) * 100)
    elapsed <- as.numeric(difftime(Sys.time(), script_start, units="mins"))
    est_remaining <- if(i > 0) (elapsed / i) * (n_athletes - i) else NA
    
    create_log_entry(sprintf(
      "Progress: %d/%d (%d%%) | Success: %d | Failed: %d | Elapsed: %.1f min | Est remaining: %.1f min",
      i, n_athletes, pct_complete, success_count, fail_count, elapsed, est_remaining
    ), "INFO", reason="progress_update")
  }
  
  create_log_entry(glue("[{i}/{n_athletes}] Training models for {athlete_id}"))
  
  tryCatch({
    ath_data  <- data_clean[official_id == athlete_id]
    ath_train <- ath_data[is_test == 1]
    ath_test  <- ath_data[is_test == 2]
    
    if (nrow(ath_train) < MIN_TRAIN_OBS) {
      reason <- glue(
        "Athlete has only {nrow(ath_train)} training samples (minimum required: {MIN_TRAIN_OBS}). ",
        "Date range: {min(ath_train$date)} to {max(ath_train$date)}. ",
        "Consider extending date range or lowering MIN_TRAIN_OBS threshold."
      )
      create_log_entry(glue("  SKIP: {reason}"), "WARN", reason="insufficient_training_data")
      fail_count <- fail_count + 1
      next
    }
    
    preds <- intersect(all_predictors, names(ath_train))
    if (length(preds) == 0) {
      create_log_entry("  SKIP: no predictors available", "WARN", reason="no_predictors")
      fail_count <- fail_count + 1
      next
    }
    
    ath_train_df <- as.data.frame(ath_train[, c(response_var, preds), with=FALSE])
    ath_test_df  <- as.data.frame(ath_test[,  c(response_var, preds), with=FALSE])
    ath_train_df <- to_numeric_df(ath_train_df, preds)
    if (nrow(ath_test_df) > 0) ath_test_df <- to_numeric_df(ath_test_df, preds)
    
    imp <- impute_train_test(ath_train_df, ath_test_df, preds)
    ath_train_df <- imp$train
    ath_test_df <- imp$test
    
    sds <- sapply(ath_train_df[, preds, drop=FALSE], stats::sd, na.rm=TRUE)
    keep <- names(sds)[is.finite(sds) & sds > MIN_PREDICTOR_VARIANCE]
    if (length(keep) < 1) {
      create_log_entry("  SKIP: no predictor variance", "WARN", reason="no_variance")
      fail_count <- fail_count + 1
      next
    }
    preds <- keep
    
    n_tr <- nrow(ath_train_df)
    n_te <- nrow(ath_test_df)
    val_method <- if (n_te >= MIN_TEST_OBS_FOR_EVAL && n_tr/(n_tr+n_te) >= MIN_TRAIN_RATIO) {
      "test_set"
    } else {
      "cv"
    }
    if (val_method == "cv") ath_test_df <- data.frame()
    
    # Train candidates
    cands <- list(
      safe_runner("linear",              run_linear,    ath_train_df, ath_test_df, preds, val_method),
      safe_runner("ridge",               run_ridge,     ath_train_df, ath_test_df, preds, val_method),
      safe_runner("lasso",               run_lasso,     ath_train_df, ath_test_df, preds, val_method),
      safe_runner("elastic_net",         run_elastic,   ath_train_df, ath_test_df, preds, val_method),
      safe_runner("bayesian_regression", run_bayes_reg, ath_train_df, ath_test_df, preds, val_method),
      safe_runner("bayesian_network",    run_bayes_net, ath_train_df, ath_test_df, preds, val_method)
    )
    
    prim_rmse <- sapply(cands, function(c) c$primary_rmse)
    finite_idx <- which(is.finite(prim_rmse))
    if (length(finite_idx) == 0) {
      create_log_entry("  ERROR: all models failed", "ERROR")
      fail_count <- fail_count + 1
      next
    }
    
    best_idx <- finite_idx[which.min(prim_rmse[finite_idx])]
    best_cand <- cands[[best_idx]]
    
    # Save best model to registry
    save_model_to_registry <- function(athlete_id, model_name, candidate, version_id) {
      if (is.null(candidate$fitted) || candidate$fitted$type == "error" || !nzchar(gcs_bucket)) {
        return(list(
          success = FALSE, 
          model_id = paste(team_name, model_name, sep=":"), 
          version_id = version_id, 
          artifact_uri = NA
        ))
      }
      
      tryCatch({
        model_id <- paste(team_name, model_name, sep = ":")
        
        # Save artifact to GCS
        dir.create("artifact", showWarnings = FALSE)
        model_save <- list(
          model=candidate$fitted$model, 
          type=candidate$model_type,
          predictors=candidate$fitted$predictors, 
          preprocessing=candidate$fitted$preprocessing,
          date_created=Sys.time(), 
          athlete_id=athlete_id
        )
        saveRDS(model_save, "artifact/model.rds")
        sha <- digest::digest(file="artifact/model.rds", algo="sha256")
        
        manifest <- list(
          model_name=model_name, 
          version_id=version_id, 
          created_at=as.character(Sys.time()),
          r_version=R.version$version.string, 
          checksum_sha256=sha, 
          hyperparameters=candidate$hyper
        )
        jsonlite::write_json(manifest, "artifact/manifest.json", auto_unbox=TRUE, pretty=TRUE)
        
        safe_model_name <- gsub("[^A-Za-z0-9]+", "_", model_name)
        gcs_path <- sprintf("models/%s/%s/%s/", team_name, safe_model_name, version_id)
        
        gcs_upload("artifact/model.rds", name=paste0(gcs_path, "model.rds"))
        gcs_upload("artifact/manifest.json", name=paste0(gcs_path, "manifest.json"))
        artifact_uri <- sprintf("gs://%s/%smodel.rds", gcs_bucket, gcs_path)
        
        # Save to registry with enhanced metadata (FIX 1.3)
        con <- DBI::dbConnect(bigrquery::bigquery(), project = project, dataset = dataset)
        
        DBI::dbExecute(con, glue("
          MERGE `{project}.{dataset}.registry_models` T 
          USING (SELECT '{model_id}' AS model_id, '{model_name}' AS model_name,
                        '{team_name}' AS team, 'auto' AS owner, 
                        'Athlete readiness model' AS description,
                        CURRENT_TIMESTAMP() AS created_at) S
          ON T.model_id = S.model_id
          WHEN NOT MATCHED THEN INSERT ROW
        "))
        
        # Prepare values outside glue to avoid nested quotes
        run_id_val <- Sys.getenv("GITHUB_RUN_ID", "manual")
        git_sha_val <- Sys.getenv("GITHUB_SHA", "unknown")
        preds_json <- jsonlite::toJSON(preds, auto_unbox=TRUE)
        hyper_json <- jsonlite::toJSON(candidate$hyper, auto_unbox=TRUE)
        
        DBI::dbExecute(con, glue("
          INSERT INTO `{project}.{dataset}.registry_versions`
            (model_id, version_id, created_at, created_by, artifact_uri, artifact_sha256,
             framework, r_version, package_info, notes,
             training_data_start_date, training_data_end_date,
             n_training_samples, n_test_samples, validation_method,
             predictor_list, hyperparameters,
             train_rmse, cv_rmse, test_rmse,
             pipeline_run_id, git_commit_sha)
          VALUES
            ('{model_id}','{version_id}',CURRENT_TIMESTAMP(),'github_actions',
             '{artifact_uri}','{sha}','R/glmnet','{R.version$version.string}','','',
             '{cfg_start_date}', '{cfg_end_date}',
             {n_tr}, {n_te}, '{val_method}',
             '{preds_json}',
             '{hyper_json}',
             {candidate$train_rmse}, {candidate$cv_rmse}, {candidate$test_rmse},
             '{run_id_val}',
             '{git_sha_val}')
        "))
        
        metrics <- tibble(
          model_id = model_id, 
          version_id = version_id,
          metric_name = c("train_rmse","cv_rmse","test_rmse","primary_rmse"),
          metric_value = c(candidate$train_rmse, candidate$cv_rmse, candidate$test_rmse, candidate$primary_rmse),
          split = c("train","cv","test","primary"), 
          logged_at = Sys.time()
        )
        bq_table_upload(
          bq_table(project, dataset, "registry_metrics"), 
          metrics, 
          write_disposition="WRITE_APPEND"
        )
        
        DBI::dbExecute(con, glue("
          INSERT INTO `{project}.{dataset}.registry_stages`
            (model_id, version_id, stage, set_at, set_by, reason)
          VALUES
            ('{model_id}','{version_id}','Staging',CURRENT_TIMESTAMP(),'auto','Initial training')
        "))
        
        DBI::dbDisconnect(con)
        unlink("artifact", recursive=TRUE)
        
        list(success=TRUE, model_id=model_id, version_id=version_id, artifact_uri=artifact_uri)
      }, error = function(e) {
        create_log_entry(paste("Model save failed for", model_name, ":", e$message), "ERROR")
        list(success=FALSE, model_id=NA, version_id=NA, artifact_uri=NA)
      })
    }
    
    model_name <- paste(athlete_id, best_cand$model_type, sep = "_")
    save_result <- save_model_to_registry(athlete_id, model_name, best_cand, version_id)
    
    if (isTRUE(save_result$success) || is.na(save_result$success)) {
      create_log_entry(glue("  SUCCESS: {best_cand$model_type} (RMSE={round(best_cand$primary_rmse, 3)})"))
      success_count <- success_count + 1
      
      # Generate predictions
      all_data <- rbind(ath_train_df, ath_test_df)
      if (nrow(all_data) > 0) {
        preds_model <- tryCatch({
          if (best_cand$fitted$type == "lm") {
            predict(best_cand$fitted$model, all_data)
          } else if (best_cand$fitted$type == "glmnet") {
            if (isTRUE(best_cand$fitted$preprocessing$normalization)) {
              X_scaled <- safe_scale_apply(all_data, best_cand$fitted$preprocessing)
              as.numeric(predict(best_cand$fitted$model, X_scaled, s="lambda.min"))
            } else {
              X <- as.matrix(all_data[, preds, drop=FALSE])
              as.numeric(predict(best_cand$fitted$model, X, s="lambda.min"))
            }
          } else if (best_cand$fitted$type == "bnlearn") {
            as.numeric(predict(best_cand$fitted$model, node = response_var, data = all_data[, preds, drop=FALSE]))
          } else {
            rep(NA_real_, nrow(all_data))
          }
        }, error = function(e) {
          create_log_entry(glue("  Prediction generation failed: {e$message}"), "WARN")
          rep(NA_real_, nrow(all_data))
        })
        
        pred_meta <- rbind(
          ath_train[, .(official_id, date, readiness, is_test)],
          ath_test[,  .(official_id, date, readiness, is_test)]
        )
        
        pred_df <- pred_meta %>%
          mutate(
            predicted_readiness = preds_model,
            model_id = save_result$model_id %||% NA_character_,
            version_id = save_result$version_id %||% NA_character_,
            prediction_date = Sys.time()
          )
        all_predictions[[length(all_predictions) + 1]] <- pred_df
      }
      
      # Save summary row
      all_results[[length(all_results) + 1]] <- tibble(
        athlete_id = athlete_id,
        roster_name = NA_character_,
        model_type = best_cand$model_type,
        model_id = save_result$model_id %||% NA_character_,
        version_id = save_result$version_id %||% NA_character_,
        artifact_uri = save_result$artifact_uri %||% NA_character_,
        train_rmse = best_cand$train_rmse,
        cv_rmse = best_cand$cv_rmse,
        test_rmse = best_cand$test_rmse,
        primary_rmse = best_cand$primary_rmse,
        n_train = n_tr,
        n_test = n_te,
        n_predictors = length(preds),
        validation_method = val_method,
        hyperparameters = best_cand$hyper,
        trained_at = Sys.time()
      )
    } else {
      create_log_entry(glue("  ERROR: model save failed"), "ERROR")
      fail_count <- fail_count + 1
    }
  }, error = function(e) {
    create_log_entry(glue("  ERROR: {e$message}"), "ERROR")
    fail_count <- fail_count + 1
  })
}

################################################################################
# SAVE RESULTS TO BIGQUERY
################################################################################
create_log_entry("Saving results to BigQuery")

if (length(all_results) > 0) {
  results_df <- bind_rows(all_results)
  tbl <- bq_table(project, dataset, "model_training_summary")
  if (!bq_table_exists(tbl)) {
    bq_table_create(tbl, 
                    fields = as_bq_fields(results_df),
                    time_partitioning = list(type="DAY", field="trained_at"))
  }
  bq_table_upload(tbl, results_df, write_disposition = "WRITE_APPEND")
  create_log_entry(glue("Model training summary saved: {nrow(results_df)} rows"))
}

# FIX 2.4: Predictions with APPEND strategy and run metadata
if (length(all_predictions) > 0) {
  preds_df <- bind_rows(all_predictions) %>%
    mutate(
      prediction_run_timestamp = Sys.time(),
      prediction_run_id = Sys.getenv("GITHUB_RUN_ID", "manual")
    )
  
  tbl <- bq_table(project, dataset, predictions_table)
  if (!bq_table_exists(tbl)) {
    bq_table_create(tbl, 
                    fields = as_bq_fields(preds_df),
                    time_partitioning = list(type="DAY", field="prediction_run_timestamp"),
                    clustering_fields = c("official_id", "date", "model_id"))
  }
  
  # WRITE_APPEND preserves historical predictions
  bq_table_upload(tbl, preds_df, write_disposition = "WRITE_APPEND")
  create_log_entry(glue("Predictions saved: {nrow(preds_df)} rows (appended to history)"))
}

################################################################################
# FIX 2.3: PIPELINE HEALTH METRICS
################################################################################
create_log_entry("Saving pipeline health metrics")

health_metrics <- tibble(
  pipeline_run_id = Sys.getenv("GITHUB_RUN_ID", "manual"),
  run_timestamp = Sys.time(),
  team_name = team_name,
  
  # Data availability
  workload_rows_loaded = nrow(workload_data),
  readiness_rows_loaded = nrow(readiness_data),
  joined_rows = nrow(data_joined),
  
  # Training outcomes
  athletes_attempted = length(athletes),
  athletes_succeeded = success_count,
  athletes_failed = fail_count,
  success_rate = if(length(athletes) > 0) round(success_count / length(athletes), 3) else NA_real_,
  
  # Model quality (aggregate)
  avg_primary_rmse = if(length(all_results) > 0) {
    mean(bind_rows(all_results)$primary_rmse, na.rm=TRUE)
  } else NA_real_,
  median_primary_rmse = if(length(all_results) > 0) {
    median(bind_rows(all_results)$primary_rmse, na.rm=TRUE)
  } else NA_real_,
  
  # Execution metrics
  execution_time_minutes = as.numeric(difftime(Sys.time(), script_start, units="mins")),
  
  # Config snapshot
  config_start_date = cfg_start_date,
  config_end_date = cfg_end_date,
  config_train_fraction = cfg_frac_train
)

tryCatch({
  health_tbl <- bq_table(project, dataset, "pipeline_health_metrics")
  if (!bq_table_exists(health_tbl)) {
    bq_table_create(health_tbl, 
                    fields = as_bq_fields(health_metrics),
                    time_partitioning = list(type="DAY", field="run_timestamp"))
  }
  bq_table_upload(health_tbl, health_metrics, write_disposition = "WRITE_APPEND")
  create_log_entry("Pipeline health metrics saved")
}, error = function(e) {
  create_log_entry(paste("Health metrics save failed (non-fatal):", e$message), "WARN")
})

################################################################################
# SUMMARY
################################################################################
dur <- round(as.numeric(difftime(Sys.time(), script_start, units="mins")), 2)
create_log_entry(glue("Total execution time: {dur} minutes"))
create_log_entry(glue("Success: {success_count} athletes"))
create_log_entry(glue("Failed: {fail_count} athletes"))
create_log_entry("=== MODEL TRAINING END ===", "END")
upload_logs_to_bigquery()
cat("Script completed successfully\n")
