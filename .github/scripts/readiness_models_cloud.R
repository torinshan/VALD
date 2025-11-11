#!/usr/bin/env Rscript

################################################################################
# PREDICTIVE MODELS — Cloud Version with Model Registry
# - Read workload data from BigQuery (workload_daily table)
# - Read readiness data from BigQuery (ML Builder / VALD data)
# - Train models per athlete using 80/20 chronological split
# - Save models to GCS with metadata in BigQuery registry
# - Save predictions back to BigQuery
################################################################################

# ===== Packages (match workload ingest pattern) =====
tryCatch({
  suppressPackageStartupMessages({
    library(bigrquery); library(DBI)
    library(dplyr); library(tidyr); library(data.table)
    library(lubridate); library(slider); library(stringr)
    library(gargle); library(glue); library(digest)
    library(googleCloudStorageR)
    library(glmnet); library(bnlearn); library(reticulate)
  })
}, error = function(e) { 
  cat("Error loading packages:", e$message, "\n"); quit(status=1)
})

# Disable BigQuery Storage API (same as workload ingest)
options(bigrquery.use_bqstorage = FALSE)
Sys.setenv(BIGRQUERY_USE_BQ_STORAGE = "false")

################################################################################
# CONFIG (match workload ingest env var pattern)
################################################################################
project    <- Sys.getenv("GCP_PROJECT", "sac-vald-hub")
dataset    <- Sys.getenv("BQ_DATASET",  "analytics")
location   <- Sys.getenv("BQ_LOCATION", "US")
gcs_bucket <- Sys.getenv("GCS_BUCKET",  "sac-ml-models")
team_name  <- Sys.getenv("TEAM_NAME",   "sacstate-football")

# Table names
workload_table    <- Sys.getenv("WORKLOAD_TABLE",    "workload_daily")
readiness_table   <- Sys.getenv("READINESS_TABLE",   "ml_builder_readiness")
roster_table      <- Sys.getenv("ROSTER_TABLE",      "roster_mapping")
predictions_table <- Sys.getenv("PREDICTIONS_TABLE", "readiness_predictions")

# Model training config
cfg_start_date <- as.Date(Sys.getenv("START_DATE", "2025-06-01"))
cfg_end_date   <- as.Date(Sys.getenv("END_DATE",   "2025-12-31"))
cfg_frac_train <- as.numeric(Sys.getenv("TRAIN_FRACTION", "0.80"))
min_obs_train  <- as.integer(Sys.getenv("MIN_TRAIN_OBS", "3"))

cat("=== CONFIGURATION ===\n")
cat("GCP Project:", project, "\n")
cat("BQ Dataset:", dataset, "\n")
cat("GCS Bucket:", gcs_bucket, "\n")
cat("Team:", team_name, "\n")
cat("Workload table:", workload_table, "\n")
cat("Readiness table:", readiness_table, "\n")
cat("Date range:", cfg_start_date, "to", cfg_end_date, "\n")
cat("Train fraction:", cfg_frac_train, "\n\n")

################################################################################
# LOGGING
################################################################################
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
  
  # GCS auth
  gcs_auth(token = gargle::gargle2.0_token(
    scope = 'https://www.googleapis.com/auth/devstorage.full_control',
    client = gargle::gargle_client(),
    credentials = list(access_token = GLOBAL_ACCESS_TOKEN)
  ))
  gcs_global_bucket(gcs_bucket)
  
  create_log_entry("Authentication successful")
}, error = function(e) {
  create_log_entry(paste("Authentication failed:", e$message), "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
})

# ===== Token Refresh Function (match workload ingest) =====
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
      gcs_auth(token = gargle::gargle2.0_token(
        scope = 'https://www.googleapis.com/auth/devstorage.full_control',
        client = gargle::gargle_client(),
        credentials = list(access_token = GLOBAL_ACCESS_TOKEN)
      ))
      create_log_entry("Token refreshed", "INFO")
    }
  }, error = function(e) {
    create_log_entry(paste("Token refresh warning:", e$message), "WARN")
  })
}

################################################################################
# HELPER FUNCTIONS
################################################################################
normalize_key <- function(x) {
  y <- tolower(as.character(x))
  y <- stringr::str_replace_all(y, "[^a-z0-9 ]", " ")
  stringr::str_squish(y)
}

rmse <- function(obs, pred) sqrt(mean((obs - pred)^2, na.rm=TRUE))

to_numeric_df <- function(df, cols) {
  for (c in cols) df[[c]] <- as.numeric(df[[c]])
  df
}

enough_rows <- function(n, min_n = 2) {
  !is.na(n) && is.finite(n) && n >= min_n
}

choose_nfolds <- function(n) {
  if (is.na(n) || n < 2) return(2)
  pmin(5, n)
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

################################################################################
# INITIALIZE MODEL REGISTRY IN BIGQUERY
################################################################################
create_log_entry("Initializing model registry tables")
tryCatch({
  ds <- bq_dataset(project, dataset)
  if (!bq_dataset_exists(ds)) bq_dataset_create(ds, location = location)
  
  # Registry tables
  registry_tables <- list(
    models = "
      CREATE TABLE IF NOT EXISTS `{project}.{dataset}.registry_models` (
        model_id STRING,
        model_name STRING,
        team STRING,
        owner STRING,
        description STRING,
        created_at TIMESTAMP
      )",
    versions = "
      CREATE TABLE IF NOT EXISTS `{project}.{dataset}.registry_versions` (
        model_id STRING,
        version_id STRING,
        created_at TIMESTAMP,
        created_by STRING,
        artifact_uri STRING,
        artifact_sha256 STRING,
        framework STRING,
        r_version STRING,
        package_info STRING,
        notes STRING
      )",
    metrics = "
      CREATE TABLE IF NOT EXISTS `{project}.{dataset}.registry_metrics` (
        model_id STRING,
        version_id STRING,
        metric_name STRING,
        metric_value FLOAT64,
        split STRING,
        logged_at TIMESTAMP
      )",
    stages = "
      CREATE TABLE IF NOT EXISTS `{project}.{dataset}.registry_stages` (
        model_id STRING,
        version_id STRING,
        stage STRING,
        set_at TIMESTAMP,
        set_by STRING,
        reason STRING
      )"
  )
  
  for (tbl_name in names(registry_tables)) {
    sql <- glue(registry_tables[[tbl_name]])
    bq_project_query(project, sql)
    create_log_entry(glue("Registry table created/verified: registry_{tbl_name}"))
  }
  
}, error = function(e) {
  create_log_entry(paste("Registry initialization failed:", e$message), "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
})

################################################################################
# LOAD ROSTER MAPPING FROM BIGQUERY
################################################################################
create_log_entry("Loading roster mapping from BigQuery")
roster_map <- tryCatch({
  sql <- glue("SELECT offical_id, kinexon_name, vald_name FROM `{project}.{dataset}.{roster_table}`")
  bq_table_download(bq_project_query(project, sql)) %>%
    mutate(
      offical_id = as.character(offical_id),
      kinexon_name = normalize_key(kinexon_name),
      vald_name = normalize_key(vald_name)
    )
}, error = function(e) {
  create_log_entry(paste("Roster mapping load failed:", e$message), "WARN")
  tibble(offical_id = character(), kinexon_name = character(), vald_name = character())
})
create_log_entry(glue("Roster map loaded: {nrow(roster_map)} rows"))

################################################################################
# LOAD WORKLOAD DATA FROM BIGQUERY
################################################################################
create_log_entry("Loading workload data from BigQuery")
workload_data <- tryCatch({
  sql <- glue("
    SELECT 
      roster_name, date,
      distance, high_speed_distance, mechanical_load,
      distance_7d, distance_28d, distance_monotony_7d,
      hsd_7d, hsd_28d,
      ml_7d, ml_28d, ml_monotony_7d
    FROM `{project}.{dataset}.{workload_table}`
    WHERE date BETWEEN '{cfg_start_date}' AND '{cfg_end_date}'
  ")
  result <- bq_table_download(bq_project_query(project, sql))
  result %>%
    mutate(
      date = as.Date(date),
      roster_name_norm = normalize_key(roster_name)
    ) %>%
    left_join(
      roster_map %>% select(offical_id, kinexon_name),
      by = c("roster_name_norm" = "kinexon_name")
    ) %>%
    filter(!is.na(offical_id)) %>%
    select(-roster_name_norm)
}, error = function(e) {
  create_log_entry(paste("Workload data load failed:", e$message), "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
})
create_log_entry(glue("Workload data loaded: {nrow(workload_data)} rows"))

################################################################################
# LOAD READINESS DATA FROM BIGQUERY
################################################################################
create_log_entry("Loading readiness data from BigQuery")
readiness_data <- tryCatch({
  sql <- glue("
    SELECT 
      date, vald_name, readiness
    FROM `{project}.{dataset}.{readiness_table}`
    WHERE date BETWEEN '{cfg_start_date}' AND '{cfg_end_date}'
      AND readiness IS NOT NULL
  ")
  result <- bq_table_download(bq_project_query(project, sql))
  result %>%
    mutate(
      date = as.Date(date),
      vald_name_norm = normalize_key(vald_name),
      readiness = as.numeric(readiness)
    ) %>%
    left_join(
      roster_map %>% select(offical_id, vald_name),
      by = c("vald_name_norm" = "vald_name")
    ) %>%
    filter(!is.na(offical_id), !is.na(readiness)) %>%
    select(offical_id, date, readiness)
}, error = function(e) {
  create_log_entry(paste("Readiness data load failed:", e$message), "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
})
create_log_entry(glue("Readiness data loaded: {nrow(readiness_data)} rows"))

################################################################################
# MERGE WORKLOAD + READINESS
################################################################################
create_log_entry("Merging workload and readiness data")
data_joined <- workload_data %>%
  left_join(readiness_data, by = c("offical_id", "date")) %>%
  filter(!is.na(readiness))

create_log_entry(glue("Joined data: {nrow(data_joined)} rows with readiness"))

if (nrow(data_joined) == 0) {
  create_log_entry("No data after join - exiting", "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
}

################################################################################
# CREATE TRAIN/TEST SPLIT (80/20 chronological per athlete)
################################################################################
create_log_entry("Creating train/test splits")

assign_split <- function(df, frac_train = 0.80) {
  df <- df %>% arrange(date)
  n <- nrow(df)
  if (n == 0) { df$is_test <- NA_integer_; return(df) }
  k <- max(1L, floor(n * frac_train))
  df$is_test <- c(rep(1L, k), rep(2L, n - k))
  df
}

data_split <- data_joined %>%
  group_by(offical_id) %>%
  group_modify(~assign_split(.x, cfg_frac_train)) %>%
  ungroup()

################################################################################
# PREPARE MODELING TABLE
################################################################################
response_var <- "readiness"
all_predictors <- c(
  "distance", "high_speed_distance", "mechanical_load",
  "distance_7d", "hsd_7d", "ml_7d",
  "distance_28d", "hsd_28d", "ml_28d",
  "distance_monotony_7d", "ml_monotony_7d"
)

# Rename columns to match model expectations
data_model <- data_split %>%
  transmute(
    offical_id,
    roster_name,
    date,
    is_test,
    distance = distance,
    hsd = high_speed_distance,
    ml = mechanical_load,
    distance_7_day = distance_7d,
    hsd_7_day = hsd_7d,
    ml_7_day = ml_7d,
    distance_28_day = distance_28d,
    hsd_28_day = hsd_28d,
    ml_28_day = ml_28d,
    distance_montony = distance_monotony_7d,
    ml_montony = ml_monotony_7d,
    readiness = readiness
  )

# Update predictor names to match
all_predictors <- c(
  "distance", "hsd", "ml",
  "distance_7_day", "hsd_7_day", "ml_7_day",
  "distance_28_day", "hsd_28_day", "ml_28_day",
  "distance_montony", "ml_montony"
)

data_clean <- as.data.table(data_model)

create_log_entry(glue("Model input: {nrow(data_clean)} rows, {length(unique(data_clean$offical_id))} athletes"))

################################################################################
# MODEL HYPERPARAMETERS
################################################################################
lambda_grid    <- exp(seq(log(1e-4), log(100), length.out = 100))
lambda_grid_en <- exp(seq(log(1e-4), log(100), length.out = 50))

################################################################################
# MODEL TRAINING FUNCTIONS
################################################################################
run_glmnet_cvfit <- function(train_df, preds, alpha_glmnet, lambda_grid, fit_intercept, normalization) {
  mm_cols <- c(response_var, preds)
  tr_cc <- train_df[stats::complete.cases(train_df[, mm_cols, drop = FALSE]), , drop = FALSE]
  if (nrow(tr_cc) < 2) stop("Insufficient complete cases")
  
  if (normalization) {
    fit_sc <- safe_scale_fit(tr_cc, preds)
    Xtr <- fit_sc$X
    sc  <- fit_sc
  } else {
    Xtr <- as.matrix(tr_cc[, preds, drop = FALSE])
    sc  <- list(center = NULL, scale = NULL, cols = preds)
  }
  y <- as.numeric(tr_cc[[response_var]])
  
  nfolds_used <- choose_nfolds(nrow(tr_cc))
  cv <- suppressWarnings(glmnet::cv.glmnet(
    Xtr, y, alpha = alpha_glmnet, lambda = lambda_grid,
    intercept = fit_intercept, standardize = FALSE,
    nfolds = nfolds_used, grouped = FALSE
  ))
  list(cv = cv, scaler = sc, Xtr = Xtr, y = y, train_cc = tr_cc)
}

run_linear <- function(train_df, test_df, preds, val_method) {
  best <- NULL
  for (fi in c(TRUE, FALSE)) for (sc in c(TRUE, FALSE)) {
    suppressWarnings({
      rhs <- paste(preds, collapse = " + ")
      form <- as.formula(paste(response_var, "~", if (fi) rhs else paste("0 +", rhs)))
      
      tr_df <- to_numeric_df(train_df, preds)
      te_df <- if (nrow(test_df) > 0) to_numeric_df(test_df, preds) else test_df
      
      mm_cols <- c(response_var, preds)
      tr_cc <- tr_df[stats::complete.cases(tr_df[, mm_cols, drop = FALSE]), , drop = FALSE]
      if (!enough_rows(nrow(tr_cc), 2L)) stop("Insufficient rows")
      
      if (sc) {
        fit_sc <- safe_scale_fit(tr_cc, preds)
        tr_fit <- tr_cc; tr_fit[, preds] <- fit_sc$X
        m  <- stats::lm(form, data = tr_fit, singular.ok = TRUE)
      } else {
        fit_sc <- NULL
        m <- stats::lm(form, data = tr_cc)
        tr_fit <- tr_cc
      }
      
      train_rmse_val <- rmse(tr_fit[[response_var]], stats::predict(m, tr_fit))
      
      # Simple CV estimate
      k <- choose_nfolds(nrow(tr_cc))
      folds <- sample(rep(1:k, length.out = nrow(tr_cc)))
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
            te_fit <- te_cc; te_fit[, preds] <- te_scaled
            test_rmse_val <- rmse(te_fit[[response_var]], stats::predict(m, te_fit))
          } else {
            test_rmse_val <- rmse(te_cc[[response_var]], stats::predict(m, te_cc))
          }
        } else test_rmse_val <- NA_real_
        primary <- test_rmse_val
      } else {
        test_rmse_val <- NA_real_
        primary <- cv_val
      }
      
      cand <- list(
        model_type = "linear",
        train_rmse = train_rmse_val, cv_rmse = cv_val, test_rmse = test_rmse_val, primary_rmse = primary,
        hyper = sprintf("fit_intercept=%s; normalization=%s", fi, sc),
        fitted = list(
          type = "lm", model = m, predictors = preds,
          preprocessing = list(normalization = sc,
                               center = if (sc) fit_sc$center else NULL,
                               scale  = if (sc) fit_sc$scale  else NULL,
                               cols   = if (sc) fit_sc$cols   else preds,
                               fit_intercept = fi)
        )
      )
      if (is.null(best) || (is.finite(cand$primary_rmse) && cand$primary_rmse < best$primary_rmse)) best <- cand
    })
  }
  best
}

run_ridge <- function(train_df, test_df, preds, val_method) {
  best <- NULL
  for (fi in c(TRUE,FALSE)) for (sc_norm in c(TRUE,FALSE)) {
    fit <- run_glmnet_cvfit(train_df, preds, 0, lambda_grid, fi, sc_norm)
    train_rmse_val <- rmse(fit$y, as.numeric(predict(fit$cv, fit$Xtr, s="lambda.min")))
    cv_rmse_val    <- sqrt(min(fit$cv$cvm))
    
    if (val_method == "test_set" && nrow(test_df) > 0) {
      te_df <- to_numeric_df(test_df, preds)
      te_cc <- te_df[stats::complete.cases(te_df[, preds, drop = FALSE]), , drop = FALSE]
      if (nrow(te_cc) > 0) {
        Xn <- if (sc_norm) safe_scale_apply(te_cc, fit$scaler) else as.matrix(te_cc[, preds, drop = FALSE])
        test_rmse_val <- rmse(te_cc[[response_var]], as.numeric(predict(fit$cv, Xn, s="lambda.min")))
      } else test_rmse_val <- NA_real_
      primary <- test_rmse_val
    } else {
      test_rmse_val <- NA_real_
      primary <- cv_rmse_val
    }
    
    cand <- list(
      model_type = "ridge",
      train_rmse = train_rmse_val, cv_rmse = cv_rmse_val, test_rmse = test_rmse_val, primary_rmse = primary,
      hyper = sprintf("lambda=%.6f; fit_intercept=%s; normalization=%s", fit$cv$lambda.min, fi, sc_norm),
      fitted = list(
        type="glmnet", model=fit$cv, predictors=preds,
        preprocessing = list(normalization=sc_norm,
                             center=if (sc_norm) fit$scaler$center else NULL,
                             scale =if (sc_norm) fit$scaler$scale  else NULL,
                             cols  =fit$scaler$cols,
                             fit_intercept=fi),
        lambda_min=fit$cv$lambda.min, alpha=0
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
    train_rmse_val <- rmse(fit$y, as.numeric(predict(fit$cv, fit$Xtr, s="lambda.min")))
    cv_rmse_val    <- sqrt(min(fit$cv$cvm))
    
    if (val_method == "test_set" && nrow(test_df) > 0) {
      te_df <- to_numeric_df(test_df, preds)
      te_cc <- te_df[stats::complete.cases(te_df[, preds, drop = FALSE]), , drop = FALSE]
      if (nrow(te_cc) > 0) {
        Xn <- if (sc_norm) safe_scale_apply(te_cc, fit$scaler) else as.matrix(te_cc[, preds, drop = FALSE])
        test_rmse_val <- rmse(te_cc[[response_var]], as.numeric(predict(fit$cv, Xn, s="lambda.min")))
      } else test_rmse_val <- NA_real_
      primary <- test_rmse_val
    } else {
      test_rmse_val <- NA_real_
      primary <- cv_rmse_val
    }
    
    cand <- list(
      model_type = "lasso",
      train_rmse = train_rmse_val, cv_rmse = cv_rmse_val, test_rmse = test_rmse_val, primary_rmse = primary,
      hyper = sprintf("lambda=%.6f; fit_intercept=%s; normalization=%s", fit$cv$lambda.min, fi, sc_norm),
      fitted = list(
        type="glmnet", model=fit$cv, predictors=preds,
        preprocessing = list(normalization=sc_norm,
                             center=if (sc_norm) fit$scaler$center else NULL,
                             scale =if (sc_norm) fit$scaler$scale  else NULL,
                             cols  =fit$scaler$cols,
                             fit_intercept=fi),
        lambda_min=fit$cv$lambda.min, alpha=1
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
    train_rmse_val <- rmse(fit$y, as.numeric(predict(fit$cv, fit$Xtr, s="lambda.min")))
    cv_rmse_val    <- sqrt(min(fit$cv$cvm))
    
    if (val_method == "test_set" && nrow(test_df) > 0) {
      te_df <- to_numeric_df(test_df, preds)
      te_cc <- te_df[stats::complete.cases(te_df[, preds, drop = FALSE]), , drop = FALSE]
      if (nrow(te_cc) > 0) {
        Xn <- if (sc_norm) safe_scale_apply(te_cc, fit$scaler) else as.matrix(te_cc[, preds, drop = FALSE])
        test_rmse_val <- rmse(te_cc[[response_var]], as.numeric(predict(fit$cv, Xn, s="lambda.min")))
      } else test_rmse_val <- NA_real_
      primary <- test_rmse_val
    } else {
      test_rmse_val <- NA_real_
      primary <- cv_rmse_val
    }
    
    cand <- list(
      model_type = "elastic_net",
      train_rmse = train_rmse_val, cv_rmse = cv_rmse_val, test_rmse = test_rmse_val, primary_rmse = primary,
      hyper = sprintf("lambda=%.6f; l1_ratio=%.1f; fit_intercept=%s; normalization=%s", fit$cv$lambda.min, ai, fi, sc_norm),
      fitted = list(
        type="glmnet", model=fit$cv, predictors=preds,
        preprocessing = list(normalization=sc_norm,
                             center=if (sc_norm) fit$scaler$center else NULL,
                             scale =if (sc_norm) fit$scaler$scale  else NULL,
                             cols  =fit$scaler$cols,
                             fit_intercept=fi),
        lambda_min=fit$cv$lambda.min, alpha=ai
      )
    )
    if (is.null(best) || (is.finite(cand$primary_rmse) && cand$primary_rmse < best$primary_rmse)) best <- cand
  }
  best
}

run_bayes_reg <- function(train_df, test_df, preds, val_method) {
  if (!reticulate::py_module_available("sklearn")) {
    return(list(model_type = "bayesian_regression", train_rmse = Inf, cv_rmse = Inf, 
                test_rmse = Inf, primary_rmse = Inf,
                hyper = "sklearn not available", fitted = list(type = "error")))
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
    folds <- sample(rep(1:k, length.out = nrow(tr_cc))); cv_vals <- numeric(k)
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
    } else {
      test_rmse_val <- NA_real_
      primary <- cv_rmse_val
    }
    
    coef_vec <- as.numeric(reticulate::py_to_r(m$coef_))
    intercept_val <- as.numeric(reticulate::py_to_r(m$intercept_))
    
    cand <- list(
      model_type = "bayesian_regression",
      train_rmse = train_rmse_val, cv_rmse = cv_rmse_val, test_rmse = test_rmse_val, primary_rmse = primary,
      hyper = sprintf("alpha_1=%.0e; lambda_1=%.0e", a1, l1),
      fitted = list(
        type="bayesridge_params",
        predictors = preds,
        preprocessing = list(normalization=TRUE, center=fit_sc$center, scale=fit_sc$scale, 
                           cols=fit_sc$cols, fit_intercept=TRUE),
        coefficients = coef_vec, intercept = intercept_val,
        priors = list(alpha_1=a1, alpha_2=a1, lambda_1=l1, lambda_2=l1)
      )
    )
    if (is.null(best) || (is.finite(cand$primary_rmse) && cand$primary_rmse < best$primary_rmse)) best <- cand
  }
  best
}

run_bayes_net <- function(train_df, test_df, preds, val_method) {
  best <- NULL
  cols <- c(response_var, preds)
  
  tr_df <- train_df[, cols, drop = FALSE]
  for (cl in cols) if (!is.numeric(tr_df[[cl]])) tr_df[[cl]] <- as.numeric(tr_df[[cl]])
  tr_df <- tr_df[stats::complete.cases(tr_df), , drop = FALSE]
  if (nrow(tr_df) < 3) return(list(model_type = "bayesian_network", train_rmse = Inf, 
                                   cv_rmse = Inf, test_rmse = Inf, primary_rmse = Inf,
                                   hyper = "insufficient data", fitted = list(type = "error")))
  
  if (nrow(test_df) > 0) {
    te_df <- test_df[, cols, drop = FALSE]
    for (cl in cols) if (!is.numeric(te_df[[cl]])) te_df[[cl]] <- as.numeric(te_df[[cl]])
    te_df <- te_df[stats::complete.cases(te_df), , drop = FALSE]
  } else te_df <- data.frame()
  
  for (alg in c("hc","tabu")) {
    net <- if (alg == "hc") bnlearn::hc(tr_df, score = "bic-g") else bnlearn::tabu(tr_df, score = "bic-g")
    fit <- bnlearn::bn.fit(net, data = tr_df, method = "mle-g")
    
    pr_tr <- as.numeric(predict(fit, node = response_var, data = tr_df[, preds, drop = FALSE]))
    train_rmse_val <- rmse(tr_df[[response_var]], pr_tr)
    
    k <- choose_nfolds(nrow(tr_df))
    folds <- sample(rep(seq_len(k), length.out = nrow(tr_df)))
    cv_vals <- numeric(k)
    for (f in seq_len(k)) {
      tr_ix <- folds != f; te_ix <- folds == f
      if (sum(tr_ix) < 3 || sum(te_ix) < 1) { cv_vals[f] <- NA_real_; next }
      net_cv <- if (alg == "hc") bnlearn::hc(tr_df[tr_ix, ], score = "bic-g") else bnlearn::tabu(tr_df[tr_ix, ], score = "bic-g")
      fit_cv <- bnlearn::bn.fit(net_cv, data = tr_df[tr_ix, ], method = "mle-g")
      pr_cv  <- as.numeric(predict(fit_cv, node = response_var, data = tr_df[te_ix, preds, drop = FALSE]))
      cv_vals[f] <- rmse(tr_df[[response_var]][te_ix], pr_cv)
    }
    cv_rmse_val <- mean(cv_vals, na.rm = TRUE)
    
    if (val_method == "test_set" && nrow(te_df) > 0) {
      pr_te <- as.numeric(predict(fit, node = response_var, data = te_df[, preds, drop = FALSE]))
      test_rmse_val <- rmse(te_df[[response_var]], pr_te)
      primary <- test_rmse_val
    } else {
      test_rmse_val <- NA_real_
      primary <- cv_rmse_val
    }
    
    cand <- list(
      model_type = "bayesian_network",
      train_rmse = train_rmse_val, cv_rmse = cv_rmse_val, test_rmse = test_rmse_val, primary_rmse = primary,
      hyper = sprintf("structure=%s; param=mle", alg),
      fitted = list(
        type="bnlearn", model=fit, predictors=preds,
        preprocessing = list(normalization=FALSE, center=NULL, scale=NULL, cols=preds),
        structure=alg, param_method="mle"
      )
    )
    if (is.null(best) || (is.finite(cand$primary_rmse) && cand$primary_rmse < best$primary_rmse)) best <- cand
  }
  best
}

################################################################################
# SAVE MODEL TO GCS + BIGQUERY REGISTRY
################################################################################
save_model_to_registry <- function(athlete_id, model_name, candidate, timestamp) {
  if (is.null(candidate$fitted) || candidate$fitted$type == "error") {
    return(list(success = FALSE, model_id = NA, version_id = NA))
  }
  
  tryCatch({
    # Generate IDs
    model_id <- paste(team_name, model_name, sep = ":")
    version_id <- timestamp
    
    # Save model locally
    dir.create("artifact", showWarnings = FALSE)
    model_save <- list(
      model = candidate$fitted$model,
      type = candidate$model_type,
      predictors = candidate$fitted$predictors,
      preprocessing = candidate$fitted$preprocessing,
      date_created = Sys.time(),
      athlete_id = athlete_id
    )
    saveRDS(model_save, "artifact/model.rds")
    
    # Calculate checksum
    sha <- digest::digest(file = "artifact/model.rds", algo = "sha256")
    
    # Create manifest
    manifest <- list(
      model_name = model_name,
      version_id = version_id,
      created_at = as.character(Sys.time()),
      r_version = R.version$version.string,
      checksum_sha256 = sha,
      hyperparameters = candidate$hyper
    )
    jsonlite::write_json(manifest, "artifact/manifest.json", auto_unbox = TRUE, pretty = TRUE)
    
    # Upload to GCS
    safe_model_name <- gsub("[^A-Za-z0-9]+", "_", model_name)
    gcs_path <- sprintf("models/%s/%s/%s/", team_name, safe_model_name, version_id)
    
    gcs_upload("artifact/model.rds", name = paste0(gcs_path, "model.rds"))
    gcs_upload("artifact/manifest.json", name = paste0(gcs_path, "manifest.json"))
    
    artifact_uri <- sprintf("gs://%s/%smodel.rds", gcs_bucket, gcs_path)
    
    # Insert into registry tables
    con <- DBI::dbConnect(bigrquery::bigquery(), project = project, dataset = dataset)
    
    # Upsert model
    DBI::dbExecute(con, glue("
      MERGE `{project}.{dataset}.registry_models` T 
      USING (SELECT '{model_id}' AS model_id, '{model_name}' AS model_name,
                    '{team_name}' AS team, 'auto' AS owner, 
                    'Athlete readiness model' AS description,
                    CURRENT_TIMESTAMP() AS created_at) S
      ON T.model_id = S.model_id
      WHEN NOT MATCHED THEN INSERT ROW
    "))
    
    # Insert version
    DBI::dbExecute(con, glue("
      INSERT INTO `{project}.{dataset}.registry_versions`
        (model_id, version_id, created_at, created_by, artifact_uri, artifact_sha256,
         framework, r_version, package_info, notes)
      VALUES
        ('{model_id}','{version_id}',CURRENT_TIMESTAMP(),'github_actions',
         '{artifact_uri}','{sha}','R/glmnet','{R.version$version.string}','','')
    "))
    
    # Insert metrics
    metrics <- tibble(
      model_id = model_id,
      version_id = version_id,
      metric_name = c("train_rmse", "cv_rmse", "test_rmse", "primary_rmse"),
      metric_value = c(candidate$train_rmse, candidate$cv_rmse, 
                      candidate$test_rmse, candidate$primary_rmse),
      split = c("train", "cv", "test", "primary"),
      logged_at = Sys.time()
    )
    bq_table_upload(
      bq_table(project, dataset, "registry_metrics"),
      metrics,
      write_disposition = "WRITE_APPEND"
    )
    
    # Set stage to Staging
    DBI::dbExecute(con, glue("
      INSERT INTO `{project}.{dataset}.registry_stages`
        (model_id, version_id, stage, set_at, set_by, reason)
      VALUES
        ('{model_id}','{version_id}','Staging',CURRENT_TIMESTAMP(),'auto','Initial training')
    "))
    
    DBI::dbDisconnect(con)
    
    # Cleanup
    unlink("artifact", recursive = TRUE)
    
    list(success = TRUE, model_id = model_id, version_id = version_id, artifact_uri = artifact_uri)
    
  }, error = function(e) {
    create_log_entry(paste("Model save failed for", model_name, ":", e$message), "ERROR")
    list(success = FALSE, model_id = NA, version_id = NA, error = e$message)
  })
}

################################################################################
# TRAIN MODELS FOR ALL ATHLETES
################################################################################
create_log_entry("Starting model training loop")

athletes <- unique(na.omit(data_clean$offical_id))
n_athletes <- length(athletes)
create_log_entry(glue("Training models for {n_athletes} athletes"))

timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
all_results <- list()
all_predictions <- list()
success_count <- 0
fail_count <- 0

for (i in seq_len(n_athletes)) {
  athlete_id <- athletes[i]
  roster_name <- data_clean[offical_id == athlete_id]$roster_name[1]
  
  create_log_entry(glue("[{i}/{n_athletes}] Training models for {roster_name} ({athlete_id})"))
  
  tryCatch({
    ath_data  <- data_clean[offical_id == athlete_id]
    ath_train <- ath_data[is_test == 1]
    ath_test  <- ath_data[is_test == 2]
    
    if (nrow(ath_train) < min_obs_train) {
      create_log_entry(glue("  SKIP: insufficient training rows ({nrow(ath_train)})"), "WARN")
      fail_count <- fail_count + 1
      next
    }
    
    preds <- intersect(all_predictors, names(ath_train))
    if (length(preds) == 0) {
      create_log_entry("  SKIP: no predictors available", "WARN")
      fail_count <- fail_count + 1
      next
    }
    
    ath_train_df <- as.data.frame(ath_train[, c(response_var, preds), with=FALSE])
    ath_test_df  <- as.data.frame(ath_test[, c(response_var, preds), with=FALSE])
    
    ath_train_df <- to_numeric_df(ath_train_df, preds)
    if (nrow(ath_test_df) > 0) ath_test_df <- to_numeric_df(ath_test_df, preds)
    
    imp <- impute_train_test(ath_train_df, ath_test_df, preds)
    ath_train_df <- imp$train; ath_test_df <- imp$test
    
    sds <- sapply(ath_train_df[, preds, drop=FALSE], stats::sd, na.rm=TRUE)
    keep <- names(sds)[is.finite(sds) & sds > 0]
    if (length(keep) < 1) {
      create_log_entry("  SKIP: no predictor variance", "WARN")
      fail_count <- fail_count + 1
      next
    }
    preds <- keep
    
    n_tr <- nrow(ath_train_df); n_te <- nrow(ath_test_df)
    val_method <- if (n_te >= 5 && n_tr/(n_tr+n_te) >= 0.7) "test_set" else "cv"
    if (val_method == "cv") ath_test_df <- data.frame()
    
    create_log_entry(glue("  Training: {n_tr} train, {n_te} test, method={val_method}"))
    
    # Train 6 model types
    cands <- list(
      safe_runner("linear",              run_linear,    ath_train_df, ath_test_df, preds, val_method),
      safe_runner("ridge",               run_ridge,     ath_train_df, ath_test_df, preds, val_method),
      safe_runner("lasso",               run_lasso,     ath_train_df, ath_test_df, preds, val_method),
      safe_runner("elastic_net",         run_elastic,   ath_train_df, ath_test_df, preds, val_method),
      safe_runner("bayesian_regression", run_bayes_reg, ath_train_df, ath_test_df, preds, val_method),
      safe_runner("bayesian_network",    run_bayes_net, ath_train_df, ath_test_df, preds, val_method)
    )
    
    # Find best model
    prim_rmse <- sapply(cands, function(c) c$primary_rmse)
    finite_idx <- which(is.finite(prim_rmse))
    if (length(finite_idx) == 0) {
      create_log_entry("  ERROR: all models failed", "ERROR")
      fail_count <- fail_count + 1
      next
    }
    best_idx <- finite_idx[which.min(prim_rmse[finite_idx])]
    best_cand <- cands[[best_idx]]
    
    # Save best model
    model_name <- paste(athlete_id, best_cand$model_type, sep = "_")
    save_result <- save_model_to_registry(athlete_id, model_name, best_cand, timestamp)
    
    if (save_result$success) {
      create_log_entry(glue("  SUCCESS: {best_cand$model_type} (RMSE={round(best_cand$primary_rmse, 3)})"))
      success_count <- success_count + 1
      
      # Store results
      all_results[[length(all_results) + 1]] <- tibble(
        athlete_id = athlete_id,
        roster_name = roster_name,
        model_type = best_cand$model_type,
        model_id = save_result$model_id,
        version_id = save_result$version_id,
        artifact_uri = save_result$artifact_uri,
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
      
      # Generate predictions on all data
      all_data <- rbind(ath_train_df, ath_test_df)
      if (nrow(all_data) > 0) {
        preds_model <- tryCatch({
          if (best_cand$fitted$type == "lm") {
            predict(best_cand$fitted$model, all_data)
          } else if (best_cand$fitted$type == "glmnet") {
            if (best_cand$fitted$preprocessing$normalization) {
              X_scaled <- safe_scale_apply(all_data, best_cand$fitted$preprocessing)
              as.numeric(predict(best_cand$fitted$model, X_scaled, s="lambda.min"))
            } else {
              X <- as.matrix(all_data[, preds, drop=FALSE])
              as.numeric(predict(best_cand$fitted$model, X, s="lambda.min"))
            }
          } else if (best_cand$fitted$type == "bnlearn") {
            as.numeric(predict(best_cand$fitted$model, node = response_var, 
                             data = all_data[, preds, drop=FALSE]))
          } else {
            rep(NA_real_, nrow(all_data))
          }
        }, error = function(e) {
          create_log_entry(glue("  Prediction generation failed: {e$message}"), "WARN")
          rep(NA_real_, nrow(all_data))
        })
        
        pred_df <- bind_rows(
          ath_train[, .(offical_id, roster_name, date, readiness, is_test)],
          ath_test[, .(offical_id, roster_name, date, readiness, is_test)]
        ) %>%
          mutate(
            predicted_readiness = preds_model,
            model_id = save_result$model_id,
            version_id = save_result$version_id,
            prediction_date = Sys.time()
          )
        
        all_predictions[[length(all_predictions) + 1]] <- pred_df
      }
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

# Save model summary
if (length(all_results) > 0) {
  results_df <- bind_rows(all_results)
  tbl <- bq_table(project, dataset, "model_training_summary")
  
  if (!bq_table_exists(tbl)) {
    bq_table_create(tbl, fields = as_bq_fields(results_df),
                   time_partitioning = list(type="DAY", field="trained_at"))
  }
  
  bq_table_upload(tbl, results_df, write_disposition = "WRITE_APPEND")
  create_log_entry(glue("Model training summary saved: {nrow(results_df)} rows"))
}

# Save predictions
if (length(all_predictions) > 0) {
  preds_df <- bind_rows(all_predictions)
  tbl <- bq_table(project, dataset, predictions_table)
  
  if (!bq_table_exists(tbl)) {
    bq_table_create(tbl, fields = as_bq_fields(preds_df),
                   time_partitioning = list(type="DAY", field="date"),
                   clustering_fields = c("offical_id", "model_id"))
  }
  
  # Truncate and reload (or implement merge logic if needed)
  bq_table_upload(tbl, preds_df, write_disposition = "WRITE_TRUNCATE")
  create_log_entry(glue("Predictions saved: {nrow(preds_df)} rows"))
}

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
