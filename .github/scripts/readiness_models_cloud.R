#!/usr/bin/env Rscript
# ===== Packages =====
tryCatch({
  suppressPackageStartupMessages({
    library(bigrquery); library(DBI)
    library(dplyr); library(tidyr); library(data.table)
    library(lubridate); library(slider); library(stringr)
    library(gargle); library(glue); library(digest)
    library(glmnet); library(bnlearn); library(reticulate)
    library(jsonlite)
  })
}, error = function(e) { 
  cat("Error loading packages:", conditionMessage(e), "\n"); quit(status=1)
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
skip_training          <- tolower(Sys.getenv("SKIP_TRAINING", "false")) %in% c("1","true","yes")
skip_registry_versions <- tolower(Sys.getenv("SKIP_REGISTRY_VERSIONS", "false")) %in% c("1","true","yes")
has_matches_hint       <- Sys.getenv("HAS_MATCHES", "")
match_lookback_days    <- as.integer(Sys.getenv("MATCH_LOOKBACK_DAYS", "7"))

cat("=== CONFIGURATION ===\n")
cat("GCP Project:", project, "\n")
cat("BQ Dataset:", dataset, "\n")
cat("Team:", team_name, "\n")
cat("Workload table:", workload_table, "\n")
cat("Readiness table:", readiness_table, "\n")
cat("Roster table:", roster_table, "\n")
cat("Predictions table:", predictions_table, "\n")
cat("Date range:", format(cfg_start_date, "%m/%d/%Y"), "to", format(cfg_end_date, "%m/%d/%Y"), "\n")
cat("Train fraction:", cfg_frac_train, "\n")
cat("Min train obs:", MIN_TRAIN_OBS, "\n")
cat("Skip training:", skip_training, "\n")
cat("Skip registry versions:", skip_registry_versions, "\n")
cat("HAS_MATCHES hint:", ifelse(nzchar(has_matches_hint), has_matches_hint, "not provided"), "\n")
cat("Match lookback days:", match_lookback_days, "\n")
cat("Storage: BigQuery tables only\n\n")

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
  }, error=function(e){ cat("Log upload failed:", conditionMessage(e), "\n"); FALSE })
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
  create_log_entry("Authentication successful")
}, error = function(e) {
  create_log_entry(paste("Authentication failed:", conditionMessage(e)), "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
})

################################################################################
# HELPER FUNCTIONS
################################################################################

# Safe accessor for nested list/object fields to avoid "$ operator is invalid for atomic vectors" error
safe_get_field <- function(obj, field_path) {
  # field_path is a character vector like c("jobReference", "jobId")
  # Returns NULL if any level is not a list or doesn't exist
  tryCatch({
    result <- obj
    for (field in field_path) {
      if (!is.list(result) || is.null(result[[field]])) {
        return(NULL)
      }
      result <- result[[field]]
    }
    result
  }, error = function(e) {
    NULL
  })
}

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

# Helper to fetch and log BigQuery job details by job ID
fetch_and_log_bq_job_details <- function(job_id_full, operation_name = "BigQuery operation") {
  tryCatch({
    if (is.null(job_id_full) || !nzchar(job_id_full)) {
      create_log_entry("  ⚠️  No job ID provided to fetch details", "WARN")
      return(FALSE)
    }
    
    create_log_entry(sprintf("  🔍 Fetching BigQuery job details for: %s", job_id_full), "INFO")
    
    # Try to get job details using bq command line tool
    cmd <- sprintf("bq --format=json show -j %s:%s 2>&1", project, job_id_full)
    result <- tryCatch({
      system(cmd, intern = TRUE)
    }, error = function(e) {
      create_log_entry(sprintf("  ⚠️  Failed to run bq command: %s", conditionMessage(e)), "WARN")
      return(NULL)
    })
    
    if (!is.null(result) && length(result) > 0) {
      # Try to parse JSON response
      job_json <- tryCatch({
        jsonlite::fromJSON(paste(result, collapse = "\n"))
      }, error = function(e) {
        create_log_entry(sprintf("  ⚠️  Failed to parse job JSON: %s", conditionMessage(e)), "WARN")
        return(NULL)
      })
      
      if (!is.null(job_json)) {
        # Log job status using safe accessor
        job_state <- safe_get_field(job_json, c("status", "state"))
        if (!is.null(job_state)) {
          create_log_entry(sprintf("  📊 Job state: %s", job_state), "ERROR")
        }
        
        # Log error details if present
        error_result <- safe_get_field(job_json, c("status", "errorResult"))
        if (!is.null(error_result) && is.list(error_result)) {
          create_log_entry("  ❌ BigQuery Error Details:", "ERROR")
          create_log_entry(sprintf("     Reason: %s", error_result$reason %||% "unknown"), "ERROR")
          create_log_entry(sprintf("     Message: %s", error_result$message %||% "unknown"), "ERROR")
          if (!is.null(error_result$location)) {
            create_log_entry(sprintf("     Location: %s", error_result$location), "ERROR")
          }
        }
        
        # Log all errors if present
        errors_list <- safe_get_field(job_json, c("status", "errors"))
        if (!is.null(errors_list) && length(errors_list) > 0) {
          create_log_entry("  ❌ All job errors:", "ERROR")
          for (i in seq_along(errors_list)) {
            err <- errors_list[[i]]
            if (is.list(err)) {
              create_log_entry(sprintf("     [%d] %s: %s", i, err$reason %||% "unknown", err$message %||% "unknown"), "ERROR")
            }
          }
        }
        
        # Log job configuration for debugging schema issues
        configuration <- safe_get_field(job_json, c("configuration"))
        if (!is.null(configuration) && is.list(configuration)) {
          load_conf <- safe_get_field(configuration, c("load"))
          if (!is.null(load_conf) && is.list(load_conf)) {
            create_log_entry("  📋 Load job configuration:", "INFO")
            dest_table <- safe_get_field(load_conf, c("destinationTable"))
            if (!is.null(dest_table) && is.list(dest_table)) {
              create_log_entry(sprintf("     Destination: %s:%s.%s", 
                dest_table$projectId %||% "?",
                dest_table$datasetId %||% "?",
                dest_table$tableId %||% "?"), "INFO")
            }
            source_format <- safe_get_field(load_conf, c("sourceFormat"))
            if (!is.null(source_format)) {
              create_log_entry(sprintf("     Source format: %s", source_format), "INFO")
            }
            write_disp <- safe_get_field(load_conf, c("writeDisposition"))
            if (!is.null(write_disp)) {
              create_log_entry(sprintf("     Write disposition: %s", write_disp), "INFO")
            }
          }
        }
        
        return(TRUE)
      }
    }
    
    # If bq command failed, log the command for manual execution
    create_log_entry(sprintf("  🔍 Manual inspection: %s", cmd), "ERROR")
    return(FALSE)
    
  }, error = function(e) {
    create_log_entry(sprintf("  ⚠️  Error fetching job details: %s", conditionMessage(e)), "WARN")
    return(FALSE)
  })
}

# Helper to extract and log BigQuery job error details
log_bq_job_error <- function(job_or_error, operation_name = "BigQuery operation") {
  tryCatch({
    # Try to extract job information from the error or job object
    if (inherits(job_or_error, "error") || inherits(job_or_error, "simpleError")) {
      error_msg <- conditionMessage(job_or_error)
      create_log_entry(sprintf("  ❌ %s error: %s", operation_name, error_msg), "ERROR")
      
      # Try to parse job ID from error message if present
      if (grepl("job_[a-zA-Z0-9_-]+", error_msg)) {
        job_id_match <- regmatches(error_msg, regexpr("job_[a-zA-Z0-9_-]+\\.[A-Z]+", error_msg))
        if (length(job_id_match) > 0) {
          create_log_entry(sprintf("  📋 BigQuery Job ID: %s", job_id_match[1]), "ERROR")
          
          # Actively fetch job details from BigQuery
          fetch_and_log_bq_job_details(job_id_match[1], operation_name)
        }
      }
    } else if (is.list(job_or_error) && inherits(job_or_error, "bq_job")) {
      # If we have a job object, try to extract error details using safe accessors
      # Use safe_get_field to avoid "$ operator is invalid for atomic vectors" error
      job_id <- safe_get_field(job_or_error, c("jobReference", "jobId"))
      job_location <- safe_get_field(job_or_error, c("jobReference", "location"))
      
      if (!is.null(job_id)) {
        job_location <- job_location %||% location
        job_id_full <- paste0(job_id, ".", job_location)
        
        create_log_entry(sprintf("  📋 BigQuery Job ID: %s", job_id_full), "ERROR")
        
        # Actively fetch job details from BigQuery
        fetch_and_log_bq_job_details(job_id_full, operation_name)
      }
      
      error_result <- safe_get_field(job_or_error, c("status", "errorResult"))
      if (!is.null(error_result) && is.list(error_result)) {
        create_log_entry(sprintf("  ❌ Error reason: %s", error_result$reason %||% "unknown"), "ERROR")
        create_log_entry(sprintf("  ❌ Error message: %s", error_result$message %||% "unknown"), "ERROR")
      }
      
      errors_list <- safe_get_field(job_or_error, c("status", "errors"))
      if (!is.null(errors_list) && length(errors_list) > 0) {
        create_log_entry("  ❌ All errors:", "ERROR")
        for (i in seq_along(errors_list)) {
          err <- errors_list[[i]]
          if (is.list(err)) {
            create_log_entry(sprintf("    [%d] %s: %s", i, err$reason %||% "unknown", err$message %||% "unknown"), "ERROR")
          }
        }
      }
    }
  }, error = function(e) {
    # If we can't parse the error, just log that
    create_log_entry(sprintf("  ⚠️  Could not parse BigQuery error details: %s", conditionMessage(e)), "WARN")
  })
}

# Retry helper with exponential backoff and enhanced BigQuery error logging
retry_operation <- function(expr, max_attempts = MAX_RETRY_ATTEMPTS, 
                           wait_seconds = RETRY_WAIT_SECONDS, 
                           operation_name = "operation",
                           exponential_backoff = TRUE) {
  for (attempt in 1:max_attempts) {
    result <- tryCatch({
      force(expr)
      return(list(success = TRUE, result = expr))
    }, error = function(e) {
      if (attempt < max_attempts) {
        # Use exponential backoff: wait_seconds * 2^(attempt-1)
        wait_time <- if (exponential_backoff) {
          wait_seconds * (2 ^ (attempt - 1))
        } else {
          wait_seconds
        }
        
        create_log_entry(
          sprintf("%s failed (attempt %d/%d): %s. Retrying in %d seconds...", 
                  operation_name, attempt, max_attempts, conditionMessage(e), wait_time),
          "WARN",
          reason = "retry_transient_error"
        )
        
        # Log detailed BigQuery error if it's a BQ-related error
        if (grepl("bigquery|bq_|job_", conditionMessage(e), ignore.case = TRUE)) {
          log_bq_job_error(e, operation_name)
        }
        
        Sys.sleep(wait_time)
        NULL
      } else {
        create_log_entry(
          sprintf("%s failed after %d attempts: %s", 
                  operation_name, max_attempts, conditionMessage(e)),
          "ERROR",
          reason = "retry_exhausted"
        )
        
        # Log detailed BigQuery error on final failure
        if (grepl("bigquery|bq_|job_", conditionMessage(e), ignore.case = TRUE)) {
          log_bq_job_error(e, operation_name)
        }
        
        return(list(success = FALSE, error = conditionMessage(e)))
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
  stopifnot(all(fit[["cols"]] %in% names(df)))
  X <- as.matrix(df[, fit[["cols"]], drop = FALSE])
  s <- fit[["scale"]]; s[is.na(s) | s == 0] <- 1
  scale(X, center = fit[["center"]], scale = s)
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
# MODEL FLATTENING FUNCTIONS
# Save model coefficients to BigQuery in long format
################################################################################

#' Extract coefficients from glmnet models (elastic_net, lasso, ridge)
#' Returns a data frame in LONG format: one row per coefficient
flatten_glmnet_model <- function(model_obj, model_id, athlete_id, model_type) {
  tryCatch({
    # Unwrap model if nested
    fit <- model_obj
    if (is.list(fit) && !is.null(fit$model)) fit <- fit$model
    if (is.list(fit) && !is.null(fit$glmnet.fit)) fit <- fit$glmnet.fit
    if (is.list(fit) && !is.null(fit$finalModel)) fit <- fit$finalModel
    
    # Get lambda
    lambda <- if (inherits(fit, "cv.glmnet")) {
      fit$lambda.min  # or lambda.1se for more regularization
    } else if (!is.null(fit$lambda)) {
      fit$lambda[1]
    } else {
      NA_real_
    }
    
    # Get alpha (elastic net mixing parameter)
    alpha <- if (grepl("lasso", model_type, ignore.case = TRUE)) {
      1.0
    } else if (grepl("ridge", model_type, ignore.case = TRUE)) {
      0.0
    } else {
      # Try to extract from model
      tryCatch(as.numeric(fit$call$alpha), error = function(e) NA_real_)
    }
    
    # Extract coefficients
    coef_matrix <- as.matrix(coef(fit, s = lambda))
    coef_values <- as.numeric(coef_matrix[, 1])
    coef_names <- rownames(coef_matrix)
    
    # Create long format data frame
    coef_df <- tibble(
      model_id = model_id,
      athlete_id = athlete_id,
      model_type = model_type,
      coefficient_name = coef_names,
      coefficient_value = coef_values,
      coefficient_type = if_else(coefficient_name == "(Intercept)", "intercept", "predictor"),
      is_zero = abs(coefficient_value) < 1e-10,
      extracted_at = Sys.time()
    )
    
    # Add hyperparameters as special rows
    hyper_df <- tibble(
      model_id = model_id,
      athlete_id = athlete_id,
      model_type = model_type,
      coefficient_name = c("_lambda", "_alpha", "_n_features"),
      coefficient_value = c(lambda, alpha, sum(!coef_df$is_zero & coef_df$coefficient_type == "predictor")),
      coefficient_type = "hyperparameter",
      is_zero = FALSE,
      extracted_at = Sys.time()
    )
    
    bind_rows(coef_df, hyper_df)
    
  }, error = function(e) {
    create_log_entry(
      sprintf("Failed to flatten glmnet model %s: %s", model_id, conditionMessage(e)),
      "WARN"
    )
    NULL
  })
}

#' Extract structure from Bayesian Network models
flatten_bn_model <- function(model_obj, model_id, athlete_id, model_type) {
  tryCatch({
    # Unwrap model if nested
    bn_model <- model_obj
    if (is.list(bn_model) && !is.null(bn_model$model)) bn_model <- bn_model$model
    
    # Get nodes and edges
    nodes <- bnlearn::nodes(bn_model)
    arcs <- bnlearn::arcs(bn_model)
    
    # Create coefficient-like representation for BN structure
    # Each arc becomes a "coefficient" representing the edge
    if (nrow(arcs) > 0) {
      arc_df <- tibble(
        model_id = model_id,
        athlete_id = athlete_id,
        model_type = model_type,
        coefficient_name = paste0(arcs[, "from"], " -> ", arcs[, "to"]),
        coefficient_value = 1.0,  # Presence of edge
        coefficient_type = "edge",
        is_zero = FALSE,
        extracted_at = Sys.time()
      )
    } else {
      arc_df <- tibble()
    }
    
    # Add structure metadata
    meta_df <- tibble(
      model_id = model_id,
      athlete_id = athlete_id,
      model_type = model_type,
      coefficient_name = c("_n_nodes", "_n_edges"),
      coefficient_value = c(length(nodes), nrow(arcs)),
      coefficient_type = "hyperparameter",
      is_zero = FALSE,
      extracted_at = Sys.time()
    )
    
    bind_rows(arc_df, meta_df)
    
  }, error = function(e) {
    create_log_entry(
      sprintf("Failed to flatten BN model %s: %s", model_id, conditionMessage(e)),
      "WARN"
    )
    NULL
  })
}

#' Main function to flatten any model type
#' Returns data frame ready for BigQuery upload
flatten_model <- function(model_obj, model_id, athlete_id, model_type) {
  
  # Normalize model_type
  model_type_lower <- tolower(model_type)
  
  # Route to appropriate flattener
  if (model_type_lower %in% c("elastic_net", "lasso", "ridge")) {
    return(flatten_glmnet_model(model_obj, model_id, athlete_id, model_type))
  } else if (model_type_lower == "bayesian_network") {
    return(flatten_bn_model(model_obj, model_id, athlete_id, model_type))
  } else {
    create_log_entry(
      sprintf("Unknown model type for flattening: %s", model_type),
      "WARN"
    )
    return(NULL)
  }
}

#' Upload flattened coefficients to BigQuery
upload_model_coefficients <- function(coef_df, project, dataset) {
  if (is.null(coef_df) || nrow(coef_df) == 0) {
    return(FALSE)
  }
  
  tryCatch({
    # Create table reference
    coef_table <- bq_table(project, dataset, "model_coefficients")
    
    # Ensure table exists
    if (!bq_table_exists(coef_table)) {
      # Create with partitioning by extraction date
      bq_project_query(project, glue("
        CREATE TABLE `{project}.{dataset}.model_coefficients` (
          model_id STRING NOT NULL,
          athlete_id STRING,
          model_type STRING NOT NULL,
          coefficient_name STRING NOT NULL,
          coefficient_value FLOAT64,
          coefficient_type STRING,
          is_zero BOOLEAN,
          extracted_at TIMESTAMP NOT NULL
        )
        PARTITION BY DATE(extracted_at)
        CLUSTER BY model_id, athlete_id, coefficient_type
        OPTIONS(
          description='Flattened model coefficients in long format'
        )
      "))
      create_log_entry("Created model_coefficients table")
    }
    
    # Upload data
    bq_table_upload(coef_table, coef_df, write_disposition = "WRITE_APPEND")
    
    create_log_entry(
      sprintf("Uploaded %d coefficients for model %s", 
              nrow(coef_df), coef_df$model_id[1])
    )
    
    TRUE
    
  }, error = function(e) {
    create_log_entry(
      sprintf("Failed to upload coefficients: %s", conditionMessage(e)),
      "ERROR"
    )
    FALSE
  })
}

#' Create wide format coefficients view (optional)
#' This is useful for exporting to CSV or R analysis
create_wide_coefficients_view <- function(project, dataset) {
  tryCatch({
    # Create a view that pivots to wide format
    # Note: This will only work if you know the coefficient names in advance
    # For dynamic pivoting, use a scheduled query or R script
    
    sql <- glue("
      CREATE OR REPLACE VIEW `{project}.{dataset}.model_coefficients_wide` AS
      WITH coef_data AS (
        SELECT
          model_id,
          athlete_id,
          model_type,
          coefficient_name,
          coefficient_value,
          extracted_at
        FROM `{project}.{dataset}.model_coefficients`
        WHERE coefficient_type != 'hyperparameter'
      ),
      hyper_data AS (
        SELECT
          model_id,
          MAX(IF(coefficient_name = '_lambda', coefficient_value, NULL)) as lambda,
          MAX(IF(coefficient_name = '_alpha', coefficient_value, NULL)) as alpha,
          MAX(IF(coefficient_name = '_n_features', coefficient_value, NULL)) as n_features
        FROM `{project}.{dataset}.model_coefficients`
        WHERE coefficient_type = 'hyperparameter'
        GROUP BY model_id
      )
      SELECT
        c.model_id,
        c.athlete_id,
        c.model_type,
        h.lambda,
        h.alpha,
        h.n_features,
        MAX(IF(c.coefficient_name = '(Intercept)', c.coefficient_value, NULL)) as intercept,
        -- Add more coefficients as needed
        -- MAX(IF(c.coefficient_name = 'distance_7d', c.coefficient_value, NULL)) as coef_distance_7d,
        MAX(c.extracted_at) as extracted_at
      FROM coef_data c
      LEFT JOIN hyper_data h ON c.model_id = h.model_id
      GROUP BY c.model_id, c.athlete_id, c.model_type, h.lambda, h.alpha, h.n_features
    ")
    
    bq_project_query(project, sql)
    create_log_entry("Created model_coefficients_wide view")
    TRUE
    
  }, error = function(e) {
    create_log_entry(
      sprintf("Failed to create wide view: %s", conditionMessage(e)),
      "WARN"
    )
    FALSE
  })
}

#' Reconstruct glmnet model from stored coefficients for prediction
#' Loads coefficients from BigQuery and creates a prediction function
reconstruct_glmnet_from_coefficients <- function(model_id, project, dataset) {
  tryCatch({
    # Load coefficients
    sql <- glue("
      SELECT 
        coefficient_name,
        coefficient_value,
        coefficient_type
      FROM `{project}.{dataset}.model_coefficients`
      WHERE model_id = '{model_id}'
    ")
    
    coef_data <- bq_table_download(bq_project_query(project, sql))
    
    if (nrow(coef_data) == 0) {
      stop("No coefficients found for model_id: ", model_id)
    }
    
    # Extract hyperparameters
    lambda <- coef_data %>% 
      filter(coefficient_name == "_lambda") %>% 
      pull(coefficient_value) %>% 
      first()
    
    alpha <- coef_data %>% 
      filter(coefficient_name == "_alpha") %>% 
      pull(coefficient_value) %>% 
      first()
    
    # Extract coefficients (excluding hyperparameters)
    coefs <- coef_data %>%
      filter(coefficient_type %in% c("intercept", "predictor")) %>%
      select(coefficient_name, coefficient_value)
    
    # Create prediction function from coefficients
    predict_fn <- function(new_data) {
      # Get intercept
      intercept <- coefs %>%
        filter(coefficient_name == "(Intercept)") %>%
        pull(coefficient_value) %>%
        first()
      
      if (length(intercept) == 0) intercept <- 0
      
      # Get predictor coefficients
      pred_coefs <- coefs %>%
        filter(coefficient_name != "(Intercept)")
      
      # Calculate prediction
      pred <- intercept
      for (i in seq_len(nrow(pred_coefs))) {
        feat_name <- pred_coefs$coefficient_name[i]
        coef_val <- pred_coefs$coefficient_value[i]
        
        if (feat_name %in% names(new_data)) {
          pred <- pred + coef_val * new_data[[feat_name]]
        }
      }
      
      pred
    }
    
    list(
      predict = predict_fn,
      coefficients = coefs,
      lambda = lambda,
      alpha = alpha
    )
    
  }, error = function(e) {
    create_log_entry(
      sprintf("Failed to reconstruct model %s: %s", model_id, conditionMessage(e)),
      "ERROR"
    )
    NULL
  })
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
  create_log_entry(paste("Config snapshot save failed (non-fatal):", conditionMessage(e)), "WARN")
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
      create_log_entry(paste("Readiness match query failed:", conditionMessage(e)), "WARN", reason="query_failed")
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
# FIX 1.3: INIT REGISTRY TABLES (with partitioning and clustering)
# Note: PRIMARY KEY constraints removed to fix BigQuery MERGE failures.
# Uniqueness is enforced via MERGE logic instead of table constraints.
################################################################################
create_log_entry("Initializing model registry tables")
tryCatch({
  ds <- bq_dataset(project, dataset)
  if (!bq_dataset_exists(ds)) bq_dataset_create(ds, location = location)
  
  # Registry Models
  # Stores metadata for each model (one row per model_id)
  bq_project_query(project, glue("
    CREATE TABLE IF NOT EXISTS `{project}.{dataset}.registry_models` (
      model_id STRING NOT NULL,
      model_name STRING NOT NULL,
      team STRING,
      owner STRING,
      description STRING,
      created_at TIMESTAMP NOT NULL
    )
    PARTITION BY DATE(created_at)
    CLUSTER BY team, model_id
  "))
  create_log_entry("Registry table verified: registry_models")
  
  # Registry Versions
  # Stores version information for each model (one row per model_id + version_id)
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
      notes STRING
    )
    PARTITION BY DATE(created_at)
    CLUSTER BY model_id, version_id
  "))
  create_log_entry("Registry table verified: registry_versions")
  
  # Registry Metrics
  # Stores performance metrics for model versions (multiple rows per version)
  bq_project_query(project, glue("
    CREATE TABLE IF NOT EXISTS `{project}.{dataset}.registry_metrics` (
      model_id STRING NOT NULL,
      version_id STRING NOT NULL,
      metric_name STRING NOT NULL,
      metric_value FLOAT64,
      split STRING NOT NULL,
      logged_at TIMESTAMP NOT NULL
    )
    PARTITION BY DATE(logged_at)
    CLUSTER BY model_id, version_id, metric_name
  "))
  create_log_entry("Registry table verified: registry_metrics")
  
  # Registry Stages
  # Tracks model lifecycle stages (e.g., dev, staging, production)
  bq_project_query(project, glue("
    CREATE TABLE IF NOT EXISTS `{project}.{dataset}.registry_stages` (
      model_id STRING NOT NULL,
      version_id STRING NOT NULL,
      stage STRING NOT NULL,
      set_at TIMESTAMP NOT NULL,
      set_by STRING,
      reason STRING
    )
    PARTITION BY DATE(set_at)
    CLUSTER BY model_id, version_id, stage
  "))
  create_log_entry("Registry table verified: registry_stages")
  
  # Model Coefficients Table (for flattened models)
  create_log_entry("Initializing model coefficients table")
  coef_table_result <- tryCatch({
    bq_project_query(project, glue("
      CREATE TABLE IF NOT EXISTS `{project}.{dataset}.model_coefficients` (
        model_id STRING NOT NULL,
        athlete_id STRING,
        model_type STRING NOT NULL,
        coefficient_name STRING NOT NULL,
        coefficient_value FLOAT64,
        coefficient_type STRING,
        is_zero BOOLEAN,
        extracted_at TIMESTAMP NOT NULL
      )
      PARTITION BY DATE(extracted_at)
      CLUSTER BY model_id, athlete_id, coefficient_type
      OPTIONS(
        description='Flattened model coefficients in long format for easy querying'
      )
    "))
    create_log_entry("Registry table verified: model_coefficients")
    
    # Optionally create wide format view
    create_wide_coefficients_view(project, dataset)
    TRUE
  }, error = function(e) {
    create_log_entry(paste("Model coefficients table initialization warning:", conditionMessage(e)), "WARN")
    FALSE
  })
  
}, error = function(e) {
  create_log_entry(paste("Registry initialization failed:", conditionMessage(e)), "ERROR")
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

if (!isTRUE(workload_daily_result[["success"]])) {
  create_log_entry("Workload data load failed after retries", "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
}
workload_daily <- workload_daily_result[["result"]]

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

if (!isTRUE(vald_fd_jumps_result[["success"]])) {
  create_log_entry("VALD FD jumps data load failed after retries", "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
}
vald_fd_jumps <- vald_fd_jumps_result[["result"]]

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

if (!isTRUE(roster_mapping_result[["success"]])) {
  create_log_entry("Roster mapping load failed after retries", "ERROR")
  upload_logs_to_bigquery(); quit(status=1)
}
roster_mapping <- roster_mapping_result[["result"]]
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
# SAVE ML_BUILDER_READINESS TABLE
################################################################################
# Create ml_builder_readiness table with full_name (not normalized) and official_id
# This makes it easier to join with workload data for model building
create_log_entry("Creating ml_builder_readiness table")

ml_builder_readiness <- vald_fd_jumps_with_off_id %>% 
  select(date, official_id, jump_height_readiness, epf_readiness, rsi_readiness, vald_name) %>% 
  mutate(
    # Calculate readiness using SAFE_DIVIDE logic
    readiness_sum = coalesce(jump_height_readiness, 0) + coalesce(epf_readiness, 0) + coalesce(rsi_readiness, 0),
    readiness_count = (!is.na(jump_height_readiness)) + (!is.na(epf_readiness)) + (!is.na(rsi_readiness)),
    readiness = if_else(readiness_count > 0, readiness_sum / readiness_count, NA_real_),
    full_name = vald_name  # Use non-normalized full name
  ) %>% 
  select(date, full_name, official_id, readiness) %>%
  filter(!is.na(readiness))  # Only include rows with valid readiness scores

create_log_entry(glue("ml_builder_readiness prepared: {nrow(ml_builder_readiness)} rows"))

# Save to BigQuery
tryCatch({
  ml_readiness_tbl <- bq_table(project, dataset, "ml_builder_readiness")
  
  # Create table if it doesn't exist
  if (!bq_table_exists(ml_readiness_tbl)) {
    bq_table_create(
      ml_readiness_tbl,
      fields = as_bq_fields(ml_builder_readiness),
      time_partitioning = list(type="DAY", field="date"),
      clustering_fields = c("official_id", "full_name")
    )
    create_log_entry("Created ml_builder_readiness table")
  }
  
  # Upload data (WRITE_TRUNCATE to replace with latest data each run)
  bq_table_upload(ml_readiness_tbl, ml_builder_readiness, write_disposition = "WRITE_TRUNCATE")
  create_log_entry(glue("ml_builder_readiness table saved: {nrow(ml_builder_readiness)} rows"))
  
}, error = function(e) {
  create_log_entry(paste("ml_builder_readiness save failed (non-fatal):", conditionMessage(e)), "WARN")
})

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
    fit_sc <- safe_scale_fit(tr_cc, preds); Xtr <- fit_sc[["X"]]; sc <- fit_sc
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
          center=if (sc) fit_sc[["center"]] else NULL,
          scale=if (sc) fit_sc[["scale"]] else NULL, 
          cols=if (sc) fit_sc[["cols"]] else preds,
          fit_intercept=fi
        )
      )
    )
    if (is.null(best) || (is.finite(cand[["primary_rmse"]]) && cand[["primary_rmse"]] < best[["primary_rmse"]])) best <- cand
  }
  best
}

run_ridge <- function(train_df, test_df, preds, val_method) {
  best <- NULL
  for (fi in c(TRUE,FALSE)) for (sc_norm in c(TRUE,FALSE)) {
    fit <- run_glmnet_cvfit(train_df, preds, 0, lambda_grid, fi, sc_norm)
    train_rmse_val <- rmse(fit[["y"]], run_glmnet_predict(fit[["cv"]], fit[["Xtr"]], "lambda.min"))
    cv_rmse_val <- sqrt(min(fit[["cv"]][["cvm"]]))
    if (val_method == "test_set" && nrow(test_df) > 0) {
      te_df <- to_numeric_df(test_df, preds)
      te_cc <- te_df[stats::complete.cases(te_df[, preds, drop = FALSE]), , drop = FALSE]
      if (nrow(te_cc) > 0) {
        Xn <- if (sc_norm) safe_scale_apply(te_cc, fit[["scaler"]]) else as.matrix(te_cc[, preds, drop = FALSE])
        test_rmse_val <- rmse(te_cc[[response_var]], run_glmnet_predict(fit[["cv"]], Xn, "lambda.min"))
      } else test_rmse_val <- NA_real_
      primary <- test_rmse_val
    } else { test_rmse_val <- NA_real_; primary <- cv_rmse_val }
    cand <- list(
      model_type="ridge", 
      train_rmse=train_rmse_val, 
      cv_rmse=cv_rmse_val, 
      test_rmse=test_rmse_val, 
      primary_rmse=primary,
      hyper=sprintf("lambda=%.6f; fit_intercept=%s; normalization=%s", fit[["cv"]][["lambda.min"]], fi, sc_norm),
      fitted=list(
        type="glmnet", 
        model=fit[["cv"]], 
        predictors=preds,
        preprocessing=list(
          normalization=sc_norm, 
          center=if (sc_norm) fit[["scaler"]][["center"]] else NULL,
          scale=if (sc_norm) fit[["scaler"]][["scale"]] else NULL, 
          cols=fit[["scaler"]][["cols"]],
          fit_intercept=fi
        ),
        lambda_min=fit[["cv"]][["lambda.min"]], 
        alpha=0
      )
    )
    if (is.null(best) || (is.finite(cand[["primary_rmse"]]) && cand[["primary_rmse"]] < best[["primary_rmse"]])) best <- cand
  }
  best
}

run_lasso <- function(train_df, test_df, preds, val_method) {
  best <- NULL
  for (fi in c(TRUE,FALSE)) for (sc_norm in c(TRUE,FALSE)) {
    fit <- run_glmnet_cvfit(train_df, preds, 1, lambda_grid, fi, sc_norm)
    train_rmse_val <- rmse(fit[["y"]], run_glmnet_predict(fit[["cv"]], fit[["Xtr"]], "lambda.min"))
    cv_rmse_val <- sqrt(min(fit[["cv"]][["cvm"]]))
    if (val_method == "test_set" && nrow(test_df) > 0) {
      te_df <- to_numeric_df(test_df, preds)
      te_cc <- te_df[stats::complete.cases(te_df[, preds, drop = FALSE]), , drop = FALSE]
      if (nrow(te_cc) > 0) {
        Xn <- if (sc_norm) safe_scale_apply(te_cc, fit[["scaler"]]) else as.matrix(te_cc[, preds, drop = FALSE])
        test_rmse_val <- rmse(te_cc[[response_var]], run_glmnet_predict(fit[["cv"]], Xn, "lambda.min"))
      } else test_rmse_val <- NA_real_
      primary <- test_rmse_val
    } else { test_rmse_val <- NA_real_; primary <- cv_rmse_val }
    cand <- list(
      model_type="lasso", 
      train_rmse=train_rmse_val, 
      cv_rmse=cv_rmse_val, 
      test_rmse=test_rmse_val, 
      primary_rmse=primary,
      hyper=sprintf("lambda=%.6f; fit_intercept=%s; normalization=%s", fit[["cv"]][["lambda.min"]], fi, sc_norm),
      fitted=list(
        type="glmnet", 
        model=fit[["cv"]], 
        predictors=preds,
        preprocessing=list(
          normalization=sc_norm, 
          center=if (sc_norm) fit[["scaler"]][["center"]] else NULL,
          scale=if (sc_norm) fit[["scaler"]][["scale"]] else NULL, 
          cols=fit[["scaler"]][["cols"]],
          fit_intercept=fi
        ),
        lambda_min=fit[["cv"]][["lambda.min"]], 
        alpha=1
      )
    )
    if (is.null(best) || (is.finite(cand[["primary_rmse"]]) && cand[["primary_rmse"]] < best[["primary_rmse"]])) best <- cand
  }
  best
}

run_elastic <- function(train_df, test_df, preds, val_method) {
  best <- NULL
  for (ai in seq(0.1,0.9,by=0.2)) for (fi in c(TRUE,FALSE)) for (sc_norm in c(TRUE,FALSE)) {
    fit <- run_glmnet_cvfit(train_df, preds, ai, lambda_grid_en, fi, sc_norm)
    train_rmse_val <- rmse(fit[["y"]], run_glmnet_predict(fit[["cv"]], fit[["Xtr"]], "lambda.min"))
    cv_rmse_val <- sqrt(min(fit[["cv"]][["cvm"]]))
    if (val_method == "test_set" && nrow(test_df) > 0) {
      te_df <- to_numeric_df(test_df, preds)
      te_cc <- te_df[stats::complete.cases(te_df[, preds, drop = FALSE]), , drop = FALSE]
      if (nrow(te_cc) > 0) {
        Xn <- if (sc_norm) safe_scale_apply(te_cc, fit[["scaler"]]) else as.matrix(te_cc[, preds, drop = FALSE])
        test_rmse_val <- rmse(te_cc[[response_var]], run_glmnet_predict(fit[["cv"]], Xn, "lambda.min"))
      } else test_rmse_val <- NA_real_
      primary <- test_rmse_val
    } else { test_rmse_val <- NA_real_; primary <- cv_rmse_val }
    cand <- list(
      model_type="elastic_net", 
      train_rmse=train_rmse_val, 
      cv_rmse=cv_rmse_val, 
      test_rmse=test_rmse_val, 
      primary_rmse=primary,
      hyper=sprintf("lambda=%.6f; l1_ratio=%.1f; fit_intercept=%s; normalization=%s", fit[["cv"]][["lambda.min"]], ai, fi, sc_norm),
      fitted=list(
        type="glmnet", 
        model=fit[["cv"]], 
        predictors=preds,
        preprocessing=list(
          normalization=sc_norm, 
          center=if (sc_norm) fit[["scaler"]][["center"]] else NULL,
          scale=if (sc_norm) fit[["scaler"]][["scale"]] else NULL, 
          cols=fit[["scaler"]][["cols"]],
          fit_intercept=fi
        ),
        lambda_min=fit[["cv"]][["lambda.min"]], 
        alpha=ai
      )
    )
    if (is.null(best) || (is.finite(cand[["primary_rmse"]]) && cand[["primary_rmse"]] < best[["primary_rmse"]])) best <- cand
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
  BayesRidge <- sk[["BayesianRidge"]]
  best <- NULL
  for (a1 in c(1e-6, 1e-4)) for (l1 in c(1e-6, 1e-4)) {
    tr_df <- to_numeric_df(train_df, preds)
    mm_cols <- c(response_var, preds)
    tr_cc <- tr_df[stats::complete.cases(tr_df[, mm_cols, drop = FALSE]), , drop = FALSE]
    if (!enough_rows(nrow(tr_cc), 2L)) next
    fit_sc <- safe_scale_fit(tr_cc, preds)
    Xtr <- fit_sc[["X"]]; y <- as.numeric(tr_cc[[response_var]])
    m <- BayesRidge(alpha_1=a1, alpha_2=a1, lambda_1=l1, lambda_2=l1, fit_intercept=TRUE)
    m[["fit"]](Xtr, y)
    train_rmse_val <- rmse(y, as.numeric(m[["predict"]](Xtr)))
    k <- choose_nfolds(nrow(tr_cc))
    folds <- sample(rep(seq_len(k), length.out=nrow(tr_cc)))
    cv_vals <- numeric(k)
    for (f in seq_len(k)) {
      tr_ix <- folds != f; te_ix <- folds == f
      if (sum(tr_ix) < 2 || sum(te_ix) < 1) { cv_vals[f] <- NA_real_; next }
      sc_cv <- safe_scale_fit(tr_cc[tr_ix, , drop=FALSE], preds)
      m_cv <- BayesRidge(alpha_1=a1, alpha_2=a1, lambda_1=l1, lambda_2=l1)
      m_cv[["fit"]](sc_cv[["X"]], as.numeric(tr_cc[[response_var]][tr_ix]))
      Xte_cv <- safe_scale_apply(tr_cc[te_ix, , drop=FALSE], sc_cv)
      cv_vals[f] <- rmse(tr_cc[[response_var]][te_ix], as.numeric(m_cv[["predict"]](Xte_cv)))
    }
    cv_rmse_val <- mean(cv_vals, na.rm=TRUE)
    if (val_method == "test_set" && nrow(test_df) > 0) {
      te_df <- to_numeric_df(test_df, preds)
      te_cc <- te_df[stats::complete.cases(te_df[, preds, drop = FALSE]), , drop = FALSE]
      if (nrow(te_cc) > 0) {
        Xte <- safe_scale_apply(te_cc, fit_sc)
        test_rmse_val <- rmse(te_cc[[response_var]], as.numeric(m[["predict"]](Xte)))
      } else test_rmse_val <- NA_real_
      primary <- test_rmse_val
    } else { test_rmse_val <- NA_real_; primary <- cv_rmse_val }
    coef_vec <- tryCatch(as.numeric(reticulate::py_to_r(m[["coef_"]])), error=function(e) rep(NA_real_, length(preds)))
    intercept_val <- tryCatch(as.numeric(reticulate::py_to_r(m[["intercept_"]])), error=function(e) NA_real_)
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
          center=fit_sc[["center"]], 
          scale=fit_sc[["scale"]], 
          cols=fit_sc[["cols"]], 
          fit_intercept=TRUE
        ),
        coefficients=coef_vec, 
        intercept=intercept_val,
        priors=list(alpha_1=a1, alpha_2=a1, lambda_1=l1, lambda_2=l1)
      )
    )
    if (is.null(best) || (is.finite(cand[["primary_rmse"]]) && cand[["primary_rmse"]] < best[["primary_rmse"]])) best <- cand
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
    if (is.null(best) || (is.finite(cand[["primary_rmse"]]) && cand[["primary_rmse"]] < best[["primary_rmse"]])) best <- cand
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
    ath_train_df <- imp[["train"]]
    ath_test_df <- imp[["test"]]
    
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
    
    prim_rmse <- sapply(cands, function(c) c[["primary_rmse"]])
    finite_idx <- which(is.finite(prim_rmse))
    if (length(finite_idx) == 0) {
      create_log_entry("  ERROR: all models failed", "ERROR")
      fail_count <- fail_count + 1
      next
    }
    
    best_idx <- finite_idx[which.min(prim_rmse[finite_idx])]
    best_cand <- cands[[best_idx]]
    
    # Helper function to safely convert numeric values for BigQuery
    # Returns NA_real_ for invalid values so BigQuery can properly handle NULLs
    safe_numeric_for_bq <- function(x) {
      if (is.null(x) || !is.numeric(x)) return(NA_real_)
      if (is.na(x) || is.nan(x) || is.infinite(x)) return(NA_real_)
      return(as.numeric(x))
    }
    
    # Helper function to escape strings for BigQuery
    escape_bq_string <- function(x) {
      if (is.null(x) || is.na(x)) return("")
      # Escape single quotes and backslashes
      x <- gsub("\\\\", "\\\\\\\\", x)
      x <- gsub("'", "\\\\'", x)
      return(x)
    }
    
    # Save best model to registry (coefficients saved to BigQuery)
    save_model_to_registry <- function(athlete_id, model_name, candidate, version_id) {
      # Defensive check: ensure candidate is a list
      if (!is.list(candidate)) {
        create_log_entry(
          sprintf("Cannot save model for %s: candidate is not a list structure", model_name),
          "ERROR",
          reason = "invalid_candidate_structure"
        )
        return(list(
          success = FALSE, 
          model_id = paste(team_name, model_name, sep=":"), 
          version_id = version_id, 
          artifact_uri = "bigquery:model_coefficients",
          model_object = NULL,
          error = "invalid_candidate_structure"
        ))
      }
      
      fitted <- candidate[["fitted"]]
      if (is.null(fitted) || !is.list(fitted) || identical(fitted[["type"]], "error")) {
        create_log_entry(
          sprintf("Cannot save model for %s: model fitting failed or returned error", model_name),
          "ERROR",
          reason = "model_fit_error"
        )
        return(list(
          success = FALSE, 
          model_id = paste(team_name, model_name, sep=":"), 
          version_id = version_id, 
          artifact_uri = "bigquery:model_coefficients",
          model_object = NULL,
          error = "model_fit_error"
        ))
      }
      
      tryCatch({
        model_id <- paste(team_name, model_name, sep = ":")
        
        # Check if registry writes should be skipped
        if (skip_registry_versions) {
          create_log_entry(
            sprintf("  ⏭️  SKIP_REGISTRY_VERSIONS=true - skipping registry writes for %s", model_name),
            "WARN",
            reason = "skip_registry_versions_enabled"
          )
          
          # Return success without saving to allow training to continue
          return(list(
            success = TRUE,
            model_id = model_id,
            version_id = version_id,
            artifact_uri = "skipped",
            model_object = candidate[["fitted"]][["model"]],
            skipped = TRUE
          ))
        }
        
        # Coefficients saved to BigQuery model_coefficients table
        artifact_uri <- sprintf("bigquery:%s.%s.model_coefficients?model_id=%s", 
                               project, dataset, model_id)
        
        # Validate numeric values before saving (using [[ ]] for safer access)
        train_rmse_val <- safe_numeric_for_bq(candidate[["train_rmse"]])
        cv_rmse_val <- safe_numeric_for_bq(candidate[["cv_rmse"]])
        test_rmse_val <- safe_numeric_for_bq(candidate[["test_rmse"]])
        
        # Prepare values and escape strings
        run_id_val <- escape_bq_string(Sys.getenv("GITHUB_RUN_ID", "manual"))
        git_sha_val <- escape_bq_string(Sys.getenv("GITHUB_SHA", "unknown"))
        preds_json <- escape_bq_string(jsonlite::toJSON(preds, auto_unbox=TRUE))
        hyper_json <- escape_bq_string(jsonlite::toJSON(candidate[["hyper"]], auto_unbox=TRUE))
        escaped_model_id <- escape_bq_string(model_id)
        escaped_model_name <- escape_bq_string(model_name)
        escaped_version_id <- escape_bq_string(version_id)
        
        # Save to registry with free tier compatible approach
        tryCatch({
          # Step 1: Save to registry_models using streaming inserts (not DML)
          
          create_log_entry(sprintf("  Saving to registry_models (free tier compatible)..."))
          result <- retry_operation(
            {
              tryCatch({
                # FREE TIER FIX: Check if model exists (SELECT allowed on free tier)
                check_sql <- glue("
                  SELECT COUNT(*) as count 
                  FROM `{project}.{dataset}.registry_models` 
                  WHERE model_id = '{escaped_model_id}'
                ")
                
                check_result <- bq_project_query(project, check_sql, use_legacy_sql = FALSE)
                check_df <- bq_table_download(check_result)
                
                if (check_df$count[1] > 0) {
                  # Model already exists - skip (or could update via delete+insert if needed)
                  create_log_entry(sprintf("  Model already exists in registry, skipping insert"))
                  return(TRUE)
                }
                
                # Model doesn't exist - insert using streaming API (NOT DML - works on free tier!)
                new_row <- tibble(
                  model_id = escaped_model_id,
                  model_name = escaped_model_name,
                  team = team_name,
                  owner = "auto",
                  description = "Athlete readiness model",
                  created_at = Sys.time()
                )
                
                tbl <- bq_table(project, dataset, "registry_models")
                
                # Log the data being uploaded for debugging
                create_log_entry(sprintf("    Uploading model row: model_id=%s", escaped_model_id))
                
                # bq_table_upload uses streaming insert API (not DML) - free tier compatible!
                upload_job <- bq_table_upload(tbl, new_row, write_disposition = "WRITE_APPEND")
                
                # If the upload returns a job object, wait for it and check status
                if (!is.null(upload_job) && is.list(upload_job) && inherits(upload_job, "bq_job")) {
                  # Use safe_get_field to avoid "$ operator is invalid for atomic vectors" error
                  job_id <- safe_get_field(upload_job, c("jobReference", "jobId"))
                  job_location <- safe_get_field(upload_job, c("jobReference", "location"))
                  
                  if (!is.null(job_id)) {
                    job_location <- job_location %||% location
                    create_log_entry(sprintf("    ✅ Upload job created: %s.%s", job_id, job_location))
                    
                    # Wait for job completion and check status
                    tryCatch({
                      completed_job <- bq_job_wait(upload_job, quiet = TRUE)
                      
                      # Check if job failed using safe accessor
                      error_result <- safe_get_field(completed_job, c("status", "errorResult"))
                      if (!is.null(error_result)) {
                        create_log_entry(sprintf("    ❌ Job %s.%s FAILED", job_id, job_location), "ERROR")
                        log_bq_job_error(completed_job, "registry_models upload")
                        stop(sprintf("BigQuery job %s.%s failed", job_id, job_location))
                      }
                      
                      create_log_entry(sprintf("    ✅ Job %s.%s completed successfully", job_id, job_location))
                    }, error = function(wait_err) {
                      create_log_entry(sprintf("    ⚠️  Error waiting for job: %s", wait_err$message), "WARN")
                      # Try to fetch job details even if wait failed
                      log_bq_job_error(upload_job, "registry_models upload")
                      stop(wait_err)
                    })
                  }
                }
                
                TRUE
              }, error = function(e) {
                create_log_entry(sprintf("  Registry save error: %s", conditionMessage(e)), "ERROR")
                create_log_entry(sprintf("  Error class: %s", paste(class(e), collapse=", ")), "ERROR")
                
                # Log detailed BigQuery error
                log_bq_job_error(e, "registry_models upload")
                
                stop(sprintf("registry_models insert failed: %s", conditionMessage(e)))
              })
            },
            max_attempts = 3,
            wait_seconds = 2,
            operation_name = "registry_models save",
            exponential_backoff = TRUE
          )
          
          if (!isTRUE(result[["success"]])) {
            stop(sprintf("Failed to save to registry_models after retries: %s", result[["error"]]))
          }
          create_log_entry(sprintf("  Saved to registry_models: %s", model_id))
          
          # Step 2: Save to registry_versions using streaming insert (not DML)
          create_log_entry(sprintf("  Saving to registry_versions (free tier compatible)..."))
          result <- retry_operation(
            {
              tryCatch({
                # FREE TIER FIX: Check if version exists (SELECT allowed on free tier)
                check_sql <- glue("
                  SELECT COUNT(*) as count 
                  FROM `{project}.{dataset}.registry_versions` 
                  WHERE model_id = '{escaped_model_id}' AND version_id = '{escaped_version_id}'
                ")
                
                check_result <- bq_project_query(project, check_sql, use_legacy_sql = FALSE)
                check_df <- bq_table_download(check_result)
                
                if (check_df$count[1] > 0) {
                  # Version already exists - skip (or could update via delete+insert if needed)
                  create_log_entry(sprintf("  Version already exists in registry, skipping insert"))
                  return(TRUE)
                }
                
                # Prepare version row for streaming insert
                version_row <- tibble(
                  model_id = escaped_model_id,
                  version_id = escaped_version_id,
                  created_at = Sys.time(),
                  created_by = "github_actions",
                  artifact_uri = artifact_uri,
                  artifact_sha256 = "bigquery",
                  framework = "R/glmnet",
                  r_version = R.version$version.string,
                  package_info = "",
                  notes = "",
                  training_data_start_date = cfg_start_date,
                  training_data_end_date = cfg_end_date,
                  n_training_samples = as.integer(n_tr),
                  n_test_samples = as.integer(n_te),
                  validation_method = val_method,
                  predictor_list = preds_json,
                  hyperparameters = hyper_json,
                  train_rmse = train_rmse_val,
                  cv_rmse = cv_rmse_val,
                  test_rmse = test_rmse_val,
                  pipeline_run_id = run_id_val,
                  git_commit_sha = git_sha_val
                )
                
                tbl <- bq_table(project, dataset, "registry_versions")
                
                # Log the data being uploaded for debugging
                create_log_entry(sprintf("    Uploading version row: model_id=%s, version_id=%s", 
                                        escaped_model_id, escaped_version_id))
                
                # bq_table_upload uses streaming insert API (not DML) - free tier compatible!
                upload_job <- bq_table_upload(tbl, version_row, write_disposition = "WRITE_APPEND")
                
                # If the upload returns a job object, wait for it and check status
                if (!is.null(upload_job) && is.list(upload_job) && inherits(upload_job, "bq_job")) {
                  # Use safe_get_field to avoid "$ operator is invalid for atomic vectors" error
                  job_id <- safe_get_field(upload_job, c("jobReference", "jobId"))
                  job_location <- safe_get_field(upload_job, c("jobReference", "location"))
                  
                  if (!is.null(job_id)) {
                    job_location <- job_location %||% location
                    create_log_entry(sprintf("    ✅ Upload job created: %s.%s", job_id, job_location))
                    
                    # Wait for job completion and check status
                    tryCatch({
                      completed_job <- bq_job_wait(upload_job, quiet = TRUE)
                      
                      # Check if job failed using safe accessor
                      error_result <- safe_get_field(completed_job, c("status", "errorResult"))
                      if (!is.null(error_result)) {
                        create_log_entry(sprintf("    ❌ Job %s.%s FAILED", job_id, job_location), "ERROR")
                        log_bq_job_error(completed_job, "registry_versions upload")
                        stop(sprintf("BigQuery job %s.%s failed", job_id, job_location))
                      }
                      
                      create_log_entry(sprintf("    ✅ Job %s.%s completed successfully", job_id, job_location))
                    }, error = function(wait_err) {
                      create_log_entry(sprintf("    ⚠️  Error waiting for job: %s", wait_err$message), "WARN")
                      # Try to fetch job details even if wait failed
                      log_bq_job_error(upload_job, "registry_versions upload")
                      stop(wait_err)
                    })
                  }
                }
                
                TRUE
              }, error = function(e) {
                create_log_entry(sprintf("  Registry versions save error: %s", conditionMessage(e)), "ERROR")
                create_log_entry(sprintf("  Error class: %s", paste(class(e), collapse=", ")), "ERROR")
                
                # Log detailed BigQuery error
                log_bq_job_error(e, "registry_versions upload")
                
                stop(sprintf("registry_versions insert failed: %s", conditionMessage(e)))
              })
            },
            max_attempts = 3,
            wait_seconds = 2,
            operation_name = "registry_versions save",
            exponential_backoff = TRUE
          )
          
          if (!isTRUE(result[["success"]])) {
            stop(sprintf("Failed to save to registry_versions after retries: %s", result[["error"]]))
          }
          create_log_entry(sprintf("  Saved to registry_versions: %s", version_id))
          
          # Step 3: Save metrics (using [[ ]] for safer access)
          metrics <- tibble(
            model_id = model_id, 
            version_id = version_id,
            metric_name = c("train_rmse","cv_rmse","test_rmse","primary_rmse"),
            metric_value = c(
              if (!is.finite(candidate[["train_rmse"]])) NA_real_ else candidate[["train_rmse"]],
              if (!is.finite(candidate[["cv_rmse"]])) NA_real_ else candidate[["cv_rmse"]],
              if (!is.finite(candidate[["test_rmse"]])) NA_real_ else candidate[["test_rmse"]],
              if (!is.finite(candidate[["primary_rmse"]])) NA_real_ else candidate[["primary_rmse"]]
            ),
            split = c("train","cv","test","primary"), 
            logged_at = Sys.time()
          )
          
          create_log_entry(sprintf("  Uploading metrics to registry_metrics..."))
          result <- retry_operation(
            {
              tryCatch({
                bq_table_upload(
                  bq_table(project, dataset, "registry_metrics"), 
                  metrics, 
                  write_disposition="WRITE_APPEND"
                )
                TRUE
              }, error = function(e) {
                create_log_entry(sprintf("  Metrics upload error: %s", conditionMessage(e)), "ERROR")
                create_log_entry(sprintf("  Metrics data: %s", paste(capture.output(str(metrics)), collapse="\n")), "ERROR")
                stop(sprintf("registry_metrics upload failed: %s", conditionMessage(e)))
              })
            },
            max_attempts = 3,
            wait_seconds = 2,
            operation_name = "registry_metrics upload",
            exponential_backoff = TRUE
          )
          
          if (!isTRUE(result[["success"]])) {
            stop(sprintf("Failed to upload metrics after retries: %s", result[["error"]]))
          }
          create_log_entry(sprintf("  Saved metrics to registry_metrics"))
          
          # Step 4: Save to registry_stages
          sql_stages <- glue("
            INSERT INTO `{project}.{dataset}.registry_stages`
              (model_id, version_id, stage, set_at, set_by, reason)
            VALUES
              ('{escaped_model_id}','{escaped_version_id}','Staging',CURRENT_TIMESTAMP(),'auto','Initial training')
          ")
          
          create_log_entry(sprintf("  Executing SQL for registry_stages..."))
          result <- retry_operation(
            {
              tryCatch({
                bq_project_query(project, sql_stages, use_legacy_sql = FALSE)
                TRUE
              }, error = function(e) {
                create_log_entry(sprintf("  SQL that failed:\n%s", sql_stages), "ERROR")
                create_log_entry(sprintf("  BigQuery error: %s", conditionMessage(e)), "ERROR")
                stop(sprintf("registry_stages insert failed: %s", conditionMessage(e)))
              })
            },
            max_attempts = 3,
            wait_seconds = 2,
            operation_name = "registry_stages save",
            exponential_backoff = TRUE
          )
          
          if (!isTRUE(result[["success"]])) {
            stop(sprintf("Failed to save to registry_stages after retries: %s", result[["error"]]))
          }
          create_log_entry(sprintf("  Saved to registry_stages: Staging"))
          
        }, error = function(e) {
          create_log_entry(
            sprintf("  ERROR saving to BigQuery registry for %s: %s", model_name, conditionMessage(e)),
            "ERROR",
            reason = "bigquery_registry_failed"
          )
          # Print detailed error information
          create_log_entry(sprintf("  Error class: %s", paste(class(e), collapse=", ")), "ERROR")
          stop(sprintf("BigQuery registry save failed: %s", conditionMessage(e)))
        })
        
        # Return success with model object for flattening
        list(
          success=TRUE, 
          model_id=model_id, 
          version_id=version_id, 
          artifact_uri=artifact_uri,
          model_object=candidate[["fitted"]][["model"]]  # Model object for coefficient flattening
        )
      }, error = function(e) {
        # Top-level error handler with full details
        create_log_entry(
          sprintf("Model save FAILED for %s: %s", model_name, conditionMessage(e)),
          "ERROR",
          reason = "model_save_error"
        )
        
        # Print full error details
        cat("\n")
        cat("════════════════════════════════════════════════════\n")
        cat("ERROR DETAILS for model:", model_name, "\n")
        cat("════════════════════════════════════════════════════\n")
        cat("Error message:", conditionMessage(e), "\n")
        cat("\n")
        
        # Print call stack if available
        tb <- tryCatch({
          capture.output(traceback())
        }, error = function(e2) NULL)
        
        if (!is.null(tb) && length(tb) > 0) {
          cat("Call stack:\n")
          cat(paste(tb, collapse="\n"), "\n")
          cat("\n")
        }
        
        # Diagnostic information
        cat("Diagnostic info:\n")
        cat("  Model type:", candidate[["model_type"]], "\n")
        cat("  Athlete ID:", athlete_id, "\n")
        cat("  Version ID:", version_id, "\n")
        cat("\n")
        cat("════════════════════════════════════════════════════\n")
        cat("\n")
        
        list(
          success=FALSE, 
          model_id=NA, 
          version_id=NA, 
          artifact_uri=NA, 
          model_object=NULL,
          error=conditionMessage(e)
        )
      })
    }
    
    model_name <- paste(athlete_id, best_cand[["model_type"]], sep = "_")
    save_result <- save_model_to_registry(athlete_id, model_name, best_cand, version_id)
    
    # ============================================
    # FLATTEN AND UPLOAD COEFFICIENTS TO BIGQUERY
    # ============================================
    if (isTRUE(save_result[["success"]]) && !is.null(save_result[["model_object"]])) {
      
      create_log_entry(sprintf("  Flattening %s model for athlete %s...", best_cand[["model_type"]], athlete_id))
      
      # Flatten the model
      coef_df <- flatten_model(
        model_obj = save_result[["model_object"]],
        model_id = save_result[["model_id"]],
        athlete_id = athlete_id,
        model_type = best_cand[["model_type"]]
      )
      
      # Upload to BigQuery
      if (!is.null(coef_df) && nrow(coef_df) > 0) {
        upload_success <- upload_model_coefficients(
          coef_df = coef_df,
          project = project,
          dataset = dataset
        )
        
        if (upload_success) {
          n_coefs <- sum(!coef_df$is_zero & coef_df$coefficient_type == "predictor")
          n_total <- nrow(coef_df)
          
          create_log_entry(
            sprintf("  ✓ Flattened: %d total entries (%d non-zero predictors)", 
                    n_total, n_coefs)
          )
          
          # Store flattening metadata (using [[ ]] for safer access)
          save_result[["n_coefficients"]] <- n_total
          save_result[["n_nonzero_coefficients"]] <- n_coefs
          save_result[["flattened"]] <- TRUE
        } else {
          create_log_entry(
            sprintf("  ⚠ Failed to upload coefficients for model %s", save_result[["model_id"]]),
            "WARN"
          )
          save_result[["n_coefficients"]] <- NA_integer_
          save_result[["n_nonzero_coefficients"]] <- NA_integer_
          save_result[["flattened"]] <- FALSE
        }
      } else {
        create_log_entry(
          sprintf("  ⚠ No coefficients extracted for model %s", save_result[["model_id"]]),
          "WARN"
        )
        save_result[["n_coefficients"]] <- NA_integer_
        save_result[["n_nonzero_coefficients"]] <- NA_integer_
        save_result[["flattened"]] <- FALSE
      }
    } else {
      # Model save failed or no model object
      save_result[["n_coefficients"]] <- NA_integer_
      save_result[["n_nonzero_coefficients"]] <- NA_integer_
      save_result[["flattened"]] <- FALSE
    }
    # ============================================
    
    if (isTRUE(save_result[["success"]]) || is.na(save_result[["success"]])) {
      create_log_entry(glue("  SUCCESS: {best_cand[['model_type']]} (RMSE={round(best_cand[['primary_rmse']], 3)})"))
      success_count <- success_count + 1
      
      # Generate predictions
      all_data <- rbind(ath_train_df, ath_test_df)
      if (nrow(all_data) > 0) {
        preds_model <- tryCatch({
          fitted_type <- best_cand[["fitted"]][["type"]]
          if (identical(fitted_type, "lm")) {
            predict(best_cand[["fitted"]][["model"]], all_data)
          } else if (identical(fitted_type, "glmnet")) {
            if (isTRUE(best_cand[["fitted"]][["preprocessing"]][["normalization"]])) {
              X_scaled <- safe_scale_apply(all_data, best_cand[["fitted"]][["preprocessing"]])
              as.numeric(predict(best_cand[["fitted"]][["model"]], X_scaled, s="lambda.min"))
            } else {
              X <- as.matrix(all_data[, preds, drop=FALSE])
              as.numeric(predict(best_cand[["fitted"]][["model"]], X, s="lambda.min"))
            }
          } else if (identical(fitted_type, "bnlearn")) {
            as.numeric(predict(best_cand[["fitted"]][["model"]], node = response_var, data = all_data[, preds, drop=FALSE]))
          } else {
            rep(NA_real_, nrow(all_data))
          }
        }, error = function(e) {
          create_log_entry(glue("  Prediction generation failed: {conditionMessage(e)}"), "WARN")
          rep(NA_real_, nrow(all_data))
        })
        
        pred_meta <- rbind(
          ath_train[, .(official_id, date, readiness, is_test)],
          ath_test[,  .(official_id, date, readiness, is_test)]
        )
        
        pred_df <- pred_meta %>%
          mutate(
            predicted_readiness = preds_model,
            model_id = save_result[["model_id"]] %||% NA_character_,
            version_id = save_result[["version_id"]] %||% NA_character_,
            prediction_date = Sys.time()
          )
        all_predictions[[length(all_predictions) + 1]] <- pred_df
      }
      
      # Save summary row
      all_results[[length(all_results) + 1]] <- tibble(
        athlete_id = athlete_id,
        roster_name = NA_character_,
        model_type = best_cand[["model_type"]],
        model_id = save_result[["model_id"]] %||% NA_character_,
        version_id = save_result[["version_id"]] %||% NA_character_,
        artifact_uri = save_result[["artifact_uri"]] %||% NA_character_,
        train_rmse = best_cand[["train_rmse"]],
        cv_rmse = best_cand[["cv_rmse"]],
        test_rmse = best_cand[["test_rmse"]],
        primary_rmse = best_cand[["primary_rmse"]],
        n_train = n_tr,
        n_test = n_te,
        n_predictors = length(preds),
        validation_method = val_method,
        hyperparameters = best_cand[["hyper"]],
        flattened = save_result[["flattened"]] %||% FALSE,
        n_coefficients = save_result[["n_coefficients"]] %||% NA_integer_,
        n_nonzero_coefficients = save_result[["n_nonzero_coefficients"]] %||% NA_integer_,
        trained_at = Sys.time()
      )
    } else {
      create_log_entry(glue("  ERROR: model save failed"), "ERROR")
      fail_count <- fail_count + 1
    }
  }, error = function(e) {
    create_log_entry(glue("  ERROR: {conditionMessage(e)}"), "ERROR")
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
  workload_rows_loaded = nrow(workload_daily),
  readiness_rows_loaded = nrow(vald_fd_jumps),
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
  create_log_entry(paste("Health metrics save failed (non-fatal):", conditionMessage(e)), "WARN")
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
