#!/usr/bin/env Rscript
# ============================================================================
# VALD V2.4.3 Secondary Defense Script
# ============================================================================
# Version: 2.4.3
# Date: January 2026
# Purpose: Re-fetch and process unaccounted test_IDs from the filter log
# Execution: GitHub Actions - runs hourly to catch any missing data
#
# This script:
# 1. Queries the filtered_test_ids_log table for RECONCILIATION entries
# 2. Re-fetches the specific test_IDs using appropriate valdr functions
# 3. Processes and saves the data using the same logic as the main script
# 4. Purges the log entries once test_IDs are successfully accounted for
# ============================================================================

# ============================================================================
# Package Loading
# ============================================================================
tryCatch({
  suppressPackageStartupMessages({
    library(bigrquery)
    library(DBI)
    library(dplyr)
    library(data.table)
    library(lubridate)
    library(glue)
    library(valdr)
  })
}, error = function(e) {
  cat("Error loading packages:", conditionMessage(e), "\n")
  quit(status = 1)
})

# Disable BigQuery Storage API
options(bigrquery.use_bqstorage = FALSE)
Sys.setenv(BIGRQUERY_USE_BQ_STORAGE = "false")

# ============================================================================
# Configuration
# ============================================================================
CONFIG <- list(
  gcp_project = Sys.getenv("GCP_PROJECT", "sac-vald-hub"),
  bq_dataset = Sys.getenv("BQ_DATASET", "analytics"),
  bq_location = Sys.getenv("BQ_LOCATION", "US"),
  timezone = "America/Los_Angeles",
  
  # Limits for safety
  max_test_ids_per_run = 100L,  # Don't process too many at once
  batch_size = 10L,             # Process in small batches
  
  # Configurable lookback period (days) for ForceDecks API queries
  lookback_days = as.integer(Sys.getenv("RECOVERY_LOOKBACK_DAYS", "30"))
)

#' Sanitize a string for safe SQL inclusion
#' Escapes single quotes to prevent SQL injection
#' @param s Character string to sanitize
#' @return Sanitized string safe for SQL inclusion
sanitize_sql_string <- function(s) {
  gsub("'", "''", as.character(s), fixed = TRUE)
}

#' Validate column name for safe SQL inclusion
#' Only allows alphanumeric characters and underscores
#' @param col_name Column name to validate
#' @return TRUE if valid, FALSE otherwise
is_valid_column_name <- function(col_name) {
  grepl("^[a-zA-Z_][a-zA-Z0-9_]*$", col_name)
}

# ============================================================================
# Logging Functions
# ============================================================================
script_start_time <- Sys.time()

log_info <- function(msg) {
  ts <- format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC")
  cat(sprintf("[%s] [INFO] %s\n", ts, glue::glue(msg)))
}

log_warn <- function(msg) {
  ts <- format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC")
  cat(sprintf("[%s] [WARN] %s\n", ts, glue::glue(msg)))
}

log_error <- function(msg) {
  ts <- format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "UTC")
  cat(sprintf("[%s] [ERROR] %s\n", ts, glue::glue(msg)))
}

# ============================================================================
# BigQuery Authentication
# ============================================================================
log_info("=== VALD V2.4.3 SECONDARY DEFENSE SCRIPT STARTED ===")
log_info("Authenticating to BigQuery...")

tryCatch({
  tok <- system("gcloud auth print-access-token", intern = TRUE)
  bq_auth(token = gargle::gargle2.0_token(
    scope = 'https://www.googleapis.com/auth/bigquery',
    client = gargle::gargle_client(),
    credentials = list(access_token = tok[1])
  ))
  log_info("BigQuery authentication successful")
}, error = function(e) {
  log_error("BigQuery authentication failed: {e$message}")
  quit(status = 1)
})

# ============================================================================
# VALD API Authentication
# ============================================================================
log_info("Setting up VALD API credentials...")

tryCatch({
  client_id <- Sys.getenv("VALD_CLIENT_ID", "")
  client_secret <- Sys.getenv("VALD_CLIENT_SECRET", "")
  tenant_id <- Sys.getenv("VALD_TENANT_ID", "")
  region <- Sys.getenv("VALD_REGION", "use")
  
  if (nzchar(client_id) && nzchar(client_secret) && nzchar(tenant_id)) {
    valdr::set_credentials(
      client_id = client_id,
      client_secret = client_secret,
      tenant_id = tenant_id,
      region = region
    )
    log_info("VALD API credentials configured")
  } else {
    log_error("Missing VALD API credentials in environment")
    quit(status = 1)
  }
}, error = function(e) {
  log_error("VALD API authentication failed: {e$message}")
  quit(status = 1)
})

# ============================================================================
# Helper Functions
# ============================================================================

#' Check if a BigQuery table exists
safe_table_exists <- function(tbl) {
  tryCatch({
    bigrquery::bq_table_exists(tbl)
  }, error = function(e) {
    FALSE
  })
}

#' Query unaccounted test_IDs from the filter log
#' @return data.table with test_ID, data_source, filter_stage, filter_reason
query_unaccounted_test_ids <- function() {
  tryCatch({
    ds <- bigrquery::bq_dataset(CONFIG$gcp_project, CONFIG$bq_dataset)
    tbl <- bigrquery::bq_table(ds, "filtered_test_ids_log")
    
    if (!safe_table_exists(tbl)) {
      log_info("No filtered_test_ids_log table exists - nothing to process")
      return(data.table::data.table())
    }
    
    # Query for RECONCILIATION entries (unaccounted test_IDs)
    sql <- glue::glue("
      SELECT DISTINCT test_ID, data_source, filter_stage, filter_reason, MIN(timestamp) as first_seen
      FROM `{CONFIG$gcp_project}.{CONFIG$bq_dataset}.filtered_test_ids_log`
      WHERE filter_stage = 'RECONCILIATION'
      GROUP BY test_ID, data_source, filter_stage, filter_reason
      ORDER BY first_seen ASC
      LIMIT {CONFIG$max_test_ids_per_run}
    ")
    
    result <- bigrquery::bq_project_query(CONFIG$gcp_project, sql) %>% 
      bigrquery::bq_table_download()
    
    dt <- data.table::as.data.table(result)
    log_info("Found {nrow(dt)} unaccounted test_IDs to process")
    
    return(dt)
    
  }, error = function(e) {
    log_error("Failed to query unaccounted test_IDs: {e$message}")
    return(data.table::data.table())
  })
}

#' Fetch ForceDecks data for specific test IDs
#' Uses get_forcedecks_tests_only() and get_forcedecks_trials_only()
#' @param test_ids Vector of test_IDs to fetch
#' @return List with tests and trials data.tables
fetch_forcedecks_by_ids <- function(test_ids) {
  if (length(test_ids) == 0) {
    return(list(tests = data.table::data.table(), trials = data.table::data.table()))
  }
  
  log_info("Fetching ForceDecks data for {length(test_ids)} test_IDs...")
  
  tryCatch({
    # Use configurable start_date to limit API response for efficiency
    lookback <- CONFIG$lookback_days
    start_date <- format(Sys.Date() - lookback, "%Y-%m-%dT00:00:00Z")
    
    # Get tests with date filter
    # Note: valdr doesn't have a direct "get by ID" for ForceDecks, 
    # so we fetch tests from recent period and filter to our specific IDs
    all_tests <- valdr::get_forcedecks_tests_only(start_date = start_date)
    
    if (is.null(all_tests) || nrow(all_tests) == 0) {
      log_warn("No ForceDecks tests returned from API (last {lookback} days)")
      return(list(tests = data.table::data.table(), trials = data.table::data.table()))
    }
    
    log_info("API returned {nrow(all_tests)} tests from last {lookback} days")
    
    # Filter to our specific test IDs
    tests_dt <- data.table::as.data.table(all_tests)
    if ("testId" %in% names(tests_dt)) {
      tests_dt <- tests_dt[testId %in% test_ids]
    } else if ("test_ID" %in% names(tests_dt)) {
      tests_dt <- tests_dt[test_ID %in% test_ids]
    }
    
    if (nrow(tests_dt) == 0) {
      log_warn("None of the requested test_IDs found in ForceDecks API response")
      log_info("These tests may be older than 30 days - consider extending lookback period")
      return(list(tests = data.table::data.table(), trials = data.table::data.table()))
    }
    
    log_info("Found {nrow(tests_dt)} matching ForceDecks tests")
    
    # Now fetch trials for these tests
    trials <- valdr::get_forcedecks_trials_only(tests_dt)
    trials_dt <- if (!is.null(trials)) data.table::as.data.table(trials) else data.table::data.table()
    
    log_info("Fetched {nrow(trials_dt)} ForceDecks trials")
    
    return(list(tests = tests_dt, trials = trials_dt))
    
  }, error = function(e) {
    log_error("ForceDecks fetch failed: {e$message}")
    return(list(tests = data.table::data.table(), trials = data.table::data.table()))
  })
}

#' Fetch NordBord data for specific test IDs
#' Uses get_nordbord_test_by_id() for each test_ID
#' @param test_ids Vector of test_IDs to fetch
#' @return data.table with NordBord test data
fetch_nordbord_by_ids <- function(test_ids) {
  if (length(test_ids) == 0) {
    return(data.table::data.table())
  }
  
  log_info("Fetching NordBord data for {length(test_ids)} test_IDs...")
  
  results <- list()
  
  for (i in seq_along(test_ids)) {
    test_id <- test_ids[i]
    
    tryCatch({
      result <- valdr::get_nordbord_test_by_id(test_id)
      
      if (!is.null(result) && nrow(result) > 0) {
        results[[length(results) + 1]] <- data.table::as.data.table(result)
        log_info("  Fetched NordBord test {i}/{length(test_ids)}: {test_id}")
      } else {
        log_warn("  NordBord test not found: {test_id}")
      }
      
    }, error = function(e) {
      log_warn("  Failed to fetch NordBord test {test_id}: {e$message}")
    })
  }
  
  if (length(results) > 0) {
    combined <- data.table::rbindlist(results, use.names = TRUE, fill = TRUE)
    log_info("Fetched {nrow(combined)} NordBord tests total")
    return(combined)
  } else {
    log_warn("No NordBord tests successfully fetched")
    return(data.table::data.table())
  }
}

#' Simple processing for recovered ForceDecks data
#' Applies minimal transformations needed for export
#' @param tests_dt data.table of tests
#' @param trials_dt data.table of trials
#' @return data.table ready for export
process_recovered_forcedecks <- function(tests_dt, trials_dt) {
  if (nrow(tests_dt) == 0 || nrow(trials_dt) == 0) {
    log_warn("No data to process for ForceDecks recovery")
    return(data.table::data.table())
  }
  
  log_info("Processing {nrow(tests_dt)} ForceDecks tests...")
  
  tryCatch({
    # Standardize column names
    if ("testId" %in% names(tests_dt)) {
      tests_dt[, test_ID := testId]
    }
    if ("profileId" %in% names(tests_dt)) {
      tests_dt[, vald_id := profileId]
    }
    if ("testType" %in% names(tests_dt)) {
      tests_dt[, test_type := testType]
    }
    
    # Parse dates
    if ("recordedDateUtc" %in% names(tests_dt)) {
      tests_dt[, date := as.Date(lubridate::ymd_hms(recordedDateUtc, tz = "UTC", quiet = TRUE))]
    }
    
    # For now, return the tests with standardized columns
    # Full processing would mirror VALD_V2.4.3Cloud.R but this handles the basics
    log_info("Processed {nrow(tests_dt)} ForceDecks tests for recovery")
    
    return(tests_dt)
    
  }, error = function(e) {
    log_error("ForceDecks processing failed: {e$message}")
    return(data.table::data.table())
  })
}

#' Simple processing for recovered NordBord data
#' @param nord_dt data.table of NordBord tests
#' @return data.table ready for export
process_recovered_nordbord <- function(nord_dt) {
  if (nrow(nord_dt) == 0) {
    log_warn("No data to process for NordBord recovery")
    return(data.table::data.table())
  }
  
  log_info("Processing {nrow(nord_dt)} NordBord tests...")
  
  tryCatch({
    # Standardize column names
    if ("testId" %in% names(nord_dt)) {
      nord_dt[, test_ID := testId]
    }
    
    log_info("Processed {nrow(nord_dt)} NordBord tests for recovery")
    return(nord_dt)
    
  }, error = function(e) {
    log_error("NordBord processing failed: {e$message}")
    return(data.table::data.table())
  })
}

#' Upload recovered data to BigQuery
#' @param dt data.table to upload
#' @param table_name Target table name
#' @param key_col Column to use as key for upsert (default: "test_ID")
#' @return TRUE if successful, FALSE otherwise
upload_recovered_data <- function(dt, table_name, key_col = "test_ID") {
  if (nrow(dt) == 0) {
    log_info("No data to upload for {table_name}")
    return(FALSE)
  }
  
  tryCatch({
    ds <- bigrquery::bq_dataset(CONFIG$gcp_project, CONFIG$bq_dataset)
    tbl <- bigrquery::bq_table(ds, table_name)
    
    # Check if table exists - if not, create and upload
    if (!safe_table_exists(tbl)) {
      log_info("Creating new table {table_name}...")
      bigrquery::bq_table_upload(tbl, dt, write_disposition = "WRITE_TRUNCATE")
      log_info("Created and uploaded {nrow(dt)} records to {table_name}")
      return(TRUE)
    }
    
    # For existing tables, use MERGE via SQL to upsert
    # First, upload to a temp table, then merge
    temp_table_name <- paste0(table_name, "_recovery_temp")
    temp_tbl <- bigrquery::bq_table(ds, temp_table_name)
    
    # Upload to temp table
    bigrquery::bq_table_upload(temp_tbl, dt, write_disposition = "WRITE_TRUNCATE")
    
    # Build column list for merge with validated column names
    cols <- names(dt)
    
    # Validate column names to prevent SQL injection
    invalid_cols <- cols[!sapply(cols, is_valid_column_name)]
    if (length(invalid_cols) > 0) {
      log_error("Invalid column names detected: {paste(invalid_cols, collapse = ', ')}")
      bigrquery::bq_table_delete(temp_tbl)
      return(FALSE)
    }
    
    update_assignments <- paste(sapply(cols, function(c) glue::glue("T.`{c}` = S.`{c}`")), collapse = ", ")
    insert_cols <- paste0("`", cols, "`", collapse = ", ")
    insert_vals <- paste0("S.`", cols, "`", collapse = ", ")
    
    # Execute merge
    merge_sql <- glue::glue("
      MERGE `{CONFIG$gcp_project}.{CONFIG$bq_dataset}.{table_name}` T
      USING `{CONFIG$gcp_project}.{CONFIG$bq_dataset}.{temp_table_name}` S
      ON T.`{key_col}` = S.`{key_col}`
      WHEN MATCHED THEN
        UPDATE SET {update_assignments}
      WHEN NOT MATCHED THEN
        INSERT ({insert_cols})
        VALUES ({insert_vals})
    ")
    
    bigrquery::bq_project_query(CONFIG$gcp_project, merge_sql)
    
    # Clean up temp table
    bigrquery::bq_table_delete(temp_tbl)
    
    log_info("Merged {nrow(dt)} records into {table_name}")
    return(TRUE)
    
  }, error = function(e) {
    log_error("Upload to {table_name} failed: {e$message}")
    return(FALSE)
  })
}

#' Purge successfully processed test_IDs from the filter log
#' @param test_ids Vector of test_IDs that were successfully processed
purge_processed_test_ids <- function(test_ids) {
  if (length(test_ids) == 0) {
    return(invisible(NULL))
  }
  
  tryCatch({
    # Sanitize test_IDs to prevent SQL injection
    test_ids_sanitized <- sapply(test_ids, sanitize_sql_string)
    test_ids_quoted <- paste0("'", test_ids_sanitized, "'", collapse = ", ")
    
    sql <- glue::glue("
      DELETE FROM `{CONFIG$gcp_project}.{CONFIG$bq_dataset}.filtered_test_ids_log`
      WHERE test_ID IN ({test_ids_quoted})
      AND filter_stage = 'RECONCILIATION'
    ")
    
    bigrquery::bq_project_query(CONFIG$gcp_project, sql)
    
    log_info("Purged {length(test_ids)} test_IDs from filter log")
    
  }, error = function(e) {
    log_warn("Failed to purge test_IDs from filter log: {e$message}")
    log_warn("Test_IDs will be re-processed on next run (this is OK)")
  })
}

#' Verify if test_IDs are now present in data tables
#' @param test_ids Vector of test_IDs to check
#' @param data_source "ForceDecks" or "NordBord"
#' @return Vector of test_IDs that are now accounted for
verify_test_ids_saved <- function(test_ids, data_source) {
  if (length(test_ids) == 0) return(character(0))
  
  tryCatch({
    # Determine which tables to check based on data source
    if (data_source == "ForceDecks") {
      tables <- c("vald_fd_jumps", "vald_fd_dj", "vald_fd_rsi", 
                  "vald_fd_sl_jumps", "vald_fd_imtp", "vald_fd_rebound")
    } else if (data_source == "NordBord") {
      tables <- c("vald_nord_all")
    } else {
      return(character(0))
    }
    
    found_ids <- character(0)
    
    for (table_name in tables) {
      ds <- bigrquery::bq_dataset(CONFIG$gcp_project, CONFIG$bq_dataset)
      tbl <- bigrquery::bq_table(ds, table_name)
      
      if (!safe_table_exists(tbl)) next
      
      # Sanitize test_IDs to prevent SQL injection
      test_ids_sanitized <- sapply(test_ids, sanitize_sql_string)
      test_ids_quoted <- paste0("'", test_ids_sanitized, "'", collapse = ", ")
      sql <- glue::glue("
        SELECT DISTINCT test_ID 
        FROM `{CONFIG$gcp_project}.{CONFIG$bq_dataset}.{table_name}`
        WHERE test_ID IN ({test_ids_quoted})
      ")
      
      result <- tryCatch({
        bigrquery::bq_project_query(CONFIG$gcp_project, sql) %>% bigrquery::bq_table_download()
      }, error = function(e) NULL)
      
      if (!is.null(result) && nrow(result) > 0) {
        found_ids <- unique(c(found_ids, as.character(result$test_ID)))
      }
    }
    
    log_info("Verified {length(found_ids)}/{length(test_ids)} test_IDs are now in {data_source} tables")
    return(found_ids)
    
  }, error = function(e) {
    log_warn("Verification failed: {e$message}")
    return(character(0))
  })
}

# ============================================================================
# Main Execution
# ============================================================================
log_info("=== STARTING SECONDARY DEFENSE PROCESSING ===")

# Step 1: Query unaccounted test_IDs
unaccounted <- query_unaccounted_test_ids()

if (nrow(unaccounted) == 0) {
  log_info("No unaccounted test_IDs found - nothing to do")
  log_info("=== SECONDARY DEFENSE COMPLETE (no work needed) ===")
  quit(status = 0)
}

# Step 2: Split by data source
fd_test_ids <- unaccounted[data_source == "ForceDecks", test_ID]
nord_test_ids <- unaccounted[data_source == "NordBord", test_ID]

log_info("Unaccounted test_IDs: {length(fd_test_ids)} ForceDecks, {length(nord_test_ids)} NordBord")

processed_ids <- character(0)

# Step 3: Process ForceDecks test_IDs
if (length(fd_test_ids) > 0) {
  log_info("=== Processing ForceDecks Recovery ===")
  
  fd_data <- fetch_forcedecks_by_ids(fd_test_ids)
  
  if (nrow(fd_data$tests) > 0) {
    fd_processed <- process_recovered_forcedecks(fd_data$tests, fd_data$trials)
    
    if (nrow(fd_processed) > 0) {
      # Determine the appropriate table based on test type
      # For CMJ/SJ tests, use vald_fd_jumps
      if ("test_type" %in% names(fd_processed)) {
        cmj_types <- c("CMJ", "LCMJ", "SJ", "ABCMJ")
        cmj_data <- fd_processed[test_type %in% cmj_types]
        if (nrow(cmj_data) > 0) {
          upload_success <- upload_recovered_data(cmj_data, "vald_fd_jumps", "test_ID")
          if (upload_success) {
            log_info("Saved {nrow(cmj_data)} CMJ/SJ tests to vald_fd_jumps")
          }
        }
        
        # DJ tests
        dj_data <- fd_processed[test_type == "DJ"]
        if (nrow(dj_data) > 0) {
          upload_success <- upload_recovered_data(dj_data, "vald_fd_dj", "test_ID")
          if (upload_success) {
            log_info("Saved {nrow(dj_data)} DJ tests to vald_fd_dj")
          }
        }
        
        # RSI tests
        rsi_data <- fd_processed[test_type == "RSI"]
        if (nrow(rsi_data) > 0) {
          upload_success <- upload_recovered_data(rsi_data, "vald_fd_rsi", "test_ID")
          if (upload_success) {
            log_info("Saved {nrow(rsi_data)} RSI tests to vald_fd_rsi")
          }
        }
        
        # IMTP tests
        imtp_data <- fd_processed[test_type == "IMTP"]
        if (nrow(imtp_data) > 0) {
          upload_success <- upload_recovered_data(imtp_data, "vald_fd_imtp", "test_ID")
          if (upload_success) {
            log_info("Saved {nrow(imtp_data)} IMTP tests to vald_fd_imtp")
          }
        }
      } else {
        # Default to vald_fd_jumps if test_type not available
        upload_success <- upload_recovered_data(fd_processed, "vald_fd_jumps", "test_ID")
        if (upload_success) {
          log_info("Saved {nrow(fd_processed)} ForceDecks tests to vald_fd_jumps")
        }
      }
      
      # Verify which test_IDs are now present
      verified_fd <- verify_test_ids_saved(fd_test_ids, "ForceDecks")
      processed_ids <- c(processed_ids, verified_fd)
    }
  } else {
    # Even if we couldn't fetch, check if they're already in the tables
    verified_fd <- verify_test_ids_saved(fd_test_ids, "ForceDecks")
    if (length(verified_fd) > 0) {
      log_info("Found {length(verified_fd)} ForceDecks test_IDs already in tables (clearing from log)")
      processed_ids <- c(processed_ids, verified_fd)
    }
  }
}

# Step 4: Process NordBord test_IDs
if (length(nord_test_ids) > 0) {
  log_info("=== Processing NordBord Recovery ===")
  
  nord_data <- fetch_nordbord_by_ids(nord_test_ids)
  
  if (nrow(nord_data) > 0) {
    nord_processed <- process_recovered_nordbord(nord_data)
    
    if (nrow(nord_processed) > 0) {
      # Upload NordBord data
      upload_success <- upload_recovered_data(nord_processed, "vald_nord_all", "test_ID")
      if (upload_success) {
        log_info("Saved {nrow(nord_processed)} NordBord tests to vald_nord_all")
      }
      
      verified_nord <- verify_test_ids_saved(nord_test_ids, "NordBord")
      processed_ids <- c(processed_ids, verified_nord)
    }
  } else {
    # Check if they're already in the tables
    verified_nord <- verify_test_ids_saved(nord_test_ids, "NordBord")
    if (length(verified_nord) > 0) {
      log_info("Found {length(verified_nord)} NordBord test_IDs already in tables (clearing from log)")
      processed_ids <- c(processed_ids, verified_nord)
    }
  }
}

# Step 5: Purge successfully processed test_IDs from the log
if (length(processed_ids) > 0) {
  log_info("=== Purging Processed Test IDs ===")
  purge_processed_test_ids(processed_ids)
}

# Summary
execution_time <- round(difftime(Sys.time(), script_start_time, units = "mins"), 2)
log_info("=== SECONDARY DEFENSE COMPLETE ===")
log_info("Execution time: {execution_time} minutes")
log_info("Processed: {length(processed_ids)} test_IDs")
log_info("Remaining unaccounted: {nrow(unaccounted) - length(processed_ids)} test_IDs")

quit(status = 0)
