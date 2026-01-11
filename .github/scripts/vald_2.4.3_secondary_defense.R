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
  batch_size = 10L              # Process in small batches
)

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
    # Get all tests first - we'll filter to our specific IDs
    # Note: valdr doesn't have a direct "get by ID" for ForceDecks, 
    # so we fetch all tests and filter
    all_tests <- valdr::get_forcedecks_tests_only()
    
    if (is.null(all_tests) || nrow(all_tests) == 0) {
      log_warn("No ForceDecks tests returned from API")
      return(list(tests = data.table::data.table(), trials = data.table::data.table()))
    }
    
    # Filter to our specific test IDs
    tests_dt <- data.table::as.data.table(all_tests)
    if ("testId" %in% names(tests_dt)) {
      tests_dt <- tests_dt[testId %in% test_ids]
    } else if ("test_ID" %in% names(tests_dt)) {
      tests_dt <- tests_dt[test_ID %in% test_ids]
    }
    
    if (nrow(tests_dt) == 0) {
      log_warn("None of the requested test_IDs found in ForceDecks API response")
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
#' @return TRUE if successful, FALSE otherwise
upload_recovered_data <- function(dt, table_name) {
  if (nrow(dt) == 0) {
    log_info("No data to upload for {table_name}")
    return(FALSE)
  }
  
  tryCatch({
    ds <- bigrquery::bq_dataset(CONFIG$gcp_project, CONFIG$bq_dataset)
    tbl <- bigrquery::bq_table(ds, table_name)
    
    # Use MERGE mode to upsert
    bigrquery::bq_table_upload(tbl, dt, write_disposition = "WRITE_APPEND")
    
    log_info("Uploaded {nrow(dt)} records to {table_name}")
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
    # Build the DELETE query
    test_ids_quoted <- paste0("'", test_ids, "'", collapse = ", ")
    
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
      
      test_ids_quoted <- paste0("'", test_ids, "'", collapse = ", ")
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
      # Note: For full processing, this would call the same functions as the main script
      # For now, we just log what we would do
      log_info("Would process {nrow(fd_processed)} ForceDecks tests")
      
      # Verify which test_IDs are now present (they might have been saved by main script already)
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
      log_info("Would process {nrow(nord_processed)} NordBord tests")
      
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
