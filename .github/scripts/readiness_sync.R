#!/usr/bin/env Rscript

# ═══════════════════════════════════════════════════════════════════════════
# READINESS DATA SYNC SCRIPT
# ═══════════════════════════════════════════════════════════════════════════
# Purpose: Sync VALD readiness data from sac-vald-hub to sac-ml-models
# Usage:   Rscript readiness_sync.R [--force] [--check-only]
# ═══════════════════════════════════════════════════════════════════════════

# ===== Packages =====
tryCatch({
  suppressPackageStartupMessages({
    library(bigrquery); library(DBI)
    library(dplyr); library(glue)
    library(gargle); library(lubridate)
  })
}, error = function(e) { 
  cat("Error loading packages:", conditionMessage(e), "\n")
  quit(status=1)
})

# Disable BigQuery Storage API
options(bigrquery.use_bqstorage = FALSE)
Sys.setenv(BIGRQUERY_USE_BQ_STORAGE = "false")

# ===== Configuration =====
# Source (where readiness data originates)
SOURCE_PROJECT   <- Sys.getenv("GCP_PROJECT_READINESS", "sac-vald-hub")
SOURCE_DATASET   <- Sys.getenv("BQ_DATASET", "analytics")
SOURCE_TABLE     <- Sys.getenv("READINESS_TABLE", "vald_fd_jumps")

# Destination (where ML models run)
DEST_PROJECT     <- Sys.getenv("GCP_PROJECT", "sac-ml-models")
DEST_DATASET     <- Sys.getenv("BQ_DATASET", "analytics")
DEST_TABLE       <- Sys.getenv("READINESS_TABLE", "vald_fd_jumps")

LOCATION         <- Sys.getenv("BQ_LOCATION", "US")

# Parse command line arguments
args <- commandArgs(trailingOnly = TRUE)
FORCE_SYNC    <- "--force" %in% args
CHECK_ONLY    <- "--check-only" %in% args
SKIP_IF_FRESH <- !FORCE_SYNC  # Skip sync if data is already fresh

cat("═══════════════════════════════════════════════════════════════\n")
cat("  READINESS DATA SYNC: sac-vald-hub → sac-ml-models\n")
cat("═══════════════════════════════════════════════════════════════\n\n")

cat("Source:      ", glue("{SOURCE_PROJECT}.{SOURCE_DATASET}.{SOURCE_TABLE}"), "\n")
cat("Destination: ", glue("{DEST_PROJECT}.{DEST_DATASET}.{DEST_TABLE}"), "\n")
cat("Force sync:  ", FORCE_SYNC, "\n")
cat("Check only:  ", CHECK_ONLY, "\n\n")

# ===== Logging =====
log_entries <- data.frame(
  timestamp = as.POSIXct(character(0)),
  level = character(0),
  message = character(0),
  stringsAsFactors = FALSE
)

log_msg <- function(message, level = "INFO") {
  ts <- Sys.time()
  cat(sprintf("[%s] [%s] %s\n", 
              format(ts, "%Y-%m-%d %H:%M:%S", tz = "UTC"), 
              level, 
              message))
  log_entries <<- rbind(log_entries, data.frame(
    timestamp = ts,
    level = level,
    message = message,
    stringsAsFactors = FALSE
  ))
}

# ===== Authentication =====
log_msg("Authenticating to BigQuery...")

# Primary auth (destination project - for writes)
DEST_TOKEN <- NULL
tryCatch({
  tok <- system("gcloud auth print-access-token", intern = TRUE)
  DEST_TOKEN <<- tok[1]
  stopifnot(nzchar(DEST_TOKEN))
  
  bq_auth(token = gargle::gargle2.0_token(
    scope = 'https://www.googleapis.com/auth/bigquery',
    client = gargle::gargle_client(),
    credentials = list(access_token = DEST_TOKEN)
  ))
  
  log_msg(glue("Authenticated to destination project: {DEST_PROJECT}"))
}, error = function(e) {
  log_msg(paste("Authentication to destination failed:", conditionMessage(e)), "ERROR")
  quit(status = 1)
})

# Source auth (readiness project - for reads)
SOURCE_TOKEN <- NULL
SOURCE_CREDS_PATH <- Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS_READINESS", "")

if (nzchar(SOURCE_CREDS_PATH) && file.exists(SOURCE_CREDS_PATH)) {
  tryCatch({
    cmd <- sprintf("gcloud auth print-access-token --key-file='%s'", SOURCE_CREDS_PATH)
    tok <- system(cmd, intern = TRUE)
    SOURCE_TOKEN <<- tok[1]
    
    if (nzchar(SOURCE_TOKEN)) {
      log_msg(glue("Authenticated to source project: {SOURCE_PROJECT}"))
    } else {
      log_msg("Warning: Could not get source project token", "WARN")
      log_msg("Will attempt to use destination credentials for both", "WARN")
      SOURCE_TOKEN <<- DEST_TOKEN
    }
  }, error = function(e) {
    log_msg(paste("Source auth warning:", conditionMessage(e)), "WARN")
    SOURCE_TOKEN <<- DEST_TOKEN
  })
} else {
  log_msg("No separate source credentials provided - using destination credentials", "INFO")
  SOURCE_TOKEN <- DEST_TOKEN
}

# ===== Helper Functions =====

# Get table metadata (row count, latest date, column count)
get_table_info <- function(project, dataset, table, use_source_token = FALSE) {
  tryCatch({
    # Set appropriate token
    token_to_use <- if (use_source_token && !is.null(SOURCE_TOKEN)) SOURCE_TOKEN else DEST_TOKEN
    
    bq_auth(token = gargle::gargle2.0_token(
      scope = 'https://www.googleapis.com/auth/bigquery',
      client = gargle::gargle_client(),
      credentials = list(access_token = token_to_use)
    ))
    
    tbl <- bq_table(project, dataset, table)
    
    if (!bq_table_exists(tbl)) {
      return(list(
        exists = FALSE,
        num_rows = 0,
        max_date = NA,
        num_columns = 0
      ))
    }
    
    # Get metadata
    meta <- bq_table_meta(tbl)
    num_rows <- as.numeric(meta$numRows %||% "0")
    num_columns <- length(meta$schema$fields)
    
    # Get latest date
    sql <- glue("SELECT MAX(DATE(date)) as max_date FROM `{project}.{dataset}.{table}`")
    result <- tryCatch({
      bq_project_query(project, sql) %>% bq_table_download()
    }, error = function(e) {
      log_msg(paste("Could not query max date:", conditionMessage(e)), "WARN")
      data.frame(max_date = NA)
    })
    
    max_date <- if (nrow(result) > 0 && !is.na(result$max_date[1])) {
      as.Date(result$max_date[1])
    } else {
      as.Date(NA)
    }
    
    list(
      exists = TRUE,
      num_rows = num_rows,
      max_date = max_date,
      num_columns = num_columns
    )
    
  }, error = function(e) {
    log_msg(paste("Error getting table info:", conditionMessage(e)), "ERROR")
    list(
      exists = FALSE,
      num_rows = 0,
      max_date = NA,
      num_columns = 0,
      error = conditionMessage(e)
    )
  })
}

# Check if sync is needed
check_sync_needed <- function(source_info, dest_info) {
  # If destination doesn't exist, sync is needed
  if (!dest_info$exists) {
    return(list(
      needed = TRUE,
      reason = "Destination table does not exist"
    ))
  }
  
  # If source doesn't exist, sync is not possible
  if (!source_info$exists) {
    return(list(
      needed = FALSE,
      reason = "Source table does not exist"
    ))
  }
  
  # Compare dates
  if (is.na(source_info$max_date) && is.na(dest_info$max_date)) {
    return(list(
      needed = FALSE,
      reason = "Both tables have no valid dates"
    ))
  }
  
  if (is.na(dest_info$max_date)) {
    return(list(
      needed = TRUE,
      reason = "Destination has no valid dates"
    ))
  }
  
  if (is.na(source_info$max_date)) {
    return(list(
      needed = FALSE,
      reason = "Source has no valid dates"
    ))
  }
  
  if (source_info$max_date > dest_info$max_date) {
    days_behind <- as.numeric(source_info$max_date - dest_info$max_date)
    return(list(
      needed = TRUE,
      reason = glue("Destination is {days_behind} days behind source")
    ))
  }
  
  # Check column count differences
  if (source_info$num_columns != dest_info$num_columns) {
    return(list(
      needed = TRUE,
      reason = glue("Column count mismatch: source={source_info$num_columns}, dest={dest_info$num_columns}")
    ))
  }
  
  return(list(
    needed = FALSE,
    reason = "Data is already up to date"
  ))
}

# Copy table using BigQuery transfer
copy_table_bq <- function() {
  tryCatch({
    log_msg("Starting table copy via BigQuery...")
    
    # Use destination credentials for the copy operation
    bq_auth(token = gargle::gargle2.0_token(
      scope = 'https://www.googleapis.com/auth/bigquery',
      client = gargle::gargle_client(),
      credentials = list(access_token = DEST_TOKEN)
    ))
    
    source_tbl <- bq_table(SOURCE_PROJECT, SOURCE_DATASET, SOURCE_TABLE)
    dest_tbl <- bq_table(DEST_PROJECT, DEST_DATASET, DEST_TABLE)
    
    # Ensure destination dataset exists
    dest_ds <- bq_dataset(DEST_PROJECT, DEST_DATASET)
    if (!bq_dataset_exists(dest_ds)) {
      bq_dataset_create(dest_ds, location = LOCATION)
      log_msg(glue("Created destination dataset: {DEST_DATASET}"))
    }
    
    # Delete destination table if it exists
    if (bq_table_exists(dest_tbl)) {
      bq_table_delete(dest_tbl)
      log_msg("Deleted existing destination table")
    }
    
    # Copy via SQL query
    sql <- glue("
      CREATE TABLE `{DEST_PROJECT}.{DEST_DATASET}.{DEST_TABLE}` AS
      SELECT * FROM `{SOURCE_PROJECT}.{SOURCE_DATASET}.{SOURCE_TABLE}`
    ")
    
    job <- bq_project_query(DEST_PROJECT, sql)
    bq_job_wait(job)
    
    log_msg("Table copy completed successfully", "SUCCESS")
    return(TRUE)
    
  }, error = function(e) {
    log_msg(paste("Table copy failed:", conditionMessage(e)), "ERROR")
    
    # Try alternative method: download and upload
    log_msg("Attempting alternative method: download and upload...", "WARN")
    tryCatch({
      # Switch to source credentials for reading
      bq_auth(token = gargle::gargle2.0_token(
        scope = 'https://www.googleapis.com/auth/bigquery',
        client = gargle::gargle_client(),
        credentials = list(access_token = SOURCE_TOKEN)
      ))
      
      source_tbl <- bq_table(SOURCE_PROJECT, SOURCE_DATASET, SOURCE_TABLE)
      data <- bq_table_download(source_tbl, n_max = Inf)
      log_msg(glue("Downloaded {nrow(data)} rows from source"))
      
      # Switch to destination credentials for writing
      bq_auth(token = gargle::gargle2.0_token(
        scope = 'https://www.googleapis.com/auth/bigquery',
        client = gargle::gargle_client(),
        credentials = list(access_token = DEST_TOKEN)
      ))
      
      dest_tbl <- bq_table(DEST_PROJECT, DEST_DATASET, DEST_TABLE)
      bq_table_upload(dest_tbl, data, write_disposition = "WRITE_TRUNCATE")
      log_msg("Upload completed successfully", "SUCCESS")
      return(TRUE)
      
    }, error = function(e2) {
      log_msg(paste("Alternative method also failed:", conditionMessage(e2)), "ERROR")
      return(FALSE)
    })
  })
}

# ===== Main Execution =====

log_msg("=== STEP 1: Check Source Table ===")
source_info <- get_table_info(SOURCE_PROJECT, SOURCE_DATASET, SOURCE_TABLE, use_source_token = TRUE)

if (!source_info$exists) {
  log_msg("❌ Source table does not exist or is inaccessible", "ERROR")
  log_msg(glue("   Table: {SOURCE_PROJECT}.{SOURCE_DATASET}.{SOURCE_TABLE}"), "ERROR")
  if (!is.null(source_info$error)) {
    log_msg(glue("   Error: {source_info$error}"), "ERROR")
  }
  log_msg("\nTroubleshooting:", "ERROR")
  log_msg("  1. Verify the table exists in sac-vald-hub", "ERROR")
  log_msg("  2. Check service account has bigquery.dataViewer role", "ERROR")
  log_msg("  3. Verify GOOGLE_APPLICATION_CREDENTIALS_READINESS is set", "ERROR")
  quit(status = 1)
}

cat("\n📊 Source Table Info:\n")
cat(glue("   Rows:        {format(source_info$num_rows, big.mark=',')}"), "\n")
cat(glue("   Columns:     {source_info$num_columns}"), "\n")
cat(glue("   Latest date: {if (!is.na(source_info$max_date)) format(source_info$max_date, '%Y-%m-%d') else 'N/A'}"), "\n")

log_msg("\n=== STEP 2: Check Destination Table ===")
dest_info <- get_table_info(DEST_PROJECT, DEST_DATASET, DEST_TABLE, use_source_token = FALSE)

if (dest_info$exists) {
  cat("\n📊 Destination Table Info:\n")
  cat(glue("   Rows:        {format(dest_info$num_rows, big.mark=',')}"), "\n")
  cat(glue("   Columns:     {dest_info$num_columns}"), "\n")
  cat(glue("   Latest date: {if (!is.na(dest_info$max_date)) format(dest_info$max_date, '%Y-%m-%d') else 'N/A'}"), "\n")
} else {
  cat("\n⚠️  Destination table does not exist\n")
}

log_msg("\n=== STEP 3: Determine Sync Requirement ===")
sync_check <- check_sync_needed(source_info, dest_info)

cat(glue("\nSync needed: {if (sync_check$needed) '✅ YES' else '⏭️  NO'}"), "\n")
cat(glue("Reason: {sync_check$reason}"), "\n\n")

# Exit if check-only mode
if (CHECK_ONLY) {
  log_msg("Check-only mode - exiting without sync")
  cat("\n═══════════════════════════════════════════════════════════════\n")
  if (sync_check$needed) {
    quit(status = 0)  # Exit 0 but sync is needed
  } else {
    quit(status = 0)  # Exit 0 and sync not needed
  }
}

# Exit if sync not needed and not forced
if (!sync_check$needed && SKIP_IF_FRESH) {
  log_msg("Data is already up to date - skipping sync", "SUCCESS")
  cat("\n✅ No sync required\n")
  cat("\nTo force sync anyway, use: Rscript readiness_sync.R --force\n")
  cat("\n═══════════════════════════════════════════════════════════════\n")
  quit(status = 0)
}

# Perform sync
if (FORCE_SYNC) {
  log_msg("Force sync enabled - syncing regardless of freshness", "WARN")
}

log_msg("=== STEP 4: Sync Data ===")
success <- copy_table_bq()

if (!success) {
  log_msg("❌ Sync failed", "ERROR")
  cat("\n═══════════════════════════════════════════════════════════════\n")
  quit(status = 1)
}

# Verify sync
log_msg("=== STEP 5: Verify Sync ===")
final_info <- get_table_info(DEST_PROJECT, DEST_DATASET, DEST_TABLE, use_source_token = FALSE)

cat("\n📊 Final Destination Table Info:\n")
cat(glue("   Rows:        {format(final_info$num_rows, big.mark=',')}"), "\n")
cat(glue("   Columns:     {final_info$num_columns}"), "\n")
cat(glue("   Latest date: {if (!is.na(final_info$max_date)) format(final_info$max_date, '%Y-%m-%d') else 'N/A'}"), "\n\n")

if (final_info$exists && final_info$num_rows > 0) {
  log_msg("✅ Sync completed successfully!", "SUCCESS")
  cat("\n✅ SUCCESS: Readiness data synced to sac-ml-models\n")
} else {
  log_msg("⚠️  Sync may have issues - destination table empty or missing", "WARN")
  cat("\n⚠️  WARNING: Destination table may not be properly populated\n")
}

cat("\n═══════════════════════════════════════════════════════════════\n")
