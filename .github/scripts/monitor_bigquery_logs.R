#!/usr/bin/env Rscript
# ============================================================================
# BigQuery Log Monitor Script
# ============================================================================
# Purpose: Read BigQuery logs for the last 3 days and summarize errors/issues
# Execution: GitHub Actions (every 15 minutes)
# Output: Summary of errors and issues to console/workflow logs
# ============================================================================

suppressPackageStartupMessages({
  library(bigrquery)
  library(dplyr)
  library(tidyr)
  library(lubridate)
  library(stringr)
  library(data.table)
})

# ============================================================================
# Configuration
# ============================================================================
CONFIG <- list(
  gcp_project = Sys.getenv("GCP_PROJECT", "sac-vald-hub"),
  bq_dataset = Sys.getenv("BQ_DATASET", "analytics"),
  bq_location = Sys.getenv("BQ_LOCATION", "US"),
  lookback_days = as.integer(Sys.getenv("LOOKBACK_DAYS", "3")),
  log_table = "vald_processing_log"
)

cat("=================================================================\n")
cat("     BigQuery Log Monitor - Last", CONFIG$lookback_days, "Days\n")
cat("=================================================================\n")
cat("Project:", CONFIG$gcp_project, "\n")
cat("Dataset:", CONFIG$bq_dataset, "\n")
cat("Table:", CONFIG$log_table, "\n")
cat("Lookback Period:", CONFIG$lookback_days, "days\n")
cat("Current Time (UTC):", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"), "\n")
cat("=================================================================\n\n")

# ============================================================================
# BigQuery Authentication
# ============================================================================
bq_auth_check <- function() {
  tryCatch({
    # Test connection
    ds <- bq_dataset(CONFIG$gcp_project, CONFIG$bq_dataset)
    log_tbl <- bq_table(ds, CONFIG$log_table)
    
    # Check if table exists
    if (!bq_table_exists(log_tbl)) {
      cat("⚠️  WARNING: Log table does not exist yet\n")
      cat("   Table:", paste0(CONFIG$gcp_project, ".", CONFIG$bq_dataset, ".", CONFIG$log_table), "\n")
      cat("   Logs will be created after first workflow run\n\n")
      return(FALSE)
    }
    
    cat("✅ BigQuery connection established\n")
    cat("✅ Log table exists\n\n")
    return(TRUE)
    
  }, error = function(e) {
    cat("❌ ERROR: BigQuery authentication failed\n")
    cat("   Message:", e$message, "\n\n")
    return(FALSE)
  })
}

# ============================================================================
# Query Logs from BigQuery
# ============================================================================
query_logs <- function() {
  cat("📊 Querying logs from BigQuery...\n")
  
  cutoff_date <- Sys.time() - days(CONFIG$lookback_days)
  
  query <- sprintf("
    SELECT 
      timestamp,
      level,
      message,
      run_id,
      repository
    FROM `%s.%s.%s`
    WHERE timestamp >= TIMESTAMP('%s')
    ORDER BY timestamp DESC
  ", 
    CONFIG$gcp_project, 
    CONFIG$bq_dataset, 
    CONFIG$log_table,
    format(cutoff_date, "%Y-%m-%d %H:%M:%S")
  )
  
  tryCatch({
    logs <- bq_project_query(
      CONFIG$gcp_project, 
      query,
      use_legacy_sql = FALSE
    ) %>%
      bq_table_download()
    
    cat("   Retrieved:", nrow(logs), "log entries\n")
    
    if (nrow(logs) > 0) {
      cat("   Date Range:", 
          format(min(logs$timestamp), "%Y-%m-%d %H:%M"),
          "to",
          format(max(logs$timestamp), "%Y-%m-%d %H:%M"), "\n\n")
    } else {
      cat("   No logs found in the specified time range\n\n")
    }
    
    return(logs)
    
  }, error = function(e) {
    cat("❌ ERROR: Failed to query logs\n")
    cat("   Message:", e$message, "\n\n")
    return(NULL)
  })
}

# ============================================================================
# Analyze Logs for Errors and Issues
# ============================================================================
analyze_logs <- function(logs) {
  if (is.null(logs) || nrow(logs) == 0) {
    cat("⚠️  No logs found for analysis\n\n")
    return(NULL)
  }
  
  cat("🔍 Analyzing logs for errors and issues...\n\n")
  
  # Convert to data.table for better performance
  logs_dt <- as.data.table(logs)
  
  # ===== ERROR LOGS =====
  errors <- logs_dt[level == "ERROR" | grepl("(?i)error|fail|exception", message)]
  
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  cat("   ERRORS & FAILURES\n")
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")
  
  if (nrow(errors) > 0) {
    cat("Total Errors Found:", nrow(errors), "\n\n")
    
    # Group by error type
    error_patterns <- errors %>%
      mutate(
        error_type = case_when(
          grepl("(?i)authentication|auth|credentials", message) ~ "Authentication",
          grepl("(?i)timeout|timed out", message) ~ "Timeout",
          grepl("(?i)api|vald api", message) ~ "API Error",
          grepl("(?i)bigquery|bq", message) ~ "BigQuery Error",
          grepl("(?i)connection|network", message) ~ "Connection",
          grepl("(?i)missing|not found|does not exist", message) ~ "Missing Data/Resource",
          grepl("(?i)invalid|incorrect", message) ~ "Invalid Data",
          TRUE ~ "Other Error"
        )
      ) %>%
      group_by(error_type) %>%
      summarise(
        count = n(),
        latest = max(timestamp),
        .groups = "drop"
      ) %>%
      arrange(desc(count))
    
    cat("📋 Error Summary by Type:\n")
    print(error_patterns, n = Inf)
    cat("\n")
    
    # Show recent errors
    cat("🕐 Most Recent Errors (Last 10):\n")
    recent_errors <- errors %>%
      arrange(desc(timestamp)) %>%
      head(10) %>%
      select(timestamp, level, message, run_id) %>%
      mutate(
        timestamp = format(timestamp, "%Y-%m-%d %H:%M:%S"),
        message = str_trunc(message, 80)
      )
    
    print(recent_errors, n = Inf)
    cat("\n")
    
  } else {
    cat("✅ No errors found in the last", CONFIG$lookback_days, "days\n\n")
  }
  
  # ===== WARNING LOGS =====
  warnings <- logs_dt[level == "WARN" | grepl("(?i)warning|warn", message)]
  
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  cat("   WARNINGS\n")
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")
  
  if (nrow(warnings) > 0) {
    cat("Total Warnings Found:", nrow(warnings), "\n\n")
    
    # Show recent warnings
    cat("🕐 Most Recent Warnings (Last 10):\n")
    recent_warnings <- warnings %>%
      arrange(desc(timestamp)) %>%
      head(10) %>%
      select(timestamp, level, message, run_id) %>%
      mutate(
        timestamp = format(timestamp, "%Y-%m-%d %H:%M:%S"),
        message = str_trunc(message, 80)
      )
    
    print(recent_warnings, n = Inf)
    cat("\n")
    
  } else {
    cat("✅ No warnings found in the last", CONFIG$lookback_days, "days\n\n")
  }
  
  # ===== RUN STATISTICS =====
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  cat("   WORKFLOW RUN STATISTICS\n")
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")
  
  runs_summary <- logs_dt %>%
    group_by(run_id) %>%
    summarise(
      start_time = min(timestamp),
      end_time = max(timestamp),
      num_logs = n(),
      num_errors = sum(level == "ERROR" | grepl("(?i)error|fail", message)),
      num_warnings = sum(level == "WARN" | grepl("(?i)warning", message)),
      .groups = "drop"
    ) %>%
    mutate(
      duration_min = as.numeric(difftime(end_time, start_time, units = "mins")),
      status = case_when(
        num_errors > 0 ~ "❌ FAILED",
        num_warnings > 0 ~ "⚠️  WITH WARNINGS",
        TRUE ~ "✅ SUCCESS"
      )
    ) %>%
    arrange(desc(start_time))
  
  cat("Total Workflow Runs:", nrow(runs_summary), "\n")
  cat("Runs with Errors:", sum(runs_summary$num_errors > 0), "\n")
  cat("Runs with Warnings:", sum(runs_summary$num_warnings > 0), "\n")
  cat("Successful Runs:", sum(runs_summary$num_errors == 0 & runs_summary$num_warnings == 0), "\n\n")
  
  cat("📋 Recent Workflow Runs (Last 15):\n")
  recent_runs <- runs_summary %>%
    head(15) %>%
    select(run_id, start_time, duration_min, num_errors, num_warnings, status) %>%
    mutate(
      start_time = format(start_time, "%Y-%m-%d %H:%M"),
      duration_min = round(duration_min, 1)
    )
  
  print(recent_runs, n = Inf)
  cat("\n")
  
  # ===== ISSUE PATTERNS =====
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  cat("   COMMON ISSUE PATTERNS\n")
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")
  
  # Detect common patterns
  issue_logs <- logs_dt[grepl("(?i)timeout|failed|error|skipped|no data|missing", message)]
  
  if (nrow(issue_logs) > 0) {
    # Extract key phrases
    common_issues <- issue_logs %>%
      mutate(
        issue = case_when(
          grepl("(?i)vald api.*timeout", message) ~ "VALD API Timeout",
          grepl("(?i)no.*data|no matching|no records", message) ~ "No Data Available",
          grepl("(?i)skipped|bypassed|disabled", message) ~ "Processing Skipped",
          grepl("(?i)authentication.*failed", message) ~ "Authentication Failed",
          grepl("(?i)table.*not.*exist", message) ~ "Missing Table",
          grepl("(?i)fetch.*failed", message) ~ "Data Fetch Failed",
          TRUE ~ "Other Issue"
        )
      ) %>%
      filter(issue != "Other Issue") %>%
      group_by(issue) %>%
      summarise(
        occurrences = n(),
        first_seen = min(timestamp),
        last_seen = max(timestamp),
        .groups = "drop"
      ) %>%
      arrange(desc(occurrences))
    
    if (nrow(common_issues) > 0) {
      cat("📋 Frequently Occurring Issues:\n")
      print(common_issues %>% mutate(
        first_seen = format(first_seen, "%Y-%m-%d %H:%M"),
        last_seen = format(last_seen, "%Y-%m-%d %H:%M")
      ), n = Inf)
      cat("\n")
    } else {
      cat("ℹ️  No common issue patterns detected\n\n")
    }
    
  } else {
    cat("✅ No issues detected in the logs\n\n")
  }
  
  # ===== DAILY SUMMARY =====
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
  cat("   DAILY SUMMARY\n")
  cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")
  
  daily_summary <- logs_dt %>%
    mutate(date = as.Date(timestamp)) %>%
    group_by(date) %>%
    summarise(
      total_logs = n(),
      num_runs = n_distinct(run_id),
      errors = sum(level == "ERROR" | grepl("(?i)error|fail", message)),
      warnings = sum(level == "WARN" | grepl("(?i)warning", message)),
      .groups = "drop"
    ) %>%
    arrange(desc(date))
  
  cat("📋 Activity by Day:\n")
  print(daily_summary, n = Inf)
  cat("\n")
  
  return(list(
    errors = errors,
    warnings = warnings,
    runs_summary = runs_summary,
    daily_summary = daily_summary
  ))
}

# ============================================================================
# Main Execution
# ============================================================================
main <- function() {
  cat("Starting BigQuery log monitoring...\n\n")
  
  # Check authentication
  if (!bq_auth_check()) {
    cat("❌ Cannot proceed without BigQuery access\n")
    cat("   Ensure GOOGLE_APPLICATION_CREDENTIALS is set correctly\n")
    return(invisible(1))
  }
  
  # Query logs
  logs <- query_logs()
  
  if (is.null(logs) || nrow(logs) == 0) {
    cat("⚠️  No logs available to analyze\n")
    cat("   This may be normal if workflows haven't run recently\n")
    return(invisible(0))
  }
  
  # Analyze logs
  analysis <- analyze_logs(logs)
  
  cat("=================================================================\n")
  cat("     Log Monitoring Complete\n")
  cat("=================================================================\n")
  cat("Summary generated at:", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"), "\n")
  cat("=================================================================\n")
  
  # Return exit code (0 = success, even if errors found in logs)
  return(invisible(0))
}

# Run the script
exit_code <- main()
quit(status = exit_code)
