#!/usr/bin/env Rscript

# ═══════════════════════════════════════════════════════════════════════════
# DATA FRESHNESS CHECKER
# ═══════════════════════════════════════════════════════════════════════════
# Purpose: Check data freshness across all tables in sac-ml-models
# Usage:   Rscript check_data_freshness.R [--json]
# ═══════════════════════════════════════════════════════════════════════════

# ===== Packages =====
tryCatch({
  suppressPackageStartupMessages({
    library(bigrquery); library(DBI)
    library(dplyr); library(glue); library(lubridate)
    library(gargle); library(jsonlite)
  })
}, error = function(e) { 
  cat("Error loading packages:", conditionMessage(e), "\n")
  quit(status=1)
})

options(bigrquery.use_bqstorage = FALSE)
Sys.setenv(BIGRQUERY_USE_BQ_STORAGE = "false")

# ===== Configuration =====
PROJECT  <- Sys.getenv("GCP_PROJECT", "sac-ml-models")
DATASET  <- Sys.getenv("BQ_DATASET", "analytics")
LOCATION <- Sys.getenv("BQ_LOCATION", "US")

# Parse arguments
args <- commandArgs(trailingOnly = TRUE)
JSON_OUTPUT <- "--json" %in% args

# Tables to check
TABLES <- list(
  list(
    name = "workload_daily",
    description = "GPS workload data",
    source = "GitHub repo",
    expected_update = "Daily (every 15 min)"
  ),
  list(
    name = "vald_fd_jumps",
    description = "VALD readiness data",
    source = "sac-vald-hub (synced)",
    expected_update = "Weekly (Saturday)"
  ),
  list(
    name = "roster_mapping",
    description = "Athlete roster mapping",
    source = "GitHub repo",
    expected_update = "Manual updates"
  ),
  list(
    name = "readiness_predictions_byname",
    description = "ML model predictions",
    source = "Model training pipeline",
    expected_update = "Daily (after training)"
  )
)

# ===== Authentication =====
ACCESS_TOKEN <- NULL
tryCatch({
  tok <- system("gcloud auth print-access-token", intern = TRUE)
  ACCESS_TOKEN <<- tok[1]
  stopifnot(nzchar(ACCESS_TOKEN))
  
  bq_auth(token = gargle::gargle2.0_token(
    scope = 'https://www.googleapis.com/auth/bigquery',
    client = gargle::gargle_client(),
    credentials = list(access_token = ACCESS_TOKEN)
  ))
}, error = function(e) {
  cat("Authentication failed:", conditionMessage(e), "\n")
  quit(status = 1)
})

# ===== Helper Functions =====

get_table_stats <- function(table_name) {
  tryCatch({
    tbl <- bq_table(PROJECT, DATASET, table_name)
    
    if (!bq_table_exists(tbl)) {
      return(list(
        table = table_name,
        exists = FALSE,
        num_rows = 0,
        min_date = NA,
        max_date = NA,
        date_range_days = NA,
        last_updated = NA,
        days_since_update = NA,
        status = "missing"
      ))
    }
    
    # Get metadata
    meta <- bq_table_meta(tbl)
    num_rows <- as.numeric(meta$numRows %||% "0")
    last_modified <- tryCatch({
      as.POSIXct(as.numeric(meta$lastModifiedTime) / 1000, origin = "1970-01-01", tz = "UTC")
    }, error = function(e) NA)
    
    # Get date range if table has date column
    date_stats <- tryCatch({
      sql <- glue("
        SELECT 
          MIN(DATE(date)) as min_date,
          MAX(DATE(date)) as max_date,
          COUNT(*) as row_count
        FROM `{PROJECT}.{DATASET}.{table_name}`
      ")
      result <- bq_project_query(PROJECT, sql) %>% bq_table_download()
      
      if (nrow(result) > 0) {
        list(
          min_date = as.Date(result$min_date[1]),
          max_date = as.Date(result$max_date[1]),
          row_count = as.numeric(result$row_count[1])
        )
      } else {
        list(min_date = NA, max_date = NA, row_count = 0)
      }
    }, error = function(e) {
      list(min_date = NA, max_date = NA, row_count = num_rows)
    })
    
    # Calculate freshness
    days_since_update <- if (!is.na(last_modified)) {
      as.numeric(difftime(Sys.time(), last_modified, units = "days"))
    } else {
      NA
    }
    
    days_since_latest_data <- if (!is.na(date_stats$max_date)) {
      as.numeric(difftime(Sys.Date(), date_stats$max_date, units = "days"))
    } else {
      NA
    }
    
    date_range_days <- if (!is.na(date_stats$min_date) && !is.na(date_stats$max_date)) {
      as.numeric(date_stats$max_date - date_stats$min_date)
    } else {
      NA
    }
    
    # Determine status
    status <- if (num_rows == 0) {
      "empty"
    } else if (!is.na(days_since_latest_data)) {
      if (days_since_latest_data == 0) "current"
      else if (days_since_latest_data <= 2) "recent"
      else if (days_since_latest_data <= 7) "stale"
      else "very_stale"
    } else {
      "unknown"
    }
    
    list(
      table = table_name,
      exists = TRUE,
      num_rows = num_rows,
      min_date = date_stats$min_date,
      max_date = date_stats$max_date,
      date_range_days = date_range_days,
      last_updated = last_modified,
      days_since_update = days_since_update,
      days_since_latest_data = days_since_latest_data,
      status = status
    )
    
  }, error = function(e) {
    list(
      table = table_name,
      exists = NA,
      num_rows = NA,
      error = conditionMessage(e),
      status = "error"
    )
  })
}

format_date <- function(d) {
  if (is.na(d)) return("N/A")
  if (inherits(d, "POSIXt")) return(format(d, "%Y-%m-%d %H:%M UTC"))
  if (inherits(d, "Date")) return(format(d, "%Y-%m-%d"))
  return(as.character(d))
}

format_number <- function(n) {
  if (is.na(n)) return("N/A")
  format(n, big.mark = ",", scientific = FALSE)
}

get_status_icon <- function(status) {
  switch(status,
    "current" = "✅",
    "recent" = "🟢",
    "stale" = "🟡",
    "very_stale" = "🔴",
    "empty" = "⚠️",
    "missing" = "❌",
    "error" = "❌",
    "unknown" = "❔"
  )
}

get_status_label <- function(status) {
  switch(status,
    "current" = "Current",
    "recent" = "Recent (1-2 days old)",
    "stale" = "Stale (3-7 days old)",
    "very_stale" = "Very Stale (>7 days old)",
    "empty" = "Empty",
    "missing" = "Missing",
    "error" = "Error",
    "unknown" = "Unknown"
  )
}

# ===== Main Execution =====

if (!JSON_OUTPUT) {
  cat("═══════════════════════════════════════════════════════════════\n")
  cat("  DATA FRESHNESS CHECK: sac-ml-models.analytics\n")
  cat("═══════════════════════════════════════════════════════════════\n\n")
  cat(glue("Timestamp: {format(Sys.time(), '%Y-%m-%d %H:%M:%S %Z')}"), "\n")
  cat(glue("Project:   {PROJECT}"), "\n")
  cat(glue("Dataset:   {DATASET}"), "\n\n")
}

# Check all tables
results <- list()
for (table_info in TABLES) {
  stats <- get_table_stats(table_info$name)
  results[[table_info$name]] <- c(table_info, stats)
}

# Output results
if (JSON_OUTPUT) {
  # JSON output for programmatic use
  cat(toJSON(results, pretty = TRUE, auto_unbox = TRUE))
} else {
  # Human-readable output
  for (table_name in names(results)) {
    r <- results[[table_name]]
    
    cat("───────────────────────────────────────────────────────────────\n")
    cat(glue("📊 {r$name}"), "\n")
    cat(glue("   {r$description}"), "\n")
    cat("───────────────────────────────────────────────────────────────\n")
    
    if (!r$exists) {
      if (is.na(r$exists)) {
        cat(glue("❌ Status: Error accessing table"), "\n")
        if (!is.null(r$error)) {
          cat(glue("   Error: {r$error}"), "\n")
        }
      } else {
        cat(glue("❌ Status: Table does not exist"), "\n")
      }
      cat(glue("   Source: {r$source}"), "\n")
      cat(glue("   Expected: {r$expected_update}"), "\n\n")
      next
    }
    
    icon <- get_status_icon(r$status)
    label <- get_status_label(r$status)
    
    cat(glue("{icon} Status: {label}"), "\n")
    cat(glue("   Rows:          {format_number(r$num_rows)}"), "\n")
    
    if (!is.na(r$min_date)) {
      cat(glue("   Date range:    {format_date(r$min_date)} to {format_date(r$max_date)}"), "\n")
      cat(glue("   Span:          {format_number(r$date_range_days)} days"), "\n")
      
      if (!is.na(r$days_since_latest_data)) {
        age_desc <- if (r$days_since_latest_data == 0) {
          "today"
        } else if (r$days_since_latest_data == 1) {
          "1 day ago"
        } else {
          glue("{round(r$days_since_latest_data)} days ago")
        }
        cat(glue("   Latest data:   {age_desc}"), "\n")
      }
    }
    
    if (!is.na(r$last_updated)) {
      cat(glue("   Last updated:  {format_date(r$last_updated)}"), "\n")
      if (!is.na(r$days_since_update)) {
        cat(glue("                  ({round(r$days_since_update, 1)} days ago)"), "\n")
      }
    }
    
    cat(glue("   Source:        {r$source}"), "\n")
    cat(glue("   Expected:      {r$expected_update}"), "\n\n")
  }
  
  # Summary
  cat("═══════════════════════════════════════════════════════════════\n")
  cat("  SUMMARY\n")
  cat("═══════════════════════════════════════════════════════════════\n\n")
  
  statuses <- sapply(results, function(r) r$status)
  
  current_count <- sum(statuses == "current")
  recent_count <- sum(statuses == "recent")
  stale_count <- sum(statuses == "stale")
  very_stale_count <- sum(statuses == "very_stale")
  empty_count <- sum(statuses == "empty")
  missing_count <- sum(statuses == "missing")
  error_count <- sum(statuses == "error")
  
  cat(glue("✅ Current:     {current_count}"), "\n")
  cat(glue("🟢 Recent:      {recent_count}"), "\n")
  cat(glue("🟡 Stale:       {stale_count}"), "\n")
  cat(glue("🔴 Very Stale:  {very_stale_count}"), "\n")
  cat(glue("⚠️  Empty:       {empty_count}"), "\n")
  cat(glue("❌ Missing:     {missing_count}"), "\n")
  
  if (error_count > 0) {
    cat(glue("❌ Errors:      {error_count}"), "\n")
  }
  
  cat("\n")
  
  # Overall status
  if (missing_count > 0 || error_count > 0) {
    cat("🔴 Overall Status: CRITICAL - Missing or inaccessible tables\n")
    exit_code <- 2
  } else if (very_stale_count > 0) {
    cat("🟡 Overall Status: WARNING - Some tables are very stale (>7 days old)\n")
    exit_code <- 1
  } else if (stale_count > 0) {
    cat("🟡 Overall Status: CAUTION - Some tables are stale (3-7 days old)\n")
    exit_code <- 0
  } else if (empty_count > 0) {
    cat("⚠️  Overall Status: INCOMPLETE - Some tables are empty\n")
    exit_code <- 1
  } else {
    cat("✅ Overall Status: GOOD - All tables are current or recent\n")
    exit_code <- 0
  }
  
  cat("\n═══════════════════════════════════════════════════════════════\n")
  
  # Exit with appropriate code
  quit(status = exit_code)
}
