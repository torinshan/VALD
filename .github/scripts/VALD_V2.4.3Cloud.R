#!/usr/bin/env Rscript
# ============================================================================
# VALD V2 Data Processing Script - Cloud Version
# ============================================================================
# Version: 2.4.3-cloud
# Date: January 2026
# Purpose: Fetch, process, and export VALD ForceDecks and Nordbord data
# Execution: GitHub Actions with BigQuery persistence
# 
# Cloud-Specific Features:
# - Environment variable configuration (no hardcoded credentials)
# - BigQuery authentication via service account JSON
# - Change detection gate check (STANDDOWN if no new data)
# - Independent ForceDecks/NordBord error handling
# - Structured logging with BigQuery persistence
# - Exit code management for workflow status
#
# V2.4.3 Updates:
# - RSI Scaling: Corrected 100x error (now decimal form: 0.43 not 43.2)
# - Readiness Calculation: Changed from observation-based to time-based 30-day window
# - MDC Thresholds: Athlete-specific with population fallback (not static)
# - New columns: mdc_tier, jh_mdc_threshold, rsi_mdc_threshold, epf_mdc_threshold
# - NordBord Body Weight: Now pulls from ForceDecks fd_raw (not roster)
# - process_rsi() Bug Fix: Group by test_ID to preserve multiple same-day tests
#
# V2.4.2 Updates:
# - Adaptive Fetch Framework: auto-selects bundled/parallel/sequential strategy
# - Nordbord: unilateral detection, 14-column schema
# - IMTP: reduced to 12-column schema with performance scores
# - SL Jumps: limb detection, QC layers, 34-column schema
# - RSI: limb detection, 22-column schema
# - DJ: QC layers, new ratios, 27-column schema
# ============================================================================

# ============================================================================
# CRITICAL: Define execution_status FIRST (before any code that could fail)
# ============================================================================
.GlobalEnv$execution_status <- list(
  packages_loaded = FALSE,
  credentials_valid = FALSE,
  bq_authenticated = FALSE,
  roster_loaded = FALSE,
  fd_fetched = FALSE,
  fd_cmj_processed = FALSE,
  fd_dj_processed = FALSE,
  fd_rsi_processed = FALSE,
  fd_rebound_processed = FALSE,
  fd_slj_processed = FALSE,
  fd_imtp_processed = FALSE,
  nord_fetched = FALSE,
  nord_processed = FALSE,
  refs_updated = FALSE
)

.GlobalEnv$execution_errors <- list()

# Safe update functions
update_status <- function(field, value) {
  .GlobalEnv$execution_status[[field]] <- value
}

record_error <- function(component, error_msg) {
  .GlobalEnv$execution_errors[[component]] <- error_msg
  if (exists("log_error", mode = "function")) {
    tryCatch(log_error(paste0(component, " FAILED: ", error_msg)), error = function(e) NULL)
  }
  cat(sprintf("[ERROR] %s FAILED: %s\n", component, error_msg))
}

determine_exit_code <- function() {
  status <- .GlobalEnv$execution_status
  any_fd_success <- any(status$fd_cmj_processed, status$fd_dj_processed,
    status$fd_rsi_processed, status$fd_rebound_processed,
    status$fd_slj_processed, status$fd_imtp_processed)
  any_nord_success <- status$nord_processed
  if (!any_fd_success && !any_nord_success) return(1)
  return(0)
}

# ============================================================================
# Package Loading (Fatal on Failure)
# ============================================================================
tryCatch({
  suppressPackageStartupMessages({
    library(bigrquery); library(DBI)
    library(dplyr); library(tidyr); library(readr); library(stringr)
    library(purrr); library(tibble); library(data.table)
    library(hms); library(lubridate)
    library(httr2)
    library(jsonlite); library(xml2); library(curl)
    library(valdr); library(gargle); library(rlang); library(lifecycle)
    library(glue); library(slider); library(R.utils)
    library(logger)
    library(readxl)
    library(furrr); library(future)
  })
  update_status("packages_loaded", TRUE)
}, error = function(e) {
  cat("FATAL: Package loading failed:", e$message, "\n")
  quit(status = 1)
})

# ============================================================================
# Environment Detection
# ============================================================================
LOCAL_MODE <- Sys.getenv("LOCAL_MODE", "FALSE") == "TRUE"

if (LOCAL_MODE) {
  cat("=== RUNNING IN LOCAL MODE ===\n")
} else {
  cat("=== RUNNING IN CLOUD MODE (GitHub Actions) ===\n")
}

# ============================================================================
# Configuration Constants
# ============================================================================
CONFIG <- list(
  # Date Configuration
  start_date = "2024-01-01",
  backstop_date = "2024-01-01",  # Fallback if BQ tables missing
  overlap_days = 1L,
  
  # Processing Parameters
  session_gap_hours = 2,
  timezone = "America/Los_Angeles",
  jump_height_min_inches = 5,
  jump_height_max_inches = 28,
  bodyweight_deviation_pct = 0.15,
  physics_v_ratio_min = 0.80,
  physics_v_ratio_max = 1.25,
  
  # Z-Score Configuration
  z_windows = c(30, 60, 90, Inf),
  z_min_observations = 4,
  z_threshold_small = 5,
  z_threshold_medium = 4,
  z_threshold_large = 3,
  
  # Timeout Settings
  timeout_fd_probe = 120,
  timeout_fd_full = 900,
  timeout_nordbord = 300,
  max_same_cursor = 3,
  
  # BigQuery Configuration (from environment)
  gcp_project = Sys.getenv("GCP_PROJECT", "sac-vald-hub"),
  bq_dataset = Sys.getenv("BQ_DATASET", "analytics"),
  bq_location = Sys.getenv("BQ_LOCATION", "US"),
  
  # Adaptive Fetch Settings
  baseline_tests = 4570,
  baseline_start = "2024-01-01",
  baseline_end = "2026-01-08",
  tests_per_day = 6.19,
  parallel_overhead_sec = 40,
  per_test_latency_sec = 2.0,
  parallel_threshold = 27
)

# ============================================================================
# Production Column Schema (54 columns total)
# ============================================================================
PRODUCTION_CMJ_COLUMNS <- c(
  # Identifiers (6 columns)
  "test_ID",
  "vald_id",
  "full_name",
  "team",
  "date",
  "test_type",
  
  # Performance Metrics (7 columns)
  "jump_height_inches_imp_mom",
  "rsi_modified_imp_mom",
  "body_weight_lbs",
  "bodymass_relative_takeoff_power",
  "relative_peak_eccentric_force",
  "relative_peak_concentric_force",
  "countermovement_depth",
  
  # Temporal Metrics (4 columns)
  "concentric_duration",
  "eccentric_time",
  "contraction_time",
  "braking_phase_duration",
  
  # Force & Velocity (4 columns)
  "peak_takeoff_force",
  "peak_takeoff_velocity",
  "takeoff_velocity",
  "concentric_impulse",
  
  # Impulse Metrics (2 columns)
  "positive_impulse",
  "positive_takeoff_impulse",
  
  # Power & Work (2 columns)
  "eccentric_peak_power",
  "displacement_at_takeoff",
  
  # Ratios (3 columns)
  "flight_eccentric_time_ratio",
  "eccentric_concentric_duration_ratio",
  "mean_ecc_con_ratio",
  
  # Additional Velocities (3 columns)
  "mean_takeoff_velocity",
  "mean_takeoff_acceleration",
  "peak_takeoff_acceleration",
  
  # Eccentric Metrics (2 columns)
  "mean_eccentric_force",
  "eccentric_braking_impulse",
  
  # Force at Zero Velocity (2 columns)
  "force_at_zero_velocity",
  "bm_rel_force_at_zero_velocity",
  
  # Work & Time (2 columns)
  "total_work",
  "start_to_peak_force_time",
  
  # RFD Metrics (2 columns)
  "concentric_rfd_100",
  "concentric_rfd_200",
  
  # Stiffness (1 column)
  "cmj_stiffness",
  
  # Landing Metrics (3 columns)
  "relative_peak_landing_force",
  "landing_impulse",
  "peak_drop_force",
  
  # Readiness & Performance (5 columns)
  "jump_height_readiness",
  "rsi_readiness",
  "epf_readiness",
  "performance_score",
  "team_performance_score",
  
  # MDC Status (3 columns)
  "jh_mdc_status",
  "rsi_mdc_status",
  "epf_mdc_status",
  
  # Asymmetry Metrics (3 columns)
  "landing_impulse_asymmetry",
  "peak_takeoff_force_asymmetry",
  "eccentric_braking_impulse_asymmetry"
)

# ============================================================================
# Secondary Table Schemas (V2.4.2)
# ============================================================================

# Nordbord: 14 columns
NORD_EXPORT_COLUMNS <- c(
  "test_ID", "vald_id", "date", "test_type", "trial_limb",
  "reps_left", "reps_right",
  "avg_force_bilateral", "max_force_bilateral",
  "avg_force_asymmetry", "max_force_asymmetry",
  "max_force_relative_bw", "max_force_left_relative_bw", "max_force_right_relative_bw"
)

# IMTP: 12 columns
IMTP_EXPORT_COLUMNS <- c(
  "test_ID", "vald_id", "date", "test_type",
  "iso_bm_rel_force_peak", "iso_bm_rel_force_100", "iso_bw_rel_force_peak",
  "rfd_at_100ms", "start_to_peak_force", "asym_peak_vertical_force",
  "performance_score", "team_performance_score"
)

# Drop Jump: 27 columns
DJ_EXPORT_COLUMNS <- c(
  "test_ID", "vald_id", "date", "body_weight_lbs",
  "jump_height_inches_imp_mom", "reactive_strength_index",
  "contact_time", "contact_velocity", "peak_takeoff_velocity", "peak_landing_velocity",
  "peak_impact_force", "peak_driveoff_force", "countermovement_depth", "eccentric_time",
  "positive_takeoff_impulse", "active_stiffness", "passive_stiffness",
  "active_stiffness_index", "passive_stiffness_index", "coefficient_of_restitution",
  "bm_rel_force_at_zero_velocity",
  "velocity_ratio", "force_ratio", "stiffness_ratio",
  "rel_impact_force", "rel_driveoff_force",
  "asym_peak_impact_force", "asym_peak_driveoff_force"
)

# Single-Leg Jumps: 34 columns
SLJ_EXPORT_COLUMNS <- c(
  "test_ID", "vald_id", "date", "test_type", "trial_limb", "body_weight_lbs",
  "jump_height_inches_imp_mom_left", "jump_height_inches_imp_mom_right", "jump_height_inches_imp_mom_bilateral",
  "lower_limb_stiffness_left", "lower_limb_stiffness_right", "lower_limb_stiffness_bilateral",
  "relative_peak_concentric_force_left", "relative_peak_concentric_force_right", "relative_peak_concentric_force_bilateral",
  "relative_peak_eccentric_force_left", "relative_peak_eccentric_force_right", "relative_peak_eccentric_force_bilateral",
  "weight_relative_peak_takeoff_force_left", "weight_relative_peak_takeoff_force_right", "weight_relative_peak_takeoff_force_bilateral",
  "weight_relative_peak_landing_force_left", "weight_relative_peak_landing_force_right", "weight_relative_peak_landing_force_bilateral",
  "time_to_peak_force_left", "time_to_peak_force_right", "time_to_peak_force_bilateral",
  "rsi_modified_imp_mom_left", "rsi_modified_imp_mom_right", "rsi_modified_imp_mom_bilateral",
  "asym_peak_landing_force", "asym_weight_relative_peak_takeoff_force"
)

# RSI: 22 columns
RSI_EXPORT_COLUMNS <- c(
  "test_ID", "vald_id", "date", "test_type", "trial_limb",
  "iso_bm_rel_force_peak_left", "iso_bm_rel_force_peak_right", "iso_bm_rel_force_peak_bilateral",
  "iso_bm_rel_force_100_left", "iso_bm_rel_force_100_right", "iso_bm_rel_force_100_bilateral",
  "iso_abs_impulse_100_left", "iso_abs_impulse_100_right", "iso_abs_impulse_100_bilateral",
  "rfd_at_100ms_left", "rfd_at_100ms_right", "rfd_at_100ms_bilateral",
  "start_to_peak_force_left", "start_to_peak_force_right", "start_to_peak_force_bilateral",
  "asym_peak_vertical_force"
)

# ============================================================================
# MDC Configuration (Athlete-Specific with Population Fallback)
# ============================================================================
MDC_CONFIG <- list(
  athlete_min_total_obs = 15L,
  athlete_min_recent_obs = 10L,
  athlete_recent_window_days = 90L,
  pop_min_test_days = 2L,
  pop_window_days = 60L,
  mdc_multiplier = 2.77
)

MDC_THRESHOLDS_STATIC <- list(
  jump_height_pct = 0.04,
  rsi_modified_pct = 0.08,
  eccentric_force_pct = 0.07,
  imtp_peak_force_pct = 0.05,
  nordic_peak_force_pct = 0.07
)

# ============================================================================
# Athlete-Specific MDC Calculation Function
# ============================================================================
calculate_athlete_mdc <- function(dt, metric_col, baseline_col, mdc_col, status_col) {
  
  data.table::setorder(dt, full_name, test_type, date)
  
  dt[, athlete_n_total := seq_len(.N), by = .(full_name, test_type)]
  
  dt[, c("athlete_n_90d", "athlete_mean_90d", "athlete_sd_90d") := {
    n_90d <- numeric(.N)
    mean_90d <- numeric(.N)
    sd_90d <- numeric(.N)
    
    for (i in seq_len(.N)) {
      if (i == 1L) {
        n_90d[i] <- 0L
        mean_90d[i] <- NA_real_
        sd_90d[i] <- NA_real_
      } else {
        idx <- which(date[1:(i-1)] >= (date[i] - 90))
        n_90d[i] <- length(idx)
        if (length(idx) >= 2) {
          vals <- get(metric_col)[idx]
          mean_90d[i] <- mean(vals, na.rm = TRUE)
          sd_90d[i] <- sd(vals, na.rm = TRUE)
        } else {
          mean_90d[i] <- NA_real_
          sd_90d[i] <- NA_real_
        }
      }
    }
    list(n_90d, mean_90d, sd_90d)
  }, by = .(full_name, test_type)]
  
  dt[, athlete_cv := data.table::fifelse(
    athlete_mean_90d > 0 & !is.na(athlete_sd_90d),
    athlete_sd_90d / athlete_mean_90d,
    NA_real_
  )]
  
  dt[, athlete_mdc_eligible := (athlete_n_total >= MDC_CONFIG$athlete_min_total_obs) & 
                               (athlete_n_90d >= MDC_CONFIG$athlete_min_recent_obs)]
  
  dt[, athlete_mdc_pct := data.table::fifelse(
    athlete_mdc_eligible & !is.na(athlete_cv),
    athlete_cv * MDC_CONFIG$mdc_multiplier,
    NA_real_
  )]
  
  dt[, c("pop_n_days_60d", "pop_mean_60d", "pop_sd_60d") := {
    n_days <- numeric(.N)
    p_mean <- numeric(.N)
    p_sd <- numeric(.N)
    
    for (i in seq_len(.N)) {
      current_date <- date[i]
      current_athlete <- full_name[i]
      current_team <- team[i]
      current_test_type <- test_type[i]
      
      pop_mask <- dt$team == current_team & 
                  dt$test_type == current_test_type &
                  dt$full_name != current_athlete &
                  dt$date >= (current_date - 60) &
                  dt$date < current_date
      
      pop_data <- dt[pop_mask]
      
      n_days[i] <- length(unique(pop_data$date))
      
      if (nrow(pop_data) >= 2) {
        vals <- pop_data[[metric_col]]
        p_mean[i] <- mean(vals, na.rm = TRUE)
        p_sd[i] <- sd(vals, na.rm = TRUE)
      } else {
        p_mean[i] <- NA_real_
        p_sd[i] <- NA_real_
      }
    }
    list(n_days, p_mean, p_sd)
  }]
  
  dt[, pop_cv := data.table::fifelse(
    pop_mean_60d > 0 & !is.na(pop_sd_60d),
    pop_sd_60d / pop_mean_60d,
    NA_real_
  )]
  
  dt[, pop_mdc_eligible := pop_n_days_60d >= MDC_CONFIG$pop_min_test_days]
  
  dt[, pop_mdc_pct := data.table::fifelse(
    pop_mdc_eligible & !is.na(pop_cv),
    pop_cv * MDC_CONFIG$mdc_multiplier,
    NA_real_
  )]
  
  dt[, (baseline_col) := data.table::fcase(
    athlete_mdc_eligible & !is.na(athlete_mean_90d), athlete_mean_90d,
    pop_mdc_eligible & !is.na(pop_mean_60d), pop_mean_60d,
    default = NA_real_
  )]
  
  dt[, (mdc_col) := data.table::fcase(
    athlete_mdc_eligible & !is.na(athlete_mdc_pct), athlete_mdc_pct,
    pop_mdc_eligible & !is.na(pop_mdc_pct), pop_mdc_pct,
    default = NA_real_
  )]
  
  dt[, mdc_tier := data.table::fcase(
    athlete_mdc_eligible & !is.na(athlete_mdc_pct), "ATHLETE",
    pop_mdc_eligible & !is.na(pop_mdc_pct), "POPULATION",
    default = "INSUFFICIENT_DATA"
  )]
  
  dt[, (status_col) := {
    current_val <- get(metric_col)
    baseline_val <- get(baseline_col)
    threshold <- get(mdc_col)
    
    data.table::fcase(
      is.na(current_val) | is.na(baseline_val) | is.na(threshold), NA_character_,
      baseline_val == 0, NA_character_,
      (current_val - baseline_val) / baseline_val >= threshold, "MEANINGFUL_INCREASE",
      (current_val - baseline_val) / baseline_val <= -threshold, "MEANINGFUL_DECREASE",
      default = "WITHIN_NOISE"
    )
  }]
  
  temp_cols <- c("athlete_n_total", "athlete_n_90d", "athlete_mean_90d", 
                 "athlete_sd_90d", "athlete_cv", "athlete_mdc_eligible", "athlete_mdc_pct",
                 "pop_n_days_60d", "pop_mean_60d", "pop_sd_60d", "pop_cv",
                 "pop_mdc_eligible", "pop_mdc_pct")
  
  dt[, (temp_cols) := NULL]
  
  return(dt)
}

# ============================================================================
# Parallel Fetching Configuration
# ============================================================================
PARALLEL_CONFIG <- list(
  n_workers = 4,
  stagger_sec = 0.5,
  max_retries = 3
)

# ============================================================================
# VALD API Credentials (Environment Variables)
# ============================================================================
cat("=== CONFIGURING VALD API ===\n")
client_id     <- Sys.getenv("VALD_CLIENT_ID", "")
client_secret <- Sys.getenv("VALD_CLIENT_SECRET", "")
tenant_id     <- Sys.getenv("VALD_TENANT_ID", "")
region        <- Sys.getenv("VALD_REGION", "use")

# Validate credentials are present
if (nchar(client_id) == 0 || nchar(client_secret) == 0 || nchar(tenant_id) == 0) {
  cat("FATAL: Missing VALD API credentials in environment variables\n")
  cat("Required: VALD_CLIENT_ID, VALD_CLIENT_SECRET, VALD_TENANT_ID\n")
  quit(status = 1)
}

# Set credentials using valdr package
tryCatch({
  valdr::set_credentials(client_id, client_secret, tenant_id, region)
  cat("VALD API credentials configured successfully\n")
}, error = function(e) {
  cat("FATAL: Failed to set VALD credentials:", conditionMessage(e), "\n")
  quit(status = 1)
})

# Set start date for API queries
tryCatch({
  valdr::set_start_date(paste0(CONFIG$start_date, "T00:00:00Z"))
  cat("VALD API start date set to:", CONFIG$start_date, "\n")
}, error = function(e) {
  cat("FATAL: Failed to set start date:", conditionMessage(e), "\n")
  quit(status = 1)
})

# Mark credentials as valid
update_status("credentials_valid", TRUE)

# ============================================================================
# Path Configuration (Environment-Aware)
# ============================================================================
if (LOCAL_MODE) {
  LOCAL_PATHS <- list(
    output_dir = Sys.getenv("LOCAL_OUTPUT_DIR", 
      "C:/Users/Torin/OneDrive - California State University, Sacramento/Sac State Football/GPS/Clean_Backup"),
    log_dir = Sys.getenv("LOCAL_LOG_DIR",
      "C:/Users/Torin/OneDrive - California State University, Sacramento/Sac State Football/GPS/Clean_Backup/logs")
  )
  ROSTER_PATHS <- list(
    roster_csv = Sys.getenv("LOCAL_ROSTER_CSV",
      "C:/Users/Torin/OneDrive - California State University, Sacramento/Sac State Football/Testing/Data/vald_roster.csv"),
    roster_xlsx = Sys.getenv("LOCAL_ROSTER_XLSX",
      "C:/Users/Torin/OneDrive - California State University, Sacramento/Sac State Football/Sac State Roster - Summer 2025.xlsx")
  )
} else {
  LOCAL_PATHS <- list(
    output_dir = "/tmp/vald_output",
    log_dir = "/tmp/logs"
  )
  ROSTER_PATHS <- list(
    roster_csv = ".github/vald_roster.csv",
    roster_xlsx = ".github/Sac State Roster - Summer 2025.xlsx"
  )
}

# Ensure directories exist
if (!dir.exists(LOCAL_PATHS$output_dir)) dir.create(LOCAL_PATHS$output_dir, recursive = TRUE)
if (!dir.exists(LOCAL_PATHS$log_dir)) dir.create(LOCAL_PATHS$log_dir, recursive = TRUE)

# ============================================================================
# BigQuery Authentication (Cloud Mode Only)
# ============================================================================
GLOBAL_ACCESS_TOKEN <- NULL

if (!LOCAL_MODE) {
  auth_success <- FALSE
  
  # Try Method 1: Workload Identity Federation (WIF) via gcloud
  tryCatch({
    cat("=== Authenticating to BigQuery ===\n")
    cat("Attempting authentication via WIF (gcloud)...\n")
    
    # Disable Storage API to avoid permission issues
    options(bigrquery.use_bqstorage = FALSE)
    Sys.setenv(BIGRQUERY_USE_BQ_STORAGE = "false")
    
    # Use Workload Identity Federation (WIF) via gcloud
    access_token_result <- system("gcloud auth print-access-token", intern = TRUE)
    GLOBAL_ACCESS_TOKEN <<- access_token_result[1]
    
    if (nchar(GLOBAL_ACCESS_TOKEN) > 0) {
      cat("Access token obtained from gcloud (WIF)\n")
      token <- gargle::gargle2.0_token(
        scope = 'https://www.googleapis.com/auth/bigquery',
        client = gargle::gargle_client(),
        credentials = list(access_token = GLOBAL_ACCESS_TOKEN)
      )
      bigrquery::bq_auth(token = token)
      cat("BigQuery authentication successful via WIF\n")
      
      # Verify authentication by checking dataset exists
      ds <- bigrquery::bq_dataset(CONFIG$gcp_project, CONFIG$bq_dataset)
      invisible(bigrquery::bq_dataset_exists(ds))
      cat("Authentication test passed (dataset visible via REST)\n")
      
      auth_success <- TRUE
    }
  }, error = function(e) {
    cat("WIF authentication failed:", e$message, "\n")
  })
  
  # Try Method 2: Service Account JSON (fallback)
  if (!auth_success) {
    tryCatch({
      cat("Attempting fallback authentication via service account JSON...\n")
      
      # Get service account key from environment
      sa_key_json <- Sys.getenv("GCP_SA_KEY", "")
      
      if (nchar(sa_key_json) > 0) {
        # Write SA key to temp file
        sa_key_path <- tempfile(fileext = ".json")
        writeLines(sa_key_json, sa_key_path)
        
        # Authenticate with service account
        bigrquery::bq_auth(path = sa_key_path)
        cat("BigQuery authentication successful via service account JSON\n")
        
        # Clean up key file immediately
        unlink(sa_key_path)
        
        # Verify authentication by checking dataset exists
        ds <- bigrquery::bq_dataset(CONFIG$gcp_project, CONFIG$bq_dataset)
        invisible(bigrquery::bq_dataset_exists(ds))
        cat("Authentication test passed (dataset visible via REST)\n")
        
        auth_success <- TRUE
      } else {
        cat("GCP_SA_KEY environment variable not set, skipping service account JSON fallback\n")
      }
    }, error = function(e) {
      cat("Service account JSON authentication failed:", e$message, "\n")
    })
  }
  
  # Check if any authentication method succeeded
  if (!auth_success) {
    cat("FATAL: All authentication methods failed\n")
    cat("Tried: 1) WIF (gcloud), 2) Service Account JSON (GCP_SA_KEY)\n")
    quit(status = 1)
  }
  
  update_status("bq_authenticated", TRUE)
  
} else {
  cat("Local mode: BigQuery authentication skipped\n")
  update_status("bq_authenticated", TRUE)
}

# ============================================================================
# Logging System
# ============================================================================
LOG_FILENAME <- file.path(
  LOCAL_PATHS$log_dir,
  paste0("vald_processing_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log")
)

log_appender(appender_file(LOG_FILENAME))
log_threshold(INFO)
log_formatter(formatter_glue_or_sprintf)

LOG_RUN_TYPE <- "UNKNOWN"

log_entries_dt <- data.table::data.table(
  timestamp = as.POSIXct(character(0)),
  level = character(0),
  message = character(0),
  run_id = character(0),
  repository = character(0)
)

log_and_store <- function(msg, level = "INFO") {
  interpolated_msg <- tryCatch({
    glue::glue(msg, .envir = parent.frame())
  }, error = function(e) msg)
  
  # Console output (visible in GitHub Actions logs)
  cat(sprintf("[%s] [%s] %s\n", 
              format(Sys.time(), "%Y-%m-%d %H:%M:%S"), 
              level, 
              interpolated_msg))
  
  # File logging
  switch(level,
    "INFO" = log_info(interpolated_msg),
    "WARN" = log_warn(interpolated_msg),
    "ERROR" = log_error(interpolated_msg),
    "START" = log_info(paste("=== START ===", interpolated_msg)),
    "END" = log_info(paste("=== END ===", interpolated_msg)),
    log_info(interpolated_msg)
  )
  
  # Accumulate for BigQuery upload (cloud mode)
  if (!LOCAL_MODE) {
    log_entries_dt <<- data.table::rbindlist(list(
      log_entries_dt,
      data.table::data.table(
        timestamp = Sys.time(),
        level = level,
        message = as.character(interpolated_msg),
        run_id = Sys.getenv("GITHUB_RUN_ID", "manual"),
        repository = Sys.getenv("GITHUB_REPOSITORY", "unknown")
      )
    ), use.names = TRUE, fill = TRUE)
  }
}

log_check_summary <- function(table_name, current_rows, current_date, api_rows, api_date, result) {
  msg <- sprintf("CHECK: %s | BQ: %d rows, %s | API: %d rows, %s | Result: %s",
    table_name, current_rows, as.character(current_date), api_rows, as.character(api_date), result)
  log_info(msg)
}

finalize_logging <- function() {
  log_info("=== LOGGING FINALIZED ===")
  log_info(paste("Run type:", LOG_RUN_TYPE))
  log_info(paste("Log file:", LOG_FILENAME))
  errors <- .GlobalEnv$execution_errors
  log_info(paste("Errors recorded:", length(errors)))
  if (length(errors) > 0) {
    log_info(paste("Failed components:", paste(names(errors), collapse = ', ')))
  }
}

create_log_entry <- function(message, level = "INFO") {
  log_and_store(message, level)
}

# ============================================================================
# Log Upload to BigQuery
# ============================================================================
upload_logs_to_bigquery <- function() {
  if (LOCAL_MODE) {
    log_info("Local mode: logs saved to {LOG_FILENAME}")
    return(invisible(TRUE))
  }
  
  if (nrow(log_entries_dt) == 0) {
    log_info("No log entries to upload")
    return(invisible(TRUE))
  }
  
  tryCatch({
    ds <- bigrquery::bq_dataset(CONFIG$gcp_project, CONFIG$bq_dataset)
    log_tbl <- bigrquery::bq_table(ds, "vald_processing_log")
    
    log_table_exists <- bigrquery::bq_table_exists(log_tbl)
    if (is.na(log_table_exists) || !log_table_exists) {
      log_info("Creating vald_processing_log table...")
      bigrquery::bq_table_create(log_tbl, fields = bigrquery::as_bq_fields(log_entries_dt))
    }
    
    bigrquery::bq_table_upload(log_tbl, log_entries_dt, write_disposition = "WRITE_APPEND")
    log_info("Uploaded {nrow(log_entries_dt)} log entries to BigQuery")
    TRUE
    
  }, error = function(e) {
    cat("WARNING: Log upload failed:", e$message, "\n")
    FALSE
  })
}

# ============================================================================
# Error Handling
# ============================================================================
vald_error <- function(message, type = "general", ...) {
  structure(list(message = message, type = type, details = list(...)),
    class = c(paste0("vald_", type, "_error"), "vald_error", "error", "condition"))
}

safe_execute <- function(expr, context = "", on_error = "stop") {
  tryCatch(expr,
    vald_error = function(e) {
      log_error("{context}: {e$message} (type: {e$type})")
      if (on_error == "stop") stop(e) else return(NULL)
    },
    error = function(e) {
      log_error("{context}: Unexpected error - {e$message}")
      if (on_error == "stop") stop(e) else return(NULL)
    }
  )
}

validate_columns <- function(dt, required_cols, context = "data") {
  missing <- setdiff(required_cols, names(dt))
  if (length(missing) > 0) {
    log_error("{context} missing required columns: {paste(missing, collapse = ', ')}")
    stop(vald_error(paste("Missing columns:", paste(missing, collapse = ", ")), type = "schema", context = context))
  }
  invisible(TRUE)
}

validate_row_count <- function(dt, min_rows = 0, max_rows = Inf, context = "data") {
  n <- nrow(dt)
  if (n < min_rows) log_warn("{context}: Only {n} rows (expected >= {min_rows})")
  if (n > max_rows) log_warn("{context}: {n} rows exceeds expected max of {max_rows}")
  invisible(TRUE)
}

# ============================================================================
# Data Read Function (Dual Mode)
# ============================================================================
read_bq_table <- function(table_name) {
  if (LOCAL_MODE) {
    # Local: Read from CSV
    local_path <- file.path(LOCAL_PATHS$output_dir, paste0(table_name, ".csv"))
    if (file.exists(local_path)) {
      dt <- data.table::fread(local_path)
      log_info("Read local table: {table_name} ({nrow(dt)} rows)")
      return(dt)
    } else {
      log_info("Local table not found: {table_name}")
      return(data.table::data.table())
    }
  }
  
  # Cloud: Read from BigQuery
  tryCatch({
    ds <- bigrquery::bq_dataset(CONFIG$gcp_project, CONFIG$bq_dataset)
    tbl <- bigrquery::bq_table(ds, table_name)
    
    table_exists <- bigrquery::bq_table_exists(tbl)
    if (is.na(table_exists) || !table_exists) {
      log_info("BigQuery table does not exist: {table_name}")
      return(data.table::data.table())
    }
    
    result <- bigrquery::bq_table_download(tbl, n_max = Inf)
    dt <- data.table::as.data.table(result)
    log_info("Read BigQuery table: {table_name} ({nrow(dt)} rows)")
    return(dt)
    
  }, error = function(e) {
    log_error("Failed to read {table_name}: {e$message}")
    return(data.table::data.table())
  })
}

# Alias for compatibility
read_local_table <- read_bq_table

# ============================================================================
# Data Write Function (Dual Mode with MERGE)
# ============================================================================
bq_upsert <- function(data, table_name, key = "test_ID", mode = c("MERGE", "TRUNCATE"),
                      partition_field = "date", cluster_fields = character()) {
  mode <- match.arg(mode)
  
  if (nrow(data) == 0) {
    log_info("No rows to write for {table_name}")
    return(invisible(TRUE))
  }
  
  if (LOCAL_MODE) {
    # Local: Write to CSV with merge logic
    return(local_upsert(data, table_name, key, mode, partition_field, cluster_fields))
  }
  
  # Cloud: Write to BigQuery
  staging_tbl <- NULL  # Track staging table for cleanup
  staging_name <- NULL
  
  tryCatch({
    ds <- bigrquery::bq_dataset(CONFIG$gcp_project, CONFIG$bq_dataset)
    tbl <- bigrquery::bq_table(ds, table_name)
    
    # PRE-FLIGHT DIAGNOSTICS: Check for common MERGE failure causes
    # 1. Check for duplicate keys
    if (!(key %in% names(data))) {
      stop(sprintf("Key column '%s' not found in data. Available columns: %s", 
                   key, paste(names(data), collapse = ", ")))
    }
    
    dup_keys <- data[[key]][duplicated(data[[key]])]
    if (length(dup_keys) > 0) {
      log_error("MERGE WILL FAIL: Found {length(dup_keys)} duplicate keys in data")
      log_error("Example duplicate keys: {paste(head(unique(dup_keys), 5), collapse = ', ')}")
      stop(sprintf("Cannot MERGE: %d duplicate keys found in column '%s'", length(dup_keys), key))
    }
    
    # 2. Check for NULL/NA values in key column
    null_keys <- sum(is.na(data[[key]]) | data[[key]] == "")
    if (null_keys > 0) {
      log_error("MERGE WILL FAIL: Found {null_keys} NULL/empty keys in column '{key}'")
      stop(sprintf("Cannot MERGE: %d NULL/empty values in key column '%s'", null_keys, key))
    }
    
    log_info("Pre-flight checks passed: {nrow(data)} unique keys, no NULLs")
    
    # Create table if not exists (WITHOUT expiration)
    table_exists <- bigrquery::bq_table_exists(tbl)
    # Handle potential NA return value from bq_table_exists
    if (is.na(table_exists)) {
      log_error("Unable to determine if table {table_name} exists - treating as non-existent")
      table_exists <- FALSE
    }
    if (!table_exists) {
      log_info("Creating BigQuery table: {table_name} (no expiration)")
      bigrquery::bq_table_create(
        tbl, 
        fields = bigrquery::as_bq_fields(data),
        expiration_time = NULL
      )
    } else {
      # Table exists - check for schema compatibility
      existing_schema <- bigrquery::bq_table_meta(tbl)$schema$fields
      existing_cols <- sapply(existing_schema, function(f) f$name)
      new_cols <- names(data)
      
      missing_in_existing <- setdiff(new_cols, existing_cols)
      if (length(missing_in_existing) > 0) {
        log_warn("New columns not in existing table: {paste(missing_in_existing, collapse = ', ')}")
        log_warn("MERGE may fail due to schema mismatch - consider adding columns to table first")
      }
    }
    
    if (mode == "TRUNCATE") {
      bigrquery::bq_table_upload(tbl, data, write_disposition = "WRITE_TRUNCATE")
      log_info("Wrote {table_name}: {nrow(data)} rows (TRUNCATE)")
    } else {
      # MERGE mode: Use staging table + MERGE SQL
      staging_name <- paste0(table_name, "_staging_", format(Sys.time(), "%Y%m%d%H%M%S"))
      staging_tbl <- bigrquery::bq_table(ds, staging_name)
      
      # Create staging table with 1-hour expiration as safety net
      # This ensures it auto-deletes even if manual cleanup fails
      log_info("Creating temporary staging table: {staging_name} (1-hour expiration)")
      tryCatch({
        bigrquery::bq_table_create(
          staging_tbl,
          fields = bigrquery::as_bq_fields(data),
          expiration_time = as.numeric(Sys.time() + 3600)  # 1 hour from now (Unix timestamp)
        )
      }, error = function(e) {
        # If table already exists from previous failed run, delete and recreate
        staging_exists <- bigrquery::bq_table_exists(staging_tbl)
        if (!is.na(staging_exists) && staging_exists) {
          log_warn("Staging table {staging_name} already exists, deleting old one")
          bigrquery::bq_table_delete(staging_tbl)
          bigrquery::bq_table_create(
            staging_tbl,
            fields = bigrquery::as_bq_fields(data),
            expiration_time = as.numeric(Sys.time() + 3600)  # 1 hour from now (Unix timestamp)
          )
        } else {
          stop(e)
        }
      })
      
      # Upload to staging
      log_info("Uploading {nrow(data)} rows to staging table: {staging_name}")
      bigrquery::bq_table_upload(staging_tbl, data, write_disposition = "WRITE_TRUNCATE")
      
      # Build column lists for MERGE
      update_cols <- setdiff(names(data), key)
      update_clause <- paste(sapply(update_cols, function(col) {
        paste0("T.`", col, "` = S.`", col, "`")
      }), collapse = ", ")
      
      insert_cols <- paste0("`", names(data), "`", collapse = ", ")
      insert_vals <- paste0("S.`", names(data), "`", collapse = ", ")
      
      merge_sql <- glue::glue("
        MERGE `{CONFIG$gcp_project}.{CONFIG$bq_dataset}.{table_name}` T
        USING `{CONFIG$gcp_project}.{CONFIG$bq_dataset}.{staging_name}` S
        ON T.`{key}` = S.`{key}`
        WHEN MATCHED THEN
          UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols})
          VALUES ({insert_vals})
      ")
      
      log_info("Executing MERGE for {table_name}...")
      
      # Execute merge and capture job details
      job <- bigrquery::bq_project_query(CONFIG$gcp_project, merge_sql)
      
      # Wait for job completion and get detailed status
      job_result <- bigrquery::bq_job_wait(job, quiet = FALSE)
      
      # Check if job succeeded
      job_status <- bigrquery::bq_job_status(job)
      if (!is.null(job_status$errorResult)) {
        # Job failed - extract detailed error information
        error_msg <- job_status$errorResult$message
        error_reason <- job_status$errorResult$reason
        error_location <- job_status$errorResult$location
        
        log_error("MERGE job failed with reason: {error_reason}")
        log_error("Error message: {error_msg}")
        if (!is.null(error_location)) {
          log_error("Error location: {error_location}")
        }
        
        # Try to drop staging table even on failure
        tryCatch({
          staging_exists <- bigrquery::bq_table_exists(staging_tbl)
          if (!is.na(staging_exists) && staging_exists) {
            bigrquery::bq_table_delete(staging_tbl)
            log_info("Cleaned up staging table after failure")
          }
        }, error = function(cleanup_err) {
          log_warn("Could not clean up staging table: {cleanup_err$message}")
        })
        
        stop(sprintf("BigQuery MERGE failed: [%s] %s", error_reason, error_msg))
      }
      
      # Success - drop staging table immediately
      tryCatch({
        staging_exists <- bigrquery::bq_table_exists(staging_tbl)
        if (!is.na(staging_exists) && staging_exists) {
          bigrquery::bq_table_delete(staging_tbl)
          log_info("Dropped staging table: {staging_name}")
        } else {
          log_warn("Staging table {staging_name} already deleted or never existed")
        }
      }, error = function(e) {
        log_error("Failed to drop staging table {staging_name}: {e$message}")
        log_warn("Staging table will auto-expire in 1 hour")
      })
      
      log_info("Merged {table_name}: {nrow(data)} rows (MERGE)")
    }
    
    invisible(TRUE)
    
  }, error = function(e) {
    log_error("BigQuery upsert failed for {table_name}: {e$message}")
    
    # Log additional context for debugging
    log_error("Data shape: {nrow(data)} rows x {ncol(data)} columns")
    log_error("Key column: '{key}' (type: {class(data[[key]])[1]})")
    log_error("Mode: {mode}")
    
    # Cleanup staging table if it exists
    if (!is.null(staging_tbl) && !is.null(staging_name)) {
      tryCatch({
        staging_exists <- bigrquery::bq_table_exists(staging_tbl)
        if (!is.na(staging_exists) && staging_exists) {
          bigrquery::bq_table_delete(staging_tbl)
          log_info("Cleaned up staging table {staging_name} after error")
        }
      }, error = function(cleanup_err) {
        log_warn("Could not clean up staging table {staging_name}: {cleanup_err$message}")
        log_warn("Staging table will auto-expire in 1 hour")
      })
    }
    
    record_error(paste0("BQ_", table_name), e$message)
    invisible(FALSE)
  })
}

# Local upsert function (for LOCAL_MODE)
local_upsert <- function(data, table_name, key = "test_ID", mode = c("MERGE", "TRUNCATE"),
                         partition_field = "date", cluster_fields = character()) {
  mode <- match.arg(mode)
  if (nrow(data) == 0) {
    log_info("No rows to write for {table_name}")
    return(TRUE)
  }
  
  local_path <- file.path(LOCAL_PATHS$output_dir, paste0(table_name, ".csv"))
  
  if (mode == "TRUNCATE" || !file.exists(local_path)) {
    data.table::fwrite(data, local_path)
    log_info("Wrote {table_name}: {nrow(data)} rows (mode: {if(file.exists(local_path)) mode else 'CREATE'})")
    return(TRUE)
  }
  
  existing <- data.table::fread(local_path)
  if (!(key %in% names(data))) {
    log_error("Key column '{key}' missing")
    stop(paste("Key", key, "missing"))
  }
  
  existing[[key]] <- as.character(existing[[key]])
  data[[key]] <- as.character(data[[key]])
  
  all_cols <- union(names(existing), names(data))
  for (cn in setdiff(all_cols, names(existing))) existing[[cn]] <- NA
  for (cn in setdiff(all_cols, names(data))) data[[cn]] <- NA
  
  data.table::setcolorder(existing, all_cols)
  data.table::setcolorder(data, all_cols)
  
  common_cols <- intersect(names(existing), names(data))
  for (col in common_cols) {
    if (class(existing[[col]])[1] != class(data[[col]])[1]) {
      if (is.numeric(existing[[col]]) || is.numeric(data[[col]])) {
        existing[, (col) := as.numeric(as.character(get(col)))]
        data[, (col) := as.numeric(as.character(get(col)))]
      } else {
        existing[, (col) := as.character(get(col))]
        data[, (col) := as.character(get(col))]
      }
    }
  }
  
  combined <- data.table::rbindlist(list(existing, data), use.names = TRUE, fill = TRUE, ignore.attr = TRUE)
  combined[, .row_id := .I]
  data.table::setorderv(combined, ".row_id")
  combined <- combined[, .SD[.N], by = key]
  combined[, .row_id := NULL]
  
  data.table::fwrite(combined, local_path)
  log_info("Merged {table_name}: {nrow(combined)} rows (added {nrow(data)} new/updated)")
  return(TRUE)
}

export_local_summary <- function() {
  output_files <- list.files(LOCAL_PATHS$output_dir, pattern = "\\.csv$", full.names = TRUE)
  log_info("=== LOCAL EXPORT SUMMARY ===")
  log_info("Output directory: {LOCAL_PATHS$output_dir}")
  for (f in output_files) {
    file_info <- file.info(f)
    row_count <- tryCatch({
      length(readLines(f)) - 1
    }, error = function(e) NA)
    log_info("  {basename(f)}: {row_count} rows, modified {file_info$mtime}")
  }
}

# ============================================================================
# Schema Mismatch Tracking
# ============================================================================
schema_mismatches <- data.table::data.table(
  timestamp = as.POSIXct(character(0)),
  table_name = character(0),
  column_name = character(0),
  expected_type = character(0),
  actual_type = character(0),
  action_taken = character(0),
  run_id = character(0),
  repository = character(0)
)

record_schema_mismatch <- function(table_name, column_name, expected_type, actual_type, action_taken) {
  schema_mismatches <<- data.table::rbindlist(list(schema_mismatches,
    data.table::data.table(
      timestamp = Sys.time(), 
      table_name = table_name, 
      column_name = column_name,
      expected_type = expected_type, 
      actual_type = actual_type, 
      action_taken = action_taken,
      run_id = Sys.getenv("GITHUB_RUN_ID", "manual"),
      repository = Sys.getenv("GITHUB_REPOSITORY", "unknown")
    )), use.names = TRUE, fill = TRUE)
  
  log_warn("{table_name}.{column_name}: Schema mismatch - expected {expected_type}, got {actual_type}")
}

upload_schema_mismatches <- function() {
  if (LOCAL_MODE) {
    if (nrow(schema_mismatches) > 0) {
      mismatch_file <- file.path(LOCAL_PATHS$log_dir, "schema_mismatches.csv")
      data.table::fwrite(schema_mismatches, mismatch_file)
      log_info("Saved {nrow(schema_mismatches)} schema mismatches to {mismatch_file}")
    }
    return(invisible(TRUE))
  }
  
  if (nrow(schema_mismatches) == 0) return(invisible(TRUE))
  
  tryCatch({
    ds <- bigrquery::bq_dataset(CONFIG$gcp_project, CONFIG$bq_dataset)
    tbl <- bigrquery::bq_table(ds, "schema_mismatches")
    
    mismatch_table_exists <- bigrquery::bq_table_exists(tbl)
    if (is.na(mismatch_table_exists) || !mismatch_table_exists) {
      bigrquery::bq_table_create(tbl, fields = bigrquery::as_bq_fields(schema_mismatches))
    }
    
    bigrquery::bq_table_upload(tbl, schema_mismatches, write_disposition = "WRITE_APPEND")
    log_info("Uploaded {nrow(schema_mismatches)} schema mismatches")
    TRUE
    
  }, error = function(e) {
    log_error("Schema mismatch upload failed: {e$message}")
    FALSE
  })
}

# ============================================================================
# Reconciliation Helper: Retry Missing Test IDs
# ============================================================================
reconcile_missing_trials <- function(missing_ids, round_number = 1) {
  
  if (length(missing_ids) == 0) {
    return(data.table::data.table())
  }
  
  wait_time <- 2^round_number
  log_info("RECONCILIATION Round {round_number}: Retrying {length(missing_ids)} test IDs after {wait_time}s delay")
  
  Sys.sleep(wait_time)
  
  batch_size <- 10
  batches <- split(missing_ids, ceiling(seq_along(missing_ids) / batch_size))
  
  log_info("  Split into {length(batches)} batches of max {batch_size} tests")
  
  recovered <- purrr::map_dfr(batches, function(batch_ids) {
    tryCatch({
      batch_trials <- valdr::get_forcedecks_trials(test_ids = batch_ids)
      
      if (is.null(batch_trials) || length(batch_trials) == 0 || nrow(batch_trials) == 0) {
        return(data.table::data.table())
      }
      
      return(data.table::as.data.table(batch_trials))
      
    }, error = function(e) {
      log_warn("  Batch retry failed: {e$message}")
      return(data.table::data.table())
    })
  })
  
  n_recovered <- if (nrow(recovered) > 0) length(unique(recovered$testId)) else 0
  log_info("  Round {round_number}: Recovered {n_recovered}/{length(missing_ids)} test IDs")
  
  return(recovered)
}

# ============================================================================
# Adaptive Fetch Framework (V2.4.2)
# ============================================================================

calculate_expected_tests <- function(start_date) {
  days <- as.numeric(Sys.Date() - as.Date(start_date))
  ceiling(days * CONFIG$tests_per_day)
}

select_fetch_strategy <- function(expected_tests) {
  if (expected_tests < CONFIG$parallel_threshold) {
    return("sequential")
  }
  return("parallel")
}

estimate_fetch_time <- function(expected_tests, strategy) {
  switch(strategy,
    "sequential" = 10 + (expected_tests * 0.05),
    "parallel" = (expected_tests / PARALLEL_CONFIG$n_workers) * CONFIG$per_test_latency_sec + CONFIG$parallel_overhead_sec
  )
}

fetch_sequential <- function(timeout_seconds) {
  start_time <- Sys.time()
  
  log_info("Using SEQUENTIAL fetch strategy (bundled API)...")
  
  result <- tryCatch({
    R.utils::withTimeout({
      valdr::get_forcedecks_data()
    }, timeout = timeout_seconds)
  }, TimeoutException = function(e) {
    log_error("Sequential fetch timeout after {timeout_seconds} seconds")
    NULL
  }, error = function(e) {
    log_error("Sequential fetch error: {e$message}")
    NULL
  })
  
  elapsed <- round(as.numeric(difftime(Sys.time(), start_time, units = "secs")), 1)
  
  if (is.null(result)) {
    log_warn("Sequential fetch returned NULL after {elapsed}s")
    return(NULL)
  }
  
  profiles <- if (!is.null(result$profiles)) data.table::as.data.table(result$profiles) else data.table::data.table()
  tests <- if (!is.null(result$tests)) data.table::as.data.table(result$tests) else data.table::data.table()
  trials <- if (!is.null(result$trials)) data.table::as.data.table(result$trials) else data.table::data.table()
  
  log_info("Sequential fetch complete in {elapsed}s: {nrow(profiles)} profiles, {nrow(tests)} tests, {nrow(trials)} trials")
  
  return(list(
    profiles = profiles,
    result_definitions = data.table::data.table(),
    tests = tests,
    trials = trials
  ))
}

fetch_trials_chunk_with_retry <- function(chunk, max_retries = PARALLEL_CONFIG$max_retries) {
  chunk_size <- nrow(chunk)
  
  for (attempt in seq_len(max_retries + 1)) {
    result <- tryCatch({
      trials <- valdr::get_forcedecks_trials_only(chunk)
      return(data.table::as.data.table(trials))
    }, error = function(e) {
      is_rate_limit <- grepl("429|rate|limit|throttl", e$message, ignore.case = TRUE)
      
      if (is_rate_limit && attempt <= max_retries) {
        log_warn("Rate limit hit, retry {attempt}/{max_retries}")
        Sys.sleep(2^attempt)
        return(NULL)
      }
      
      log_error("Chunk fetch failed: {e$message}")
      log_error("Chunk size: {chunk_size} tests")
      log_error("Attempt: {attempt}/{max_retries}")
      
      if (attempt <= max_retries) {
        log_warn("Retrying after error...")
        Sys.sleep(2^attempt)
        return(NULL)
      }
      
      log_error("CHUNK FAILED after {max_retries} retries - returning empty")
      return(data.table::data.table())
    })
    if (!is.null(result)) return(result)
  }
  return(data.table::data.table())
}

fetch_parallel <- function(timeout_seconds) {
  start_time <- Sys.time()
  N_WORKERS <- PARALLEL_CONFIG$n_workers
  STAGGER_SEC <- PARALLEL_CONFIG$stagger_sec
  
  log_info("Using PARALLEL fetch strategy ({N_WORKERS} workers, {STAGGER_SEC*1000}ms stagger)...")
  
  profiles <- tryCatch({
    result <- valdr::get_profiles_only()
    data.table::as.data.table(result)
  }, error = function(e) {
    log_error("Failed to fetch profiles: {e$message}")
    data.table::data.table()
  })
  
  if (nrow(profiles) > 0) {
    log_info("Fetched {nrow(profiles)} profiles")
  } else {
    log_warn("No profiles returned")
  }
  
  tests <- tryCatch({
    result <- valdr::get_forcedecks_tests_only()
    data.table::as.data.table(result)
  }, error = function(e) {
    log_error("Failed to fetch tests: {e$message}")
    data.table::data.table()
  })
  
  if (nrow(tests) == 0) {
    log_warn("No tests returned from API")
    return(list(
      profiles = profiles,
      result_definitions = data.table::data.table(),
      tests = data.table::data.table(),
      trials = data.table::data.table()
    ))
  }
  
  log_info("Fetched {nrow(tests)} tests, starting parallel trials fetch...")
  
  requested_test_ids <- unique(tests$testId)
  n_requested <- length(requested_test_ids)
  
  trials <- tryCatch({
    n_tests <- nrow(tests)
    chunk_indices <- cut(seq_len(n_tests), N_WORKERS, labels = FALSE)
    chunks <- split(tests, chunk_indices)
    
    log_info("Split {n_tests} tests into {length(chunks)} chunks")
    
    future::plan(future::multisession, workers = N_WORKERS)
    
    trials_list <- furrr::future_map(seq_along(chunks), function(i) {
      Sys.sleep((i - 1) * STAGGER_SEC)
      fetch_trials_chunk_with_retry(chunks[[i]])
    }, .options = furrr::furrr_options(seed = TRUE))
    
    future::plan(future::sequential)
    
    trials_combined <- data.table::rbindlist(trials_list, use.names = TRUE, fill = TRUE)
    
    log_info("Fetched {nrow(trials_combined)} trials across {N_WORKERS} workers")
    
    retrieved_test_ids <- unique(trials_combined$testId)
    n_retrieved <- length(retrieved_test_ids)
    retrieval_rate <- n_retrieved / n_requested
    
    log_info("Trial retrieval: {n_retrieved}/{n_requested} tests ({round(retrieval_rate*100, 1)}%)")
    
    chunk_sizes <- sapply(trials_list, nrow)
    log_info("Chunk sizes: {paste(chunk_sizes, collapse = ', ')}")
    
    empty_chunks <- which(chunk_sizes == 0)
    if (length(empty_chunks) > 0) {
      log_error("EMPTY CHUNKS DETECTED: {paste(empty_chunks, collapse = ', ')}")
    }
    
    missing_test_ids <- setdiff(requested_test_ids, retrieved_test_ids)
    
    if (length(missing_test_ids) > 0) {
      log_warn("RECONCILIATION: {length(missing_test_ids)} test IDs missing from parallel fetch")
      
      max_rounds <- 3
      for (round in 1:max_rounds) {
        if (length(missing_test_ids) == 0) break
        
        recovered_trials <- reconcile_missing_trials(missing_test_ids, round)
        
        if (nrow(recovered_trials) > 0) {
          trials_combined <- data.table::rbindlist(
            list(trials_combined, recovered_trials), 
            fill = TRUE
          )
          
          retrieved_test_ids <- unique(trials_combined$testId)
          missing_test_ids <- setdiff(requested_test_ids, retrieved_test_ids)
          
          log_info("Reconciliation recovered {nrow(recovered_trials)} additional trials")
        }
        
        if (length(missing_test_ids) == 0) {
          log_info("RECONCILIATION COMPLETE: All test IDs retrieved after {round} round(s)")
          break
        }
      }
      
      if (length(missing_test_ids) > 0) {
        pct_missing <- round(length(missing_test_ids)/n_requested*100, 1)
        log_warn("Reconciliation incomplete: {length(missing_test_ids)} test IDs still missing ({pct_missing}%)")
      }
    } else {
      log_info("AUDIT PASSED: All {n_requested} test IDs retrieved on first attempt")
    }
    
    trials_combined
    
  }, error = function(e) {
    log_error("Parallel trials fetch failed: {e$message}")
    future::plan(future::sequential)
    
    log_warn("Falling back to sequential trials fetch...")
    tryCatch({
      result <- valdr::get_forcedecks_trials_only(tests)
      data.table::as.data.table(result)
    }, error = function(e2) {
      log_error("Sequential fallback also failed: {e2$message}")
      data.table::data.table()
    })
  })
  
  elapsed <- round(as.numeric(difftime(Sys.time(), start_time, units = "secs")), 1)
  log_info("Parallel fetch complete in {elapsed}s: {nrow(profiles)} profiles, {nrow(tests)} tests, {nrow(trials)} trials")
  
  return(list(
    profiles = profiles,
    result_definitions = data.table::data.table(),
    tests = tests,
    trials = trials
  ))
}

adaptive_fetch_forcedecks <- function(timeout_seconds = CONFIG$timeout_fd_full) {
  
  expected_tests <- calculate_expected_tests(CONFIG$start_date)
  log_info("Expected tests: {expected_tests} (from {CONFIG$start_date})")
  
  strategy <- select_fetch_strategy(expected_tests)
  estimated_time <- estimate_fetch_time(expected_tests, strategy)
  log_info("Selected strategy: {strategy}")
  log_info("Estimated time: {round(estimated_time, 0)}s")
  
  result <- NULL
  actual_start <- Sys.time()
  
  if (strategy == "sequential") {
    result <- fetch_sequential(timeout_seconds)
    if (is.null(result) || nrow(result$trials) == 0) {
      log_warn("Sequential fetch failed, falling back to parallel...")
      strategy <- "parallel"
      result <- NULL
    }
  }
  
  if (strategy == "parallel") {
    result <- fetch_parallel(timeout_seconds)
    if (is.null(result) || nrow(result$trials) == 0) {
      log_warn("Parallel fetch failed, trying sequential as fallback...")
      result <- fetch_sequential(timeout_seconds)
    }
  }
  
  actual_time <- round(as.numeric(difftime(Sys.time(), actual_start, units = "secs")), 1)
  log_info("Actual time: {actual_time}s")
  
  if (is.null(result)) {
    log_error("All fetch strategies failed")
    return(list(
      profiles = data.table::data.table(),
      result_definitions = data.table::data.table(),
      tests = data.table::data.table(),
      trials = data.table::data.table()
    ))
  }
  
  profiles_n <- if (!is.null(result$profiles)) nrow(result$profiles) else 0
  tests_n <- if (!is.null(result$tests)) nrow(result$tests) else 0
  trials_n <- if (!is.null(result$trials)) nrow(result$trials) else 0
  
  log_and_store("ForceDecks fetch complete: {profiles_n} profiles, {tests_n} tests, {trials_n} trials")
  
  return(result)
}

safe_fetch_forcedecks <- adaptive_fetch_forcedecks

safe_fetch_nordbord <- function(timeout_seconds = CONFIG$timeout_nordbord) {
  start_time <- Sys.time()
  
  result <- tryCatch({
    R.utils::withTimeout({
      valdr::get_nordbord_data()
    }, timeout = timeout_seconds)
  }, TimeoutException = function(e) {
    log_error("Nordbord fetch timeout after {timeout_seconds} seconds")
    NULL
  }, error = function(e) {
    log_error("Nordbord API error: {e$message}")
    NULL
  })
  
  elapsed <- round(as.numeric(difftime(Sys.time(), start_time, units = "secs")), 1)
  
  if (is.null(result)) {
    log_warn("Nordbord fetch returned NULL after {elapsed} seconds")
    return(list(tests = data.table::data.table()))
  }
  
  if (!"tests" %in% names(result)) {
    log_warn("Nordbord response missing 'tests' component")
  }
  
  for (comp in names(result)) {
    if (is.data.frame(result[[comp]])) {
      result[[comp]] <- data.table::as.data.table(result[[comp]])
    }
  }
  
  tests_n <- if (!is.null(result$tests)) nrow(result$tests) else 0
  log_info("Nordbord fetch complete in {elapsed}s: {tests_n} tests")
  
  return(result)
}

# ============================================================================
# CMJ Validation Helper Functions
# ============================================================================

create_session_ids <- function(vald_id, dt, gap_hours = CONFIG$session_gap_hours) {
  if (length(dt) == 0) return(character(0))
  ord <- order(dt)
  dt_sorted <- dt[ord]
  breaks <- c(TRUE, difftime(dt_sorted[-1], dt_sorted[-length(dt_sorted)], units = "hours") > gap_hours)
  session_num <- cumsum(breaks)
  out <- character(length(dt))
  out[ord] <- paste0(vald_id[1], "_", session_num)
  out
}

adaptive_z_vectorized <- function(dt, value_col, date_col, id_col,
                                   windows = CONFIG$z_windows, min_obs = CONFIG$z_min_observations) {
  data.table::setDT(dt)
  z_col <- paste0("z_", value_col)
  data.table::setkeyv(dt, c(id_col, date_col))
  dt[, (z_col) := NA_real_]
  
  for (w in windows[is.finite(windows)]) {
    mean_col <- paste0(".mean_", w)
    sd_col <- paste0(".sd_", w)
    n_col <- paste0(".n_", w)
    dt[, (mean_col) := data.table::frollapply(get(value_col), n = w, FUN = function(x) mean(x, na.rm = TRUE), align = "right", fill = NA), by = c(id_col)]
    dt[, (sd_col) := data.table::frollapply(get(value_col), n = w, FUN = function(x) sd(x, na.rm = TRUE), align = "right", fill = NA), by = c(id_col)]
    dt[, (n_col) := data.table::frollapply(!is.na(get(value_col)), n = w, FUN = sum, align = "right", fill = NA), by = c(id_col)]
  }
  
  dt[, (z_col) := data.table::fifelse(!is.na(.mean_30) & .n_30 >= min_obs & .sd_30 > 0, (get(value_col) - .mean_30) / .sd_30,
    data.table::fifelse(!is.na(.mean_60) & .n_60 >= min_obs & .sd_60 > 0, (get(value_col) - .mean_60) / .sd_60,
    data.table::fifelse(!is.na(.mean_90) & .n_90 >= min_obs & .sd_90 > 0, (get(value_col) - .mean_90) / .sd_90, NA_real_)))]
  
  temp_cols <- grep("^\\.(mean|sd|n)_", names(dt), value = TRUE)
  if (length(temp_cols) > 0) dt[, (temp_cols) := NULL]
  return(z_col)
}

robust_z <- function(x) {
  med <- median(x, na.rm = TRUE)
  mad_val <- mad(x, na.rm = TRUE)
  if (is.na(mad_val) || mad_val == 0) return(rep(NA_real_, length(x)))
  (x - med) / mad_val
}

# ============================================================================
# QC Layer Functions
# ============================================================================
apply_layer1 <- function(dt) {
  data.table::setDT(dt)
  if (!"jump_height_inches_imp_mom" %in% names(dt) || !"peak_takeoff_velocity" %in% names(dt)) {
    dt[, physics_flag := FALSE]
    return(dt)
  }
  dt[, expected_v := sqrt(2 * 9.81 * (jump_height_inches_imp_mom * 0.0254))]
  dt[, v_ratio := peak_takeoff_velocity / expected_v]
  dt[, physics_flag := data.table::fifelse(is.na(v_ratio), FALSE,
    v_ratio < CONFIG$physics_v_ratio_min | v_ratio > CONFIG$physics_v_ratio_max)]
  dt[, c("expected_v", "v_ratio") := NULL]
  return(dt)
}

apply_layer2 <- function(dt) {
  data.table::setDT(dt)
  if (!"body_weight_lbs" %in% names(dt) || !"vald_id" %in% names(dt)) {
    dt[, flag_bw_delta := FALSE]
    return(dt)
  }
  data.table::setorder(dt, vald_id, date)
  dt[, median_bw := data.table::frollmean(body_weight_lbs, n = 10, na.rm = TRUE, align = "right"), by = vald_id]
  dt[, flag_bw_delta := data.table::fifelse(is.na(median_bw) | median_bw == 0, FALSE,
    abs(body_weight_lbs - median_bw) / median_bw > CONFIG$bodyweight_deviation_pct)]
  dt[, median_bw := NULL]
  return(dt)
}

apply_layer3 <- function(dt) {
  data.table::setDT(dt)
  if (!"session_id" %in% names(dt)) {
    dt[, flag_session_contamination := FALSE]
    return(dt)
  }
  dt[, session_count := .N, by = session_id]
  dt[, flag_session_contamination := data.table::fifelse(session_count > 10, TRUE, FALSE)]
  dt[, session_count := NULL]
  return(dt)
}

apply_layer4 <- function(dt) {
  data.table::setDT(dt)
  z_cols <- grep("^z_", names(dt), value = TRUE)
  if (length(z_cols) > 0) {
    dt[, n_extreme := rowSums(sapply(.SD, function(x) abs(x) > 3), na.rm = TRUE), .SDcols = z_cols]
    dt[, flag_multiple_extremes := data.table::fifelse(n_extreme >= 3, TRUE, FALSE)]
    dt[, n_extreme := NULL]
  } else {
    dt[, flag_multiple_extremes := FALSE]
  }
  return(dt)
}

apply_final_and_clean <- function(dt) {
  data.table::setDT(dt)
  
  if (!"physics_flag" %in% names(dt)) dt[, physics_flag := FALSE]
  if (!"flag_bw_delta" %in% names(dt)) dt[, flag_bw_delta := FALSE]
  if (!"flag_session_contamination" %in% names(dt)) dt[, flag_session_contamination := FALSE]
  if (!"flag_multiple_extremes" %in% names(dt)) dt[, flag_multiple_extremes := FALSE]
  
  dt[, final_classification := data.table::fcase(
    physics_flag == TRUE, "LIKELY_INACCURATE_MEASUREMENT",
    flag_bw_delta == TRUE & flag_session_contamination == TRUE, "LIKELY_INACCURATE_MEASUREMENT",
    flag_multiple_extremes == TRUE & (flag_bw_delta == TRUE | flag_session_contamination == TRUE), "VALID_BUT_OUTLIER",
    flag_multiple_extremes == TRUE, "VALID_EXTREME",
    default = "LIKELY_VALID")]
  
  clean_classifications <- c("LIKELY_VALID", "VALID_EXTREME")
  
  if ("jump_height_inches_imp_mom" %in% names(dt)) {
    dt[, jump_height_clean_inches := data.table::fifelse(final_classification %in% clean_classifications, jump_height_inches_imp_mom, NA_real_)]
  }
  if ("peak_takeoff_velocity" %in% names(dt)) {
    dt[, takeoff_v_ms_clean := data.table::fifelse(final_classification %in% clean_classifications, peak_takeoff_velocity, NA_real_)]
  }
  if ("rsi_modified_imp_mom" %in% names(dt)) {
    dt[, rsi_modified_imp_mom_clean := data.table::fifelse(final_classification %in% clean_classifications, rsi_modified_imp_mom, NA_real_)]
  }
  
  return(dt)
}

# ============================================================================
# Multi-Layer Calibration Error Detection
# ============================================================================
apply_calibration_error_detection <- function(dt) {
  data.table::setDT(dt)
  
  log_info("Running calibration error detection...")
  
  if (!all(c("jump_height_inches_imp_mom", "body_weight_lbs", "vald_id", "date") %in% names(dt))) {
    log_warn("Missing required columns for calibration detection, skipping")
    dt[, qc_flag := "PASS"]
    return(dt)
  }
  
  dt[, `:=`(
    jh_z = (jump_height_inches_imp_mom - mean(jump_height_inches_imp_mom, na.rm = TRUE)) / 
           sd(jump_height_inches_imp_mom, na.rm = TRUE),
    bw_z = (body_weight_lbs - mean(body_weight_lbs, na.rm = TRUE)) / 
           sd(body_weight_lbs, na.rm = TRUE)
  ), by = vald_id]
  
  dt[is.nan(jh_z) | is.infinite(jh_z), jh_z := 0]
  dt[is.nan(bw_z) | is.infinite(bw_z), bw_z := 0]
  
  dt[, combined_magnitude := abs(jh_z) + abs(bw_z)]
  
  dt[, inverse_outlier := (
    (abs(jh_z) > 2.0) & 
    (abs(bw_z) > 2.0) & 
    (jh_z * bw_z < 0)
  )]
  
  dt[, qc_flag := data.table::fcase(
    combined_magnitude > 5.0 & inverse_outlier == TRUE, "CALIBRATION_ERROR_CRITICAL",
    combined_magnitude > 4.0 & inverse_outlier == TRUE, "CALIBRATION_ERROR_HIGH",
    default = "PASS"
  )]
  
  if ("team" %in% names(dt)) {
    cluster_analysis <- dt[qc_flag %in% c("CALIBRATION_ERROR_CRITICAL", "CALIBRATION_ERROR_HIGH"), 
                           .(n_outliers = .N), by = .(team, date)]
    
    equipment_errors <- cluster_analysis[n_outliers >= 3]
    
    if (nrow(equipment_errors) > 0) {
      log_warn("Systematic errors detected: {nrow(equipment_errors)} testing sessions")
      dt[, equipment_error_flag := FALSE]
      dt[equipment_errors, on = .(team, date), equipment_error_flag := TRUE]
    } else {
      dt[, equipment_error_flag := FALSE]
    }
  } else {
    dt[, equipment_error_flag := FALSE]
  }
  
  n_critical <- sum(dt$qc_flag == "CALIBRATION_ERROR_CRITICAL", na.rm = TRUE)
  n_high <- sum(dt$qc_flag == "CALIBRATION_ERROR_HIGH", na.rm = TRUE)
  
  log_info("Calibration errors: CRITICAL={n_critical}, HIGH={n_high}")
  
  cleanup_cols <- c("jh_z", "bw_z", "combined_magnitude", "inverse_outlier")
  cleanup_cols <- intersect(cleanup_cols, names(dt))
  if (length(cleanup_cols) > 0) dt[, (cleanup_cols) := NULL]
  
  return(dt)
}

# ============================================================================
# Schema Validation Functions
# ============================================================================
standardize_data_types <- function(dt) {
  data.table::setDT(dt)
  for (col in names(dt)) {
    if (is.factor(dt[[col]])) dt[, (col) := as.character(get(col))]
    if (grepl("height|weight|force|power|velocity|impulse|duration|ratio|rsi|stiffness", col, ignore.case = TRUE)) {
      if (!is.numeric(dt[[col]])) {
        tryCatch({
          dt[, (col) := as.numeric(get(col))]
        }, error = function(e) NULL)
      }
    }
  }
  return(dt)
}

# ============================================================================
# Secondary Table Processing Functions (V2.4.2)
# ============================================================================

process_nordbord <- function(nord_raw, roster) {
  data.table::setDT(nord_raw)
  
  log_info("Processing Nordbord with unilateral detection...")
  
  nord_raw[, modifiedDateUtc_chr := as.character(modifiedDateUtc)]
  nord_raw[, modifiedDateUtc_parsed := data.table::fcoalesce(
    lubridate::ymd_hms(modifiedDateUtc_chr, tz = "UTC", quiet = TRUE),
    lubridate::ymd_hm(modifiedDateUtc_chr, tz = "UTC", quiet = TRUE),
    lubridate::ymd_h(modifiedDateUtc_chr, tz = "UTC", quiet = TRUE),
    lubridate::ymd(modifiedDateUtc_chr, tz = "UTC", quiet = TRUE)
  )]
  nord_raw[, modifiedDateUtc_local := lubridate::with_tz(modifiedDateUtc_parsed, CONFIG$timezone)]
  nord_raw[, date := as.Date(modifiedDateUtc_local)]
  
  nord_raw[, `:=`(
    vald_id = athleteId,
    test_ID = testId,
    test_type = testTypeName
  )]
  
  nord_raw[, `:=`(
    avg_force_left = data.table::fifelse(leftRepetitions == 0, NA_real_, leftAvgForce - leftCalibration),
    max_force_left = data.table::fifelse(leftRepetitions == 0, NA_real_, leftMaxForce - leftCalibration),
    avg_force_right = data.table::fifelse(rightRepetitions == 0, NA_real_, rightAvgForce - rightCalibration),
    max_force_right = data.table::fifelse(rightRepetitions == 0, NA_real_, rightMaxForce - rightCalibration),
    reps_left = leftRepetitions,
    reps_right = rightRepetitions
  )]
  
  nord_raw[, `:=`(
    avg_force_bilateral = rowMeans(cbind(avg_force_left, avg_force_right), na.rm = TRUE),
    max_force_bilateral = rowMeans(cbind(max_force_left, max_force_right), na.rm = TRUE)
  )]
  
  nord_raw[, avg_force_asymmetry := data.table::fcase(
    is.na(avg_force_left) | is.na(avg_force_right), NA_real_,
    avg_force_left == 0 & avg_force_right == 0, NA_real_,
    avg_force_left >= avg_force_right, (avg_force_left - avg_force_right) / avg_force_left,
    default = (avg_force_right - avg_force_left) / avg_force_right
  )]
  
  nord_raw[, max_force_asymmetry := data.table::fcase(
    is.na(max_force_left) | is.na(max_force_right), NA_real_,
    max_force_left == 0 & max_force_right == 0, NA_real_,
    max_force_left >= max_force_right, (max_force_left - max_force_right) / max_force_left,
    default = (max_force_right - max_force_left) / max_force_right
  )]
  
  nord_raw[, is_unilateral := (
    grepl("ISO", test_type, ignore.case = TRUE) &
    (reps_left != reps_right | abs(max_force_asymmetry) > 0.70)
  )]
  nord_raw[is.na(is_unilateral), is_unilateral := FALSE]
  
  nord_raw[, tested_side := data.table::fcase(
    is_unilateral == FALSE, "Both",
    reps_left > reps_right, "Left",
    reps_right > reps_left, "Right",
    !is.na(max_force_left) & !is.na(max_force_right) & max_force_left > max_force_right, "Left",
    default = "Right"
  )]
  
  nord_raw[, trial_limb := tested_side]
  
  nord_raw[is_unilateral == TRUE & grepl("ISO.*Prone", test_type, ignore.case = TRUE),
           test_type := "ISO Prone - Unilateral"]
  
  nord_raw[tested_side == "Left", `:=`(
    max_force_right = NA_real_,
    avg_force_right = NA_real_,
    avg_force_bilateral = avg_force_left,
    max_force_bilateral = max_force_left,
    avg_force_asymmetry = NA_real_,
    max_force_asymmetry = NA_real_
  )]
  
  nord_raw[tested_side == "Right", `:=`(
    max_force_left = NA_real_,
    avg_force_left = NA_real_,
    avg_force_bilateral = avg_force_right,
    max_force_bilateral = max_force_right,
    avg_force_asymmetry = NA_real_,
    max_force_asymmetry = NA_real_
  )]
  
  if (exists("fd_raw", envir = .GlobalEnv) && 
      is.data.table(get("fd_raw", envir = .GlobalEnv)) && 
      "body_weight_lbs" %in% names(get("fd_raw", envir = .GlobalEnv))) {
    
    fd_data <- get("fd_raw", envir = .GlobalEnv)
    
    if ("date" %in% names(fd_data)) {
      fd_bw <- fd_data[!is.na(body_weight_lbs)][
        order(date)
      ][, 
        .(body_weight_lbs = last(body_weight_lbs)),
        by = vald_id
      ]
    } else {
      fd_bw <- fd_data[!is.na(body_weight_lbs), 
                       .(body_weight_lbs = last(body_weight_lbs)),
                       by = vald_id]
    }
    
    nord_raw <- merge(nord_raw, fd_bw, by = "vald_id", all.x = TRUE)
    
    n_matched <- sum(!is.na(nord_raw$body_weight_lbs))
    n_total <- nrow(nord_raw)
    log_info("Body weight from ForceDecks: {n_matched}/{n_total} ({round(n_matched/n_total*100, 1)}%)")
    
  } else {
    nord_raw[, body_weight_lbs := NA_real_]
    log_warn("No ForceDecks data available for body weight lookup")
  }
  
  nord_raw[, `:=`(
    max_force_relative_bw = data.table::fifelse(
      is.na(body_weight_lbs) | body_weight_lbs == 0, NA_real_,
      max_force_bilateral / body_weight_lbs
    ),
    max_force_left_relative_bw = data.table::fifelse(
      tested_side == "Right" | is.na(body_weight_lbs) | body_weight_lbs == 0, NA_real_,
      max_force_left / body_weight_lbs
    ),
    max_force_right_relative_bw = data.table::fifelse(
      tested_side == "Left" | is.na(body_weight_lbs) | body_weight_lbs == 0, NA_real_,
      max_force_right / body_weight_lbs
    )
  )]
  
  n_unilateral <- sum(nord_raw$is_unilateral, na.rm = TRUE)
  n_bilateral <- sum(!nord_raw$is_unilateral, na.rm = TRUE)
  log_info("Nordbord: {n_bilateral} bilateral, {n_unilateral} unilateral tests detected")
  
  for (col in NORD_EXPORT_COLUMNS) {
    if (!col %in% names(nord_raw)) {
      nord_raw[, (col) := NA]
    }
  }
  
  nord_export <- nord_raw[, ..NORD_EXPORT_COLUMNS]
  
  log_info("Nordbord export: {nrow(nord_export)} rows, {ncol(nord_export)} columns")
  
  return(nord_export)
}

process_imtp <- function(imtp_raw) {
  data.table::setDT(imtp_raw)
  
  log_info("Processing IMTP with performance scores...")
  
  required_metrics <- c("iso_bm_rel_force_peak", "iso_bm_rel_force_100", "iso_bw_rel_force_peak",
                        "rfd_at_100ms", "start_to_peak_force", "asym_peak_vertical_force")
  
  for (col in required_metrics) {
    if (!col %in% names(imtp_raw)) {
      imtp_raw[, (col) := NA_real_]
    }
  }
  
  if (nrow(imtp_raw) > 1) {
    imtp_raw[, calc_perf := data.table::frank(iso_bm_rel_force_peak, na.last = "keep") / .N]
    imtp_raw[, performance_score := data.table::frank(calc_perf, na.last = "keep") / .N * 100]
    
    if ("team" %in% names(imtp_raw)) {
      imtp_raw[, team_performance_score := data.table::frank(calc_perf, na.last = "keep") / .N * 100, by = team]
    } else {
      imtp_raw[, team_performance_score := performance_score]
    }
    
    imtp_raw[, calc_perf := NULL]
  } else {
    imtp_raw[, `:=`(performance_score = NA_real_, team_performance_score = NA_real_)]
  }
  
  for (col in IMTP_EXPORT_COLUMNS) {
    if (!col %in% names(imtp_raw)) {
      imtp_raw[, (col) := NA]
    }
  }
  
  imtp_export <- imtp_raw[, ..IMTP_EXPORT_COLUMNS]
  
  log_info("IMTP export: {nrow(imtp_export)} rows, {ncol(imtp_export)} columns")
  
  return(imtp_export)
}

process_dj <- function(dj_raw) {
  data.table::setDT(dj_raw)
  
  log_info("Processing Drop Jump with QC layers and ratios...")
  
  if (!"body_weight_lbs" %in% names(dj_raw)) {
    dj_raw[, body_weight_lbs := NA_real_]
  }
  
  has_pcf <- "peak_concentric_force" %in% names(dj_raw)
  has_rpcf <- "relative_peak_concentric_force" %in% names(dj_raw)
  
  if (has_pcf && has_rpcf) {
    can_impute <- is.na(dj_raw$body_weight_lbs) & 
                  !is.na(dj_raw$peak_concentric_force) & 
                  !is.na(dj_raw$relative_peak_concentric_force) &
                  dj_raw$relative_peak_concentric_force != 0
    
    if (any(can_impute)) {
      dj_raw[can_impute, body_weight_lbs := peak_concentric_force / relative_peak_concentric_force]
      log_info("DJ: Imputed body weight for {sum(can_impute)} tests")
    }
  }
  
  if ("jump_height_inches_imp_mom" %in% names(dj_raw)) {
    n_before <- nrow(dj_raw)
    dj_raw <- dj_raw[is.na(jump_height_inches_imp_mom) | 
                     (jump_height_inches_imp_mom >= CONFIG$jump_height_min_inches & 
                      jump_height_inches_imp_mom <= CONFIG$jump_height_max_inches)]
    n_removed <- n_before - nrow(dj_raw)
    if (n_removed > 0) log_info("DJ: Removed {n_removed} tests outside jump height bounds")
  }
  
  if ("jump_height_inches_imp_mom" %in% names(dj_raw) && "peak_takeoff_velocity" %in% names(dj_raw)) {
    dj_raw[, expected_v := sqrt(2 * 9.81 * (jump_height_inches_imp_mom * 0.0254))]
    dj_raw[, v_ratio := peak_takeoff_velocity / expected_v]
    dj_raw[, physics_flag := data.table::fifelse(is.na(v_ratio), FALSE,
      v_ratio < CONFIG$physics_v_ratio_min | v_ratio > CONFIG$physics_v_ratio_max)]
    
    n_physics_fail <- sum(dj_raw$physics_flag, na.rm = TRUE)
    if (n_physics_fail > 0) log_info("DJ: {n_physics_fail} tests flagged for physics validation")
  } else {
    dj_raw[, physics_flag := FALSE]
  }
  
  if ("body_weight_lbs" %in% names(dj_raw) && "vald_id" %in% names(dj_raw)) {
    data.table::setorder(dj_raw, vald_id, date)
    dj_raw[, median_bw := data.table::frollmean(body_weight_lbs, n = 10, na.rm = TRUE, align = "right"), by = vald_id]
    dj_raw[, flag_bw_delta := data.table::fifelse(is.na(median_bw) | median_bw == 0, FALSE,
      abs(body_weight_lbs - median_bw) / median_bw > CONFIG$bodyweight_deviation_pct)]
    dj_raw[, median_bw := NULL]
  } else {
    dj_raw[, flag_bw_delta := FALSE]
  }
  
  dj_raw[, final_classification := data.table::fcase(
    physics_flag == TRUE, "LIKELY_INACCURATE_MEASUREMENT",
    flag_bw_delta == TRUE, "VALID_BUT_OUTLIER",
    default = "LIKELY_VALID"
  )]
  
  clean_classifications <- c("LIKELY_VALID", "VALID_EXTREME")
  n_before <- nrow(dj_raw)
  dj_raw <- dj_raw[final_classification %in% clean_classifications]
  n_removed <- n_before - nrow(dj_raw)
  if (n_removed > 0) log_info("DJ: Removed {n_removed} tests failing QC")
  
  if ("peak_takeoff_velocity" %in% names(dj_raw) && "peak_landing_velocity" %in% names(dj_raw)) {
    dj_raw[, velocity_ratio := data.table::fifelse(
      is.na(peak_landing_velocity) | peak_landing_velocity == 0, NA_real_,
      peak_takeoff_velocity / peak_landing_velocity
    )]
  } else {
    dj_raw[, velocity_ratio := NA_real_]
  }
  
  if ("peak_impact_force" %in% names(dj_raw) && "peak_driveoff_force" %in% names(dj_raw)) {
    dj_raw[, force_ratio := data.table::fifelse(
      is.na(peak_driveoff_force) | peak_driveoff_force == 0, NA_real_,
      peak_impact_force / peak_driveoff_force
    )]
  } else {
    dj_raw[, force_ratio := NA_real_]
  }
  
  if ("active_stiffness" %in% names(dj_raw) && "passive_stiffness" %in% names(dj_raw)) {
    dj_raw[, stiffness_ratio := data.table::fifelse(
      is.na(passive_stiffness) | passive_stiffness == 0, NA_real_,
      active_stiffness / passive_stiffness
    )]
  } else {
    dj_raw[, stiffness_ratio := NA_real_]
  }
  
  if ("peak_impact_force" %in% names(dj_raw)) {
    dj_raw[, rel_impact_force := data.table::fifelse(
      is.na(body_weight_lbs) | body_weight_lbs == 0, NA_real_,
      peak_impact_force / body_weight_lbs
    )]
  } else {
    dj_raw[, rel_impact_force := NA_real_]
  }
  
  if ("peak_driveoff_force" %in% names(dj_raw)) {
    dj_raw[, rel_driveoff_force := data.table::fifelse(
      is.na(body_weight_lbs) | body_weight_lbs == 0, NA_real_,
      peak_driveoff_force / body_weight_lbs
    )]
  } else {
    dj_raw[, rel_driveoff_force := NA_real_]
  }
  
  if (!"asym_peak_impact_force" %in% names(dj_raw)) dj_raw[, asym_peak_impact_force := NA_real_]
  if (!"asym_peak_driveoff_force" %in% names(dj_raw)) dj_raw[, asym_peak_driveoff_force := NA_real_]
  
  for (col in DJ_EXPORT_COLUMNS) {
    if (!col %in% names(dj_raw)) {
      dj_raw[, (col) := NA]
    }
  }
  
  dj_export <- dj_raw[, ..DJ_EXPORT_COLUMNS]
  
  log_info("DJ export: {nrow(dj_export)} rows, {ncol(dj_export)} columns")
  
  return(dj_export)
}

process_sl_jumps <- function(slj_raw) {
  data.table::setDT(slj_raw)
  
  log_info("Processing Single-Leg Jumps with pivot and QC...")
  
  if (!"body_weight_lbs" %in% names(slj_raw)) {
    slj_raw[, body_weight_lbs := NA_real_]
  }
  
  has_pcf <- "peak_concentric_force" %in% names(slj_raw)
  has_rpcf <- "relative_peak_concentric_force" %in% names(slj_raw)
  
  if (has_pcf && has_rpcf) {
    can_impute <- is.na(slj_raw$body_weight_lbs) & 
                  !is.na(slj_raw$peak_concentric_force) & 
                  !is.na(slj_raw$relative_peak_concentric_force) &
                  slj_raw$relative_peak_concentric_force != 0
    
    if (any(can_impute)) {
      slj_raw[can_impute, body_weight_lbs := peak_concentric_force / relative_peak_concentric_force]
      log_info("SLJ: Imputed body weight for {sum(can_impute)} tests")
    }
  }
  
  if (!"trial_limb" %in% names(slj_raw)) {
    slj_raw[, trial_limb := "Both"]
  }
  
  slj_raw[, trial_limb := data.table::fcase(
    grepl("left|L", trial_limb, ignore.case = TRUE), "Left",
    grepl("right|R", trial_limb, ignore.case = TRUE), "Right",
    default = "Both"
  )]
  
  slj_metrics <- c("jump_height_inches_imp_mom", "lower_limb_stiffness", 
                   "relative_peak_concentric_force", "relative_peak_eccentric_force",
                   "weight_relative_peak_takeoff_force", "weight_relative_peak_landing_force",
                   "time_to_peak_force", "rsi_modified_imp_mom")
  
  slj_metrics <- intersect(slj_metrics, names(slj_raw))
  
  if (length(slj_metrics) == 0) {
    log_warn("SLJ: No metrics found for pivot")
    return(data.table::data.table())
  }
  
  slj_summary <- slj_raw[, .(
    has_left = any(trial_limb == "Left"),
    has_right = any(trial_limb == "Right"),
    has_both_in_row = any(trial_limb == "Both"),
    test_ID = first(test_ID),
    body_weight_lbs = mean(body_weight_lbs, na.rm = TRUE)
  ), by = .(vald_id, date)]
  
  slj_summary[, `:=`(
    is_bilateral = (has_left & has_right) | has_both_in_row,
    tested_limb = data.table::fcase(
      has_left & has_right, "Both",
      has_both_in_row, "Both",
      has_left & !has_right, "Left",
      has_right & !has_left, "Right",
      default = "Both"
    )
  )]
  
  slj_summary[, test_type := data.table::fifelse(is_bilateral, "SLCMJ - Bilateral", "SLCMJ - Unilateral")]
  slj_summary[, trial_limb := tested_limb]
  
  slj_left <- slj_raw[trial_limb == "Left", 
                       c(lapply(.SD, function(x) mean(x, na.rm = TRUE))),
                       by = .(vald_id, date),
                       .SDcols = slj_metrics]
  
  slj_right <- slj_raw[trial_limb == "Right",
                        c(lapply(.SD, function(x) mean(x, na.rm = TRUE))),
                        by = .(vald_id, date),
                        .SDcols = slj_metrics]
  
  slj_both <- slj_raw[trial_limb == "Both",
                       c(lapply(.SD, function(x) mean(x, na.rm = TRUE))),
                       by = .(vald_id, date),
                       .SDcols = slj_metrics]
  
  if (nrow(slj_left) > 0) {
    old_names <- slj_metrics
    new_names <- paste0(slj_metrics, "_left")
    data.table::setnames(slj_left, old_names, new_names)
  }
  
  if (nrow(slj_right) > 0) {
    old_names <- slj_metrics
    new_names <- paste0(slj_metrics, "_right")
    data.table::setnames(slj_right, old_names, new_names)
  }
  
  slj_wide <- slj_summary[, .(vald_id, date, test_ID, body_weight_lbs, test_type, trial_limb)]
  
  if (nrow(slj_left) > 0) {
    slj_wide <- merge(slj_wide, slj_left, by = c("vald_id", "date"), all.x = TRUE)
  }
  
  if (nrow(slj_right) > 0) {
    slj_wide <- merge(slj_wide, slj_right, by = c("vald_id", "date"), all.x = TRUE)
  }
  
  if (nrow(slj_both) > 0) {
    for (metric in slj_metrics) {
      left_col <- paste0(metric, "_left")
      right_col <- paste0(metric, "_right")
      
      if (!left_col %in% names(slj_wide)) slj_wide[, (left_col) := NA_real_]
      if (!right_col %in% names(slj_wide)) slj_wide[, (right_col) := NA_real_]
      
      both_keys <- slj_both[, .(vald_id, date)]
      slj_wide[both_keys, on = .(vald_id, date), (left_col) := slj_both[[metric]]]
      slj_wide[both_keys, on = .(vald_id, date), (right_col) := slj_both[[metric]]]
    }
  }
  
  for (metric in slj_metrics) {
    left_col <- paste0(metric, "_left")
    right_col <- paste0(metric, "_right")
    if (!left_col %in% names(slj_wide)) slj_wide[, (left_col) := NA_real_]
    if (!right_col %in% names(slj_wide)) slj_wide[, (right_col) := NA_real_]
  }
  
  for (metric in slj_metrics) {
    left_col <- paste0(metric, "_left")
    right_col <- paste0(metric, "_right")
    bilateral_col <- paste0(metric, "_bilateral")
    
    slj_wide[, (bilateral_col) := data.table::fcase(
      !is.na(get(left_col)) & !is.na(get(right_col)), (get(left_col) + get(right_col)) / 2,
      !is.na(get(left_col)), get(left_col),
      !is.na(get(right_col)), get(right_col),
      default = NA_real_
    )]
  }
  
  if ("jump_height_inches_imp_mom_bilateral" %in% names(slj_wide)) {
    n_before <- nrow(slj_wide)
    slj_wide <- slj_wide[is.na(jump_height_inches_imp_mom_bilateral) | 
                         (jump_height_inches_imp_mom_bilateral >= CONFIG$jump_height_min_inches & 
                          jump_height_inches_imp_mom_bilateral <= CONFIG$jump_height_max_inches)]
    n_removed <- n_before - nrow(slj_wide)
    if (n_removed > 0) log_info("SLJ: Removed {n_removed} tests outside jump height bounds")
  }
  
  slj_wide[, physics_flag := FALSE]
  
  if ("body_weight_lbs" %in% names(slj_wide) && "vald_id" %in% names(slj_wide)) {
    data.table::setorder(slj_wide, vald_id, date)
    slj_wide[, median_bw := data.table::frollmean(body_weight_lbs, n = 10, na.rm = TRUE, align = "right"), by = vald_id]
    slj_wide[, flag_bw_delta := data.table::fifelse(is.na(median_bw) | median_bw == 0, FALSE,
      abs(body_weight_lbs - median_bw) / median_bw > CONFIG$bodyweight_deviation_pct)]
    slj_wide[, median_bw := NULL]
  } else {
    slj_wide[, flag_bw_delta := FALSE]
  }
  
  slj_wide[, final_classification := data.table::fcase(
    physics_flag == TRUE, "LIKELY_INACCURATE_MEASUREMENT",
    flag_bw_delta == TRUE, "VALID_BUT_OUTLIER",
    default = "LIKELY_VALID"
  )]
  
  clean_classifications <- c("LIKELY_VALID", "VALID_EXTREME", "VALID_BUT_OUTLIER")
  n_before <- nrow(slj_wide)
  slj_wide <- slj_wide[final_classification %in% clean_classifications]
  n_removed <- n_before - nrow(slj_wide)
  if (n_removed > 0) log_info("SLJ: Removed {n_removed} tests failing QC")
  
  if ("weight_relative_peak_landing_force_left" %in% names(slj_wide) && 
      "weight_relative_peak_landing_force_right" %in% names(slj_wide)) {
    slj_wide[, asym_peak_landing_force := data.table::fifelse(
      trial_limb != "Both" | is.na(weight_relative_peak_landing_force_left) | is.na(weight_relative_peak_landing_force_right),
      NA_real_,
      (weight_relative_peak_landing_force_right - weight_relative_peak_landing_force_left) / 
        ((weight_relative_peak_landing_force_right + weight_relative_peak_landing_force_left) / 2) * 100
    )]
  } else {
    slj_wide[, asym_peak_landing_force := NA_real_]
  }
  
  if ("weight_relative_peak_takeoff_force_left" %in% names(slj_wide) && 
      "weight_relative_peak_takeoff_force_right" %in% names(slj_wide)) {
    slj_wide[, asym_weight_relative_peak_takeoff_force := data.table::fifelse(
      trial_limb != "Both" | is.na(weight_relative_peak_takeoff_force_left) | is.na(weight_relative_peak_takeoff_force_right),
      NA_real_,
      (weight_relative_peak_takeoff_force_right - weight_relative_peak_takeoff_force_left) / 
        ((weight_relative_peak_takeoff_force_right + weight_relative_peak_takeoff_force_left) / 2) * 100
    )]
  } else {
    slj_wide[, asym_weight_relative_peak_takeoff_force := NA_real_]
  }
  
  n_bilateral <- sum(slj_wide$trial_limb == "Both", na.rm = TRUE)
  n_left_only <- sum(slj_wide$trial_limb == "Left", na.rm = TRUE)
  n_right_only <- sum(slj_wide$trial_limb == "Right", na.rm = TRUE)
  log_info("SLJ pivot complete: {n_bilateral} bilateral, {n_left_only} left-only, {n_right_only} right-only")
  
  for (col in SLJ_EXPORT_COLUMNS) {
    if (!col %in% names(slj_wide)) {
      slj_wide[, (col) := NA]
    }
  }
  
  slj_export <- slj_wide[, ..SLJ_EXPORT_COLUMNS]
  
  log_info("SLJ export: {nrow(slj_export)} rows, {ncol(slj_export)} columns")
  
  return(slj_export)
}

process_rsi <- function(rsi_raw) {
  data.table::setDT(rsi_raw)
  
  log_info("Processing RSI with pivot and limb detection...")
  
  if (!"trial_limb" %in% names(rsi_raw)) {
    rsi_raw[, trial_limb := "Both"]
  }
  
  rsi_raw[, trial_limb := data.table::fcase(
    grepl("left|L", trial_limb, ignore.case = TRUE), "Left",
    grepl("right|R", trial_limb, ignore.case = TRUE), "Right",
    default = "Both"
  )]
  
  rsi_metrics <- c("iso_bm_rel_force_peak", "iso_bm_rel_force_100", "iso_abs_impulse_100",
                   "rfd_at_100ms", "start_to_peak_force", "peak_vertical_force")
  
  rsi_metrics <- intersect(rsi_metrics, names(rsi_raw))
  
  if (length(rsi_metrics) == 0) {
    log_warn("RSI: No metrics found for pivot")
    return(data.table::data.table())
  }
  
  rsi_raw[, original_test_type := test_type]
  
  rsi_summary <- rsi_raw[, .(
    vald_id = first(vald_id),
    date = first(date),
    test_type = first(test_type),
    has_left = any(trial_limb == "Left"),
    has_right = any(trial_limb == "Right"),
    has_both_in_row = any(trial_limb == "Both"),
    original_test_type = first(original_test_type)
  ), by = .(test_ID)]
  
  rsi_summary[, `:=`(
    is_bilateral = (has_left & has_right) | has_both_in_row,
    tested_limb = data.table::fcase(
      has_left & has_right, "Both",
      has_both_in_row, "Both",
      has_left & !has_right, "Left",
      has_right & !has_left, "Right",
      default = "Both"
    )
  )]
  
  rsi_summary[, test_type := data.table::fifelse(
    is_bilateral,
    paste0(original_test_type, " - Bilateral"),
    paste0(original_test_type, " - Unilateral")
  )]
  rsi_summary[, trial_limb := tested_limb]
  
  rsi_left <- rsi_raw[trial_limb == "Left", 
                       c(lapply(.SD, function(x) mean(x, na.rm = TRUE))),
                       by = .(test_ID),
                       .SDcols = rsi_metrics]
  
  rsi_right <- rsi_raw[trial_limb == "Right",
                        c(lapply(.SD, function(x) mean(x, na.rm = TRUE))),
                        by = .(test_ID),
                        .SDcols = rsi_metrics]
  
  rsi_both <- rsi_raw[trial_limb == "Both",
                       c(lapply(.SD, function(x) mean(x, na.rm = TRUE))),
                       by = .(test_ID),
                       .SDcols = rsi_metrics]
  
  if (nrow(rsi_left) > 0) {
    old_names <- rsi_metrics
    new_names <- paste0(rsi_metrics, "_left")
    data.table::setnames(rsi_left, old_names, new_names)
  }
  
  if (nrow(rsi_right) > 0) {
    old_names <- rsi_metrics
    new_names <- paste0(rsi_metrics, "_right")
    data.table::setnames(rsi_right, old_names, new_names)
  }
  
  rsi_wide <- rsi_summary[, .(test_ID, vald_id, date, original_test_type, test_type, trial_limb)]
  
  if (nrow(rsi_left) > 0) {
    rsi_wide <- merge(rsi_wide, rsi_left, by = "test_ID", all.x = TRUE)
  }
  
  if (nrow(rsi_right) > 0) {
    rsi_wide <- merge(rsi_wide, rsi_right, by = "test_ID", all.x = TRUE)
  }
  
  if (nrow(rsi_both) > 0) {
    for (metric in rsi_metrics) {
      left_col <- paste0(metric, "_left")
      right_col <- paste0(metric, "_right")
      
      if (!left_col %in% names(rsi_wide)) rsi_wide[, (left_col) := NA_real_]
      if (!right_col %in% names(rsi_wide)) rsi_wide[, (right_col) := NA_real_]
      
      both_keys <- rsi_both[, .(test_ID)]
      rsi_wide[both_keys, on = .(test_ID), (left_col) := rsi_both[[metric]]]
      rsi_wide[both_keys, on = .(test_ID), (right_col) := rsi_both[[metric]]]
    }
  }
  
  rsi_wide[, original_test_type := NULL]
  
  for (metric in rsi_metrics) {
    left_col <- paste0(metric, "_left")
    right_col <- paste0(metric, "_right")
    if (!left_col %in% names(rsi_wide)) rsi_wide[, (left_col) := NA_real_]
    if (!right_col %in% names(rsi_wide)) rsi_wide[, (right_col) := NA_real_]
  }
  
  for (metric in rsi_metrics) {
    left_col <- paste0(metric, "_left")
    right_col <- paste0(metric, "_right")
    bilateral_col <- paste0(metric, "_bilateral")
    
    rsi_wide[, (bilateral_col) := data.table::fcase(
      !is.na(get(left_col)) & !is.na(get(right_col)), (get(left_col) + get(right_col)) / 2,
      !is.na(get(left_col)), get(left_col),
      !is.na(get(right_col)), get(right_col),
      default = NA_real_
    )]
  }
  
  if ("peak_vertical_force_left" %in% names(rsi_wide) && 
      "peak_vertical_force_right" %in% names(rsi_wide)) {
    rsi_wide[, asym_peak_vertical_force := data.table::fifelse(
      trial_limb != "Both" | is.na(peak_vertical_force_left) | is.na(peak_vertical_force_right),
      NA_real_,
      (peak_vertical_force_right - peak_vertical_force_left) / 
        ((peak_vertical_force_right + peak_vertical_force_left) / 2) * 100
    )]
  } else {
    rsi_wide[, asym_peak_vertical_force := NA_real_]
  }
  
  n_bilateral <- sum(rsi_wide$trial_limb == "Both", na.rm = TRUE)
  n_left_only <- sum(rsi_wide$trial_limb == "Left", na.rm = TRUE)
  n_right_only <- sum(rsi_wide$trial_limb == "Right", na.rm = TRUE)
  log_info("RSI pivot complete: {n_bilateral} bilateral, {n_left_only} left-only, {n_right_only} right-only")
  
  for (col in RSI_EXPORT_COLUMNS) {
    if (!col %in% names(rsi_wide)) {
      rsi_wide[, (col) := NA]
    }
  }
  
  rsi_export <- rsi_wide[, ..RSI_EXPORT_COLUMNS]
  
  log_info("RSI export: {nrow(rsi_export)} rows, {ncol(rsi_export)} columns")
  
  return(rsi_export)
}

# ============================================================================
# Change Detection (Gate Check)
# ============================================================================

determine_run_status <- function() {
  
  BACKSTOP_DATE <- as.Date(CONFIG$backstop_date)
  
  log_info("=== CHANGE DETECTION GATE CHECK ===")
  
  # -------------------------------------------------------------------------
  # Step 1: Read Current BigQuery State
  # -------------------------------------------------------------------------
  
  log_info("Reading current BigQuery state...")
  
  current_dates <- tryCatch({
    read_bq_table("dates")
  }, error = function(e) {
    log_warn("Could not read dates table: {e$message}")
    data.table::data.table()
  })
  
  tests_tbl <- tryCatch({
    read_bq_table("tests")
  }, error = function(e) {
    log_warn("Could not read tests table: {e$message}")
    data.table::data.table()
  })
  
  nordbord_tbl <- tryCatch({
    read_bq_table("vald_nord_all")
  }, error = function(e) {
    log_warn("Could not read vald_nord_all: {e$message}")
    data.table::data.table()
  })
  
  # Extract comparison metrics
  latest_date_current <- if (nrow(current_dates) > 0) max(current_dates$date, na.rm = TRUE) else BACKSTOP_DATE
  count_tests_current <- nrow(tests_tbl)
  latest_nord_date_current <- if (nrow(nordbord_tbl) > 0) max(nordbord_tbl$date, na.rm = TRUE) else BACKSTOP_DATE
  count_nord_tests_current <- nrow(nordbord_tbl)
  
  log_info("BQ State - FD: {count_tests_current} tests, max date {latest_date_current}")
  log_info("BQ State - Nord: {count_nord_tests_current} tests, max date {latest_nord_date_current}")
  
  # -------------------------------------------------------------------------
  # Step 2: Probe Live APIs
  # -------------------------------------------------------------------------
  
  log_info("Probing VALD APIs...")
  
  # ForceDecks probe
  fd_probe <- tryCatch({
    probe_result <- valdr::get_forcedecks_tests_only()
    data.table::as.data.table(probe_result)
  }, error = function(e) {
    log_error("ForceDecks probe failed: {e$message}")
    data.table::data.table()
  })
  
  # NordBord probe
  nord_probe <- tryCatch({
    probe_result <- valdr::get_nordbord_data()
    if (!is.null(probe_result$tests)) {
      data.table::as.data.table(probe_result$tests)
    } else {
      data.table::data.table()
    }
  }, error = function(e) {
    log_error("NordBord probe failed: {e$message}")
    data.table::data.table()
  })
  
  # Extract API metrics
  if (nrow(fd_probe) > 0) {
    # Check if required columns exist
    if (!"recordedDateUtc" %in% names(fd_probe)) {
      log_error("Missing 'recordedDateUtc' column in ForceDecks probe data. Available columns: {paste(names(fd_probe), collapse = ', ')}")
      api_latest_date <- BACKSTOP_DATE
      api_test_count <- 0
    } else if (!"testId" %in% names(fd_probe)) {
      log_error("Missing 'testId' column in ForceDecks probe data. Available columns: {paste(names(fd_probe), collapse = ', ')}")
      api_latest_date <- BACKSTOP_DATE
      api_test_count <- 0
    } else {
      fd_probe[, date := as.Date(recordedDateUtc)]
      fd_probe[, test_ID := testId]
      api_latest_date <- max(fd_probe$date, na.rm = TRUE)
      api_test_count <- data.table::uniqueN(fd_probe$test_ID)
    }
  } else {
    api_latest_date <- BACKSTOP_DATE
    api_test_count <- 0
  }
  
  if (nrow(nord_probe) > 0) {
    # Check if required columns exist
    if (!"recordedDateUtc" %in% names(nord_probe)) {
      log_error("Missing 'recordedDateUtc' column in NordBord probe data. Available columns: {paste(names(nord_probe), collapse = ', ')}")
      nord_api_latest_date <- BACKSTOP_DATE
      nord_api_test_count <- 0
    } else if (!"testId" %in% names(nord_probe)) {
      log_error("Missing 'testId' column in NordBord probe data. Available columns: {paste(names(nord_probe), collapse = ', ')}")
      nord_api_latest_date <- BACKSTOP_DATE
      nord_api_test_count <- 0
    } else {
      nord_probe[, date := as.Date(recordedDateUtc)]
      nord_probe[, test_ID := testId]
      nord_api_latest_date <- max(nord_probe$date, na.rm = TRUE)
      nord_api_test_count <- data.table::uniqueN(nord_probe$test_ID)
    }
  } else {
    nord_api_latest_date <- BACKSTOP_DATE
    nord_api_test_count <- 0
  }
  
  log_info("API State - FD: {api_test_count} tests, max date {api_latest_date}")
  log_info("API State - Nord: {nord_api_test_count} tests, max date {nord_api_latest_date}")
  
  # -------------------------------------------------------------------------
  # Step 3: Compare and Decide
  # -------------------------------------------------------------------------
  
  fd_date_mismatch <- !identical(as.Date(api_latest_date), as.Date(latest_date_current))
  fd_count_mismatch <- api_test_count != count_tests_current
  nord_date_mismatch <- !identical(as.Date(nord_api_latest_date), as.Date(latest_nord_date_current))
  nord_count_mismatch <- nord_api_test_count != count_nord_tests_current
  
  fd_changed <- fd_date_mismatch || fd_count_mismatch
  nord_changed <- nord_date_mismatch || nord_count_mismatch
  any_changes <- fd_changed || nord_changed
  
  log_check_summary("ForceDecks", count_tests_current, latest_date_current, 
                    api_test_count, api_latest_date, 
                    if(fd_changed) "CHANGED" else "NO_CHANGE")
  log_check_summary("NordBord", count_nord_tests_current, latest_nord_date_current,
                    nord_api_test_count, nord_api_latest_date,
                    if(nord_changed) "CHANGED" else "NO_CHANGE")
  
  # -------------------------------------------------------------------------
  # Step 4: Determine Run Type
  # -------------------------------------------------------------------------
  
  if (!any_changes) {
    return(list(
      run_type = "STANDDOWN",
      fd_changed = FALSE,
      nord_changed = FALSE,
      any_changes = FALSE
    ))
  }
  
  # If tables were empty, this is a full run
  run_type <- if (count_tests_current == 0 && count_nord_tests_current == 0) "FULL_RUN" else "UPDATE_RUN"
  
  return(list(
    run_type = run_type,
    fd_changed = fd_changed,
    nord_changed = nord_changed,
    any_changes = TRUE,
    max_fd_date = latest_date_current,
    max_nord_date = latest_nord_date_current
  ))
}

# ============================================================================
# Main Processing Section
# ============================================================================

script_start_time <- Sys.time()
log_and_store("=== VALD DATA PROCESSING SCRIPT STARTED ===", "START")
log_and_store("Script version: 2.4.3-cloud")
log_and_store("Mode: {if(LOCAL_MODE) 'LOCAL' else 'CLOUD'}")
log_and_store("Start date config: {CONFIG$start_date}")
log_and_store("Parallel config: {PARALLEL_CONFIG$n_workers} workers, {PARALLEL_CONFIG$stagger_sec*1000}ms stagger")
log_info("Production schema: {length(PRODUCTION_CMJ_COLUMNS)} columns")

# ============================================================================
# Gate Check: Determine Run Status
# ============================================================================

if (LOCAL_MODE) {
  # Local: Always run everything
  fd_changed <- TRUE
  nord_changed <- TRUE
  LOG_RUN_TYPE <- "FULL_RUN"
  max_fd_date <- NULL
  max_nord_date <- NULL
  log_and_store("Run type: FULL_RUN (local default)")
  
} else {
  # Cloud: Smart change detection
  run_status <- determine_run_status()
  
  fd_changed <- run_status$fd_changed
  nord_changed <- run_status$nord_changed
  LOG_RUN_TYPE <- run_status$run_type
  max_fd_date <- run_status$max_fd_date
  max_nord_date <- run_status$max_nord_date
  
  if (!run_status$any_changes) {
    log_and_store("=== STANDDOWN: No changes detected ===")
    log_and_store("ForceDecks and NordBord data is current - no processing needed")
    upload_logs_to_bigquery()
    quit(status = 0)  # Clean exit - nothing to do
  }
  
  log_and_store("Run type: {LOG_RUN_TYPE}")
  log_and_store("FD changed: {fd_changed} | Nord changed: {nord_changed}")
  log_and_store("Max dates - FD: {max_fd_date} | Nord: {max_nord_date}")
}

# ============================================================================
# Load Roster
# ============================================================================
log_and_store("Loading roster...")

Vald_roster <- tryCatch({
  roster <- data.table::data.table()
  
  if (file.exists(ROSTER_PATHS$roster_csv)) {
    roster <- data.table::fread(ROSTER_PATHS$roster_csv)
    log_info("Loaded roster from CSV: {nrow(roster)} athletes ({ROSTER_PATHS$roster_csv})")
  } else if (file.exists(ROSTER_PATHS$roster_xlsx)) {
    roster <- data.table::as.data.table(readxl::read_excel(ROSTER_PATHS$roster_xlsx))
    log_info("Loaded roster from Excel: {nrow(roster)} athletes ({ROSTER_PATHS$roster_xlsx})")
  } else {
    log_warn("No roster file found at expected paths:")
    log_warn("  CSV: {ROSTER_PATHS$roster_csv}")
    log_warn("  XLSX: {ROSTER_PATHS$roster_xlsx}")
  }
  
  # Standardize column names
  if (nrow(roster) > 0) {
    if ("vald-id" %in% names(roster)) data.table::setnames(roster, "vald-id", "vald_id")
    if ("Category 1" %in% names(roster)) data.table::setnames(roster, "Category 1", "team")
    if ("Group 1" %in% names(roster)) data.table::setnames(roster, "Group 1", "position")
    if ("category_1" %in% names(roster)) data.table::setnames(roster, "category_1", "team")
    if ("group_1" %in% names(roster)) data.table::setnames(roster, "group_1", "position")
    update_status("roster_loaded", TRUE)
  }
  
  roster
  
}, error = function(e) {
  log_error("Failed to load roster: {e$message}")
  record_error("Roster", e$message)
  data.table::data.table()
})

Vald_roster_backfill <- Vald_roster
log_and_store("Roster loaded: {nrow(Vald_roster)} athletes")

# ============================================================================
# Reset Start Date AS Needed - FD
# ============================================================================                    
# After run type decision, before ForceDecks Branch:

if (LOG_RUN_TYPE == "FULL_RUN") {
  fetch_start_date <- CONFIG$start_date  # "2024-01-01"
  log_and_store("Using FULL_RUN start date: {fetch_start_date}")
  
} else if (LOG_RUN_TYPE %in% c("UPDATE_RUN", "PARTIAL_RUN")) {
  # UPDATE_RUN and PARTIAL_RUN are synonymous - both mean incremental update
  # BUG FIX: Was checking only for "PARTIAL_RUN" which was never set
  if (!is.null(max_fd_date) && !is.na(max_fd_date)) {
    fetch_start_date <- as.character(as.Date(max_fd_date) - CONFIG$overlap_days)
    log_and_store("Using {LOG_RUN_TYPE} start date: {fetch_start_date} (max_fd_date: {max_fd_date}, overlap: {CONFIG$overlap_days} days)")
  } else {
    # Fallback to full run if max_fd_date not available
    fetch_start_date <- CONFIG$start_date
    log_warn("max_fd_date not available, falling back to FULL_RUN start date: {fetch_start_date}")
  }
  
} else {
  # STANDDOWN or unknown - use default
  fetch_start_date <- CONFIG$start_date
  log_and_store("Using default start date for {LOG_RUN_TYPE}: {fetch_start_date}")
}

# Reset the consumed cursor
valdr::set_start_date(paste0(fetch_start_date, "T00:00:00Z"))
log_and_store("Start date reset for {LOG_RUN_TYPE}: {fetch_start_date}")
                    
# ============================================================================
# ForceDecks Processing Branch
# ============================================================================
if (fd_changed) {
  tryCatch({
    log_and_store("=== FORCEDECKS BRANCH START ===")
    log_and_store("API Start date: {CONFIG$start_date}")
    
    fd_data <- adaptive_fetch_forcedecks(timeout_seconds = CONFIG$timeout_fd_full)
        tests_count <- if (!is.null(fd_data$tests)) nrow(fd_data$tests) else 0
    trials_count <- if (!is.null(fd_data$trials)) nrow(fd_data$trials) else 0
    
    if (tests_count == 0 || trials_count == 0) {
      log_warn("No ForceDecks data returned from API (tests: {tests_count}, trials: {trials_count})")
    } else {
      update_status("fd_fetched", TRUE)
      
      profiles    <- fd_data$profiles
      definitions <- fd_data$result_definitions
      tests_raw   <- fd_data$tests
      trials_raw  <- fd_data$trials
      
      log_and_store("Fetched: {nrow(profiles)} profiles, {nrow(tests_raw)} tests, {nrow(trials_raw)} trial records")
      
      # ========================================================================
      # Build Roster from API Profiles
      # ========================================================================
      if (nrow(profiles) > 0 && "profileId" %in% names(profiles)) {
        roster_from_api <- profiles[, .(
          vald_id = profileId,
          full_name = paste(trimws(givenName), trimws(familyName)),
          first_name = givenName,
          last_name = familyName
        )]
        
        if (nrow(Vald_roster) > 0 && "vald_id" %in% names(Vald_roster)) {
          roster_cols <- intersect(names(Vald_roster), c("vald_id", "team", "position"))
          if (length(roster_cols) > 1) {
            roster_from_api <- merge(roster_from_api, Vald_roster[, ..roster_cols], 
                                      by = "vald_id", all.x = TRUE)
          }
        }
        log_and_store("Built merged roster: {nrow(roster_from_api)} athletes")
      } else {
        log_warn("No profiles with profileId, using local roster")
        roster_from_api <- Vald_roster
      }
      
      # ========================================================================
      # Process Tests Table
      # ========================================================================
      log_and_store("Processing tests table...")
      
      tests_raw[, `:=`(
        vald_id = profileId,
        test_type = testType,
        test_ID = testId
      )]
      
      tests_raw[, recordedDateUtc_parsed := lubridate::ymd_hms(recordedDateUtc, tz = "UTC", quiet = TRUE)]
      tests_raw[, recordedDateUtc_local := lubridate::with_tz(recordedDateUtc_parsed, CONFIG$timezone)]
      tests_raw[, `:=`(
        date = as.Date(recordedDateUtc_local),
        time = hms::as_hms(recordedDateUtc_local)
      )]
      
      # ========================================================================
      # Pivot Trials from Long to Wide Format
      # ========================================================================
      log_and_store("Pivoting trials: {nrow(trials_raw)} records...")
      
      if ("resultLimb" %in% names(trials_raw)) {
        trials_wide <- data.table::dcast(
          trials_raw,
          testId + trialId + athleteId + recordedUTC + recordedTimezone + trialLimb ~ resultLimb + definition_result,
          sep = "_",
          value.var = "value",
          fun.aggregate = function(x) if(length(x) > 0) x[1] else NA_real_
        )
        
        if ("Asym_LANDING_IMPULSE" %in% names(trials_wide)) {
          data.table::setnames(trials_wide, "Asym_LANDING_IMPULSE", "landing_impulse_asymmetry")
        }
        if ("Asym_PEAK_TAKEOFF_FORCE" %in% names(trials_wide)) {
          data.table::setnames(trials_wide, "Asym_PEAK_TAKEOFF_FORCE", "peak_takeoff_force_asymmetry")
        }
        if ("Asym_ECCENTRIC_BRAKING_IMPULSE" %in% names(trials_wide)) {
          data.table::setnames(trials_wide, "Asym_ECCENTRIC_BRAKING_IMPULSE", "eccentric_braking_impulse_asymmetry")
        }
        
        asym_cols_found <- sum(c("landing_impulse_asymmetry", "peak_takeoff_force_asymmetry", 
                                  "eccentric_braking_impulse_asymmetry") %in% names(trials_wide))
        log_info("Asymmetry data captured: {asym_cols_found} columns")
        
      } else {
        log_warn("resultLimb column not found, using standard pivot")
        trials_wide <- data.table::dcast(
          trials_raw,
          testId + trialId + athleteId + recordedUTC + recordedTimezone + trialLimb ~ definition_result,
          value.var = "value",
          fun.aggregate = function(x) if(length(x) > 0) x[1] else NA_real_
        )
      }
      
      log_and_store("Pivoted to {nrow(trials_wide)} trials x {ncol(trials_wide)} metrics")
      
      # ========================================================================
      # Clean Column Names
      # ========================================================================
      id_cols_pivot <- c("testId", "trialId", "athleteId", "recordedUTC", "recordedTimezone", "trialLimb")
      value_cols_pivot <- setdiff(names(trials_wide), id_cols_pivot)
      for (col in value_cols_pivot) {
        if (!is.numeric(trials_wide[[col]])) {
          trials_wide[, (col) := as.numeric(get(col))]
        }
      }
      
      data.table::setnames(trials_wide, "testId", "test_ID", skip_absent = TRUE)
      data.table::setnames(trials_wide, "trialId", "trial_id", skip_absent = TRUE)
      data.table::setnames(trials_wide, "athleteId", "vald_id", skip_absent = TRUE)
      data.table::setnames(trials_wide, "trialLimb", "trial_limb", skip_absent = TRUE)
      
      id_cols_to_preserve <- c("test_ID", "trial_id", "vald_id", "trial_limb", "recordedUTC", "recordedTimezone")
      metric_cols <- setdiff(names(trials_wide), id_cols_to_preserve)
      
      for (col in metric_cols) {
        new_name <- tolower(gsub("([a-z])([A-Z])", "\\1_\\2", col))
        new_name <- gsub("[^a-z0-9_]", "_", new_name)
        new_name <- gsub("_+", "_", new_name)
        new_name <- gsub("^_|_$", "", new_name)
        if (new_name != col && !new_name %in% names(trials_wide)) {
          data.table::setnames(trials_wide, col, new_name)
        }
      }
      
      # Handle Trial_ prefix columns
      trial_prefix_cols <- grep("^trial_", names(trials_wide), value = TRUE)
      for (col in trial_prefix_cols) {
        if (!col %in% c("trial_id", "trial_limb", "trial_date", "trial_time")) {
          new_name <- gsub("^trial_", "", col)
          if (!new_name %in% names(trials_wide)) {
            data.table::setnames(trials_wide, col, new_name)
          }
        }
      }
      
      # ========================================================================
      # Body Weight Imputation
      # ========================================================================
      log_info("Checking for missing body weight...")
      
      bw_col <- NULL
      if ("body_weight_lbs" %in% names(trials_wide)) {
        bw_col <- "body_weight_lbs"
      } else if ("body_weight" %in% names(trials_wide)) {
        data.table::setnames(trials_wide, "body_weight", "body_weight_lbs")
        bw_col <- "body_weight_lbs"
      }
      
      has_pcf <- "peak_concentric_force" %in% names(trials_wide)
      has_rpcf <- "relative_peak_concentric_force" %in% names(trials_wide)
      
      if (!is.null(bw_col) && has_pcf && has_rpcf) {
        n_total <- nrow(trials_wide)
        n_missing_bw <- sum(is.na(trials_wide$body_weight_lbs))
        
        if (n_missing_bw > 0) {
          log_info("Missing body weight: {n_missing_bw} tests ({round(n_missing_bw/n_total*100, 1)}%)")
          
          trials_wide[, bw_imputation_flag := FALSE]
          
          can_impute <- is.na(trials_wide$body_weight_lbs) & 
                        !is.na(trials_wide$peak_concentric_force) & 
                        !is.na(trials_wide$relative_peak_concentric_force) &
                        trials_wide$relative_peak_concentric_force != 0
          
          n_can_impute <- sum(can_impute)
          
          if (n_can_impute > 0) {
            trials_wide[can_impute, `:=`(
              body_weight_lbs = peak_concentric_force / relative_peak_concentric_force,
              bw_imputation_flag = TRUE
            )]
            
            log_info("Body weight imputed: {n_can_impute}/{n_missing_bw} tests")
          }
        } else {
          log_info("No missing body weight - imputation not needed")
          trials_wide[, bw_imputation_flag := FALSE]
        }
      } else {
        log_info("Body weight imputation skipped: required columns not found")
        if (!"bw_imputation_flag" %in% names(trials_wide)) {
          trials_wide[, bw_imputation_flag := FALSE]
        }
      }
      
      if ("recordedutc" %in% names(trials_wide)) {
        trials_wide[, recorded_parsed := lubridate::ymd_hms(recordedutc, tz = "UTC", quiet = TRUE)]
        trials_wide[, recorded_local := lubridate::with_tz(recorded_parsed, CONFIG$timezone)]
        trials_wide[, `:=`(
          trial_date = as.Date(recorded_local),
          trial_time = hms::as_hms(recorded_local)
        )]
        trials_wide[, c("recorded_parsed", "recorded_local", "recordedutc", "recordedtimezone") := NULL]
      }
      
      # ========================================================================
      # Average Trials by Test
      # ========================================================================
      log_and_store("Averaging trials by test...")
      
      if ("testid" %in% names(trials_wide)) data.table::setnames(trials_wide, "testid", "test_ID")
      if ("athleteid" %in% names(trials_wide)) data.table::setnames(trials_wide, "athleteid", "vald_id")
      
      id_cols_in_data <- intersect(c("test_ID", "trial_id", "vald_id", "trial_limb", 
                                      "recordedUTC", "recordedTimezone", "trial_date", "trial_time",
                                      "bw_imputation_flag"), 
                                    names(trials_wide))
      value_cols <- setdiff(names(trials_wide), id_cols_in_data)
      numeric_cols <- value_cols[sapply(value_cols, function(col) is.numeric(trials_wide[[col]]))]
      
      log_and_store("Found {length(numeric_cols)} numeric columns to average")
      
      if (length(numeric_cols) > 0) {
        trials_avg <- trials_wide[, c(
          lapply(.SD[, ..numeric_cols], function(x) mean(x, na.rm = TRUE)),
          list(bw_imputation_flag = any(bw_imputation_flag, na.rm = TRUE))
        ), by = .(test_ID, vald_id)]
        
        if ("trial_limb" %in% names(trials_wide)) {
          limb_info <- trials_wide[, .(trial_limb = first(trial_limb)), by = .(test_ID, vald_id)]
          trials_avg <- merge(trials_avg, limb_info, by = c("test_ID", "vald_id"), all.x = TRUE)
        }
        
        log_and_store("Averaged to {nrow(trials_avg)} unique tests with {ncol(trials_avg)} columns")
      } else {
        log_warn("No numeric columns found for averaging!")
        trials_avg <- unique(trials_wide[, .(test_ID, vald_id, trial_limb, bw_imputation_flag)], by = c("test_ID", "vald_id"))
      }
      
      # ========================================================================
      # Merge Tests + Trials + Roster
      # ========================================================================
      log_and_store("Merging data sources...")
      
      test_meta <- tests_raw[, .(test_ID, test_type, date, time)]
      test_meta <- unique(test_meta, by = "test_ID")
      
      fd_raw <- merge(trials_avg, test_meta, by = "test_ID", all.x = TRUE)
      
      if (nrow(roster_from_api) > 0 && "vald_id" %in% names(roster_from_api)) {
        roster_join <- roster_from_api[, .(vald_id, full_name, team, position)]
        roster_join <- unique(roster_join, by = "vald_id")
        fd_raw <- merge(fd_raw, roster_join, by = "vald_id", all.x = TRUE)
      }
      
      log_and_store("Merged: {nrow(fd_raw)} records, {ncol(fd_raw)} columns")
      
      fd_raw <- standardize_data_types(fd_raw)
      
      # ========================================================================
      # RSI Scaling Correction (V2.4.3)
      # ========================================================================
      if ("rsi_modified_imp_mom" %in% names(fd_raw)) {
        fd_raw[, rsi_modified_imp_mom := rsi_modified_imp_mom / 100]
        log_info("RSI scaled to decimal form (divided by 100)")
      }
      
      # ========================================================================
      # Process CMJ/SJ Tests
      # ========================================================================
      log_and_store("Processing CMJ family tests...")
      
      cmj_types <- c("CMJ", "LCMJ", "SJ", "ABCMJ")
      cmj_all <- fd_raw[test_type %in% cmj_types]
      
      log_and_store("CMJ data has {nrow(cmj_all)} rows, {ncol(cmj_all)} columns")
      
      if (nrow(cmj_all) > 0 && "jump_height_inches_imp_mom" %in% names(cmj_all)) {
        
        sample_vals <- cmj_all[!is.na(jump_height_inches_imp_mom), head(jump_height_inches_imp_mom, 100)]
        if (length(sample_vals) > 0) {
          median_val <- median(sample_vals, na.rm = TRUE)
          if (median_val > 100) {
            cmj_all[, jump_height_inches_imp_mom := jump_height_inches_imp_mom / 25.4]
            log_info("Converted jump height from mm to inches")
          } else if (median_val > 35 && median_val < 100) {
            cmj_all[, jump_height_inches_imp_mom := jump_height_inches_imp_mom / 2.54]
            log_info("Converted jump height from cm to inches")
          } else if (median_val < 1) {
            cmj_all[, jump_height_inches_imp_mom := jump_height_inches_imp_mom * 39.37]
            log_info("Converted jump height from m to inches")
          }
        }
        
        cmj_all <- cmj_all[!is.na(jump_height_inches_imp_mom)]
        cmj_all <- cmj_all[jump_height_inches_imp_mom >= CONFIG$jump_height_min_inches & 
                           jump_height_inches_imp_mom <= CONFIG$jump_height_max_inches]
        
        log_and_store("CMJ after height filter: {nrow(cmj_all)} records")
        
        if (nrow(cmj_all) > 0) {
          cmj_all[, event_datetime := as.POSIXct(paste(date, time), tz = CONFIG$timezone)]
          data.table::setorder(cmj_all, vald_id, event_datetime)
          cmj_all[, session_id := create_session_ids(vald_id, event_datetime), by = vald_id]
          
          log_and_store("Applying QC validation layers...")
          cmj_all <- apply_layer1(cmj_all)
          cmj_all <- apply_layer2(cmj_all)
          cmj_all <- apply_layer3(cmj_all)
          cmj_all <- apply_layer4(cmj_all)
          cmj_all <- apply_final_and_clean(cmj_all)
          
          validation_summary <- cmj_all[, .N, by = final_classification]
          log_and_store("Validation: {paste(paste0(validation_summary$final_classification, '=', validation_summary$N), collapse = ', ')}")
          
          clean_classifications <- c("LIKELY_VALID", "VALID_EXTREME")
          cmj_clean <- cmj_all[final_classification %in% clean_classifications]
          log_and_store("Clean CMJ records: {nrow(cmj_clean)} ({round(100*nrow(cmj_clean)/nrow(cmj_all), 1)}%)")
          
          # Apply Calibration Error Detection
          cmj_clean <- apply_calibration_error_detection(cmj_clean)
          
          n_critical <- sum(cmj_clean$qc_flag == "CALIBRATION_ERROR_CRITICAL", na.rm = TRUE)
          if (n_critical > 0) {
            cmj_clean <- cmj_clean[qc_flag != "CALIBRATION_ERROR_CRITICAL"]
            log_and_store("Removed {n_critical} tests with critical calibration errors")
          }
          
          if ("qc_flag" %in% names(cmj_clean)) cmj_clean[, qc_flag := NULL]
          if ("equipment_error_flag" %in% names(cmj_clean)) cmj_clean[, equipment_error_flag := NULL]
          
          # Calculate Readiness Metrics
          log_and_store("Calculating readiness metrics...")
          
          has_jh <- "jump_height_inches_imp_mom" %in% names(cmj_clean)
          has_rsi <- "rsi_modified_imp_mom" %in% names(cmj_clean)
          has_epf <- "relative_peak_eccentric_force" %in% names(cmj_clean)
          has_power <- "bodymass_relative_takeoff_power" %in% names(cmj_clean)
          
          if (has_jh && nrow(cmj_clean) > 0) {
            data.table::setorder(cmj_clean, full_name, test_type, date)
            
            # Jump Height: 30-day rolling mean (time-indexed)
            cmj_clean[, jh_cmj_mean_30d := slider::slide_index_dbl(
              .x = jump_height_inches_imp_mom,
              .i = date,
              .f = ~ mean(.x, na.rm = TRUE),
              .before = lubridate::days(30),
              .complete = FALSE
            ), by = .(full_name, test_type)]
            
            if (has_rsi) {
              cmj_clean[, rsi_cmj_mean_30d := slider::slide_index_dbl(
                .x = rsi_modified_imp_mom,
                .i = date,
                .f = ~ mean(.x, na.rm = TRUE),
                .before = lubridate::days(30),
                .complete = FALSE
              ), by = .(full_name, test_type)]
            }
            
            if (has_epf) {
              cmj_clean[, epf_cmj_mean_30d := slider::slide_index_dbl(
                .x = relative_peak_eccentric_force,
                .i = date,
                .f = ~ mean(.x, na.rm = TRUE),
                .before = lubridate::days(30),
                .complete = FALSE
              ), by = .(full_name, test_type)]
            }
            
            # Calculate Readiness Scores
            cmj_clean[, jump_height_readiness := data.table::fifelse(
              !is.na(jh_cmj_mean_30d) & jh_cmj_mean_30d != 0,
              (jump_height_inches_imp_mom - jh_cmj_mean_30d) / jh_cmj_mean_30d,
              NA_real_
            )]
            
            if (has_rsi) {
              cmj_clean[, rsi_readiness := data.table::fifelse(
                !is.na(rsi_cmj_mean_30d) & rsi_cmj_mean_30d != 0,
                (rsi_modified_imp_mom - rsi_cmj_mean_30d) / rsi_cmj_mean_30d,
                NA_real_
              )]
            }
            
            if (has_epf) {
              cmj_clean[, epf_readiness := data.table::fifelse(
                !is.na(epf_cmj_mean_30d) & epf_cmj_mean_30d != 0,
                (relative_peak_eccentric_force - epf_cmj_mean_30d) / epf_cmj_mean_30d,
                NA_real_
              )]
            }
            
            if (has_rsi && has_epf && has_power) {
              cmj_clean[, calc_perf := (
                data.table::frank(jump_height_inches_imp_mom, na.last = "keep") / .N +
                data.table::frank(rsi_modified_imp_mom, na.last = "keep") / .N +
                data.table::frank(relative_peak_eccentric_force, na.last = "keep") / .N +
                data.table::frank(bodymass_relative_takeoff_power, na.last = "keep") / .N
              )]
              cmj_clean[, performance_score := data.table::frank(calc_perf, na.last = "keep") / .N * 100]
              cmj_clean[, team_performance_score := data.table::frank(calc_perf, na.last = "keep") / .N * 100, by = team]
              cmj_clean[, calc_perf := NULL]
            }
            
            # Apply Athlete-Specific MDC Thresholds (V2.4.3)
            log_and_store("Calculating athlete-specific MDC thresholds...")
            
            cmj_clean <- calculate_athlete_mdc(
              dt = cmj_clean,
              metric_col = "jump_height_inches_imp_mom",
              baseline_col = "jh_mdc_baseline",
              mdc_col = "jh_mdc_threshold",
              status_col = "jh_mdc_status"
            )
            
            if (has_rsi) {
              cmj_clean <- calculate_athlete_mdc(
                dt = cmj_clean,
                metric_col = "rsi_modified_imp_mom",
                baseline_col = "rsi_mdc_baseline",
                mdc_col = "rsi_mdc_threshold",
                status_col = "rsi_mdc_status"
              )
            }
            
            if (has_epf) {
              cmj_clean <- calculate_athlete_mdc(
                dt = cmj_clean,
                metric_col = "relative_peak_eccentric_force",
                baseline_col = "epf_mdc_baseline",
                mdc_col = "epf_mdc_threshold",
                status_col = "epf_mdc_status"
              )
            }
            
            if ("mdc_tier" %in% names(cmj_clean)) {
              tier_summary <- cmj_clean[, .N, by = mdc_tier]
              log_and_store("MDC Tiers: {paste(paste0(tier_summary$mdc_tier, '=', tier_summary$N), collapse = ', ')}")
            }
            
            if ("jh_mdc_status" %in% names(cmj_clean)) {
              status_summary <- cmj_clean[!is.na(jh_mdc_status), .N, by = jh_mdc_status]
              log_and_store("JH MDC Status: {paste(paste0(status_summary$jh_mdc_status, '=', status_summary$N), collapse = ', ')}")
            }
            
            # Cleanup readiness temp columns
            temp_cols <- c("jh_cmj_mean_30d", "rsi_cmj_mean_30d", "epf_cmj_mean_30d")
            temp_cols <- intersect(temp_cols, names(cmj_clean))
            if (length(temp_cols) > 0) cmj_clean[, (temp_cols) := NULL]
          }
          
          # Clean up QC columns
          qc_cols <- c("event_datetime", "session_id", "physics_flag", "flag_bw_delta", 
                       "flag_session_contamination", "flag_multiple_extremes", 
                       "final_classification", "jump_height_clean_inches", 
                       "takeoff_v_ms_clean", "rsi_modified_imp_mom_clean", "mdc_alert")
          qc_cols <- intersect(qc_cols, names(cmj_clean))
          if (length(qc_cols) > 0) cmj_clean[, (qc_cols) := NULL]
          
          # Column Selection
          log_info("Selecting production columns from {ncol(cmj_clean)} available columns")
          
          available_prod_cols <- intersect(PRODUCTION_CMJ_COLUMNS, names(cmj_clean))
          missing_cols <- setdiff(PRODUCTION_CMJ_COLUMNS, names(cmj_clean))
          
          log_info("Found {length(available_prod_cols)}/{length(PRODUCTION_CMJ_COLUMNS)} production columns")
          
          if (length(missing_cols) > 0) {
            log_warn("Missing {length(missing_cols)} columns - adding as NA")
            for (col in missing_cols) {
              cmj_clean[, (col) := NA]
            }
          }
          
          cmj_export <- cmj_clean[, ..PRODUCTION_CMJ_COLUMNS]
          
          log_info("Export schema: {ncol(cmj_export)} columns, {nrow(cmj_export)} rows")
          
          bq_upsert(cmj_export, "vald_fd_jumps", key = "test_ID", mode = "MERGE",
                    partition_field = "date", cluster_fields = c("team", "test_type", "vald_id"))
          update_status("fd_cmj_processed", TRUE)
          log_and_store("Exported {nrow(cmj_export)} CMJ records with {ncol(cmj_export)} columns (MERGE mode)")
        }
      } else {
        log_warn("No valid CMJ data to process")
      }
      
      # ========================================================================
      # Phase 2: Secondary Tables with V2.4.2 Processing
      # ========================================================================
      log_info("Phase 2: Processing secondary tables with V2.4.2 schemas...")
      
      # Process Drop Jump Tests (V2.4.2)
      dj_all <- fd_raw[test_type %in% c("DJ")]
      if (nrow(dj_all) > 0) {
        tryCatch({
          dj_all <- standardize_data_types(dj_all)
          dj_export <- process_dj(dj_all)
          bq_upsert(dj_export, "vald_fd_dj", key = "test_ID", mode = "MERGE",
                    partition_field = "date", cluster_fields = c("vald_id"))
          update_status("fd_dj_processed", TRUE)
          log_and_store("Exported {nrow(dj_export)} DJ records with {ncol(dj_export)} columns")
        }, error = function(e) {
          record_error("DJ_Processing", e$message)
        })
      } else {
        log_and_store("No Drop Jump data found")
      }
      
      # Process RSI Tests (V2.4.2)
      rsi_all <- fd_raw[grepl("RSI|RSAIP|RSHIP|RSKIP", test_type, ignore.case = TRUE)]
      if (nrow(rsi_all) > 0) {
        tryCatch({
          rsi_all <- standardize_data_types(rsi_all)
          rsi_export <- process_rsi(rsi_all)
          bq_upsert(rsi_export, "vald_fd_rsi", key = "test_ID", mode = "MERGE",
                    partition_field = "date", cluster_fields = c("vald_id"))
          update_status("fd_rsi_processed", TRUE)
          log_and_store("Exported {nrow(rsi_export)} RSI records with {ncol(rsi_export)} columns")
        }, error = function(e) {
          record_error("RSI_Processing", e$message)
        })
      } else {
        log_and_store("No RSI data found")
      }
      
      # Process Rebound Tests
      rebound_all <- fd_raw[grepl("Rebound|CMRJ|Multi.*Hop", test_type, ignore.case = TRUE)]
      if (nrow(rebound_all) > 0) {
        tryCatch({
          rebound_all <- standardize_data_types(rebound_all)
          bq_upsert(rebound_all, "vald_fd_rebound", key = "test_ID", mode = "MERGE",
                    partition_field = "date", cluster_fields = c("vald_id"))
          update_status("fd_rebound_processed", TRUE)
          log_and_store("Exported {nrow(rebound_all)} Rebound records")
        }, error = function(e) {
          record_error("Rebound_Processing", e$message)
        })
      } else {
        log_and_store("No Rebound data found")
      }
      
      # Process Single-Leg Jump Tests (V2.4.2)
      slj_all <- fd_raw[grepl("Single.*Leg|SL.*Jump|SLJ|SLCMJ", test_type, ignore.case = TRUE)]
      if (nrow(slj_all) > 0) {
        tryCatch({
          slj_all <- standardize_data_types(slj_all)
          slj_export <- process_sl_jumps(slj_all)
          bq_upsert(slj_export, "vald_fd_sl_jumps", key = "test_ID", mode = "MERGE",
                    partition_field = "date", cluster_fields = c("vald_id"))
          update_status("fd_slj_processed", TRUE)
          log_and_store("Exported {nrow(slj_export)} SLJ records with {ncol(slj_export)} columns")
        }, error = function(e) {
          record_error("SLJ_Processing", e$message)
        })
      } else {
        log_and_store("No Single-Leg Jump data found")
      }
      
      # Process IMTP Tests (V2.4.2)
      imtp_all <- fd_raw[grepl("IMTP|Isometric|Mid.*Thigh", test_type, ignore.case = TRUE)]
      if (nrow(imtp_all) > 0) {
        tryCatch({
          imtp_all <- standardize_data_types(imtp_all)
          imtp_export <- process_imtp(imtp_all)
          bq_upsert(imtp_export, "vald_fd_imtp", key = "test_ID", mode = "MERGE",
                    partition_field = "date", cluster_fields = c("vald_id"))
          update_status("fd_imtp_processed", TRUE)
          log_and_store("Exported {nrow(imtp_export)} IMTP records with {ncol(imtp_export)} columns")
        }, error = function(e) {
          record_error("IMTP_Processing", e$message)
        })
      } else {
        log_and_store("No IMTP data found")
      }
      
      # ========================================================================
      # Update Reference Tables
      # ========================================================================
      if (nrow(fd_raw) > 0) {
        tryCatch({
          dates_df <- data.table::data.table(
            date = unique(fd_raw$date),
            source = "ForceDecks",
            updated = Sys.time()
          )
          dates_df <- dates_df[!is.na(date)]
          if (nrow(dates_df) > 0) {
            bq_upsert(dates_df, "dates", key = "date", mode = "MERGE")
            log_and_store("Updated dates: {nrow(dates_df)} unique dates")
          }
          
          tests_df <- unique(fd_raw[, .(test_ID, vald_id, date, test_type)], by = "test_ID")
          tests_df <- tests_df[!is.na(test_ID)]
          if (nrow(tests_df) > 0) {
            bq_upsert(tests_df, "tests", key = "test_ID", mode = "MERGE")
            update_status("refs_updated", TRUE)
            log_and_store("Updated tests: {nrow(tests_df)} tests")
          }
        }, error = function(e) {
          record_error("RefTables", e$message)
        })
      }
    }
    
    log_and_store("=== FORCEDECKS BRANCH COMPLETE ===")
    
  }, error = function(e) {
    log_error("ForceDecks branch failed: {e$message}")
    record_error("ForceDecks_Branch", e$message)
  })
} else {
  log_and_store("ForceDecks: No changes detected - skipping")
}

# ============================================================================
# Reset Start Date AS Needed - Nord
# ============================================================================                    
if (LOG_RUN_TYPE == "FULL_RUN") {
  fetch_start_date <- CONFIG$start_date  # "2024-01-01"
  log_and_store("NordBord: Using FULL_RUN start date: {fetch_start_date}")
  
} else if (LOG_RUN_TYPE %in% c("UPDATE_RUN", "PARTIAL_RUN")) {
  # UPDATE_RUN and PARTIAL_RUN are synonymous - both mean incremental update
  # BUG FIX: Was checking only for "PARTIAL_RUN" which was never set
  # BUG FIX: Was using max_fd_date instead of max_nord_date
  if (!is.null(max_nord_date) && !is.na(max_nord_date)) {
    fetch_start_date <- as.character(as.Date(max_nord_date) - CONFIG$overlap_days)
    log_and_store("NordBord: Using {LOG_RUN_TYPE} start date: {fetch_start_date} (max_nord_date: {max_nord_date}, overlap: {CONFIG$overlap_days} days)")
  } else {
    # Fallback to full run if max_nord_date not available
    fetch_start_date <- CONFIG$start_date
    log_warn("max_nord_date not available, falling back to FULL_RUN start date: {fetch_start_date}")
  }
  
} else {
  # STANDDOWN or unknown - use default  
  fetch_start_date <- CONFIG$start_date
  log_and_store("NordBord: Using default start date for {LOG_RUN_TYPE}: {fetch_start_date}")
}

# Reset the consumed cursor
valdr::set_start_date(paste0(fetch_start_date, "T00:00:00Z"))
log_and_store("Start date reset for {LOG_RUN_TYPE}: {fetch_start_date}")
                                  
# ============================================================================
# NordBord Processing Branch
# ============================================================================
if (nord_changed) {
  tryCatch({
    log_and_store("=== NORDBORD BRANCH START ===")
    
    # Reset start date before Nordbord fetch
    set_start_date(paste0(CONFIG$start_date, "T00:00:00Z"))
    log_info("Reset start date to {CONFIG$start_date} for Nordbord fetch")
    
    nord_data <- safe_fetch_nordbord(timeout_seconds = CONFIG$timeout_nordbord)
    
    tests_count <- if (!is.null(nord_data$tests)) nrow(nord_data$tests) else 0
    
    if (tests_count == 0) {
      log_warn("No Nordbord tests returned")
    } else {
      nord_raw <- nord_data$tests
      update_status("nord_fetched", TRUE)
      log_and_store("Fetched {nrow(nord_raw)} Nordbord tests")
      
      # Process with V2.4.2 unilateral detection
      nord_export <- process_nordbord(nord_raw, Vald_roster)
      
      bq_upsert(nord_export, "vald_nord_all", key = "test_ID", mode = "MERGE",
                partition_field = "date", cluster_fields = c("vald_id"))
      update_status("nord_processed", TRUE)
      log_and_store("Exported {nrow(nord_export)} Nordbord records with {ncol(nord_export)} columns")
    }
    
    log_and_store("=== NORDBORD BRANCH COMPLETE ===")
    
  }, error = function(e) {
    log_error("NordBord branch failed: {e$message}")
    record_error("NordBord_Branch", e$message)
  })
} else {
  log_and_store("NordBord: No changes detected - skipping")
}

# ============================================================================
# Finalization
# ============================================================================

execution_time <- round(difftime(Sys.time(), script_start_time, units = "mins"), 2)
log_and_store("Total execution time: {execution_time} minutes")

# Execution summary
log_and_store("=== EXECUTION SUMMARY ===")
status <- .GlobalEnv$execution_status
log_and_store("FD CMJ: {if(status$fd_cmj_processed) 'SUCCESS' else 'SKIPPED/FAILED'}")
log_and_store("FD DJ: {if(status$fd_dj_processed) 'SUCCESS' else 'SKIPPED/FAILED'}")
log_and_store("FD RSI: {if(status$fd_rsi_processed) 'SUCCESS' else 'SKIPPED/FAILED'}")
log_and_store("FD Rebound: {if(status$fd_rebound_processed) 'SUCCESS' else 'SKIPPED/FAILED'}")
log_and_store("FD SLJ: {if(status$fd_slj_processed) 'SUCCESS' else 'SKIPPED/FAILED'}")
log_and_store("FD IMTP: {if(status$fd_imtp_processed) 'SUCCESS' else 'SKIPPED/FAILED'}")
log_and_store("NordBord: {if(status$nord_processed) 'SUCCESS' else 'SKIPPED/FAILED'}")
errors <- .GlobalEnv$execution_errors
log_and_store("Errors: {length(errors)}")

log_and_store("=== VALD DATA PROCESSING SCRIPT ENDED ===", "END")

# Clean up
if (LOCAL_MODE) export_local_summary()
finalize_logging()

# Upload logs and schema mismatches (always attempt, even on partial failure)
tryCatch({
  upload_logs_to_bigquery()
}, error = function(e) {
  cat("Warning: Log upload failed:", e$message, "\n")
})

tryCatch({
  upload_schema_mismatches()
}, error = function(e) {
  cat("Warning: Schema mismatch upload failed:", e$message, "\n")
})

# Clean up parallel workers
future::plan(future::sequential)

# Exit with appropriate code
exit_code <- determine_exit_code()

if (exit_code == 0) {
  cat("Script completed successfully\n")
} else {
  cat("Script completed with errors\n")
}

quit(status = exit_code)
