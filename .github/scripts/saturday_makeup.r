#!/usr/bin/env Rscript

# Saturday Data Reconciliation Script
# Purpose: Identify and backfill missing test_IDs across all VALD data tables

# Load required libraries with error handling
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
  })
}, error = function(e) {
  cat("Error loading required packages:", e$message, "\n")
  cat("Please ensure all required packages are installed\n")
  quit(status = 1)
})

################################################################################
# BigQuery Configuration and Setup
################################################################################

project  <- Sys.getenv("GCP_PROJECT",  "sac-vald-hub")
dataset  <- Sys.getenv("BQ_DATASET",   "analytics")
location <- Sys.getenv("BQ_LOCATION",  "US")

cat("=== Saturday Data Reconciliation Started ===\n")
cat("GCP Project:", project, "\n")
cat("BQ Dataset:", dataset, "\n")
cat("BQ Location:", location, "\n")

# Authenticate to BigQuery using working method from main script
tryCatch({
  cat("=== Authenticating to BigQuery ===\n")
  
  # Get access token from gcloud
  access_token_result <- system("gcloud auth print-access-token", intern = TRUE)
  access_token <- access_token_result[1]
  
  if (nchar(access_token) > 0) {
    cat("Access token obtained from gcloud\n")
    
    # Create gargle token object
    library(gargle)
    token <- gargle::gargle2.0_token(
      scope = 'https://www.googleapis.com/auth/bigquery',
      client = gargle::gargle_client(),
      credentials = list(access_token = access_token)
    )
    
    # Set token for bigrquery
    bigrquery::bq_auth(token = token)
    cat("BigQuery authentication successful\n")
    
    # Test authentication
    test_con <- DBI::dbConnect(bigrquery::bigquery(), project = project)
    test_result <- DBI::dbGetQuery(test_con, "SELECT 1 as test")
    DBI::dbDisconnect(test_con)
    
    if (nrow(test_result) == 1 && test_result$test == 1) {
      cat("Authentication test passed\n")
    } else {
      stop("Authentication test failed")
    }
    
  } else {
    stop("Could not obtain access token from gcloud")
  }
  
}, error = function(e) {
  cat("BigQuery authentication failed:", e$message, "\n")
  quit(status = 1)
})

# Create BigQuery connection
con <- DBI::dbConnect(bigrquery::bigquery(), project = project)
ds <- bq_dataset(project, dataset)

################################################################################
# Logging Functions
################################################################################

# Initialize log storage
log_entries <- data.frame(
  timestamp = as.POSIXct(character(0)),
  level = character(0),
  message = character(0),
  run_id = character(0),
  repository = character(0),
  stringsAsFactors = FALSE
)

create_log_entry <- function(message, level = "INFO") {
  actual_timestamp <- Sys.time()
  display_timestamp <- format(actual_timestamp, "%Y-%m-%d %H:%M:%S", tz = "UTC")
  log_message <- paste0("[", display_timestamp, "] [", level, "] ", message)
  cat(log_message, "\n")
  
  new_entry <- data.frame(
    timestamp = actual_timestamp,
    level = level,
    message = message,
    run_id = Sys.getenv("GITHUB_RUN_ID", "manual-saturday"),
    repository = Sys.getenv("GITHUB_REPOSITORY", "unknown"),
    stringsAsFactors = FALSE
  )
  
  log_entries <<- rbind(log_entries, new_entry)
  return(log_message)
}

upload_logs_to_bigquery <- function() {
  if (nrow(log_entries) == 0) {
    cat("No logs to upload\n")
    return(TRUE)
  }
  
  tryCatch({
    cat("Uploading", nrow(log_entries), "log entries to BigQuery...\n")
    
    log_tbl <- bq_table(ds, "vald_processing_log")
    
    if (!bq_table_exists(log_tbl)) {
      bq_table_create(log_tbl, fields = bigrquery::as_bq_fields(log_entries))
      cat("Created vald_processing_log table\n")
    }
    
    bq_table_upload(log_tbl, log_entries, write_disposition = "WRITE_APPEND")
    cat("Successfully uploaded", nrow(log_entries), "log entries to BigQuery\n")
    return(TRUE)
    
  }, error = function(e) {
    cat("Failed to upload logs to BigQuery:", e$message, "\n")
    return(FALSE)
  })
}

script_start_time <- Sys.time()
create_log_entry("=== SATURDAY DATA RECONCILIATION SCRIPT STARTED ===", "START")

################################################################################
# VALD API Setup
################################################################################

tryCatch({
  client_id <- Sys.getenv("VALD_CLIENT_ID")
  client_secret <- Sys.getenv("VALD_CLIENT_SECRET")
  tenant_id <- Sys.getenv("VALD_TENANT_ID")
  region <- Sys.getenv("VALD_REGION", "use")

  if (client_id == "" || client_secret == "" || tenant_id == "") {
    stop("Missing required VALD API credentials")
  }

  set_credentials(client_id, client_secret, tenant_id, region)
  create_log_entry("VALD API credentials configured successfully")
}, error = function(e) {
  create_log_entry(paste("VALD API setup failed:", e$message), "ERROR")
  quit(status = 1)
})

################################################################################
# Helper Functions
################################################################################

# Get all BigQuery table names in the dataset
get_dataset_tables <- function() {
  tables_query <- sprintf("
    SELECT table_name, row_count, size_bytes
    FROM `%s.%s.INFORMATION_SCHEMA.TABLES` 
    WHERE table_type = 'BASE_TABLE'
    ORDER BY table_name
  ", project, dataset)
  
  result <- DBI::dbGetQuery(con, tables_query)
  create_log_entry(paste("Found", nrow(result), "tables in dataset"))
  return(result$table_name)
}

# Extract test_IDs from a specific table
extract_test_ids_from_table <- function(table_name) {
  # Check if table has test_ID column
  schema_query <- sprintf("
    SELECT column_name
    FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS` 
    WHERE table_name = '%s' AND column_name = 'test_ID'
  ", project, dataset, table_name)
  
  schema_result <- DBI::dbGetQuery(con, schema_query)
  
  if (nrow(schema_result) == 0) {
    create_log_entry(paste("Table", table_name, "does not have test_ID column"), "INFO")
    return(data.frame(test_ID = character(0), source_table = character(0)))
  }
  
  tryCatch({
    test_ids_query <- sprintf("
      SELECT DISTINCT test_ID, '%s' as source_table
      FROM `%s.%s.%s`
      WHERE test_ID IS NOT NULL AND test_ID != ''
    ", table_name, project, dataset, table_name)
    
    result <- DBI::dbGetQuery(con, test_ids_query)
    create_log_entry(paste("Extracted", nrow(result), "test_IDs from", table_name))
    return(result)
    
  }, error = function(e) {
    create_log_entry(paste("Error extracting test_IDs from", table_name, ":", e$message), "ERROR")
    return(data.frame(test_ID = character(0), source_table = character(0)))
  })
}

# Process missing ForceDecks data with full processing pipeline
process_missing_forcedecks_data <- function(missing_test_ids, existing_data) {
  if (length(missing_test_ids) == 0) {
    create_log_entry("No missing ForceDecks test_IDs to process")
    return(NULL)
  }
  
  create_log_entry(paste("Processing", length(missing_test_ids), "missing ForceDecks test_IDs"))
  
  # Get comprehensive ForceDecks data with rate limiting
  tryCatch({
    injest_forcedecks_data <- rate_limited_api_call(
      get_forcedecks_data, 
      description = "ForceDecks comprehensive data"
    )
    
    if (is.null(injest_forcedecks_data) || length(injest_forcedecks_data$tests) == 0) {
      create_log_entry("No ForceDecks data retrieved from API", "WARN")
      return(NULL)
    }
    
    # Filter to only missing test_IDs
    missing_tests <- injest_forcedecks_data$tests %>%
      filter(testId %in% missing_test_ids)
    
    if (nrow(missing_tests) == 0) {
      create_log_entry("No missing ForceDecks tests found in API data", "WARN")
      return(NULL)
    }
    
    create_log_entry(paste("Found", nrow(missing_tests), "missing ForceDecks tests in API data"))
    
    # Filter other components to match missing tests
    injest_forcedecks_data$tests <- missing_tests
    injest_forcedecks_data$trials <- injest_forcedecks_data$trials %>%
      filter(testId %in% missing_test_ids)
    
    # Process using the same pipeline as the main script
    processed_data <- process_vald_data_missing(injest_forcedecks_data, existing_data)
    
    return(processed_data)
    
  }, error = function(e) {
    create_log_entry(paste("Error retrieving missing ForceDecks data:", e$message), "ERROR")
    return(NULL)
  })
}

# Full VALD data processing pipeline (adapted from main script)
process_vald_data_missing <- function(injest_data, existing_data) {
  create_log_entry("Starting missing VALD data processing pipeline", "INFO")
  
  profiles <- setDT(injest_data$profiles)
  definitions <- setDT(injest_data$result_definitions)
  tests <- setDT(injest_data$tests)
  trials <- setDT(injest_data$trials)
  
  create_log_entry(paste("Processing data counts - Profiles:", nrow(profiles), "Tests:", nrow(tests), "Trials:", nrow(trials)))
  
  # Process profiles for roster
  roster <- profiles %>%
    select(profileId, givenName, familyName) %>% 
    mutate(
      full_name = paste(trimws(givenName), trimws(familyName), sep = " "),
      first_name = givenName,
      last_name = familyName,
      vald_id = profileId
    ) %>% 
    select(-givenName, -familyName, -profileId)
  
  # Process tests with test_ID preservation
  tests_processed <- tests %>% 
    mutate(
      vald_id = as.character(profileId),
      test_type = testType,
      test_ID = as.character(testId),
      recordedDateUtc_parsed = ymd_hms(recordedDateUtc, tz = "UTC", quiet = TRUE),
      recordedDateUtc_local = if_else(
        recordedDateTimezone %in% c("Pacific Standard Time", "Pacific Daylight Time", "Pacific Time"),
        with_tz(recordedDateUtc_parsed, "America/Los_Angeles"),
        recordedDateUtc_parsed
      ),
      date = as.Date(recordedDateUtc_local),
      time = hms::as_hms(recordedDateUtc_local)
    ) %>% 
    select(
      -testId, -tenantId, -profileId, -testType, -weight, -analysedDateUtc, 
      -analysedDateOffset, -analysedDateTimezone, -recordedDateUtc_parsed, 
      -recordedDateUtc_local, -recordedDateUtc, -recordedDateOffset, 
      -recordedDateTimezone, -recordingId
    )
  
  # Process definitions
  definitions_processed <- definitions %>% 
    select(
      resultId,
      r_name = resultIdString,
      metric = resultName,
      definition = resultDescription,
      unit_of_measure = resultUnit,
      unit_of_measure_symbol = resultUnitName,
      unit_of_measure_scale = resultUnitScaleFactor,
      decimal_places = numberOfDecimalPlaces,
      group = resultGroup,
      asymmetry = supportsAsymmetry,
      repeatable = isRepeatResult,
      good_trend_direction = trendDirection
    )
  
  # Process trials with test_ID preservation
  trials_wider <- trials %>% 
    pivot_wider(
      id_cols = c(testId, trialId, athleteId, recordedUTC, recordedTimezone, trialLimb),
      names_from = definition_result,
      values_from = value,
      values_fn = dplyr::first
    ) %>% 
    mutate(
      recordedUTC_parsed = ymd_hms(recordedUTC, tz = "UTC", quiet = TRUE),
      recordedUTC_local  = if_else(
        recordedTimezone %in% c("Pacific Standard Time", "Pacific Daylight Time", "Pacific Time"),
        with_tz(recordedUTC_parsed, "America/Los_Angeles"),
        recordedUTC_parsed
      ),
      date = as.Date(recordedUTC_local),
      time = hms::as_hms(recordedUTC_local)
    ) %>% 
    select(-recordedUTC_parsed, -recordedUTC_local) %>% 
    rename_with(tolower)
  
  # Convert numeric columns
  start_col <- which(names(trials_wider) == "start_of_movement")
  if (length(start_col) > 0) {
    trials_wider <- trials_wider %>% 
      mutate(across(all_of(start_col:ncol(.)), as.numeric))
  }
  
  # Create mergable trials with test_ID preservation
  mergable_trials <- trials_wider %>% 
    mutate(test_ID = as.character(testid)) %>%
    select(-testid) %>% 
    group_by(test_ID) %>% 
    summarise(
      athleteid = first(athleteid),
      triallimb = first(triallimb),
      date = first(date),
      time = first(time),
      across(where(is.numeric), mean, na.rm = TRUE),
      .groups = "drop"
    ) %>% 
    mutate(vald_id = as.character(athleteid)) %>%
    select(-athleteid)
  
  # Create mergable tests
  mergable_tests <- tests_processed %>% 
    select(test_ID, test_type) %>%
    mutate(test_ID = as.character(test_ID))
  
  # Get roster data (use existing or process new)
  if (nrow(existing_data$roster) > 0) {
    Vald_roster <- existing_data$roster
    create_log_entry(paste("Using existing roster:", nrow(Vald_roster), "athletes"))
  } else {
    # Process simple roster from new data
    Vald_roster <- data.frame(
      external_id = character(0), position = character(0), sport = character(0), 
      height = integer(0), sex = character(0), date_of_birth = as.Date(character(0)), 
      email = character(0), vald_id = character(0), family_name = character(0), 
      weight = numeric(0), given_name = character(0)
    )
    create_log_entry("Using empty roster for missing data processing", "INFO")
  }
  
  # Create mergable roster
  mergable_roster <- roster %>% 
    select(-first_name, -last_name) %>% 
    left_join(Vald_roster %>% select(vald_id, position, sport), by = "vald_id")
  
  # Master merge with test_ID preservation
  forcedecks_raw <- mergable_trials %>% 
    left_join(mergable_tests, by = "test_ID") %>% 
    left_join(mergable_roster, by = "vald_id") %>% 
    mutate(
      date = as.Date(date, origin = "1970-01-01"),
      time = as_hms(time),
      test_ID = as.character(test_ID)
    )
  
  create_log_entry(paste("Missing ForceDecks raw data created:", nrow(forcedecks_raw), "rows"))
  
  return(list(
    forcedecks_raw = forcedecks_raw,
    tests_processed = tests_processed,
    trials_wider = trials_wider,
    mergable_roster = mergable_roster
  ))
}

# Complete processing functions for each test type

# Process CMJ, LCMJ, SJ, ABCMJ data
process_missing_cmj_data <- function(forcedecks_raw) {
  cmj_temp <- forcedecks_raw %>% filter(test_type %in% c("CMJ", "LCMJ", "SJ", "ABCMJ"))
  
  if(nrow(cmj_temp) == 0) {
    create_log_entry("No missing CMJ data to process")
    return(NULL)
  }
  
  create_log_entry(paste("Processing", nrow(cmj_temp), "missing CMJ records"))
  
  cmj_processed <- cmj_temp %>%
    mutate(test_ID = as.character(test_ID)) %>%
    select(any_of(c(
      "test_ID", "vald_id", "full_name", "position", "team", "test_type", "date", "time", "body_weight_lbs",
      "countermovement_depth", "jump_height_inches_imp_mom", "bodymass_relative_takeoff_power",
      "mean_landing_power", "mean_eccentric_force", "mean_takeoff_acceleration", "mean_ecc_con_ratio",
      "mean_takeoff_velocity", "peak_landing_velocity", "peak_takeoff_force", "peak_takeoff_velocity",
      "concentric_rfd_100", "start_to_peak_force_time", "contraction_time", "concentric_duration",
      "eccentric_concentric_duration_ratio", "flight_eccentric_time_ratio", "displacement_at_takeoff",
      "rsi_modified_imp_mom", "positive_takeoff_impulse", "positive_impulse", "concentric_impulse",
      "eccentric_braking_impulse", "total_work", "relative_peak_landing_force",
      "relative_peak_concentric_force", "relative_peak_eccentric_force", "bm_rel_force_at_zero_velocity",
      "landing_impulse", "force_at_zero_velocity", "cmj_stiffness", "braking_phase_duration",
      "takeoff_velocity", "eccentric_time", "peak_landing_acceleration", "peak_takeoff_acceleration",
      "concentric_rfd_200", "eccentric_peak_power"
    ))) %>%
    clean_column_headers() %>%
    filter(!is.na(jump_height_inches_imp_mom)) %>%
    filter(between(jump_height_inches_imp_mom, 5, 28)) %>%
    arrange(full_name, test_type, date)
  
  create_log_entry(paste("Processed", nrow(cmj_processed), "CMJ records for upload"))
  return(cmj_processed)
}

# Process DJ data
process_missing_dj_data <- function(forcedecks_raw) {
  dj_temp <- forcedecks_raw %>% filter(test_type %in% c("DJ"))
  
  if(nrow(dj_temp) == 0) {
    create_log_entry("No missing DJ data to process")
    return(NULL)
  }
  
  create_log_entry(paste("Processing", nrow(dj_temp), "missing DJ records"))
  
  dj_processed <- dj_temp %>%
    select(any_of(c(
      "test_ID", "vald_id", "full_name", "position", "team", "date", "time", "body_weight_lbs", 
      "peak_takeoff_velocity", "peak_landing_velocity", "countermovement_depth", "peak_landing_force", 
      "eccentric_time", "jump_height_inches_imp_mom", "bm_rel_force_at_zero_velocity", "contact_time", 
      "contact_velocity", "active_stiffness", "passive_stiffness", "reactive_strength_index", 
      "positive_takeoff_impulse", "coefficient_of_restitution", "active_stiffness_index", 
      "passive_stiffness_index", "peak_impact_force", "peak_driveoff_force"
    ))) %>% 
    mutate(
      test_ID = as.character(test_ID),
      velocity_ratio = if_else(!is.na(peak_takeoff_velocity) & !is.na(peak_landing_velocity) & peak_landing_velocity != 0,
                              peak_takeoff_velocity / peak_landing_velocity, NA_real_),
      force_ratio = if_else(!is.na(peak_impact_force) & !is.na(peak_driveoff_force) & peak_driveoff_force != 0,
                           peak_impact_force / peak_driveoff_force, NA_real_),
      stiffness_ratio = if_else(!is.na(active_stiffness) & !is.na(passive_stiffness) & passive_stiffness != 0,
                               active_stiffness / passive_stiffness, NA_real_)
    ) %>% 
    filter(!is.na(jump_height_inches_imp_mom)) %>% 
    filter(jump_height_inches_imp_mom < 30 & jump_height_inches_imp_mom > 2)
  
  create_log_entry(paste("Processed", nrow(dj_processed), "DJ records for upload"))
  return(dj_processed)
}

# Process RSI data (RSAIP, RSHIP, RSKIP)
process_missing_rsi_data <- function(trials_wider, tests_processed, mergable_roster) {
  rsi_temp <- trials_wider %>%
    mutate(test_ID = as.character(testid), vald_id = athleteid) %>% 
    left_join(tests_processed %>% select(test_ID, test_type), by = "test_ID") %>% 
    filter(test_type %in% c("RSAIP", "RSHIP", "RSKIP"))
  
  if(nrow(rsi_temp) == 0) {
    create_log_entry("No missing RSI data to process")
    return(NULL)
  }
  
  create_log_entry(paste("Processing", nrow(rsi_temp), "missing RSI records"))
  
  rsi_processed <- rsi_temp %>%
    select(-testid, -athleteid) %>% 
    left_join(mergable_roster, by = "vald_id") %>% 
    mutate(
      test_ID = as.character(test_ID),
      date = as.Date(date, origin = "1970-01-01"),
      time = as_hms(time)
    ) %>% 
    select(any_of(c(
      "triallimb", "test_ID", "vald_id", "full_name", "position", "team", "date", "time", "body_weight_lbs",
      "start_to_peak_force", "peak_vertical_force", "rfd_at_100ms", "rfd_at_250ms",
      "iso_bm_rel_force_peak", "iso_bm_rel_force_100", "iso_bm_rel_force_200", "iso_abs_impulse_100"
    ))) %>% 
    group_by(test_ID, vald_id, triallimb) %>%
    summarise(
      across(any_of(c("full_name", "position", "team")), first),
      across(any_of(c("date", "time", "body_weight_lbs")), first), 
      across(any_of(c("start_to_peak_force", "peak_vertical_force", "rfd_at_100ms", "rfd_at_250ms",
               "iso_bm_rel_force_peak", "iso_bm_rel_force_100", "iso_bm_rel_force_200", "iso_abs_impulse_100")), 
             ~mean(.x, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    mutate(test_ID = as.character(test_ID)) %>%
    pivot_wider(
      id_cols = any_of(c("test_ID", "vald_id", "full_name", "position", "team", "date", "time", "body_weight_lbs")),
      names_from = triallimb,
      values_from = any_of(c("start_to_peak_force", "peak_vertical_force", "rfd_at_100ms", "rfd_at_250ms",
                      "iso_bm_rel_force_peak", "iso_bm_rel_force_100", "iso_bm_rel_force_200", "iso_abs_impulse_100")),
      names_sep = "_"
    )
  
  # Calculate bilateral averages
  if ("start_to_peak_force_Left" %in% names(rsi_processed) && "start_to_peak_force_Right" %in% names(rsi_processed)) {
    rsi_processed <- rsi_processed %>%
      mutate(
        start_to_peak_force_bilateral = rowMeans(cbind(start_to_peak_force_Left, start_to_peak_force_Right), na.rm = TRUE),
        peak_vertical_force_bilateral = rowMeans(cbind(peak_vertical_force_Left, peak_vertical_force_Right), na.rm = TRUE),
        rfd_at_100ms_bilateral = rowMeans(cbind(rfd_at_100ms_Left, rfd_at_100ms_Right), na.rm = TRUE),
        rfd_at_250ms_bilateral = rowMeans(cbind(rfd_at_250ms_Left, rfd_at_250ms_Right), na.rm = TRUE),
        iso_bm_rel_force_peak_bilateral = rowMeans(cbind(iso_bm_rel_force_peak_Left, iso_bm_rel_force_peak_Right), na.rm = TRUE),
        iso_bm_rel_force_100_bilateral = rowMeans(cbind(iso_bm_rel_force_100_Left, iso_bm_rel_force_100_Right), na.rm = TRUE),
        iso_bm_rel_force_200_bilateral = rowMeans(cbind(iso_bm_rel_force_200_Left, iso_bm_rel_force_200_Right), na.rm = TRUE),
        iso_abs_impulse_100_bilateral = rowMeans(cbind(iso_abs_impulse_100_Left, iso_abs_impulse_100_Right), na.rm = TRUE)
      )
  }
  
  rsi_processed <- rsi_processed %>%
    rename_with(~str_replace(.x, "_Left$", "_left"), everything()) %>%
    rename_with(~str_replace(.x, "_Right$", "_right"), everything())
  
  create_log_entry(paste("Processed", nrow(rsi_processed), "RSI records for upload"))
  return(rsi_processed)
}

# Process Rebound data (CMRJ, SLCMRJ)
process_missing_rebound_data <- function(trials_wider, tests_processed, mergable_roster) {
  rebound_temp <- trials_wider %>%
    mutate(test_ID = as.character(testid), vald_id = athleteid) %>% 
    left_join(tests_processed %>% select(test_ID, test_type), by = "test_ID") %>% 
    filter(test_type %in% c("CMRJ", "SLCMRJ"))
  
  if(nrow(rebound_temp) == 0) {
    create_log_entry("No missing Rebound data to process")
    return(NULL)
  }
  
  create_log_entry(paste("Processing", nrow(rebound_temp), "missing Rebound records"))
  
  rebound_processed <- rebound_temp %>%
    select(-testid, -athleteid) %>% 
    left_join(mergable_roster, by = "vald_id") %>% 
    mutate(
      test_ID = as.character(test_ID),
      date = as.Date(date, origin = "1970-01-01"),
      time = as_hms(time)
    ) %>% 
    select(any_of(c(
      "triallimb", "test_ID", "test_type", "vald_id", "full_name", "position", "team", "date", "time", "body_weight_lbs",
      "cmrj_takeoff_bm_rel_peak_force", "cmrj_takeoff_countermovement_depth", 
      "cmrj_takeoff_concentric_peak_force", "cmrj_takeoff_contraction_time", "cmrj_takeoff_ecc_decel_impulse",
      "cmrj_takeoff_ecc_duration", "cmrj_takeoff_bm_rel_ecc_peak_force", "cmrj_takeoff_jump_height_imp_mom_inches",
      "cmrj_takeoff_rsi_modified_imp_mom", "cmrj_rebound_active_stiffness", "cmrj_rebound_active_stiffness_index",
      "cmrj_rebound_contact_time", "cmrj_rebound_countermovement_depth", "cmrj_rebound_passive_stiffness",
      "cmrj_rebound_passive_stiffness_index", "cmrj_rebound_jump_height_imp_mom_inches"
    ))) %>% 
    group_by(test_ID, vald_id, test_type, triallimb) %>%
    summarise(
      across(any_of(c("full_name", "position", "team")), first),
      across(any_of(c("date", "time", "body_weight_lbs")), first), 
      across(where(is.numeric), ~mean(.x, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    mutate(
      test_ID = as.character(test_ID),
      limb_suffix = case_when(
        test_type == "CMRJ" ~ "bilateral",
        test_type == "SLCMRJ" & triallimb == "Left" ~ "left", 
        test_type == "SLCMRJ" & triallimb == "Right" ~ "right",
        TRUE ~ "bilateral"
      )
    ) %>%
    pivot_wider(
      id_cols = any_of(c("test_ID", "vald_id", "test_type", "full_name", "position", "team", "date", "time", "body_weight_lbs")),
      names_from = limb_suffix,
      values_from = any_of(c("cmrj_takeoff_bm_rel_peak_force", "cmrj_takeoff_countermovement_depth", 
                      "cmrj_takeoff_concentric_peak_force", "cmrj_takeoff_contraction_time", "cmrj_takeoff_ecc_decel_impulse",
                      "cmrj_takeoff_ecc_duration", "cmrj_takeoff_bm_rel_ecc_peak_force", "cmrj_takeoff_jump_height_imp_mom_inches",
                      "cmrj_takeoff_rsi_modified_imp_mom", "cmrj_rebound_active_stiffness", "cmrj_rebound_active_stiffness_index",
                      "cmrj_rebound_contact_time", "cmrj_rebound_countermovement_depth", "cmrj_rebound_passive_stiffness",
                      "cmrj_rebound_passive_stiffness_index", "cmrj_rebound_jump_height_imp_mom_inches")),
      names_sep = "_"
    )
  
  # Calculate bilateral averages for SLCMRJ tests
  bilateral_cols <- names(rebound_processed)[grepl("_bilateral$", names(rebound_processed))]
  for (col in bilateral_cols) {
    left_col <- str_replace(col, "_bilateral$", "_left")
    right_col <- str_replace(col, "_bilateral$", "_right")
    
    if (left_col %in% names(rebound_processed) && right_col %in% names(rebound_processed)) {
      rebound_processed <- rebound_processed %>%
        mutate(!!col := case_when(
          test_type == "SLCMRJ" & is.na(!!sym(col)) ~ 
            rowMeans(cbind(!!sym(left_col), !!sym(right_col)), na.rm = TRUE),
          TRUE ~ !!sym(col)
        ))
    }
  }
  
  create_log_entry(paste("Processed", nrow(rebound_processed), "Rebound records for upload"))
  return(rebound_processed)
}

# Process Single Leg Jump data
process_missing_slj_data <- function(trials_wider, tests_processed, mergable_roster) {
  slj_temp <- trials_wider %>%
    mutate(test_ID = as.character(testid), vald_id = athleteid) %>% 
    left_join(tests_processed %>% select(test_ID, test_type), by = "test_ID") %>% 
    filter(test_type == "SLJ")
  
  if(nrow(slj_temp) == 0) {
    create_log_entry("No missing SLJ data to process")
    return(NULL)
  }
  
  create_log_entry(paste("Processing", nrow(slj_temp), "missing SLJ records"))
  
  slj_processed <- slj_temp %>%
    select(-testid, -athleteid) %>% 
    left_join(mergable_roster, by = "vald_id") %>% 
    mutate(
      test_ID = as.character(test_ID),
      date = as.Date(date, origin = "1970-01-01"),
      time = as_hms(time)
    ) %>% 
    select(any_of(c(
      "triallimb", "test_ID", "vald_id", "full_name", "position", "team", "date", "time", "body_weight_lbs",
      "peak_landing_force", "peak_landing_velocity", "peak_takeoff_velocity", "time_to_peak_force",
      "weight_relative_peak_takeoff_force", "weight_relative_peak_landing_force", 
      "relative_peak_concentric_force", "relative_peak_eccentric_force", "lower_limb_stiffness", 
      "rsi_modified_imp_mom"
    ))) %>% 
    group_by(test_ID, vald_id, triallimb) %>%
    summarise(
      across(any_of(c("full_name", "position", "team")), first),
      across(any_of(c("date", "time", "body_weight_lbs")), first), 
      across(where(is.numeric), ~mean(.x, na.rm = TRUE)),
      .groups = "drop"
    ) %>%
    mutate(test_ID = as.character(test_ID)) %>%
    pivot_wider(
      id_cols = any_of(c("test_ID", "vald_id", "full_name", "position", "team", "date", "time", "body_weight_lbs")),
      names_from = triallimb,
      values_from = any_of(c("peak_landing_force", "peak_landing_velocity", "peak_takeoff_velocity", "time_to_peak_force",
                      "weight_relative_peak_takeoff_force", "weight_relative_peak_landing_force", 
                      "relative_peak_concentric_force", "relative_peak_eccentric_force", "lower_limb_stiffness", 
                      "rsi_modified_imp_mom")),
      names_sep = "_"
    )
  
  # Calculate bilateral averages
  if("peak_landing_force_Left" %in% names(slj_processed) & "peak_landing_force_Right" %in% names(slj_processed)) {
    slj_processed <- slj_processed %>%
      mutate(
        peak_landing_force_bilateral = rowMeans(cbind(peak_landing_force_Left, peak_landing_force_Right), na.rm = TRUE),
        peak_landing_velocity_bilateral = rowMeans(cbind(peak_landing_velocity_Left, peak_landing_velocity_Right), na.rm = TRUE),
        peak_takeoff_velocity_bilateral = rowMeans(cbind(peak_takeoff_velocity_Left, peak_takeoff_velocity_Right), na.rm = TRUE),
        time_to_peak_force_bilateral = rowMeans(cbind(time_to_peak_force_Left, time_to_peak_force_Right), na.rm = TRUE),
        weight_relative_peak_takeoff_force_bilateral = rowMeans(cbind(weight_relative_peak_takeoff_force_Left, weight_relative_peak_takeoff_force_Right), na.rm = TRUE),
        weight_relative_peak_landing_force_bilateral = rowMeans(cbind(weight_relative_peak_landing_force_Left, weight_relative_peak_landing_force_Right), na.rm = TRUE),
        relative_peak_concentric_force_bilateral = rowMeans(cbind(relative_peak_concentric_force_Left, relative_peak_concentric_force_Right), na.rm = TRUE),
        relative_peak_eccentric_force_bilateral = rowMeans(cbind(relative_peak_eccentric_force_Left, relative_peak_eccentric_force_Right), na.rm = TRUE),
        lower_limb_stiffness_bilateral = rowMeans(cbind(lower_limb_stiffness_Left, lower_limb_stiffness_Right), na.rm = TRUE),
        rsi_modified_imp_mom_bilateral = rowMeans(cbind(rsi_modified_imp_mom_Left, rsi_modified_imp_mom_Right), na.rm = TRUE)
      )
  }
  
  slj_processed <- slj_processed %>%
    rename_with(~str_replace(.x, "_Left$", "_left"), everything()) %>%
    rename_with(~str_replace(.x, "_Right$", "_right"), everything())
  
  create_log_entry(paste("Processed", nrow(slj_processed), "SLJ records for upload"))
  return(slj_processed)
}

# Process IMTP data
process_missing_imtp_data <- function(forcedecks_raw) {
  imtp_temp <- forcedecks_raw %>% filter(test_type == "IMTP")
  
  if(nrow(imtp_temp) == 0) {
    create_log_entry("No missing IMTP data to process")
    return(NULL)
  }
  
  create_log_entry(paste("Processing", nrow(imtp_temp), "missing IMTP records"))
  
  imtp_processed <- imtp_temp %>%
    select(any_of(c(
      "test_ID", "vald_id", "full_name", "position", "team", "date", "time",
      "start_to_peak_force", "rfd_at_100ms", "rfd_at_200ms", "force_at_100ms", 
      "iso_bm_rel_force_peak", "peak_vertical_force", "force_at_200ms"
    ))) %>% 
    clean_column_headers() %>%
    filter(!is.na(peak_vertical_force)) %>%
    arrange(full_name, date)
  
  # Add performance scores if we have the required columns
  if (all(c("peak_vertical_force", "start_to_peak_force", "rfd_at_100ms") %in% names(imtp_processed))) {
    imtp_processed <- imtp_processed %>%
      mutate(
        calc_performance_score = percent_rank(peak_vertical_force) * 100 * 2 +
          (100 - percent_rank(start_to_peak_force) * 100) +
          percent_rank(rfd_at_100ms) * 100,
        performance_score = percent_rank(calc_performance_score) * 100
      )
    
    if ("team" %in% names(imtp_processed)) {
      imtp_processed <- imtp_processed %>%
        group_by(team) %>%
        mutate(team_performance_score = percent_rank(calc_performance_score) * 100) %>%
        ungroup()
    }
    
    imtp_processed <- imtp_processed %>% select(-calc_performance_score)
  }
  
# Process missing NordBord data with rate limiting
process_missing_nordbord_data <- function(missing_test_ids) {
  if (length(missing_test_ids) == 0) {
    create_log_entry("No missing NordBord test_IDs to process")
    return(NULL)
  }
  
  create_log_entry(paste("Processing", length(missing_test_ids), "missing NordBord test_IDs with rate limiting"))
  
  missing_nordbord_tests <- list()
  successful_retrievals <- 0
  failed_retrievals <- 0
  
  for (i in seq_along(missing_test_ids)) {
    test_id <- missing_test_ids[i]
    
    create_log_entry(paste("Retrieving NordBord test", i, "of", length(missing_test_ids), "(ID:", test_id, ")"))
    
    test_data <- rate_limited_api_call(
      get_nordbord_test_by_id,
      test_id = test_id,
      description = paste("NordBord test", test_id)
    )
    
    if (!is.null(test_data) && nrow(test_data) > 0) {
      missing_nordbord_tests[[test_id]] <- test_data
      successful_retrievals <- successful_retrievals + 1
    } else {
      create_log_entry(paste("No data returned for NordBord test_ID:", test_id), "WARN")
      failed_retrievals <- failed_retrievals + 1
    }
    
    # Progress update every 10 retrievals
    if (i %% 10 == 0) {
      create_log_entry(paste("Progress:", i, "of", length(missing_test_ids), "completed"))
    }
  }
  
  create_log_entry(paste("NordBord retrieval complete - Success:", successful_retrievals, "Failed:", failed_retrievals))
  
  if (length(missing_nordbord_tests) > 0) {
    # Combine all missing tests into a single data frame
    combined_tests <- bind_rows(missing_nordbord_tests)
    
    # Process NordBord data (full processing from main script)
    nordbord_processed <- combined_tests %>% 
      select(-any_of(c("device", "notes", "testTypeId"))) %>% 
      mutate(
        modifiedDateUtc_chr = as.character(modifiedDateUtc),
        modifiedDateUtc_parsed = coalesce(
          ymd_hms(modifiedDateUtc_chr, tz = "UTC", quiet = TRUE),
          ymd_hm(modifiedDateUtc_chr,  tz = "UTC", quiet = TRUE),
          ymd_h(modifiedDateUtc_chr,   tz = "UTC", quiet = TRUE),
          ymd(modifiedDateUtc_chr,     tz = "UTC")
        ),
        modifiedDateUtc_local = with_tz(modifiedDateUtc_parsed, "America/Los_Angeles"),
        date = as.Date(modifiedDateUtc_local),
        time = hms::as_hms(modifiedDateUtc_local)
      ) %>%
      select(-any_of(c("modifiedDateUtc_chr", "modifiedDateUtc_parsed", "modifiedDateUtc_local", "modifiedDateUtc", "testDateUtc"))) %>% 
      mutate(
        vald_id = athleteId,
        test_ID = as.character(testId),
        test_type = testTypeName,
        impulse_left = if_else(leftRepetitions == 0, NA_integer_, if_else(leftCalibration == 0, leftImpulse, NA_integer_)),
        avg_force_left = if_else(leftRepetitions == 0, NA_integer_, leftAvgForce - leftCalibration),
        max_force_left = if_else(leftRepetitions == 0, NA_integer_, leftMaxForce - leftCalibration),
        impulse_right = if_else(rightRepetitions == 0, NA_integer_, if_else(rightCalibration == 0, rightImpulse, NA_integer_)),
        avg_force_right = if_else(rightRepetitions == 0, NA_integer_, rightAvgForce - rightCalibration),
        max_force_right = if_else(rightRepetitions == 0, NA_integer_, rightMaxForce - rightCalibration),
        reps_left = leftRepetitions,
        reps_right = rightRepetitions,
        avg_force_bilateral = rowMeans(cbind(avg_force_left, avg_force_right), na.rm = TRUE),
        max_force_bilateral = rowMeans(cbind(max_force_left, max_force_right), na.rm = TRUE),
        avg_force_asymmetry = case_when(
          is.na(avg_force_left) | is.na(avg_force_right) ~ NA_integer_,
          avg_force_left == 0 & avg_force_right == 0 ~ NA_integer_,
          avg_force_left >= avg_force_right ~ ((avg_force_left - avg_force_right) / avg_force_left),
          TRUE ~ ((avg_force_right - avg_force_left) / avg_force_right) 
        ),
        max_force_asymmetry = case_when(
          is.na(max_force_left) | is.na(max_force_right) ~ NA_integer_,
          max_force_left == 0 & max_force_right == 0 ~ NA_integer_,
          max_force_left >= max_force_right ~ ((max_force_left - max_force_right) / max_force_left),
          TRUE ~ ((max_force_right - max_force_left) / max_force_right) 
        ),
        impulse_asymmetry = case_when(
          is.na(impulse_left) | is.na(impulse_right) ~ NA_integer_,
          impulse_left == 0 & impulse_right == 0 ~ NA_integer_,
          impulse_left >= impulse_right ~ ((impulse_left - impulse_right) / impulse_left),
          TRUE ~ ((impulse_right - impulse_left) / impulse_right) 
        )
      ) %>% 
      select(-any_of(c("testTypeName", "athleteId", "testId", "leftTorque", "rightTorque", "leftMaxForce", 
                "rightMaxForce", "leftRepetitions", "rightRepetitions")))
    
    # Add body weight data if available (simplified approach for missing data)
    # In the main script this joins with body weight data, but for missing data we'll skip this complexity
    
    create_log_entry(paste("Successfully processed", nrow(nordbord_processed), "missing NordBord tests"))
    return(nordbord_processed)
    
  } else {
    create_log_entry("No missing NordBord tests could be retrieved", "WARN")
    return(NULL)
  }
}
process_missing_nordbord_data <- function(missing_test_ids) {
  if (length(missing_test_ids) == 0) {
    create_log_entry("No missing NordBord test_IDs to process")
    return(NULL)
  }
  
  create_log_entry(paste("Processing", length(missing_test_ids), "missing NordBord test_IDs"))
  
  missing_nordbord_tests <- list()
  successful_retrievals <- 0
  
  for (test_id in missing_test_ids) {
    tryCatch({
      test_data <- get_nordbord_test_by_id(test_id)
      if (!is.null(test_data) && nrow(test_data) > 0) {
        missing_nordbord_tests[[test_id]] <- test_data
        successful_retrievals <- successful_retrievals + 1
      } else {
        create_log_entry(paste("No data returned for NordBord test_ID:", test_id), "WARN")
      }
    }, error = function(e) {
      create_log_entry(paste("Error retrieving NordBord test_ID", test_id, ":", e$message), "ERROR")
    })
    
    # Add small delay to avoid rate limiting
    Sys.sleep(0.1)
  }
  
  if (length(missing_nordbord_tests) > 0) {
    # Combine all missing tests into a single data frame
    combined_tests <- bind_rows(missing_nordbord_tests)
    create_log_entry(paste("Successfully retrieved", successful_retrievals, "missing NordBord tests"))
    return(list(tests = combined_tests))
  } else {
    create_log_entry("No missing NordBord tests could be retrieved", "WARN")
    return(NULL)
  }
}

# Rate limiting for API calls
api_call_count <- 0
last_api_call <- Sys.time()
API_RATE_LIMIT <- 10  # calls per minute
API_CALL_DELAY <- 6   # seconds between calls

rate_limited_api_call <- function(api_function, ..., description = "API call") {
  # Check if we need to wait
  time_since_last <- as.numeric(difftime(Sys.time(), last_api_call, units = "secs"))
  
  if (time_since_last < API_CALL_DELAY) {
    wait_time <- API_CALL_DELAY - time_since_last
    create_log_entry(paste("Rate limiting: waiting", round(wait_time, 1), "seconds for", description), "INFO")
    Sys.sleep(wait_time)
  }
  
  # Reset counter every minute
  if (api_call_count >= API_RATE_LIMIT) {
    create_log_entry("Rate limit reached, waiting 60 seconds...", "INFO")
    Sys.sleep(60)
    api_call_count <<- 0
  }
  
  # Make the API call
  result <- tryCatch({
    api_function(...)
  }, error = function(e) {
    create_log_entry(paste("Rate-limited API call failed for", description, ":", e$message), "ERROR")
    NULL
  })
  
  # Update tracking
  api_call_count <<- api_call_count + 1
  last_api_call <<- Sys.time()
  
  return(result)
}

################################################################################
# Data Processing Functions (from main script)
################################################################################

# Clean column headers function
clean_column_headers <- function(df, remove_index = TRUE) {
  original_names <- names(df)
  
  if (remove_index) {
    index_cols <- which(original_names %in% c("", "X", "...1", "X1") | 
                          grepl("^\\.\\.\\.[0-9]+$", original_names) |
                          grepl("^X[0-9]*$", original_names))
    
    if (length(index_cols) > 0) {
      df <- df[, -index_cols, drop = FALSE]
    }
  }
  
  # Identify test_ID column position before cleaning
  test_id_positions <- which(names(df) %in% c("test_ID", "test_id"))
  
  cleaned_names <- names(df) %>%
    gsub("([a-z])([A-Z])", "\\1_\\2", .) %>%
    tolower() %>%
    gsub("[^a-zA-Z0-9_]", "_", .) %>%
    gsub("_{2,}", "_", .) %>%
    gsub("^_|_$", "", .) %>%
    ifelse(grepl("^[0-9]", .), paste0("x_", .), .)
  
  # Restore test_ID as primary key after cleaning
  if (length(test_id_positions) > 0) {
    cleaned_names[test_id_positions] <- "test_ID"
  }
  
  names(df) <- cleaned_names
  return(df)
}

# Standardize data types function
standardize_data_types <- function(data, table_name) {
  create_log_entry(paste("Standardizing data types for", table_name), "INFO")
  
  # Define expected data types for common columns
  character_columns <- c("name", "triallimb", "vald_id", "test_id", "test_ID", "team", "position", 
                        "position_class", "test_type", "full_name", "external_id", 
                        "email", "family_name", "given_name", "sport", "sex")
  
  date_columns <- c("date", "date_of_birth")
  time_columns <- c("time")
  integer_columns <- c("height", "reps_left", "reps_right")
  
  # Handle duplicate columns first
  if ("test_ID" %in% names(data) && "test_id" %in% names(data)) {
    create_log_entry(paste("Found duplicate test ID columns in", table_name, "- removing test_id and keeping test_ID"), "WARN")
    data <- data %>% select(-test_id)
  }
  
  for (col_name in names(data)) {
    original_type <- class(data[[col_name]])[1]
    target_type <- "numeric"  # Default to numeric/float
    
    # Determine target type based on column name
    if (col_name %in% character_columns) {
      target_type <- "character"
    } else if (col_name %in% date_columns) {
      target_type <- "Date"
    } else if (col_name %in% time_columns) {
      target_type <- "hms"
    } else if (col_name %in% integer_columns) {
      target_type <- "integer"
    }
    
    # Convert if needed
    if (original_type != target_type) {
      tryCatch({
        if (target_type == "character") {
          data[[col_name]] <- as.character(data[[col_name]])
        } else if (target_type == "Date") {
          if (is.character(data[[col_name]]) || is.factor(data[[col_name]])) {
            data[[col_name]] <- as.Date(data[[col_name]])
          } else if (is.numeric(data[[col_name]])) {
            data[[col_name]] <- as.Date(data[[col_name]], origin = "1970-01-01")
          }
        } else if (target_type == "hms") {
          if (is.character(data[[col_name]])) {
            data[[col_name]] <- hms::as_hms(data[[col_name]])
          } else if ("POSIXct" %in% class(data[[col_name]]) || "POSIXt" %in% class(data[[col_name]])) {
            data[[col_name]] <- hms::as_hms(format(data[[col_name]], "%H:%M:%S"))
          }
        } else if (target_type == "integer") {
          if (is.character(data[[col_name]])) {
            data[[col_name]] <- as.integer(as.numeric(data[[col_name]]))
          } else {
            data[[col_name]] <- as.integer(data[[col_name]])
          }
        } else if (target_type == "numeric") {
          if (is.character(data[[col_name]])) {
            numeric_vals <- suppressWarnings(as.numeric(data[[col_name]]))
            data[[col_name]] <- numeric_vals
          } else if (is.factor(data[[col_name]])) {
            data[[col_name]] <- as.numeric(as.character(data[[col_name]]))
          } else if (is.logical(data[[col_name]])) {
            data[[col_name]] <- as.numeric(data[[col_name]])
          } else {
            data[[col_name]] <- as.numeric(data[[col_name]])
          }
          
          if (is.numeric(data[[col_name]])) {
            data[[col_name]][is.infinite(data[[col_name]])] <- NA
            data[[col_name]][is.nan(data[[col_name]])] <- NA
          }
        }
      }, error = function(e) {
        create_log_entry(paste("Failed to convert", col_name, "to", target_type, ":", e$message), "ERROR")
      })
    }
  }
  
  return(data)
}

# Validate data frame function
validate_data_frame <- function(data, table_name) {
  if (nrow(data) == 0) {
    create_log_entry(paste("WARNING:", table_name, "data frame is empty"), "WARN")
    return(list(valid = FALSE, data = data))
  }
  
  if (ncol(data) == 0) {
    create_log_entry(paste("ERROR:", table_name, "data frame has no columns"), "ERROR")
    return(list(valid = FALSE, data = data))
  }
  
  # Standardize data types
  data <- standardize_data_types(data, table_name)
  
  # Check for invalid column names and fix them
  invalid_cols <- names(data)[!grepl("^[a-zA-Z_][a-zA-Z0-9_]*$", names(data))]
  if (length(invalid_cols) > 0) {
    fixed_names <- make.names(names(data), unique = TRUE)
    fixed_names <- gsub("\\.", "_", fixed_names)
    fixed_names <- gsub("^X", "x_", fixed_names)
    names(data) <- fixed_names
    create_log_entry(paste("FIXED: Invalid column names corrected for", table_name), "INFO")
  }
  
  return(list(valid = TRUE, data = data))
}

# Enhanced upload function with retry logic
enhanced_upload_to_bq <- function(data, table_name, write_disposition = "WRITE_APPEND", max_retries = 3) {
  create_log_entry(paste("Starting upload process for", table_name), "INFO")
  
  # Validate data first
  validation_result <- validate_data_frame(data, table_name)
  
  if (!validation_result$valid) {
    create_log_entry(paste("Data validation failed for", table_name), "ERROR")
    return(FALSE)
  }
  
  if (nrow(validation_result$data) == 0) {
    create_log_entry(paste("No data to upload for table:", table_name), "WARN")
    return(TRUE)
  }
  
  validated_data <- validation_result$data
  tbl <- bq_table(ds, table_name)
  
  # Attempt upload with retries
  for (attempt in 1:max_retries) {
    tryCatch({
      create_log_entry(paste("Upload attempt", attempt, "for", table_name), "INFO")
      
      # Ensure table exists
      if (!bq_table_exists(tbl)) {
        bq_table_create(tbl, fields = bigrquery::as_bq_fields(validated_data))
        create_log_entry(paste("Created table:", table_name), "INFO")
      }
      
      # Perform the upload
      bq_table_upload(tbl, validated_data, write_disposition = write_disposition)
      
      create_log_entry(paste("Successfully uploaded", nrow(validated_data), "rows to", table_name), "INFO")
      return(TRUE)
      
    }, error = function(e) {
      error_msg <- as.character(e$message)
      create_log_entry(paste("Upload attempt", attempt, "failed for", table_name, ":", error_msg), "ERROR")
      
      if (attempt < max_retries) {
        wait_time <- attempt * 5
        create_log_entry(paste("Waiting", wait_time, "seconds before retry..."), "INFO")
        Sys.sleep(wait_time)
      } else {
        create_log_entry(paste("All", max_retries, "upload attempts failed for", table_name), "ERROR")
        return(FALSE)
      }
    })
  }
  
  return(FALSE)
}

################################################################################
# Main Reconciliation Logic
################################################################################

create_log_entry("=== PHASE 1: COLLECTING EXISTING TEST_IDS FROM BIGQUERY ===")

# Get all tables in the dataset
all_tables <- get_dataset_tables()

# Exclude system tables and focus on VALD data tables
vald_tables <- all_tables[grepl("^vald_", all_tables) | all_tables %in% c("tests", "dates")]
create_log_entry(paste("Identified", length(vald_tables), "VALD-related tables:", paste(vald_tables, collapse = ", ")))

# Extract test_IDs from all tables
all_existing_test_ids <- data.frame(test_ID = character(0), source_table = character(0))

for (table_name in vald_tables) {
  table_test_ids <- extract_test_ids_from_table(table_name)
  all_existing_test_ids <- bind_rows(all_existing_test_ids, table_test_ids)
}

# Clean and deduplicate
existing_test_ids_clean <- all_existing_test_ids %>%
  filter(!is.na(test_ID), test_ID != "") %>%
  mutate(test_ID = as.character(test_ID)) %>%
  arrange(test_ID, source_table)

unique_existing_test_ids <- unique(existing_test_ids_clean$test_ID)
create_log_entry(paste("Found", nrow(existing_test_ids_clean), "total test_ID records across all tables"))
create_log_entry(paste("Found", length(unique_existing_test_ids), "unique test_IDs in BigQuery"))

################################################################################
# PHASE 2: GET COMPLETE LIST FROM VALD APIs
################################################################################

create_log_entry("=== PHASE 2: COLLECTING ALL TEST_IDS FROM VALD APIS ===")

# Get all ForceDecks tests
create_log_entry("Retrieving all ForceDecks tests from API...")
tryCatch({
  all_forcedecks_tests <- get_forcedecks_tests_only()
  if (!is.null(all_forcedecks_tests) && nrow(all_forcedecks_tests) > 0) {
    forcedecks_test_ids <- all_forcedecks_tests %>%
      mutate(test_ID = as.character(testId), source_api = "forcedecks") %>%
      select(test_ID, source_api)
    create_log_entry(paste("Retrieved", nrow(forcedecks_test_ids), "ForceDecks test_IDs from API"))
  } else {
    forcedecks_test_ids <- data.frame(test_ID = character(0), source_api = character(0))
    create_log_entry("No ForceDecks tests retrieved from API", "WARN")
  }
}, error = function(e) {
  create_log_entry(paste("Error retrieving ForceDecks tests:", e$message), "ERROR")
  forcedecks_test_ids <- data.frame(test_ID = character(0), source_api = character(0))
})

# Get all NordBord tests
create_log_entry("Retrieving all NordBord tests from API...")
tryCatch({
  all_nordbord_tests <- get_nordbord_tests_only()
  if (!is.null(all_nordbord_tests) && nrow(all_nordbord_tests) > 0) {
    nordbord_test_ids <- all_nordbord_tests %>%
      mutate(test_ID = as.character(testId), source_api = "nordbord") %>%
      select(test_ID, source_api)
    create_log_entry(paste("Retrieved", nrow(nordbord_test_ids), "NordBord test_IDs from API"))
  } else {
    nordbord_test_ids <- data.frame(test_ID = character(0), source_api = character(0))
    create_log_entry("No NordBord tests retrieved from API", "WARN")
  }
}, error = function(e) {
  create_log_entry(paste("Error retrieving NordBord tests:", e$message), "ERROR")
  nordbord_test_ids <- data.frame(test_ID = character(0), source_api = character(0))
})

# Combine all API test_IDs
all_api_test_ids <- bind_rows(forcedecks_test_ids, nordbord_test_ids)
unique_api_test_ids <- unique(all_api_test_ids$test_ID)
create_log_entry(paste("Total unique test_IDs from APIs:", length(unique_api_test_ids)))

################################################################################
# PHASE 3: IDENTIFY MISSING TEST_IDS
################################################################################

create_log_entry("=== PHASE 3: IDENTIFYING MISSING TEST_IDS ===")

# Find missing test_IDs (in API but not in BigQuery)
missing_test_ids <- setdiff(unique_api_test_ids, unique_existing_test_ids)
create_log_entry(paste("Found", length(missing_test_ids), "missing test_IDs"))

if (length(missing_test_ids) == 0) {
  create_log_entry("No missing test_IDs found - data is in sync!", "INFO")
  create_log_entry("=== RECONCILIATION COMPLETED - NO ACTION NEEDED ===")
} else {
  # Categorize missing test_IDs by source
  missing_forcedecks <- missing_test_ids[missing_test_ids %in% forcedecks_test_ids$test_ID]
  missing_nordbord <- missing_test_ids[missing_test_ids %in% nordbord_test_ids$test_ID]
  
  create_log_entry(paste("Missing ForceDecks test_IDs:", length(missing_forcedecks)))
  create_log_entry(paste("Missing NordBord test_IDs:", length(missing_nordbord)))
  
  # Log sample of missing IDs for debugging
  if (length(missing_test_ids) <= 20) {
    create_log_entry(paste("Missing test_IDs:", paste(head(missing_test_ids, 20), collapse = ", ")))
  } else {
    create_log_entry(paste("First 10 missing test_IDs:", paste(head(missing_test_ids, 10), collapse = ", ")))
  }

################################################################################
# PHASE 4: RETRIEVE AND PROCESS MISSING DATA
################################################################################

  create_log_entry("=== PHASE 4: RETRIEVING AND PROCESSING MISSING DATA ===")
  
  # Process missing ForceDecks data with full pipeline
  upload_results <- list()
  
  if (length(missing_forcedecks) > 0) {
    create_log_entry("Processing missing ForceDecks data with full pipeline...")
    
    # Get existing data for context
    existing_data <- list()
    existing_data$roster <- tryCatch({
      roster_query <- sprintf("SELECT * FROM `%s.%s.vald_roster`", project, dataset)
      DBI::dbGetQuery(con, roster_query)
    }, error = function(e) {
      create_log_entry("Could not load existing roster data", "WARN")
      data.frame()
    })
    
    missing_forcedecks_data <- process_missing_forcedecks_data(missing_forcedecks, existing_data)
    
    if (!is.null(missing_forcedecks_data)) {
      forcedecks_raw <- missing_forcedecks_data$forcedecks_raw
      
      create_log_entry(paste("Retrieved missing ForceDecks data:", nrow(forcedecks_raw), "raw records"))
      
      # Process each test type using the full processing pipeline
      
      # 1. CMJ, LCMJ, SJ, ABCMJ processing
      cmj_processed <- process_missing_cmj_data(forcedecks_raw)
      if (!is.null(cmj_processed)) {
        upload_results[["vald_fd_jumps"]] <- enhanced_upload_to_bq(cmj_processed, "vald_fd_jumps", "WRITE_APPEND")
        if (upload_results[["vald_fd_jumps"]]) {
          create_log_entry(paste("Successfully backfilled", nrow(cmj_processed), "missing CMJ records"))
        }
      }
      
      # 2. DJ processing
      dj_processed <- process_missing_dj_data(forcedecks_raw)
      if (!is.null(dj_processed)) {
        upload_results[["vald_fd_dj"]] <- enhanced_upload_to_bq(dj_processed, "vald_fd_dj", "WRITE_APPEND")
        if (upload_results[["vald_fd_dj"]]) {
          create_log_entry(paste("Successfully backfilled", nrow(dj_processed), "missing DJ records"))
        }
      }
      
      # 3. RSI processing (RSAIP, RSHIP, RSKIP)
      rsi_processed <- process_missing_rsi_data(
        missing_forcedecks_data$trials_wider, 
        missing_forcedecks_data$tests_processed, 
        missing_forcedecks_data$mergable_roster
      )
      if (!is.null(rsi_processed)) {
        upload_results[["vald_fd_rsi"]] <- enhanced_upload_to_bq(rsi_processed, "vald_fd_rsi", "WRITE_APPEND")
        if (upload_results[["vald_fd_rsi"]]) {
          create_log_entry(paste("Successfully backfilled", nrow(rsi_processed), "missing RSI records"))
        }
      }
      
      # 4. Rebound processing (CMRJ, SLCMRJ)
      rebound_processed <- process_missing_rebound_data(
        missing_forcedecks_data$trials_wider, 
        missing_forcedecks_data$tests_processed, 
        missing_forcedecks_data$mergable_roster
      )
      if (!is.null(rebound_processed)) {
        upload_results[["vald_fd_rebound"]] <- enhanced_upload_to_bq(rebound_processed, "vald_fd_rebound", "WRITE_APPEND")
        if (upload_results[["vald_fd_rebound"]]) {
          create_log_entry(paste("Successfully backfilled", nrow(rebound_processed), "missing Rebound records"))
        }
      }
      
      # 5. Single Leg Jump processing (SLJ)
      slj_processed <- process_missing_slj_data(
        missing_forcedecks_data$trials_wider, 
        missing_forcedecks_data$tests_processed, 
        missing_forcedecks_data$mergable_roster
      )
      if (!is.null(slj_processed)) {
        upload_results[["vald_fd_sl_jumps"]] <- enhanced_upload_to_bq(slj_processed, "vald_fd_sl_jumps", "WRITE_APPEND")
        if (upload_results[["vald_fd_sl_jumps"]]) {
          create_log_entry(paste("Successfully backfilled", nrow(slj_processed), "missing SLJ records"))
        }
      }
      
      # 6. IMTP processing
      imtp_processed <- process_missing_imtp_data(forcedecks_raw)
      if (!is.null(imtp_processed)) {
        upload_results[["vald_fd_imtp"]] <- enhanced_upload_to_bq(imtp_processed, "vald_fd_imtp", "WRITE_APPEND")
        if (upload_results[["vald_fd_imtp"]]) {
          create_log_entry(paste("Successfully backfilled", nrow(imtp_processed), "missing IMTP records"))
        }
      }
      
      # Update dates and tests tables with new missing data
      new_dates <- forcedecks_raw %>% 
        select(date) %>% 
        unique() %>%
        anti_join(
          # Get existing dates to avoid duplicates
          tryCatch({
            dates_query <- sprintf("SELECT DISTINCT date FROM `%s.%s.dates`", project, dataset)
            DBI::dbGetQuery(con, dates_query)
          }, error = function(e) data.frame(date = as.Date(character(0)))),
          by = "date"
        )
      
      if (nrow(new_dates) > 0) {
        upload_results[["dates"]] <- enhanced_upload_to_bq(new_dates, "dates", "WRITE_APPEND")
        if (upload_results[["dates"]]) {
          create_log_entry(paste("Added", nrow(new_dates), "new dates"))
        }
      }
      
      new_tests <- forcedecks_raw %>% 
        select(test_ID) %>% 
        mutate(test_ID = as.character(test_ID)) %>%
        unique() %>%
        anti_join(
          # Get existing test_IDs to avoid duplicates
          tryCatch({
            tests_query <- sprintf("SELECT DISTINCT test_ID FROM `%s.%s.tests`", project, dataset)
            DBI::dbGetQuery(con, tests_query)
          }, error = function(e) data.frame(test_ID = character(0))),
          by = "test_ID"
        )
      
      if (nrow(new_tests) > 0) {
        upload_results[["tests"]] <- enhanced_upload_to_bq(new_tests, "tests", "WRITE_APPEND")
        if (upload_results[["tests"]]) {
          create_log_entry(paste("Added", nrow(new_tests), "new test_IDs"))
        }
      }
      
    } else {
      create_log_entry("No ForceDecks missing data could be processed", "WARN")
    }
  }
  
  # Process missing NordBord data with full pipeline and rate limiting
  if (length(missing_nordbord) > 0) {
    create_log_entry("Processing missing NordBord data with full pipeline and rate limiting...")
    
    nordbord_processed <- process_missing_nordbord_data(missing_nordbord)
    
    if (!is.null(nordbord_processed)) {
      upload_results[["vald_nord_all"]] <- enhanced_upload_to_bq(nordbord_processed, "vald_nord_all", "WRITE_APPEND")
      
      if (upload_results[["vald_nord_all"]]) {
        create_log_entry(paste("Successfully backfilled", nrow(nordbord_processed), "missing NordBord records"))
        
        # Update dates and tests tables with NordBord data
        nord_dates <- nordbord_processed %>% 
          select(date) %>% 
          unique() %>%
          anti_join(
            tryCatch({
              dates_query <- sprintf("SELECT DISTINCT date FROM `%s.%s.dates`", project, dataset)
              DBI::dbGetQuery(con, dates_query)
            }, error = function(e) data.frame(date = as.Date(character(0)))),
            by = "date"
          )
        
        if (nrow(nord_dates) > 0) {
          upload_results[["dates_nord"]] <- enhanced_upload_to_bq(nord_dates, "dates", "WRITE_APPEND")
          if (upload_results[["dates_nord"]]) {
            create_log_entry(paste("Added", nrow(nord_dates), "new NordBord dates"))
          }
        }
        
        nord_tests <- nordbord_processed %>% 
          select(test_ID) %>% 
          mutate(test_ID = as.character(test_ID)) %>%
          unique() %>%
          anti_join(
            tryCatch({
              tests_query <- sprintf("SELECT DISTINCT test_ID FROM `%s.%s.tests`", project, dataset)
              DBI::dbGetQuery(con, tests_query)
            }, error = function(e) data.frame(test_ID = character(0))),
            by = "test_ID"
          )
        
        if (nrow(nord_tests) > 0) {
          upload_results[["tests_nord"]] <- enhanced_upload_to_bq(nord_tests, "tests", "WRITE_APPEND")
          if (upload_results[["tests_nord"]]) {
            create_log_entry(paste("Added", nrow(nord_tests), "new NordBord test_IDs"))
          }
        }
      }
    } else {
      create_log_entry("No NordBord missing data could be processed", "WARN")
    }
  }
  
  # Summary of upload results
  create_log_entry("=== UPLOAD SUMMARY ===")
  successful_uploads <- sum(unlist(upload_results), na.rm = TRUE)
  total_uploads <- length(upload_results)
  
  for (table_name in names(upload_results)) {
    status <- if (upload_results[[table_name]]) "SUCCESS" else "FAILED"
    create_log_entry(paste(table_name, ":", status), if (upload_results[[table_name]]) "INFO" else "ERROR")
  }
  
  create_log_entry(paste("Upload summary:", successful_uploads, "of", total_uploads, "operations completed successfully"))
  
  if (successful_uploads < total_uploads) {
    failed_operations <- names(upload_results)[!unlist(upload_results)]
    create_log_entry(paste("Failed operations:", paste(failed_operations, collapse = ", ")), "ERROR")
  }
  
  create_log_entry("=== RECONCILIATION PROCESSING COMPLETED ===")
}

################################################################################
# PHASE 5: FINAL REPORTING
################################################################################

create_log_entry("=== PHASE 5: FINAL REPORTING ===")

# Summary statistics
create_log_entry(paste("Summary:"))
create_log_entry(paste("- Tables scanned:", length(vald_tables)))
create_log_entry(paste("- Existing unique test_IDs:", length(unique_existing_test_ids)))
create_log_entry(paste("- API unique test_IDs:", length(unique_api_test_ids)))
create_log_entry(paste("- Missing test_IDs identified:", length(missing_test_ids)))

if (length(missing_test_ids) > 0) {
  create_log_entry(paste("- Missing ForceDecks test_IDs:", length(missing_forcedecks)))
  create_log_entry(paste("- Missing NordBord test_IDs:", length(missing_nordbord)))
  
  if (length(missing_forcedecks) > 0) {
    create_log_entry("ACTION NEEDED: ForceDecks missing data requires manual processing due to complexity", "WARN")
  }
  
  if (length(missing_nordbord) > 0) {
    create_log_entry("Missing NordBord data processed and uploaded")
  }
} else {
  create_log_entry("No missing data found - all systems in sync!")
}

create_log_entry(paste("Total execution time:", 
                      round(difftime(Sys.time(), script_start_time, units = "mins"), 2), "minutes"))
create_log_entry("=== SATURDAY DATA RECONCILIATION COMPLETED ===", "END")

# Upload logs to BigQuery
upload_logs_to_bigquery()

# Close BigQuery connection
tryCatch({
  DBI::dbDisconnect(con)
}, error = function(e) {
  create_log_entry(paste("Warning: Could not close BigQuery connection:", e$message), "WARN")
})

cat("Saturday data reconciliation completed successfully\n")
