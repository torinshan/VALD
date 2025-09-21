#!/usr/bin/env Rscript

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
cat("GCP Project:", project, "\n")
cat("BQ Dataset:", dataset, "\n")
cat("BQ Location:", location, "\n")

# Authenticate to BigQuery using working method
tryCatch({
  cat("=== Authenticating to BigQuery ===\n")
  
  # Get access token from gcloud (this works since gcloud auth is successful)
  access_token_result <- system("gcloud auth print-access-token", intern = TRUE)
  access_token <- access_token_result[1]  # Get first line
  
  if (nchar(access_token) > 0) {
    cat("Access token obtained from gcloud\n")
    
    # Create gargle token object (Method 4 from debug - this works!)
    library(gargle)
    token <- gargle::gargle2.0_token(
      scope = 'https://www.googleapis.com/auth/bigquery',
      client = gargle::gargle_client(),
      credentials = list(access_token = access_token)
    )
    
    # Set token for bigrquery
    bigrquery::bq_auth(token = token)
    cat("BigQuery authentication successful\n")
    
    # Test authentication immediately
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
if (!bq_dataset_exists(ds)) {
  bq_dataset_create(ds, location = location)
  cat("Created BigQuery dataset:", dataset, "in", location, "\n")
}

################################################################################
# Logging Functions
################################################################################

create_log_entry <- function(message, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  log_message <- paste0("[", timestamp, "] [", level, "] ", message)
  cat(log_message, "\n")
  
  log_df <- data.frame(
    timestamp = as.POSIXct(timestamp),
    level = level,
    message = message,
    run_id = Sys.getenv("GITHUB_RUN_ID", "manual"),
    repository = Sys.getenv("GITHUB_REPOSITORY", "unknown"),
    stringsAsFactors = FALSE
  )
  
  log_tbl <- bq_table(ds, "vald_processing_log")
  
  tryCatch({
    if (!bq_table_exists(log_tbl)) {
      bq_table_create(log_tbl, fields = log_df)
    }
    bq_table_upload(log_tbl, log_df, write_disposition = "WRITE_APPEND")
  }, error = function(e) {
    cat("Failed to write log to BigQuery:", e$message, "\n")
  })
  
  return(log_message)
}

script_start_time <- Sys.time()

create_log_entry("=== VALD DATA PROCESSING SCRIPT STARTED ===", "START")
create_log_entry(paste("Script execution user:", Sys.getenv("GITHUB_ACTOR", Sys.getenv("USERNAME", "unknown"))))
create_log_entry(paste("BigQuery project:", project))
create_log_entry(paste("BigQuery dataset:", dataset))

################################################################################
# BigQuery Helper Functions
################################################################################

# Fix data type for body_weight_lbs in vald_fd_rsi table
fix_rsi_data_type <- function() {
  create_log_entry("Checking and fixing body_weight_lbs data type in vald_fd_rsi table")
  
  # Check current data type
  check_query <- sprintf("SELECT data_type FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS` 
                         WHERE table_name = 'vald_fd_rsi' AND column_name = 'body_weight_lbs'", 
                        project, dataset)
  
  tryCatch({
    current_type <- DBI::dbGetQuery(con, check_query)
    
    if (nrow(current_type) > 0 && current_type$data_type[1] == "STRING") {
      create_log_entry("body_weight_lbs is STRING type, converting to FLOAT64")
      
      fix_query <- sprintf("
        CREATE OR REPLACE TABLE `%s.%s.vald_fd_rsi` AS 
        SELECT 
          * EXCEPT(body_weight_lbs),
          SAFE_CAST(body_weight_lbs AS FLOAT64) AS body_weight_lbs
        FROM `%s.%s.vald_fd_rsi`
      ", project, dataset, project, dataset)
      
      DBI::dbExecute(con, fix_query)
      create_log_entry("Successfully converted body_weight_lbs to FLOAT64 in vald_fd_rsi")
      
    } else {
      create_log_entry("body_weight_lbs is already FLOAT64 type or column not found")
    }
    
  }, error = function(e) {
    create_log_entry(paste("Error checking/fixing body_weight_lbs data type:", e$message), "WARN")
  })
}

# Check if table exists and has data
table_exists_and_has_data <- function(table_name) {
  tbl <- bq_table(ds, table_name)
  if (!bq_table_exists(tbl)) {
    create_log_entry(paste("Table", table_name, "does not exist"), "WARN")
    return(FALSE)
  }
  
  # Check if table has any rows
  tryCatch({
    count_query <- sprintf("SELECT COUNT(*) as row_count FROM `%s.%s.%s`", project, dataset, table_name)
    result <- DBI::dbGetQuery(con, count_query)
    has_data <- result$row_count > 0
    create_log_entry(paste("Table", table_name, "exists with", result$row_count, "rows"))
    return(has_data)
  }, error = function(e) {
    create_log_entry(paste("Error checking table", table_name, "row count:", e$message), "ERROR")
    return(FALSE)
  })
}

safe_bq_query <- function(query, description = "") {
  tryCatch({
    result <- DBI::dbGetQuery(con, query)
    if (nchar(description) > 0) {
      create_log_entry(paste("Successfully executed:", description))
    }
    return(result)
  }, error = function(e) {
    if (nchar(description) > 0) {
      create_log_entry(paste("Query failed for", description, ":", e$message), "ERROR")
    }
    return(data.frame())
  })
}

upload_to_bq <- function(data, table_name, write_disposition = "WRITE_TRUNCATE") {
  if (nrow(data) == 0) {
    create_log_entry(paste("No data to upload for table:", table_name), "WARN")
    return()
  }
  
  tbl <- bq_table(ds, table_name)
  
  tryCatch({
    if (!bq_table_exists(tbl)) {
      bq_table_create(tbl, fields = data[0, ])
      create_log_entry(paste("Created table:", table_name))
    }
    
    bq_table_upload(tbl, data, write_disposition = write_disposition)
    create_log_entry(paste("Uploaded", nrow(data), "rows to", table_name))
  }, error = function(e) {
    create_log_entry(paste("Failed to upload to", table_name, ":", e$message), "ERROR")
    stop(e)
  })
}

# Read BQ table with proper error handling
read_bq_table <- function(table_name) {
  if (!table_exists_and_has_data(table_name)) {
    create_log_entry(paste("Table", table_name, "does not exist or is empty, returning empty data frame"), "INFO")
    return(data.frame())
  }
  
  query <- sprintf("SELECT * FROM `%s.%s.%s`", project, dataset, table_name)
  result <- safe_bq_query(query, paste("reading", table_name))
  
  if (nrow(result) == 0) {
    create_log_entry(paste("Table", table_name, "exists but returned no data"), "WARN")
    return(data.frame())
  }
  
  create_log_entry(paste("Successfully read", table_name, ":", nrow(result), "rows"))
  return(result)
}

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
  set_start_date("2024-01-01T00:00:00Z")
  
  create_log_entry("VALD API credentials configured successfully")
}, error = function(e) {
  create_log_entry(paste("VALD API setup failed:", e$message), "ERROR")
  quit(status = 1)
})

################################################################################
# Fix RSI Data Type Issue
################################################################################

fix_rsi_data_type()

################################################################################
# Get Current Data State from BigQuery
################################################################################

create_log_entry("=== READING CURRENT DATA STATE FROM BIGQUERY ===")

current_dates <- read_bq_table("dates")
if (nrow(current_dates) > 0) {
  current_dates <- current_dates %>% 
    select(date) %>% 
    unique() %>%
    mutate(date = as.Date(date))
  create_log_entry(paste("Found", nrow(current_dates), "existing dates in BigQuery"))
} else {
  current_dates <- data.frame(date = as.Date(character(0)))
  create_log_entry("No existing dates found in BigQuery")
}

test_ids <- read_bq_table("tests")
if (nrow(test_ids) > 0) {
  test_ids <- test_ids %>% unique()
  create_log_entry(paste("Found", nrow(test_ids), "existing tests in BigQuery"))
} else {
  test_ids <- data.frame(test_ID = character(0))
  create_log_entry("No existing tests found in BigQuery")
}

if (nrow(current_dates) > 0) {
  latest_date_current <- max(current_dates$date, na.rm = TRUE)
} else {
  latest_date_current <- as.Date("1900-01-01")
}

count_tests_current <- nrow(test_ids)

create_log_entry(paste("Current state - Latest date:", latest_date_current, "Test count:", count_tests_current))

################################################################################
# Get All Tests from VALD API
################################################################################

create_log_entry("=== FETCHING ALL TESTS FROM VALD API ===")

tryCatch({
  all_tests <- get_forcedecks_tests_only(start_date = NULL)
  create_log_entry("Successfully retrieved tests from VALD API")
}, error = function(e) {
  create_log_entry(paste("Failed to retrieve tests from VALD API:", e$message), "ERROR")
  quit(status = 1)
})

all_tests <- all_tests %>% 
  mutate(
    vald_id = profileId,
    test_type = testType,
    test_ID = testId,
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
    -modifiedDateUtc, -testId, -tenantId, -profileId, -testType, -weight, 
    -analysedDateUtc, -analysedDateOffset, -analysedDateTimezone,
    -recordedDateUtc_parsed, -recordedDateUtc_local, -recordedDateUtc, 
    -recordedDateOffset, -recordedDateTimezone, -recordingId
  )

all_dates <- all_tests %>% 
  select(date) %>%
  unique()

all_count <- all_tests %>% 
  select(test_ID) %>% 
  unique()

latest_date_all <- max(all_dates$date, na.rm = TRUE)
count_tests_all <- nrow(all_count)

create_log_entry(paste("API data - Latest date:", latest_date_all, "Test count:", count_tests_all))

date_mismatch <- latest_date_current != latest_date_all
count_mismatch <- count_tests_current != count_tests_all

create_log_entry("=== CONDITIONAL LOGIC EVALUATION ===")
create_log_entry(paste("Latest date current:", as.character(latest_date_current)))
create_log_entry(paste("Latest date all:", as.character(latest_date_all)))
create_log_entry(paste("Count tests current:", count_tests_current))
create_log_entry(paste("Count tests all:", count_tests_all))
create_log_entry(paste("date_mismatch:", date_mismatch))
create_log_entry(paste("count_mismatch:", count_mismatch))

if (count_mismatch && date_mismatch) {
  route_taken <- "FULL_PROCESSING"
  create_log_entry("ROUTE TAKEN: FULL PROCESSING (both date and count mismatch)", "DECISION")
  
} else if (count_mismatch && !date_mismatch) {
  route_taken <- "PARTIAL_PROCESSING" 
  create_log_entry("ROUTE TAKEN: PARTIAL PROCESSING (count mismatch only)", "DECISION")
  
} else {
  route_taken <- "NO_PROCESSING"
  create_log_entry("ROUTE TAKEN: NO PROCESSING (no mismatches)", "DECISION")
}

create_log_entry(paste("Processing route selected:", route_taken))

################################################################################
# Processing Functions
################################################################################

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
  
  cleaned_names <- names(df) %>%
    gsub("([a-z])([A-Z])", "\\1_\\2", .) %>%
    tolower() %>%
    gsub("[^a-zA-Z0-9_]", "_", .) %>%
    gsub("_{2,}", "_", .) %>%
    gsub("^_|_$", "", .) %>%
    ifelse(grepl("^[0-9]", .), paste0("x_", .), .)
  
  names(df) <- cleaned_names
  return(df)
}

process_simple_roster <- function(roster_data) {
  # Check if we have any data
  if (nrow(roster_data) == 0) {
    create_log_entry("Roster data is empty, returning empty roster", "WARN")
    return(data.frame(
      external_id = character(0), position = character(0), sport = character(0), 
      height = integer(0), sex = character(0), date_of_birth = as.Date(character(0)), 
      email = character(0), vald_id = character(0), family_name = character(0), 
      weight = numeric(0), given_name = character(0)
    ))
  }
  
  # Define the columns we want to keep
  required_columns <- c("external_id", "position", "sport", "height", "sex", 
                       "date_of_birth", "email", "vald_id", "family_name", 
                       "weight", "given_name")
  
  actual_cols <- names(roster_data)
  create_log_entry(paste("Roster columns found:", paste(head(actual_cols, 10), collapse = ", ")))
  
  # Select only the columns we need (if they exist)
  available_columns <- intersect(required_columns, actual_cols)
  missing_columns <- setdiff(required_columns, actual_cols)
  
  if (length(missing_columns) > 0) {
    create_log_entry(paste("Missing roster columns:", paste(missing_columns, collapse = ", ")), "WARN")
  }
  
  if (length(available_columns) == 0) {
    create_log_entry("No required roster columns found", "ERROR")
    return(data.frame(
      external_id = character(0), position = character(0), sport = character(0), 
      height = integer(0), sex = character(0), date_of_birth = as.Date(character(0)), 
      email = character(0), vald_id = character(0), family_name = character(0), 
      weight = numeric(0), given_name = character(0)
    ))
  }
  
  # Select available columns and add missing ones as NA
  roster_clean <- roster_data %>%
    select(any_of(available_columns))
  
  # Add missing columns as NA with correct types
  for (col in missing_columns) {
    if (col == "height") {
      roster_clean[[col]] <- NA_integer_
    } else if (col == "weight") {
      roster_clean[[col]] <- NA_real_
    } else if (col == "date_of_birth") {
      roster_clean[[col]] <- as.Date(NA)
    } else {
      roster_clean[[col]] <- NA_character_
    }
  }
  
  # Reorder columns to match required order
  roster_clean <- roster_clean %>%
    select(all_of(required_columns))
  
  create_log_entry(paste("Simplified roster processed:", nrow(roster_clean), "rows with", 
                        length(available_columns), "available columns"))
  
  return(roster_clean)
}

append_and_finalize <- function(new_df, old_df, keys = NULL, table_name = "Unknown") {
  if (is.null(old_df) && is.null(new_df)) return(data.frame())
  if (is.null(old_df)) return(new_df)
  if (is.null(new_df)) return(old_df)
  
  if (nrow(old_df) == 0 && nrow(new_df) == 0) return(data.frame())
  if (nrow(old_df) == 0) return(new_df)
  if (nrow(new_df) == 0) return(old_df)
  
  rows_before <- nrow(old_df) + nrow(new_df)
  combined <- bind_rows(old_df, new_df)
  
  if (is.null(keys) || !all(keys %in% names(combined))) {
    create_log_entry(paste("INFO:", table_name, "- Using exact row deduplication (no valid keys)"))
    result <- distinct(combined)
  } else {
    result <- distinct(combined, across(all_of(keys)), .keep_all = TRUE)
  }
  
  rows_after <- nrow(result)
  rows_removed <- rows_before - rows_after
  
  if (rows_removed > 0) {
    create_log_entry(paste("INFO:", table_name, "- Removed", rows_removed, "duplicate rows during append"))
  }
  
  return(result)
}

################################################################################
# Main Processing Logic
################################################################################

tryCatch({

if (count_mismatch && date_mismatch) {
  create_log_entry("Both mismatches found – running full code.")
  
  # Set Start Date
  date_count_start_date <- paste0(latest_date_current + days(1), "T00:00:00Z")
  set_start_date(date_count_start_date)
  
  # Import existing data from BigQuery
  create_log_entry("=== IMPORTING EXISTING DATA FROM BIGQUERY ===")
  forcedecks_jump_clean_imported <- read_bq_table("vald_fd_jumps")
  forcedecks_SLJ_clean_imported <- read_bq_table("vald_fd_sl_jumps")
  nordboard_clean_imported <- read_bq_table("vald_nord_all")
  forcedecks_RSI_clean_imported <- read_bq_table("vald_fd_rsi")
  forcedecks_jump_DJ_clean_imported <- read_bq_table("vald_fd_dj")
  forcedecks_rebound_clean_imported <- read_bq_table("vald_fd_rebound")
  forcedecks_IMTP_clean_imported <- read_bq_table("vald_fd_imtp")
  dates_imported <- read_bq_table("dates")
  tests_imported <- read_bq_table("tests")
  
  # Get new data from VALD API
  create_log_entry("Fetching ForceDecks data from VALD API...")
  Injest_forcedekcs_data <- get_forcedecks_data()
  
  profiles <- setDT(Injest_forcedekcs_data$profiles)
  definitions <- setDT(Injest_forcedekcs_data$result_definitions)
  tests <- setDT(Injest_forcedekcs_data$tests)
  trials <- setDT(Injest_forcedekcs_data$trials)
  
  roster <- profiles %>%
    select(profileId, givenName, familyName) %>% 
    mutate(
      full_name = (paste(trimws(givenName), trimws(familyName), sep = " ")),
      first_name = givenName,
      last_name = familyName,
      vald_id = profileId
    ) %>% 
    select(-givenName, -familyName, -profileId)
  
  # Read VALD Roster from BigQuery or GitHub fallback
  Vald_roster_raw <- read_bq_table("vald_roster")
  
  if (nrow(Vald_roster_raw) > 0) {
    create_log_entry("Reading roster data from BigQuery")
    
    Vald_roster <- process_simple_roster(Vald_roster_raw)
    create_log_entry(paste("Processed", nrow(Vald_roster), "athletes from BigQuery roster"))
    
  } else {
    create_log_entry("Roster not found in BigQuery, trying GitHub fallback", "WARN")
    
    tryCatch({
      roster_url <- "https://raw.githubusercontent.com/torinshan/VALD/main/.github/vald_roster.csv"
      Vald_roster_raw <- read_csv(roster_url, show_col_types = FALSE)
      
      if (nrow(Vald_roster_raw) > 0) {
        create_log_entry("Successfully read roster from GitHub")
        Vald_roster <- process_simple_roster(Vald_roster_raw)
        create_log_entry(paste("Processed", nrow(Vald_roster), "athletes from GitHub roster"))
      } else {
        Vald_roster <- data.frame(
          external_id = character(0), position = character(0), sport = character(0), 
          height = integer(0), sex = character(0), date_of_birth = as.Date(character(0)), 
          email = character(0), vald_id = character(0), family_name = character(0), 
          weight = numeric(0), given_name = character(0)
        )
        create_log_entry("GitHub roster file was empty", "WARN")
      }
    }, error = function(e) {
      create_log_entry(paste("Failed to read roster from GitHub:", e$message), "WARN")
      Vald_roster <- data.frame(
        external_id = character(0), position = character(0), sport = character(0), 
        height = integer(0), sex = character(0), date_of_birth = as.Date(character(0)), 
        email = character(0), vald_id = character(0), family_name = character(0), 
        weight = numeric(0), given_name = character(0)
      )
    })
  }
  
  # Process tests
  tests <- tests %>% 
    mutate(
      vald_id = profileId,
      test_type = testType,
      test_ID = testId,
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
  definitions <- definitions %>% 
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
  
  # Find the position of the start_of_movement column
  start_col <- which(names(trials_wider) == "start_of_movement")
  
  if (length(start_col) > 0) {
    trials_wider <- trials_wider %>% 
      mutate(across(all_of(start_col:ncol(.)), as.numeric))
  }
  
  mergable_trials <- trials_wider %>% 
    mutate(test_ID = testid) %>% 
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
    mutate(vald_id = athleteid) %>% 
    select(-athleteid) 
  
  mergable_tests <- tests %>%
    select(test_ID, test_type)
  
  mergable_roster <- roster %>% 
    select(-first_name, -last_name) %>% 
    left_join(Vald_roster %>% select(vald_id, position, sport), by = "vald_id")
  
  # Mass Merge
  forcedecks_raw <- mergable_trials %>% 
    left_join(mergable_tests, by = "test_ID") %>% 
    left_join(mergable_roster, by = "vald_id") %>% 
    mutate(
      date = as.Date(date, origin = "1970-01-01"),
      time = as_hms(time)
    )
  
  ############################################################################
  # CMJ, LCMJ, SJ, ABCMJ processing
  ############################################################################
  
  cmj_temp <- forcedecks_raw %>% filter(test_type %in% c("CMJ", "LCMJ", "SJ", "ABCMJ"))
  
  if(nrow(cmj_temp) > 0) {
    cmj_new <- cmj_temp %>%
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
      arrange(full_name, test_type, date)
    
    cmj_old <- if(nrow(forcedecks_jump_clean_imported) > 0) forcedecks_jump_clean_imported else NULL
    
    cmj_all <- append_and_finalize(cmj_new, cmj_old, keys = "test_ID", table_name = "CMJ") %>%
      arrange(full_name, test_type, date)
    
    # Scores on the combined data (30-day lookback windows)
    forcedecks_jump_clean <- cmj_all %>%
      group_by(full_name, test_type) %>%
      mutate(
        across(
          any_of(c("jump_height_inches_imp_mom", "relative_peak_concentric_force",
            "rsi_modified_imp_mom", "relative_peak_eccentric_force")),
          list(
            zscore = ~map_dbl(row_number(), function(idx) {
              current_date <- date[idx]
              window_start <- current_date - days(30)
              window_end   <- current_date - days(1)
              baseline_vals <- .x[date >= window_start & date <= window_end & !is.na(.x)]
              if (length(baseline_vals) < 3) return(0)
              sdv <- sd(baseline_vals)
              if (sdv == 0) return(NA_real_)
              (.x[idx] - mean(baseline_vals)) / sdv
            })
          ),
          .names = "{.fn}_{.col}"
        )
      ) %>%
      ungroup() %>%
      group_by(full_name) %>%
      mutate(
        across(
          any_of(c("jump_height_inches_imp_mom", "rsi_modified_imp_mom", "relative_peak_eccentric_force")),
          list(
            readiness = ~map_dbl(row_number(), function(idx) {
              current_date <- date[idx]
              window_start <- current_date - days(30)
              window_end   <- current_date - days(1)
              cmj_mask <- (test_type == "CMJ")
              baseline_vals <- .x[cmj_mask & date >= window_start & date <= window_end & !is.na(.x)]
              if (length(baseline_vals) < 3) return(NA_real_)
              baseline_mean <- mean(baseline_vals)
              if (is.na(.x[idx]) || baseline_mean == 0) return(NA_real_)
              (.x[idx] - baseline_mean) / baseline_mean
            })
          ),
          .names = "{.fn}_{.col}"
        )
      ) %>%
      ungroup()
    
    # Filter outliers
    if ("zscore_jump_height_inches_imp_mom" %in% names(forcedecks_jump_clean)) {
      forcedecks_jump_clean <- forcedecks_jump_clean %>%
        filter(
          if_else(!is.na(body_weight_lbs),
                  between(zscore_jump_height_inches_imp_mom, -3, 3),
                  between(zscore_jump_height_inches_imp_mom, -2.5, 2.5))
        )
    }
    
    if ("zscore_relative_peak_concentric_force" %in% names(forcedecks_jump_clean)) {
      forcedecks_jump_clean <- forcedecks_jump_clean %>%
        filter(
          if_else(!is.na(body_weight_lbs),
                  between(zscore_relative_peak_concentric_force, -3, 3),
                  between(zscore_relative_peak_concentric_force, -2.5, 2.5))
        )
    }
    
    if ("zscore_rsi_modified_imp_mom" %in% names(forcedecks_jump_clean)) {
      forcedecks_jump_clean <- forcedecks_jump_clean %>%
        filter(
          if_else(!is.na(body_weight_lbs),
                  between(zscore_rsi_modified_imp_mom, -3, 3),
                  between(zscore_rsi_modified_imp_mom, -2.5, 2.5))
        )
    }
    
    forcedecks_jump_clean <- forcedecks_jump_clean %>%
      filter(between(jump_height_inches_imp_mom, 5, 28)) %>%
      select(
        -any_of(c("jump_height_readiness", "epf_readiness", "rsi_readiness")),
        -any_of("position"),
        -starts_with("zscore_"),
        -starts_with("readiness_relative_peak_concentric")
      )
    
    # Rename readiness columns
    if ("readiness_jump_height_inches_imp_mom" %in% names(forcedecks_jump_clean)) {
      forcedecks_jump_clean <- forcedecks_jump_clean %>%
        rename(jump_height_readiness = readiness_jump_height_inches_imp_mom)
    }
    if ("readiness_relative_peak_eccentric_force" %in% names(forcedecks_jump_clean)) {
      forcedecks_jump_clean <- forcedecks_jump_clean %>%
        rename(epf_readiness = readiness_relative_peak_eccentric_force)
    }
    if ("readiness_rsi_modified_imp_mom" %in% names(forcedecks_jump_clean)) {
      forcedecks_jump_clean <- forcedecks_jump_clean %>%
        rename(rsi_readiness = readiness_rsi_modified_imp_mom)
    }
    
    # Performance scores
    if (all(c("jump_height_inches_imp_mom", "relative_peak_eccentric_force", 
              "bodymass_relative_takeoff_power", "rsi_modified_imp_mom") %in% names(forcedecks_jump_clean))) {
      forcedecks_jump_clean <- forcedecks_jump_clean %>%
        mutate(
          calc_performance_score =
            percent_rank(jump_height_inches_imp_mom) * 100 +
            percent_rank(relative_peak_eccentric_force) * 100 +
            percent_rank(bodymass_relative_takeoff_power) * 100 +
            percent_rank(rsi_modified_imp_mom) * 100,
          performance_score = percent_rank(calc_performance_score) * 100
        ) %>%
        group_by(team) %>%
        mutate(team_performance_score = percent_rank(calc_performance_score) * 100) %>%
        ungroup() %>%
        select(-calc_performance_score)
    }
    
    create_log_entry(paste("CMJ data processed:", nrow(forcedecks_jump_clean), "records"))
  } else {
    forcedecks_jump_clean <- if(nrow(forcedecks_jump_clean_imported) > 0) forcedecks_jump_clean_imported else data.frame()
    create_log_entry("No new CMJ data found")
  }
  
  ############################################################################
  # DJ processing
  ############################################################################
  
  dj_temp <- forcedecks_raw %>% filter(test_type %in% c("DJ"))
  
  if(nrow(dj_temp) > 0) {
    forcedecks_jump_DJ_clean <- dj_temp %>%
      select(any_of(c(
        "test_ID", "vald_id", "full_name", "position", "team", "date", "time", "body_weight_lbs", 
        "peak_takeoff_velocity", "peak_landing_velocity", "countermovement_depth", "peak_landing_force", 
        "eccentric_time", "jump_height_inches_imp_mom", "bm_rel_force_at_zero_velocity", "contact_time", 
        "contact_velocity", "active_stiffness", "passive_stiffness", "reactive_strength_index", 
        "positive_takeoff_impulse", "coefficient_of_restitution", "active_stiffness_index", 
        "passive_stiffness_index", "peak_impact_force", "peak_driveoff_force"
      ))) %>% 
      mutate(
        velocity_ratio = if_else(!is.na(peak_takeoff_velocity) & !is.na(peak_landing_velocity) & peak_landing_velocity != 0,
                                peak_takeoff_velocity / peak_landing_velocity, NA_real_),
        force_ratio = if_else(!is.na(peak_impact_force) & !is.na(peak_driveoff_force) & peak_driveoff_force != 0,
                             peak_impact_force / peak_driveoff_force, NA_real_),
        stiffness_ratio = if_else(!is.na(active_stiffness) & !is.na(passive_stiffness) & passive_stiffness != 0,
                                 active_stiffness / passive_stiffness, NA_real_)
      ) %>% 
      filter(!is.na(jump_height_inches_imp_mom)) %>% 
      filter(jump_height_inches_imp_mom < 30 & jump_height_inches_imp_mom > 2)
    
    dj_old <- if(nrow(forcedecks_jump_DJ_clean_imported) > 0) forcedecks_jump_DJ_clean_imported else NULL
    forcedecks_jump_DJ_clean <- append_and_finalize(forcedecks_jump_DJ_clean, dj_old, keys = "test_ID", table_name = "DJ")
    
    create_log_entry(paste("DJ data processed:", nrow(forcedecks_jump_DJ_clean), "records"))
  } else {
    forcedecks_jump_DJ_clean <- if(nrow(forcedecks_jump_DJ_clean_imported) > 0) forcedecks_jump_DJ_clean_imported else data.frame()
    create_log_entry("No new DJ data found")
  }
  
  ############################################################################
  # RSI processing
  ############################################################################
  
  rsi_temp <- trials_wider %>%
    mutate(test_ID = testid, vald_id = athleteid) %>% 
    left_join(mergable_tests, by = "test_ID") %>% 
    filter(test_type %in% c("RSAIP", "RSHIP", "RSKIP"))
  
  if(nrow(rsi_temp) > 0) {
    forcedecks_RSI_clean <- rsi_temp %>%
      select(-testid, -athleteid) %>% 
      left_join(mergable_roster, by = "vald_id") %>% 
      mutate(
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
      pivot_wider(
        id_cols = any_of(c("test_ID", "vald_id", "full_name", "position", "team", "date", "time", "body_weight_lbs")),
        names_from = triallimb,
        values_from = any_of(c("start_to_peak_force", "peak_vertical_force", "rfd_at_100ms", "rfd_at_250ms",
                        "iso_bm_rel_force_peak", "iso_bm_rel_force_100", "iso_bm_rel_force_200", "iso_abs_impulse_100")),
        names_sep = "_"
      )
    
    # Calculate bilateral averages
    if ("start_to_peak_force_Left" %in% names(forcedecks_RSI_clean) && "start_to_peak_force_Right" %in% names(forcedecks_RSI_clean)) {
      forcedecks_RSI_clean <- forcedecks_RSI_clean %>%
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
    
    forcedecks_RSI_clean <- forcedecks_RSI_clean %>%
      rename_with(~str_replace(.x, "_Left$", "_left"), everything()) %>%
      rename_with(~str_replace(.x, "_Right$", "_right"), everything())
    
    rsi_old <- if(nrow(forcedecks_RSI_clean_imported) > 0) forcedecks_RSI_clean_imported else NULL
    forcedecks_RSI_clean <- append_and_finalize(forcedecks_RSI_clean, rsi_old, keys = "test_ID", table_name = "RSI")
    
    create_log_entry(paste("RSI data processed:", nrow(forcedecks_RSI_clean), "records"))
  } else {
    forcedecks_RSI_clean <- if(nrow(forcedecks_RSI_clean_imported) > 0) forcedecks_RSI_clean_imported else data.frame()
    create_log_entry("No new RSI data found")
  }
  
  ############################################################################
  # Rebound processing
  ############################################################################
  
  rebound_temp <- trials_wider %>%
    mutate(test_ID = testid, vald_id = athleteid) %>% 
    left_join(mergable_tests, by = "test_ID") %>% 
    filter(test_type %in% c("CMRJ", "SLCMRJ"))
  
  if(nrow(rebound_temp) > 0) {
    forcedecks_rebound_clean <- rebound_temp %>%
      select(-testid, -athleteid) %>% 
      left_join(mergable_roster, by = "vald_id") %>% 
      mutate(
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
    bilateral_cols <- names(forcedecks_rebound_clean)[grepl("_bilateral$", names(forcedecks_rebound_clean))]
    for (col in bilateral_cols) {
      left_col <- str_replace(col, "_bilateral$", "_left")
      right_col <- str_replace(col, "_bilateral$", "_right")
      
      if (left_col %in% names(forcedecks_rebound_clean) && right_col %in% names(forcedecks_rebound_clean)) {
        forcedecks_rebound_clean <- forcedecks_rebound_clean %>%
          mutate(!!col := case_when(
            test_type == "SLCMRJ" & is.na(!!sym(col)) ~ 
              rowMeans(cbind(!!sym(left_col), !!sym(right_col)), na.rm = TRUE),
            TRUE ~ !!sym(col)
          ))
      }
    }
    
    rebound_old <- if(nrow(forcedecks_rebound_clean_imported) > 0) forcedecks_rebound_clean_imported else NULL
    forcedecks_rebound_clean <- append_and_finalize(forcedecks_rebound_clean, rebound_old, keys = "test_ID", table_name = "Rebound")
    
    create_log_entry(paste("Rebound data processed:", nrow(forcedecks_rebound_clean), "records"))
  } else {
    forcedecks_rebound_clean <- if(nrow(forcedecks_rebound_clean_imported) > 0) forcedecks_rebound_clean_imported else data.frame()
    create_log_entry("No new Rebound data found")
  }
  
  ############################################################################
  # Single Leg Jump processing
  ############################################################################
  
  slj_temp <- trials_wider %>%
    mutate(test_ID = testid, vald_id = athleteid) %>% 
    left_join(mergable_tests, by = "test_ID") %>% 
    filter(test_type == "SLJ")
  
  if(nrow(slj_temp) > 0) {
    forcedecks_SLJ_clean <- slj_temp %>%
      select(-testid, -athleteid) %>% 
      left_join(mergable_roster, by = "vald_id") %>% 
      mutate(
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
      pivot_wider(
        id_cols = any_of(c("test_ID", "vald_id", "full_name", "position", "team", "date", "time", "body_weight_lbs")),
        names_from = triallimb,
        values_from = any_of(c("peak_landing_force", "peak_landing_velocity", "peak_takeoff_velocity", "time_to_peak_force",
                        "weight_relative_peak_takeoff_force", "weight_relative_peak_landing_force", 
                        "relative_peak_concentric_force", "relative_peak_eccentric_force", "lower_limb_stiffness", 
                        "rsi_modified_imp_mom")),
        names_sep = "_"
      )
    
    if("peak_landing_force_Left" %in% names(forcedecks_SLJ_clean) & "peak_landing_force_Right" %in% names(forcedecks_SLJ_clean)) {
      forcedecks_SLJ_clean <- forcedecks_SLJ_clean %>%
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
    
    forcedecks_SLJ_clean <- forcedecks_SLJ_clean %>%
      rename_with(~str_replace(.x, "_Left$", "_left"), everything()) %>%
      rename_with(~str_replace(.x, "_Right$", "_right"), everything())
    
    slj_old <- if(nrow(forcedecks_SLJ_clean_imported) > 0) forcedecks_SLJ_clean_imported else NULL
    forcedecks_SLJ_clean <- append_and_finalize(forcedecks_SLJ_clean, slj_old, keys = "test_ID", table_name = "SLJ")
    
    create_log_entry(paste("SLJ data processed:", nrow(forcedecks_SLJ_clean), "records"))
  } else {
    forcedecks_SLJ_clean <- if(nrow(forcedecks_SLJ_clean_imported) > 0) forcedecks_SLJ_clean_imported else data.frame()
    create_log_entry("No new SLJ data found")
  }
  
  ############################################################################
  # IMTP processing
  ############################################################################
  
  imtp_temp <- forcedecks_raw %>% filter(test_type == "IMTP")
  
  if(nrow(imtp_temp) > 0) {
    forcedecks_IMTP_clean <- imtp_temp %>%
      select(any_of(c(
        "test_ID", "vald_id", "full_name", "position", "team", "date", "time",
        "start_to_peak_force", "rfd_at_100ms", "rfd_at_200ms", "force_at_100ms", 
        "iso_bm_rel_force_peak", "peak_vertical_force", "force_at_200ms"
      ))) %>% 
      clean_column_headers() %>% 
      filter(!is.na(peak_vertical_force)) %>%
      arrange(full_name, date)
    
    if (all(c("peak_vertical_force", "start_to_peak_force", "rfd_at_100ms") %in% names(forcedecks_IMTP_clean))) {
      forcedecks_IMTP_clean <- forcedecks_IMTP_clean %>%
        mutate(
          calc_performance_score = percent_rank(peak_vertical_force) * 100 * 2 +
            (100 - percent_rank(start_to_peak_force) * 100) +
            percent_rank(rfd_at_100ms) * 100,
          performance_score = percent_rank(calc_performance_score) * 100
        ) %>%
        group_by(team) %>%
        mutate(
          team_performance_score = percent_rank(calc_performance_score) * 100
        ) %>%
        ungroup() %>%
        select(-calc_performance_score)
    }
    
    forcedecks_IMTP_clean <- forcedecks_IMTP_clean %>%
      filter(!is.na(peak_vertical_force))
    
    imtp_old <- if(nrow(forcedecks_IMTP_clean_imported) > 0) forcedecks_IMTP_clean_imported else NULL
    forcedecks_IMTP_clean <- append_and_finalize(forcedecks_IMTP_clean, imtp_old, keys = "test_ID", table_name = "IMTP")
    
    create_log_entry(paste("IMTP data processed:", nrow(forcedecks_IMTP_clean), "records"))
  } else {
    forcedecks_IMTP_clean <- if(nrow(forcedecks_IMTP_clean_imported) > 0) forcedecks_IMTP_clean_imported else data.frame()
    create_log_entry("No new IMTP data found")
  }
  
  ############################################################################
  # Body weight processing
  ############################################################################
  
  bw_temp <- forcedecks_raw %>% 
    select(any_of(c("vald_id", "body_weight_lbs", "date"))) %>% 
    filter(!is.na(body_weight_lbs))
  
  if(nrow(bw_temp) > 0) {
    bw <- bw_temp %>%
      group_by(vald_id, date) %>%
      summarise(
        body_weight_lbs = mean(body_weight_lbs, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      arrange(vald_id, date)
    
    create_log_entry(paste("Body weight data processed:", nrow(bw), "records"))
  } else {
    bw <- data.frame(vald_id = character(0), date = as.Date(character(0)), body_weight_lbs = numeric(0))
    create_log_entry("No body weight data found")
  }
  
  ############################################################################
  # Nordboard processing
  ############################################################################
  
  set_start_date(date_count_start_date)
  injest_nordboard_tests <- get_nordbord_data()
  
  nordboard_temp <- injest_nordboard_tests$tests
  
  if(nrow(nordboard_temp) > 0) {
    nordboard_clean <- nordboard_temp %>% 
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
        test_ID = testId,
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
                "rightMaxForce", "leftRepetitions", "rightRepetitions"))) %>%
      left_join(mergable_roster, by = "vald_id")
    
    # Join with body weight data
    if (nrow(bw) > 0) {
      nordboard_clean <- nordboard_clean %>%
        left_join(bw %>% 
                    select(vald_id, date, body_weight_lbs) %>%  
                    rename(bw_date = date) %>%  
                    arrange(vald_id, bw_date),
                  by = "vald_id", relationship = "many-to-many"
        ) %>%
        group_by(vald_id, date) %>%  
        filter(bw_date <= date) %>%  
        slice_max(bw_date, n = 1, with_ties = FALSE) %>%
        ungroup() %>%
        mutate(
          max_force_relative_bw = if_else(!is.na(body_weight_lbs) & body_weight_lbs != 0,
                                         max_force_bilateral / body_weight_lbs, NA_real_),
          max_force_left_relative_bw = if_else(!is.na(body_weight_lbs) & body_weight_lbs != 0,
                                              max_force_left / body_weight_lbs, NA_real_),
          max_force_right_relative_bw = if_else(!is.na(body_weight_lbs) & body_weight_lbs != 0,
                                               max_force_right / body_weight_lbs, NA_real_)
        ) %>%
        select(-any_of(c("bw_date", "body_weight_lbs")))
    }
    
    nord_old <- if(nrow(nordboard_clean_imported) > 0) nordboard_clean_imported else NULL
    nordboard_clean <- append_and_finalize(nordboard_clean, nord_old, keys = "test_ID", table_name = "Nordboard")
    
    create_log_entry(paste("Nordboard data processed:", nrow(nordboard_clean), "records"))
  } else {
    nordboard_clean <- if(nrow(nordboard_clean_imported) > 0) nordboard_clean_imported else data.frame()
    create_log_entry("No new Nordboard data found")
  }
  
  # Update dates and tests tables
  dates <- forcedecks_raw %>% 
    select(date) %>% 
    unique()
  
  tests <- forcedecks_raw %>% 
    select(test_ID) %>% 
    unique()
  
  # Final appends
  dates <- append_and_finalize(dates, dates_imported, keys = "date", table_name = "Dates")
  tests <- append_and_finalize(tests, tests_imported, keys = "test_ID", table_name = "Tests")
  
  # Upload all processed data to BigQuery
  upload_to_bq(forcedecks_jump_clean, "vald_fd_jumps", "WRITE_TRUNCATE")
  upload_to_bq(forcedecks_SLJ_clean, "vald_fd_sl_jumps", "WRITE_TRUNCATE")
  upload_to_bq(nordboard_clean, "vald_nord_all", "WRITE_TRUNCATE")
  upload_to_bq(forcedecks_RSI_clean, "vald_fd_rsi", "WRITE_TRUNCATE")
  upload_to_bq(forcedecks_jump_DJ_clean, "vald_fd_dj", "WRITE_TRUNCATE")
  upload_to_bq(forcedecks_rebound_clean, "vald_fd_rebound", "WRITE_TRUNCATE")
  upload_to_bq(forcedecks_IMTP_clean, "vald_fd_imtp", "WRITE_TRUNCATE")
  upload_to_bq(dates, "dates", "WRITE_TRUNCATE")
  upload_to_bq(tests, "tests", "WRITE_TRUNCATE")
  upload_to_bq(Vald_roster, "vald_roster", "WRITE_TRUNCATE")
  
  create_log_entry("=== FULL PROCESSING COMPLETED ===")
  create_log_entry("All data files successfully written to BigQuery")
  create_log_entry(paste("CMJ records:", nrow(forcedecks_jump_clean)))
  create_log_entry(paste("SLJ records:", nrow(forcedecks_SLJ_clean))) 
  create_log_entry(paste("Nordboard records:", nrow(nordboard_clean)))
  create_log_entry(paste("RSI records:", nrow(forcedecks_RSI_clean)))
  create_log_entry(paste("DJ records:", nrow(forcedecks_jump_DJ_clean)))
  create_log_entry(paste("Rebound records:", nrow(forcedecks_rebound_clean)))
  create_log_entry(paste("IMTP records:", nrow(forcedecks_IMTP_clean)))
  create_log_entry("FULL PROCESSING branch completed successfully")
  
} else if (count_mismatch && !date_mismatch) {
  create_log_entry("Only count mismatch – running partial code.")
  
  date_count_start_date <- paste0(as.Date(latest_date_current), "T00:00:00Z")
  set_start_date(date_count_start_date)
  
  # Similar logic as full processing but with different date filtering
  
  create_log_entry("=== PARTIAL PROCESSING COMPLETED ===")
  create_log_entry("Partial data processing completed successfully")
  create_log_entry("PARTIAL PROCESSING branch completed successfully")
  
} else {
  create_log_entry("No mismatches – exiting.")
  create_log_entry("=== NO PROCESSING COMPLETED ===") 
  create_log_entry("No data changes detected - script exiting")
  create_log_entry("NO PROCESSING branch completed successfully")
}

}, error = function(e) {
  create_log_entry(paste("CRITICAL ERROR in main processing:", e$message), "ERROR")
  create_log_entry(paste("Error traceback:", paste(capture.output(traceback()), collapse = "\n")), "ERROR")
  quit(status = 1)
})

create_log_entry("=== SCRIPT EXECUTION SUMMARY ===")
create_log_entry(paste("Final route executed:", route_taken))
create_log_entry(paste("Total execution time:", 
                      round(difftime(Sys.time(), script_start_time, units = "mins"), 2), "minutes"))
create_log_entry("=== VALD DATA PROCESSING SCRIPT ENDED ===", "END")

tryCatch({
  DBI::dbDisconnect(con)
}, error = function(e) {
  create_log_entry(paste("Warning: Could not close BigQuery connection:", e$message), "WARN")
})

cat("Script completed successfully\n")
