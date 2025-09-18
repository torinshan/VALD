#!/usr/bin/env Rscript

# Load required libraries
suppressPackageStartupMessages({
  library(bigrquery)
  library(DBI)
  library(tidyverse)
  library(valdr)       # ensure installed via CRAN or GitHub in the workflow
  library(data.table)
  library(hms)
  library(lubridate)
})

################################################################################
# BigQuery Configuration and Setup
################################################################################

# Helper to require env vars (fail fast if missing)
need <- function(x) {
  v <- Sys.getenv(x, ""); if (v == "") stop(x, " is not set", call. = FALSE); v
}

# Project/dataset (keep in sync with workflow env)
project <- Sys.getenv("GCP_PROJECT", "sac-vald-hub")
dataset <- Sys.getenv("BQ_DATASET", "analytics")   # <— unified with workflow

# Authenticate to BigQuery using Application Default Credentials (from OIDC)
bigrquery::bq_auth()

# Create BigQuery connection
con <- DBI::dbConnect(bigrquery::bigquery(), project = project)

# Ensure dataset exists
ds <- bq_dataset(project, dataset)
if (!bq_dataset_exists(ds)) {
  bq_dataset_create(ds)
}

################################################################################
# Logging Functions
################################################################################

create_log_entry <- function(message, level = "INFO") {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  log_message <- paste0("[", timestamp, "] [", level, "] ", message)
  cat(log_message, "\n")

  # Write to BigQuery log table
  log_df <- data.frame(
    timestamp = as.POSIXct(timestamp),
    level = level,
    message = message,
    run_id = Sys.getenv("GITHUB_RUN_ID", "manual"),
    repository = Sys.getenv("GITHUB_REPOSITORY", "unknown"),
    stringsAsFactors = FALSE
  )

  log_tbl <- bq_table(ds, "vald_processing_log")
  if (!bq_table_exists(log_tbl)) {
    bq_table_create(log_tbl, fields = log_df)
  }

  tryCatch({
    bq_table_upload(log_tbl, log_df, write_disposition = "WRITE_APPEND")
  }, error = function(e) {
    cat("Failed to write log to BigQuery:", e$message, "\n")
  })

  return(log_message)
}

script_start_time <- Sys.time()

# Script start logging
create_log_entry("=== VALD DATA PROCESSING SCRIPT STARTED ===", "START")
create_log_entry(paste("Script execution user:", Sys.getenv("GITHUB_ACTOR", Sys.getenv("USERNAME", "unknown"))))
create_log_entry(paste("BigQuery project:", project))
create_log_entry(paste("BigQuery dataset:", dataset))

################################################################################
# BigQuery Helper Functions
################################################################################

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

read_bq_table <- function(table_name) {
  query <- sprintf("SELECT * FROM `%s.%s.%s`", project, dataset, table_name)
  return(safe_bq_query(query, paste("reading", table_name)))
}

################################################################################
# VALD API Setup (secrets passed via workflow env)
################################################################################

client_id     <- need("VALD_CLIENT_ID")
client_secret <- need("VALD_CLIENT_SECRET")
tenant_id     <- need("VALD_TENANT_ID")
region        <- Sys.getenv("VALD_REGION", "use")

set_credentials(client_id, client_secret, tenant_id, region)
set_start_date("2024-01-01T00:00:00Z")

################################################################################
# Get Current Data State from BigQuery
################################################################################

current_dates <- read_bq_table("dates")
if (nrow(current_dates) > 0) {
  current_dates <- current_dates %>%
    select(date) %>%
    unique() %>%
    mutate(date = as.Date(date))
} else {
  current_dates <- data.frame(date = as.Date(character(0)))
}

test_ids <- read_bq_table("tests")
if (nrow(test_ids) > 0) {
  test_ids <- test_ids %>% unique()
} else {
  test_ids <- data.frame(test_ID = character(0))
}

if (nrow(current_dates) > 0) {
  latest_date_current <- max(current_dates$date, na.rm = TRUE)
} else {
  latest_date_current <- as.Date("1900-01-01")
}
count_tests_current <- nrow(test_ids)

# Pull all tests from VALD API
all_tests <- get_forcedecks_tests_only(start_date = NULL)

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

all_dates <- all_tests %>% select(date) %>% unique()
all_count <- all_tests %>% select(test_ID) %>% unique()

latest_date_all <- max(all_dates$date, na.rm = TRUE)
count_tests_all <- nrow(all_count)

# Compare current vs all data
date_mismatch  <- latest_date_current != latest_date_all
count_mismatch <- count_tests_current != count_tests_all

create_log_entry("=== CONDITIONAL LOGIC EVALUATION ===")
create_log_entry(paste("Latest date current:", as.character(latest_date_current)))
create_log_entry(paste("Latest date all:", as.character(latest_date_all)))
create_log_entry(paste("Count tests current:", count_tests_current))
create_log_entry(paste("Count tests all:", count_tests_all))
create_log_entry(paste("date_mismatch:", date_mismatch))
create_log_entry(paste("count_mismatch:", count_mismatch))

route_taken <- "NO_PROCESSING"
if (count_mismatch && date_mismatch) {
  route_taken <- "FULL_PROCESSING"
  create_log_entry("ROUTE TAKEN: FULL PROCESSING (both date and count mismatch)", "DECISION")
} else if (count_mismatch && !date_mismatch) {
  route_taken <- "PARTIAL_PROCESSING"
  create_log_entry("ROUTE TAKEN: PARTIAL PROCESSING (count mismatch only)", "DECISION")
} else {
  create_log_entry("ROUTE TAKEN: NO PROCESSING (no mismatches)", "DECISION")
}

################################################################################
# Processing Functions (selected ones you provided)
################################################################################

clean_column_headers <- function(df, remove_index = TRUE) {
  original_names <- names(df)
  if (remove_index) {
    index_cols <- which(original_names %in% c("", "X", "...1", "X1") |
                          grepl("^\\.\\.\\.[0-9]+$", original_names) |
                          grepl("^X[0-9]*$", original_names))
    if (length(index_cols) > 0) df <- df[, -index_cols, drop = FALSE]
  }
  cleaned_names <- names(df) %>%
    gsub("([a-z])([A-Z])", "\\1_\\2", .) %>%
    tolower() %>%
    gsub("[^a-zA-Z0-9_]", "_", .) %>%
    gsub("_{2,}", "_", .) %>%
    gsub("^_|_$", "", .) %>%
    ifelse(grepl("^[0-9]", .), paste0("x_", .), .)
  names(df) <- cleaned_names
  df
}

process_vald_roster_detailed <- function(roster_data) {
  roster_processed <- roster_data %>%
    filter(!is.na(`Category 1`) | !is.na(`Group 1`)) %>%
    mutate(
      team = `Category 1`,
      sport = case_when(
        str_detect(team, "Football") ~ "Football",
        str_detect(team, "Soccer") ~ "Soccer",
        str_detect(team, "Basketball") ~ "Basketball",
        str_detect(team, "Softball") ~ "Softball",
        str_detect(team, "Gymnastics") ~ "Gymnastics",
        str_detect(team, "Track") ~ "Track & Field",
        str_detect(team, "Volleyball") ~ "Volleyball",
        TRUE ~ "Other"
      ),
      position = case_when(
        !is.na(`Group 4`) & sport == "Football" ~ `Group 4`,
        !is.na(`Group 3`) & sport == "Football" & `Group 3` %in% c("WR","RB","TE","QB","DB","LB","DL","OL") ~ `Group 3`,
        !is.na(`Group 2`) & sport == "Football" & `Group 2` %in% c("WR","RB","TE","QB","DB","LB","DL","OL") ~ `Group 2`,
        !is.na(`Group 1`) & sport == "Football" & `Group 1` %in% c("WR","RB","TE","QB","DB","LB","DL","OL") ~ `Group 1`,
        sport != "Football" & !is.na(`Group 1`) ~ `Group 1`,
        TRUE ~ "Unspecified"
      ),
      position_class = case_when(
        sport == "Football" & position %in% c("TE","LB") ~ "Combo",
        sport == "Football" & position %in% c("DB","WR") ~ "Skill",
        sport == "Football" & position %in% c("DL","OL") ~ "Line",
        sport == "Football" & position %in% c("RB","QB") ~ "Offense",
        sport == "Football" & (`Group 1` == "Skill" | `Group 2` == "Skill" | `Group 3` == "Skill") ~ "Skill",
        sport == "Football" & (`Group 1` == "Combo" | `Group 2` == "Combo" | `Group 3` == "Combo") ~ "Combo",
        sport == "Football" & (`Group 1` == "Line" | `Group 2` == "Line" | `Group 3` == "Line") ~ "Line",
        sport == "Football" & (`Group 1` == "SPEC" | `Group 2` == "SPEC" | `Group 3` == "SPEC") ~ "Special Teams",
        TRUE ~ NA_character_
      ),
      unit = case_when(
        sport == "Football" & (`Group 1` == "Offense" | `Group 2` == "Offense" | `Group 3` == "Offense") ~ "Offense",
        sport == "Football" & (`Group 1` == "Defense" | `Group 2` == "Defense" | `Group 3` == "Defense") ~ "Defense",
        sport == "Football" & position %in% c("QB","RB","WR","TE","OL") ~ "Offense",
        sport == "Football" & position %in% c("DB","LB","DL") ~ "Defense",
        TRUE ~ NA_character_
      ),
      athlete_status = case_when(
        `Group 1` %in% c("Staff","Archive") ~ "Staff/Archive",
        `Group 1` == "Alum" ~ "Alumni",
        team == "Uncategorised" ~ "Research/Intern",
        TRUE ~ "Active Athlete"
      ),
      gender = case_when(
        str_detect(team, "Women|W's") ~ "Women",
        str_detect(team, "Men") ~ "Men",
        `Group 1` %in% c("Women's Soccer","Men's Soccer") ~ str_extract(`Group 1`, "Women's|Men's"),
        TRUE ~ "Mixed/Unknown"
      )
    )
  return(roster_processed)
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
  if (rows_removed > 0) create_log_entry(paste("INFO:", table_name, "- Removed", rows_removed, "duplicate rows during append"))
  result
}

################################################################################
# Main Processing Logic (FULL/PARTIAL/NO)
################################################################################

if (route_taken == "FULL_PROCESSING") {
  create_log_entry("Both mismatches found – running full code.")

  date_count_start_date <- paste0(latest_date_current + days(1), "T00:00:00Z")
  set_start_date(date_count_start_date)

  # Import existing data from BigQuery
  forcedecks_jump_clean_imported   <- read_bq_table("vald_fd_jumps")
  forcedecks_SLJ_clean_imported    <- read_bq_table("vald_fd_sl_jumps")
  nordboard_clean_imported         <- read_bq_table("vald_nord_all")
  forcedecks_RSI_clean_imported    <- read_bq_table("vald_fd_rsi")
  forcedecks_jump_DJ_clean_imported<- read_bq_table("vald_fd_dj")
  forcedecks_rebound_clean_imported<- read_bq_table("vald_fd_rebound")
  forcedecks_IMTP_clean_imported   <- read_bq_table("vald_fd_imtp")
  dates_imported                   <- read_bq_table("dates")
  tests_imported                   <- read_bq_table("tests")

  # Get new data from VALD API
  Injest_forcedekcs_data <- get_forcedecks_data()

  profiles    <- setDT(Injest_forcedekcs_data$profiles)
  definitions <- setDT(Injest_forcedekcs_data$result_definitions)
  tests       <- setDT(Injest_forcedekcs_data$tests)
  trials      <- setDT(Injest_forcedekcs_data$trials)

  roster <- profiles %>%
    select(profileId, givenName, familyName) %>%
    mutate(
      full_name = paste(trimws(givenName), trimws(familyName), sep = " "),
      first_name = givenName,
      last_name  = familyName,
      vald_id    = profileId
    ) %>%
    select(-givenName, -familyName, -profileId)

  Vald_roster_raw <- read_bq_table("vald_roster")
  if (nrow(Vald_roster_raw) > 0) {
    Vald_roster <- Vald_roster_raw %>%
      mutate(vald_id = `vald-id`) %>%
      select(-`vald-id`) %>%
      process_vald_roster_detailed() %>%
      select(vald_id, team, position_class, unit, position, athlete_status) %>%
      filter(athlete_status == "Active Athlete")
  } else {
    Vald_roster <- tibble(vald_id=character(), team=character(), position_class=character(), unit=character(), position=character(), athlete_status=character())
    create_log_entry("VALD roster table not found - continuing without roster data", "WARN")
  }

  tests <- tests %>%
    mutate(
      vald_id = profileId,
      test_type = testType,
      test_ID = testId,
      recordedDateUtc_parsed = ymd_hms(recordedDateUtc, tz = "UTC", quiet = TRUE),
      recordedDateUtc_local = if_else(
        recordedDateTimezone %in% c("Pacific Standard Time","Pacific Daylight Time","Pacific Time"),
        with_tz(recordedDateUtc_parsed, "America/Los_Angeles"),
        recordedDateUtc_parsed
      ),
      date = as.Date(recordedDateUtc_local),
      time = hms::as_hms(recordedDateUtc_local)
    ) %>%
    select(-testId, -tenantId, -profileId, -testType, -weight, -analysedDateUtc, -analysedDateOffset, -analysedDateTimezone,
           -recordedDateUtc_parsed, -recordedDateUtc_local, -recordedDateUtc, -recordedDateOffset, -recordedDateTimezone, -recordingId)

  definitions <- definitions %>%
    select(resultId, r_name = resultIdString, metric = resultName, definition = resultDescription,
           unit_of_measure = resultUnit, unit_of_measure_symbol = resultUnitName,
           unit_of_measure_scale = resultUnitScaleFactor, decimal_places = numberOfDecimalPlaces,
           group = resultGroup, asymmetry = supportsAsymmetry, repeatable = isRepeatResult,
           good_trend_direction = trendDirection)

  trials_wider <- trials %>%
    pivot_wider(
      id_cols = c(testId, trialId, athleteId, recordedUTC, recordedTimezone, trialLimb),
      names_from = definition_result,
      values_from = value,
      values_fn = dplyr::first
    ) %>%
    mutate(
      recordedUTC_parsed = ymd_hms(recordedUTC, tz = "UTC", quiet = TRUE),
      recordedUTC_local = if_else(
        recordedTimezone %in% c("Pacific Standard Time","Pacific Daylight Time","Pacific Time"),
        with_tz(recordedUTC_parsed, "America/Los_Angeles"),
        recordedUTC_parsed
      ),
      date = as.Date(recordedUTC_local),
      time = hms::as_hms(recordedUTC_local)
    ) %>%
    select(-recordedUTC_parsed, -recordedUTC_local) %>%
    rename_with(tolower)

  start_col <- which(names(trials_wider) == "start_of_movement")
  if (length(start_col) > 0) {
    trials_wider <- trials_wider %>% mutate(across(all_of(start_col:ncol(.)), as.numeric))
  }

  mergable_trials <- trials_wider %>%
    mutate(test_ID = testid) %>% select(-testid) %>%
    group_by(test_ID) %>%
    summarise(athleteid = first(athleteid), triallimb = first(triallimb), date = first(date), time = first(time),
              across(where(is.numeric), mean, na.rm = TRUE), .groups = 'drop') %>%
    mutate(vald_id = athleteid) %>% select(-athleteid)

  mergable_tests <- tests %>% select(test_ID, test_type)
  mergable_roster <- roster %>% select(-first_name, -last_name) %>% left_join(Vald_roster, by = "vald_id") %>%
    select(-any_of(c("athlete_status","position_class","unit")))

  forcedecks_raw <- mergable_trials %>% left_join(mergable_tests, by = "test_ID") %>% left_join(mergable_roster, by = "vald_id") %>%
    mutate(date = as.Date(date, origin = "1970-01-01"), time = as_hms(time))

  # === CMJ/LCMJ/SJ/ABCMJ ===
  # (unchanged from your logic, trimmed for brevity)
  # ... keep your CMJ, DJ, RSI, Rebound, SLJ, IMTP, BW, Nordboard blocks here ...
  # NOTE: If the valdr function is named get_nordboard_data() (with 'board'), rename below accordingly.

  # Example end writes
  # upload_to_bq(forcedecks_jump_clean,  "vald_fd_jumps",   "WRITE_TRUNCATE")
  # upload_to_bq(forcedecks_SLJ_clean,   "vald_fd_sl_jumps","WRITE_TRUNCATE")
  # upload_to_bq(nordboard_clean,        "vald_nord_all",   "WRITE_TRUNCATE")
  # upload_to_bq(forcedecks_RSI_clean,   "vald_fd_rsi",     "WRITE_TRUNCATE")
  # upload_to_bq(forcedecks_jump_DJ_clean,"vald_fd_dj",     "WRITE_TRUNCATE")
  # upload_to_bq(forcedecks_rebound_clean,"vald_fd_rebound","WRITE_TRUNCATE")
  # upload_to_bq(forcedecks_IMTP_clean,  "vald_fd_imtp",    "WRITE_TRUNCATE")
  # upload_to_bq(dates,                  "dates",           "WRITE_TRUNCATE")
  # upload_to_bq(tests,                  "tests",           "WRITE_TRUNCATE")

  create_log_entry("=== FULL PROCESSING COMPLETED ===")

} else if (route_taken == "PARTIAL_PROCESSING") {
  create_log_entry("Only count mismatch – running partial code.")
  date_count_start_date <- paste0(as.Date(latest_date_current), "T00:00:00Z")
  set_start_date(date_count_start_date)
  create_log_entry("=== PARTIAL PROCESSING COMPLETED ===")

} else {
  create_log_entry("No mismatches – exiting.")
  create_log_entry("=== NO PROCESSING COMPLETED ===")
}

create_log_entry("=== SCRIPT EXECUTION SUMMARY ===")
create_log_entry(paste("Final route executed:", route_taken))
create_log_entry(paste("Total execution time:", round(difftime(Sys.time(), script_start_time, units = "mins"), 2), "minutes"))
create_log_entry("=== VALD DATA PROCESSING SCRIPT ENDED ===", "END")

DBI::dbDisconnect(con)
