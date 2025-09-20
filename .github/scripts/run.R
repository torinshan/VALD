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

# Authenticate to BigQuery
tryCatch({
  creds_file <- Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS")
  if (nchar(creds_file) > 0 && file.exists(creds_file)) {
    bigrquery::bq_auth(path = creds_file)
  } else {
    bigrquery::bq_auth()
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
        !is.na(`Group 3`) & sport == "Football" & 
          `Group 3` %in% c("WR", "RB", "TE", "QB", "DB", "LB", "DL", "OL") ~ `Group 3`,
        !is.na(`Group 2`) & sport == "Football" & 
          `Group 2` %in% c("WR", "RB", "TE", "QB", "DB", "LB", "DL", "OL") ~ `Group 2`,
        !is.na(`Group 1`) & sport == "Football" & 
          `Group 1` %in% c("WR", "RB", "TE", "QB", "DB", "LB", "DL", "OL") ~ `Group 1`,
        sport != "Football" & !is.na(`Group 1`) ~ `Group 1`,
        TRUE ~ "Unspecified"
      ),
      position_class = case_when(
        sport == "Football" & position %in% c("TE", "LB") ~ "Combo",
        sport == "Football" & position %in% c("DB", "WR") ~ "Skill", 
        sport == "Football" & position %in% c("DL", "OL") ~ "Line",
        sport == "Football" & position == "RB" ~ "Offense",
        sport == "Football" & position == "QB" ~ "Offense",
        sport == "Football" & (`Group 1` == "Skill" | `Group 2` == "Skill" | `Group 3` == "Skill") ~ "Skill",
        sport == "Football" & (`Group 1` == "Combo" | `Group 2` == "Combo" | `Group 3` == "Combo") ~ "Combo",
        sport == "Football" & (`Group 1` == "Line" | `Group 2` == "Line" | `Group 3` == "Line") ~ "Line",
        sport == "Football" & (`Group 1` == "SPEC" | `Group 2` == "SPEC" | `Group 3` == "SPEC") ~ "Special Teams",
        TRUE ~ NA_character_
      ),
      unit = case_when(
        sport == "Football" & (`Group 1` == "Offense" | `Group 2` == "Offense" | `Group 3` == "Offense") ~ "Offense",
        sport == "Football" & (`Group 1` == "Defense" | `Group 2` == "Defense" | `Group 3` == "Defense") ~ "Defense", 
        sport == "Football" & position %in% c("QB", "RB", "WR", "TE", "OL") ~ "Offense",
        sport == "Football" & position %in% c("DB", "LB", "DL") ~ "Defense",
        TRUE ~ NA_character_
      ),
      athlete_status = case_when(
        `Group 1` %in% c("Staff", "Archive") ~ "Staff/Archive",
        `Group 1` == "Alum" ~ "Alumni", 
        team == "Uncategorised" ~ "Research/Intern",
        TRUE ~ "Active Athlete"
      ),
      gender = case_when(
        str_detect(team, "Women|W's") ~ "Women",
        str_detect(team, "Men") ~ "Men", 
        `Group 1` %in% c("Women's Soccer", "Men's Soccer") ~ str_extract(`Group 1`, "Women's|Men's"),
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
  
  date_count_start_date <- paste0(latest_date_current + days(1), "T00:00:00Z")
  set_start_date(date_count_start_date)
  
  forcedecks_jump_clean_imported <- read_bq_table("vald_fd_jumps")
  forcedecks_SLJ_clean_imported <- read_bq_table("vald_fd_sl_jumps")
  nordboard_clean_imported <- read_bq_table("vald_nord_all")
  forcedecks_RSI_clean_imported <- read_bq_table("vald_fd_rsi")
  forcedecks_jump_DJ_clean_imported <- read_bq_table("vald_fd_dj")
  forcedecks_rebound_clean_imported <- read_bq_table("vald_fd_rebound")
  forcedecks_IMTP_clean_imported <- read_bq_table("vald_fd_imtp")
  dates_imported <- read_bq_table("dates")
  tests_imported <- read_bq_table("tests")
  
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
  
  Vald_roster_raw <- read_bq_table("vald_roster")
  
  if (nrow(Vald_roster_raw) > 0) {
    create_log_entry("Reading roster data from BigQuery")
    
    if ("vald-id" %in% names(Vald_roster_raw)) {
      Vald_roster_raw <- Vald_roster_raw %>% mutate(vald_id = `vald-id`)
    } else if ("vald_id" %in% names(Vald_roster_raw)) {
      # Already has vald_id column
    } else {
      create_log_entry("Warning: No vald_id or vald-id column found in roster data", "WARN")
    }
    
    Vald_roster <- Vald_roster_raw %>% 
      process_vald_roster_detailed() %>% 
      select(any_of(c("vald_id", "team", "position_class", "unit", "position", "athlete_status"))) %>% 
      filter(athlete_status == "Active Athlete")
      
    create_log_entry(paste("Processed", nrow(Vald_roster), "active athletes from roster"))
    
  } else {
    create_log_entry("Roster not found in BigQuery, trying GitHub fallback", "WARN")
    
    tryCatch({
      roster_url <- "https://raw.githubusercontent.com/torinshan/VALD/main/.github/vald_roster.csv"
      Vald_roster_raw <- read_csv(roster_url, show_col_types = FALSE)
      
      if (nrow(Vald_roster_raw) > 0) {
        create_log_entry("Successfully read roster from GitHub")
        
        if ("vald-id" %in% names(Vald_roster_raw)) {
          Vald_roster_raw <- Vald_roster_raw %>% mutate(vald_id = `vald-id`)
        } else if ("vald_id" %in% names(Vald_roster_raw)) {
          # Already has vald_id column
        }
        
        Vald_roster <- Vald_roster_raw %>% 
          process_vald_roster_detailed() %>% 
          select(any_of(c("vald_id", "team", "position_class", "unit", "position", "athlete_status"))) %>% 
          filter(athlete_status == "Active Athlete")
          
        create_log_entry(paste("Processed", nrow(Vald_roster), "active athletes from GitHub roster"))
      } else {
        Vald_roster <- data.frame()
        create_log_entry("GitHub roster file was empty", "WARN")
      }
    }, error = function(e) {
      create_log_entry(paste("Failed to read roster from GitHub:", e$message), "WARN")
      Vald_roster <- data.frame(
        vald_id = character(0),
        team = character(0),
        position_class = character(0),
        unit = character(0),
        position = character(0),
        athlete_status = character(0)
      )
    })
  }
  
  # Continue with rest of processing logic...
  # [Rest of the original processing code continues here]
  
  create_log_entry("=== FULL PROCESSING COMPLETED ===")
  
} else if (count_mismatch && !date_mismatch) {
  create_log_entry("Only count mismatch – running partial code.")
  create_log_entry("=== PARTIAL PROCESSING COMPLETED ===")
  
} else {
  create_log_entry("No mismatches – exiting.")
  create_log_entry("=== NO PROCESSING COMPLETED ===") 
}

}, error = function(e) {
  create_log_entry(paste("CRITICAL ERROR in main processing:", e$message), "ERROR")
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
