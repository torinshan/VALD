#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(readxl)
  library(dplyr)
  library(stringr)
  library(tibble)
  library(janitor)
  library(bigrquery)
  library(DBI)
})

# Env vars
PROJECT   <- Sys.getenv("GCP_PROJECT", unset = NA)
DATASET   <- Sys.getenv("BQ_DATASET",  unset = NA)
LOCATION  <- Sys.getenv("BQ_LOCATION", unset = "US")
TABLE     <- Sys.getenv("ROSTER_TABLE", unset = "roster_mapping")
XL_PATH   <- Sys.getenv("ROSTER_LOCAL_FILE", unset = ".github/Sac State Roster - Summer 2025.xlsx")
XL_SHEET  <- Sys.getenv("ROSTER_SHEET",     unset = "Master")

if (is.na(PROJECT) || is.na(DATASET)) {
  stop("GCP_PROJECT and BQ_DATASET environment variables are required", call. = FALSE)
}

message("Roster ingest starting…")
message("  Project:  ", PROJECT)
message("  Dataset:  ", DATASET)
message("  Table:    ", TABLE)
message("  Location: ", LOCATION)
message("  File:     ", XL_PATH)
message("  Sheet:    ", XL_SHEET)

if (!file.exists(XL_PATH)) {
  stop(sprintf("Roster file not found: %s", XL_PATH), call. = FALSE)
}

# Read and clean column names to snake_case
raw <- readxl::read_xlsx(path = XL_PATH, sheet = XL_SHEET)
df  <- janitor::clean_names(raw)

# Expect cleaned columns like:
# team, roster_name, simple_name, offical_id, initial_name, first_name, last_name,
# vald_name, kinexon_name, perch_name, data_hub_name, position, position_class,
# offical_id2, starters

# Build mapping with strict non-empty values
map <- df %>%
  mutate(
    offical_id = coalesce(.data$offical_id, .data$offical_id2),
    offical_id = as.character(offical_id),
    offical_id = str_trim(offical_id),
    vald_name  = as.character(.data$vald_name),
    vald_name  = str_trim(vald_name)
  ) %>%
  transmute(offical_id, vald_name) %>%
  filter(!is.na(offical_id), offical_id != "",
         !is.na(vald_name),  vald_name  != "") %>%
  distinct()

if (nrow(map) == 0) {
  stop("No valid rows found for roster mapping (need offical_id and vald_name)", call. = FALSE)
}

# Rename to match required BigQuery schema: offical_id and 'Vald Name'
names(map) <- c("offical_id", "Vald Name")

message(sprintf("Prepared %d unique roster mapping rows", nrow(map)))

# Ensure dataset exists (using ADC from runner)
bigrquery::bq_auth(path = Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS", unset = ""))

ds <- bigrquery::bq_dataset(PROJECT, DATASET)
if (!bigrquery::bq_dataset_exists(ds)) {
  message("Creating dataset: ", sprintf("%s.%s", PROJECT, DATASET))
  bigrquery::bq_dataset_create(ds, location = LOCATION)
}

# Upload table (truncate/replace)
tbl <- bigrquery::bq_table(ds, TABLE)
bigrquery::bq_table_upload(
  tbl,
  map,
  create_disposition = "CREATE_IF_NEEDED",
  write_disposition  = "WRITE_TRUNCATE"
)

message("Roster mapping uploaded to BigQuery: ", sprintf("%s.%s.%s", PROJECT, DATASET, TABLE))
