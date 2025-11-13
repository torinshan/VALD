#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(readxl)
  library(dplyr)
  library(stringr)
  library(janitor)
  library(bigrquery)
  library(DBI)
})

# Read required environment variables (provided by the workflow)
project      <- Sys.getenv("GCP_PROJECT")
dataset      <- Sys.getenv("BQ_DATASET")
location     <- Sys.getenv("BQ_LOCATION", unset = "US")
table_name   <- Sys.getenv("ROSTER_TABLE", unset = "roster_mapping")
xlsx_path    <- Sys.getenv("ROSTER_LOCAL_FILE", unset = ".github/Sac State Roster - Summer 2025.xlsx")
xlsx_sheet   <- Sys.getenv("ROSTER_SHEET", unset = "Master")

stopifnot(nzchar(project), nzchar(dataset), nzchar(location))

message("Reading roster from: ", xlsx_path, " (sheet=", xlsx_sheet, ")")
raw <- readxl::read_excel(xlsx_path, sheet = xlsx_sheet)

# Clean column names for easier matching while preserving originals
clean <- janitor::clean_names(raw)

# Helper to safely pluck a column if present, else NULL
pluck_col <- function(df, candidates) {
  for (nm in candidates) {
    if (nm %in% names(df)) return(df[[nm]])
  }
  NULL
}

# Candidates observed/expected:
# - "Offical ID" (as spelled in the sheet) -> clean_names => "offical_id"
# - Sometimes a second id column "Offical ID2" -> "offical_id2"
# - "Vald Name" -> "vald_name"
id1 <- pluck_col(clean, c("offical_id", "official_id", "id"))
id2 <- pluck_col(clean, c("offical_id2", "official_id2"))
valdn <- pluck_col(clean, c("vald_name", "vald_full_name", "full_name", "name"))

if (is.null(id1) && is.null(id2)) {
  stop("Could not find an ID column (expected something like 'Offical ID' or 'Offical ID2').")
}
if (is.null(valdn)) {
  stop("Could not find a 'Vald Name' column (expected something like 'Vald Name').")
}

df <- tibble(
  offical_id = coalesce(as.character(id1), as.character(id2)) |> str_trim(),
  vald_name  = as.character(valdn) |> str_squish()
) |>
  filter(!is.na(offical_id), offical_id != "", !is.na(vald_name), vald_name != "") |>
  distinct()

message("Prepared ", nrow(df), " roster rows for upload.")
if (nrow(df) == 0) {
  stop("No valid roster rows to upload after cleaning.")
}

# Connect to BigQuery using ADC provided by the GitHub Action
con <- dbConnect(
  bigrquery::bigquery(),
  project = project,
  dataset = dataset,
  billing = project,
  location = location
)
on.exit(try(dbDisconnect(con), silent = TRUE))

# Ensure table schema is as expected; overwrite for simplicity
# Columns:
# - offical_id (STRING)
# - vald_name  (STRING)
message("Writing to BigQuery: ", project, ".", dataset, ".", table_name)
dbWriteTable(
  con,
  name = table_name,
  value = df,
  overwrite = TRUE
)

# Row count verification
res <- dbGetQuery(con, sprintf("SELECT COUNT(*) AS c FROM `%s.%s.%s`", project, dataset, table_name))
message("Upload complete. Row count in ", table_name, ": ", res$c[1])