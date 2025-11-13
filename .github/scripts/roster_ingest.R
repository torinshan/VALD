#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(readxl)
  library(dplyr)
  library(stringr)
  library(janitor)
  library(bigrquery)
  library(DBI)
  library(tibble)
})

# Environment
project      <- Sys.getenv("GCP_PROJECT")
dataset      <- Sys.getenv("BQ_DATASET")
location     <- Sys.getenv("BQ_LOCATION", unset = "US")
table_name   <- Sys.getenv("ROSTER_TABLE", unset = "roster_mapping")
xlsx_path    <- Sys.getenv("ROSTER_LOCAL_FILE", unset = ".github/Sac State Roster - Summer 2025.xlsx")
xlsx_sheet   <- Sys.getenv("ROSTER_SHEET", unset = "Master")

stopifnot(nzchar(project), nzchar(dataset), nzchar(location))

message("Reading roster from: ", xlsx_path, " (sheet=", xlsx_sheet, ")")
raw <- readxl::read_excel(xlsx_path, sheet = xlsx_sheet)

# Clean column names for matching while preserving originals separately
clean <- janitor::clean_names(raw)

# Helper to safely pluck a column if present, else NULL
pluck_col <- function(df, candidates) {
  for (nm in candidates) {
    if (nm %in% names(df)) return(df[[nm]])
  }
  NULL
}

# Candidates observed/expected:
# - "Offical ID" (as spelled) -> clean_names => "offical_id"
# - Sometimes "Offical ID2"   -> "offical_id2"
# - "Vald Name"               -> "vald_name"
id1   <- pluck_col(clean, c("offical_id", "official_id", "id"))
id2   <- pluck_col(clean, c("offical_id2", "official_id2"))
valdn <- pluck_col(clean, c("vald_name", "vald_full_name", "full_name", "name"))

if (is.null(id1) && is.null(id2)) {
  stop("Could not find an ID column (expected something like 'Offical ID' or 'Offical ID2').")
}
if (is.null(valdn)) {
  stop("Could not find a 'Vald Name' column (expected something like 'Vald Name').")
}

df <- tibble(
  offical_id = coalesce(as.character(id1), as.character(id2)),
  vald_name  = as.character(valdn)
) |> 
  mutate(
    offical_id = str_trim(offical_id),
    vald_name  = str_squish(vald_name)
  ) |> 
  filter(offical_id != "", vald_name != "") |> 
  distinct() |> 
  rename(`Vald Name` = vald_name)

message("Prepared ", nrow(df), " roster rows for upload.")
if (nrow(df) == 0) stop("No valid roster rows to upload after cleaning.")

# Connect to BigQuery (ADC from GitHub Action)
con <- dbConnect(
  bigrquery::bigquery(),
  project = project,
  dataset = dataset,
  billing = project,
  location = location
)
on.exit(try(dbDisconnect(con), silent = TRUE))

message("Writing to BigQuery: ", project, ".", dataset, ".", table_name)
dbWriteTable(
  con,
  name = table_name,
  value = df,
  overwrite = TRUE
)

# Correct query format string
query <- sprintf("SELECT COUNT(*) AS c FROM `%s.%s.%s`", project, dataset, table_name)
res <- dbGetQuery(con, query)
message("Upload complete. Row count in ", table_name, ": ", res$c[1])
