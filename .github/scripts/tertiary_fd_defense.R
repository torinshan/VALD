#!/usr/bin/env Rscript
# Purpose: tertiary defense that (1) checks for duplicate test_ID in vald_fd_jumps,
# (2) writes dupes to BQ + CSV artifact, (3) materializes a deduplicated table/view,
# (4) validates vald_id presence and team/position mapping,
# and (5) exits successfully while letting the workflow email on duplicates.

suppressPackageStartupMessages({
  library(DBI)
  library(bigrquery)
  library(dplyr)
  library(readr)
  library(lubridate)
  library(gargle)
  library(stringr)
})

log <- function(...) cat(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "-", ..., "\n")

# ------------------ Config ------------------
project <- Sys.getenv("GCP_PROJECT", "sac-vald-hub")
dataset <- Sys.getenv("BQ_DATASET", "analytics")
location <- Sys.getenv("BQ_LOCATION", "US")
base_tbl <- Sys.getenv("BASE_TABLE", "vald_fd_jumps")
dupes_tbl <- Sys.getenv("DUPES_TABLE", "vald_fd_jumps_dupes")
unique_tbl <- Sys.getenv("UNIQUE_TABLE", "vald_fd_jumps_unique")
unique_view <- Sys.getenv("UNIQUE_VIEW", "vald_fd_jumps_no_dupes")
roster_tbl <- Sys.getenv("ROSTER_TABLE", "vald_roster")
artifacts_dir <- Sys.getenv("ARTIFACTS_DIR", "artifacts")

# prefer date+time to rank which duplicate to keep
rank_order_cols <- c("date", "time")

# ------------------ Auth ------------------
log("Authenticating to BigQuery via gcloud token …")
access_token <- tryCatch(system("gcloud auth print-access-token", intern = TRUE)[1], error = function(e) "")
if (nzchar(access_token)) {
  token <- gargle::gargle2.0_token(
    scope = 'https://www.googleapis.com/auth/bigquery',
    client = gargle::gargle_client(),
    credentials = list(access_token = access_token)
  )
  bigrquery::bq_auth(token = token)
  log("bq_auth OK")
} else {
  stop("Could not obtain gcloud access token")
}

con <- DBI::dbConnect(bigrquery::bigquery(), project = project)
dir.create(artifacts_dir, showWarnings = FALSE, recursive = TRUE)
fq <- function(t) sprintf("`%s.%s.%s`", project, dataset, t)

# ------------------ Build ranking expression ------------------
# We will prefer the most recent record by (date desc, time desc) when deduping.
rank_expr <- paste(
  c(
    if ("date" %in% DBI::dbListFields(con, Id(project = project, dataset = dataset, table = base_tbl))) "date DESC" else NULL,
    if ("time" %in% DBI::dbListFields(con, Id(project = project, dataset = dataset, table = base_tbl))) "time DESC" else NULL,
    "_PARTITIONTIME DESC", # fallback if table is partitioned
    "CURRENT_TIMESTAMP() DESC" # ultimate fallback (stable tie-breaker)
  ),
  collapse = ", "
)
if (rank_expr == "") rank_expr <- "CURRENT_TIMESTAMP() DESC"

# ------------------ Duplicate detection ------------------
log("Running duplicate scan on", paste0(project, ".", dataset, ".", base_tbl))

# [Your existing duplicate detection logic goes here]
# ...

# ------------------ Data Quality Checks ------------------
log("Running data quality checks...")

# Check 1: Missing vald_id validation
log("Checking for missing vald_id values...")
missing_vald_id_query <- sprintf("
  SELECT 
    COUNT(*) as missing_count,
    COUNT(CASE WHEN vald_id IS NULL OR vald_id = '' THEN 1 END) as null_empty_count
  FROM %s
  WHERE vald_id IS NULL OR TRIM(vald_id) = ''
", fq(base_tbl))

missing_vald_id_result <- DBI::dbGetQuery(con, missing_vald_id_query)
if (missing_vald_id_result$missing_count > 0) {
  log("WARNING:", missing_vald_id_result$missing_count, "rows missing vald_id")
  
  # Export missing vald_id records to artifact
  missing_vald_records_query <- sprintf("
    SELECT *
    FROM %s
    WHERE vald_id IS NULL OR TRIM(vald_id) = ''
  ", fq(base_tbl))
  
  missing_vald_records <- DBI::dbGetQuery(con, missing_vald_records_query)
  if (nrow(missing_vald_records) > 0) {
    readr::write_csv(missing_vald_records, file.path(artifacts_dir, "missing_vald_id_records.csv"))
    log("Exported", nrow(missing_vald_records), "missing vald_id records to missing_vald_id_records.csv")
  }
} else {
  log("✓ All records have vald_id")
}

# Check 2: Team/Position mapping validation
log("Checking team/position mapping from vald_roster...")

# Check if roster table exists
roster_exists <- tryCatch({
  DBI::dbExistsTable(con, Id(project = project, dataset = dataset, table = roster_tbl))
}, error = function(e) FALSE)

if (roster_exists) {
  missing_mapping_query <- sprintf("
    WITH base_data AS (
      SELECT 
        vald_id,
        full_name,
        test_id
      FROM %s
      WHERE vald_id IS NOT NULL AND TRIM(vald_id) != ''
    ),
    roster_data AS (
      SELECT 
        vald_id,
        team,
        position
      FROM %s
    ),
    missing_mappings AS (
      SELECT 
        b.vald_id,
        b.full_name,
        b.test_id,
        r.team,
        r.position,
        CASE 
          WHEN r.team IS NULL OR r.position IS NULL THEN 
            CONCAT(COALESCE(b.full_name, 'Unknown'), '_', 'team/position', '_', 'Unknown')
          ELSE 
            CONCAT(COALESCE(b.full_name, 'Unknown'), '_', COALESCE(r.team, 'Unknown'), '_', COALESCE(r.position, 'Unknown'))
        END as mapping_identifier
      FROM base_data b
      LEFT JOIN roster_data r ON b.vald_id = r.vald_id
    )
    SELECT 
      COUNT(*) as total_records,
      COUNT(CASE WHEN team IS NULL OR position IS NULL THEN 1 END) as missing_mapping_count,
      COUNT(CASE WHEN team IS NOT NULL AND position IS NOT NULL THEN 1 END) as valid_mapping_count
    FROM missing_mappings
  ", fq(base_tbl), fq(roster_tbl))
  
  mapping_summary <- DBI::dbGetQuery(con, missing_mapping_query)
  
  log("Team/Position mapping summary:")
  log("- Total records:", mapping_summary$total_records)
  log("- Valid mappings:", mapping_summary$valid_mapping_count)
  log("- Missing mappings:", mapping_summary$missing_mapping_count)
  
  if (mapping_summary$missing_mapping_count > 0) {
    log("WARNING:", mapping_summary$missing_mapping_count, "records missing team/position mapping")
    
    # Export records with missing mappings and their generated identifiers
    missing_mapping_details_query <- sprintf("
      WITH base_data AS (
        SELECT 
          vald_id,
          full_name,
          test_id,
          date,
          time
        FROM %s
        WHERE vald_id IS NOT NULL AND TRIM(vald_id) != ''
      ),
      roster_data AS (
        SELECT 
          vald_id,
          team,
          position
        FROM %s
      )
      SELECT 
        b.*,
        r.team,
        r.position,
        CASE 
          WHEN r.team IS NULL OR r.position IS NULL THEN 
            CONCAT(COALESCE(b.full_name, 'Unknown'), '_', 'team/position', '_', 'Unknown')
          ELSE 
            CONCAT(COALESCE(b.full_name, 'Unknown'), '_', COALESCE(r.team, 'Unknown'), '_', COALESCE(r.position, 'Unknown'))
        END as mapping_identifier,
        CASE 
          WHEN r.team IS NULL OR r.position IS NULL THEN TRUE 
          ELSE FALSE 
        END as missing_mapping
      FROM base_data b
      LEFT JOIN roster_data r ON b.vald_id = r.vald_id
      WHERE r.team IS NULL OR r.position IS NULL
      ORDER BY b.full_name, b.date DESC, b.time DESC
    ", fq(base_tbl), fq(roster_tbl))
    
    missing_mappings <- DBI::dbGetQuery(con, missing_mapping_details_query)
    if (nrow(missing_mappings) > 0) {
      readr::write_csv(missing_mappings, file.path(artifacts_dir, "missing_team_position_mappings.csv"))
      log("Exported", nrow(missing_mappings), "records with missing team/position to missing_team_position_mappings.csv")
      
      # Show sample of concatenated identifiers
      sample_identifiers <- head(missing_mappings$mapping_identifier, 5)
      log("Sample mapping identifiers for missing data:")
      for (i in seq_along(sample_identifiers)) {
        log("-", sample_identifiers[i])
      }
    }
  } else {
    log("✓ All records have valid team/position mappings")
  }
  
} else {
  log("WARNING: vald_roster table not found - skipping team/position validation")
}

log("Data quality checks completed.")

# [Your existing deduplication logic continues here]
# ...

log("Finished tertiary defense; exiting 0 so the workflow can notify if needed.")
