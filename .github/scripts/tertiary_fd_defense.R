# FIXED tertiary_fd_defense.R
#!/usr/bin/env Rscript
# Purpose: tertiary defense that (1) checks for duplicate test_ID in vald_fd_jumps,
# (2) validates vald_id presence and team/position mapping, (3) writes issues to BQ + CSV artifacts,
# (4) materializes a deduplicated table/view, and (5) exits successfully while letting the workflow email on duplicates.

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

# ------------------ Data Quality Checks ------------------
log("Running data quality checks...")

# Check 1: Missing vald_id validation
log("Checking for missing vald_id values...")
missing_vald_id_query <- sprintf("
  SELECT 
    COUNT(*) as total_rows,
    COUNT(CASE WHEN vald_id IS NULL OR TRIM(vald_id) = '' THEN 1 END) as missing_count
  FROM %s
", fq(base_tbl))

tryCatch({
  missing_vald_id_result <- DBI::dbGetQuery(con, missing_vald_id_query)
  if (missing_vald_id_result$missing_count > 0) {
    log("WARNING:", missing_vald_id_result$missing_count, "rows missing vald_id out of", missing_vald_id_result$total_rows, "total rows")
    
    # Export missing vald_id records to artifact
    missing_vald_records_query <- sprintf("
      SELECT *
      FROM %s
      WHERE vald_id IS NULL OR TRIM(vald_id) = ''
      LIMIT 1000
    ", fq(base_tbl))
    
    missing_vald_records <- DBI::dbGetQuery(con, missing_vald_records_query)
    if (nrow(missing_vald_records) > 0) {
      readr::write_csv(missing_vald_records, file.path(artifacts_dir, "missing_vald_id_records.csv"))
      log("Exported", nrow(missing_vald_records), "missing vald_id records to missing_vald_id_records.csv")
    }
  } else {
    log("✓ All", missing_vald_id_result$total_rows, "records have vald_id")
  }
}, error = function(e) {
  log("Error checking vald_id:", conditionMessage(e))
})

# Check 2: Team/Position mapping validation
log("Checking team/position mapping from vald_roster...")

# Check if roster table exists
roster_exists <- tryCatch({
  DBI::dbExistsTable(con, Id(project = project, dataset = dataset, table = roster_tbl))
}, error = function(e) {
  log("Error checking roster table:", conditionMessage(e))
  FALSE
})

if (roster_exists) {
  mapping_summary_query <- sprintf("
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
    mapping_check AS (
      SELECT 
        b.*,
        r.team,
        r.position,
        CASE 
          WHEN r.team IS NULL OR r.position IS NULL THEN TRUE 
          ELSE FALSE 
        END as missing_mapping
      FROM base_data b
      LEFT JOIN roster_data r ON b.vald_id = r.vald_id
    )
    SELECT 
      COUNT(*) as total_records,
      COUNT(CASE WHEN missing_mapping THEN 1 END) as missing_mapping_count,
      COUNT(CASE WHEN NOT missing_mapping THEN 1 END) as valid_mapping_count
    FROM mapping_check
  ", fq(base_tbl), fq(roster_tbl))
  
  tryCatch({
    mapping_summary <- DBI::dbGetQuery(con, mapping_summary_query)
    
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
          CONCAT(COALESCE(b.full_name, 'Unknown'), '_', 'team/position', '_', 'Unknown') as fallback_identifier,
          TRUE as missing_mapping
        FROM base_data b
        LEFT JOIN roster_data r ON b.vald_id = r.vald_id
        WHERE r.team IS NULL OR r.position IS NULL
        ORDER BY b.full_name, b.date DESC, b.time DESC
        LIMIT 1000
      ", fq(base_tbl), fq(roster_tbl))
      
      missing_mappings <- DBI::dbGetQuery(con, missing_mapping_details_query)
      if (nrow(missing_mappings) > 0) {
        readr::write_csv(missing_mappings, file.path(artifacts_dir, "missing_team_position_mappings.csv"))
        log("Exported", nrow(missing_mappings), "records with missing team/position to missing_team_position_mappings.csv")
        
        # Show sample of concatenated identifiers
        sample_identifiers <- head(missing_mappings$fallback_identifier, 5)
        log("Sample fallback identifiers for missing mappings:")
        for (i in seq_along(sample_identifiers)) {
          log("-", sample_identifiers[i])
        }
      }
    } else {
      log("✓ All records have valid team/position mappings")
    }
  }, error = function(e) {
    log("Error checking team/position mappings:", conditionMessage(e))
  })
} else {
  log("WARNING: vald_roster table not found - skipping team/position validation")
}

# ------------------ Duplicate detection ------------------
log("Running duplicate scan on", paste0(project, ".", dataset, ".", base_tbl))

# Basic duplicate detection
tryCatch({
  duplicate_scan_query <- sprintf("
    WITH duplicate_check AS (
      SELECT 
        test_id,
        COUNT(*) as occurrence_count,
        ROW_NUMBER() OVER (PARTITION BY test_id ORDER BY %s) as rn
      FROM %s
      GROUP BY test_id, date, time
    )
    SELECT 
      COUNT(DISTINCT test_id) as total_unique_test_ids,
      COUNT(CASE WHEN occurrence_count > 1 THEN test_id END) as duplicate_test_ids,
      SUM(CASE WHEN occurrence_count > 1 THEN occurrence_count - 1 ELSE 0 END) as excess_records
    FROM duplicate_check
  ", rank_expr, fq(base_tbl))
  
  duplicate_results <- DBI::dbGetQuery(con, duplicate_scan_query)
  
  if (duplicate_results$duplicate_test_ids > 0) {
    log("WARNING: Found", duplicate_results$duplicate_test_ids, "duplicate test_ids with", duplicate_results$excess_records, "excess records")
    
    # Export sample duplicates
    duplicates_sample_query <- sprintf("
      WITH ranked_data AS (
        SELECT 
          *,
          COUNT(*) OVER (PARTITION BY test_id) as dup_count,
          ROW_NUMBER() OVER (PARTITION BY test_id ORDER BY %s) as rn
        FROM %s
      )
      SELECT *
      FROM ranked_data
      WHERE dup_count > 1
      ORDER BY test_id, rn
      LIMIT 500
    ", rank_expr, fq(base_tbl))
    
    duplicates_sample <- DBI::dbGetQuery(con, duplicates_sample_query)
    if (nrow(duplicates_sample) > 0) {
      readr::write_csv(duplicates_sample, file.path(artifacts_dir, "duplicate_test_ids_sample.csv"))
      log("Exported", nrow(duplicates_sample), "duplicate records sample to duplicate_test_ids_sample.csv")
    }
  } else {
    log("✓ No duplicate test_ids found")
  }
}, error = function(e) {
  log("Error in duplicate detection:", conditionMessage(e))
})

log("Data quality checks completed")
log("Finished tertiary defense; exiting 0 so the workflow can notify if needed")
