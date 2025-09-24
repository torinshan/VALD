#!/usr/bin/env Rscript
# Purpose: tertiary defense that (1) checks for duplicate test_ID in vald_fd_jumps,
# (2) writes dupes to BQ + CSV artifact, (3) materializes a deduplicated table/view,
# and (4) exits successfully while letting the workflow email on duplicates.
suppressPackageStartupMessages({
library(DBI)
library(bigrquery)
library(dplyr)
library(readr)
library(lubridate)
library(gargle)
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
artifacts_dir <- Sys.getenv("ARTIFACTS_DIR", "artifacts")
# prefer date+time to rank which duplicate to keep
rank_order_cols <- c("date", "time")
# ------------------ Auth ------------------
log("Authenticating to BigQuery via WIF credentials …")

# WIF creates a credentials file that we can use directly
# Check for the standard application default credentials path
cred_file <- Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

if (nzchar(cred_file) && file.exists(cred_file)) {
  log("Using WIF credentials file:", cred_file)
  # Use service account credentials directly
  bigrquery::bq_auth(path = cred_file)
} else {
  # Fallback: try application default credentials (ADC)
  log("Using Application Default Credentials")
  bigrquery::bq_auth()
}

log("BigQuery authentication completed")

con <- DBI::dbConnect(bigrquery::bigquery(), project = project)

fq <- function(t) sprintf("%s.%s.%s", project, dataset, t)
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
log("Finished tertiary defense; exiting 0 so the workflow can notify if needed.
