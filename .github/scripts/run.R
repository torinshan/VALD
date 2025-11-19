#!/usr/bin/env Rscript

tryCatch({
  suppressPackageStartupMessages({
    library(bigrquery); library(DBI)
    library(dplyr); library(tidyr); library(readr); library(stringr)
    library(purrr); library(tibble); library(data.table)
    library(hms); library(lubridate)
    library(httr); library(jsonlite); library(xml2); library(curl)
    library(valdr); library(gargle); library(rlang); library(lifecycle)
    library(glue); library(slider); library(R.utils)
  })
}, error = function(e) { cat("Error loading packages:", e$message, "\n"); quit(status=1) })

# Disable BigQuery Storage API to avoid readsessions permission
options(bigrquery.use_bqstorage = FALSE)
Sys.setenv(BIGRQUERY_USE_BQ_STORAGE = "false")

project  <- Sys.getenv("GCP_PROJECT",  "sac-vald-hub")
dataset  <- Sys.getenv("BQ_DATASET",   "analytics")
location <- Sys.getenv("BQ_LOCATION",  "US")
cat("GCP Project:", project, "\n")
cat("BQ Dataset:", dataset, "\n")
cat("BQ Location:", location, "\n")

# ---------- CMJ validation helpers (added) ----------
# Session id: per-athlete, group contiguous tests ≤ 2 hours apart
create_session_ids <- function(vald_id, dt) {
  dplyr::coalesce(
    paste0(vald_id, "_",
           cumsum(dplyr::if_else(
             is.na(dt) | dplyr::lag(is.na(dt), default = TRUE),
             TRUE,
             as.numeric(dt - dplyr::lag(dt)) > (2*60*60)
           ))),
    paste0(vald_id, "_", seq_along(dt))
  )
}

# Adaptive z-score using rolling "look-back" windows: 30 → 60 → 90 → all history
adaptive_z <- function(x, t, windows = c(30, 60, 90, Inf)) {
  n <- length(x); out <- rep(NA_real_, n)
  for (i in seq_len(n)) {
    hist <- t < t[i]
    picked <- NULL
    for (w in windows) {
      mask <- if (is.infinite(w)) hist else hist & (t >= t[i] - lubridate::days(w))
      if (sum(!is.na(x[mask])) >= 4) { picked <- mask; break }
    }
    if (!is.null(picked)) {
      m <- mean(x[picked], na.rm=TRUE); s <- stats::sd(x[picked], na.rm=TRUE)
      out[i] <- if (isTRUE(s>0)) (x[i]-m)/s else NA_real_
    }
  }
  out
}

# Robust z using MAD
robust_z <- function(r) {
  med <- stats::median(r, na.rm = TRUE)
  mad <- stats::mad(r, center = med, constant = 1.4826, na.rm = TRUE)
  if (!is.finite(mad) || mad == 0) return(rep(NA_real_, length(r)))
  (r - med)/mad
}

# Layer 1 — Quick guardrails
apply_layer1 <- function(df) {
  g <- 9.80665
  df %>%
    dplyr::arrange(vald_id, event_datetime) %>%
    dplyr::group_by(vald_id) %>%
    dplyr::mutate(
      bw_med_30d = slider::slide_index_dbl(body_weight_lbs, date, ~stats::median(.x, na.rm=TRUE),
                                           .before = lubridate::days(30), .complete = FALSE),
      bw_delta_pct = dplyr::if_else(is.finite(bw_med_30d) & bw_med_30d>0,
                                    abs(body_weight_lbs - bw_med_30d)/bw_med_30d, NA_real_),
      flag_bw_delta = !is.na(bw_delta_pct) & bw_delta_pct > 0.15,
      v_expected = sqrt(2*g*jump_height_m),
      v_ratio = dplyr::if_else(is.finite(v_expected) & v_expected>0, takeoff_v_ms / v_expected, NA_real_),
      flag_physics_violation = !is.na(v_ratio) & (v_ratio < 0.80 | v_ratio > 1.25)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(session_id) %>%
    dplyr::mutate(
      session_extreme = sum(flag_physics_violation, na.rm = TRUE),
      flag_session_contamination = !is.na(session_id) & session_extreme > 10
    ) %>%
    dplyr::ungroup()
}

# Layer 2 — Progressive z-scores (adaptive windows)
apply_layer2 <- function(df) {
  df %>%
    dplyr::arrange(vald_id, date, time) %>%
    dplyr::group_by(vald_id) %>%
    dplyr::mutate(
      z_jh  = adaptive_z(jump_height_inches_imp_mom, date),
      z_bw  = adaptive_z(body_weight_lbs, date),
      z_rsi = adaptive_z(rsi_modified_imp_mom, date),
      z_v   = adaptive_z(takeoff_v_ms, date),
      total_tests = dplyr::n(),
      z_threshold = dplyr::case_when(total_tests <= 7 ~ 5, total_tests <= 12 ~ 4, TRUE ~ 3),
      extreme_count = (abs(z_jh) > 4) + (abs(z_bw) > 4) + (abs(z_rsi) > 4) + (abs(z_v) > 4),
      flag_multiple_extremes = extreme_count >= 3
    ) %>% dplyr::ungroup()
}

# Layer 3 — Physics consistency
apply_layer3 <- function(df) {
  g <- 9.80665
  df %>%
    dplyr::mutate(
      h_from_v = (takeoff_v_ms^2) / (2*g),
      ape_h = dplyr::if_else(jump_height_m > 0, abs(jump_height_m - h_from_v)/jump_height_m, NA_real_),
      v_residual = takeoff_v_ms - sqrt(pmax(0, 2*g*jump_height_m)),
      z_resid = robust_z(v_residual),
      physics_flag = dplyr::case_when(
        jump_height_m < 0.10 ~ "SKIP",
        !is.na(v_ratio) & (v_ratio < 0.80 | v_ratio > 1.25) ~ "RED",
        !is.na(v_ratio) & (v_ratio < 0.90 | v_ratio > 1.15) ~ "AMBER",
        !is.na(ape_h)   & ape_h > 0.35 ~ "RED",
        !is.na(ape_h)   & ape_h > 0.15 ~ "AMBER",
        !is.na(z_resid) & abs(z_resid) > 4 ~ "RED",
        !is.na(z_resid) & abs(z_resid) > 3 ~ "AMBER",
        TRUE ~ "GREEN"
      )
    )
}

# Layer 4 — mRSI reasonableness
apply_layer4 <- function(df) {
  df %>%
    dplyr::group_by(vald_id) %>%
    dplyr::arrange(date, time, .by_group = TRUE) %>%
    dplyr::mutate(
      z30_mrsi = adaptive_z(rsi_modified_imp_mom, date, windows = c(30, 60, 90, Inf)),
      z30_jh   = adaptive_z(jump_height_inches_imp_mom, date, windows = c(30, 60, 90, Inf)),
      z30_ct   = adaptive_z(contraction_time, date, windows = c(30, 60, 90, Inf)),
      mrsi_invalid = (!is.na(z30_mrsi) & z30_mrsi >= 2) & (!is.na(z30_jh) & z30_jh <= 1),
      rsi_modified_imp_mom_clean = dplyr::if_else(mrsi_invalid, NA_real_, rsi_modified_imp_mom)
    ) %>% dplyr::ungroup()
}

# Final classification + clean metrics
apply_final_and_clean <- function(df) {
  df %>%
    dplyr::mutate(
      coherence_flag = dplyr::case_when(
        z_jh >  2 & z_v >  1 ~ TRUE,
        z_jh < -2 & z_v < -1 ~ TRUE,
        abs(z_jh) > 2 & abs(z_v) < 1 ~ FALSE,
        TRUE ~ TRUE
      ),
      final_classification = dplyr::case_when(
        physics_flag == "RED" & (flag_bw_delta | flag_session_contamination | !coherence_flag) ~ "LIKELY_INACCURATE_MEASUREMENT",
        physics_flag %in% c("AMBER","RED") & coherence_flag & total_tests >= 5 ~ "VALID_BUT_OUTLIER",
        physics_flag == "GREEN" & coherence_flag & abs(z_jh) > 3 ~ "VALID_EXTREME",
        TRUE ~ "LIKELY_VALID"
      ),
      jump_height_clean_inches = dplyr::if_else(final_classification == "LIKELY_INACCURATE_MEASUREMENT", NA_real_, jump_height_inches_imp_mom),
      takeoff_v_ms_clean = dplyr::if_else(final_classification == "LIKELY_INACCURATE_MEASUREMENT", NA_real_, takeoff_v_ms)
    )
}

# Global variable to store access token for REST API calls
GLOBAL_ACCESS_TOKEN <- NULL

# ---------- Schema Mismatch Tracking ----------
schema_mismatches <- tibble(
  table_name = character(0),
  missing_columns = character(0),
  column_type = character(0),
  timestamp = as.POSIXct(character(0))
)

log_schema_mismatch <- function(table_name, column_name, column_type) {
  schema_mismatches <<- bind_rows(schema_mismatches, tibble(
    table_name = table_name,
    missing_columns = column_name,
    column_type = column_type,
    timestamp = Sys.time()
  ))
  create_log_entry(paste("SCHEMA MISMATCH:", table_name, "- Missing column:", column_name, "Type:", column_type), "WARN")
}

upload_schema_mismatches <- function() {
  if (nrow(schema_mismatches) == 0) return(invisible(TRUE))
  tryCatch({
    mismatch_tbl <- bq_table(ds, "vald_schema_mismatches")
    if (!bq_table_exists(mismatch_tbl)) {
      bq_table_create(mismatch_tbl, fields = as_bq_fields(schema_mismatches))
    }
    bq_table_upload(mismatch_tbl, schema_mismatches, write_disposition = "WRITE_APPEND")
    create_log_entry(paste("Logged", nrow(schema_mismatches), "schema mismatches"))
    TRUE
  }, error = function(e) {
    cat("Schema mismatch upload failed:", e$message, "\n")
    FALSE
  })
}

# ---------- VALD API Pagination Safety ----------
# Track pagination state to detect stuck loops
pagination_state <- list()

detect_stuck_pagination <- function(description, current_cursor = NULL, max_same = 3) {
  if (is.null(current_cursor)) return(FALSE)
  
  if (!exists(description, envir = as.environment(pagination_state))) {
    pagination_state[[description]] <<- list(
      last_cursor = current_cursor,
      same_count = 1,
      total_pages = 1
    )
    return(FALSE)
  }
  
  state <- pagination_state[[description]]
  
  if (identical(state$last_cursor, current_cursor)) {
    state$same_count <- state$same_count + 1
    state$total_pages <- state$total_pages + 1
    
    if (state$same_count >= max_same) {
      create_log_entry(paste(
        "CIRCUIT BREAKER TRIGGERED:", description,
        "- Stuck at cursor:", substr(current_cursor, 1, 50),
        "- Repeated", state$same_count, "times"
      ), "ERROR")
      pagination_state[[description]] <<- state
      return(TRUE)
    }
    
    create_log_entry(paste(
      "Pagination warning:", description,
      "- Same cursor repeated", state$same_count, "times"
    ), "WARN")
  } else {
    state$same_count <- 1
  }
  
  state$last_cursor <- current_cursor
  state$total_pages <- state$total_pages + 1
  pagination_state[[description]] <<- state
  
  return(FALSE)
}

# Wrapper for VALD API calls with timeout and pagination protection
safe_vald_fetch <- function(fetch_function, description = "VALD API", 
                           timeout_seconds = 600, max_same_cursor = 3, ...) {
  create_log_entry(paste("Starting", description, "fetch with", timeout_seconds, "second timeout"))
  
  # Reset pagination state for this fetch
  if (exists(description, envir = as.environment(pagination_state))) {
    pagination_state[[description]] <<- NULL
  }
  
  result <- NULL
  timed_out <- FALSE
  circuit_breaker_tripped <- FALSE
  
  # Wrap the fetch function to monitor pagination
  monitored_fetch <- function(...) {
    start_time <- Sys.time()
    last_log_time <- start_time
    
    tryCatch({
      result <- fetch_function(...)
      
      # Check if we spent too long (potential stuck pagination)
      elapsed <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
      if (elapsed > timeout_seconds * 0.8) {
        create_log_entry(paste(
          "WARNING:", description, "took", round(elapsed, 1),
          "seconds - approaching timeout"
        ), "WARN")
      }
      
      return(result)
    }, error = function(e) {
      # Check if error message indicates pagination issues
      if (grepl("pagination|cursor|timeout", e$message, ignore.case = TRUE)) {
        create_log_entry(paste(
          "Pagination error detected in", description, ":", e$message
        ), "ERROR")
      }
      stop(e)
    })
  }
  
  tryCatch({
    result <- withTimeout({
      monitored_fetch(...)
    }, timeout = timeout_seconds, onTimeout = "error")
  }, TimeoutException = function(e) {
    timed_out <<- TRUE
    create_log_entry(paste(description, "fetch TIMEOUT after", timeout_seconds, "seconds"), "ERROR")
    create_log_entry("This indicates a VALD API pagination bug or network issue", "ERROR")
    
    # Log pagination state if available
    if (exists(description, envir = as.environment(pagination_state))) {
      state <- pagination_state[[description]]
      create_log_entry(paste(
        "Pagination state at timeout - Last cursor:",
        substr(state$last_cursor, 1, 50),
        "- Total pages:", state$total_pages,
        "- Same cursor count:", state$same_count
      ), "ERROR")
    }
  }, error = function(e) {
    if (!timed_out) {
      create_log_entry(paste(description, "fetch ERROR:", e$message), "ERROR")
    }
  })
  
  if (timed_out || is.null(result)) {
    create_log_entry("=== VALD API FETCH FAILED ===", "ERROR")
    upload_logs_to_bigquery()
    upload_schema_mismatches()
    quit(status = 1)
  }
  
  # Log successful completion
  if (exists(description, envir = as.environment(pagination_state))) {
    state <- pagination_state[[description]]
    create_log_entry(paste(
      description, "completed successfully -",
      "Total pages:", state$total_pages
    ))
  }
  
  result
}

# ---------- Schema Validation and Matching ----------
infer_bq_type <- function(column_data) {
  if (is.logical(column_data)) return("BOOLEAN")
  if (inherits(column_data, "POSIXct") || inherits(column_data, "POSIXt")) return("TIMESTAMP")
  if (inherits(column_data, "Date")) return("DATE")
  if (inherits(column_data, "hms") || inherits(column_data, "difftime")) return("TIME")
  if (is.integer(column_data)) return("INTEGER")
  if (is.numeric(column_data)) return("FLOAT")
  return("STRING")
}

validate_and_fix_schema <- function(data, table_name, ds) {
  tbl <- bq_table(ds, table_name)
  
  # If table doesn't exist, no validation needed
  if (!bq_table_exists(tbl)) {
    return(data)
  }
  
  # Get current schema
  tryCatch({
    meta <- bq_table_meta(tbl)
    existing_fields <- sapply(meta$schema$fields, function(f) f$name)
    data_fields <- names(data)
    
    # Find new columns that will be added to BigQuery schema
    new_columns <- setdiff(data_fields, existing_fields)
    
    if (length(new_columns) > 0) {
      create_log_entry(paste(
        "Schema evolution in", table_name, "-",
        length(new_columns), "new columns will be added to BigQuery:",
        paste(head(new_columns, 10), collapse = ", ")
      ), "INFO")
      
      # Log each new column for tracking
      for (col in new_columns) {
        col_type <- infer_bq_type(data[[col]])
        log_schema_mismatch(table_name, col, col_type)
      }
      
      # Keep the new columns - BigQuery will auto-extend schema on WRITE_TRUNCATE
      create_log_entry(paste(
        "Allowing", length(new_columns),
        "new columns to be added to BigQuery schema"
      ), "INFO")
    }
    
    # Check for columns in schema but not in data (less critical, just informational)
    missing_in_data <- setdiff(existing_fields, data_fields)
    if (length(missing_in_data) > 0) {
      create_log_entry(paste(
        "Note:", length(missing_in_data),
        "columns exist in BigQuery schema but not in current data:",
        paste(head(missing_in_data, 10), collapse = ", ")
      ), "INFO")
    }
    
  }, error = function(e) {
    create_log_entry(paste(
      "Error validating schema for", table_name, ":", e$message
    ), "WARN")
  })
  
  return(data)
}

# ---------- Auth (Working Version) ----------
tryCatch({
  cat("=== Authenticating to BigQuery ===\n")
  access_token_result <- system("gcloud auth print-access-token", intern = TRUE)
  GLOBAL_ACCESS_TOKEN <<- access_token_result[1]
  if (nchar(GLOBAL_ACCESS_TOKEN) > 0) {
    cat("Access token obtained from gcloud\n")
    token <- gargle::gargle2.0_token(
      scope = 'https://www.googleapis.com/auth/bigquery',
      client = gargle::gargle_client(),
      credentials = list(access_token = GLOBAL_ACCESS_TOKEN)
    )
    bigrquery::bq_auth(token = token)
    cat("BigQuery authentication successful\n")
    options(bigrquery.use_bqstorage = FALSE)
    Sys.setenv(BIGRQUERY_USE_BQ_STORAGE = "false")
    ds <- bigrquery::bq_dataset(project, dataset)
    invisible(bigrquery::bq_dataset_exists(ds))
    cat("Authentication test passed (dataset visible via REST)\n")
  } else {
    stop("Could not obtain access token from gcloud")
  }
}, error = function(e) {
  cat("BigQuery authentication failed:", e$message, "\n")
  quit(status = 1)
})

con <- DBI::dbConnect(
  bigrquery::bigquery(), 
  project = project,
  use_legacy_sql = FALSE,
  bigint = "integer64"
)
ds <- bq_dataset(project, dataset)
if (!bq_dataset_exists(ds)) {
  bq_dataset_create(ds, location = location)
  cat("Created BigQuery dataset:", dataset, "in", location, "\n")
}

# ---------- Logging ----------
log_entries <- tibble(
  timestamp = as.POSIXct(character(0)), level = character(0), message = character(0),
  run_id = character(0), repository = character(0)
)
create_log_entry <- function(message, level="INFO") {
  ts <- Sys.time()
  cat(sprintf("[%s] [%s] %s\n", format(ts, "%Y-%m-%d %H:%M:%S", tz="UTC"), level, message))
  log_entries <<- bind_rows(log_entries, tibble(
    timestamp = ts, level = level, message = message,
    run_id = Sys.getenv("GITHUB_RUN_ID", "manual"),
    repository = Sys.getenv("GITHUB_REPOSITORY", "unknown")
  ))
}
upload_logs_to_bigquery <- function() {
  if (nrow(log_entries)==0) return(invisible(TRUE))
  tryCatch({
    log_tbl <- bq_table(ds, "vald_processing_log")
    if (!bq_table_exists(log_tbl)) bq_table_create(log_tbl, fields = as_bq_fields(log_entries))
    bq_table_upload(log_tbl, log_entries, write_disposition = "WRITE_APPEND")
    TRUE
  }, error=function(e){ cat("Log upload failed:", e$message, "\n"); FALSE })
}

script_start_time <- Sys.time()
create_log_entry("=== VALD DATA PROCESSING SCRIPT STARTED ===", "START")

# ---------- Helpers: REST-only reads, schema, upload ----------

read_bq_table <- function(table_name) {
  tbl <- bq_table(ds, table_name)
  if (!bq_table_exists(tbl)) {
    create_log_entry(paste("Table", table_name, "does not exist"), "INFO")
    return(data.frame())
  }
  create_log_entry(paste("Reading table", table_name, "using REST API"), "INFO")
  tryCatch({
    meta <- bq_table_meta(tbl)
    fields <- meta$schema$fields
    all_rows <- list()
    page_token <- NULL
    page_num <- 1
    repeat {
      url <- sprintf(
        "https://bigquery.googleapis.com/bigquery/v2/projects/%s/datasets/%s/tables/%s/data",
        project, dataset, table_name
      )
      query_params <- list(maxResults = 10000)
      if (!is.null(page_token)) query_params$pageToken <- page_token
      response <- httr::GET(
        url,
        httr::add_headers(Authorization = paste("Bearer", GLOBAL_ACCESS_TOKEN)),
        query = query_params
      )
      if (httr::http_error(response)) {
        stop(paste("REST API error:", httr::content(response, "text")))
      }
      content <- httr::content(response, "parsed")
      if (is.null(content$rows) || length(content$rows) == 0) {
        if (page_num == 1) {
          create_log_entry(paste("Table", table_name, "returned no data"), "INFO")
          return(data.frame())
        }
        break
      }
      page_data <- lapply(content$rows, function(row) {
        values <- lapply(seq_along(fields), function(i) {
          val <- row$f[[i]]$v
          field <- fields[[i]]
          if (is.null(val)) {
            return(NA)
          } else if (field$type == "INTEGER" || field$type == "INT64") {
            return(as.integer(val))
          } else if (field$type == "FLOAT" || field$type == "FLOAT64") {
            return(as.numeric(val))
          } else if (field$type == "DATE") {
            return(as.Date(val))
          } else if (field$type == "TIME") {
            return(hms::as_hms(val))
          } else if (field$type == "TIMESTAMP") {
            return(as.POSIXct(as.numeric(as.character(val)), origin = "1970-01-01", tz = "UTC"))
          } else {
            return(as.character(val))
          }
        })
        names(values) <- sapply(fields, function(f) f$name)
        as.data.frame(values, stringsAsFactors = FALSE)
      })
      all_rows[[page_num]] <- bind_rows(page_data)
      page_token <- content$nextPageToken
      if (is.null(page_token)) break
      page_num <- page_num + 1
      if (page_num > 100) {
        create_log_entry(paste("Warning: Hit page limit for", table_name), "WARN")
        break
      }
    }
    result <- bind_rows(all_rows)
    create_log_entry(paste("Successfully read", table_name, ":", nrow(result), "rows via REST API"))
    if ("position" %in% names(result)) {
      result <- result %>% select(-position)
      create_log_entry(paste("Removed position column from", table_name))
    }
    result
  }, error = function(e) {
    create_log_entry(paste("Error reading", table_name, ":", e$message), "ERROR")
    data.frame()
  })
}

ensure_table <- function(tbl, data, partition_field="date", cluster_fields=character()) {
  if (bq_table_exists(tbl)) return(invisible(TRUE))

  # Build time partitioning object in a way that's compatible with different bigrquery versions
  tp <- NULL
  if (!is.null(partition_field) && partition_field %in% names(data)) {
    if ("bq_time_partitioning" %in% getNamespaceExports("bigrquery")) {
      tp <- bigrquery::bq_time_partitioning(type = "DAY", field = partition_field)
    } else if ("bq_time_partition" %in% getNamespaceExports("bigrquery")) {
      tp <- bigrquery::bq_time_partition("DAY", field = partition_field)
    } else {
      tp <- list(type = "DAY", field = partition_field)
    }
  }

  cl <- intersect(cluster_fields, names(data))
  if (length(cl) == 0) cl <- NULL

  # NOTE: some bigrquery versions expect `clustering_fields` (not `clustering`).
  bq_table_create(
    tbl,
    fields = as_bq_fields(data),
    time_partitioning = tp,
    clustering_fields = cl
  )

  partition_info <- ifelse(is.null(partition_field), "none", partition_field)
  cluster_info <- if (is.null(cl)) "none" else paste(cl, collapse = ",")
  create_log_entry(paste("Created table", tbl$table, "- partition:", partition_info, "cluster:", cluster_info))
  invisible(TRUE)
}

is_character_col <- function(colname) {
  colname %in% c("name","full_name","given_name","family_name","email","team",
                 "position","position_class","sport","sex","test_type","triallimb",
                 "vald_id","test_ID","external_id","unit_of_measure","unit_of_measure_symbol",
                 "definition","metric","r_name","group")
}
is_date_col <- function(colname) colname %in% c("date","date_of_birth")
is_time_col <- function(colname) colname %in% c("time")
is_integer_col <- function(colname) colname %in% c("height","reps_left","reps_right")

standardize_data_types <- function(data, table_name) {
  if (nrow(data) == 0) return(data)
  data <- data[, names(data)[!vapply(data, function(x) all(is.na(x)), logical(1))], drop=FALSE]
  for (cn in names(data)) {
    x <- data[[cn]]
    if (is_character_col(cn)) {
      data[[cn]] <- as.character(x)
    } else if (is_date_col(cn)) {
      data[[cn]] <- as.Date(x)
    } else if (is_time_col(cn)) {
      data[[cn]] <- hms::as_hms(x)
    } else if (is_integer_col(cn)) {
      if (is.character(x)) suppressWarnings(data[[cn]] <- as.integer(as.numeric(x))) else data[[cn]] <- as.integer(x)
    } else {
      if (is.numeric(x)) data[[cn]] <- as.numeric(x) else suppressWarnings(data[[cn]] <- as.numeric(x))
      data[[cn]][is.infinite(data[[cn]]) | is.nan(data[[cn]])] <- NA_real_
    }
  }
  data
}

# ---------- DML-FREE UPSERT with Schema Validation ----------
bq_upsert <- function(data, table_name, key="test_ID",
                      mode=c("MERGE","TRUNCATE"),
                      partition_field="date",
                      cluster_fields=c("team","test_type","vald_id")) {
  mode <- match.arg(mode)

  if ("position" %in% names(data)) {
    data <- data %>% select(-position)
    create_log_entry(paste("Removed position column before uploading to", table_name))
  }

  data <- standardize_data_types(data, table_name)
  
  # Validate schema and fix mismatches
  data <- validate_and_fix_schema(data, table_name, ds)
  
  tbl <- bq_table(ds, table_name)

  if (nrow(data) == 0) {
    create_log_entry(paste("No rows to upload for", table_name))
    return(TRUE)
  }

  if (!bq_table_exists(tbl)) {
    ensure_table(tbl, data, partition_field, cluster_fields)
    bq_table_upload(tbl, data, write_disposition = "WRITE_TRUNCATE")
    meta <- bq_table_meta(tbl)
    num_rows <- if (!is.null(meta$numRows)) meta$numRows else "NA"
    create_log_entry(paste("Uploaded", num_rows, "rows to new table", table_name))
    return(TRUE)
  }

  if (mode == "TRUNCATE") {
    bq_table_upload(tbl, data, write_disposition = "WRITE_TRUNCATE")
    meta <- bq_table_meta(tbl)
    num_rows <- if (!is.null(meta$numRows)) meta$numRows else "NA"
    create_log_entry(paste("Truncated+uploaded;", table_name, "now has", num_rows, "rows"))
    return(TRUE)
  }

  # MERGE (client-side)
  if (!(key %in% names(data))) {
    stop(paste("Key", key, "missing for", table_name))
  }
  existing <- read_bq_table(table_name)

  if (nrow(existing) == 0) {
    bq_table_upload(tbl, data, write_disposition = "WRITE_TRUNCATE")
  } else {
    existing[[key]] <- as.character(existing[[key]])
    data[[key]]     <- as.character(data[[key]])

    all_cols <- union(names(existing), names(data))
    for (cn in setdiff(all_cols, names(existing))) existing[[cn]] <- NA
    for (cn in setdiff(all_cols, names(data)))     data[[cn]]     <- NA
    existing <- existing[, all_cols, drop=FALSE]
    data     <- data[, all_cols, drop=FALSE]

    combined <- bind_rows(existing, data) %>%
      mutate(.row_id = row_number()) %>%
      arrange(.row_id) %>%
      group_by(.data[[key]]) %>%
      slice_tail(n = 1) %>%
      ungroup() %>%
      select(-.row_id)

    combined <- standardize_data_types(combined, table_name)
    bq_table_upload(tbl, combined, write_disposition = "WRITE_TRUNCATE")
  }

  meta <- bq_table_meta(tbl)
  num_rows <- if (!is.null(meta$numRows)) meta$numRows else "NA"
  create_log_entry(paste("MERGE (client-side) complete;", table_name, "now has", num_rows, "rows"))
  TRUE
}

# Fix RSI data type without DDL/DML
fix_rsi_data_type <- function() {
  create_log_entry("Checking body_weight_lbs type in vald_fd_rsi (DML-free)")
  tbl <- bq_table(ds, "vald_fd_rsi")
  if (!bq_table_exists(tbl)) {
    create_log_entry("Table vald_fd_rsi does not exist yet - skipping type fix")
    return(invisible(TRUE))
  }
  meta <- bq_table_meta(tbl)
  fields <- meta$schema$fields
  bw_field <- fields[sapply(fields, function(f) f$name == "body_weight_lbs")]
  if (length(bw_field) == 0) {
    create_log_entry("body_weight_lbs column missing - skipping")
    return(invisible(TRUE))
  }
  current <- read_bq_table("vald_fd_rsi")
  if (!("body_weight_lbs" %in% names(current))) {
    create_log_entry("body_weight_lbs not present in data - skipping")
    return(invisible(TRUE))
  }
  if (!is.character(current$body_weight_lbs)) {
    create_log_entry("body_weight_lbs already numeric (or coerced) - no change")
    return(invisible(TRUE))
  }
  create_log_entry("Converting body_weight_lbs STRING -> numeric locally, then WRITE_TRUNCATE")
  suppressWarnings(current$body_weight_lbs <- as.numeric(current$body_weight_lbs))
  current <- standardize_data_types(current, "vald_fd_rsi")
  bq_table_upload(tbl, current, write_disposition = "WRITE_TRUNCATE")
  create_log_entry("Converted body_weight_lbs to numeric via load job")
  invisible(TRUE)
}

# ---------- Current BQ state ----------
create_log_entry("=== READING CURRENT DATA STATE FROM BIGQUERY ===")

# Read dates table
current_dates <- read_bq_table("dates")
if (nrow(current_dates) > 0) {
  current_dates <- current_dates %>% select(date) %>% distinct() %>% mutate(date = as.Date(date))
} else {
  current_dates <- tibble(date = as.Date(character(0)))
}

# Read tests table
tests_tbl <- read_bq_table("tests")
if (nrow(tests_tbl) > 0) {
  tests_tbl <- tests_tbl %>% mutate(test_ID = as.character(test_ID)) %>% distinct()
} else {
  tests_tbl <- tibble(test_ID = character(0))
}

# Read Nordbord table for gate check
nordbord_tbl <- read_bq_table("vald_nord_all")
if (nrow(nordbord_tbl) > 0) {
  nordbord_tbl <- nordbord_tbl %>% 
    select(any_of(c("test_ID", "date"))) %>% 
    mutate(test_ID = as.character(test_ID), date = as.Date(date)) %>% 
    distinct()
} else {
  nordbord_tbl <- tibble(test_ID = character(0), date = as.Date(character(0)))
}

latest_date_current <- if (nrow(current_dates)>0) max(current_dates$date, na.rm=TRUE) else as.Date("1900-01-01")
count_tests_current <- nrow(tests_tbl)
latest_nord_date_current <- if (nrow(nordbord_tbl)>0) max(nordbord_tbl$date, na.rm=TRUE) else as.Date("1900-01-01")
count_nord_tests_current <- nrow(nordbord_tbl)

create_log_entry(paste("Current ForceDecks state - Latest date:", latest_date_current, "Test count:", count_tests_current))
create_log_entry(paste("Current Nordbord state - Latest date:", latest_nord_date_current, "Test count:", count_nord_tests_current))

# ---------- Read roster early for use in gate check and later processing ----------
create_log_entry("=== READING ROSTER DATA ===")
Vald_roster_backfill <- read_bq_table("vald_roster")

if (nrow(Vald_roster_backfill) > 0) {
  if ("team" %in% names(Vald_roster_backfill)) {
    Vald_roster_backfill <- Vald_roster_backfill %>% select(-team)
    create_log_entry("Removed existing team column (will use category_1)")
  }
  if ("position" %in% names(Vald_roster_backfill)) {
    Vald_roster_backfill <- Vald_roster_backfill %>% select(-position)
    create_log_entry("Removed existing position column (will use group_1)")
  }
  if ("category_1" %in% names(Vald_roster_backfill)) {
    Vald_roster_backfill <- Vald_roster_backfill %>% rename(team = category_1)
    create_log_entry("Renamed category_1 to team in vald_roster")
  } else {
    create_log_entry("Warning: category_1 column not found in vald_roster", "WARN")
  }
  if ("group_1" %in% names(Vald_roster_backfill)) {
    Vald_roster_backfill <- Vald_roster_backfill %>% rename(position = group_1)
    create_log_entry("Renamed group_1 to position in vald_roster")
  } else {
    create_log_entry("Warning: group_1 column not found in vald_roster", "WARN")
  }
}

Vald_roster <- Vald_roster_backfill
if (nrow(Vald_roster)==0) create_log_entry("No vald_roster in BQ; proceeding without team/position", "WARN")

# ---------- VALD API Setup ----------
create_log_entry("=== CONFIGURING VALD API ===")
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
  upload_logs_to_bigquery()
  upload_schema_mismatches()
  quit(status = 1)
})

# ---------- Gate: run only if EITHER ForceDecks OR Nordbord data changed ----------
create_log_entry("=== PROBING VALD APIs FOR CHANGES ===")

# Probe ForceDecks API
create_log_entry("Probing ForceDecks API for tests-only to compare date & count")
all_tests_probe <- safe_vald_fetch(
  fetch_function = get_forcedecks_tests_only,
  description = "ForceDecks tests probe",
  timeout_seconds = 600,
  max_same_cursor = 3,
  start_date = NULL
)

probe_df <- as_tibble(all_tests_probe) %>%
  mutate(
    recorded_parsed = suppressWarnings(lubridate::ymd_hms(recordedDateUtc, tz = "UTC", quiet = TRUE)),
    recorded_local = dplyr::if_else(
      recordedDateTimezone %in% c("Pacific Standard Time","Pacific Daylight Time","Pacific Time"),
      with_tz(recorded_parsed, "America/Los_Angeles"),
      recorded_parsed
    ),
    date = as.Date(recorded_local),
    test_ID = as.character(testId)
  )

api_latest_date <- suppressWarnings(max(probe_df$date, na.rm = TRUE))
api_test_count <- dplyr::n_distinct(probe_df$test_ID)

create_log_entry(paste("ForceDecks API - Latest date:", api_latest_date, "Test count:", api_test_count))

fd_date_mismatch  <- !identical(api_latest_date, latest_date_current)
fd_count_mismatch <- api_test_count != count_tests_current

create_log_entry(paste("ForceDecks - date_mismatch:", fd_date_mismatch, "count_mismatch:", fd_count_mismatch))

# Probe Nordbord API
create_log_entry("Probing Nordbord API for tests to compare date & count")
nordbord_probe <- safe_vald_fetch(
  fetch_function = function() {
    result <- get_nordbord_data(start_date = NULL)
    return(result$tests)
  },
  description = "Nordbord tests probe",
  timeout_seconds = 300,
  max_same_cursor = 3
)

if (nrow(nordbord_probe) > 0) {
  nordbord_probe_df <- as_tibble(nordbord_probe) %>%
    mutate(
      modifiedDateUtc_chr = as.character(modifiedDateUtc),
      modifiedDateUtc_parsed = coalesce(
        ymd_hms(modifiedDateUtc_chr, tz="UTC", quiet=TRUE),
        ymd_hm(modifiedDateUtc_chr,  tz="UTC", quiet=TRUE),
        ymd_h(modifiedDateUtc_chr,   tz="UTC", quiet=TRUE),
        ymd(modifiedDateUtc_chr,     tz="UTC")
      ),
      modifiedDateUtc_local = with_tz(modifiedDateUtc_parsed, "America/Los_Angeles"),
      date = as.Date(modifiedDateUtc_local),
      test_ID = as.character(testId)
    )
  
  nord_api_latest_date <- suppressWarnings(max(nordbord_probe_df$date, na.rm = TRUE))
  nord_api_test_count <- dplyr::n_distinct(nordbord_probe_df$test_ID)
} else {
  nord_api_latest_date <- as.Date("1900-01-01")
  nord_api_test_count <- 0
}

create_log_entry(paste("Nordbord API - Latest date:", nord_api_latest_date, "Test count:", nord_api_test_count))

nord_date_mismatch  <- !identical(nord_api_latest_date, latest_nord_date_current)
nord_count_mismatch <- nord_api_test_count != count_nord_tests_current

create_log_entry(paste("Nordbord - date_mismatch:", nord_date_mismatch, "count_mismatch:", nord_count_mismatch))

# Decide whether to proceed
any_changes <- fd_date_mismatch || fd_count_mismatch || nord_date_mismatch || nord_count_mismatch

if (!any_changes) {
  create_log_entry("No changes detected in ForceDecks or Nordbord - all dates and counts match. Exiting.")
  create_log_entry("=== VALD DATA PROCESSING SCRIPT ENDED ===", "END")
  upload_logs_to_bigquery()
  upload_schema_mismatches()
  try(DBI::dbDisconnect(con), silent=TRUE)
  quit(status=0)
}

create_log_entry(paste("Changes detected - Running update"))
create_log_entry(paste("  ForceDecks changes:", fd_date_mismatch || fd_count_mismatch))
create_log_entry(paste("  Nordbord changes:", nord_date_mismatch || nord_count_mismatch))

# ---------- Backfill missing team data ----------
create_log_entry("=== CHECKING FOR INCOMPLETE TEAM DATA ===")
total_backfilled <- 0
if (nrow(Vald_roster_backfill) > 0) {
  tables_to_check <- c("vald_fd_jumps", "vald_fd_dj", "vald_fd_rsi", "vald_fd_rebound", 
                       "vald_fd_sl_jumps", "vald_fd_imtp", "vald_nord_all")
  for (table_name in tables_to_check) {
    tryCatch({
      existing_data <- read_bq_table(table_name)
      if (nrow(existing_data) == 0) {
        create_log_entry(paste("Table", table_name, "is empty - skipping backfill check"))
        next
      }
      has_test_id <- "test_ID" %in% names(existing_data)
      has_vald_id <- "vald_id" %in% names(existing_data)
      has_team <- "team" %in% names(existing_data)
      if (!has_test_id || !has_vald_id) {
        create_log_entry(paste("Skipping", table_name, "- missing test_ID or vald_id columns"), "WARN")
        next
      }
      if (!has_team) {
        create_log_entry(paste("Table", table_name, "has no team column - skipping"), "INFO")
        next
      }
      missing_info <- existing_data %>%
        filter(is.na(team) | as.character(team) == "" | as.character(team) == "NA") %>%
        select(test_ID, vald_id) %>% distinct()
      if (nrow(missing_info) > 0) {
        create_log_entry(paste("Found", nrow(missing_info), "records in", table_name, "missing team"))
        roster_updates <- missing_info %>%
          left_join(Vald_roster_backfill %>% select(vald_id, team), by = "vald_id") %>%
          filter(!is.na(team))
        if (nrow(roster_updates) > 0) {
          create_log_entry(paste("Found team data for", nrow(roster_updates), "records in", table_name))
          records_to_update <- existing_data %>%
            filter(test_ID %in% roster_updates$test_ID) %>%
            select(-team) %>%
            left_join(Vald_roster_backfill %>% select(vald_id, team), by = "vald_id") %>%
            select(-any_of("position"))
          if (nrow(records_to_update) > 0) {
            create_log_entry(paste("Updating", nrow(records_to_update), "records in", table_name))
            remaining <- existing_data %>% filter(!(test_ID %in% records_to_update$test_ID))
            new_table <- bind_rows(remaining, records_to_update)
            bq_upsert(new_table, table_name, key = "test_ID", mode = "TRUNCATE",
                      partition_field = "date", cluster_fields = c("team", "vald_id"))
            create_log_entry(paste("Successfully backfilled team for", nrow(records_to_update), "records in", table_name))
            total_backfilled <- total_backfilled + nrow(records_to_update)
          }
        } else {
          create_log_entry(paste("No matching team data found for", table_name, "missing records"), "WARN")
        }
      } else {
        create_log_entry(paste("All records in", table_name, "have team data"))
      }
    }, error = function(e) {
      create_log_entry(paste("Error during backfill for", table_name, ":", e$message), "WARN")
    })
  }
  if (total_backfilled > 0) {
    create_log_entry(paste("=== BACKFILL COMPLETE: Updated", total_backfilled, "total records across all tables ==="))
  } else {
    create_log_entry("=== BACKFILL COMPLETE: No records needed updating ===")
  }
} else {
  create_log_entry("No vald_roster found - skipping team backfill", "WARN")
}

# ---------- Overlap start date (earliest date across both sources - 1 day), then fetch ----------
overlap_days <- 1L
start_dt <- min(latest_date_current, latest_nord_date_current) - lubridate::days(overlap_days)
set_start_date(sprintf("%sT00:00:00Z", start_dt))
create_log_entry(paste("Running with overlap start:", start_dt, "T00:00:00Z"))

create_log_entry("Fetching ForceDecks data from VALD API...")

# Use safe fetch with timeout protection and circuit breaker
injest_fd <- safe_vald_fetch(
  fetch_function = get_forcedecks_data,
  description = "ForceDecks full data",
  timeout_seconds = 900,
  max_same_cursor = 3
)

profiles <- as.data.table(injest_fd$profiles)
definitions <- as.data.table(injest_fd$result_definitions)
tests <- as.data.table(injest_fd$tests)
trials <- as.data.table(injest_fd$trials)

roster <- as_tibble(profiles)[, c("profileId","givenName","familyName")] %>%
  mutate(full_name = paste(trimws(givenName), trimws(familyName)),
         vald_id = as.character(profileId)) %>%
  select(vald_id, full_name)

tests_processed <- as_tibble(tests) %>%
  mutate(
    vald_id = as.character(profileId),
    test_type = testType,
    test_ID = as.character(testId),
    recordedDateUtc_parsed = lubridate::ymd_hms(recordedDateUtc, tz = "UTC", quiet = TRUE),
    recordedDateUtc_local = dplyr::if_else(
      recordedDateTimezone %in% c("Pacific Standard Time","Pacific Daylight Time","Pacific Time"),
      lubridate::with_tz(recordedDateUtc_parsed, "America/Los_Angeles"),
      recordedDateUtc_parsed
    ),
    date = as.Date(recordedDateUtc_local),
    time = hms::as_hms(recordedDateUtc_local)
  ) %>%
  select(-testId, -tenantId, -profileId, -testType, -weight,
         -analysedDateUtc, -analysedDateOffset, -analysedDateTimezone,
         -recordedDateUtc_parsed, -recordedDateUtc_local, -recordedDateUtc,
         -recordedDateOffset, -recordedDateTimezone, -recordingId)

new_test_types <- sort(unique(tests_processed$test_type))
create_log_entry(paste("New ForceDecks test types present:", paste(new_test_types, collapse = ", ")))

trials_wider <- as_tibble(trials) %>%
  tidyr::pivot_wider(
    id_cols = c(testId, trialId, athleteId, recordedUTC, recordedTimezone, trialLimb),
    names_from = definition_result, values_from = value, values_fn = dplyr::first
  ) %>%
  mutate(
    recordedUTC_parsed = ymd_hms(recordedUTC, tz="UTC", quiet=TRUE),
    recordedUTC_local = dplyr::if_else(
      recordedTimezone %in% c("Pacific Standard Time","Pacific Daylight Time","Pacific Time"),
      lubridate::with_tz(recordedUTC_parsed, "America/Los_Angeles"),
      recordedUTC_parsed
    ),
    date = as.Date(recordedUTC_local), time = hms::as_hms(recordedUTC_local)
  ) %>%
  select(-recordedUTC_parsed, -recordedUTC_local) %>%
  rename_with(tolower)

if ("start_of_movement" %in% names(trials_wider)) {
  start_col <- which(names(trials_wider) == "start_of_movement")
  num_cols <- names(trials_wider)[start_col:ncol(trials_wider)]
  trials_wider <- trials_wider %>% mutate(across(all_of(num_cols), as.numeric))
  create_log_entry(paste("Converted", ncol(trials_wider) - start_col + 1, "metric columns to numeric"))
} else {
  create_log_entry("Warning: start_of_movement column not found - cannot identify metric columns", "WARN")
}

mergable_trials <- trials_wider %>%
  mutate(test_ID = as.character(testid)) %>%
  group_by(test_ID) %>%
  summarise(across(where(is.numeric), ~mean(.x, na.rm=TRUE)),
            triallimb = first(triallimb),
            date = first(date), time = first(time),
            athleteid = first(athleteid), .groups="drop") %>%
  mutate(vald_id = as.character(athleteid)) %>% select(-athleteid)

mergable_roster <- roster %>% 
  left_join(
    Vald_roster %>% select(any_of(c("vald_id", "position", "sport", "team"))), 
    by = "vald_id"
  )

forcedecks_raw <- mergable_trials %>%
  left_join(tests_processed %>% select(test_ID, test_type), by="test_ID") %>%
  left_join(mergable_roster, by="vald_id") %>%
  mutate(date = as.Date(date), time = hms::as_hms(time), test_ID = as.character(test_ID))

clean_column_names <- function(df) {
  names(df) <- names(df) %>%
    gsub("([a-z])([A-Z])", "\\1_\\2", .) %>%
    tolower() %>%
    gsub("[^a-zA-Z0-9_]", "_", .) %>%
    gsub("_{2,}", "_", .) %>%
    gsub("^_|_$", "", .)
  df
}

forcedecks_raw <- clean_column_names(forcedecks_raw)

if ("test_id" %in% names(forcedecks_raw) && !("test_ID" %in% names(forcedecks_raw))) {
  forcedecks_raw <- forcedecks_raw %>% dplyr::rename(test_ID = test_id)
}

first_50_cols <- head(names(forcedecks_raw), 50)
create_log_entry(paste("forcedecks_raw columns (first 50):", paste(first_50_cols, collapse = ", ")))
create_log_entry(paste("forcedecks_raw total columns:", length(names(forcedecks_raw))))

if ("body_weight_lbs" %in% names(forcedecks_raw)) {
  create_log_entry("Processing body weight lookup table")
  bw <- forcedecks_raw %>%
    select(vald_id, date, body_weight_lbs) %>%
    filter(!is.na(body_weight_lbs)) %>% group_by(vald_id, date) %>%
    summarise(body_weight_lbs = mean(body_weight_lbs, na.rm=TRUE), .groups="drop") %>%
    arrange(vald_id, date)
  create_log_entry(paste("Created body weight lookup with", nrow(bw), "records"))
  attach_bw <- function(df) {
    if (!all(c("vald_id","date") %in% names(df))) return(df)
    if (nrow(bw)==0) {
      create_log_entry("No body weight data available for attachment", "WARN")
      return(df)
    }
    dt <- as.data.table(df); bw_dt <- as.data.table(bw)
    setkey(bw_dt, vald_id, date); setkey(dt, vald_id, date)
    dt[bw_dt, body_weight_lbs := i.body_weight_lbs, roll=Inf, on=.(vald_id, date)]
    as_tibble(dt)
  }
} else {
  create_log_entry("No body_weight_lbs column in forcedecks_raw - skipping body weight lookup creation")
  attach_bw <- function(df) df
}

# -------- Sections (gated) - All now with schema validation --------
# CMJ: TRUNCATE + 4-layer validation (added)
if (any(new_test_types %in% c("CMJ","LCMJ","SJ","ABCMJ"))) {
  create_log_entry("Processing CMJ/LCMJ/SJ/ABCMJ with 4-layer validation")
  cmj_temp <- forcedecks_raw %>% filter(test_type %in% c("CMJ","LCMJ","SJ","ABCMJ"))
  create_log_entry(paste("cmj_temp row count:", nrow(cmj_temp)))
  create_log_entry(paste("cmj_temp columns:", paste(names(cmj_temp), collapse = ", ")))
  if (nrow(cmj_temp) == 0) {
    create_log_entry("No CMJ test data found after filtering - skipping CMJ processing", "WARN")
  } else if (!"jump_height_inches_imp_mom" %in% names(cmj_temp)) {
    create_log_entry("CMJ data missing required column jump_height_inches_imp_mom - skipping CMJ processing", "WARN")
  } else {
    cmj_new <- cmj_temp %>%
      select(any_of(c(
        "test_ID","vald_id","full_name","team","test_type","date","time",
        "countermovement_depth","jump_height_inches_imp_mom","bodymass_relative_takeoff_power",
        "mean_landing_power","mean_eccentric_force","mean_takeoff_acceleration","mean_ecc_con_ratio",
        "mean_takeoff_velocity","peak_landing_velocity","peak_takeoff_force","peak_takeoff_velocity",
        "concentric_rfd_100","start_to_peak_force_time","contraction_time","concentric_duration",
        "eccentric_concentric_duration_ratio","flight_eccentric_time_ratio","displacement_at_takeoff",
        "rsi_modified_imp_mom","positive_takeoff_impulse","positive_impulse","concentric_impulse",
        "eccentric_braking_impulse","total_work","relative_peak_landing_force","relative_peak_concentric_force",
        "relative_peak_eccentric_force","bm_rel_force_at_zero_velocity","landing_impulse","force_at_zero_velocity",
        "cmj_stiffness","braking_phase_duration","takeoff_velocity","eccentric_time","peak_landing_acceleration",
        "peak_takeoff_acceleration","concentric_rfd_200","eccentric_peak_power","body_weight_lbs"
      ))) %>%
      filter(!is.na(jump_height_inches_imp_mom)) %>% arrange(full_name, test_type, date)

    cmj_existing <- read_bq_table("vald_fd_jumps")
    if (nrow(cmj_existing) > 0) {
      cmj_existing <- cmj_existing %>%
        mutate(test_ID = as.character(test_ID)) %>%
        select(-any_of("position"))
    }

    cmj_all <- bind_rows(cmj_existing, cmj_new) %>% distinct(test_ID, .keep_all = TRUE) %>% arrange(full_name, test_type, date)

    # --- 4-layer validation pipeline on combined history (before 5-28in filter) ---
    cmj_all_enh <- cmj_all %>%
      mutate(
        event_datetime = as.POSIXct(paste(as.character(date), time), tz = "America/Los_Angeles"),
        jump_height_m  = jump_height_inches_imp_mom * 0.0254,
        takeoff_v_ms   = dplyr::coalesce(peak_takeoff_velocity, takeoff_velocity)
      ) %>%
      group_by(vald_id) %>% arrange(date, time, .by_group = TRUE) %>%
      mutate(session_id = dplyr::if_else(is.na(vald_id) | is.na(event_datetime),
                                         NA_character_,
                                         create_session_ids(vald_id, event_datetime))) %>%
      ungroup()

    cmj_all_qc <- cmj_all_enh %>%
      apply_layer1() %>%
      apply_layer2() %>%
      apply_layer3() %>%
      apply_layer4() %>%
      apply_final_and_clean()

    # --- Preserve existing readiness/performance calculations and range filter ---
    fd <- cmj_all %>% 
      arrange(full_name, test_type, date) %>%
      group_by(full_name, test_type) %>%
      arrange(date, .by_group = TRUE) %>%
      mutate(
        mean_30d_jh = slide_index_dbl(jump_height_inches_imp_mom, date, ~mean(.x, na.rm = TRUE), .before = days(30), .complete = FALSE),
        sd_30d_jh   = slide_index_dbl(jump_height_inches_imp_mom, date, ~sd(.x, na.rm = TRUE), .before = days(30), .complete = FALSE),
        zscore_jump_height_inches_imp_mom = if_else(sd_30d_jh > 0, (jump_height_inches_imp_mom - mean_30d_jh)/sd_30d_jh, NA_real_),
        mean_30d_rpcf = slide_index_dbl(relative_peak_concentric_force, date, ~mean(.x, na.rm = TRUE), .before = days(30), .complete = FALSE),
        sd_30d_rpcf   = slide_index_dbl(relative_peak_concentric_force, date, ~sd(.x, na.rm = TRUE), .before = days(30), .complete = FALSE),
        zscore_relative_peak_concentric_force = if_else(sd_30d_rpcf > 0, (relative_peak_concentric_force - mean_30d_rpcf)/sd_30d_rpcf, NA_real_),
        mean_30d_rsi = slide_index_dbl(rsi_modified_imp_mom, date, ~mean(.x, na.rm = TRUE), .before = days(30), .complete = FALSE),
        sd_30d_rsi   = slide_index_dbl(rsi_modified_imp_mom, date, ~sd(.x, na.rm = TRUE), .before = days(30), .complete = FALSE),
        zscore_rsi_modified_imp_mom = if_else(sd_30d_rsi > 0, (rsi_modified_imp_mom - mean_30d_rsi)/sd_30d_rsi, NA_real_)
      ) %>% ungroup() %>%
      group_by(full_name) %>%
      arrange(date, .by_group = TRUE) %>%
      mutate(
        cmj_mask = (test_type == "CMJ"),
        jh_cmj_mean_30d = slide_index_dbl(if_else(cmj_mask, jump_height_inches_imp_mom, NA_real_), date, ~mean(.x, na.rm=TRUE), .before = days(30)),
        rsi_cmj_mean_30d = slide_index_dbl(if_else(cmj_mask, rsi_modified_imp_mom, NA_real_), date, ~mean(.x, na.rm=TRUE), .before = days(30)),
        epf_cmj_mean_30d = slide_index_dbl(if_else(cmj_mask, relative_peak_eccentric_force, NA_real_), date, ~mean(.x, na.rm=TRUE), .before = days(30)),
        jump_height_readiness = if_else(!is.na(jump_height_inches_imp_mom) & !is.na(jh_cmj_mean_30d) & jh_cmj_mean_30d != 0,
                                        (jump_height_inches_imp_mom - jh_cmj_mean_30d)/jh_cmj_mean_30d, NA_real_),
        rsi_readiness = if_else(!is.na(rsi_modified_imp_mom) & !is.na(rsi_cmj_mean_30d) & rsi_cmj_mean_30d != 0,
                                (rsi_modified_imp_mom - rsi_cmj_mean_30d)/rsi_cmj_mean_30d, NA_real_),
        epf_readiness = if_else(!is.na(relative_peak_eccentric_force) & !is.na(epf_cmj_mean_30d) & epf_cmj_mean_30d != 0,
                                (relative_peak_eccentric_force - epf_cmj_mean_30d)/epf_cmj_mean_30d, NA_real_)
      ) %>%
      ungroup() %>%
      filter(between(jump_height_inches_imp_mom, 5, 28)) %>%
      select(-starts_with("mean_30d_"), -starts_with("sd_30d_"), -cmj_mask,
             -starts_with("jh_cmj_"), -starts_with("rsi_cmj_"), -starts_with("epf_cmj_"),
             -starts_with("zscore_"))

    if (all(c("jump_height_inches_imp_mom","relative_peak_eccentric_force",
              "bodymass_relative_takeoff_power","rsi_modified_imp_mom") %in% names(fd))) {
      fd <- fd %>%
        mutate(calc_performance_score =
                 percent_rank(jump_height_inches_imp_mom) * 100 +
                 percent_rank(relative_peak_eccentric_force) * 100 +
                 percent_rank(bodymass_relative_takeoff_power) * 100 +
                 percent_rank(rsi_modified_imp_mom) * 100,
               performance_score = percent_rank(calc_performance_score) * 100) %>%
        group_by(team) %>% mutate(team_performance_score = percent_rank(calc_performance_score) * 100) %>%
        ungroup() %>% select(-calc_performance_score)
    }

    temp_cols_to_remove <- c(
      "zscore_jump_height_inches_imp_mom","zscore_relative_peak_concentric_force","zscore_rsi_modified_imp_mom",
      "mean_30d_jh","sd_30d_jh","mean_30d_rpcf","sd_30d_rpcf","mean_30d_rsi","sd_30d_rsi",
      "jh_cmj_mean_30d","rsi_cmj_mean_30d","epf_cmj_mean_30d","cmj_mask","calc_performance_score"
    )
    fd <- fd %>% select(-any_of(temp_cols_to_remove))

    bq_columns <- c("test_ID","vald_id","full_name","team","test_type","date","time",
                    "countermovement_depth","jump_height_inches_imp_mom","bodymass_relative_takeoff_power",
                    "mean_landing_power","mean_eccentric_force","mean_takeoff_acceleration",
                    "mean_ecc_con_ratio","mean_takeoff_velocity","peak_landing_velocity",
                    "peak_takeoff_force","peak_takeoff_velocity","concentric_rfd_100",
                    "start_to_peak_force_time","contraction_time","concentric_duration",
                    "eccentric_concentric_duration_ratio","flight_eccentric_time_ratio",
                    "displacement_at_takeoff","rsi_modified_imp_mom","positive_takeoff_impulse",
                    "positive_impulse","concentric_impulse","eccentric_braking_impulse",
                    "total_work","relative_peak_landing_force","relative_peak_concentric_force",
                    "relative_peak_eccentric_force","bm_rel_force_at_zero_velocity",
                    "landing_impulse","force_at_zero_velocity","cmj_stiffness",
                    "braking_phase_duration","takeoff_velocity","eccentric_time",
                    "peak_landing_acceleration","peak_takeoff_acceleration","concentric_rfd_200",
                    "eccentric_peak_power","jump_height_readiness","rsi_readiness",
                    "epf_readiness","performance_score","team_performance_score","body_weight_lbs")
    fd <- fd %>% select(any_of(bq_columns))

    # Upload primary table unchanged (backward compatible)
    bq_upsert(fd, "vald_fd_jumps", key="test_ID", mode="TRUNCATE",
              partition_field="date", cluster_fields=c("team","test_type","vald_id"))

    # Upload QC companion table with new validation columns
    qc_cols <- c(
      "test_ID","vald_id","team","test_type","date","session_id",
      "flag_bw_delta","flag_physics_violation","flag_session_contamination","flag_multiple_extremes",
      "z_jh","z_bw","z_rsi","z_v","z_resid","physics_flag","final_classification",
      "jump_height_clean_inches","takeoff_v_ms_clean","rsi_modified_imp_mom_clean"
    )
    cmj_qc <- cmj_all_qc %>% dplyr::select(dplyr::any_of(qc_cols)) %>% dplyr::distinct(test_ID, .keep_all = TRUE)

    bq_upsert(cmj_qc, "vald_fd_jumps_qc", key="test_ID", mode="MERGE",
              partition_field="date", cluster_fields=c("team","test_type","vald_id"))

    validation_summary <- cmj_qc %>% group_by(final_classification) %>% summarise(count = n(), .groups = "drop") %>%
      mutate(pct = round(100*count/sum(count), 1))
    create_log_entry(paste("Validation results:", 
      paste(paste0(validation_summary$final_classification, " ", validation_summary$pct, "%"), collapse = ", ")))
  }
} else {
  create_log_entry("No new CMJ-family tests - skipping CMJ section")
}

# DJ: MERGE
if ("DJ" %in% new_test_types) {
  create_log_entry("Processing DJ")
  dj_new <- forcedecks_raw %>% filter(test_type=="DJ") %>%
    select(any_of(c(
      "test_ID","vald_id","full_name","position","team","date","time","body_weight_lbs",
      "peak_takeoff_velocity","peak_landing_velocity","countermovement_depth","peak_landing_force",
      "eccentric_time","jump_height_inches_imp_mom","bm_rel_force_at_zero_velocity","contact_time",
      "contact_velocity","active_stiffness","passive_stiffness","reactive_strength_index",
      "positive_takeoff_impulse","coefficient_of_restitution","active_stiffness_index",
      "passive_stiffness_index","peak_impact_force","peak_driveoff_force"
    ))) %>%
    filter(!is.na(jump_height_inches_imp_mom), jump_height_inches_imp_mom>2, jump_height_inches_imp_mom<30) %>%
    mutate(
      velocity_ratio = if_else(!is.na(peak_takeoff_velocity) & !is.na(peak_landing_velocity) & peak_landing_velocity != 0,
                               peak_takeoff_velocity/peak_landing_velocity, NA_real_),
      force_ratio    = if_else(!is.na(peak_impact_force) & !is.na(peak_driveoff_force) & peak_driveoff_force != 0,
                               peak_impact_force/peak_driveoff_force, NA_real_),
      stiffness_ratio = if_else(!is.na(active_stiffness) & !is.na(passive_stiffness) & passive_stiffness != 0,
                                active_stiffness/passive_stiffness, NA_real_)
    )
  bq_upsert(dj_new, "vald_fd_dj", key="test_ID", mode="MERGE",
            partition_field="date", cluster_fields=c("team","vald_id"))
} else {
  create_log_entry("No new DJ tests - skipping DJ section")
}

# RSI: MERGE
if (any(new_test_types %in% c("RSAIP","RSHIP","RSKIP"))) {
  create_log_entry("Processing RSI")
  rsi_new <- trials_wider %>%
    mutate(test_ID = as.character(testid), vald_id = athleteid) %>%
    left_join(tests_processed %>% select(test_ID, test_type), by="test_ID") %>%
    filter(test_type %in% c("RSAIP","RSHIP","RSKIP")) %>%
    select(-testid, -athleteid) %>% left_join(mergable_roster, by="vald_id") %>%
    mutate(date=as.Date(date), time=hms::as_hms(time)) %>%
    select(any_of(c(
      "triallimb","test_ID","test_type","vald_id","full_name","position","team","date","time","body_weight_lbs",
      "start_to_peak_force","peak_vertical_force","rfd_at_100ms","rfd_at_250ms",
      "iso_bm_rel_force_peak","iso_bm_rel_force_100","iso_bm_rel_force_200","iso_abs_impulse_100"
    ))) %>%
    group_by(test_ID, vald_id, test_type, triallimb) %>%
    summarise(across(any_of(c("full_name","position","team")), first),
              across(any_of(c("date","time","body_weight_lbs")), first),
              across(where(is.numeric), ~mean(.x, na.rm=TRUE)), .groups="drop") %>%
    tidyr::pivot_wider(
      id_cols = any_of(c("test_ID","vald_id","test_type","full_name","position","team","date","time","body_weight_lbs")),
      names_from = triallimb, values_from = -c(test_ID, vald_id, test_type, full_name, position, team, date, time, body_weight_lbs, triallimb),
      names_sep = "_"
    ) %>%
    rename_with(~str_replace(.x, "_Left$", "_left")) %>%
    rename_with(~str_replace(.x, "_Right$", "_right"))
  bq_upsert(rsi_new, "vald_fd_rsi", key="test_ID", mode="MERGE",
            partition_field="date", cluster_fields=c("team","vald_id"))
} else {
  create_log_entry("No new RSI tests - skipping RSI section")
}

# Rebound: MERGE
if (any(new_test_types %in% c("CMRJ","SLCMRJ"))) {
  create_log_entry("Processing Rebound")
  rebound_new <- trials_wider %>%
    mutate(test_ID = as.character(testid), vald_id = athleteid) %>%
    left_join(tests_processed %>% select(test_ID, test_type), by="test_ID") %>%
    filter(test_type %in% c("CMRJ","SLCMRJ")) %>%
    select(-testid, -athleteid) %>% left_join(mergable_roster, by="vald_id") %>%
    mutate(date=as.Date(date), time=hms::as_hms(time)) %>%
    select(any_of(c(
      "triallimb","test_ID","test_type","vald_id","full_name","position","team","date","time","body_weight_lbs",
      "cmrj_takeoff_bm_rel_peak_force","cmrj_takeoff_countermovement_depth",
      "cmrj_takeoff_concentric_peak_force","cmrj_takeoff_contraction_time","cmrj_takeoff_ecc_decel_impulse",
      "cmrj_takeoff_ecc_duration","cmrj_takeoff_bm_rel_ecc_peak_force","cmrj_takeoff_jump_height_imp_mom_inches",
      "cmrj_takeoff_rsi_modified_imp_mom","cmrj_rebound_active_stiffness","cmrj_rebound_active_stiffness_index",
      "cmrj_rebound_contact_time","cmrj_rebound_countermovement_depth","cmrj_rebound_passive_stiffness",
      "cmrj_rebound_passive_stiffness_index","cmrj_rebound_jump_height_imp_mom_inches"
    ))) %>%
    group_by(test_ID, vald_id, test_type, triallimb) %>%
    summarise(across(any_of(c("full_name","position","team")), first),
              across(any_of(c("date","time","body_weight_lbs")), first),
              across(where(is.numeric), ~mean(.x, na.rm=TRUE)), .groups="drop") %>%
    mutate(limb_suffix = case_when(
      test_type == "CMRJ" ~ "bilateral",
      test_type == "SLCMRJ" & triallimb == "Left" ~ "left",
      test_type == "SLCMRJ" & triallimb == "Right" ~ "right",
      TRUE ~ "bilateral"
    )) %>%
    tidyr::pivot_wider(
      id_cols = any_of(c("test_ID","vald_id","test_type","full_name","position","team","date","time","body_weight_lbs")),
      names_from = limb_suffix, values_from = -c(test_ID, vald_id, test_type, full_name, position, team, date, time, body_weight_lbs, triallimb, limb_suffix),
      names_sep = "_"
    )
  bq_upsert(rebound_new, "vald_fd_rebound", key="test_ID", mode="MERGE",
            partition_field="date", cluster_fields=c("team","vald_id"))
} else {
  create_log_entry("No new Rebound tests - skipping Rebound section")
}

# SLJ: MERGE
if ("SLJ" %in% new_test_types) {
  create_log_entry("Processing SLJ")
  slj_new <- trials_wider %>%
    mutate(test_ID = as.character(testid), vald_id = athleteid) %>%
    left_join(tests_processed %>% select(test_ID, test_type), by="test_ID") %>%
    filter(test_type == "SLJ") %>%
    select(-testid, -athleteid) %>% left_join(mergable_roster, by="vald_id") %>%
    mutate(date=as.Date(date), time=hms::as_hms(time)) %>%
    select(any_of(c(
      "triallimb","test_ID","vald_id","full_name","position","team","date","time","body_weight_lbs",
      "jump_height_inches_imp_mom",
      "peak_landing_force","peak_landing_velocity","peak_takeoff_velocity","time_to_peak_force",
      "weight_relative_peak_takeoff_force","weight_relative_peak_landing_force",
      "relative_peak_concentric_force","relative_peak_eccentric_force","lower_limb_stiffness",
      "rsi_modified_imp_mom"
    ))) %>%
    group_by(test_ID, vald_id, triallimb) %>%
    summarise(across(any_of(c("full_name","position","team")), first),
              across(any_of(c("date","time","body_weight_lbs")), first),
              across(where(is.numeric), ~mean(.x, na.rm=TRUE)), .groups="drop") %>%
    mutate(limb_suffix = case_when(
      triallimb == "Left" ~ "left",
      triallimb == "Right" ~ "right",
      triallimb == "Bilateral" ~ "bilateral",
      TRUE ~ "bilateral"
    )) %>%
    tidyr::pivot_wider(
      id_cols = any_of(c("test_ID","vald_id","full_name","position","team","date","time","body_weight_lbs")),
      names_from = limb_suffix, values_from = -c(test_ID, vald_id, full_name, position, team, date, time, body_weight_lbs, triallimb),
      names_sep = "_"
    )
  bq_upsert(slj_new, "vald_fd_sl_jumps", key="test_ID", mode="MERGE",
            partition_field="date", cluster_fields=c("team","vald_id"))
} else {
  create_log_entry("No new SLJ tests - skipping SLJ section")
}

# IMTP: MERGE
if ("IMTP" %in% new_test_types) {
  create_log_entry("Processing IMTP")
  imtp_new <- forcedecks_raw %>% filter(test_type=="IMTP") %>%
    select(any_of(c(
      "test_ID","vald_id","full_name","position","team","date","time",
      "start_to_peak_force","rfd_at_100ms","rfd_at_200ms","force_at_100ms",
      "iso_bm_rel_force_peak","peak_vertical_force","force_at_200ms"
    ))) %>%
    filter(!is.na(peak_vertical_force)) %>% arrange(full_name, date)

  if (all(c("peak_vertical_force","start_to_peak_force","rfd_at_100ms") %in% names(imtp_new))) {
    imtp_new <- imtp_new %>%
      mutate(calc_performance_score = percent_rank(peak_vertical_force) * 200 +
               (100 - percent_rank(start_to_peak_force) * 100) +
               percent_rank(rfd_at_100ms) * 100,
             performance_score = percent_rank(calc_performance_score) * 100) %>%
      group_by(team) %>% mutate(team_performance_score = percent_rank(calc_performance_score) * 100) %>%
      ungroup() %>% select(-calc_performance_score)
  }
  bq_upsert(imtp_new, "vald_fd_imtp", key="test_ID", mode="MERGE",
            partition_field="date", cluster_fields=c("team","vald_id"))
} else {
  create_log_entry("No new IMTP tests - skipping IMTP section")
}

# Nordbord: MERGE
create_log_entry("Fetching Nordbord data from VALD API...")

# Use safe fetch with timeout protection and circuit breaker
injest_nord <- safe_vald_fetch(
  fetch_function = get_nordbord_data,
  description = "Nordbord data",
  timeout_seconds = 300,
  max_same_cursor = 3
)

nord_tests <- injest_nord$tests
if (nrow(nord_tests) > 0) {
  create_log_entry(paste("Processing Nordbord (", nrow(nord_tests), " tests)"))
  nb <- as_tibble(nord_tests) %>%
    select(-any_of(c("device","notes","testTypeId"))) %>%
    mutate(
      modifiedDateUtc_chr = as.character(modifiedDateUtc),
      modifiedDateUtc_parsed = coalesce(
        ymd_hms(modifiedDateUtc_chr, tz="UTC", quiet=TRUE),
        ymd_hm(modifiedDateUtc_chr,  tz="UTC", quiet=TRUE),
        ymd_h(modifiedDateUtc_chr,   tz="UTC", quiet=TRUE),
        ymd(modifiedDateUtc_chr,     tz="UTC")
      ),
      modifiedDateUtc_local = with_tz(modifiedDateUtc_parsed, "America/Los_Angeles"),
      date = as.Date(modifiedDateUtc_local),
      time = hms::as_hms(modifiedDateUtc_local),
      vald_id = athleteId,
      test_ID = as.character(testId),
      test_type = testTypeName,
      impulse_left  = if_else(leftRepetitions==0, NA_real_, if_else(leftCalibration==0, leftImpulse, NA_real_)),
      avg_force_left = if_else(leftRepetitions==0, NA_real_, leftAvgForce - leftCalibration),
      max_force_left = if_else(leftRepetitions==0, NA_real_, leftMaxForce - leftCalibration),
      impulse_right  = if_else(rightRepetitions==0, NA_real_, if_else(rightCalibration==0, rightImpulse, NA_real_)),
      avg_force_right = if_else(rightRepetitions==0, NA_real_, rightAvgForce - rightCalibration),
      max_force_right = if_else(rightRepetitions==0, NA_real_, rightMaxForce - rightCalibration),
      reps_left  = leftRepetitions, reps_right = rightRepetitions,
      avg_force_bilateral = rowMeans(cbind(avg_force_left, avg_force_right), na.rm = TRUE),
      max_force_bilateral = rowMeans(cbind(max_force_left, max_force_right), na.rm = TRUE),
      avg_force_asymmetry = case_when(
        is.na(avg_force_left) | is.na(avg_force_right) ~ NA_real_,
        avg_force_left == 0 & avg_force_right == 0 ~ NA_real_,
        avg_force_left >= avg_force_right ~ ((avg_force_left - avg_force_right) / avg_force_left),
        TRUE ~ ((avg_force_right - avg_force_left) / avg_force_right)
      ),
      max_force_asymmetry = case_when(
        is.na(max_force_left) | is.na(max_force_right) ~ NA_real_,
        max_force_left == 0 & max_force_right == 0 ~ NA_real_,
        max_force_left >= max_force_right ~ ((max_force_left - max_force_right) / max_force_left),
        TRUE ~ ((max_force_right - max_force_left) / max_force_right)
      ),
      impulse_asymmetry = case_when(
        is.na(impulse_left) | is.na(impulse_right) ~ NA_real_,
        impulse_left == 0 & impulse_right == 0 ~ NA_real_,
        impulse_left >= impulse_right ~ ((impulse_left - impulse_right) / impulse_left),
        TRUE ~ ((impulse_right - impulse_left) / impulse_right)
      )
    ) %>%
    select(-any_of(c("modifiedDateUtc_chr","modifiedDateUtc_parsed","modifiedDateUtc_local",
                     "modifiedDateUtc","testDateUtc","athleteId","testId",
                     "leftTorque","rightTorque","leftMaxForce","rightMaxForce",
                     "leftRepetitions","rightRepetitions","testTypeName"))) %>%
    left_join(mergable_roster, by="vald_id") %>%
    attach_bw()

  if ("body_weight_lbs" %in% names(nb)) {
    nb <- nb %>%
      mutate(
        max_force_relative_bw = if_else(!is.na(body_weight_lbs) & body_weight_lbs != 0,
                                        max_force_bilateral / body_weight_lbs, NA_real_),
        max_force_left_relative_bw  = if_else(!is.na(body_weight_lbs) & body_weight_lbs != 0,
                                              max_force_left / body_weight_lbs, NA_real_),
        max_force_right_relative_bw = if_else(!is.na(body_weight_lbs) & body_weight_lbs != 0,
                                              max_force_right / body_weight_lbs, NA_real_)
      ) %>%
      select(-any_of("body_weight_lbs"))
  }
  bq_upsert(nb, "vald_nord_all", key="test_ID", mode="MERGE",
            partition_field="date", cluster_fields=c("team","vald_id"))
} else {
  create_log_entry("No Nordbord tests - skipping Nordbord section")
}

# Dates & Tests (MERGE)
dates_delta <- forcedecks_raw %>% select(date) %>% distinct()
tests_delta <- forcedecks_raw %>% select(test_ID) %>% distinct()
bq_upsert(dates_delta, "dates", key="date", mode="MERGE", partition_field="date", cluster_fields = character())
bq_upsert(tests_delta, "tests", key="test_ID", mode="MERGE", partition_field=NULL, cluster_fields = character())

# RSI fix
fix_rsi_data_type()

create_log_entry("=== SCRIPT EXECUTION SUMMARY ===")
execution_time <- round(difftime(Sys.time(), script_start_time, units="mins"), 2)
create_log_entry(paste("Total execution time:", execution_time, "minutes"))
create_log_entry("=== VALD DATA PROCESSING SCRIPT ENDED ===", "END")
upload_logs_to_bigquery()
upload_schema_mismatches()
try(DBI::dbDisconnect(con), silent = TRUE)
cat("Script completed successfully\n")
