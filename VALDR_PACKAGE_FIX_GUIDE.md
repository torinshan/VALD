# How to Fix the valdr Package - Implementation Guide

## Background

The `valdr` R package has a critical performance issue where it fetches trial data sequentially, one test at a time. This causes timeouts when processing thousands of tests.

## Current Implementation (SLOW)

The valdr package currently does something like this:

```r
get_forcedecks_data <- function(start_date = NULL) {
  # Get list of all tests
  tests <- fetch_all_tests(start_date)
  
  # Fetch trials for each test ONE AT A TIME (SLOW!)
  trials <- list()
  for (test_id in tests$testId) {
    cat("Fetching trials for Test ID:", test_id, "\n")  # This is what we see in logs
    trial_data <- fetch_trials_for_test(test_id)
    trials[[test_id]] <- trial_data
    Sys.sleep(0.1)  # Rate limiting
  }
  
  return(list(tests = tests, trials = do.call(rbind, trials)))
}
```

**Problem**: With 5,000+ tests, this takes 5,000+ seconds (83+ minutes)!

## Solution 1: Batch Fetching (RECOMMENDED)

### Step 1: Check if VALD API Supports Batch Endpoints

Check the VALD API documentation for endpoints like:
- `POST /trials/batch` with body: `{"testIds": ["id1", "id2", ...]}`
- `GET /trials?testIds=id1,id2,id3...`

### Step 2: Implement Batch Fetching

If batch endpoints exist:

```r
get_forcedecks_data_batched <- function(start_date = NULL, batch_size = 100) {
  # Get list of all tests
  tests <- fetch_all_tests(start_date)
  test_ids <- tests$testId
  
  # Split into batches
  batches <- split(test_ids, ceiling(seq_along(test_ids) / batch_size))
  
  # Fetch trials in batches
  all_trials <- list()
  for (i in seq_along(batches)) {
    batch_ids <- batches[[i]]
    cat(sprintf("Fetching batch %d/%d (%d tests)\n", i, length(batches), length(batch_ids)))
    
    # Call batch endpoint
    batch_trials <- tryCatch({
      fetch_trials_batch(batch_ids)  # NEW: Batch API call
    }, error = function(e) {
      warning(sprintf("Batch %d failed: %s", i, e$message))
      NULL
    })
    
    if (!is.null(batch_trials)) {
      all_trials[[i]] <- batch_trials
    }
    
    # Rate limiting
    if (i < length(batches)) Sys.sleep(0.5)
  }
  
  trials_df <- do.call(rbind, all_trials)
  
  return(list(tests = tests, trials = trials_df))
}

# Helper function to call batch API
fetch_trials_batch <- function(test_ids) {
  response <- httr::POST(
    url = paste0(base_url, "/trials/batch"),
    httr::add_headers(Authorization = paste("Bearer", access_token)),
    body = list(testIds = test_ids),
    encode = "json",
    httr::timeout(30)
  )
  
  if (httr::http_error(response)) {
    stop("Batch fetch failed: ", httr::content(response, "text"))
  }
  
  data <- httr::content(response, "parsed")
  return(data)
}
```

**Performance**: 5,000 tests / 100 per batch = 50 API calls at ~1 sec each = 50 seconds (100x faster!)

## Solution 2: Parallel Fetching

If batch endpoints don't exist, use parallel processing:

```r
library(future)
library(furrr)

get_forcedecks_data_parallel <- function(start_date = NULL, n_workers = 4) {
  # Get list of all tests
  tests <- fetch_all_tests(start_date)
  test_ids <- tests$testId
  
  cat(sprintf("Fetching trials for %d tests using %d parallel workers\n", 
              length(test_ids), n_workers))
  
  # Set up parallel processing
  old_plan <- future::plan()
  on.exit(future::plan(old_plan), add = TRUE)
  future::plan(multisession, workers = n_workers)
  
  # Fetch trials in parallel with progress
  trials_list <- furrr::future_map(
    test_ids,
    function(test_id) {
      tryCatch({
        fetch_trials_for_test(test_id)
      }, error = function(e) {
        warning(sprintf("Failed to fetch trials for test %s: %s", test_id, e$message))
        NULL
      })
    },
    .options = furrr_options(seed = TRUE, globals = FALSE),
    .progress = TRUE  # Show progress bar
  )
  
  # Remove NULLs (failed fetches) and combine
  trials_list <- Filter(Negate(is.null), trials_list)
  trials_df <- do.call(rbind, trials_list)
  
  return(list(tests = tests, trials = trials_df))
}
```

**Performance**: With 4 workers processing in parallel, approximately 1,250 test fetches per worker at ~1 sec each = ~1,250 seconds (21 minutes total) - still might timeout!

## Solution 3: Hybrid - Batch + Parallel

Best approach: Combine batching and parallelization:

```r
get_forcedecks_data_hybrid <- function(start_date = NULL, 
                                        batch_size = 100, 
                                        n_workers = 4) {
  # Get list of all tests
  tests <- fetch_all_tests(start_date)
  test_ids <- tests$testId
  
  # Split into batches
  batches <- split(test_ids, ceiling(seq_along(test_ids) / batch_size))
  
  cat(sprintf("Fetching trials for %d tests in %d batches using %d workers\n",
              length(test_ids), length(batches), n_workers))
  
  # Set up parallel processing
  old_plan <- future::plan()
  on.exit(future::plan(old_plan), add = TRUE)
  future::plan(multisession, workers = n_workers)
  
  # Fetch batches in parallel
  all_trials <- furrr::future_map(
    batches,
    function(batch_ids) {
      tryCatch({
        # If batch API exists, use it
        if (exists("fetch_trials_batch")) {
          fetch_trials_batch(batch_ids)
        } else {
          # Otherwise, fetch sequentially within this batch
          batch_trials <- lapply(batch_ids, fetch_trials_for_test)
          do.call(rbind, batch_trials)
        }
      }, error = function(e) {
        warning(sprintf("Batch failed: %s", e$message))
        NULL
      })
    },
    .options = furrr_options(seed = TRUE),
    .progress = TRUE
  )
  
  # Combine results
  all_trials <- Filter(Negate(is.null), all_trials)
  trials_df <- do.call(rbind, all_trials)
  
  return(list(tests = tests, trials = trials_df))
}
```

**Performance**: 50 batches / 4 workers = 12.5 API calls per worker at ~1 sec each = 13 seconds (400x faster!)

## Solution 4: Smart Incremental Sync

Only fetch trials for NEW tests:

```r
get_forcedecks_data_incremental <- function(existing_test_ids = character()) {
  # Get all tests from API
  all_tests <- fetch_all_tests(start_date = NULL)
  
  # Find NEW tests only
  new_test_ids <- setdiff(all_tests$testId, existing_test_ids)
  
  cat(sprintf("Found %d total tests, %d are new\n", 
              nrow(all_tests), length(new_test_ids)))
  
  if (length(new_test_ids) == 0) {
    return(list(tests = all_tests, trials = data.frame()))
  }
  
  # Fetch trials only for new tests (using one of the methods above)
  new_trials <- fetch_trials_parallel_or_batch(new_test_ids)
  
  return(list(tests = all_tests, trials = new_trials))
}
```

**Performance**: If only 50 new tests out of 5,000 = 50 seconds (instead of 5,000 seconds)

## Implementation Steps

### 1. Find the valdr Package Source

```bash
# Check where valdr is installed from
R -e "packageDescription('valdr')"

# Look for GitHub repository or source URL
# Common locations:
# - https://github.com/[user]/valdr
# - https://cran.r-project.org/package=valdr
```

### 2. Fork or Clone

```bash
# If on GitHub
gh repo fork [original-repo] --clone

# Or clone directly
git clone https://github.com/[original-repo]/valdr.git
cd valdr
```

### 3. Locate the Function

```bash
# Find where get_forcedecks_data is defined
grep -r "get_forcedecks_data" .

# Typical location: R/forcedecks.R or R/api.R
```

### 4. Implement the Fix

Replace the sequential fetching code with one of the solutions above.

### 5. Test Locally

```r
# In R, install your local version
devtools::install_local("/path/to/valdr")

# Test with a small dataset first
library(valdr)
set_credentials(...)
data <- get_forcedecks_data(start_date = "2026-01-01T00:00:00Z")
```

### 6. Update the Workflow

If you fork valdr to your own GitHub:

```yaml
# In .github/workflows/run-every-15m.yml
extra-packages: |
  DBI
  bigrquery
  ...
  torinshan/valdr  # Your forked version
```

### 7. Deploy and Monitor

```bash
# Push changes
git add .
git commit -m "Fix pagination performance issue"
git push

# Monitor workflow execution time
gh run watch
```

## Testing Checklist

- [ ] Test with 10 tests (should take <10 seconds)
- [ ] Test with 100 tests (should take <30 seconds)
- [ ] Test with 1,000 tests (should take <5 minutes)
- [ ] Test with 5,000 tests (should take <15 minutes)
- [ ] Verify all data is fetched correctly
- [ ] Check for memory issues with large batches
- [ ] Verify error handling works (network failures, etc.)

## Common Pitfalls

1. **Rate Limiting**: VALD API might have rate limits. Add `Sys.sleep()` between batches if needed.

2. **Memory Issues**: Large batches might cause memory problems. Start with smaller batch sizes (50-100).

3. **Network Errors**: Always wrap API calls in `tryCatch()` and implement retry logic.

4. **Incomplete Data**: Verify that batch fetching returns ALL trial data, not just summaries.

5. **API Authentication**: Ensure authentication tokens are passed correctly in parallel workers.

## Alternative: Custom VALD API Client

If modifying valdr is too complex, create a custom client:

```r
# .github/scripts/vald_api_client.R

fetch_forcedecks_fast <- function(credentials, start_date) {
  # Authenticate
  token <- authenticate_vald(credentials)
  
  # Get all tests
  tests <- fetch_all_tests_paginated(token, start_date)
  
  # Fetch trials in batches/parallel
  trials <- fetch_all_trials_batched(token, tests$testId, batch_size = 100)
  
  return(list(tests = tests, trials = trials))
}
```

Then update `run.R` to use this instead of valdr.

## Support

If you need help:
- **VALD API Documentation**: Check for batch endpoints and rate limits
- **R Parallel Processing**: See `?future::plan` and `?furrr::future_map`
- **Package Development**: See https://r-pkgs.org/

---

**Status**: IMPLEMENTATION GUIDE READY
**Complexity**: MEDIUM
**Est. Time**: 4-8 hours for experienced R developer
