# VALD API Timeout Fix - Root Cause Analysis and Solutions

## Problem Statement
The BigQuery every 15-minute workflow (`run-every-15m.yml`) is failing with a timeout error after 900 seconds (15 minutes) while fetching ForceDecks data from the VALD API.

### Error Message
```
[ERROR] ForceDecks full data fetch TIMEOUT after 900 seconds
[ERROR] This indicates a VALD API pagination bug or network issue
[ERROR] === VALD API FETCH FAILED ===
```

## Root Cause Analysis

### Primary Issue: Sequential Trial Fetching
The `valdr` R package's `get_forcedecks_data()` function fetches trials for each test **sequentially**, one test at a time. With 5,494 tests in the system (as shown in logs), this sequential approach causes severe performance degradation:

- **Time per test**: ~0.5-1 second per test (based on log timestamps)
- **Total time for 5,494 tests**: 2,747 - 5,494 seconds (46-92 minutes)
- **Current timeout**: 900 seconds (15 minutes)

### Log Evidence
```
2026-01-07T01:15:08.5515310Z Fetching trials for Test ID: 10804baa-9b13-4b16-9690-22f1333d8c69
2026-01-07T01:15:09.5115622Z Fetching trials for Test ID: 54c34db0-9f38-4239-bb1b-22c3ef48c33b
2026-01-07T01:15:10.4059818Z Fetching trials for Test ID: 7dd37bca-5b0c-42c7-848d-9d32cc90dac4
...
[continues for 900 seconds until timeout]
```

Each test takes approximately 1 second to fetch, demonstrating the sequential nature of the operation.

### Secondary Issues
1. **Overlap Window Too Large**: The script fetches from `earliest_date - 1 day`, which may include thousands of tests
2. **No Incremental Progress**: If the fetch times out, all progress is lost and the fetch restarts from scratch
3. **No Pagination Optimization**: The VALD API might support batch operations that aren't being utilized

## Solutions (In Order of Effectiveness)

### Solution 1: Fix the valdr Package (RECOMMENDED - Root Cause Fix)

The `valdr` package needs to be updated to fetch trials more efficiently. There are three approaches:

#### Option A: Use Batch/Bulk API Endpoints
If the VALD API supports batch endpoints for fetching trials:
```r
# Instead of:
for (test_id in test_ids) {
  trials <- fetch_trials_for_test(test_id)  # Sequential
}

# Use:
trials <- fetch_trials_batch(test_ids)  # Single batch request
```

#### Option B: Implement Parallel Fetching
Use R's parallel processing capabilities:
```r
library(future)
library(furrr)

# Set up parallel workers
plan(multisession, workers = 4)

# Fetch trials in parallel
trials_list <- future_map(test_ids, fetch_trials_for_test, 
                          .options = furrr_options(seed = TRUE))
```

#### Option C: Add Pagination Skip Logic
Only fetch trials for NEW tests (those not already in BigQuery):
```r
# Get existing test IDs from BigQuery
existing_test_ids <- get_existing_test_ids_from_bq()

# Only fetch trials for new tests
new_test_ids <- setdiff(all_test_ids, existing_test_ids)
trials <- fetch_trials_for_tests(new_test_ids)
```

**Action Required**: 
1. Check if `valdr` is maintained internally or is an external package
2. If internal: Implement one of the above fixes in the `valdr` source code
3. If external: Fork the package and apply fixes, or contact maintainers

### Solution 2: Optimize the Fetch Window

**Problem**: Currently fetching overlap of 1 day (`overlap_days <- 1L` at line 1050 in run.R)

**Solution**: Reduce overlap window when data is stable:
```r
# In run.R around line 1050, modify:
# Old:
overlap_days <- 1L

# New (example logic):
overlap_days <- if (fd_count_mismatch) 7L else 1L  # Only 1 day if just new tests
```

OR even better, use a smarter window:
```r
# Fetch only tests modified since last successful run
# This requires tracking last_successful_run_timestamp
overlap_start <- last_successful_run_timestamp - hours(1)  # 1 hour buffer
set_start_date(format(overlap_start, "%Y-%m-%dT%H:%M:%SZ"))
```

### Solution 3: Increase Timeout with Better Monitoring

**Short-term mitigation** (NOT a root cause fix, but buys time):

Update `.github/scripts/run.R` line 1061:
```r
# Old:
timeout_seconds = 900,

# New:
timeout_seconds = 3600,  # 60 minutes - enough for ~3600 tests at 1 sec each
```

Also increase workflow timeout in `.github/workflows/run-every-15m.yml` line 12:
```yaml
# Old:
timeout-minutes: 60

# New:
timeout-minutes: 90
```

**Add progress checkpointing** to save partial results:
```r
# Pseudo-code for checkpointing
checkpoint_file <- "/tmp/vald_fetch_progress.rds"

fetch_with_checkpointing <- function(test_ids) {
  # Resume from checkpoint if exists
  if (file.exists(checkpoint_file)) {
    checkpoint <- readRDS(checkpoint_file)
    processed_ids <- checkpoint$processed_ids
    trials <- checkpoint$trials
  } else {
    processed_ids <- character()
    trials <- list()
  }
  
  # Fetch remaining tests
  remaining_ids <- setdiff(test_ids, processed_ids)
  
  for (id in remaining_ids) {
    trial <- fetch_trial(id)
    trials[[id]] <- trial
    processed_ids <- c(processed_ids, id)
    
    # Save checkpoint every 100 tests
    if (length(processed_ids) %% 100 == 0) {
      saveRDS(list(processed_ids = processed_ids, trials = trials), 
              checkpoint_file)
    }
  }
  
  # Clean up checkpoint on success
  if (file.exists(checkpoint_file)) file.remove(checkpoint_file)
  
  return(trials)
}
```

### Solution 4: Implement Smart Incremental Fetching

Instead of always fetching all tests since `overlap_start`, maintain state:

```r
# Track which specific test_IDs have been fully processed
# Store in a new BigQuery table: vald_sync_state

sync_state <- read_bq_table("vald_sync_state")
last_successful_sync <- max(sync_state$sync_timestamp)

# Only fetch tests that are:
# 1. New (created after last_successful_sync), OR
# 2. Modified (modifiedDateUtc > last_successful_sync)

# This requires using API filters like:
# ?modifiedAfter=2026-01-07T00:00:00Z
```

### Solution 5: Use VALD API Rate Limiting and Batch Parameters

Check VALD API documentation for:
- **Batch size parameters**: `?pageSize=1000` instead of default (might be 50)
- **Bulk endpoints**: `/tests/bulk?testIds=id1,id2,id3...`
- **Filtering capabilities**: `?includeTrials=true&expand=trials`

Update API calls to use optimal parameters.

## BigQuery Permissions Fix (Secondary Issue)

The service account `gha-bq@sac-vald-hub.iam.gserviceaccount.com` needs the following permission:

**Error**:
```
BigQuery error in update operation: Access Denied: Dataset sac-vald-hub:analytics: 
Permission bigquery.datasets.update denied on dataset sac-vald-hub:analytics
```

**Solution**:
Grant the `bigquery.datasets.update` permission to the service account. This can be done by:

1. Adding the "BigQuery Data Owner" role (includes dataset update), OR
2. Creating a custom role with just `bigquery.datasets.update` permission

**GCloud Command**:
```bash
# Option 1: Add BigQuery Data Owner role
gcloud projects add-iam-policy-binding sac-vald-hub \
  --member="serviceAccount:gha-bq@sac-vald-hub.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataOwner"

# Option 2: Use existing role that includes datasets.update
gcloud projects add-iam-policy-binding sac-vald-hub \
  --member="serviceAccount:gha-bq@sac-vald-hub.iam.gserviceaccount.com" \
  --role="roles/bigquery.admin" \
  --condition=None
```

Note: The workflow currently uses `bq update --location=US --default_table_expiration 0` which requires this permission.

## Email Notification Fix (Tertiary Issue)

**Error**:
```
Invalid login: 535-5.7.8 Username and Password not accepted.
```

**Solution**: Update the workflow secrets with valid Gmail app-specific password:
1. Generate an App Password in Gmail settings
2. Update GitHub repository secrets:
   - `GMAIL_USERNAME`: your-email@gmail.com
   - `GMAIL_APP_PASSWORD`: your-16-character-app-password

OR remove the email notification step if not needed.

## Implementation Priority

### ⚠️ CRITICAL FINDING: `valdr` Package is External

After investigation, the `valdr` package is an **external dependency** installed from CRAN or another repository. The "Fetching trials for Test ID" log messages originate from within the `valdr` package code, which is NOT present in this repository.

**This means the ROOT CAUSE cannot be fixed directly in this repository.** The pagination bug exists in the `valdr` package itself.

### Phase 1: IMMEDIATE ACTION REQUIRED

**Contact valdr Package Maintainers:**
1. Report the pagination performance issue to valdr maintainers
2. Provide them with this analysis and logs
3. Request implementation of batch/parallel fetching for `get_forcedecks_data()`

**Alternative: Fork and Fix valdr:**
1. Fork the valdr package repository
2. Implement one of the solutions from "Solution 1" above
3. Install the forked version in your workflows:
   ```yaml
   # In .github/workflows/run-every-15m.yml, add to R dependencies:
   extra-packages: |
     DBI
     ... other packages ...
     torinshan/valdr  # Use your fork instead of CRAN valdr
   ```

### Phase 2: Configuration Improvements (Until valdr is Fixed)

These are **proper configuration improvements**, not workarounds:

1. **Increase workflow and script timeouts** - Current 900s timeout is insufficient for the data volume
   - Update workflow timeout from 60min to 90min
   - Update script timeout from 900s to 3600s
   
2. **Optimize fetch window** - Don't fetch more data than necessary
   - Current: fetches from `earliest_date - 1 day`
   - Better: only fetch tests modified since last successful run

3. **Fix BigQuery permissions** - Prevents secondary errors
   - Grant `bigquery.datasets.update` to gha-bq service account

### Phase 3: Long-term Optimization (After valdr is Fixed)
1. **Implement smart incremental sync** with state tracking
2. **Add comprehensive monitoring** and alerting
3. **Optimize API parameters** for batch size and pagination

### Phase 4: Infrastructure Fixes (Immediate)
1. **Fix BigQuery permissions** ✓ (Can be done now)
2. **Fix or remove email notifications** ✓ (Can be done now)

## Testing Plan

1. **Test with limited date range** (e.g., 1 week) to validate fix works
2. **Gradually increase date range** to full production load
3. **Monitor execution time** and ensure it stays well under timeout
4. **Verify data completeness** - no tests should be skipped

## Success Criteria

- ✅ Workflow completes successfully within 30 minutes (50% of 60-min timeout)
- ✅ All ForceDecks tests are fetched and processed
- ✅ No pagination loops or stuck cursors
- ✅ Robust error handling and recovery
- ✅ Clear logging for troubleshooting

## References

- Problem Statement: See issue description
- Workflow File: `.github/workflows/run-every-15m.yml`
- R Script: `.github/scripts/run.R` (lines 1057-1063)
- Service Account Permissions: See problem statement

---

**Prepared by**: GitHub Copilot
**Date**: 2026-01-07
**Status**: READY FOR IMPLEMENTATION
