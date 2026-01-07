# API Timeout Fix - Implementation Summary

## Problem Statement
The VALD API pipeline was experiencing timeout errors and had BigQuery data missing, which would cause issues:
1. API calls timing out due to lack of rate limiting
2. Missing BigQuery files causing the pipeline to attempt fetching from 1900-01-01 (default date)
3. No mechanism to resume from partial results after timeout
4. Workflow disabled due to these issues (see line 4 of `.github/workflows/run-every-15m.yml`)

## Solution Implemented

### 1. Backstop Start Date (2024-01-01)
**What it does:** Prevents the pipeline from attempting to fetch data from ancient dates when BigQuery is empty.

**Implementation:**
- Added `BACKSTOP_START_DATE` constant: `2024-01-01`
- Added `MISSING_DATA_THRESHOLD_DATE` constant: `2000-01-01` (to detect missing data)
- Modified start date calculation logic (lines 1244-1262 in `run.R`)

**Logic:**
```r
if (calculated_start_dt < MISSING_DATA_THRESHOLD_DATE || count_tests_current == 0) {
  start_dt <- BACKSTOP_START_DATE  # Use 2024-01-01
} else {
  start_dt <- calculated_start_dt  # Use normal overlap date
}
```

**Benefit:** When BigQuery has no data, starts from 2024-01-01 instead of 1900-01-01, dramatically reducing API load.

### 2. Rate Limiting (10 calls/second)
**What it does:** Prevents overwhelming the VALD API with too many requests.

**Implementation:**
- Created `rate_limit_api_call()` function (lines 287-308 in `run.R`)
- Uses environment-based state management for thread safety
- Automatically applied to all API calls via `safe_vald_fetch()`

**Logic:**
- Tracks calls per second
- When 10 calls reached, sleeps until next second
- Resets counter after moving to new second (prevents race condition)

**Benefit:** Ensures we never exceed 10 sequential API calls per second, reducing timeout risk.

### 3. Checkpointing Infrastructure
**What it does:** Provides framework for saving/resuming partial API fetch results.

**Implementation:**
- `save_checkpoint()` - Saves progress to `/tmp/vald_checkpoints/`
- `load_checkpoint()` - Restores previous progress
- `remove_checkpoint()` - Cleans up after successful completion
- All with validation to prevent errors

**Current Limitation:** 
The valdr package functions (`get_forcedecks_data()`, `get_nordbord_data()`) handle pagination internally and return all-or-nothing results. The checkpointing infrastructure is in place for:
1. Future valdr enhancements that may support partial results
2. Error recovery when partial results become available
3. A framework that can be extended

**Benefit:** Infrastructure ready for enhanced pagination control when valdr supports it.

### 4. Enhanced safe_vald_fetch()
**What it does:** Wraps all VALD API calls with timeout protection, rate limiting, and checkpoint support.

**New parameters:**
- `enable_checkpointing` - Turn checkpointing on/off (default: TRUE for data fetches, FALSE for probes)
- `checkpoint_interval` - How often to save checkpoints (default: 100 items)

**Features:**
- Rate limiting before each call
- Checkpoint loading at start
- Enhanced error handling
- Checkpoint saving on error
- Checkpoint cleanup on success

**Applied to:**
- ForceDecks tests probe (checkpointing disabled - quick probe)
- Nordbord tests probe (checkpointing disabled - quick probe)
- ForceDecks full data fetch (checkpointing enabled)
- Nordbord data fetch (checkpointing enabled)

## Files Changed
- `.github/scripts/run.R`: +227 lines, -15 lines

## Testing Performed
1. ✅ Code review completed - all issues addressed
2. ✅ Security scan (CodeQL) - passed with no issues
3. ✅ Manual code review - logic verified correct
4. ⏳ Runtime testing - requires re-enabling workflow

## Next Steps to Complete the Fix

### 1. Test the Changes
Run the workflow manually to verify:
```bash
# Go to: https://github.com/torinshan/VALD/actions/workflows/run-every-15m.yml
# Click "Run workflow" button
```

**Expected behavior:**
- If BigQuery is empty: Should log "using backstop date: 2024-01-01"
- If BigQuery has data: Should log "Using calculated overlap start: [date]"
- API calls should be rate-limited (max 10/second)
- Should complete without timeout errors

### 2. Monitor Logs
Check for these log messages:
```
✅ "Using calculated overlap start: [date]" OR "using backstop date: 2024-01-01"
✅ "Starting ForceDecks full data fetch with 900 second timeout"
✅ "ForceDecks full data completed successfully"
✅ No timeout errors
```

### 3. Re-enable Scheduled Workflow
Once manual testing succeeds, re-enable the cron schedule:

Edit `.github/workflows/run-every-15m.yml` line 4-6:
```yaml
on:
  schedule:
    - cron: "*/15 * * * *" # UTC; gate by Pacific hours below
  workflow_dispatch:
```

### 4. Verify Service Account Permissions
Ensure the service account has proper BigQuery permissions (this was mentioned in the problem statement):
```bash
# Already done according to problem statement:
gcloud projects add-iam-policy-binding sac-vald-hub \
  --member="serviceAccount:gha-bq@sac-vald-hub.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataOwner"
```

## Expected Impact

### Before Fix:
- ❌ Workflow disabled due to timeouts
- ❌ Would attempt to fetch from 1900-01-01 when data missing
- ❌ No rate limiting (could hit API limits)
- ❌ No recovery mechanism for partial results

### After Fix:
- ✅ Backstop prevents unrealistic date ranges
- ✅ Rate limiting prevents API overload (10 calls/sec)
- ✅ Enhanced error handling and logging
- ✅ Infrastructure ready for checkpoint-based recovery
- ✅ Should be able to re-enable workflow

## Related Issues
- Workflow comment: "TEMPORARILY DISABLED: Experiencing VALD API timeouts (2026-01-05)"
- BigQuery permissions updated (mentioned in problem statement)
- Missing BigQuery files (mentioned in problem statement)

## Support
If issues persist after this fix:
1. Check workflow run logs for error messages
2. Verify backstop date is being used: grep for "backstop date" in logs
3. Verify rate limiting is working: look for even distribution of API calls over time
4. Check BigQuery service account permissions

## Code Review Notes
All code review issues have been addressed:
- ✅ Checkpoint data validation before field access
- ✅ Environment-based rate limiter (not mutable list)
- ✅ Race condition fixed in rate limiter
- ✅ Constants defined for date thresholds
- ✅ Safe field access throughout
