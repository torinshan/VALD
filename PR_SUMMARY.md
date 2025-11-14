# Pull Request Summary: Fix BigQuery Registry Write Failures

## Problem Statement

Model training runs were failing with errors like:
```
Registry versions save error: Job sac-vald-hub.job_... failed
Failed to save to registry_versions after retries
```

This caused **100% failure rate**: "Success: 0 athletes" and "Failed: 91 athletes" because every model write to the BigQuery `registry_versions` table failed (while `registry_models` writes mostly succeeded).

**Critical Issue**: The code didn't capture or log the actual BigQuery job error details, making it impossible to diagnose whether the issue was:
- Permission/role missing for the service account
- Schema or payload validation error
- Region/location mismatch
- Quota or transient BigQuery errors

## Solution Overview

This PR implements comprehensive BigQuery job error logging and diagnostics to:
1. ✅ Capture the exact BigQuery error when jobs fail
2. ✅ Wait for job completion and check status
3. ✅ Actively fetch job details from BigQuery API
4. ✅ Log error reason, message, and job configuration
5. ✅ Provide skip flag to unblock pipelines during debugging
6. ✅ Fix R programming errors that prevented error logging

## Changes Made

### 1. Enhanced BigQuery Error Logging (`.github/scripts/readiness_models_cloud.R`)

**New Functions:**
- `fetch_and_log_bq_job_details()` (line 224-302): Actively queries BigQuery using `bq show -j` to fetch complete job error information including:
  - Job state (DONE, FAILED, etc.)
  - Error reason (accessDenied, invalid, notFound, etc.)
  - Error message with details
  - Job configuration (destination table, source format, write disposition)
  
- Enhanced `log_bq_job_error()` (line 304-366): Parses job IDs from error messages and calls fetch function

**Job Status Checking:**
- Line 1971-2004: Enhanced `registry_models` upload to wait for job completion using `bq_job_wait()` and check status
- Line 2061-2094: Enhanced `registry_versions` upload to wait for job completion and check status
- Fail fast with actionable error messages when jobs fail

### 2. SKIP_REGISTRY_VERSIONS Flag

**Configuration:**
- Line 59: Added `skip_registry_versions` config variable
- Line 1896-1911: Skip logic in `save_model_to_registry()` function

**Usage:**
Set environment variable in workflow:
```yaml
env:
  SKIP_REGISTRY_VERSIONS: "true"
```

This allows training to proceed without registry writes, useful for:
- Debugging registry issues without blocking training
- Testing model training independently
- Emergency unblocking of pipelines

### 3. Bug Fixes

Fixed "$ operator is invalid for atomic vectors" error:
- Added `is.list()` checks before using `$` operator on potentially atomic objects
- Fixed in `log_bq_job_error()`, registry_models upload, and registry_versions upload

### 4. Comprehensive Troubleshooting Guide

**New File:** `TROUBLESHOOTING_REGISTRY_WRITES.md` (260 lines)

Contents:
- Root cause analysis for common failures (permissions, schema, location, etc.)
- Quick fix checklist with step-by-step commands
- How to use new diagnostic features
- Manual job inspection commands
- Expected log output for success and failure cases

## Example Log Output

### Before (Unhelpful):
```
[ERROR] Registry versions save error: Job sac-vald-hub.job_abc123.US failed
[ERROR] Failed to save to registry_versions after retries
```

### After (Actionable):
```
[ERROR] Registry versions save error: Job sac-vald-hub.job_abc123.US failed
[ERROR] Error class: simpleError, error, condition
[ERROR] ❌ registry_versions upload error: Job sac-vald-hub.job_abc123.US failed
[ERROR] 📋 BigQuery Job ID: job_abc123.US
[INFO]  🔍 Fetching BigQuery job details for: job_abc123.US
[ERROR] 📊 Job state: DONE
[ERROR] ❌ BigQuery Error Details:
[ERROR]    Reason: accessDenied
[ERROR]    Message: Access Denied: Project sac-vald-hub: User does not have permission to insert into table analytics.registry_versions
[INFO]  📋 Load job configuration:
[INFO]     Destination: sac-vald-hub:analytics.registry_versions
[INFO]     Source format: NEWLINE_DELIMITED_JSON
[INFO]     Write disposition: WRITE_APPEND
[ERROR] Failed to save to registry_versions after retries: registry_versions insert failed: Job sac-vald-hub.job_abc123.US failed
```

Now the user can immediately see the problem is `accessDenied` and needs to add BigQuery permissions to the service account.

## Files Changed

1. **`.github/scripts/readiness_models_cloud.R`**: +256 lines, -6 lines
   - Enhanced error logging
   - Job status checking
   - Skip flag support
   - Bug fixes

2. **`TROUBLESHOOTING_REGISTRY_WRITES.md`**: +260 lines (new file)
   - Comprehensive troubleshooting guide

**Total**: +516 lines, -6 lines

## Testing Recommendations

1. **Test with correct permissions** → Should succeed with detailed success logs
2. **Test with missing permissions** → Should fail with detailed error showing permission issue (e.g., "accessDenied")
3. **Test with schema mismatch** → Should fail with detailed error showing schema issue (e.g., "invalid")
4. **Test with `SKIP_REGISTRY_VERSIONS=true`** → Should skip registry writes and succeed in training

## Backward Compatibility

✅ Fully backward compatible - all existing functionality works as before, with enhanced logging added on top.

No breaking changes to:
- Workflow files
- Environment variables (new ones are optional)
- Function signatures
- Data formats

## Impact

**Before:**
- 0% success rate (0/91 athletes)
- No diagnostic information
- Unable to identify root cause
- Pipeline completely blocked

**After:**
- Detailed error logging enables quick root cause identification
- Skip flag allows unblocking pipeline while debugging
- Comprehensive troubleshooting guide speeds up resolution
- Expected to achieve 100% success rate once root cause is fixed

## Most Likely Root Cause

Based on the problem description, the most likely root cause is **missing BigQuery permissions** for the service account `ci-runner@sac-vald-hub.iam.gserviceaccount.com`.

**Quick fix** (after merging this PR and seeing detailed errors):
```bash
gcloud projects add-iam-policy-binding sac-vald-hub \
  --member="serviceAccount:ci-runner@sac-vald-hub.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding sac-vald-hub \
  --member="serviceAccount:ci-runner@sac-vald-hub.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
```

## Next Steps After Merge

1. Merge this PR
2. Run the pipeline - it will now show detailed error information
3. Follow the troubleshooting guide to fix the root cause
4. Remove `SKIP_REGISTRY_VERSIONS=true` if you added it
5. Verify 100% success rate with detailed success logs

## Related Issues

Addresses the issue reported at ref `4f1c68f187b4cecef1e1081350043fbb75301dd7` where:
- Training ran for each athlete
- `registry_models` writes mostly succeeded
- `registry_versions` writes all failed
- Pipeline reported "Success: 0 athletes" and "Failed: 91 athletes"
