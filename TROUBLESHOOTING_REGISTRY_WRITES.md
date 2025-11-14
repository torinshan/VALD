# Troubleshooting BigQuery Registry Write Failures

## Problem Summary

Model training runs were failing with errors like:
```
Registry versions save error: Job sac-vald-hub.job_... failed
Failed to save to registry_versions after retries
```

This caused all models to fail to save to the registry, resulting in "Success: 0 athletes" and "Failed: 91 athletes".

## Root Causes (Most Likely → Least Likely)

### 1. Service Account Permission Issues ⭐ **MOST LIKELY**
**Symptoms:**
- Error: `accessDenied`, `permissionDenied`
- Jobs created but fail immediately

**Solution:**
The service account used by GitHub Actions needs these IAM roles:
```bash
# Required roles
roles/bigquery.jobUser       # Create and run BigQuery jobs
roles/bigquery.dataEditor     # Insert/update rows in tables

# Or use a combined role
roles/bigquery.admin          # Full BigQuery access (overkill but works)
```

**How to verify:**
```bash
# Check current roles for the service account
gcloud projects get-iam-policy sac-vald-hub \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:ci-runner@sac-vald-hub.iam.gserviceaccount.com" \
  --format="table(bindings.role)"

# Add missing roles if needed
gcloud projects add-iam-policy-binding sac-vald-hub \
  --member="serviceAccount:ci-runner@sac-vald-hub.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding sac-vald-hub \
  --member="serviceAccount:ci-runner@sac-vald-hub.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
```

### 2. Schema Mismatch or Validation Error
**Symptoms:**
- Error: `invalidQuery`, `invalid`, `tableUnavailable`
- Error messages mention column names or types

**Common issues:**
- Column type mismatch (STRING vs FLOAT64)
- Missing required columns
- NULL values in NOT NULL columns
- Partition column type mismatch

**How to verify:**
```bash
# Check actual table schema
bq show --schema --format=prettyjson sac-vald-hub:analytics.registry_versions

# Compare with expected schema (from code line 855-881)
```

**Expected schema for registry_versions:**
```sql
CREATE TABLE IF NOT EXISTS `sac-vald-hub.analytics.registry_versions` (
  model_id STRING NOT NULL,
  version_id STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  created_by STRING,
  artifact_uri STRING,
  artifact_sha256 STRING,
  framework STRING,
  r_version STRING,
  package_info STRING,
  training_data_start_date DATE,
  training_data_end_date DATE,
  n_training_samples INT64,
  n_test_samples INT64,
  validation_method STRING,
  predictor_list STRING,
  hyperparameters STRING,
  train_rmse FLOAT64,
  cv_rmse FLOAT64,
  test_rmse FLOAT64,
  pipeline_run_id STRING,
  git_commit_sha STRING,
  notes STRING
)
PARTITION BY DATE(created_at)
CLUSTER BY model_id, version_id
```

### 3. Dataset/Table Location Mismatch
**Symptoms:**
- Error: `notFound`, `invalid`
- Error message mentions "location"

**How to verify:**
```bash
# Check dataset location
bq show sac-vald-hub:analytics | grep Location

# Expected: US (set in env var BQ_LOCATION)
```

**Solution:**
Ensure dataset is in `US` region or update `BQ_LOCATION` env var to match.

### 4. Table Does Not Exist
**Symptoms:**
- Error: `notFound`, `tableUnavailable`

**How to verify:**
```bash
# Check if table exists
bq show sac-vald-hub:analytics.registry_versions

# If not found, check if it needs to be created
bq ls sac-vald-hub:analytics
```

**Solution:**
The script should auto-create tables on first run (line 855-915). If it doesn't, run the CREATE TABLE statements manually.

### 5. BigQuery Quota or Rate Limits
**Symptoms:**
- Error: `quotaExceeded`, `rateLimitExceeded`
- Intermittent failures

**How to verify:**
- Check BigQuery quotas in Cloud Console
- Look for patterns in failure timing

**Solution:**
- Use free tier: avoid DML, use streaming inserts (already implemented)
- Add exponential backoff (already implemented)
- Reduce concurrent jobs

## Using the New Diagnostic Features

### 1. SKIP_REGISTRY_VERSIONS Flag

To allow training to proceed without registry writes while debugging:

```yaml
# In .github/workflows/combined_workflow.yml
env:
  SKIP_REGISTRY_VERSIONS: "true"
```

This will:
- ✅ Train all models successfully
- ✅ Save predictions
- ⏭️  Skip registry_models and registry_versions writes
- ⚠️  Log warnings about skipped writes

### 2. Enhanced Error Logs

The updated code now logs:
- **BigQuery Job ID**: `job_abc123.US`
- **Error reason**: e.g., `accessDenied`, `invalid`, `notFound`
- **Error message**: Full error description
- **Job configuration**: Destination table, source format, write disposition
- **Manual inspection command**: `bq show -j sac-vald-hub:job_abc123.US`

**Example log output:**
```
[ERROR] Registry versions save error: Job sac-vald-hub.job_abc123.US failed
📋 BigQuery Job ID: job_abc123.US
🔍 Fetching BigQuery job details for: job_abc123.US
📊 Job state: DONE
❌ BigQuery Error Details:
   Reason: accessDenied
   Message: Access Denied: Project sac-vald-hub: User does not have permission to insert into table
   Location: 
📋 Load job configuration:
   Destination: sac-vald-hub:analytics.registry_versions
   Source format: NEWLINE_DELIMITED_JSON
   Write disposition: WRITE_APPEND
```

### 3. Manual Job Inspection

If you see a job ID in the logs:
```bash
# Get full job details
bq show -j sac-vald-hub:job_abc123.US

# Get just the error
bq show -j sac-vald-hub:job_abc123.US --format=json | jq '.status.errorResult'

# Get all errors
bq show -j sac-vald-hub:job_abc123.US --format=json | jq '.status.errors'
```

## Quick Fix Checklist

1. **Check service account permissions** ⭐ START HERE
   ```bash
   # Verify roles
   gcloud projects get-iam-policy sac-vald-hub | grep ci-runner
   
   # Add roles if missing
   gcloud projects add-iam-policy-binding sac-vald-hub \
     --member="serviceAccount:ci-runner@sac-vald-hub.iam.gserviceaccount.com" \
     --role="roles/bigquery.jobUser"
   
   gcloud projects add-iam-policy-binding sac-vald-hub \
     --member="serviceAccount:ci-runner@sac-vald-hub.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataEditor"
   ```

2. **Verify table schema matches expected schema**
   ```bash
   bq show --schema sac-vald-hub:analytics.registry_versions
   ```

3. **Check dataset location is US**
   ```bash
   bq show sac-vald-hub:analytics | grep Location
   ```

4. **Inspect a failed job from the logs**
   ```bash
   # Get job ID from error logs, then:
   bq show -j sac-vald-hub:job_<ID>.US
   ```

5. **If still stuck, enable skip flag to unblock training**
   ```yaml
   # In workflow file
   env:
     SKIP_REGISTRY_VERSIONS: "true"
   ```

## Files Changed

- `.github/scripts/readiness_models_cloud.R`: Enhanced error logging, added SKIP_REGISTRY_VERSIONS flag, added job status checking
- `TROUBLESHOOTING_REGISTRY_WRITES.md`: This troubleshooting guide

## Next Steps After Fix

Once the root cause is resolved:
1. Remove `SKIP_REGISTRY_VERSIONS=true` if you added it
2. Re-run the training pipeline
3. Verify logs show:
   - ✅ Upload job created: job_xxx.US
   - ✅ Job job_xxx.US completed successfully
   - ✅ Saved to registry_models: ...
   - ✅ Saved to registry_versions: ...
4. Check BigQuery tables to confirm data was written:
   ```bash
   bq query --use_legacy_sql=false \
     "SELECT COUNT(*) FROM \`sac-vald-hub.analytics.registry_versions\` WHERE DATE(created_at) = CURRENT_DATE()"
   ```
