# Dual BigQuery Project Setup Guide

This repository now supports reading readiness data from one BigQuery project (`sac-vald-hub`) while writing workload and model data to another project (`sac-ml-models`).

## Overview

### Data Flow
```
┌─────────────────────────────────────┐
│  sac-vald-hub (Original Project)    │
│  - vald_fd_jumps (readiness data)   │  ────► READ ONLY
│  - Service: gha-bq@sac-vald-hub...  │
└─────────────────────────────────────┘

┌─────────────────────────────────────┐
│  sac-ml-models (New ML Project)   │
│  - workload_daily                   │
│  - roster_mapping                   │  ◄──── WRITE DATA
│  - readiness_predictions_byname     │
│  - model_training_summary           │
│  - Service: gh-actions-bq@my-ml...  │
└─────────────────────────────────────┘
```

## Required GitHub Secrets

You need to add two secrets to your GitHub repository:

### 1. `GCP_SA_KEY_ML`
Service account JSON key for the **new ML project** (`sac-ml-models`):
- **Service Account**: `gh-actions-bq@sac-ml-models.iam.gserviceaccount.com`
- **Required Roles**:
  - `roles/bigquery.jobUser` (to run queries)
  - `roles/bigquery.dataEditor` (to write data)
  - `roles/bigquery.user` (to access the project)

### 2. `GCP_SA_KEY_READINESS`
Service account JSON key for the **original readiness project** (`sac-vald-hub`):
- **Service Account**: `gha-bq@sac-vald-hub.iam.gserviceaccount.com`
- **Required Roles**:
  - `roles/bigquery.jobUser` (to run queries)
  - `roles/bigquery.dataViewer` (to read data - read-only access is sufficient)

## How to Add Secrets

1. Go to your GitHub repository
2. Click **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Add both secrets:
   - Name: `GCP_SA_KEY_ML`, Value: (paste the JSON content for sac-ml-models)
   - Name: `GCP_SA_KEY_READINESS`, Value: (paste the JSON content for sac-vald-hub)

## Configuration Details

### Environment Variables in `combined_workflow.yml`

```yaml
env:
  # Workload & Model Project (new - destination for writes)
  GCP_PROJECT_ML: sac-ml-models
  BQ_DATASET: analytics
  BQ_LOCATION: US

  # Readiness Data Project (original - source for reads)
  GCP_PROJECT_READINESS: sac-vald-hub
```

### Authentication Flow

Each job in the workflow now:
1. **Authenticates to ML project** (primary) using `GCP_SA_KEY_ML`
2. **Authenticates to Readiness project** (secondary) using `GCP_SA_KEY_READINESS`

Example:
```yaml
- name: Auth to Google Cloud (ML Project - primary)
  uses: google-github-actions/auth@v2
  with:
    credentials_json: ${{ secrets.GCP_SA_KEY_ML }}
    
- name: Auth to Readiness Project (for reading VALD data)
  run: |
    echo '${{ secrets.GCP_SA_KEY_READINESS }}' > /tmp/readiness-sa-key.json
    gcloud auth activate-service-account --key-file=/tmp/readiness-sa-key.json
```

## BigQuery Queries

All BigQuery queries have been updated to reference the correct project:

### Reads from Readiness Project
```sql
SELECT * FROM `sac-vald-hub.analytics.vald_fd_jumps`
```

### Writes to ML Project
```sql
-- workload_daily, roster_mapping, predictions, etc.
SELECT * FROM `sac-ml-models.analytics.workload_daily`
```

### Cross-Project Joins
```sql
WITH workload AS (
  SELECT * FROM `sac-ml-models.analytics.workload_daily`
),
readiness AS (
  SELECT * FROM `sac-vald-hub.analytics.vald_fd_jumps`
),
roster AS (
  SELECT * FROM `sac-ml-models.analytics.roster_mapping`
)
SELECT ... FROM workload JOIN readiness JOIN roster
```

## Scripts Updated

### 1. `combined_workflow.yml`
- Updated all jobs to use dual authentication
- Updated all environment variables to use correct projects
- Updated all `bq` commands to reference correct projects
- Updated all BigQuery SQL queries with proper project prefixes

### 2. `workload_ingest.R`
- Default project changed from `sac-vald-hub` to `sac-ml-models`
- Writes workload data to the new ML project

### 3. `readiness_models_cloud.R`
- Added `project_readiness` variable for the original project
- Reads readiness data from `sac-vald-hub`
- Writes models and predictions to `sac-ml-models`
- Cross-project queries properly reference both projects

## Verification Steps

After adding the secrets, verify the setup works:

1. **Trigger the workflow manually**:
   - Go to Actions → Workload & Model Pipeline
   - Click "Run workflow"

2. **Check the logs** for:
   ```
   ✅ BigQuery dataset accessible
   ✅ Workload ingest completed successfully
   ✅ VALD FD jumps data loaded
   ✅ Models trained successfully
   ```

3. **Verify data in BigQuery**:
   ```bash
   # Check ML project tables
   bq ls sac-ml-models:analytics
   
   # Check readiness project tables  
   bq ls sac-vald-hub:analytics
   ```

## Important Notes

### DML Limitations
The new project (`sac-ml-models`) may be on BigQuery's free tier, which has limitations:
- **No MERGE/UPDATE/DELETE support** - Only INSERT/TRUNCATE
- The workflow already handles this with `SKIP_REGISTRY_STAGES: 'true'`
- R scripts use TRUNCATE instead of MERGE when appropriate

### Security
- Both service accounts should follow principle of least privilege
- Readiness project account only needs **read** access
- ML project account needs **read/write** access

### Cost Optimization
- Reading from one project and writing to another may incur cross-project query costs
- Monitor BigQuery costs in both projects
- Consider copying static reference data if costs become significant

## Troubleshooting

### Error: "Source table not found" or "Readiness table doesn't exist"
This is **expected behavior** when the workflow runs before the `saturday_makeup` workflow has populated the readiness data:

**What happens:**
1. The sync step checks if source table exists in `sac-vald-hub`
2. If not found, sync is skipped with message: "Source table doesn't exist yet and no local copy available"
3. The workflow continues successfully, but training is skipped with reason: `no_readiness_table`

**This is OK when:**
- Running the workflow for the first time
- The `saturday_makeup` workflow hasn't run yet this week
- VALD data hasn't been uploaded yet

**How to resolve:**
- Wait for the `saturday_makeup` workflow to run and populate the table
- Or manually upload VALD data to `sac-vald-hub.analytics.vald_fd_jumps`
- Once available, the next workflow run will sync the data

**Note:** If a local copy of the readiness table already exists in `sac-ml-models`, the workflow will use that copy even if the source is unavailable.

### Error: "Access Denied"
- Verify service account has required roles
- Check that both secrets are properly set in GitHub

### Error: "Table not found"
- Ensure tables exist in the correct project
- Verify project IDs are correct in environment variables

### Error: "Invalid credentials"
- Verify JSON keys are complete and valid
- Check for any extra whitespace or formatting issues in secrets

### Query Fails with "Permission Denied"
- The ML project service account needs access to read from the readiness project
- Add `gh-actions-bq@sac-ml-models.iam.gserviceaccount.com` as a viewer on the readiness project dataset

## Support

If you encounter issues:
1. Check the workflow logs for detailed error messages
2. Verify all secrets are correctly set
3. Ensure service accounts have proper permissions
4. Review BigQuery audit logs for permission issues
