# BigQuery Permissions Fix

## Problem

The workflow is failing with this error:

```
BigQuery error in update operation: Access Denied: Dataset sac-vald-
hub:analytics: Permission bigquery.datasets.update denied on dataset sac-vald-
hub:analytics (or it may not exist).
```

## Root Cause

The service account `gha-bq@sac-vald-hub.iam.gserviceaccount.com` does NOT have the `bigquery.datasets.update` permission, but the workflow attempts to run:

```bash
bq update --location=US --default_table_expiration 0 "${GCP_PROJECT}:${BQ_DATASET}"
```

This command requires the `bigquery.datasets.update` permission.

## Current Permissions (From Problem Statement)

| Service Account | Current Roles |
|-----------------|---------------|
| `gha-bq@sac-vald-hub.iam.gserviceaccount.com` | - BigQuery Data Editor<br>- BigQuery Data Viewer<br>- BigQuery Job User<br>- BigQuery Metadata Viewer<br>- Viewer |

**Missing**: Permission to update dataset metadata (`bigquery.datasets.update`)

## Solution

You need to grant one of these roles to the service account:

### Option 1: BigQuery Data Owner (RECOMMENDED)

Includes dataset update permission plus all necessary data operations:

```bash
gcloud projects add-iam-policy-binding sac-vald-hub \
  --member="serviceAccount:gha-bq@sac-vald-hub.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataOwner"
```

### Option 2: BigQuery Admin (More Permissive)

Includes all BigQuery permissions:

```bash
gcloud projects add-iam-policy-binding sac-vald-hub \
  --member="serviceAccount:gha-bq@sac-vald-hub.iam.gserviceaccount.com" \
  --role="roles/bigquery.admin"
```

### Option 3: Custom Role (Minimal - If You Prefer Least Privilege)

Create a custom role with just the needed permission:

```bash
# Create custom role
gcloud iam roles create ValdDatasetUpdater \
  --project=sac-vald-hub \
  --title="VALD Dataset Updater" \
  --description="Allows updating BigQuery dataset metadata" \
  --permissions=bigquery.datasets.update \
  --stage=GA

# Assign to service account
gcloud projects add-iam-policy-binding sac-vald-hub \
  --member="serviceAccount:gha-bq@sac-vald-hub.iam.gserviceaccount.com" \
  --role="projects/sac-vald-hub/roles/ValdDatasetUpdater"
```

## Via GCP Console

If you prefer using the web interface:

1. Go to [GCP Console IAM Page](https://console.cloud.google.com/iam-admin/iam?project=sac-vald-hub)
2. Find the service account: `gha-bq@sac-vald-hub.iam.gserviceaccount.com`
3. Click the ✏️ (Edit) button
4. Click "+ ADD ANOTHER ROLE"
5. Search for and select "BigQuery Data Owner"
6. Click "SAVE"

## Verification

After granting the permission, verify it works:

```bash
# Test using gcloud
gcloud auth activate-service-account gha-bq@sac-vald-hub.iam.gserviceaccount.com \
  --key-file=/path/to/key.json

bq update --location=US --default_table_expiration 0 sac-vald-hub:analytics

# Should succeed without "Permission denied" error
```

## Why This Permission Is Needed

The workflow file (`.github/workflows/run-every-15m.yml`) includes this step:

```yaml
- name: Ensure BigQuery dataset exists
  if: always()
  run: |
    bq mk --dataset --location=US --default_table_expiration 0 "${GCP_PROJECT}:${BQ_DATASET}" || echo "Dataset exists or creation failed"
    bq update --location=US --default_table_expiration 0 "${GCP_PROJECT}:${BQ_DATASET}" || echo "Dataset update failed"
```

The `bq update` command modifies dataset metadata (specifically, the default table expiration setting), which requires the `bigquery.datasets.update` permission.

## Alternative: Remove the Update Step

If you don't actually need to update the dataset's default table expiration, you can remove this step from the workflow:

**Edit `.github/workflows/run-every-15m.yml`** (lines 111-119):

```yaml
- name: Ensure BigQuery dataset exists
  if: always()
  run: |
    bq mk --dataset --location=US --default_table_expiration 0 "${GCP_PROJECT}:${BQ_DATASET}" || echo "Dataset exists or creation failed"
    # REMOVED: bq update command
```

However, this means the dataset's default table expiration won't be managed by the workflow.

## Recommended Action

**Use Option 1** (BigQuery Data Owner role) - this provides the right level of permissions for a data pipeline service account that needs to:
- Create/update datasets
- Create/update tables  
- Write/read data
- Manage table metadata

---

**Status**: READY TO IMPLEMENT
**Priority**: MEDIUM (causes errors but doesn't block main pipeline)
**Est. Time**: 2 minutes
