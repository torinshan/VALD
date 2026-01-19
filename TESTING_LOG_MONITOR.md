# BigQuery Log Monitoring - Testing Guide

This document provides instructions for testing the new BigQuery log monitoring feature.

## Overview

The log monitoring system:
- Runs automatically every 15 minutes via GitHub Actions
- Analyzes logs from the last 3 days (configurable)
- Generates comprehensive summaries of errors, warnings, and issues
- Can be triggered manually for immediate analysis

## Manual Testing

### Option 1: GitHub Actions (Recommended)

1. Navigate to the repository on GitHub
2. Click on the "Actions" tab
3. Select "Monitor BigQuery Logs (Every 15m)" from the workflow list
4. Click "Run workflow" button
5. Optionally change "Number of days to look back" (default: 3)
6. Click the green "Run workflow" button to start
7. Wait for the workflow to complete (~2-3 minutes)
8. Click on the workflow run to see detailed logs and summaries

### Option 2: Local Testing (Requires R and GCP credentials)

If you have R installed and GCP credentials set up locally:

```bash
# Set environment variables
export GCP_PROJECT="sac-vald-hub"
export BQ_DATASET="analytics"
export LOOKBACK_DAYS="3"
export GOOGLE_APPLICATION_CREDENTIALS="path/to/your/credentials.json"

# Run the script
Rscript .github/scripts/monitor_bigquery_logs.R
```

## Expected Output

The monitoring script provides the following sections:

### 1. Connection Status
- ✅ BigQuery connection established
- ✅ Log table exists

### 2. Errors & Failures
- Total error count
- Errors grouped by type:
  - Authentication errors
  - Timeout errors
  - API errors
  - BigQuery errors
  - Connection errors
  - Missing data/resource errors
  - Invalid data errors
- Most recent 10 errors with timestamps

### 3. Warnings
- Total warning count
- Most recent 10 warnings with timestamps

### 4. Workflow Run Statistics
- Total runs in the period
- Runs with errors
- Runs with warnings
- Successful runs
- Recent 15 workflow runs with:
  - Run ID
  - Start time
  - Duration
  - Error/warning counts
  - Status (✅ SUCCESS / ⚠️ WITH WARNINGS / ❌ FAILED)

### 5. Common Issue Patterns
- Frequently occurring issues:
  - VALD API Timeout
  - No Data Available
  - Processing Skipped
  - Authentication Failed
  - Missing Table
  - Data Fetch Failed
- Occurrence counts and time ranges

### 6. Daily Summary
- Activity by day showing:
  - Total logs
  - Number of runs
  - Error count
  - Warning count

## Validation Checklist

When testing, verify that:

- [ ] The workflow runs successfully without errors
- [ ] The script connects to BigQuery properly
- [ ] Log entries are retrieved for the specified time period
- [ ] Error and warning summaries are generated
- [ ] Workflow run statistics are calculated correctly
- [ ] Common issue patterns are detected
- [ ] Daily summaries show activity trends
- [ ] The output is readable and well-formatted
- [ ] The workflow completes within the 10-minute timeout

## Troubleshooting

### Issue: "BigQuery authentication failed"
**Solution**: Verify that:
- GitHub Actions has the correct GCP credentials configured
- The service account has BigQuery read permissions
- Workload Identity Federation is set up correctly

### Issue: "Log table does not exist yet"
**Solution**: This is normal if no workflows have run yet. The log table will be created after the first workflow that logs to BigQuery runs.

### Issue: "No logs found for analysis"
**Solution**: This may be normal if:
- The workflows haven't run recently
- The lookback period is too short
- The log table is empty

Try increasing the `lookback_days` parameter or wait for scheduled workflows to run.

## Scheduled Execution

The workflow runs automatically every 15 minutes (synchronized with the main data processing workflows).

To modify the schedule:
1. Edit `.github/workflows/monitor-bigquery-logs.yml`
2. Change the cron expression under `schedule:`
3. Commit and push the changes

Current schedule: `*/15 * * * *` (every 15 minutes)

## Customization

### Change Lookback Period

Edit the workflow file or pass as input:
```yaml
env:
  LOOKBACK_DAYS: 7  # Change to 7 days
```

### Modify Output Format

Edit `.github/scripts/monitor_bigquery_logs.R` to:
- Add new analysis sections
- Change report formatting
- Export results to files
- Add alerting logic

## Integration with Existing Workflows

The log monitoring workflow is independent and non-blocking:
- It only reads from BigQuery (no writes)
- Failures in log monitoring don't affect other workflows
- It uses the same authentication as other workflows
- Logs from all workflows are analyzed together

## Next Steps

After validating the log monitoring:
1. Monitor the workflow runs over a few days
2. Review the error patterns and trends
3. Consider setting up alerts for critical errors
4. Adjust the analysis logic based on common issues found
