# BigQuery Log Monitoring - Quick Start Guide

This guide helps you get started with the new BigQuery log monitoring feature in under 5 minutes.

## What This Does

Automatically monitors your VALD pipeline logs every 15 minutes and summarizes:
- ❌ **Errors**: Classified by type (Timeout, API, Auth, etc.)
- ⚠️  **Warnings**: Potential issues that don't fail workflows
- 📊 **Statistics**: Success/failure rates, run durations
- 🔍 **Patterns**: Frequently occurring issues
- 📅 **Trends**: Daily activity summaries

## How to Use It

### Option 1: View Automatic Runs (Easiest)

The monitoring runs automatically every 15 minutes. Just check the results:

1. Go to your repository on GitHub
2. Click the **"Actions"** tab
3. Look for **"Monitor BigQuery Logs (Every 15m)"** in the workflows list
4. Click on any recent run to see the summary

That's it! The monitoring is already running.

### Option 2: Trigger Manually (For Immediate Results)

Want to see results right now? Trigger it manually:

1. Go to **Actions** → **"Monitor BigQuery Logs (Every 15m)"**
2. Click **"Run workflow"** button (top right)
3. Optionally change "Number of days to look back" (default: 3)
4. Click the green **"Run workflow"** button
5. Wait 2-3 minutes
6. Click on the workflow run to see results

## What to Look For

When viewing the workflow logs, you'll see:

### 1. Error Summary
```
Total Errors Found: 15

📋 Error Summary by Type:
  error_type    count  latest             
1 Timeout           8  2026-01-11 18:45:00
2 API Error         4  2026-01-11 15:30:00
```

**What it means**: You had 8 timeout errors and 4 API errors in the last 3 days.

### 2. Recent Errors
```
🕐 Most Recent Errors (Last 10):
  timestamp              message                                   
1 2026-01-11 18:45:00   VALD API request timed out after 30s...
```

**What it means**: Shows the actual error messages with timestamps.

### 3. Workflow Statistics
```
Total Workflow Runs: 288
Runs with Errors: 15
Successful Runs: 245
```

**What it means**: Out of 288 runs in 3 days, 245 succeeded (85% success rate).

### 4. Common Issues
```
📋 Frequently Occurring Issues:
  issue                occurrences
1 VALD API Timeout            42
2 No Data Available           18
```

**What it means**: VALD API timeouts are your most common issue.

## Understanding the Results

### ✅ All Good
If you see:
- "No errors found in the last 3 days"
- High success rate (>90%)
- Few or no warnings

→ Your pipeline is healthy!

### ⚠️  Some Issues
If you see:
- Occasional errors (5-10% of runs)
- Mostly "No Data Available" or "Processing Skipped"

→ This is normal. Some runs don't have new data to process.

### ❌ Problems
If you see:
- Many timeout errors (>20% of runs)
- Frequent API errors
- Authentication failures

→ Investigate! Check:
1. VALD API status
2. Network connectivity
3. Service account permissions
4. Recent configuration changes

## Common Patterns and What They Mean

| Pattern | What It Means | Action Needed? |
|---------|---------------|----------------|
| "No new data since last run" | STANDDOWN mode working correctly | ✅ No |
| "VALD API timeout" | API response is slow | ⚠️  Monitor trend |
| "No matching readiness data" | Missing test data for training | ✅ Expected |
| "Processing partial data" | Timeout occurred, some data saved | ⚠️  Check if recurring |
| "Authentication failed" | Credentials issue | ❌ Yes, fix immediately |
| "Table not found" | Missing BigQuery table | ❌ Yes, fix immediately |

## Customization

### Change How Often It Runs

Edit `.github/workflows/monitor-bigquery-logs.yml`:

```yaml
schedule:
  - cron: "0 * * * *"  # Every hour instead of every 15 minutes
```

### Change Lookback Period

When running manually, set "Number of days to look back" input.

Or edit the workflow file:
```yaml
env:
  LOOKBACK_DAYS: 7  # Look back 7 days instead of 3
```

### Add Custom Analysis

Edit `.github/scripts/monitor_bigquery_logs.R` to:
- Add new error classifications
- Create custom reports
- Export results to files
- Add alerting logic

## Troubleshooting

### "Log table does not exist yet"
**Cause**: No workflows have created the log table yet.  
**Solution**: Wait for a workflow to run (they create the table automatically).

### "Failed to query logs"
**Cause**: Authentication or permissions issue.  
**Solution**: Check that Workload Identity Federation is configured correctly.

### "No logs found"
**Cause**: Lookback period might be too short, or workflows haven't run recently.  
**Solution**: Increase lookback period or wait for workflows to run.

## Need More Details?

- **Testing Guide**: `TESTING_LOG_MONITOR.md` - Complete testing instructions
- **Example Output**: `EXAMPLE_LOG_OUTPUT.md` - Detailed output example with interpretation
- **Technical Details**: `IMPLEMENTATION_SUMMARY.md` - Full technical documentation
- **User Documentation**: `README.md` - Log monitoring section

## Quick Reference

| Task | Steps |
|------|-------|
| View latest run | Actions → Monitor BigQuery Logs → Click recent run |
| Run manually | Actions → Monitor BigQuery Logs → Run workflow |
| Change schedule | Edit `.github/workflows/monitor-bigquery-logs.yml` |
| Customize analysis | Edit `.github/scripts/monitor_bigquery_logs.R` |
| Get help | See `TESTING_LOG_MONITOR.md` |

---

**That's it!** You're now monitoring your BigQuery logs automatically every 15 minutes. 🎉
