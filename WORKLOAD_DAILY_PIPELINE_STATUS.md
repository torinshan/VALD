# Workload Daily Pipeline - Operational Status & Date Range Restrictions

## Quick Answer to Your Question

**Q: Is the workload_daily section still operational?**  
**A:** ✅ **YES** - Fully operational and running automatically every 15 minutes.

**Q: Are there date range restrictions?**  
**A:** ⚠️ **YES, for model training only:**
- **Workload ingestion:** No restrictions (all data from Excel is loaded)
- **Model training:** Restricted to **June 1, 2025 - December 31, 2025** (configurable)
- **Actual data range:** March 4, 2025 - November 11, 2025
- **⚠️ Impact:** Currently excluding 3 months of early season data (March-May 2025)

---

## Executive Summary

**Status:** ✅ **OPERATIONAL**

The workload_daily pipeline is currently operational and runs automatically every 15 minutes via GitHub Actions. However, there are **configurable date range restrictions** that limit which data is processed during model training.

---

## Pipeline Overview

### 1. Workload Ingestion (ALWAYS RUNS)
- **Source:** Local Excel file `.github/Clean_Activities_GPS.xlsx` (7.7 MB)
- **Destination:** BigQuery table `sac-ml-models.analytics.workload_daily`
- **Write Mode:** MERGE (upserts based on roster_name + date)
- **Script:** `.github/scripts/workload_ingest.R`
- **Frequency:** Every 15 minutes via cron: `*/15 * * * *`
- **Date Range:** NO RESTRICTIONS - All data from the Excel file is ingested
- **⚠️ Processing Filter:** First 6 days per athlete are dropped (needed for 7-day rolling features)
  - This is a **data processing requirement**, not a configuration setting
  - Example: If athlete's first record is March 4, 2025, their data starts from March 10, 2025
  - Impact: ~6 days of data lost per athlete at the beginning of their timeline

### 2. Model Training (CONDITIONAL)
- **Script:** `.github/scripts/readiness_models_cloud.R`
- **Frequency:** Every 15 minutes (after successful ingestion)
- **Date Range:** ⚠️ **RESTRICTED** - See section below

---

## Date Range Restrictions

### Overview
There are **THREE different date filters** in the pipeline, each serving a different purpose:

1. **Workload Ingestion:** NO date restrictions (all data loaded)
2. **Readiness Check (matching validation):** Last 90 days only
3. **Model Training:** Configurable range (default: June 1 - Dec 31, 2025)

### 1. Workload Ingestion Filter
**NO date restrictions** - all data from the Excel file is loaded into BigQuery's `workload_daily` table.

### 2. Readiness Check Filter (90-day lookback)
**Purpose:** Pre-flight check to verify matching workload and readiness data exists  
**Location:** `.github/workflows/combined_workflow.yml` (line 361, 368)  
**Filter:** `WHERE w.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)`

**Impact:**
- Only checks for matches in the **last 90 days**
- If no matches found in last 90 days → model training is **skipped**
- This is a **safety check** to avoid wasting resources when there's no recent data to train on
- Does NOT affect the actual training date range (see #3 below)

### 3. Model Training Filter (Configurable)
**Purpose:** Define the date range for actual model training data  
**Location:** `.github/workflows/combined_workflow.yml` (workflow inputs) and `readiness_models_cloud.R`  
**Filter:** Configurable via START_DATE and END_DATE environment variables

### Current Configuration (as of latest commit)

**Default Date Range:**
- **Start Date:** `2025-06-01` (June 1, 2025)
- **End Date:** `2025-12-31` (December 31, 2025)

**Configuration Location:**
```yaml
# File: .github/workflows/combined_workflow.yml
workflow_dispatch:
  inputs:
    start_date:
      description: "Model training start date (YYYY-MM-DD)"
      default: "2025-06-01"
    end_date:
      description: "Model training end date (YYYY-MM-DD)"
      default: "2025-12-31"

env:
  START_DATE: ${{ github.event.inputs.start_date || '2025-06-01' }}
  END_DATE:   ${{ github.event.inputs.end_date   || '2025-12-31' }}
```

### Where Date Restrictions Apply

The date range is used in the following SQL queries in `readiness_models_cloud.R`:

1. **Workload Data Load** (line 1273):
```r
SELECT 
  roster_name, DATE(date) AS date,
  distance, high_speed_distance, mechanical_load,
  distance_7d, distance_28d, distance_monotony_7d,
  hsd_7d, hsd_28d, ml_7d, ml_28d, ml_monotony_7d
FROM `sac-ml-models.analytics.workload_daily`
WHERE date BETWEEN '{cfg_start_date}' AND '{cfg_end_date}'
```

2. **Readiness Data Load** (line 1304):
```sql
WHERE date BETWEEN '{cfg_start_date}' AND '{cfg_end_date}'
```

3. **Matching Data Check** (lines 1059, 1079):
```sql
WHERE date BETWEEN '{cfg_start_date}' AND '{cfg_end_date}'
```

---

## How to Change Date Range

### Option 1: Manual Workflow Dispatch (Temporary)
1. Go to GitHub Actions → "Workload & Model Pipeline"
2. Click "Run workflow"
3. Enter custom dates:
   - Start date: e.g., `2024-01-01`
   - End date: e.g., `2025-12-31`
4. Click "Run workflow"

**Impact:** Only affects this single run

### Option 2: Update Default Configuration (Permanent)
Edit `.github/workflows/combined_workflow.yml`:

```yaml
workflow_dispatch:
  inputs:
    start_date:
      default: "2024-01-01"  # Change this
    end_date:
      default: "2025-12-31"  # Change this
```

AND update the environment variable fallbacks:

```yaml
env:
  START_DATE: ${{ github.event.inputs.start_date || '2024-01-01' }}  # Change this
  END_DATE:   ${{ github.event.inputs.end_date   || '2025-12-31' }}   # Change this
```

**Impact:** Affects all automatic runs (every 15 minutes)

---

## Important Notes

### 1. Workload Ingestion vs Model Training
- **Workload ingestion** writes ALL data from Excel to BigQuery (no date filtering)
- **Model training** only uses data within the configured date range
- Historical data outside the range remains in BigQuery but is not used for training

### 2. Why Date Restrictions Exist
The date range restrictions serve several purposes:
- **Performance:** Limit the amount of data processed during each training run
- **Relevance:** Focus on recent data for model training (e.g., current season)
- **Testing:** Allow training on specific date ranges for validation
- **Data Quality:** Exclude dates with known data quality issues

### 3. Implications of Current Settings (2025-06-01 to 2025-12-31)

**Current Data in Excel File:**
- **Earliest date:** March 4, 2025
- **Latest date:** November 11, 2025
- **Total records:** 9,256 rows
- **Athletes:** 120 unique athletes

**Impact of Date Range Configuration:**
- ✅ **Includes:** Data from June 1, 2025 to November 11, 2025 (latest available)
- ⚠️ **Excludes:** Data from March 4, 2025 to May 31, 2025 (~3 months of historical data)
- ⚠️ **Potential Issue:** Missing ~3 months of early season data for model training

As of December 2, 2025, this configuration:
- ✅ Will include most data (June through November 2025)
- ⚠️ Will exclude early season data (March - May 2025)
- ✅ Will continue to include new data as it arrives (through Dec 31, 2025)
- ⚠️ **Will need updating after Dec 31, 2025** to include 2026 data

### 4. Recommended Action

**⚠️ ACTION REQUIRED:** Based on analysis of the actual data:

The current configuration excludes **3 months of early season data** (March - May 2025). To include all available data:

**Recommended Configuration:**
```yaml
start_date:
  default: "2025-03-01"  # Captures all data from March 4, 2025 onward
end_date:
  default: "2026-12-31"  # Extends through next season
```

**Why This Matters:**
- More training data generally improves model accuracy
- Early season data may contain important baseline measurements
- Missing data could affect model performance for athletes who only have early season records

**How to Update:**
1. Edit `.github/workflows/combined_workflow.yml`
2. Update both the `workflow_dispatch.inputs` defaults and the `env` fallback values
3. Commit and push changes
4. Next automatic run (within 15 minutes) will use the full date range

---

## Pipeline Components

### Data Flow
```
Excel File (.github/Clean_Activities_GPS.xlsx)
    ↓
[workload_ingest.R] - Processes and enriches data
    ↓
BigQuery: sac-ml-models.analytics.workload_daily (ALL DATA)
    ↓
[readiness_models_cloud.R] - Reads with date filter
    ↓
Model Training (FILTERED BY DATE RANGE)
    ↓
Predictions: sac-ml-models.analytics.readiness_predictions_byname
```

### Related Tables
- **workload_daily**: Contains all ingested workload data
- **vald_fd_jumps_test_copy**: Readiness/VALD jump test data (also date-filtered)
- **roster_mapping**: Athlete roster (no date filtering)
- **readiness_predictions_byname**: Model predictions output

---

## Troubleshooting

### Issue: "No matching data found"
**Possible Cause:** Date range doesn't overlap with available data
**Solution:** 
1. Check what dates exist in `workload_daily`:
```sql
SELECT MIN(date) as min_date, MAX(date) as max_date 
FROM `sac-ml-models.analytics.workload_daily`
```
2. Adjust START_DATE and END_DATE to overlap with available data

### Issue: "Models not training on recent data"
**Possible Cause:** END_DATE is in the past
**Solution:** Update END_DATE to future date (e.g., `2026-12-31`)

### Issue: "Missing historical data in training"
**Possible Cause:** START_DATE is too recent
**Solution:** Update START_DATE to earlier date to include historical data

---

## Monitoring

To verify the pipeline is operational:

1. **Check Workflow Runs:**
   - GitHub Actions → "Workload & Model Pipeline"
   - Should show runs every 15 minutes
   - Check for ✅ success status

2. **Check Workload Data:**
```sql
SELECT 
  COUNT(*) as total_rows,
  MIN(date) as earliest_date,
  MAX(date) as latest_date,
  COUNT(DISTINCT roster_name) as athlete_count
FROM `sac-ml-models.analytics.workload_daily`
```

3. **Check Training Output:**
```sql
SELECT 
  COUNT(*) as predictions_count,
  MAX(trained_at) as last_training_time
FROM `sac-ml-models.analytics.readiness_predictions_byname`
WHERE DATE(trained_at) = CURRENT_DATE()
```

---

## Summary

| Component | Status | Date Restrictions | Filter Type |
|-----------|--------|-------------------|-------------|
| Workload Ingestion | ✅ Operational | ⚠️ First 6 days/athlete dropped | Processing requirement |
| Workload Table | ✅ Operational | ❌ None - stores all data | N/A |
| Readiness Check | ✅ Operational | ⚠️ Last 90 days only | Pre-flight validation |
| Model Training | ✅ Operational | ⚠️ Yes - 2025-06-01 to 2025-12-31 | Configurable |
| Training Data Query | ✅ Operational | ⚠️ Uses same range as training | Configurable |

**Key Takeaways:**
1. ✅ Pipeline is **fully operational** - runs every 15 minutes automatically
2. ⚠️ **3 months of early season data excluded** due to model training date range (March-May 2025)
3. ⚠️ **First 6 days per athlete** always excluded (needed for rolling features calculation)
4. ⚠️ Readiness check only validates **last 90 days** (doesn't affect actual training range)
5. 🔧 Date ranges are **easily configurable** - can be changed without code modifications

**Recommended Next Steps:**
1. Update START_DATE to `2025-03-01` to include all available data
2. Update END_DATE to `2026-12-31` to prepare for next season
3. Verify matches exist in last 90 days (otherwise training will skip)
