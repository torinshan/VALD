# VALD Readiness & Workload Analytics

## Overview

This repository provides an automated data pipeline for athlete readiness monitoring and workload management. It integrates VALD force plate testing data with GPS workload metrics to predict athlete readiness using machine learning models, specifically designed for Sacramento State athletic programs.

## Purpose

The system helps athletic trainers and coaches:
- **Monitor Athlete Readiness**: Track force plate jump metrics (CMJ) to assess neuromuscular fatigue and readiness
- **Manage Workload**: Ingest and analyze GPS-based training loads from practice and competition
- **Predict Performance**: Use machine learning models (Ridge, Lasso, Elastic Net, Bayesian Networks) to predict athlete readiness based on historical workload patterns
- **Identify Risk**: Flag athletes who may be at increased injury risk due to overtraining or inadequate recovery

## Key Features

### 1. Automated Data Ingestion
- **Workload Data**: Reads GPS activity data from local Excel files and syncs to BigQuery
- **Readiness Data**: Syncs VALD force plate testing data across Google Cloud projects
- **Roster Management**: Maintains athlete identity mapping between workload and readiness systems

### 2. Machine Learning Pipeline
- **Model Training**: Trains individualized predictive models for each athlete
- **Validation**: Implements adaptive z-score outlier detection and CMJ test validation
- **Cross-validation**: Uses time-aware cross-validation to prevent data leakage
- **Model Selection**: Automatically selects best-performing model per athlete (Ridge/Lasso/Elastic Net/Bayesian)

### 3. Dual-Project Architecture
- **Source Project** (`sac-vald-hub`): Stores original VALD readiness data
- **ML Project** (`sac-ml-models`): Stores workload, roster, trained models, and predictions

## Technology Stack

- **Language**: R (primary), Python (via reticulate for sklearn integration)
- **Cloud Platform**: Google Cloud Platform (BigQuery)
- **CI/CD**: GitHub Actions workflows
- **Key R Libraries**:
  - `bigrquery`, `DBI`: BigQuery integration
  - `dplyr`, `tidyr`, `data.table`: Data manipulation
  - `glmnet`: Ridge/Lasso/Elastic Net regression
  - `bnlearn`: Bayesian network modeling
  - `reticulate`: Python integration
  - `lubridate`, `slider`: Time-series analysis

## Workflows

### Combined Workflow (`combined_workflow.yml`)
The main orchestration pipeline that runs on-demand or scheduled intervals:

1. **Ingest Jobs** (Parallel):
   - Ingest workload data from local Excel file
   - Ingest roster mapping from local Excel file

2. **Check Readiness**:
   - Sync VALD data from source project if updates available
   - Check for matching workload/readiness data pairs

3. **Train Models** (Conditional):
   - Train individualized ML models when matching data exists
   - Write predictions to BigQuery
   - Generate training summary reports

4. **Summary**:
   - Report pipeline status and metrics

### Other Workflows
- **Saturday Makeup**: Special handling for weekend testing sessions
- **Tertiary FD Defense**: Position-specific defensive player analysis
- **Workload Calc**: Standalone workload calculation workflow
- **Debug Auth**: Authentication troubleshooting utilities
- **Monitor BigQuery Logs**: Automated log monitoring that runs every 15 minutes to analyze errors and issues from the last 3 days

## Data Structure

### Input Files
- **Clean_Activities_GPS.xlsx**: GPS workload metrics (distance, velocity, acceleration)
- **Sac State Roster - [Season].xlsx**: Athlete roster with name mapping (e.g., "Summer 2025")
- **vald_roster.csv**: VALD system athlete identifiers

### BigQuery Tables
- `workload_daily`: Daily GPS training load metrics per athlete
- `vald_fd_jumps_test_copy`: Force plate CMJ test results (height, power, velocity)
- `roster_mapping`: Links official names to VALD system names
- `readiness_predictions_byname`: Model predictions and confidence metrics
- `model_training_summary`: Training metadata and performance metrics

## How It Works

1. **Data Collection**: Athletes complete force plate testing (CMJ) and wear GPS units during training
2. **Ingestion**: Automated workflows upload data to BigQuery (can run on-demand or on a schedule)
3. **Data Matching**: System joins workload and readiness data by athlete and date using roster mapping
4. **Model Training**: Individual models learn the relationship between prior workload and subsequent readiness metrics
5. **Prediction**: Models predict expected readiness for upcoming sessions
6. **Alerting**: Deviations from predicted readiness flag potential fatigue or injury risk

## Log Monitoring

The system includes automated log monitoring to track errors and issues:

### Features
- **Automated Monitoring**: Runs every 15 minutes via GitHub Actions
- **Historical Analysis**: Reviews logs from the last 3 days (configurable)
- **Comprehensive Reporting**: Generates summaries including:
  - Error counts and classifications (Authentication, Timeout, API, BigQuery, etc.)
  - Warning summaries
  - Workflow run statistics (success/failure rates)
  - Common issue patterns
  - Daily activity summaries

### Running Manually
The log monitor can be triggered manually via GitHub Actions:
1. Go to Actions → "Monitor BigQuery Logs (Every 15m)"
2. Click "Run workflow"
3. Optionally specify number of days to look back (default: 3)

### Log Table
All processing logs are stored in the `vald_processing_log` BigQuery table with:
- `timestamp`: When the log entry was created
- `level`: Log level (INFO, WARN, ERROR)
- `message`: Log message content
- `run_id`: GitHub workflow run identifier
- `repository`: Repository name

## Configuration

Key environment variables:
- `GCP_PROJECT_ML`: Destination project for models and predictions
- `GCP_PROJECT_READINESS`: Source project for VALD data
- `BQ_DATASET`: BigQuery dataset name (default: `analytics`)
- `TEAM_NAME`: Team identifier (default: `sacstate-football`)
- `START_DATE`/`END_DATE`: Model training date range
- `MIN_TRAIN_OBS`: Minimum observations required for training (default: 3)

## Security

- Service account credentials managed via GitHub Secrets
- Dual-project authentication for read/write separation
- BigQuery Storage API disabled for compatibility with free tier
- No sensitive data committed to repository

## Maintainers

This system is designed for Sacramento State Athletics. For questions or support, contact the athletic performance or sports science staff.