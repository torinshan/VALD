# VALD Workflow Failures - Complete Analysis and Solutions

## Overview

This document provides a complete analysis of the VALD workflow failures and actionable solutions. The workflow `run-every-15m.yml` is currently failing due to multiple issues.

## Issues Identified

### 1. ⚠️ CRITICAL: VALD API Timeout (ROOT CAUSE)

**Status**: EXTERNAL DEPENDENCY ISSUE - Cannot be fixed directly in this repository

**Problem**: Workflow times out after 900 seconds (15 minutes) while fetching ForceDecks data from the VALD API.

**Root Cause**: The `valdr` R package (external dependency) fetches trials sequentially (1 per second), taking 45-90 minutes for 5,000+ tests.

**Evidence**: Log shows "Fetching trials for Test ID: ..." messages appearing one at a time, coming from within the valdr package.

**📖 Detailed Analysis**: See `VALD_API_TIMEOUT_FIX_SOLUTION.md`
**📋 Action Plan**: See `IMMEDIATE_FIXES_REQUIRED.md`

**Solutions**:
1. **Proper Fix** (REQUIRED): Fix or replace the `valdr` package
   - Contact valdr maintainers to report the issue
   - OR fork valdr and implement batch/parallel fetching
   - OR implement custom VALD API client

2. **Temporary Mitigation** (While waiting for proper fix):
   - Increase workflow timeout to 90 minutes
   - Increase script timeout to 3600 seconds
   - Optimize fetch window to reduce data volume

### 2. BigQuery Permissions Error

**Status**: FIXABLE - Configuration issue

**Problem**: Service account lacks `bigquery.datasets.update` permission

**Error Message**:
```
BigQuery error in update operation: Access Denied: Dataset sac-vald-hub:analytics: 
Permission bigquery.datasets.update denied
```

**📖 Detailed Fix**: See `BIGQUERY_PERMISSIONS_FIX.md`

**Solution**: Grant BigQuery Data Owner role to `gha-bq@sac-vald-hub.iam.gserviceaccount.com`

```bash
gcloud projects add-iam-policy-binding sac-vald-hub \
  --member="serviceAccount:gha-bq@sac-vald-hub.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataOwner"
```

### 3. Email Notification Failure

**Status**: FIXABLE - Configuration issue

**Problem**: Invalid Gmail credentials

**Error Message**:
```
Invalid login: 535-5.7.8 Username and Password not accepted
```

**Solution**:  
**Option A**: Disable email notifications (if not needed)  
**Option B**: Update GitHub secrets with valid Gmail App Password

See `IMMEDIATE_FIXES_REQUIRED.md` for step-by-step instructions.

## Quick Start Guide

### For Immediate Relief (Can be done in 30 minutes)

1. **Fix BigQuery Permissions** (5 min)
   ```bash
   gcloud projects add-iam-policy-binding sac-vald-hub \
     --member="serviceAccount:gha-bq@sac-vald-hub.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataOwner"
   ```

2. **Disable Email Notifications** (2 min)
   - Comment out the "Send failure notification" step in `.github/workflows/run-every-15m.yml`

3. **Increase Timeouts** (5 min)
   - Edit `.github/workflows/run-every-15m.yml`: Change timeout-minutes to 90
   - Edit `.github/scripts/run.R` line 1061: Change timeout_seconds to 3600

4. **Test the Workflow**
   ```bash
   gh workflow run run-every-15m.yml
   ```

### For Permanent Fix (Requires 1-2 weeks)

1. **Identify valdr Package Repository**
   - Search for valdr on GitHub/CRAN
   - Check package documentation

2. **Choose Your Approach**:
   - **Option A**: Contact valdr maintainers with this analysis
   - **Option B**: Fork valdr and implement the fixes yourself
   - **Option C**: Implement custom VALD API client

3. **Implement the Fix**
   - See `VALD_API_TIMEOUT_FIX_SOLUTION.md` for detailed implementation guidance
   - Key: Replace sequential trial fetching with batch or parallel approach

4. **Deploy and Test**
   - Update workflow to use fixed version
   - Test with production data
   - Monitor execution times

5. **Remove Temporary Mitigations**
   - Reduce timeouts back to reasonable levels
   - Re-enable optimizations

## Documentation Index

| Document | Purpose | Audience |
|----------|---------|----------|
| `README_VALD_FAILURES.md` (this file) | Overview and quick start | Everyone |
| `IMMEDIATE_FIXES_REQUIRED.md` | Step-by-step immediate actions | DevOps/Implementation |
| `VALD_API_TIMEOUT_FIX_SOLUTION.md` | Technical analysis and solutions | Developers |
| `BIGQUERY_PERMISSIONS_FIX.md` | BigQuery permission fix details | DevOps/GCP Admin |

## Current Status

| Issue | Status | ETA |
|-------|--------|-----|
| VALD API Timeout | ⏳ Waiting for valdr fix | 1-2 weeks |
| BigQuery Permissions | ✅ Solution ready | 5 minutes |
| Email Notifications | ✅ Solution ready | 2 minutes |
| Workflow Stability | ⚠️ Degraded (disabled) | After timeout fix |

## Success Criteria

- ✅ Workflow completes successfully within 30 minutes
- ✅ All ForceDecks tests processed (5,000+ tests)
- ✅ No BigQuery permission errors
- ✅ Email notifications working (if enabled)
- ✅ Robust error handling and recovery

## Support

If you need help:
- **VALD API Issues**: See `VALD_API_TIMEOUT_FIX_SOLUTION.md`
- **Permissions**: See `BIGQUERY_PERMISSIONS_FIX.md`
- **Immediate Actions**: See `IMMEDIATE_FIXES_REQUIRED.md`
- **Questions**: Create a GitHub issue in this repository

## Timeline

```
Week 1: Implement immediate fixes (permissions, timeouts, email)
        Contact valdr maintainers or start fork
        
Week 2: Test immediate fixes in production
        Implement valdr fixes or custom client
        
Week 3: Deploy and test valdr fixes
        Monitor performance improvements
        
Week 4: Remove temporary mitigations
        Document final solution
        Enable workflow schedule
```

---

**Last Updated**: 2026-01-07  
**Status**: DOCUMENTED - READY FOR IMPLEMENTATION  
**Priority**: HIGH - Workflow currently disabled
