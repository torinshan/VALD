# Immediate Fixes Required for VALD API Timeout Issue

## Executive Summary

**ROOT CAUSE**: The `valdr` R package (external dependency) has a pagination bug where it fetches trials sequentially for each test, causing timeouts when processing 5,000+ tests.

**STATUS**: Cannot be fixed directly in this repository. The valdr package must be updated by its maintainers OR you must fork and fix it.

## What You Need To Do RIGHT NOW

### Option A: Contact valdr Maintainers (RECOMMENDED)

1. Find the valdr package repository/maintainers
2. Report this issue with:
   - The log evidence (trials fetched sequentially)
   - Request for batch/parallel fetching implementation
   - Offer to collaborate on the fix

### Option B: Fork and Fix valdr Yourself

1. **Fork the valdr package**:
   ```bash
   # Find the valdr source repository (check CRAN or GitHub)
   # Fork it to your GitHub account (e.g., your-username/valdr)
   ```

2. **Implement the fix** in your fork:
   - Add batch fetching for trials
   - OR add parallel processing for trial fetching
   - See VALD_API_TIMEOUT_FIX_SOLUTION.md for implementation details

3. **Update this repository** to use your fork:
   - Edit `.github/workflows/run-every-15m.yml`
   - Change `valdr` to `your-username/valdr` in the dependencies (e.g., `torinshan/valdr`)

### Option C: Switch to Alternative API Client (If Available)

If there's an official VALD API client or a better-maintained alternative to `valdr`, switch to it.

## Configuration Improvements You Can Make TODAY

While waiting for valdr to be fixed, make these configuration improvements:

### 1. Fix BigQuery Permissions (5 minutes)

**Problem**: Service account lacks `bigquery.datasets.update` permission

**Fix**: Run this command:
```bash
gcloud projects add-iam-policy-binding sac-vald-hub \
  --member="serviceAccount:gha-bq@sac-vald-hub.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataOwner"
```

OR via GCP Console:
1. Go to IAM & Admin → IAM
2. Find `gha-bq@sac-vald-hub.iam.gserviceaccount.com`
3. Click Edit
4. Add role: "BigQuery Data Owner" OR "BigQuery Admin"
5. Save

### 2. Disable or Fix Email Notifications (2 minutes)

**Option A - Disable** (if you don't need them):

Edit `.github/workflows/run-every-15m.yml`, lines 130-149, and comment out the entire email step:

```yaml
# - name: Send failure notification
#   if: failure()
#   uses: dawidd6/action-send-mail@v3
#   with:
#     ...
```

**Option B - Fix** (if you need them):

1. Create Gmail App Password:
   - Go to Google Account → Security → 2-Step Verification → App Passwords
   - Generate a new app password

2. Update GitHub Secrets:
   - Go to Repository → Settings → Secrets and Variables → Actions
   - Add/Update:
     - `GMAIL_USERNAME`: your-actual-email@gmail.com
     - `GMAIL_APP_PASSWORD`: your-16-character-app-password

3. Update workflow file `.github/workflows/run-every-15m.yml`:
   ```yaml
   username: ${{ secrets.GMAIL_USERNAME }}
   password: ${{ secrets.GMAIL_APP_PASSWORD }}
   from: "VALD Bot <${{ secrets.GMAIL_USERNAME }}>"
   ```

### 3. Increase Timeouts (TEMPORARY - until valdr is fixed)

⚠️ **WARNING**: This is a TEMPORARY measure. It doesn't fix the root cause, just gives more time for the slow process to complete.

**Edit `.github/workflows/run-every-15m.yml`**:

```yaml
# Line 12: Increase workflow timeout
timeout-minutes: 90  # was 60

# This gives the workflow more time, but doesn't fix the underlying slowness
```

**Edit `.github/scripts/run.R`, line 1061**:

```r
# Old:
timeout_seconds = 900,

# New:
timeout_seconds = 3600,  # 60 minutes
```

This allows up to 3,600 tests to be fetched at 1 second each.

## Why These Are Not "Workarounds"

1. **BigQuery permissions**: This is a legitimate configuration error that needs to be fixed regardless
2. **Email notifications**: These are failing due to invalid credentials, not related to the main issue
3. **Timeout increase**: While temporary, it's appropriate to set timeouts based on expected data volume

The ROOT CAUSE (valdr pagination) still needs to be fixed for a permanent solution.

## Verification Steps

After making changes:

1. **Test the workflow**:
   ```bash
   # Trigger workflow manually
   gh workflow run run-every-15m.yml
   ```

2. **Monitor the run**:
   - Check if it completes (even if slowly)
   - Verify BigQuery permissions work
   - Confirm email notifications work (if enabled)

3. **Check execution time**:
   - If it completes in < 3600 seconds: Timeout fix is working
   - If it still times out: Need to either:
     - Increase timeout further (not recommended)
     - Fix valdr package (REQUIRED)

## Next Steps After Immediate Fixes

1. **Week 1**: Implement immediate fixes above, monitor stability
2. **Week 2**: Contact valdr maintainers or start fork
3. **Week 3**: Implement valdr fixes and test
4. **Week 4**: Deploy fixed valdr and remove timeout workarounds

## Questions or Issues?

If you need help with:
- Finding valdr package repository → Check R package documentation: `?valdr` or search GitHub
- Implementing valdr fixes → See VALD_API_TIMEOUT_FIX_SOLUTION.md for detailed code examples
- GCP permissions → Contact your GCP admin or see Google Cloud IAM documentation

---

**Status**: READY FOR IMPLEMENTATION
**Priority**: HIGH (workflow is currently disabled due to timeouts)
**Est. Time**: 30 minutes for immediate fixes, 1-2 weeks for root cause fix
