# Example Output - BigQuery Log Monitor

This document shows an example of what the log monitoring script output looks like.

## Console Output Example

```
=================================================================
     BigQuery Log Monitor - Last 3 Days
=================================================================
Project: sac-vald-hub
Dataset: analytics
Table: vald_processing_log
Lookback Period: 3 days
Current Time (UTC): 2026-01-11 21:15:00 UTC
=================================================================

Starting BigQuery log monitoring...

✅ BigQuery connection established
✅ Log table exists

📊 Querying logs from BigQuery...
   Retrieved: 1,247 log entries
   Date Range: 2026-01-08 21:15 to 2026-01-11 21:10

🔍 Analyzing logs for errors and issues...

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   ERRORS & FAILURES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Total Errors Found: 15

📋 Error Summary by Type:
# A tibble: 4 × 3
  error_type       count latest             
  <chr>            <int> <dttm>             
1 Timeout              8 2026-01-11 18:45:00
2 API Error            4 2026-01-11 15:30:00
3 Missing Data         2 2026-01-10 12:20:00
4 Connection           1 2026-01-09 09:15:00

🕐 Most Recent Errors (Last 10):
# A tibble: 10 × 4
   timestamp           level message                                          run_id      
   <chr>               <chr> <chr>                                            <chr>       
 1 2026-01-11 18:45:00 ERROR VALD API request timed out after 30s (test da… 12345678901
 2 2026-01-11 18:00:00 ERROR VALD API request timed out after 30s (test da… 12345678902
 3 2026-01-11 15:30:00 ERROR API response invalid: missing required field … 12345678903
 4 2026-01-11 14:15:00 ERROR VALD API request timed out after 30s (atletes… 12345678904
 5 2026-01-11 11:45:00 ERROR VALD API request timed out after 30s (test da… 12345678905
 6 2026-01-10 20:30:00 ERROR VALD API request timed out after 30s (test da… 12345678906
 7 2026-01-10 18:00:00 ERROR API response invalid: unexpected format         12345678907
 8 2026-01-10 12:20:00 ERROR Missing data: no workload records for athlete… 12345678908
 9 2026-01-10 09:45:00 ERROR VALD API request timed out after 30s (test da… 12345678909
10 2026-01-09 16:30:00 ERROR VALD API request timed out after 30s (test da… 12345678910

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   WARNINGS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Total Warnings Found: 28

🕐 Most Recent Warnings (Last 10):
# A tibble: 10 × 4
   timestamp           level message                                          run_id      
   <chr>               <chr> <chr>                                            <chr>       
 1 2026-01-11 20:00:00 WARN  No new data since last run - STANDDOWN mode      12345678911
 2 2026-01-11 19:15:00 WARN  Processing partial data due to timeout           12345678912
 3 2026-01-11 17:45:00 WARN  Skipping NordBord processing - fetch timeout     12345678913
 4 2026-01-11 16:30:00 WARN  Log upload took longer than expected (5.2s)      12345678914
 5 2026-01-11 14:00:00 WARN  No new data since last run - STANDDOWN mode      12345678915
 6 2026-01-11 12:45:00 WARN  Processing partial data due to timeout           12345678916
 7 2026-01-11 10:15:00 WARN  No matching readiness data for training          12345678917
 8 2026-01-10 21:30:00 WARN  Skipping training - insufficient observations    12345678918
 9 2026-01-10 19:45:00 WARN  No new data since last run - STANDDOWN mode      12345678919
10 2026-01-10 17:00:00 WARN  Processing partial data due to timeout           12345678920

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   WORKFLOW RUN STATISTICS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Total Workflow Runs: 288
Runs with Errors: 15
Runs with Warnings: 28
Successful Runs: 245

📋 Recent Workflow Runs (Last 15):
# A tibble: 15 × 6
   run_id       start_time      duration_min num_errors num_warnings status           
   <chr>        <chr>                  <dbl>      <int>        <int> <chr>            
 1 12345678911  2026-01-11 20:00         2.3          0            1 ⚠️  WITH WARNINGS
 2 12345678912  2026-01-11 19:15         8.7          0            1 ⚠️  WITH WARNINGS
 3 12345678913  2026-01-11 18:45        15.2          1            1 ❌ FAILED        
 4 12345678914  2026-01-11 18:00        15.1          1            0 ❌ FAILED        
 5 12345678915  2026-01-11 17:15         4.5          0            0 ✅ SUCCESS       
 6 12345678916  2026-01-11 16:30         5.2          0            1 ⚠️  WITH WARNINGS
 7 12345678917  2026-01-11 15:45         4.8          0            0 ✅ SUCCESS       
 8 12345678918  2026-01-11 15:00         4.3          0            0 ✅ SUCCESS       
 9 12345678919  2026-01-11 14:15         6.1          1            0 ❌ FAILED        
10 12345678920  2026-01-11 13:30         5.5          0            0 ✅ SUCCESS       
11 12345678921  2026-01-11 12:45         9.2          0            1 ⚠️  WITH WARNINGS
12 12345678922  2026-01-11 12:00         4.7          0            0 ✅ SUCCESS       
13 12345678923  2026-01-11 11:15         4.9          0            0 ✅ SUCCESS       
14 12345678924  2026-01-11 10:30         5.1          0            1 ⚠️  WITH WARNINGS
15 12345678925  2026-01-11 09:45         4.6          0            0 ✅ SUCCESS       

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   COMMON ISSUE PATTERNS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 Frequently Occurring Issues:
# A tibble: 3 × 4
  issue                 occurrences first_seen          last_seen          
  <chr>                       <int> <chr>               <chr>              
1 VALD API Timeout               42 2026-01-08 22:15:00 2026-01-11 18:45:00
2 No Data Available              18 2026-01-08 21:30:00 2026-01-11 20:00:00
3 Processing Skipped             12 2026-01-09 08:00:00 2026-01-11 19:15:00

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
   DAILY SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 Activity by Day:
# A tibble: 3 × 5
  date       total_logs num_runs errors warnings
  <date>          <int>    <int>  <int>    <int>
1 2026-01-11        623       95      8       15
2 2026-01-10        412       96      5        9
3 2026-01-09        212       97      2        4

=================================================================
     Log Monitoring Complete
=================================================================
Summary generated at: 2026-01-11 21:15:42 UTC
=================================================================
```

## Interpretation Guide

### Error Analysis
- **Timeout errors** are the most common (8 occurrences) - indicates VALD API performance issues
- **API errors** suggest data format or connectivity problems
- Most errors occur during peak hours (3-7 PM PT)

### Warning Analysis
- "No new data" warnings indicate STANDDOWN mode is working correctly
- Processing partial data due to timeout suggests 15-minute window might be tight for some operations

### Run Statistics
- Overall success rate: 85% (245/288 successful runs)
- 5.2% failure rate (15/288) - primarily due to API timeouts
- Average run duration: ~5-6 minutes when successful, 15+ minutes when timing out

### Common Issues
1. **VALD API Timeout**: Most frequent issue - consider:
   - Increasing timeout threshold
   - Implementing retry logic
   - Contacting VALD support about API performance
   
2. **No Data Available**: Expected during off-hours or non-training days
   
3. **Processing Skipped**: Normal when criteria aren't met (e.g., insufficient data)

### Recommendations
Based on this example output:
- Monitor API timeout frequency - if increasing, escalate to VALD
- Consider caching strategies for frequently accessed data
- Review whether 15-minute frequency is optimal given timeout patterns
- Investigate if API timeouts correlate with specific data types or athletes
