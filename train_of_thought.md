# Train of thought to get this to work

## Step 1: Getting everything working

### Issues found and fixed
1. Timeout of the DAG was set to 2 miliseconds, adjusted it to 30 seconds, as runs weren't taking more than 6 seconds
2. Issues when reading some csv files
    2.1. At first, I tried ignoring bad rows, as I thought it was an issue with the source data. I put in place logs to cound the amount of bad records, and was surprised that they were many.
    2.2. I then investigated why so many records were being dropped, and I noticed that free text fields were escaping double quotes with backwards slashes. So I removed the `on_bad_lines="skip"` and used `escapechar="\\"` instead. No errors were raised