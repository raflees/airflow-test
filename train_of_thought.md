# Train of thought to get this to work

## Step 1: Getting everything working (PR #1)

### Issues found and fixed
1. Timeout of the DAG was set to 2 miliseconds, adjusted it to 30 seconds, as runs weren't taking more than 6 seconds
2. Issues when reading some csv files
    2.1. At first, I tried ignoring bad rows, as I thought it was an issue with the source data. I put in place logs to cound the amount of bad records, and was surprised that they were many.
    2.2. I then investigated why so many records were being dropped, and I noticed that free text fields were escaping double quotes with backwards slashes. So I removed the `on_bad_lines="skip"` and used `escapechar="\\"` instead. No errors were raised

### Cleanups / refactoring
1. Comformed some script import statements to PEP-8
2. Encapsulated global code in functions
3. Removed binary / temporary files and added them to .gitignore

**Note**: Made a mistake when creating a Pull Request. It went to the original repo,not the forked. You guys may wanna delete any public records so what I did is not easily accessible by other candidates

## Step 2: Changing the loaded schema to `raw` (PR #2)

### Reason

Raw data is usually loaded to a Data Lake, or some sort of staging schema before cleasing and feature engineering.

### Rationale

The `raw` schema could have been created during the initialization of the environment (`docker compose up`), but that would create a dependency between the main DAG and the environment initialization. Instead, I decided to create the schema if it didn't exist already, when the data was loaded

### Refactoring

- The postgres_helper module started becoming too big, so I made it into a class