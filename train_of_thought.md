# Train of thought to get this to work

## Step 1: Getting everything working (PR #1)

### Reason

Airflow was not building correctly due to a couple bugs in the code.

### Rationale

1. Timeout of the DAG was set to 2 miliseconds, adjusted it to 30 seconds, as runs weren't taking more than 6 seconds
2. Issues when reading some csv files
    2.1. At first, I tried ignoring bad rows, as I thought it was an issue with the source data. I put in place logs to cound the amount of bad records, and was surprised that they were many.
    2.2. I then investigated why so many records were being dropped, and I noticed that free text fields were escaping double quotes with backwards slashes. So I removed the `on_bad_lines="skip"` and used `escapechar="\\"` instead. No errors were raised

### Refactoring

- Comformed some script import statements to PEP-8
- Encapsulated global code in functions
- Removed binary / temporary files and added them to .gitignore

**Note**: Made a mistake when creating a Pull Request. It went to the original repo,not the forked. You guys may wanna delete any public records so what I did is not easily accessible by other candidates

## Step 2: Changing the loaded schema to `raw` (PR #3)gi

### Reason

Raw data is usually loaded to a Data Lake, or some sort of staging schema before cleasing and feature engineering.

### Rationale

The `raw` schema could have been created during the initialization of the environment (`docker compose up`), but that would create a dependency between the main DAG and the environment initialization. Instead, I decided to create the schema if it didn't exist already, when the data was loaded

### Refactoring

- The postgres_helper module started becoming too big, so I made it into a class

## Step 3: Creating a transform pipeline with DBT (PR #4)

### Reason

DBT is a great tool for transforming data. It creates an "audit" trail, but encouraging the breakdown of transformations in small steps, each generating a model (VIEW or TABLE). It is also very readable and developer friendly, as most people in the field have knowledge of SQL.

Below is a list of transformations applied through a DBT pipeline

## Rationale

Two major operations can be employed with the raw data: unnesting and typing

**Unnesting** means extracting data from an unstructured format into a tabular format
**Typing** is changing the data types to conform to relational database types

### Other Operations
- Depedency installation
- Indexing
- Materialization
- Pivoting
- Renaming

## Refactoring

- Created a small DAG factory

## Step 4: Create a sentiment analysis DAG (PR #6)

### Reason

Implement a simple sentment analysis on the reviews and show how it can be integrated with the code

### Rationale

For simplicity, and since it's not part of the job requirements, the model was not trained with the dummy data. Instead an external model was used, Vader (Valence Aware Dictionary and Sentiment Reasoner) ((reference)[https://www.analyticsvidhya.com/blog/2022/07/sentiment-analysis-using-python/]). It was good to see the newly created class `PostgresHelper` being reused for this objective

### Refactoring

- Changed some method signatures in `PostgresHelper` so it can accomodate the new use case

## Create publish and test steps

### Reason

In data pipelines, it's often a good practice to test new data before releasing. This can be accomplished by creating two layers: data to be published and published data. Tests are performed in between them.

### Rationale

We can consider the pipeline built up until this point as to be published data. Therefore, I created a publish layer, populated with tables in the `publish` and DBT tests for PKs and FKs. Then, the DAG was updated to run these three steps in sequence: 1st layer transform, tests and 2nd layer transform. This way if any tests fail, we don't publish bad data.

### Refactoring

- Changed the way we build dbt tasks in the DAG Factory to make it more generic