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

## Step 2: Changing the loaded schema to `raw` (PR #3)

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

## Create publish and test steps (PR #7)

### Reason

In data pipelines, it's often a good practice to test new data before releasing. This can be accomplished by creating two layers: data to be published and published data. Tests are performed in between them.

### Rationale

We can consider the pipeline built up until this point as to be published data. Therefore, I created a publish layer, populated with tables in the `publish` and DBT tests for PKs and FKs. Then, the DAG was updated to run these three steps in sequence: 1st layer transform, tests and 2nd layer transform. This way if any tests fail, we don't publish bad data.

### Refactoring

- Changed the way we build dbt tasks in the DAG Factory to make it more generic

## Make Sentiment Analysis incremental (PR #8)

### Reason

For cost and time savings it better to follow incremental approaches whenever possible. The Sentiment Analysis was a full replace process, so I turned it into an incremental one.

### Rationale

Two things were changed:
1. Retrive not analyzed reviews from the table `transform.customer_reviews_google`
2. Upload analyzed data with an `append` method

### Refactoring

- Change the upload method of `PostgresHelper` to accept a `method` parameter, which should be `append` or `replace`
- Encapsulate the Sentiment Analysis logic in a class
- Refactored the inspecting functions from `PostgresHelper` to use the class `sqlalchemy.engine.reflection.Inspector`

## Create company rankings (PR #9)

### Reason

Data Analysts need to be able to rank companies

### Rationale

Ranks were created globally (using the whole datasets). If necessary, one can apply filters and order results by the global ranking.

- Ranked FMCSA companies by the number of complaints, being the 1st companies the one with the least complaints. The ranking was yearly, so I created a `is_latest_ranking` fields which is `TRUE` only for the latest year (in this case, 2023)
    - There are discrepancies between the number of complaints in `fmcsa_companies` and `fmcsa_complaints`. Like only centain complaint categories are account for in the former. For the ranking, I've used `fmcsa_complaints` as the number is always equal or higher.
    - Given the discrepancy, I've removed complaint count fields from `fmcsa_companies`, to not cause confusion
    - So to not pollute the table, only the ranking from the latest year is published.
- Companies in the Google dataset had more information that could be ranked:
    - Most posts
    - Most reviews
    - Most positive reviews percentage (per sentiment analysis)
    - Most negative reviews percentage (per sentiment analysis)
    - Best rated

### Refactoring

- Sentiment Analysis added a field, `strongest_sentiment`, to table as to aid in ranking most negative and positve reviews

## Prepare system to be reproductible (PR #10)

### Reason

In order others could spin up the environment and successfully run DAGs, some adjustments had to be made

### Rationale

I notice I had created a cyclic dependency between the transform/ranking logic and the Sentiment Analysis DAG. The SA process would pull from the `tranform` schema and write to the `analysis` schema. The ranking models would read from both, but they were part of `transform` themselves. One of two actions were required:

1. Make SA read from `raw`, instead of transform
2. Split the ranking models from the mais transform, so they could be run separately, after SA

I did both. I proceeded to drop all database schemas and re-run the DAG. It passed.

## Future improvements
- Add unit tests to the DAG scripts
- Add metadata fields to the tables. An ingest / transform timestamp would enable incremental pulls from the data analysts
- Add observability to the pipeline
    - Row count metrics
    - Query checks
- Employ other Sentiment Analyses techniques