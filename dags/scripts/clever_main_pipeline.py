import pandas as pd

from scripts.postgres_helper import PostgresHelper

def upload_to_postgres(**kwargs):
    file_name=kwargs.get('file_name')
    table_name = file_name.split('.')[0]
    
    raw_df = pd.read_csv(
        f'dags/scripts/data_examples/{file_name}',
        escapechar="\\",
    )

    helper = PostgresHelper()
    helper.create_schema_if_not_exists("raw")
    helper.upload_append_table(raw_df, table_name)
