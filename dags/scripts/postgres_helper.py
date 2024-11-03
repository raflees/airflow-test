from sqlalchemy import create_engine

import scripts.constants as c

def create_pg_engine():
    # Create SQLAlchemy engine
    engine = create_engine(
        f'postgresql+psycopg2://{c.postgres_user}:{c.postgres_password}@{c.postgres_host}:{c.postgres_port}/{c.postgres_dbname}'
    )
    return engine

def upload_overwrite_table(df, table_name):
    # Upload DataFrame to PostgreSQL
    engine = create_pg_engine()
    df.to_sql(f'{table_name}', engine, index=False, if_exists='replace')