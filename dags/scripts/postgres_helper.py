import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.schema import CreateSchema

import scripts.constants as c

class PostgresHelper:
    def __init__(self):
        self.engine = self.create_pg_engine()
        self.inspector = Inspector(self.engine)

    def create_pg_engine(self) -> Engine:
        # Create SQLAlchemy engine
        engine = create_engine(
            f'postgresql+psycopg2://{c.postgres_user}:{c.postgres_password}@{c.postgres_host}:{c.postgres_port}/{c.postgres_dbname}'
        )
        # engine = create_engine(
        #     f'postgresql+psycopg2://{c.postgres_user}:{c.postgres_password}@localhost:5433/{c.postgres_dbname}'
        # )
        return engine

    def create_schema_if_not_exists(self, schema: str):
        if not self.schema_exists(schema):
            with self.engine.connect() as conn:
                conn.execute(CreateSchema(schema))
    
    def schema_exists(self, schema: str) -> bool:
        return schema in self.inspector.get_schema_names()
    
    def table_exists(self, schema: str, table_name: str) -> bool:
        return self.inspector.has_table(table_name, schema)

    def upload_table(self, df: pd.DataFrame, schema: str, table_name: str, method: str):
        # Upload DataFrame to PostgreSQL
        df.to_sql(f'{table_name}',  self.engine, schema, index=False, if_exists=method)