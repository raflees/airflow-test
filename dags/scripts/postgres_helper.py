from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema

import scripts.constants as c

class PostgresHelper:
    def __init__(self):
        self.engine = self._create_pg_engine()

    def _create_pg_engine(self):
        # Create SQLAlchemy engine
        engine = create_engine(
            f'postgresql+psycopg2://{c.postgres_user}:{c.postgres_password}@{c.postgres_host}:{c.postgres_port}/{c.postgres_dbname}'
        )
        return engine

    def create_raw_schema_if_not_exists(self):
        with self.engine.connect() as conn:
            if not self.engine.dialect.has_schema(conn, "raw"):
                conn.execute(CreateSchema("raw"))

    def upload_overwrite_table(self, df, table_name):
        # Upload DataFrame to PostgreSQL
        df.to_sql(f'{table_name}',  self.engine, schema="raw", index=False, if_exists='replace')