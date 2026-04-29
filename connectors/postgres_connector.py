from sqlalchemy import create_engine, text
import pandas as pd
from connectors.base import BaseConnector


class PostgresConnector(BaseConnector):
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string)

    def read(self, query: str) -> pd.DataFrame:
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn)

    def write(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False,
    ):
        df.to_sql(
            table_name,
            self.engine,
            if_exists=if_exists,
            index=index,
            method="multi",
            chunksize=1000,
        )

    def execute(self, query: str):
        with self.engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()