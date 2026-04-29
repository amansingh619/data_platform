import pandas as pd
from connectors.base import BaseConnector


class OracleConnector(BaseConnector):
    """
    Using the csv file to mimic the loading of data 
    from Oracle DB.
    """

    def __init__(self, data_path: str):
        self.data_path = data_path

    def read(self, query_name: str) -> pd.DataFrame:
        """
        fetching query results.
        """
        file_path = f"{self.data_path}/{query_name}.csv"
        df = pd.read_csv(file_path)
        return df

    def write(self, df, *args, **kwargs):
        raise NotImplementedError("Oracle write not supported in this use case")