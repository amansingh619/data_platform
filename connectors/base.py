from abc import ABC, abstractmethod
import pandas as pd


class BaseConnector(ABC):
    """
    Abstract base class for all connectors
    to make sure we support only rwad & write operations.
    """

    @abstractmethod
    def read(self, *args, **kwargs) -> pd.DataFrame:
        pass

    @abstractmethod
    def write(self, df: pd.DataFrame, *args, **kwargs):
        pass