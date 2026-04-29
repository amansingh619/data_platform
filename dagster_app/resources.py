from dagster import resource
from connectors.postgres_connector import PostgresConnector
from connectors.config import ConfigLoader
from data_processing import DataProcessing


@resource
def data_processing_resource():
    config = ConfigLoader()

    pg = PostgresConnector(
        config.get("postgres")["connection_string"]
    )

    processor = DataProcessing(
        data_dir=config.get("oracle")["data_path"],
        postgres_connector=pg,
        logger=None  # Dagster handles logs
    )

    return processor