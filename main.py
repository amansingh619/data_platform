import os
import sys
from pathlib import Path
import logging
sys.path.insert(0, str(Path(__file__).parent.parent))
from connectors.postgres_connector import PostgresConnector 
from connectors.config import ConfigLoader  
from data_processing import DataProcessing

def setup_logger(logfile: str = None):
    """Setting up logger """
    logger = logging.getLogger("DataExtractor")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()  # Clear any existing handlers

    # File handler
    if logfile:
        file_handler = logging.FileHandler(logfile)
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger

def main():
    logger = setup_logger()
    LOAD_ORDER = [
        "customers.csv",
        "transactions.csv",
        "accounts.csv",
        "risk_flags.csv"
    ]
    config = ConfigLoader()
    logger.info(f"Succesfull read the config file")

    data_dir = config.get('oracle')["data_path"]
    connector = PostgresConnector(config.get("postgres")["connection_string"])
    process_data = DataProcessing(data_dir, connector, logger)

    process_data.bronze_load(LOAD_ORDER)
    process_data.silver_transform()
    process_data.gold_risk()
    process_data.gold_analytics()


if __name__ == "__main__":
    main()