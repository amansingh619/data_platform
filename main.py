import os
import sys
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(__file__).parent.parent))
from connectors.postgres_connector import PostgresConnector 
from connectors.config import ConfigLoader  


def get_table_name(file_name: str) -> str:
    """
    Maps CSV file name to table name.
    Example: users.csv -> users
    """
    return file_name.replace(".csv", "")


def main():
    LOAD_ORDER = [
        "users.csv",
        "products.csv",
        "orders.csv",
        "order_items.csv"
    ]
    config = ConfigLoader()
    data_dir = config.get('oracle')["data_path"]

    # Initialize connector
    connector = PostgresConnector(config.get("postgres")["connection_string"])

    # Loop through all CSV files in data directory
    for file in LOAD_ORDER:
        if file.endswith(".csv"):
            file_path = os.path.join(data_dir, file)

            print(f"Processing file: {file}")

            # Read CSV
            df = pd.read_csv(file_path)
            if df.empty:
                continue

            # Get table name
            table_name = get_table_name(file)

            # Write to DB
            connector.write(
                df,
                table=table_name,
                mode="append"  # or "replace" for fresh load
            )

            print(f"Loaded {file} into table {table_name}\n")


if __name__ == "__main__":
    main()