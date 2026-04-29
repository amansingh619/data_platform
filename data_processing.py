import os
from datetime import datetime
from typing import Dict
import pandas as pd

from utils import generate_row_hash


class DataProcessing:
    def __init__(self, data_dir, postgres_connector, logger):
        self.data_dir = data_dir
        self.pg = postgres_connector
        self.logger = logger

        # mapping of bronz table & it's primary key
        self.table_config: Dict = {
            "customers.csv": {
                "table": "bronze_customers",
                "pk": "customer_id",
            },
            "transactions.csv": {
                "table": "bronze_transactions",
                "pk": "txn_id",
            },
            "accounts.csv": {
                "table": "bronze_accounts",
                "pk": "customer_id",
            },
            "risk_flags.csv": {
                "table": "bronze_risk_flags",
                "pk": "customer_id",
            },
        }

    def bronze_load(self, file_list):
        for file in file_list:

            if file not in self.table_config:
                self.logger.warning(f"Skipping unknown file: {file}")
                continue

            config = self.table_config[file]
            table_name = config["table"]
            pk_column = config["pk"]

            try:
                file_path = os.path.join(self.data_dir, file)
                self.logger.info(f"Processing file: {file_path}")

                df = pd.read_csv(file_path)

                if df.empty:
                    self.logger.warning(f"{file}: No data found")
                    continue

                # ceating this columns -> hashing + timestamp
                df["row_hash"] = df.apply(generate_row_hash, axis=1)
                df["ingestion_ts"] = datetime.utcnow()

                # Getting relevant IDs only
                ids = df[pk_column].dropna().unique().tolist()

                if not ids:
                    self.logger.warning(f"{file}: No valid IDs found")
                    continue

                # SAFE QUERY 
                id_list = ",".join(map(str, ids))

                query = f"""
                    SELECT {pk_column}, row_hash
                    FROM {table_name}
                    WHERE {pk_column} IN ({id_list})
                """

                try:
                    existing_df = self.pg.read(query)
                    existing_set = set(
                        zip(existing_df[pk_column], existing_df["row_hash"])
                    )
                except Exception as e:
                    self.logger.warning(f"{table_name}: No existing data ({e})")
                    existing_set = set()

                # Filter new/changed
                df["composite_key"] = list(zip(df[pk_column], df["row_hash"]))
                new_df = df[~df["composite_key"].isin(existing_set)].drop(
                    columns=["composite_key"]
                )

                if not new_df.empty:
                    self.pg.write(new_df, table=table_name)
                    self.logger.info(f"{table_name}: Inserted {len(new_df)} rows")
                else:
                    self.logger.info(f"{table_name}: No new records")

            except Exception as e:
                self.logger.error(f"Error processing {file}: {str(e)}", exc_info=True)


    def silver_transform(self):

        for file, config in self.table_config.items():
            table_name = config["table"].replace("bronze_", "")
            pk_column = config["pk"]

            try:
                self.logger.info(f"Running silver transform for {table_name}")

                query = f"""
                SELECT *
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY {pk_column}
                               ORDER BY ingestion_ts DESC
                           ) as rn
                    FROM bronze_{table_name}
                ) t
                WHERE rn = 1
                """

                df = self.pg.read(query)

                if df.empty:
                    self.logger.warning(f"{table_name}: No bronze data")
                    continue

                # Data quality
                df = df.drop_duplicates(subset=[pk_column])
                df = df.dropna(subset=[pk_column])

                if "email" in df.columns:
                    df["email"] = df["email"].str.lower().str.strip()

                df["updated_at"] = datetime.utcnow()

                self.pg.write(
                    df.drop(columns=["rn"]),
                    table=f"silver_{table_name}",
                    mode="replace",
                )

                self.logger.info(f"silver_{table_name}: Updated")

            except Exception as e:
                self.logger.error(f"Silver error for {table_name}: {e}", exc_info=True)


    def gold_risk(self):
        try:
            self.logger.info("Running gold_risk")

            query = """
            SELECT customer_id, txn_date, amount
            FROM silver_transactions
            WHERE status = 'SUCCESS'
            """

            df = self.pg.read(query)

            if df.empty:
                self.logger.warning("gold_risk: No data")
                return

            agg = (
                df.groupby("customer_id")
                .agg(
                    last_txn=("txn_date", "max"),
                    frequency=("customer_id", "count"),
                    monetary=("amount", "sum"),
                )
                .reset_index()
            )

            agg["recency"] = (
                datetime.now() - pd.to_datetime(agg["last_txn"])
            ).dt.days

            final_df = agg[
                ["customer_id", "recency", "frequency", "monetary"]
            ]

            self.pg.write(final_df, table="gold_user_risk", mode="replace")

            self.logger.info("gold_user_risk: Updated")

        except Exception as e:
            self.logger.error(f"gold_risk error: {e}", exc_info=True)

    def gold_analytics(self):
        try:
            self.logger.info("Running gold_analytics")

            query = """
            SELECT 
                DATE_TRUNC('month', txn_date) AS month,
                COUNT(*) AS total_txns,
                SUM(amount) AS total_amount
            FROM silver_transactions
            WHERE status = 'SUCCESS'
            GROUP BY 1
            """

            df = self.pg.read(query)

            if df.empty:
                self.logger.warning("gold_analytics: No data")
                return

            self.pg.write(df, table="gold_monthly_metrics", mode="replace")

            self.logger.info("gold_monthly_metrics: Updated")

        except Exception as e:
            self.logger.error(f"gold_analytics error: {e}", exc_info=True)