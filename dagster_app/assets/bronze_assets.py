from dagster import asset

FILES = [
    "customers.csv",
    "transactions.csv",
    "accounts.csv",
    "risk_flags.csv"
]

@asset(required_resource_keys={"data_processing_resource"})
def bronze_layer(context):
    context.log.info("Starting Bronze Load")

    processor = context.resources.data_processing_resource
    processor.logger = context.log
    processor.bronze_load(FILES)

    context.log.info("Bronze Load Completed")