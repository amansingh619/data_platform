from dagster import asset

@asset(
    deps=["bronze_layer"],
    required_resource_keys={"data_processing_resource"}
)
def silver_layer(context):
    context.log.info("Starting Silver Transform")

    processor = context.resources.data_processing_resource
    processor.logger = context.log
    processor.silver_transform()

    context.log.info("Silver Transform Completed")