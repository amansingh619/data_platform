from dagster import asset

@asset(
    deps=["silver_layer"],
    required_resource_keys={"data_processing_resource"}
)
def gold_risk_asset(context):
    processor = context.resources.data_processing_resource
    processor.logger = context.log
    processor.gold_risk()


@asset(
    deps=["silver_layer"],
    required_resource_keys={"data_processing_resource"}
)
def gold_analytics_asset(context):
    processor = context.resources.data_processing_resource
    processor.gold_analytics()