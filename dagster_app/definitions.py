from dagster import Definitions

from dagster_app.assets.bronze_assets import bronze_layer
from dagster_app.assets.silver_assets import silver_layer
from dagster_app.assets.gold_assets import (
    gold_risk_asset,
    gold_analytics_asset
)

from dagster_app.resources import data_processing_resource


defs = Definitions(
    assets=[
        bronze_layer,
        silver_layer,
        gold_risk_asset,
        gold_analytics_asset,
    ],
    resources={
        "data_processing_resource": data_processing_resource
    }
)