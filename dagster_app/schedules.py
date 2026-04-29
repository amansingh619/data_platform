from dagster import ScheduleDefinition
from definitions import defs


daily_schedule = ScheduleDefinition(
    job=defs.get_implicit_job_def_for_assets(
        [
            "bronze_layer",
            "silver_layer",
            "gold_risk_asset",
            "gold_analytics_asset",
        ]
    ),
    cron_schedule="0 5 * * *",  # daily at 5 AM in morning
)