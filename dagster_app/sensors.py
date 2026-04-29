import os
from dagster import sensor, RunRequest
from pathlib import Path


@sensor(job_name="__ASSET_JOB")
def file_sensor(context):

    data_dir = Path("data")

    for file in data_dir.glob("*.csv"):
        modified_time = os.path.getmtime(file)

        last_run = context.cursor or "0"

        if str(modified_time) > last_run:
            context.update_cursor(str(modified_time))

            yield RunRequest(
                run_key=str(modified_time),
                run_config={}
            )