"""Dagster entry point:  dagster dev -m pipeline.definitions"""
import os

from dagster import AssetSelection, Definitions, build_schedule_from_partitioned_job, define_asset_job

from pipeline import assets as a
from pipeline.io import PolarsParquetIOManager

daily_job = define_asset_job("daily_verification", selection=AssetSelection.groups(
    "daily_inputs", "daily_analysis", "daily_reports", "publish"), partitions_def=a.DAILY)

defs = Definitions(
    assets=[a.lines, a.fleet_registry, a.traccar_devices, a.raw_positions, a.road_papers, a.route_fit, a.gps_trips,
            a.device_identity, a.trip_verification, a.km_report, a.bus_day_report, a.postgres_export],
    jobs=[daily_job],
    # 02:30 local time, for the day that just ended (papers are approved by about 21:00)
    schedules=[build_schedule_from_partitioned_job(daily_job, hour_of_day=2, minute_of_hour=30)],
    resources={"io_manager": PolarsParquetIOManager(base_dir=os.environ.get("FLEET_WAREHOUSE", str(a.DATA / "warehouse")))},
)
