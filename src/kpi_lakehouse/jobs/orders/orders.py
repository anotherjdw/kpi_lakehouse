"""Glue entry point of the orders ETL job.

Scaffold-owned: `generate create-job` renders it from the job spec, and it is uploaded
unchanged as the Glue job's script.
"""

from kpi_lakehouse.jobs.orders import (
    orders_transformations as job,
)
from kpi_lakehouse.runtime.job_settings import job_run, phase
from kpi_lakehouse.utils.python import resolve_processing_dates


def run() -> None:
    """Parse arguments and resolve each source's dates, then read, transform and write once."""
    args = job.parse_arguments()
    processing_dates: dict[str, list[str]] = {
        "orders": resolve_processing_dates(
            processing_type=args.PROCESSING_TYPE,
            lookback_days=args.ORDERS_LOOKBACK_DAYS,
            start_date=args.ORDERS_START_DATE,
            end_date=args.ORDERS_END_DATE,
        ),
        "fx_rates": resolve_processing_dates(
            processing_type=args.PROCESSING_TYPE,
            lookback_days=args.FX_RATES_LOOKBACK_DAYS,
            start_date=args.FX_RATES_START_DATE,
            end_date=args.FX_RATES_END_DATE,
        ),
    }
    with job_run("orders", args, processing_dates) as record:
        with phase(record, "read"):
            sources = job.read(args, processing_dates)
        with phase(record, "transform"):
            output = job.transform(**sources)
        with phase(record, "write"):
            job.write(output, args.OUTPUT_PATH, args.OUTPUT_TABLE)


if __name__ == "__main__":
    run()
