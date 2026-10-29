"""Glue entry point of the dim_customer ETL job.

Scaffold-owned: `generate create-job` renders it from the job spec, and it is uploaded
unchanged as the Glue job's script.
"""

from kpi_lakehouse.jobs.dim_customer import (
    dim_customer_transformations as job,
)
from kpi_lakehouse.runtime.job_settings import job_run, phase
from kpi_lakehouse.utils.python import resolve_processing_dates


def run() -> None:
    """Parse arguments and resolve each source's dates, then read, transform and write once."""
    args = job.parse_arguments()
    processing_dates: dict[str, list[str]] = {
        "customers": resolve_processing_dates(
            processing_type=args.PROCESSING_TYPE,
            lookback_days=args.CUSTOMERS_LOOKBACK_DAYS,
            start_date=args.CUSTOMERS_START_DATE,
            end_date=args.CUSTOMERS_END_DATE,
        ),
    }
    with job_run("dim_customer", args, processing_dates) as record:
        with phase(record, "read"):
            sources = job.read(args, processing_dates)
        with phase(record, "transform"):
            output = job.transform(**sources)
        with phase(record, "write"):
            job.write(output, args.OUTPUT_PATH, args.OUTPUT_TABLE)


if __name__ == "__main__":
    run()
