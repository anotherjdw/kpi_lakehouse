"""Glue entry point of the fact_order_line ETL job.

Scaffold-owned: `generate create-job` renders it from the job spec, and it is uploaded
unchanged as the Glue job's script.
"""

from kpi_lakehouse.jobs.fact_order_line import (
    fact_order_line_transformations as job,
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
        "order_items": resolve_processing_dates(
            processing_type=args.PROCESSING_TYPE,
            lookback_days=args.ORDER_ITEMS_LOOKBACK_DAYS,
            start_date=args.ORDER_ITEMS_START_DATE,
            end_date=args.ORDER_ITEMS_END_DATE,
        ),
        "returns": resolve_processing_dates(
            processing_type=args.PROCESSING_TYPE,
            lookback_days=args.RETURNS_LOOKBACK_DAYS,
            start_date=args.RETURNS_START_DATE,
            end_date=args.RETURNS_END_DATE,
        ),
        "dim_customer": resolve_processing_dates(
            processing_type=args.PROCESSING_TYPE,
            lookback_days=args.DIM_CUSTOMER_LOOKBACK_DAYS,
            start_date=args.DIM_CUSTOMER_START_DATE,
            end_date=args.DIM_CUSTOMER_END_DATE,
        ),
        "dim_product": resolve_processing_dates(
            processing_type=args.PROCESSING_TYPE,
            lookback_days=args.DIM_PRODUCT_LOOKBACK_DAYS,
            start_date=args.DIM_PRODUCT_START_DATE,
            end_date=args.DIM_PRODUCT_END_DATE,
        ),
        "sessions": resolve_processing_dates(
            processing_type=args.PROCESSING_TYPE,
            lookback_days=args.SESSIONS_LOOKBACK_DAYS,
            start_date=args.SESSIONS_START_DATE,
            end_date=args.SESSIONS_END_DATE,
        ),
    }
    with job_run("fact_order_line", args, processing_dates) as record:
        with phase(record, "read"):
            sources = job.read(args, processing_dates)
        with phase(record, "transform"):
            output = job.transform(**sources)
        with phase(record, "write"):
            job.write(output, args.OUTPUT_PATH, args.OUTPUT_TABLE)


if __name__ == "__main__":
    run()
