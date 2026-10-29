"""Glue entry point of the dim_dates ETL job.

Scaffold-owned: `generate create-job` renders it from the job spec, and it is uploaded
unchanged as the Glue job's script.
"""

from kpi_lakehouse.jobs.dim_dates import (
    dim_dates_transformations as job,
)
from kpi_lakehouse.runtime.job_settings import job_run, phase


def run() -> None:
    """Parse arguments and resolve each source's dates, then read, transform and write once."""
    args = job.parse_arguments()
    # No source is partitioned by date, so every source is read whole.
    processing_dates: dict[str, list[str]] = {}
    with job_run("dim_dates", args, processing_dates) as record:
        with phase(record, "read"):
            sources = job.read(args, processing_dates)
        with phase(record, "transform"):
            output = job.transform(**sources)
        with phase(record, "write"):
            job.write(output, args.OUTPUT_PATH)


if __name__ == "__main__":
    run()
