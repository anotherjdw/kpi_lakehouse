"""End-to-end test of the dim_dates job on local Spark.

The test runs the job's Glue entry point, run(), which parses its arguments from sys.argv,
reads the job's input rows from tests/fixtures/, which the test writes to disk as the job
reads them, and writes the output. The rows read back must be the job's reviewed output
rows: the expected rows of its last transformation.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from kpi_lakehouse.jobs.dim_dates import (
    dim_dates as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.dim_dates import expected, inputs
from tests.helpers import (
    SourceFixture,
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
    write_sources,
)

# How the job reads each source, and the input rows the test writes for it.
SOURCES = {
    "dates": SourceFixture(
        argument="DATES_INPUT_PATH",
        rows=inputs.DATES_ROWS,
        schema=schema_definitions.dates_schema,
        file_format="json",
        partition_key=None,
    ),
}

OUTPUT_SCHEMA = schema_definitions.dim_dates_schema


def test_run_end_to_end(
    spark: SparkSession, test_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """run() over the input rows writes exactly the job's reviewed output rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_dates")
    assert expected.CREATE_MONTH_END_COLUMN_REVIEWED, review_message(
        "dim_dates",
        "CREATE_MONTH_END_COLUMN",
    )
    output_path = str(test_dir / "output")
    argv = [
        "dim_dates.py",
        *write_sources(spark, SOURCES, test_dir / "sources"),
        "--OUTPUT_PATH",
        output_path,
        # No source is partitioned by date, so a run reads every input row, whatever the date.
    ]
    monkeypatch.setattr(sys, "argv", argv)
    job.run()
    df_actual = spark.read.parquet(output_path)
    df_expected = rows_to_df(spark, expected.CREATE_MONTH_END_COLUMN_ROWS, OUTPUT_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
