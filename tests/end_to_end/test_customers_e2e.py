"""End-to-end test of the customers job on local Spark.

The test runs the job's Glue entry point, run(), which parses its arguments from sys.argv,
reads the job's input rows from tests/fixtures/, which the test writes to disk as the job
reads them, and writes the output. The rows read back must be the job's reviewed output
rows: the expected rows of its last transformation.

The run also registers the dates it wrote in a local table standing in for the job's Glue
table, which must then hold the same rows.
"""

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from kpi_lakehouse.jobs.customers import (
    customers as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.customers import expected, inputs
from tests.helpers import (
    SourceFixture,
    column_types,
    create_catalog_table,
    fixture_dates,
    inputs_message,
    processing_dates,
    registered_partitions,
    review_message,
    row_key,
    rows_to_df,
    write_sources,
)

# How the job reads each source, and the input rows the test writes for it.
SOURCES = {
    "customers": SourceFixture(
        argument="CUSTOMERS_INPUT_PATH",
        rows=inputs.CUSTOMERS_ROWS,
        schema=schema_definitions.customers_raw_schema,
        file_format="json",
        partition_key="snapshot_date",
    ),
}

OUTPUT_SCHEMA = schema_definitions.customers_schema


@pytest.fixture
def output_table(spark: SparkSession, test_database: str, test_dir: Path) -> Iterator[str]:
    """A local table shaped like the job's Glue table, at the folder the run writes to.

    It starts with no partitions registered, like the Glue table before the job's first run.
    """
    table = f"{test_database}.customers"
    location = test_dir / "output"
    create_catalog_table(spark, table, OUTPUT_SCHEMA, "snapshot_date", location)
    yield table
    # An external table: dropping it removes the catalog entry, not the files.
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_run_end_to_end(
    spark: SparkSession, test_dir: Path, monkeypatch: pytest.MonkeyPatch, output_table: str
) -> None:
    """run() writes exactly the job's reviewed output rows and registers them in the table."""
    assert inputs.INPUTS_FILLED, inputs_message("customers")
    assert expected.REMOVE_DELETED_CUSTOMER_RECORDS_REVIEWED, review_message(
        "customers",
        "REMOVE_DELETED_CUSTOMER_RECORDS",
    )
    dates = processing_dates(SOURCES)
    output_path = str(test_dir / "output")
    argv = [
        "customers.py",
        *write_sources(spark, SOURCES, test_dir / "sources"),
        "--OUTPUT_PATH",
        output_path,
        "--OUTPUT_TABLE",
        output_table,
        # Each partitioned source reads the days its input rows fall on.
        "--PROCESSING_TYPE",
        "backfill",
        "--CUSTOMERS_START_DATE",
        dates["customers"][0],
        "--CUSTOMERS_END_DATE",
        dates["customers"][-1],
    ]
    monkeypatch.setattr(sys, "argv", argv)
    job.run()
    df_actual = spark.read.parquet(output_path)
    df_expected = rows_to_df(spark, expected.REMOVE_DELETED_CUSTOMER_RECORDS_ROWS, OUTPUT_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
    # The run registered one partition per date it wrote, so the table, which Athena reads,
    # holds the same rows as the files.
    partition_key = "snapshot_date"
    output_dates = fixture_dates(expected.REMOVE_DELETED_CUSTOMER_RECORDS_ROWS, partition_key)
    partitions = [f"{partition_key}={day}" for day in output_dates]
    assert registered_partitions(spark, output_table) == partitions
    df_table = spark.table(output_table)
    assert column_types(df_table.schema) == column_types(df_expected.schema)
    from_table = df_table.select(columns).collect()
    assert sorted(from_table, key=row_key) == sorted(expected_rows, key=row_key)
