"""End-to-end test of the dim_product job on local Spark.

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

from kpi_lakehouse.jobs.dim_product import (
    dim_product as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.dim_product import expected, inputs
from tests.helpers import (
    SourceFixture,
    column_types,
    create_catalog_table,
    fixture_dates,
    processing_dates,
    registered_partitions,
    review_message,
    row_key,
    rows_to_df,
    write_sources,
)

# How the job reads each source, and the input rows the test writes for it.
SOURCES = {
    "products": SourceFixture(
        argument="PRODUCTS_INPUT_PATH",
        rows=inputs.PRODUCTS_ROWS,
        schema=schema_definitions.products_schema,
        file_format="parquet",
        partition_key="snapshot_date",
    ),
    "categories": SourceFixture(
        argument="CATEGORIES_INPUT_PATH",
        rows=inputs.CATEGORIES_ROWS,
        schema=schema_definitions.categories_schema,
        file_format="parquet",
        partition_key="snapshot_date",
    ),
}

OUTPUT_SCHEMA = schema_definitions.dim_product_schema


@pytest.fixture
def output_table(spark: SparkSession, test_database: str, test_dir: Path) -> Iterator[str]:
    """A local table shaped like the job's Glue table, at the folder the run writes to.

    It starts with no partitions registered, like the Glue table before the job's first run.
    """
    table = f"{test_database}.dim_product"
    location = test_dir / "output"
    create_catalog_table(spark, table, OUTPUT_SCHEMA, "valid_from", location)
    yield table
    # An external table: dropping it removes the catalog entry, not the files.
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_run_end_to_end(
    spark: SparkSession, test_dir: Path, monkeypatch: pytest.MonkeyPatch, output_table: str
) -> None:
    """run() writes exactly the job's reviewed output rows and registers them in the table."""
    assert inputs.FLATTEN_CATEGORY_TREE_REVIEWED, review_message(
        "categories",
        "FLATTEN_CATEGORY_TREE",
    )
    assert inputs.CREATE_PRODUCT_VERSION_ID_REVIEWED, review_message(
        "products",
        "CREATE_PRODUCT_VERSION_ID",
    )
    assert expected.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    dates = processing_dates(SOURCES)
    output_path = str(test_dir / "output")
    argv = [
        "dim_product.py",
        *write_sources(spark, SOURCES, test_dir / "sources"),
        "--OUTPUT_PATH",
        output_path,
        "--OUTPUT_TABLE",
        output_table,
        # Each partitioned source reads the days its input rows fall on.
        "--PROCESSING_TYPE",
        "backfill",
        "--PRODUCTS_START_DATE",
        dates["products"][0],
        "--PRODUCTS_END_DATE",
        dates["products"][-1],
        "--CATEGORIES_START_DATE",
        dates["categories"][0],
        "--CATEGORIES_END_DATE",
        dates["categories"][-1],
    ]
    monkeypatch.setattr(sys, "argv", argv)
    job.run()
    df_actual = spark.read.parquet(output_path)
    df_expected = rows_to_df(spark, expected.JOIN_CATEGORY_PATH_ROWS, OUTPUT_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
    # The run registered one partition per date it wrote, so the table, which Athena reads,
    # holds the same rows as the files.
    partition_key = "valid_from"
    output_dates = fixture_dates(expected.JOIN_CATEGORY_PATH_ROWS, partition_key)
    partitions = [f"{partition_key}={day}" for day in output_dates]
    assert registered_partitions(spark, output_table) == partitions
    df_table = spark.table(output_table)
    assert column_types(df_table.schema) == column_types(df_expected.schema)
    from_table = df_table.select(columns).collect()
    assert sorted(from_table, key=row_key) == sorted(expected_rows, key=row_key)
