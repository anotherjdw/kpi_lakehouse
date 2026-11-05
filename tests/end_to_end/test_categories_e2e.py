"""End-to-end test of the categories job on local Spark.

The test runs the job's Glue entry point, run(), which parses its arguments from sys.argv,
reads the job's input rows from tests/fixtures/, which the test writes to disk as the job
reads them, and writes the output. The rows read back must be the job's reviewed output
rows: the expected rows of its last transformation.
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from kpi_lakehouse.jobs.categories import (
    categories as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.categories import expected, inputs
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
    "categories": SourceFixture(
        argument="CATEGORIES_INPUT_PATH",
        rows=inputs.CATEGORIES_ROWS,
        schema=schema_definitions.categories_raw_schema,
        file_format="json",
        partition_key=None,
    ),
}

OUTPUT_SCHEMA = schema_definitions.categories_schema


def test_run_end_to_end(
    spark: SparkSession, test_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """run() over the input rows writes exactly the job's reviewed output rows."""
    assert inputs.INPUTS_FILLED, inputs_message("categories")
    assert expected.FLATTEN_CATEGORY_TREE_REVIEWED, review_message(
        "categories",
        "FLATTEN_CATEGORY_TREE",
    )
    output_path = str(test_dir / "output")
    argv = [
        "categories.py",
        *write_sources(spark, SOURCES, test_dir / "sources"),
        "--OUTPUT_PATH",
        output_path,
        # No source is partitioned by date, so a run reads every input row, whatever the date.
    ]
    monkeypatch.setattr(sys, "argv", argv)
    job.run()
    df_actual = spark.read.parquet(output_path)
    df_expected = rows_to_df(spark, expected.FLATTEN_CATEGORY_TREE_ROWS, OUTPUT_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
