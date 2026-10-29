"""Integration tests of the fact_session run steps on local Spark.

Each test writes the job's input rows from tests/fixtures/ to disk as the job reads them: in
each source's file format and partition layout. test_transform and test_write compare with
the job's output, the reviewed expected rows of its last transformation.
"""

from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from kpi_lakehouse.jobs.fact_session import (
    fact_session_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.fact_session import expected, inputs
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

# How the job reads each source, and the input rows the tests write for it.
SOURCES = {
    "sessions": SourceFixture(
        argument="SESSIONS_INPUT_PATH",
        rows=inputs.SESSIONS_ROWS,
        schema=schema_definitions.sessions_schema,
        file_format="parquet",
        partition_key="event_date",
    ),
    "dim_customer": SourceFixture(
        argument="DIM_CUSTOMER_INPUT_PATH",
        rows=inputs.DIM_CUSTOMER_ROWS,
        schema=schema_definitions.dim_customer_schema,
        file_format="parquet",
        partition_key="valid_from",
    ),
}

OUTPUT_SCHEMA = schema_definitions.fact_session_schema

# The output contract's non-nullable columns.
NOT_NULL_COLUMNS = [
    "session_id",
    "event_date",
    "first_event_time",
    "last_event_type",
    "date_key",
    "converted_customer",
]


@pytest.fixture
def output_table(spark: SparkSession, test_database: str, test_dir: Path) -> Iterator[str]:
    """A local table shaped like the job's Glue table, at the folder the test writes to.

    It starts with no partitions registered, like the Glue table before the job's first run.
    """
    table = f"{test_database}.fact_session"
    create_catalog_table(spark, table, OUTPUT_SCHEMA, "event_date", test_dir)
    yield table
    # An external table: dropping it removes the catalog entry, not the files.
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_read(spark: SparkSession, test_dir: Path) -> None:
    """read() loads every input row of every source, in its contract's column types."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    args = job.parse_arguments(write_sources(spark, SOURCES, test_dir))
    sources = job.read(args, processing_dates(SOURCES))
    assert set(sources) == set(SOURCES)
    for alias, source in SOURCES.items():
        assert sources[alias].count() == len(source.rows), alias
        assert column_types(sources[alias].schema) == column_types(source.schema), alias


def test_read_no_data_raises(spark: SparkSession, test_dir: Path) -> None:
    """read() fails for dates with no input rows, since a source may not be empty."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    args = job.parse_arguments(write_sources(spark, SOURCES, test_dir))
    # Each partitioned source reads the day before its first input row.
    days_before = {
        alias: [(date.fromisoformat(dates[0]) - timedelta(days=1)).isoformat()]
        for alias, dates in processing_dates(SOURCES).items()
    }
    with pytest.raises(ValueError, match="read no rows"):
        job.read(args, days_before)


def test_transform(spark: SparkSession, test_dir: Path) -> None:
    """transform() over the sources read returns the job's reviewed output rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.RENAME_MARKETING_CHANNEL_REVIEWED, review_message(
        "fact_session",
        "RENAME_MARKETING_CHANNEL",
    )
    args = job.parse_arguments(write_sources(spark, SOURCES, test_dir))
    df_actual = job.transform(**job.read(args, processing_dates(SOURCES)))
    df_expected = rows_to_df(spark, expected.RENAME_MARKETING_CHANNEL_ROWS, OUTPUT_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_write(spark: SparkSession, test_dir: Path, output_table: str) -> None:
    """write() stores the reviewed output rows as parquet and registers them in the table."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.RENAME_MARKETING_CHANNEL_REVIEWED, review_message(
        "fact_session",
        "RENAME_MARKETING_CHANNEL",
    )
    output = rows_to_df(spark, expected.RENAME_MARKETING_CHANNEL_ROWS, OUTPUT_SCHEMA)
    job.write(output, str(test_dir), output_table)
    result = spark.read.parquet(str(test_dir))
    assert column_types(result.schema) == column_types(OUTPUT_SCHEMA)
    assert result.count() == len(expected.RENAME_MARKETING_CHANNEL_ROWS)
    for column in NOT_NULL_COLUMNS:
        assert result.filter(result[column].isNull()).isEmpty(), column
    # One folder per partition value.
    partition_key = "event_date"
    folders = sorted(path.name for path in test_dir.iterdir() if path.is_dir())
    dates = fixture_dates(expected.RENAME_MARKETING_CHANNEL_ROWS, partition_key)
    assert folders == [f"{partition_key}={day}" for day in dates]
    # The table, which Athena reads, lists one partition per folder and holds every row.
    assert registered_partitions(spark, output_table) == folders
    assert spark.table(output_table).count() == result.count()


def test_write_incorrect_schema(spark: SparkSession, test_dir: Path, output_table: str) -> None:
    """write() refuses output whose column types differ from the output contract's."""
    column, other_type = "session_id", "int"
    output = spark.createDataFrame([], OUTPUT_SCHEMA)
    output = output.withColumn(column, output[column].cast(other_type))
    with pytest.raises(ValueError, match="Schema validation failed"):
        job.write(output, str(test_dir), output_table)
    # A write that fails its checks registers nothing.
    assert registered_partitions(spark, output_table) == []
