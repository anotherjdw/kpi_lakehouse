"""Integration tests of the marketing_spend run steps on local Spark.

Each test writes the job's input rows from tests/fixtures/ to disk as the job reads them: in
each source's file format and partition layout. test_transform and test_write compare with
the job's output, the reviewed expected rows of its last transformation.
"""

from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from kpi_lakehouse.jobs.marketing_spend import (
    marketing_spend_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.marketing_spend import expected, inputs
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

# How the job reads each source, and the input rows the tests write for it.
SOURCES = {
    "marketing_spend": SourceFixture(
        argument="MARKETING_SPEND_INPUT_PATH",
        rows=inputs.MARKETING_SPEND_ROWS,
        schema=schema_definitions.marketing_spend_raw_schema,
        file_format="json",
        partition_key="spend_date",
    ),
    "fx_rates": SourceFixture(
        argument="FX_RATES_INPUT_PATH",
        rows=inputs.FX_RATES_ROWS,
        schema=schema_definitions.fx_rates_schema,
        file_format="parquet",
        partition_key="rate_date",
    ),
}

OUTPUT_SCHEMA = schema_definitions.marketing_spend_schema

# The output contract's non-nullable columns.
NOT_NULL_COLUMNS = [
    "spend_date",
    "platform",
    "campaign_id",
    "campaign_name",
    "impressions",
    "clicks",
    "spend_amount_eur",
    "currency_code",
    "reported_at",
]


@pytest.fixture
def output_table(spark: SparkSession, test_database: str, test_dir: Path) -> Iterator[str]:
    """A local table shaped like the job's Glue table, at the folder the test writes to.

    It starts with no partitions registered, like the Glue table before the job's first run.
    """
    table = f"{test_database}.marketing_spend"
    create_catalog_table(spark, table, OUTPUT_SCHEMA, "spend_date", test_dir)
    yield table
    # An external table: dropping it removes the catalog entry, not the files.
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_read(spark: SparkSession, test_dir: Path) -> None:
    """read() loads every input row of every source, in its contract's column types."""
    assert inputs.INPUTS_FILLED, inputs_message("marketing_spend")
    assert inputs.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
    args = job.parse_arguments(write_sources(spark, SOURCES, test_dir))
    sources = job.read(args, processing_dates(SOURCES))
    assert set(sources) == set(SOURCES)
    for alias, source in SOURCES.items():
        assert sources[alias].count() == len(source.rows), alias
        assert column_types(sources[alias].schema) == column_types(source.schema), alias


def test_read_no_data_raises(spark: SparkSession, test_dir: Path) -> None:
    """read() fails for dates with no input rows, since a source may not be empty."""
    assert inputs.INPUTS_FILLED, inputs_message("marketing_spend")
    assert inputs.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
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
    assert inputs.INPUTS_FILLED, inputs_message("marketing_spend")
    assert inputs.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
    assert expected.CONVERT_SPEND_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "marketing_spend",
        "CONVERT_SPEND_AMOUNT_TO_EUROS",
    )
    args = job.parse_arguments(write_sources(spark, SOURCES, test_dir))
    df_actual = job.transform(**job.read(args, processing_dates(SOURCES)))
    df_expected = rows_to_df(spark, expected.CONVERT_SPEND_AMOUNT_TO_EUROS_ROWS, OUTPUT_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_write(spark: SparkSession, test_dir: Path, output_table: str) -> None:
    """write() stores the reviewed output rows as parquet and registers them in the table."""
    assert inputs.INPUTS_FILLED, inputs_message("marketing_spend")
    assert inputs.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
    assert expected.CONVERT_SPEND_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "marketing_spend",
        "CONVERT_SPEND_AMOUNT_TO_EUROS",
    )
    output = rows_to_df(spark, expected.CONVERT_SPEND_AMOUNT_TO_EUROS_ROWS, OUTPUT_SCHEMA)
    job.write(output, str(test_dir), output_table)
    result = spark.read.parquet(str(test_dir))
    assert column_types(result.schema) == column_types(OUTPUT_SCHEMA)
    assert result.count() == len(expected.CONVERT_SPEND_AMOUNT_TO_EUROS_ROWS)
    for column in NOT_NULL_COLUMNS:
        assert result.filter(result[column].isNull()).isEmpty(), column
    # One folder per partition value.
    partition_key = "spend_date"
    folders = sorted(path.name for path in test_dir.iterdir() if path.is_dir())
    dates = fixture_dates(expected.CONVERT_SPEND_AMOUNT_TO_EUROS_ROWS, partition_key)
    assert folders == [f"{partition_key}={day}" for day in dates]
    # The table, which Athena reads, lists one partition per folder and holds every row.
    assert registered_partitions(spark, output_table) == folders
    assert spark.table(output_table).count() == result.count()


def test_write_incorrect_schema(spark: SparkSession, test_dir: Path, output_table: str) -> None:
    """write() refuses output whose column types differ from the output contract's."""
    column, other_type = "spend_date", "string"
    output = spark.createDataFrame([], OUTPUT_SCHEMA)
    output = output.withColumn(column, output[column].cast(other_type))
    with pytest.raises(ValueError, match="Schema validation failed"):
        job.write(output, str(test_dir), output_table)
    # A write that fails its checks registers nothing.
    assert registered_partitions(spark, output_table) == []
