"""Integration tests of the orders run steps on local Spark.

Each test writes the job's input rows from tests/fixtures/ to disk as the job reads them: in
each source's file format and partition layout. test_transform and test_write compare with
the job's output, the reviewed expected rows of its last transformation.
"""

import json
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path

import pytest
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession

from kpi_lakehouse.jobs.orders import (
    orders_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from kpi_lakehouse.utils.spark import spark_session
from tests.fixtures.orders import expected, inputs
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
    "orders": SourceFixture(
        argument="ORDERS_INPUT_PATH",
        rows=inputs.ORDERS_ROWS,
        schema=schema_definitions.orders_raw_schema,
        file_format="json",
        partition_key="export_date",
    ),
    "fx_rates": SourceFixture(
        argument="FX_RATES_INPUT_PATH",
        rows=inputs.FX_RATES_ROWS,
        schema=schema_definitions.fx_rates_schema,
        file_format="parquet",
        partition_key="rate_date",
    ),
}

OUTPUT_SCHEMA = schema_definitions.orders_schema

# The output contract's non-nullable columns.
NOT_NULL_COLUMNS = [
    "order_date",
    "export_date",
    "order_id",
    "customer_id",
    "order_ts",
    "order_status",
    "updated_at",
    "currency_code",
    "payment_method",
    "shipping_country",
    "rate_date",
    "base_currency",
    "quote_currency",
    "rate",
    "shipping_fee_eur",
]


@pytest.fixture
def output_table(spark: SparkSession, test_database: str, test_dir: Path) -> Iterator[str]:
    """A local table shaped like the job's Glue table, at the folder the test writes to.

    It starts with no partitions registered, like the Glue table before the job's first run.
    """
    table = f"{test_database}.orders"
    create_catalog_table(spark, table, OUTPUT_SCHEMA, "order_date", test_dir)
    yield table
    # An external table: dropping it removes the catalog entry, not the files.
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_read(spark: SparkSession, test_dir: Path) -> None:
    """read() loads every input row of every source, in its contract's column types."""
    assert inputs.INPUTS_FILLED, inputs_message("orders")
    assert inputs.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
    args = job.parse_arguments(write_sources(spark, SOURCES, test_dir))
    sources = job.read(args, processing_dates(SOURCES))
    assert set(sources) == set(SOURCES)
    for alias, source in SOURCES.items():
        assert sources[alias].count() == len(source.rows), alias
        assert column_types(sources[alias].schema) == column_types(source.schema), alias


def test_read_no_data_raises(spark: SparkSession, test_dir: Path) -> None:
    """read() fails for dates with no input rows, since a source may not be empty."""
    assert inputs.INPUTS_FILLED, inputs_message("orders")
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
    assert inputs.INPUTS_FILLED, inputs_message("orders")
    assert inputs.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
    assert expected.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    args = job.parse_arguments(write_sources(spark, SOURCES, test_dir))
    df_actual = job.transform(**job.read(args, processing_dates(SOURCES)))
    df_expected = rows_to_df(spark, expected.CONVERT_SHIPPING_FEE_ROWS, OUTPUT_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_write(spark: SparkSession, test_dir: Path, output_table: str) -> None:
    """write() stores the reviewed output rows as parquet and registers them in the table."""
    assert inputs.INPUTS_FILLED, inputs_message("orders")
    assert inputs.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
    assert expected.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    output = rows_to_df(spark, expected.CONVERT_SHIPPING_FEE_ROWS, OUTPUT_SCHEMA)
    job.write(output, str(test_dir), output_table)
    result = spark.read.parquet(str(test_dir))
    assert column_types(result.schema) == column_types(OUTPUT_SCHEMA)
    assert result.count() == len(expected.CONVERT_SHIPPING_FEE_ROWS)
    for column in NOT_NULL_COLUMNS:
        assert result.filter(result[column].isNull()).isEmpty(), column
    # One folder per partition value.
    partition_key = "order_date"
    folders = sorted(path.name for path in test_dir.iterdir() if path.is_dir())
    dates = fixture_dates(expected.CONVERT_SHIPPING_FEE_ROWS, partition_key)
    assert folders == [f"{partition_key}={day}" for day in dates]
    # The table, which Athena reads, lists one partition per folder and holds every row.
    assert registered_partitions(spark, output_table) == folders
    assert spark.table(output_table).count() == result.count()


def test_write_incorrect_schema(spark: SparkSession, test_dir: Path, output_table: str) -> None:
    """write() refuses output whose column types differ from the output contract's."""
    column, other_type = "order_date", "string"
    output = spark.createDataFrame([], OUTPUT_SCHEMA)
    output = output.withColumn(column, output[column].cast(other_type))
    with pytest.raises(ValueError, match="Schema validation failed"):
        job.write(output, str(test_dir), output_table)
    # A write that fails its checks registers nothing.
    assert registered_partitions(spark, output_table) == []


def _add_order_record(test_dir: Path, **changes: object) -> list[str]:
    """Write the sources, then add one record to an orders file: a copy of its first record
    under a new order_id, with `changes` applied as the JSON text the export would hold.

    Returns:
        The job's arguments, for parse_arguments.
    """
    argv = write_sources(spark_session(), SOURCES, test_dir)
    path = next((test_dir / "orders").glob("export_date=*/*.json"))
    first = json.loads(path.read_text(encoding="utf-8").splitlines()[0])
    record = {**first, "order_id": "ORD-000009999", **changes}
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record) + "\n")
    return argv


@pytest.mark.parametrize(
    "changes",
    [
        {"shipping_fee": "abc"},
        {"updated_at": "2026-10-15T10:12:30.120456Z"},
        {"updated_at": "2026-10-15T10:12:30Z"},
    ],
    ids=["value-not-of-its-type", "timestamp-finer-than-milliseconds", "timestamp-no-fraction"],
)
def test_read_malformed_value_raises(test_dir: Path, changes: dict[str, object]) -> None:
    """A value the contract's type or format can't read fails the read, not a null.

    Spark names most as malformed records. A timestamp with too many fraction digits it
    reports as one its own parser can't read, though Spark's pre-3.0 parser could
    (spark.sql.legacy.timeParserPolicy, left at its default EXCEPTION).
    """
    args = job.parse_arguments(_add_order_record(test_dir, **changes))
    with pytest.raises(Py4JJavaError, match="Malformed records|Fail to parse"):
        job.read(args, processing_dates(SOURCES))


def test_read_value_breaking_a_rule_raises(test_dir: Path) -> None:
    """A value outside the contract's allowed values fails the read."""
    args = job.parse_arguments(_add_order_record(test_dir, order_status="lost"))
    with pytest.raises(ValueError, match=r"outside the allowed values \{'order_status': 1\}"):
        job.read(args, processing_dates(SOURCES))
