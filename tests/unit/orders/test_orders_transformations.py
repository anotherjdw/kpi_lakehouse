"""Unit tests of the orders transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kpi_lakehouse.jobs.orders import (
    orders_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.orders import expected, inputs
from tests.helpers import (
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
CREATE_ORDER_DATE_SCHEMA = StructType(
    [
        StructField("export_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=True),
        StructField("currency_code", StringType(), nullable=True),
        StructField("shipping_fee", DecimalType(12, 2), nullable=True),
        StructField("voucher_code", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("shipping_country", StringType(), nullable=True),
        StructField("is_test_order", BooleanType(), nullable=True),
        StructField("order_date", DateType(), nullable=True),
    ]
)

KEEP_LATEST_STATUS_SCHEMA = StructType(
    [
        StructField("export_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=True),
        StructField("currency_code", StringType(), nullable=True),
        StructField("shipping_fee", DecimalType(12, 2), nullable=True),
        StructField("voucher_code", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("shipping_country", StringType(), nullable=True),
        StructField("order_date", DateType(), nullable=True),
    ]
)

JOIN_FX_RATES_SCHEMA = StructType(
    [
        StructField("rate_date", DateType(), nullable=True),
        StructField("base_currency", StringType(), nullable=True),
        StructField("quote_currency", StringType(), nullable=True),
        StructField("rate", DecimalType(18, 8), nullable=True),
        StructField("export_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=True),
        StructField("currency_code", StringType(), nullable=True),
        StructField("shipping_fee", DecimalType(12, 2), nullable=True),
        StructField("voucher_code", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("shipping_country", StringType(), nullable=True),
        StructField("order_date", DateType(), nullable=True),
    ]
)


def test_create_order_date(spark: SparkSession) -> None:
    """create_order_date returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("orders")
    assert inputs.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
    assert expected.CREATE_ORDER_DATE_REVIEWED, review_message("orders", "CREATE_ORDER_DATE")
    df_actual = job.create_order_date(
        rows_to_df(spark, inputs.ORDERS_ROWS, schema_definitions.orders_raw_schema),
    )
    df_expected = rows_to_df(spark, expected.CREATE_ORDER_DATE_ROWS, CREATE_ORDER_DATE_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_keep_latest_status(spark: SparkSession) -> None:
    """keep_latest_status returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("orders")
    assert inputs.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
    assert expected.KEEP_LATEST_STATUS_REVIEWED, review_message("orders", "KEEP_LATEST_STATUS")
    df_actual = job.keep_latest_status(
        rows_to_df(spark, expected.CREATE_ORDER_DATE_ROWS, CREATE_ORDER_DATE_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.KEEP_LATEST_STATUS_ROWS, KEEP_LATEST_STATUS_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_fx_rates(spark: SparkSession) -> None:
    """join_fx_rates returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("orders")
    assert inputs.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
    assert expected.JOIN_FX_RATES_REVIEWED, review_message("orders", "JOIN_FX_RATES")
    df_actual = job.join_fx_rates(
        rows_to_df(spark, inputs.FX_RATES_ROWS, schema_definitions.fx_rates_schema),
        rows_to_df(spark, expected.KEEP_LATEST_STATUS_ROWS, KEEP_LATEST_STATUS_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.JOIN_FX_RATES_ROWS, JOIN_FX_RATES_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_convert_shipping_fee(spark: SparkSession) -> None:
    """convert_shipping_fee returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("orders")
    assert inputs.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
    assert expected.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    df_actual = job.convert_shipping_fee(
        rows_to_df(spark, expected.JOIN_FX_RATES_ROWS, JOIN_FX_RATES_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CONVERT_SHIPPING_FEE_ROWS,
        schema_definitions.orders_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
