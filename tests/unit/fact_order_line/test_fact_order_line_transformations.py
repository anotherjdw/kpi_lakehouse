"""Unit tests of the fact_order_line transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kpi_lakehouse.jobs.fact_order_line import (
    fact_order_line_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.fact_order_line import expected, inputs
from tests.helpers import (
    column_types,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
SELECT_ORDER_COLUMNS_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
    ]
)

KEEP_LATEST_RETURN_STATUS_SCHEMA = StructType(
    [
        StructField("update_date", DateType(), nullable=True),
        StructField("export_date", DateType(), nullable=True),
        StructField("return_id", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("requested_ts", TimestampType(), nullable=True),
        StructField("quantity_returned", IntegerType(), nullable=True),
        StructField("return_reason", StringType(), nullable=True),
        StructField("return_status", StringType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("refund_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_refund_eur", DecimalType(12, 2), nullable=True),
    ]
)

KEEP_SESSION_LANDING_ROW_SCHEMA = StructType(
    [
        StructField("session_id", StringType(), nullable=True),
        StructField("event_date", DateType(), nullable=True),
        StructField("first_event_time", TimestampType(), nullable=True),
        StructField("last_event_time", TimestampType(), nullable=True),
        StructField("first_mkt_channel", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("last_event_type", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
    ]
)

JOIN_ORDER_ITEMS_TO_ORDERS_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_amount_eur", DecimalType(12, 2), nullable=True),
    ]
)

AGGREGATE_RETURNS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("returned_quantity", IntegerType(), nullable=True),
        StructField("refund_net_eur", DecimalType(12, 2), nullable=True),
    ]
)

JOIN_CUSTOMER_VERSION_TO_ORDER_DATA_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
    ]
)

JOIN_PRODUCT_VERSION_TO_ORDERS_DATA_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("cost_price_eur", DecimalType(12, 2), nullable=True),
        StructField("product_version_id", StringType(), nullable=True),
    ]
)

CREATE_COST_COLUMN_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("product_version_id", StringType(), nullable=True),
        StructField("cost_eur", DecimalType(12, 2), nullable=True),
    ]
)

CREATE_GROSS_MARGIN_COLUMN_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("product_version_id", StringType(), nullable=True),
        StructField("cost_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
    ]
)

JOIN_AGGREGATED_RETURNS_TO_ORDER_DATA_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("returned_quantity", IntegerType(), nullable=True),
        StructField("refund_net_eur", DecimalType(12, 2), nullable=True),
        StructField("order_date", DateType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("product_version_id", StringType(), nullable=True),
        StructField("cost_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
    ]
)

FILL_MISSING_RETURNS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("returned_quantity", IntegerType(), nullable=True),
        StructField("refund_net_eur", DecimalType(12, 2), nullable=True),
        StructField("order_date", DateType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("product_version_id", StringType(), nullable=True),
        StructField("cost_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
    ]
)

CREATE_NET_REVENUE_AFTER_RETURNS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("returned_quantity", IntegerType(), nullable=True),
        StructField("refund_net_eur", DecimalType(12, 2), nullable=True),
        StructField("order_date", DateType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("product_version_id", StringType(), nullable=True),
        StructField("cost_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
        StructField("net_revenue_after_returns_eur", DecimalType(12, 2), nullable=True),
    ]
)

JOIN_SESSION_CHANNEL_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("returned_quantity", IntegerType(), nullable=True),
        StructField("refund_net_eur", DecimalType(12, 2), nullable=True),
        StructField("order_date", DateType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("product_version_id", StringType(), nullable=True),
        StructField("cost_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
        StructField("net_revenue_after_returns_eur", DecimalType(12, 2), nullable=True),
        StructField("session_channel", StringType(), nullable=True),
    ]
)

JOIN_SESSIONS_FOR_FALLBACK_CHANNEL_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("returned_quantity", IntegerType(), nullable=True),
        StructField("refund_net_eur", DecimalType(12, 2), nullable=True),
        StructField("order_date", DateType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("product_version_id", StringType(), nullable=True),
        StructField("cost_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
        StructField("net_revenue_after_returns_eur", DecimalType(12, 2), nullable=True),
        StructField("session_channel", StringType(), nullable=True),
        StructField("fallback_channel", StringType(), nullable=True),
    ]
)

CREATE_MKT_CHANNEL_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("returned_quantity", IntegerType(), nullable=True),
        StructField("refund_net_eur", DecimalType(12, 2), nullable=True),
        StructField("order_date", DateType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("order_ts", TimestampType(), nullable=True),
        StructField("order_status", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("net_amount_eur", DecimalType(12, 2), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("product_version_id", StringType(), nullable=True),
        StructField("cost_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
        StructField("net_revenue_after_returns_eur", DecimalType(12, 2), nullable=True),
        StructField("mkt_channel", StringType(), nullable=True),
    ]
)


def test_select_order_columns(spark: SparkSession) -> None:
    """select_order_columns returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.SELECT_ORDER_COLUMNS_REVIEWED, review_message(
        "fact_order_line",
        "SELECT_ORDER_COLUMNS",
    )
    df_actual = job.select_order_columns(
        rows_to_df(spark, inputs.ORDERS_ROWS, schema_definitions.orders_schema),
    )
    df_expected = rows_to_df(spark, expected.SELECT_ORDER_COLUMNS_ROWS, SELECT_ORDER_COLUMNS_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_keep_latest_return_status(spark: SparkSession) -> None:
    """keep_latest_return_status returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.KEEP_LATEST_RETURN_STATUS_REVIEWED, review_message(
        "fact_order_line",
        "KEEP_LATEST_RETURN_STATUS",
    )
    df_actual = job.keep_latest_return_status(
        rows_to_df(spark, inputs.RETURNS_ROWS, schema_definitions.returns_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.KEEP_LATEST_RETURN_STATUS_ROWS,
        KEEP_LATEST_RETURN_STATUS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_keep_session_landing_row(spark: SparkSession) -> None:
    """keep_session_landing_row returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.KEEP_SESSION_LANDING_ROW_REVIEWED, review_message(
        "fact_order_line",
        "KEEP_SESSION_LANDING_ROW",
    )
    df_actual = job.keep_session_landing_row(
        rows_to_df(spark, inputs.SESSIONS_ROWS, schema_definitions.sessions_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.KEEP_SESSION_LANDING_ROW_ROWS,
        KEEP_SESSION_LANDING_ROW_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_order_items_to_orders(spark: SparkSession) -> None:
    """join_order_items_to_orders returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.JOIN_ORDER_ITEMS_TO_ORDERS_REVIEWED, review_message(
        "fact_order_line",
        "JOIN_ORDER_ITEMS_TO_ORDERS",
    )
    df_actual = job.join_order_items_to_orders(
        rows_to_df(spark, expected.SELECT_ORDER_COLUMNS_ROWS, SELECT_ORDER_COLUMNS_SCHEMA),
        rows_to_df(spark, inputs.ORDER_ITEMS_ROWS, schema_definitions.order_items_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.JOIN_ORDER_ITEMS_TO_ORDERS_ROWS,
        JOIN_ORDER_ITEMS_TO_ORDERS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_aggregate_returns(spark: SparkSession) -> None:
    """aggregate_returns returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.AGGREGATE_RETURNS_REVIEWED, review_message(
        "fact_order_line",
        "AGGREGATE_RETURNS",
    )
    df_actual = job.aggregate_returns(
        rows_to_df(
            spark,
            expected.KEEP_LATEST_RETURN_STATUS_ROWS,
            KEEP_LATEST_RETURN_STATUS_SCHEMA,
        ),
    )
    df_expected = rows_to_df(spark, expected.AGGREGATE_RETURNS_ROWS, AGGREGATE_RETURNS_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_customer_version_to_order_data(spark: SparkSession) -> None:
    """join_customer_version_to_order_data returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.JOIN_CUSTOMER_VERSION_TO_ORDER_DATA_REVIEWED, review_message(
        "fact_order_line",
        "JOIN_CUSTOMER_VERSION_TO_ORDER_DATA",
    )
    df_actual = job.join_customer_version_to_order_data(
        rows_to_df(
            spark,
            expected.JOIN_ORDER_ITEMS_TO_ORDERS_ROWS,
            JOIN_ORDER_ITEMS_TO_ORDERS_SCHEMA,
        ),
        rows_to_df(spark, inputs.DIM_CUSTOMER_ROWS, schema_definitions.dim_customer_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.JOIN_CUSTOMER_VERSION_TO_ORDER_DATA_ROWS,
        JOIN_CUSTOMER_VERSION_TO_ORDER_DATA_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_product_version_to_orders_data(spark: SparkSession) -> None:
    """join_product_version_to_orders_data returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.JOIN_PRODUCT_VERSION_TO_ORDERS_DATA_REVIEWED, review_message(
        "fact_order_line",
        "JOIN_PRODUCT_VERSION_TO_ORDERS_DATA",
    )
    df_actual = job.join_product_version_to_orders_data(
        rows_to_df(
            spark,
            expected.JOIN_CUSTOMER_VERSION_TO_ORDER_DATA_ROWS,
            JOIN_CUSTOMER_VERSION_TO_ORDER_DATA_SCHEMA,
        ),
        rows_to_df(spark, inputs.DIM_PRODUCT_ROWS, schema_definitions.dim_product_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.JOIN_PRODUCT_VERSION_TO_ORDERS_DATA_ROWS,
        JOIN_PRODUCT_VERSION_TO_ORDERS_DATA_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_cost_column(spark: SparkSession) -> None:
    """create_cost_column returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.CREATE_COST_COLUMN_REVIEWED, review_message(
        "fact_order_line",
        "CREATE_COST_COLUMN",
    )
    df_actual = job.create_cost_column(
        rows_to_df(
            spark,
            expected.JOIN_PRODUCT_VERSION_TO_ORDERS_DATA_ROWS,
            JOIN_PRODUCT_VERSION_TO_ORDERS_DATA_SCHEMA,
        ),
    )
    df_expected = rows_to_df(spark, expected.CREATE_COST_COLUMN_ROWS, CREATE_COST_COLUMN_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_gross_margin_column(spark: SparkSession) -> None:
    """create_gross_margin_column returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.CREATE_GROSS_MARGIN_COLUMN_REVIEWED, review_message(
        "fact_order_line",
        "CREATE_GROSS_MARGIN_COLUMN",
    )
    df_actual = job.create_gross_margin_column(
        rows_to_df(spark, expected.CREATE_COST_COLUMN_ROWS, CREATE_COST_COLUMN_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_GROSS_MARGIN_COLUMN_ROWS,
        CREATE_GROSS_MARGIN_COLUMN_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_aggregated_returns_to_order_data(spark: SparkSession) -> None:
    """join_aggregated_returns_to_order_data returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.JOIN_AGGREGATED_RETURNS_TO_ORDER_DATA_REVIEWED, review_message(
        "fact_order_line",
        "JOIN_AGGREGATED_RETURNS_TO_ORDER_DATA",
    )
    df_actual = job.join_aggregated_returns_to_order_data(
        rows_to_df(spark, expected.AGGREGATE_RETURNS_ROWS, AGGREGATE_RETURNS_SCHEMA),
        rows_to_df(
            spark,
            expected.CREATE_GROSS_MARGIN_COLUMN_ROWS,
            CREATE_GROSS_MARGIN_COLUMN_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.JOIN_AGGREGATED_RETURNS_TO_ORDER_DATA_ROWS,
        JOIN_AGGREGATED_RETURNS_TO_ORDER_DATA_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_fill_missing_returns(spark: SparkSession) -> None:
    """fill_missing_returns returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.FILL_MISSING_RETURNS_REVIEWED, review_message(
        "fact_order_line",
        "FILL_MISSING_RETURNS",
    )
    df_actual = job.fill_missing_returns(
        rows_to_df(
            spark,
            expected.JOIN_AGGREGATED_RETURNS_TO_ORDER_DATA_ROWS,
            JOIN_AGGREGATED_RETURNS_TO_ORDER_DATA_SCHEMA,
        ),
    )
    df_expected = rows_to_df(spark, expected.FILL_MISSING_RETURNS_ROWS, FILL_MISSING_RETURNS_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_net_revenue_after_returns(spark: SparkSession) -> None:
    """create_net_revenue_after_returns returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.CREATE_NET_REVENUE_AFTER_RETURNS_REVIEWED, review_message(
        "fact_order_line",
        "CREATE_NET_REVENUE_AFTER_RETURNS",
    )
    df_actual = job.create_net_revenue_after_returns(
        rows_to_df(spark, expected.FILL_MISSING_RETURNS_ROWS, FILL_MISSING_RETURNS_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_NET_REVENUE_AFTER_RETURNS_ROWS,
        CREATE_NET_REVENUE_AFTER_RETURNS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_session_channel(spark: SparkSession) -> None:
    """join_session_channel returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.JOIN_SESSION_CHANNEL_REVIEWED, review_message(
        "fact_order_line",
        "JOIN_SESSION_CHANNEL",
    )
    df_actual = job.join_session_channel(
        rows_to_df(
            spark,
            expected.CREATE_NET_REVENUE_AFTER_RETURNS_ROWS,
            CREATE_NET_REVENUE_AFTER_RETURNS_SCHEMA,
        ),
        rows_to_df(spark, expected.KEEP_SESSION_LANDING_ROW_ROWS, KEEP_SESSION_LANDING_ROW_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.JOIN_SESSION_CHANNEL_ROWS, JOIN_SESSION_CHANNEL_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_sessions_for_fallback_channel(spark: SparkSession) -> None:
    """join_sessions_for_fallback_channel returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.JOIN_SESSIONS_FOR_FALLBACK_CHANNEL_REVIEWED, review_message(
        "fact_order_line",
        "JOIN_SESSIONS_FOR_FALLBACK_CHANNEL",
    )
    df_actual = job.join_sessions_for_fallback_channel(
        rows_to_df(spark, expected.JOIN_SESSION_CHANNEL_ROWS, JOIN_SESSION_CHANNEL_SCHEMA),
        rows_to_df(spark, inputs.SESSIONS_ROWS, schema_definitions.sessions_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.JOIN_SESSIONS_FOR_FALLBACK_CHANNEL_ROWS,
        JOIN_SESSIONS_FOR_FALLBACK_CHANNEL_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_mkt_channel(spark: SparkSession) -> None:
    """create_mkt_channel returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.CREATE_MKT_CHANNEL_REVIEWED, review_message(
        "fact_order_line",
        "CREATE_MKT_CHANNEL",
    )
    df_actual = job.create_mkt_channel(
        rows_to_df(
            spark,
            expected.JOIN_SESSIONS_FOR_FALLBACK_CHANNEL_ROWS,
            JOIN_SESSIONS_FOR_FALLBACK_CHANNEL_SCHEMA,
        ),
    )
    df_expected = rows_to_df(spark, expected.CREATE_MKT_CHANNEL_ROWS, CREATE_MKT_CHANNEL_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_date_key(spark: SparkSession) -> None:
    """create_date_key returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert inputs.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    df_actual = job.create_date_key(
        rows_to_df(spark, expected.CREATE_MKT_CHANNEL_ROWS, CREATE_MKT_CHANNEL_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_DATE_KEY_ROWS,
        schema_definitions.fact_order_line_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
