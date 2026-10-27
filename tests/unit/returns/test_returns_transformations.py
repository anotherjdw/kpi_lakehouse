"""Unit tests of the returns transformations.

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

from kpi_lakehouse.jobs.returns import (
    returns_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.returns import expected, inputs
from tests.helpers import (
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
CREATE_UPDATED_DATE_COLUMN_SCHEMA = StructType(
    [
        StructField("export_date", DateType(), nullable=True),
        StructField("return_id", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("requested_ts", TimestampType(), nullable=True),
        StructField("quantity_returned", IntegerType(), nullable=True),
        StructField("refund_amount", DecimalType(12, 2), nullable=True),
        StructField("return_reason", StringType(), nullable=True),
        StructField("return_status", StringType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=True),
        StructField("update_date", DateType(), nullable=True),
    ]
)

KEEP_LATEST_STATUS_SCHEMA = StructType(
    [
        StructField("export_date", DateType(), nullable=True),
        StructField("return_id", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("requested_ts", TimestampType(), nullable=True),
        StructField("quantity_returned", IntegerType(), nullable=True),
        StructField("refund_amount", DecimalType(12, 2), nullable=True),
        StructField("return_reason", StringType(), nullable=True),
        StructField("return_status", StringType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=True),
        StructField("update_date", DateType(), nullable=True),
    ]
)

JOIN_ORDER_ITEMS_DATA_SCHEMA = StructType(
    [
        StructField("export_date", DateType(), nullable=True),
        StructField("return_id", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("requested_ts", TimestampType(), nullable=True),
        StructField("quantity_returned", IntegerType(), nullable=True),
        StructField("refund_amount", DecimalType(12, 2), nullable=True),
        StructField("return_reason", StringType(), nullable=True),
        StructField("return_status", StringType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=True),
        StructField("update_date", DateType(), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("rate", DecimalType(18, 8), nullable=True),
    ]
)

CONVERT_REFUND_AMOUNT_SCHEMA = StructType(
    [
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
        StructField("update_date", DateType(), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("refund_amount_eur", DecimalType(12, 2), nullable=True),
    ]
)


def test_create_updated_date_column(spark: SparkSession) -> None:
    """create_updated_date_column returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("returns")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert expected.CREATE_UPDATED_DATE_COLUMN_REVIEWED, review_message(
        "returns",
        "CREATE_UPDATED_DATE_COLUMN",
    )
    df_actual = job.create_updated_date_column(
        rows_to_df(spark, inputs.RETURNS_ROWS, schema_definitions.returns_raw_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_UPDATED_DATE_COLUMN_ROWS,
        CREATE_UPDATED_DATE_COLUMN_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_keep_latest_status(spark: SparkSession) -> None:
    """keep_latest_status returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("returns")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert expected.KEEP_LATEST_STATUS_REVIEWED, review_message("returns", "KEEP_LATEST_STATUS")
    df_actual = job.keep_latest_status(
        rows_to_df(
            spark,
            expected.CREATE_UPDATED_DATE_COLUMN_ROWS,
            CREATE_UPDATED_DATE_COLUMN_SCHEMA,
        ),
    )
    df_expected = rows_to_df(spark, expected.KEEP_LATEST_STATUS_ROWS, KEEP_LATEST_STATUS_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_order_items_data(spark: SparkSession) -> None:
    """join_order_items_data returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("returns")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert expected.JOIN_ORDER_ITEMS_DATA_REVIEWED, review_message(
        "returns",
        "JOIN_ORDER_ITEMS_DATA",
    )
    df_actual = job.join_order_items_data(
        rows_to_df(spark, expected.KEEP_LATEST_STATUS_ROWS, KEEP_LATEST_STATUS_SCHEMA),
        rows_to_df(spark, inputs.ORDER_ITEMS_ROWS, schema_definitions.order_items_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.JOIN_ORDER_ITEMS_DATA_ROWS,
        JOIN_ORDER_ITEMS_DATA_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_convert_refund_amount(spark: SparkSession) -> None:
    """convert_refund_amount returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("returns")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert expected.CONVERT_REFUND_AMOUNT_REVIEWED, review_message(
        "returns",
        "CONVERT_REFUND_AMOUNT",
    )
    df_actual = job.convert_refund_amount(
        rows_to_df(spark, expected.JOIN_ORDER_ITEMS_DATA_ROWS, JOIN_ORDER_ITEMS_DATA_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CONVERT_REFUND_AMOUNT_ROWS,
        CONVERT_REFUND_AMOUNT_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_compute_net_refund(spark: SparkSession) -> None:
    """compute_net_refund returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("returns")
    assert inputs.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    assert expected.COMPUTE_NET_REFUND_REVIEWED, review_message("returns", "COMPUTE_NET_REFUND")
    df_actual = job.compute_net_refund(
        rows_to_df(spark, expected.CONVERT_REFUND_AMOUNT_ROWS, CONVERT_REFUND_AMOUNT_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.COMPUTE_NET_REFUND_ROWS,
        schema_definitions.returns_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
