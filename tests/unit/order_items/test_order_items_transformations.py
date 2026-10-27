"""Unit tests of the order_items transformations.

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
)

from kpi_lakehouse.jobs.order_items import (
    order_items_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.order_items import expected, inputs
from tests.helpers import (
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
JOIN_ORDER_DATA_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("rate", DecimalType(18, 8), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
    ]
)

CREATE_GROSS_AMOUNT_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("rate", DecimalType(18, 8), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("gross_amount", DecimalType(12, 2), nullable=True),
    ]
)

CREATE_NET_AMOUNT_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("rate", DecimalType(18, 8), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("gross_amount", DecimalType(12, 2), nullable=True),
        StructField("net_amount", DecimalType(12, 2), nullable=True),
    ]
)

CONVERT_GROSS_AMOUNT_TO_EUROS_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("rate", DecimalType(18, 8), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(12, 2), nullable=True),
        StructField("discount_amount", DecimalType(12, 2), nullable=True),
        StructField("tax_rate", DecimalType(5, 4), nullable=True),
        StructField("net_amount", DecimalType(12, 2), nullable=True),
        StructField("gross_amount_eur", DecimalType(12, 2), nullable=True),
    ]
)


def test_join_order_data(spark: SparkSession) -> None:
    """join_order_data returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("order_items")
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert expected.JOIN_ORDER_DATA_REVIEWED, review_message("order_items", "JOIN_ORDER_DATA")
    df_actual = job.join_order_data(
        rows_to_df(spark, inputs.ORDERS_ROWS, schema_definitions.orders_schema),
        rows_to_df(spark, inputs.ORDER_ITEMS_ROWS, schema_definitions.order_items_raw_schema),
    )
    df_expected = rows_to_df(spark, expected.JOIN_ORDER_DATA_ROWS, JOIN_ORDER_DATA_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_gross_amount(spark: SparkSession) -> None:
    """create_gross_amount returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("order_items")
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert expected.CREATE_GROSS_AMOUNT_REVIEWED, review_message(
        "order_items",
        "CREATE_GROSS_AMOUNT",
    )
    df_actual = job.create_gross_amount(
        rows_to_df(spark, expected.JOIN_ORDER_DATA_ROWS, JOIN_ORDER_DATA_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.CREATE_GROSS_AMOUNT_ROWS, CREATE_GROSS_AMOUNT_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_net_amount(spark: SparkSession) -> None:
    """create_net_amount returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("order_items")
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert expected.CREATE_NET_AMOUNT_REVIEWED, review_message("order_items", "CREATE_NET_AMOUNT")
    df_actual = job.create_net_amount(
        rows_to_df(spark, expected.CREATE_GROSS_AMOUNT_ROWS, CREATE_GROSS_AMOUNT_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.CREATE_NET_AMOUNT_ROWS, CREATE_NET_AMOUNT_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_convert_gross_amount_to_euros(spark: SparkSession) -> None:
    """convert_gross_amount_to_euros returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("order_items")
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert expected.CONVERT_GROSS_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_GROSS_AMOUNT_TO_EUROS",
    )
    df_actual = job.convert_gross_amount_to_euros(
        rows_to_df(spark, expected.CREATE_NET_AMOUNT_ROWS, CREATE_NET_AMOUNT_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CONVERT_GROSS_AMOUNT_TO_EUROS_ROWS,
        CONVERT_GROSS_AMOUNT_TO_EUROS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_convert_net_amount_to_euros(spark: SparkSession) -> None:
    """convert_net_amount_to_euros returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("order_items")
    assert inputs.CONVERT_SHIPPING_FEE_REVIEWED, review_message("orders", "CONVERT_SHIPPING_FEE")
    assert expected.CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "order_items",
        "CONVERT_NET_AMOUNT_TO_EUROS",
    )
    df_actual = job.convert_net_amount_to_euros(
        rows_to_df(
            spark,
            expected.CONVERT_GROSS_AMOUNT_TO_EUROS_ROWS,
            CONVERT_GROSS_AMOUNT_TO_EUROS_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.CONVERT_NET_AMOUNT_TO_EUROS_ROWS,
        schema_definitions.order_items_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
