"""Unit tests of the kpi_return_rate transformations.

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

from kpi_lakehouse.jobs.kpi_return_rate import (
    kpi_return_rate_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.kpi_return_rate import expected, inputs
from tests.helpers import (
    column_types,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
SELECT_CATEGORY_LEVEL_SCHEMA = StructType(
    [
        StructField("product_version_id", StringType(), nullable=True),
        StructField("category_id_level_1", StringType(), nullable=True),
    ]
)

JOIN_CATEGORY_DATA_TO_FACT_ORDER_LINE_SCHEMA = StructType(
    [
        StructField("product_version_id", StringType(), nullable=True),
        StructField("category_id_level_1", StringType(), nullable=True),
        StructField("order_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
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
        StructField("cost_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
        StructField("returned_quantity", IntegerType(), nullable=True),
        StructField("refund_net_eur", DecimalType(12, 2), nullable=True),
        StructField("net_revenue_after_returns_eur", DecimalType(12, 2), nullable=True),
        StructField("mkt_channel", StringType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
    ]
)

FILTER_COMPLETED_ORDERS_SCHEMA = StructType(
    [
        StructField("product_version_id", StringType(), nullable=True),
        StructField("category_id_level_1", StringType(), nullable=True),
        StructField("order_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
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
        StructField("cost_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
        StructField("returned_quantity", IntegerType(), nullable=True),
        StructField("refund_net_eur", DecimalType(12, 2), nullable=True),
        StructField("net_revenue_after_returns_eur", DecimalType(12, 2), nullable=True),
        StructField("mkt_channel", StringType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
    ]
)

CREATE_MONTH_COLUMN_SCHEMA = StructType(
    [
        StructField("product_version_id", StringType(), nullable=True),
        StructField("category_id_level_1", StringType(), nullable=True),
        StructField("order_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
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
        StructField("cost_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
        StructField("returned_quantity", IntegerType(), nullable=True),
        StructField("refund_net_eur", DecimalType(12, 2), nullable=True),
        StructField("net_revenue_after_returns_eur", DecimalType(12, 2), nullable=True),
        StructField("mkt_channel", StringType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("month", DateType(), nullable=True),
    ]
)

FILTER_COMPLETE_MONTHS_SCHEMA = StructType(
    [
        StructField("product_version_id", StringType(), nullable=True),
        StructField("category_id_level_1", StringType(), nullable=True),
        StructField("order_date", DateType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("line_number", IntegerType(), nullable=True),
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
        StructField("cost_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
        StructField("returned_quantity", IntegerType(), nullable=True),
        StructField("refund_net_eur", DecimalType(12, 2), nullable=True),
        StructField("net_revenue_after_returns_eur", DecimalType(12, 2), nullable=True),
        StructField("mkt_channel", StringType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("month", DateType(), nullable=True),
    ]
)

AGGREGATE_MONTHLY_RETURNS_SCHEMA = StructType(
    [
        StructField("month", DateType(), nullable=True),
        StructField("category_id_level_1", StringType(), nullable=True),
        StructField("units_sold", IntegerType(), nullable=True),
        StructField("units_returned", IntegerType(), nullable=True),
        StructField("net_refunds_eur", DecimalType(12, 2), nullable=True),
    ]
)

CREATE_RETURN_RATE_COLUMN_SCHEMA = StructType(
    [
        StructField("month", DateType(), nullable=True),
        StructField("category_id_level_1", StringType(), nullable=True),
        StructField("units_sold", IntegerType(), nullable=True),
        StructField("units_returned", IntegerType(), nullable=True),
        StructField("net_refunds_eur", DecimalType(12, 2), nullable=True),
        StructField("return_rate", DecimalType(5, 4), nullable=True),
    ]
)


def test_select_category_level(spark: SparkSession) -> None:
    """select_category_level returns exactly its reviewed expected rows."""
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert expected.SELECT_CATEGORY_LEVEL_REVIEWED, review_message(
        "kpi_return_rate",
        "SELECT_CATEGORY_LEVEL",
    )
    df_actual = job.select_category_level(
        rows_to_df(spark, inputs.DIM_PRODUCT_ROWS, schema_definitions.dim_product_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.SELECT_CATEGORY_LEVEL_ROWS,
        SELECT_CATEGORY_LEVEL_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_category_data_to_fact_order_line(spark: SparkSession) -> None:
    """join_category_data_to_fact_order_line returns exactly its reviewed expected rows."""
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert expected.JOIN_CATEGORY_DATA_TO_FACT_ORDER_LINE_REVIEWED, review_message(
        "kpi_return_rate",
        "JOIN_CATEGORY_DATA_TO_FACT_ORDER_LINE",
    )
    df_actual = job.join_category_data_to_fact_order_line(
        rows_to_df(spark, expected.SELECT_CATEGORY_LEVEL_ROWS, SELECT_CATEGORY_LEVEL_SCHEMA),
        rows_to_df(spark, inputs.FACT_ORDER_LINE_ROWS, schema_definitions.fact_order_line_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.JOIN_CATEGORY_DATA_TO_FACT_ORDER_LINE_ROWS,
        JOIN_CATEGORY_DATA_TO_FACT_ORDER_LINE_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_filter_completed_orders(spark: SparkSession) -> None:
    """filter_completed_orders returns exactly its reviewed expected rows."""
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert expected.FILTER_COMPLETED_ORDERS_REVIEWED, review_message(
        "kpi_return_rate",
        "FILTER_COMPLETED_ORDERS",
    )
    df_actual = job.filter_completed_orders(
        rows_to_df(
            spark,
            expected.JOIN_CATEGORY_DATA_TO_FACT_ORDER_LINE_ROWS,
            JOIN_CATEGORY_DATA_TO_FACT_ORDER_LINE_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.FILTER_COMPLETED_ORDERS_ROWS,
        FILTER_COMPLETED_ORDERS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_month_column(spark: SparkSession) -> None:
    """create_month_column returns exactly its reviewed expected rows."""
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert expected.CREATE_MONTH_COLUMN_REVIEWED, review_message(
        "kpi_return_rate",
        "CREATE_MONTH_COLUMN",
    )
    df_actual = job.create_month_column(
        rows_to_df(spark, expected.FILTER_COMPLETED_ORDERS_ROWS, FILTER_COMPLETED_ORDERS_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.CREATE_MONTH_COLUMN_ROWS, CREATE_MONTH_COLUMN_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_filter_complete_months(spark: SparkSession) -> None:
    """filter_complete_months returns exactly its reviewed expected rows."""
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert expected.FILTER_COMPLETE_MONTHS_REVIEWED, review_message(
        "kpi_return_rate",
        "FILTER_COMPLETE_MONTHS",
    )
    df_actual = job.filter_complete_months(
        rows_to_df(spark, expected.CREATE_MONTH_COLUMN_ROWS, CREATE_MONTH_COLUMN_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.FILTER_COMPLETE_MONTHS_ROWS,
        FILTER_COMPLETE_MONTHS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_aggregate_monthly_returns(spark: SparkSession) -> None:
    """aggregate_monthly_returns returns exactly its reviewed expected rows."""
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert expected.AGGREGATE_MONTHLY_RETURNS_REVIEWED, review_message(
        "kpi_return_rate",
        "AGGREGATE_MONTHLY_RETURNS",
    )
    df_actual = job.aggregate_monthly_returns(
        rows_to_df(spark, expected.FILTER_COMPLETE_MONTHS_ROWS, FILTER_COMPLETE_MONTHS_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.AGGREGATE_MONTHLY_RETURNS_ROWS,
        AGGREGATE_MONTHLY_RETURNS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_return_rate_column(spark: SparkSession) -> None:
    """create_return_rate_column returns exactly its reviewed expected rows."""
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert expected.CREATE_RETURN_RATE_COLUMN_REVIEWED, review_message(
        "kpi_return_rate",
        "CREATE_RETURN_RATE_COLUMN",
    )
    df_actual = job.create_return_rate_column(
        rows_to_df(
            spark,
            expected.AGGREGATE_MONTHLY_RETURNS_ROWS,
            AGGREGATE_MONTHLY_RETURNS_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_RETURN_RATE_COLUMN_ROWS,
        CREATE_RETURN_RATE_COLUMN_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_is_final_column(spark: SparkSession) -> None:
    """create_is_final_column returns exactly its reviewed expected rows."""
    assert inputs.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert expected.CREATE_IS_FINAL_COLUMN_REVIEWED, review_message(
        "kpi_return_rate",
        "CREATE_IS_FINAL_COLUMN",
    )
    df_actual = job.create_is_final_column(
        rows_to_df(
            spark,
            expected.CREATE_RETURN_RATE_COLUMN_ROWS,
            CREATE_RETURN_RATE_COLUMN_SCHEMA,
        ),
        rows_to_df(spark, expected.FILTER_COMPLETED_ORDERS_ROWS, FILTER_COMPLETED_ORDERS_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_IS_FINAL_COLUMN_ROWS,
        schema_definitions.kpi_return_rate_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
