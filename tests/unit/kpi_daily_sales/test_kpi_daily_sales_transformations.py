"""Unit tests of the kpi_daily_sales transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StructField,
    StructType,
)

from kpi_lakehouse.jobs.kpi_daily_sales import (
    kpi_daily_sales_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.kpi_daily_sales import expected, inputs
from tests.helpers import (
    column_types,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
AGGREGATE_DAILY_TOTALS_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), nullable=True),
        StructField("orders", IntegerType(), nullable=True),
        StructField("cancelled_orders", IntegerType(), nullable=True),
        StructField("units", IntegerType(), nullable=True),
        StructField("gmv_eur", DecimalType(12, 2), nullable=True),
        StructField("net_sales_eur", DecimalType(12, 2), nullable=True),
        StructField("net_revenue_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
    ]
)

CREATE_DATE_KEY_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), nullable=True),
        StructField("orders", IntegerType(), nullable=True),
        StructField("cancelled_orders", IntegerType(), nullable=True),
        StructField("units", IntegerType(), nullable=True),
        StructField("gmv_eur", DecimalType(12, 2), nullable=True),
        StructField("net_sales_eur", DecimalType(12, 2), nullable=True),
        StructField("net_revenue_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
    ]
)

CREATE_AVERAGE_ORDER_VALUE_COLUMN_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), nullable=True),
        StructField("orders", IntegerType(), nullable=True),
        StructField("cancelled_orders", IntegerType(), nullable=True),
        StructField("units", IntegerType(), nullable=True),
        StructField("gmv_eur", DecimalType(12, 2), nullable=True),
        StructField("net_sales_eur", DecimalType(12, 2), nullable=True),
        StructField("net_revenue_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("aov_eur", DecimalType(12, 2), nullable=True),
    ]
)

CREATE_GROSS_MARGIN_PERCENTAGE_COLUMN_SCHEMA = StructType(
    [
        StructField("order_date", DateType(), nullable=True),
        StructField("orders", IntegerType(), nullable=True),
        StructField("cancelled_orders", IntegerType(), nullable=True),
        StructField("units", IntegerType(), nullable=True),
        StructField("gmv_eur", DecimalType(12, 2), nullable=True),
        StructField("net_sales_eur", DecimalType(12, 2), nullable=True),
        StructField("net_revenue_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("aov_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_pct", DecimalType(2, 2), nullable=True),
    ]
)


def test_aggregate_daily_totals(spark: SparkSession) -> None:
    """aggregate_daily_totals returns exactly its reviewed expected rows."""
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert expected.AGGREGATE_DAILY_TOTALS_REVIEWED, review_message(
        "kpi_daily_sales",
        "AGGREGATE_DAILY_TOTALS",
    )
    df_actual = job.aggregate_daily_totals(
        rows_to_df(spark, inputs.FACT_ORDER_LINE_ROWS, schema_definitions.fact_order_line_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.AGGREGATE_DAILY_TOTALS_ROWS,
        AGGREGATE_DAILY_TOTALS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_date_key(spark: SparkSession) -> None:
    """create_date_key returns exactly its reviewed expected rows."""
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert expected.CREATE_DATE_KEY_REVIEWED, review_message("kpi_daily_sales", "CREATE_DATE_KEY")
    df_actual = job.create_date_key(
        rows_to_df(spark, expected.AGGREGATE_DAILY_TOTALS_ROWS, AGGREGATE_DAILY_TOTALS_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.CREATE_DATE_KEY_ROWS, CREATE_DATE_KEY_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_average_order_value_column(spark: SparkSession) -> None:
    """create_average_order_value_column returns exactly its reviewed expected rows."""
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert expected.CREATE_AVERAGE_ORDER_VALUE_COLUMN_REVIEWED, review_message(
        "kpi_daily_sales",
        "CREATE_AVERAGE_ORDER_VALUE_COLUMN",
    )
    df_actual = job.create_average_order_value_column(
        rows_to_df(spark, expected.CREATE_DATE_KEY_ROWS, CREATE_DATE_KEY_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_AVERAGE_ORDER_VALUE_COLUMN_ROWS,
        CREATE_AVERAGE_ORDER_VALUE_COLUMN_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_gross_margin_percentage_column(spark: SparkSession) -> None:
    """create_gross_margin_percentage_column returns exactly its reviewed expected rows."""
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert expected.CREATE_GROSS_MARGIN_PERCENTAGE_COLUMN_REVIEWED, review_message(
        "kpi_daily_sales",
        "CREATE_GROSS_MARGIN_PERCENTAGE_COLUMN",
    )
    df_actual = job.create_gross_margin_percentage_column(
        rows_to_df(
            spark,
            expected.CREATE_AVERAGE_ORDER_VALUE_COLUMN_ROWS,
            CREATE_AVERAGE_ORDER_VALUE_COLUMN_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_GROSS_MARGIN_PERCENTAGE_COLUMN_ROWS,
        CREATE_GROSS_MARGIN_PERCENTAGE_COLUMN_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_cancellation_rate_column(spark: SparkSession) -> None:
    """create_cancellation_rate_column returns exactly its reviewed expected rows."""
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert expected.CREATE_CANCELLATION_RATE_COLUMN_REVIEWED, review_message(
        "kpi_daily_sales",
        "CREATE_CANCELLATION_RATE_COLUMN",
    )
    df_actual = job.create_cancellation_rate_column(
        rows_to_df(
            spark,
            expected.CREATE_GROSS_MARGIN_PERCENTAGE_COLUMN_ROWS,
            CREATE_GROSS_MARGIN_PERCENTAGE_COLUMN_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_CANCELLATION_RATE_COLUMN_ROWS,
        schema_definitions.kpi_daily_sales_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
