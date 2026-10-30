"""Unit tests of the kpi_channel_performance transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kpi_lakehouse.jobs.kpi_channel_performance import (
    kpi_channel_performance_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.kpi_channel_performance import expected, inputs
from tests.helpers import (
    column_types,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
AGGREGATE_SESSIONS_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("mkt_channel", StringType(), nullable=True),
        StructField("sessions", LongType(), nullable=True),
        StructField("converted_sessions", LongType(), nullable=True),
    ]
)

FILTER_COMPLETED_ORDERS_SCHEMA = StructType(
    [
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
        StructField("product_version_id", StringType(), nullable=True),
        StructField("cost_eur", DecimalType(12, 2), nullable=True),
        StructField("gross_margin_eur", DecimalType(12, 2), nullable=True),
        StructField("returned_quantity", IntegerType(), nullable=True),
        StructField("refund_net_eur", DecimalType(12, 2), nullable=True),
        StructField("net_revenue_after_returns_eur", DecimalType(12, 2), nullable=True),
        StructField("mkt_channel", StringType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
    ]
)

AGGREGATE_MARKETING_SPEND_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("mkt_channel", StringType(), nullable=True),
        StructField("spend_eur", DecimalType(12, 2), nullable=True),
    ]
)

AGGREGATE_ATTRIBUTED_REVENUE_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("mkt_channel", StringType(), nullable=True),
        StructField("attributed_net_revenue_eur", DecimalType(12, 2), nullable=True),
    ]
)

COMBINE_CHANNEL_METRICS_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("mkt_channel", StringType(), nullable=True),
        StructField("sessions", LongType(), nullable=True),
        StructField("converted_sessions", LongType(), nullable=True),
        StructField("attributed_net_revenue_eur", DecimalType(12, 2), nullable=True),
        StructField("spend_eur", DecimalType(12, 2), nullable=True),
    ]
)

FILL_MISSING_METRICS_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("mkt_channel", StringType(), nullable=True),
        StructField("sessions", LongType(), nullable=True),
        StructField("converted_sessions", LongType(), nullable=True),
        StructField("attributed_net_revenue_eur", DecimalType(12, 2), nullable=True),
        StructField("spend_eur", DecimalType(12, 2), nullable=True),
    ]
)

CREATE_DATE_KEY_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("mkt_channel", StringType(), nullable=True),
        StructField("sessions", LongType(), nullable=True),
        StructField("converted_sessions", LongType(), nullable=True),
        StructField("attributed_net_revenue_eur", DecimalType(12, 2), nullable=True),
        StructField("spend_eur", DecimalType(12, 2), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
    ]
)

CREATE_CONVERSION_RATE_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("mkt_channel", StringType(), nullable=True),
        StructField("sessions", LongType(), nullable=True),
        StructField("converted_sessions", LongType(), nullable=True),
        StructField("attributed_net_revenue_eur", DecimalType(12, 2), nullable=True),
        StructField("spend_eur", DecimalType(12, 2), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("conversion_rate", DecimalType(5, 4), nullable=True),
    ]
)


def test_aggregate_sessions(spark: SparkSession) -> None:
    """aggregate_sessions returns exactly its reviewed expected rows."""
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert inputs.RENAME_MARKETING_CHANNEL_REVIEWED, review_message(
        "fact_session",
        "RENAME_MARKETING_CHANNEL",
    )
    assert inputs.CONVERT_SPEND_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "marketing_spend",
        "CONVERT_SPEND_AMOUNT_TO_EUROS",
    )
    assert expected.AGGREGATE_SESSIONS_REVIEWED, review_message(
        "kpi_channel_performance",
        "AGGREGATE_SESSIONS",
    )
    df_actual = job.aggregate_sessions(
        rows_to_df(spark, inputs.FACT_SESSION_ROWS, schema_definitions.fact_session_schema),
    )
    df_expected = rows_to_df(spark, expected.AGGREGATE_SESSIONS_ROWS, AGGREGATE_SESSIONS_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_filter_completed_orders(spark: SparkSession) -> None:
    """filter_completed_orders returns exactly its reviewed expected rows."""
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert inputs.RENAME_MARKETING_CHANNEL_REVIEWED, review_message(
        "fact_session",
        "RENAME_MARKETING_CHANNEL",
    )
    assert inputs.CONVERT_SPEND_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "marketing_spend",
        "CONVERT_SPEND_AMOUNT_TO_EUROS",
    )
    assert expected.FILTER_COMPLETED_ORDERS_REVIEWED, review_message(
        "kpi_channel_performance",
        "FILTER_COMPLETED_ORDERS",
    )
    df_actual = job.filter_completed_orders(
        rows_to_df(spark, inputs.FACT_ORDER_LINE_ROWS, schema_definitions.fact_order_line_schema),
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


def test_aggregate_marketing_spend(spark: SparkSession) -> None:
    """aggregate_marketing_spend returns exactly its reviewed expected rows."""
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert inputs.RENAME_MARKETING_CHANNEL_REVIEWED, review_message(
        "fact_session",
        "RENAME_MARKETING_CHANNEL",
    )
    assert inputs.CONVERT_SPEND_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "marketing_spend",
        "CONVERT_SPEND_AMOUNT_TO_EUROS",
    )
    assert expected.AGGREGATE_MARKETING_SPEND_REVIEWED, review_message(
        "kpi_channel_performance",
        "AGGREGATE_MARKETING_SPEND",
    )
    df_actual = job.aggregate_marketing_spend(
        rows_to_df(spark, inputs.MARKETING_SPEND_ROWS, schema_definitions.marketing_spend_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.AGGREGATE_MARKETING_SPEND_ROWS,
        AGGREGATE_MARKETING_SPEND_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_aggregate_attributed_revenue(spark: SparkSession) -> None:
    """aggregate_attributed_revenue returns exactly its reviewed expected rows."""
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert inputs.RENAME_MARKETING_CHANNEL_REVIEWED, review_message(
        "fact_session",
        "RENAME_MARKETING_CHANNEL",
    )
    assert inputs.CONVERT_SPEND_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "marketing_spend",
        "CONVERT_SPEND_AMOUNT_TO_EUROS",
    )
    assert expected.AGGREGATE_ATTRIBUTED_REVENUE_REVIEWED, review_message(
        "kpi_channel_performance",
        "AGGREGATE_ATTRIBUTED_REVENUE",
    )
    df_actual = job.aggregate_attributed_revenue(
        rows_to_df(spark, expected.FILTER_COMPLETED_ORDERS_ROWS, FILTER_COMPLETED_ORDERS_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.AGGREGATE_ATTRIBUTED_REVENUE_ROWS,
        AGGREGATE_ATTRIBUTED_REVENUE_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_combine_channel_metrics(spark: SparkSession) -> None:
    """combine_channel_metrics returns exactly its reviewed expected rows."""
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert inputs.RENAME_MARKETING_CHANNEL_REVIEWED, review_message(
        "fact_session",
        "RENAME_MARKETING_CHANNEL",
    )
    assert inputs.CONVERT_SPEND_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "marketing_spend",
        "CONVERT_SPEND_AMOUNT_TO_EUROS",
    )
    assert expected.COMBINE_CHANNEL_METRICS_REVIEWED, review_message(
        "kpi_channel_performance",
        "COMBINE_CHANNEL_METRICS",
    )
    df_actual = job.combine_channel_metrics(
        rows_to_df(spark, expected.AGGREGATE_SESSIONS_ROWS, AGGREGATE_SESSIONS_SCHEMA),
        rows_to_df(
            spark,
            expected.AGGREGATE_ATTRIBUTED_REVENUE_ROWS,
            AGGREGATE_ATTRIBUTED_REVENUE_SCHEMA,
        ),
        rows_to_df(
            spark,
            expected.AGGREGATE_MARKETING_SPEND_ROWS,
            AGGREGATE_MARKETING_SPEND_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.COMBINE_CHANNEL_METRICS_ROWS,
        COMBINE_CHANNEL_METRICS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_fill_missing_metrics(spark: SparkSession) -> None:
    """fill_missing_metrics returns exactly its reviewed expected rows."""
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert inputs.RENAME_MARKETING_CHANNEL_REVIEWED, review_message(
        "fact_session",
        "RENAME_MARKETING_CHANNEL",
    )
    assert inputs.CONVERT_SPEND_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "marketing_spend",
        "CONVERT_SPEND_AMOUNT_TO_EUROS",
    )
    assert expected.FILL_MISSING_METRICS_REVIEWED, review_message(
        "kpi_channel_performance",
        "FILL_MISSING_METRICS",
    )
    df_actual = job.fill_missing_metrics(
        rows_to_df(spark, expected.COMBINE_CHANNEL_METRICS_ROWS, COMBINE_CHANNEL_METRICS_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.FILL_MISSING_METRICS_ROWS, FILL_MISSING_METRICS_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_date_key(spark: SparkSession) -> None:
    """create_date_key returns exactly its reviewed expected rows."""
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert inputs.RENAME_MARKETING_CHANNEL_REVIEWED, review_message(
        "fact_session",
        "RENAME_MARKETING_CHANNEL",
    )
    assert inputs.CONVERT_SPEND_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "marketing_spend",
        "CONVERT_SPEND_AMOUNT_TO_EUROS",
    )
    assert expected.CREATE_DATE_KEY_REVIEWED, review_message(
        "kpi_channel_performance",
        "CREATE_DATE_KEY",
    )
    df_actual = job.create_date_key(
        rows_to_df(spark, expected.FILL_MISSING_METRICS_ROWS, FILL_MISSING_METRICS_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.CREATE_DATE_KEY_ROWS, CREATE_DATE_KEY_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_conversion_rate(spark: SparkSession) -> None:
    """create_conversion_rate returns exactly its reviewed expected rows."""
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert inputs.RENAME_MARKETING_CHANNEL_REVIEWED, review_message(
        "fact_session",
        "RENAME_MARKETING_CHANNEL",
    )
    assert inputs.CONVERT_SPEND_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "marketing_spend",
        "CONVERT_SPEND_AMOUNT_TO_EUROS",
    )
    assert expected.CREATE_CONVERSION_RATE_REVIEWED, review_message(
        "kpi_channel_performance",
        "CREATE_CONVERSION_RATE",
    )
    df_actual = job.create_conversion_rate(
        rows_to_df(spark, expected.CREATE_DATE_KEY_ROWS, CREATE_DATE_KEY_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_CONVERSION_RATE_ROWS,
        CREATE_CONVERSION_RATE_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_return_on_ad_spend(spark: SparkSession) -> None:
    """create_return_on_ad_spend returns exactly its reviewed expected rows."""
    assert inputs.CREATE_DATE_KEY_REVIEWED, review_message("fact_order_line", "CREATE_DATE_KEY")
    assert inputs.RENAME_MARKETING_CHANNEL_REVIEWED, review_message(
        "fact_session",
        "RENAME_MARKETING_CHANNEL",
    )
    assert inputs.CONVERT_SPEND_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "marketing_spend",
        "CONVERT_SPEND_AMOUNT_TO_EUROS",
    )
    assert expected.CREATE_RETURN_ON_AD_SPEND_REVIEWED, review_message(
        "kpi_channel_performance",
        "CREATE_RETURN_ON_AD_SPEND",
    )
    df_actual = job.create_return_on_ad_spend(
        rows_to_df(spark, expected.CREATE_CONVERSION_RATE_ROWS, CREATE_CONVERSION_RATE_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_RETURN_ON_AD_SPEND_ROWS,
        schema_definitions.kpi_channel_performance_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
