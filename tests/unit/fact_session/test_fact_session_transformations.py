"""Unit tests of the fact_session transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kpi_lakehouse.jobs.fact_session import (
    fact_session_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.fact_session import expected, inputs
from tests.helpers import (
    column_types,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
JOIN_CUSTOMER_DATA_SCHEMA = StructType(
    [
        StructField("session_id", StringType(), nullable=True),
        StructField("event_date", DateType(), nullable=True),
        StructField("first_event_time", TimestampType(), nullable=True),
        StructField("last_event_time", TimestampType(), nullable=True),
        StructField("first_mkt_channel", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("last_event_type", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
    ]
)

CREATE_DATE_KEY_SCHEMA = StructType(
    [
        StructField("session_id", StringType(), nullable=True),
        StructField("event_date", DateType(), nullable=True),
        StructField("first_event_time", TimestampType(), nullable=True),
        StructField("last_event_time", TimestampType(), nullable=True),
        StructField("first_mkt_channel", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("last_event_type", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
    ]
)

CREATE_CONVERTED_CUSTOMER_FLAG_SCHEMA = StructType(
    [
        StructField("session_id", StringType(), nullable=True),
        StructField("event_date", DateType(), nullable=True),
        StructField("first_event_time", TimestampType(), nullable=True),
        StructField("last_event_time", TimestampType(), nullable=True),
        StructField("first_mkt_channel", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("last_event_type", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("converted_customer", BooleanType(), nullable=True),
    ]
)


def test_join_customer_data(spark: SparkSession) -> None:
    """join_customer_data returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.JOIN_CUSTOMER_DATA_REVIEWED, review_message(
        "fact_session",
        "JOIN_CUSTOMER_DATA",
    )
    df_actual = job.join_customer_data(
        rows_to_df(spark, inputs.SESSIONS_ROWS, schema_definitions.sessions_schema),
        rows_to_df(spark, inputs.DIM_CUSTOMER_ROWS, schema_definitions.dim_customer_schema),
    )
    df_expected = rows_to_df(spark, expected.JOIN_CUSTOMER_DATA_ROWS, JOIN_CUSTOMER_DATA_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_date_key(spark: SparkSession) -> None:
    """create_date_key returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.CREATE_DATE_KEY_REVIEWED, review_message("fact_session", "CREATE_DATE_KEY")
    df_actual = job.create_date_key(
        rows_to_df(spark, expected.JOIN_CUSTOMER_DATA_ROWS, JOIN_CUSTOMER_DATA_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.CREATE_DATE_KEY_ROWS, CREATE_DATE_KEY_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_converted_customer_flag(spark: SparkSession) -> None:
    """create_converted_customer_flag returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.CREATE_CONVERTED_CUSTOMER_FLAG_REVIEWED, review_message(
        "fact_session",
        "CREATE_CONVERTED_CUSTOMER_FLAG",
    )
    df_actual = job.create_converted_customer_flag(
        rows_to_df(spark, expected.CREATE_DATE_KEY_ROWS, CREATE_DATE_KEY_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_CONVERTED_CUSTOMER_FLAG_ROWS,
        CREATE_CONVERTED_CUSTOMER_FLAG_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_rename_marketing_channel(spark: SparkSession) -> None:
    """rename_marketing_channel returns exactly its reviewed expected rows."""
    assert inputs.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    assert inputs.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    assert expected.RENAME_MARKETING_CHANNEL_REVIEWED, review_message(
        "fact_session",
        "RENAME_MARKETING_CHANNEL",
    )
    df_actual = job.rename_marketing_channel(
        rows_to_df(
            spark,
            expected.CREATE_CONVERTED_CUSTOMER_FLAG_ROWS,
            CREATE_CONVERTED_CUSTOMER_FLAG_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.RENAME_MARKETING_CHANNEL_ROWS,
        schema_definitions.fact_session_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
