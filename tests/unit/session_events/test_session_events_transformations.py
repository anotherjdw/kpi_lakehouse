"""Unit tests of the session_events transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kpi_lakehouse.jobs.session_events import (
    session_events_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.session_events import expected, inputs
from tests.helpers import (
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
DEDUPLICATE_SESSION_DATA_SCHEMA = StructType(
    [
        StructField("event_date", DateType(), nullable=True),
        StructField("event_id", StringType(), nullable=True),
        StructField("event_ts", TimestampType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("anonymous_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("order_id", StringType(), nullable=True),
        StructField("utm_source", StringType(), nullable=True),
        StructField("utm_medium", StringType(), nullable=True),
        StructField("utm_campaign", StringType(), nullable=True),
        StructField("referrer_domain", StringType(), nullable=True),
        StructField("device_type", StringType(), nullable=True),
        StructField("country_code", StringType(), nullable=True),
    ]
)


def test_deduplicate_session_data(spark: SparkSession) -> None:
    """deduplicate_session_data returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("session_events")
    assert expected.DEDUPLICATE_SESSION_DATA_REVIEWED, review_message(
        "session_events",
        "DEDUPLICATE_SESSION_DATA",
    )
    df_actual = job.deduplicate_session_data(
        rows_to_df(spark, inputs.SESSION_EVENTS_ROWS, schema_definitions.session_events_raw_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.DEDUPLICATE_SESSION_DATA_ROWS,
        DEDUPLICATE_SESSION_DATA_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_derive_marketing_channel(spark: SparkSession) -> None:
    """derive_marketing_channel returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("session_events")
    assert expected.DERIVE_MARKETING_CHANNEL_REVIEWED, review_message(
        "session_events",
        "DERIVE_MARKETING_CHANNEL",
    )
    df_actual = job.derive_marketing_channel(
        rows_to_df(spark, expected.DEDUPLICATE_SESSION_DATA_ROWS, DEDUPLICATE_SESSION_DATA_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.DERIVE_MARKETING_CHANNEL_ROWS,
        schema_definitions.session_events_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
