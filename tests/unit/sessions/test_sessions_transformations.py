"""Unit tests of the sessions transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession

from kpi_lakehouse.jobs.sessions import (
    sessions_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.sessions import expected, inputs
from tests.helpers import (
    column_types,
    review_message,
    row_key,
    rows_to_df,
)


def test_aggregate_session_events(spark: SparkSession) -> None:
    """aggregate_session_events returns exactly its reviewed expected rows."""
    assert inputs.DERIVE_MARKETING_CHANNEL_REVIEWED, review_message(
        "session_events",
        "DERIVE_MARKETING_CHANNEL",
    )
    assert expected.AGGREGATE_SESSION_EVENTS_REVIEWED, review_message(
        "sessions",
        "AGGREGATE_SESSION_EVENTS",
    )
    df_actual = job.aggregate_session_events(
        rows_to_df(spark, inputs.SESSION_EVENTS_ROWS, schema_definitions.session_events_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.AGGREGATE_SESSION_EVENTS_ROWS,
        schema_definitions.sessions_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
