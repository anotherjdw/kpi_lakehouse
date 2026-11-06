"""Unit tests of the marketing_spend transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kpi_lakehouse.jobs.marketing_spend import (
    marketing_spend_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.marketing_spend import expected, inputs
from tests.helpers import (
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
JOIN_FX_RATES_SCHEMA = StructType(
    [
        StructField("rate_date", DateType(), nullable=True),
        StructField("quote_currency", StringType(), nullable=True),
        StructField("rate", DecimalType(18, 8), nullable=True),
        StructField("spend_date", DateType(), nullable=True),
        StructField("platform", StringType(), nullable=True),
        StructField("campaign_id", StringType(), nullable=True),
        StructField("campaign_name", StringType(), nullable=True),
        StructField("utm_source", StringType(), nullable=True),
        StructField("utm_medium", StringType(), nullable=True),
        StructField("utm_campaign", StringType(), nullable=True),
        StructField("impressions", LongType(), nullable=True),
        StructField("clicks", LongType(), nullable=True),
        StructField("spend_amount", DecimalType(12, 2), nullable=True),
        StructField("currency_code", StringType(), nullable=True),
        StructField("reported_at", TimestampType(), nullable=True),
    ]
)

KEEP_LATEST_RESTATEMENT_SCHEMA = StructType(
    [
        StructField("rate_date", DateType(), nullable=True),
        StructField("quote_currency", StringType(), nullable=True),
        StructField("rate", DecimalType(18, 8), nullable=True),
        StructField("spend_date", DateType(), nullable=True),
        StructField("platform", StringType(), nullable=True),
        StructField("campaign_id", StringType(), nullable=True),
        StructField("campaign_name", StringType(), nullable=True),
        StructField("utm_source", StringType(), nullable=True),
        StructField("utm_medium", StringType(), nullable=True),
        StructField("utm_campaign", StringType(), nullable=True),
        StructField("impressions", LongType(), nullable=True),
        StructField("clicks", LongType(), nullable=True),
        StructField("spend_amount", DecimalType(12, 2), nullable=True),
        StructField("currency_code", StringType(), nullable=True),
        StructField("reported_at", TimestampType(), nullable=True),
    ]
)


def test_join_fx_rates(spark: SparkSession) -> None:
    """join_fx_rates returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("marketing_spend")
    assert inputs.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
    assert expected.JOIN_FX_RATES_REVIEWED, review_message("marketing_spend", "JOIN_FX_RATES")
    df_actual = job.join_fx_rates(
        rows_to_df(spark, inputs.FX_RATES_ROWS, schema_definitions.fx_rates_schema),
        rows_to_df(
            spark,
            inputs.MARKETING_SPEND_ROWS,
            schema_definitions.marketing_spend_raw_schema,
        ),
    )
    df_expected = rows_to_df(spark, expected.JOIN_FX_RATES_ROWS, JOIN_FX_RATES_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_keep_latest_restatement(spark: SparkSession) -> None:
    """keep_latest_restatement returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("marketing_spend")
    assert inputs.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
    assert expected.KEEP_LATEST_RESTATEMENT_REVIEWED, review_message(
        "marketing_spend",
        "KEEP_LATEST_RESTATEMENT",
    )
    df_actual = job.keep_latest_restatement(
        rows_to_df(spark, expected.JOIN_FX_RATES_ROWS, JOIN_FX_RATES_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.KEEP_LATEST_RESTATEMENT_ROWS,
        KEEP_LATEST_RESTATEMENT_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_convert_spend_amount_to_euros(spark: SparkSession) -> None:
    """convert_spend_amount_to_euros returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("marketing_spend")
    assert inputs.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
    assert expected.CONVERT_SPEND_AMOUNT_TO_EUROS_REVIEWED, review_message(
        "marketing_spend",
        "CONVERT_SPEND_AMOUNT_TO_EUROS",
    )
    df_actual = job.convert_spend_amount_to_euros(
        rows_to_df(spark, expected.KEEP_LATEST_RESTATEMENT_ROWS, KEEP_LATEST_RESTATEMENT_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CONVERT_SPEND_AMOUNT_TO_EUROS_ROWS,
        schema_definitions.marketing_spend_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
