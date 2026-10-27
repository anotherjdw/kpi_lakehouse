"""Unit tests of the fx_rates transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from kpi_lakehouse.jobs.fx_rates import (
    fx_rates_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.fx_rates import expected, inputs
from tests.helpers import (
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
JOIN_FX_RATES_TO_DATE_SCHEMA = StructType(
    [
        StructField("rate_date", DateType(), nullable=True),
        StructField("base_currency", StringType(), nullable=True),
        StructField("quote_currency", StringType(), nullable=True),
        StructField("rate", DecimalType(18, 8), nullable=True),
        StructField("date", DateType(), nullable=True),
    ]
)

FILL_WEEKENDS_AND_HOLIDAYS_SCHEMA = StructType(
    [
        StructField("rate_date", DateType(), nullable=True),
        StructField("base_currency", StringType(), nullable=True),
        StructField("quote_currency", StringType(), nullable=True),
        StructField("rate", DecimalType(18, 8), nullable=True),
        StructField("date", DateType(), nullable=True),
    ]
)


def test_join_fx_rates_to_date(spark: SparkSession) -> None:
    """join_fx_rates_to_date returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("fx_rates")
    assert expected.JOIN_FX_RATES_TO_DATE_REVIEWED, review_message(
        "fx_rates",
        "JOIN_FX_RATES_TO_DATE",
    )
    df_actual = job.join_fx_rates_to_date(
        rows_to_df(spark, inputs.FX_RATES_ROWS, schema_definitions.fx_rates_raw_schema),
        rows_to_df(spark, inputs.DATES_ROWS, schema_definitions.dates_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.JOIN_FX_RATES_TO_DATE_ROWS,
        JOIN_FX_RATES_TO_DATE_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_fill_weekends_and_holidays(spark: SparkSession) -> None:
    """fill_weekends_and_holidays returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("fx_rates")
    assert expected.FILL_WEEKENDS_AND_HOLIDAYS_REVIEWED, review_message(
        "fx_rates",
        "FILL_WEEKENDS_AND_HOLIDAYS",
    )
    df_actual = job.fill_weekends_and_holidays(
        rows_to_df(spark, expected.JOIN_FX_RATES_TO_DATE_ROWS, JOIN_FX_RATES_TO_DATE_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.FILL_WEEKENDS_AND_HOLIDAYS_ROWS,
        FILL_WEEKENDS_AND_HOLIDAYS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_coalesce_rate_date(spark: SparkSession) -> None:
    """coalesce_rate_date returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("fx_rates")
    assert expected.COALESCE_RATE_DATE_REVIEWED, review_message("fx_rates", "COALESCE_RATE_DATE")
    df_actual = job.coalesce_rate_date(
        rows_to_df(
            spark,
            expected.FILL_WEEKENDS_AND_HOLIDAYS_ROWS,
            FILL_WEEKENDS_AND_HOLIDAYS_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.COALESCE_RATE_DATE_ROWS,
        schema_definitions.fx_rates_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
