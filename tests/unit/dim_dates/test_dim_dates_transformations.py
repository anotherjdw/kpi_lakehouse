"""Unit tests of the dim_dates transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StructField,
    StructType,
)

from kpi_lakehouse.jobs.dim_dates import (
    dim_dates_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.dim_dates import expected, inputs
from tests.helpers import (
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
CREATE_DATE_KEY_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
    ]
)

CREATE_YEAR_COLUMN_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
    ]
)

CREATE_QUARTER_COLUMN_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
        StructField("quarter", IntegerType(), nullable=True),
    ]
)

CREATE_MONTH_COLUMN_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
        StructField("quarter", IntegerType(), nullable=True),
        StructField("month", IntegerType(), nullable=True),
    ]
)

CREATE_ISO_WEEK_COLUMN_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
        StructField("quarter", IntegerType(), nullable=True),
        StructField("month", IntegerType(), nullable=True),
        StructField("iso_week", IntegerType(), nullable=True),
    ]
)

CREATE_WEEKDAY_COLUMN_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
        StructField("quarter", IntegerType(), nullable=True),
        StructField("month", IntegerType(), nullable=True),
        StructField("iso_week", IntegerType(), nullable=True),
        StructField("weekday", IntegerType(), nullable=True),
    ]
)

CREATE_WEEKEND_FLAG_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
        StructField("quarter", IntegerType(), nullable=True),
        StructField("month", IntegerType(), nullable=True),
        StructField("iso_week", IntegerType(), nullable=True),
        StructField("weekday", IntegerType(), nullable=True),
        StructField("is_weekend", BooleanType(), nullable=True),
    ]
)

CREATE_MONTH_START_COLUMN_SCHEMA = StructType(
    [
        StructField("date", DateType(), nullable=True),
        StructField("date_key", IntegerType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
        StructField("quarter", IntegerType(), nullable=True),
        StructField("month", IntegerType(), nullable=True),
        StructField("iso_week", IntegerType(), nullable=True),
        StructField("weekday", IntegerType(), nullable=True),
        StructField("is_weekend", BooleanType(), nullable=True),
        StructField("month_start", DateType(), nullable=True),
    ]
)


def test_create_date_key(spark: SparkSession) -> None:
    """create_date_key returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_dates")
    assert expected.CREATE_DATE_KEY_REVIEWED, review_message("dim_dates", "CREATE_DATE_KEY")
    df_actual = job.create_date_key(
        rows_to_df(spark, inputs.DATES_ROWS, schema_definitions.dates_schema),
    )
    df_expected = rows_to_df(spark, expected.CREATE_DATE_KEY_ROWS, CREATE_DATE_KEY_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_year_column(spark: SparkSession) -> None:
    """create_year_column returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_dates")
    assert expected.CREATE_YEAR_COLUMN_REVIEWED, review_message("dim_dates", "CREATE_YEAR_COLUMN")
    df_actual = job.create_year_column(
        rows_to_df(spark, expected.CREATE_DATE_KEY_ROWS, CREATE_DATE_KEY_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.CREATE_YEAR_COLUMN_ROWS, CREATE_YEAR_COLUMN_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_quarter_column(spark: SparkSession) -> None:
    """create_quarter_column returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_dates")
    assert expected.CREATE_QUARTER_COLUMN_REVIEWED, review_message(
        "dim_dates",
        "CREATE_QUARTER_COLUMN",
    )
    df_actual = job.create_quarter_column(
        rows_to_df(spark, expected.CREATE_YEAR_COLUMN_ROWS, CREATE_YEAR_COLUMN_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_QUARTER_COLUMN_ROWS,
        CREATE_QUARTER_COLUMN_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_month_column(spark: SparkSession) -> None:
    """create_month_column returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_dates")
    assert expected.CREATE_MONTH_COLUMN_REVIEWED, review_message("dim_dates", "CREATE_MONTH_COLUMN")
    df_actual = job.create_month_column(
        rows_to_df(spark, expected.CREATE_QUARTER_COLUMN_ROWS, CREATE_QUARTER_COLUMN_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.CREATE_MONTH_COLUMN_ROWS, CREATE_MONTH_COLUMN_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_iso_week_column(spark: SparkSession) -> None:
    """create_iso_week_column returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_dates")
    assert expected.CREATE_ISO_WEEK_COLUMN_REVIEWED, review_message(
        "dim_dates",
        "CREATE_ISO_WEEK_COLUMN",
    )
    df_actual = job.create_iso_week_column(
        rows_to_df(spark, expected.CREATE_MONTH_COLUMN_ROWS, CREATE_MONTH_COLUMN_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_ISO_WEEK_COLUMN_ROWS,
        CREATE_ISO_WEEK_COLUMN_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_weekday_column(spark: SparkSession) -> None:
    """create_weekday_column returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_dates")
    assert expected.CREATE_WEEKDAY_COLUMN_REVIEWED, review_message(
        "dim_dates",
        "CREATE_WEEKDAY_COLUMN",
    )
    df_actual = job.create_weekday_column(
        rows_to_df(spark, expected.CREATE_ISO_WEEK_COLUMN_ROWS, CREATE_ISO_WEEK_COLUMN_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_WEEKDAY_COLUMN_ROWS,
        CREATE_WEEKDAY_COLUMN_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_weekend_flag(spark: SparkSession) -> None:
    """create_weekend_flag returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_dates")
    assert expected.CREATE_WEEKEND_FLAG_REVIEWED, review_message("dim_dates", "CREATE_WEEKEND_FLAG")
    df_actual = job.create_weekend_flag(
        rows_to_df(spark, expected.CREATE_WEEKDAY_COLUMN_ROWS, CREATE_WEEKDAY_COLUMN_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.CREATE_WEEKEND_FLAG_ROWS, CREATE_WEEKEND_FLAG_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_month_start_column(spark: SparkSession) -> None:
    """create_month_start_column returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_dates")
    assert expected.CREATE_MONTH_START_COLUMN_REVIEWED, review_message(
        "dim_dates",
        "CREATE_MONTH_START_COLUMN",
    )
    df_actual = job.create_month_start_column(
        rows_to_df(spark, expected.CREATE_WEEKEND_FLAG_ROWS, CREATE_WEEKEND_FLAG_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_MONTH_START_COLUMN_ROWS,
        CREATE_MONTH_START_COLUMN_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_month_end_column(spark: SparkSession) -> None:
    """create_month_end_column returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("dim_dates")
    assert expected.CREATE_MONTH_END_COLUMN_REVIEWED, review_message(
        "dim_dates",
        "CREATE_MONTH_END_COLUMN",
    )
    df_actual = job.create_month_end_column(
        rows_to_df(
            spark,
            expected.CREATE_MONTH_START_COLUMN_ROWS,
            CREATE_MONTH_START_COLUMN_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_MONTH_END_COLUMN_ROWS,
        schema_definitions.dim_dates_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
