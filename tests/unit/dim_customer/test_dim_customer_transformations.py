"""Unit tests of the dim_customer transformations.

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

from kpi_lakehouse.jobs.dim_customer import (
    dim_customer_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.dim_customer import expected, inputs
from tests.helpers import (
    column_types,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
CREATE_VALID_TO_COLUMN_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("signup_ts", TimestampType(), nullable=True),
        StructField("country_code", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=True),
        StructField("marketing_opt_in", BooleanType(), nullable=True),
        StructField("is_deleted", BooleanType(), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("birth_year", IntegerType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
    ]
)

CREATE_IS_CURRENT_FLAG_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("signup_ts", TimestampType(), nullable=True),
        StructField("country_code", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=True),
        StructField("marketing_opt_in", BooleanType(), nullable=True),
        StructField("is_deleted", BooleanType(), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
        StructField("birth_year", IntegerType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
        StructField("is_current", BooleanType(), nullable=True),
    ]
)


def test_create_valid_to_column(spark: SparkSession) -> None:
    """create_valid_to_column returns exactly its reviewed expected rows."""
    assert inputs.REMOVE_DELETED_CUSTOMER_RECORDS_REVIEWED, review_message(
        "customers",
        "REMOVE_DELETED_CUSTOMER_RECORDS",
    )
    assert expected.CREATE_VALID_TO_COLUMN_REVIEWED, review_message(
        "dim_customer",
        "CREATE_VALID_TO_COLUMN",
    )
    df_actual = job.create_valid_to_column(
        rows_to_df(spark, inputs.CUSTOMERS_ROWS, schema_definitions.customers_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_VALID_TO_COLUMN_ROWS,
        CREATE_VALID_TO_COLUMN_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_is_current_flag(spark: SparkSession) -> None:
    """create_is_current_flag returns exactly its reviewed expected rows."""
    assert inputs.REMOVE_DELETED_CUSTOMER_RECORDS_REVIEWED, review_message(
        "customers",
        "REMOVE_DELETED_CUSTOMER_RECORDS",
    )
    assert expected.CREATE_IS_CURRENT_FLAG_REVIEWED, review_message(
        "dim_customer",
        "CREATE_IS_CURRENT_FLAG",
    )
    df_actual = job.create_is_current_flag(
        rows_to_df(spark, expected.CREATE_VALID_TO_COLUMN_ROWS, CREATE_VALID_TO_COLUMN_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_IS_CURRENT_FLAG_ROWS,
        CREATE_IS_CURRENT_FLAG_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_select_dimensions(spark: SparkSession) -> None:
    """select_dimensions returns exactly its reviewed expected rows."""
    assert inputs.REMOVE_DELETED_CUSTOMER_RECORDS_REVIEWED, review_message(
        "customers",
        "REMOVE_DELETED_CUSTOMER_RECORDS",
    )
    assert expected.SELECT_DIMENSIONS_REVIEWED, review_message("dim_customer", "SELECT_DIMENSIONS")
    df_actual = job.select_dimensions(
        rows_to_df(spark, expected.CREATE_IS_CURRENT_FLAG_ROWS, CREATE_IS_CURRENT_FLAG_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.SELECT_DIMENSIONS_ROWS,
        schema_definitions.dim_customer_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
