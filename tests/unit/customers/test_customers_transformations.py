"""Unit tests of the customers transformations.

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

from kpi_lakehouse.jobs.customers import (
    customers_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.customers import expected, inputs
from tests.helpers import (
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
)

# The output of each transformation that another one takes as input: the columns the job
# spec gives it, all nullable, since the type check below ignores nullable.
CREATE_TRACKED_COLUMNS_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("date_of_birth", DateType(), nullable=True),
        StructField("signup_ts", TimestampType(), nullable=True),
        StructField("country_code", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=True),
        StructField("marketing_opt_in", BooleanType(), nullable=True),
        StructField("is_deleted", BooleanType(), nullable=True),
        StructField("is_version_start", BooleanType(), nullable=True),
    ]
)

FILTER_VERSION_STARTS_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("date_of_birth", DateType(), nullable=True),
        StructField("signup_ts", TimestampType(), nullable=True),
        StructField("country_code", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=True),
        StructField("marketing_opt_in", BooleanType(), nullable=True),
        StructField("is_deleted", BooleanType(), nullable=True),
    ]
)

ADD_VALIDITY_WINDOW_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("date_of_birth", DateType(), nullable=True),
        StructField("signup_ts", TimestampType(), nullable=True),
        StructField("country_code", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=True),
        StructField("marketing_opt_in", BooleanType(), nullable=True),
        StructField("is_deleted", BooleanType(), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
    ]
)

CREATE_CUSTOMER_VERSION_ID_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("date_of_birth", DateType(), nullable=True),
        StructField("signup_ts", TimestampType(), nullable=True),
        StructField("country_code", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=True),
        StructField("marketing_opt_in", BooleanType(), nullable=True),
        StructField("is_deleted", BooleanType(), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
    ]
)

PSEUDONYMISE_EMAIL_DATA_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("date_of_birth", DateType(), nullable=True),
        StructField("signup_ts", TimestampType(), nullable=True),
        StructField("country_code", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=True),
        StructField("marketing_opt_in", BooleanType(), nullable=True),
        StructField("is_deleted", BooleanType(), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
    ]
)

DROP_PII_COLUMNS_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("email", StringType(), nullable=True),
        StructField("date_of_birth", DateType(), nullable=True),
        StructField("signup_ts", TimestampType(), nullable=True),
        StructField("country_code", StringType(), nullable=True),
        StructField("postal_code", StringType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=True),
        StructField("marketing_opt_in", BooleanType(), nullable=True),
        StructField("is_deleted", BooleanType(), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
        StructField("customer_version_id", StringType(), nullable=True),
    ]
)

CREATE_BIRTH_YEAR_SCHEMA = StructType(
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
    ]
)

SHORTEN_POSTAL_CODE_SCHEMA = StructType(
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
    ]
)


def test_create_tracked_columns(spark: SparkSession) -> None:
    """create_tracked_columns returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("customers")
    assert expected.CREATE_TRACKED_COLUMNS_REVIEWED, review_message(
        "customers",
        "CREATE_TRACKED_COLUMNS",
    )
    df_actual = job.create_tracked_columns(
        rows_to_df(spark, inputs.CUSTOMERS_ROWS, schema_definitions.customers_raw_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_TRACKED_COLUMNS_ROWS,
        CREATE_TRACKED_COLUMNS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_filter_version_starts(spark: SparkSession) -> None:
    """filter_version_starts returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("customers")
    assert expected.FILTER_VERSION_STARTS_REVIEWED, review_message(
        "customers",
        "FILTER_VERSION_STARTS",
    )
    df_actual = job.filter_version_starts(
        rows_to_df(spark, expected.CREATE_TRACKED_COLUMNS_ROWS, CREATE_TRACKED_COLUMNS_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.FILTER_VERSION_STARTS_ROWS,
        FILTER_VERSION_STARTS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_add_validity_window(spark: SparkSession) -> None:
    """add_validity_window returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("customers")
    assert expected.ADD_VALIDITY_WINDOW_REVIEWED, review_message("customers", "ADD_VALIDITY_WINDOW")
    df_actual = job.add_validity_window(
        rows_to_df(spark, expected.FILTER_VERSION_STARTS_ROWS, FILTER_VERSION_STARTS_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.ADD_VALIDITY_WINDOW_ROWS, ADD_VALIDITY_WINDOW_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_customer_version_id(spark: SparkSession) -> None:
    """create_customer_version_id returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("customers")
    assert expected.CREATE_CUSTOMER_VERSION_ID_REVIEWED, review_message(
        "customers",
        "CREATE_CUSTOMER_VERSION_ID",
    )
    df_actual = job.create_customer_version_id(
        rows_to_df(spark, expected.ADD_VALIDITY_WINDOW_ROWS, ADD_VALIDITY_WINDOW_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_CUSTOMER_VERSION_ID_ROWS,
        CREATE_CUSTOMER_VERSION_ID_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_pseudonymise_email_data(spark: SparkSession) -> None:
    """pseudonymise_email_data returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("customers")
    assert expected.PSEUDONYMISE_EMAIL_DATA_REVIEWED, review_message(
        "customers",
        "PSEUDONYMISE_EMAIL_DATA",
    )
    df_actual = job.pseudonymise_email_data(
        rows_to_df(
            spark,
            expected.CREATE_CUSTOMER_VERSION_ID_ROWS,
            CREATE_CUSTOMER_VERSION_ID_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.PSEUDONYMISE_EMAIL_DATA_ROWS,
        PSEUDONYMISE_EMAIL_DATA_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_drop_pii_columns(spark: SparkSession) -> None:
    """drop_pii_columns returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("customers")
    assert expected.DROP_PII_COLUMNS_REVIEWED, review_message("customers", "DROP_PII_COLUMNS")
    df_actual = job.drop_pii_columns(
        rows_to_df(spark, expected.PSEUDONYMISE_EMAIL_DATA_ROWS, PSEUDONYMISE_EMAIL_DATA_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.DROP_PII_COLUMNS_ROWS, DROP_PII_COLUMNS_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_birth_year(spark: SparkSession) -> None:
    """create_birth_year returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("customers")
    assert expected.CREATE_BIRTH_YEAR_REVIEWED, review_message("customers", "CREATE_BIRTH_YEAR")
    df_actual = job.create_birth_year(
        rows_to_df(spark, expected.DROP_PII_COLUMNS_ROWS, DROP_PII_COLUMNS_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.CREATE_BIRTH_YEAR_ROWS, CREATE_BIRTH_YEAR_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_shorten_postal_code(spark: SparkSession) -> None:
    """shorten_postal_code returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("customers")
    assert expected.SHORTEN_POSTAL_CODE_REVIEWED, review_message("customers", "SHORTEN_POSTAL_CODE")
    df_actual = job.shorten_postal_code(
        rows_to_df(spark, expected.CREATE_BIRTH_YEAR_ROWS, CREATE_BIRTH_YEAR_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.SHORTEN_POSTAL_CODE_ROWS, SHORTEN_POSTAL_CODE_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_remove_deleted_customer_records(spark: SparkSession) -> None:
    """remove_deleted_customer_records returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("customers")
    assert expected.REMOVE_DELETED_CUSTOMER_RECORDS_REVIEWED, review_message(
        "customers",
        "REMOVE_DELETED_CUSTOMER_RECORDS",
    )
    df_actual = job.remove_deleted_customer_records(
        rows_to_df(spark, expected.SHORTEN_POSTAL_CODE_ROWS, SHORTEN_POSTAL_CODE_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.REMOVE_DELETED_CUSTOMER_RECORDS_ROWS,
        schema_definitions.customers_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
