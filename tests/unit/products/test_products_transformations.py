"""Unit tests of the products transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from kpi_lakehouse.jobs.products import (
    products_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.products import expected, inputs
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
        StructField("product_id", StringType(), nullable=True),
        StructField("sku", StringType(), nullable=True),
        StructField("product_name", StringType(), nullable=True),
        StructField("brand", StringType(), nullable=True),
        StructField("category_id", StringType(), nullable=True),
        StructField("list_price_eur", DecimalType(12, 2), nullable=True),
        StructField("cost_price_eur", DecimalType(12, 2), nullable=True),
        StructField("is_active", BooleanType(), nullable=True),
        StructField("launched_date", DateType(), nullable=True),
        StructField("is_version_start", BooleanType(), nullable=True),
    ]
)

FILTER_VERSION_STARTS_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("sku", StringType(), nullable=True),
        StructField("product_name", StringType(), nullable=True),
        StructField("brand", StringType(), nullable=True),
        StructField("category_id", StringType(), nullable=True),
        StructField("list_price_eur", DecimalType(12, 2), nullable=True),
        StructField("cost_price_eur", DecimalType(12, 2), nullable=True),
        StructField("is_active", BooleanType(), nullable=True),
        StructField("launched_date", DateType(), nullable=True),
    ]
)

ADD_VALIDITY_WINDOW_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("sku", StringType(), nullable=True),
        StructField("product_name", StringType(), nullable=True),
        StructField("brand", StringType(), nullable=True),
        StructField("category_id", StringType(), nullable=True),
        StructField("list_price_eur", DecimalType(12, 2), nullable=True),
        StructField("cost_price_eur", DecimalType(12, 2), nullable=True),
        StructField("is_active", BooleanType(), nullable=True),
        StructField("launched_date", DateType(), nullable=True),
        StructField("valid_from", DateType(), nullable=True),
    ]
)


def test_create_tracked_columns(spark: SparkSession) -> None:
    """create_tracked_columns returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("products")
    assert expected.CREATE_TRACKED_COLUMNS_REVIEWED, review_message(
        "products",
        "CREATE_TRACKED_COLUMNS",
    )
    df_actual = job.create_tracked_columns(
        rows_to_df(spark, inputs.PRODUCTS_ROWS, schema_definitions.products_raw_schema),
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
    assert inputs.INPUTS_FILLED, inputs_message("products")
    assert expected.FILTER_VERSION_STARTS_REVIEWED, review_message(
        "products",
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
    assert inputs.INPUTS_FILLED, inputs_message("products")
    assert expected.ADD_VALIDITY_WINDOW_REVIEWED, review_message("products", "ADD_VALIDITY_WINDOW")
    df_actual = job.add_validity_window(
        rows_to_df(spark, expected.FILTER_VERSION_STARTS_ROWS, FILTER_VERSION_STARTS_SCHEMA),
    )
    df_expected = rows_to_df(spark, expected.ADD_VALIDITY_WINDOW_ROWS, ADD_VALIDITY_WINDOW_SCHEMA)
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_product_version_id(spark: SparkSession) -> None:
    """create_product_version_id returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("products")
    assert expected.CREATE_PRODUCT_VERSION_ID_REVIEWED, review_message(
        "products",
        "CREATE_PRODUCT_VERSION_ID",
    )
    df_actual = job.create_product_version_id(
        rows_to_df(spark, expected.ADD_VALIDITY_WINDOW_ROWS, ADD_VALIDITY_WINDOW_SCHEMA),
    )
    df_expected = rows_to_df(
        spark,
        expected.CREATE_PRODUCT_VERSION_ID_ROWS,
        schema_definitions.products_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
