"""Unit tests of the dim_product transformations.

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
    TimestampType,
)

from kpi_lakehouse.jobs.dim_product import (
    dim_product_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.dim_product import expected, inputs
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
        StructField("product_version_id", StringType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
    ]
)

FILTER_LATEST_CATEGORY_SNAPSHOT_SCHEMA = StructType(
    [
        StructField("snapshot_date", DateType(), nullable=True),
        StructField("category_id", StringType(), nullable=True),
        StructField("category_name", StringType(), nullable=True),
        StructField("parent_category_id", StringType(), nullable=True),
        StructField("is_active", BooleanType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=True),
        StructField("category_id_level_1", StringType(), nullable=True),
        StructField("category_id_level_2", StringType(), nullable=True),
        StructField("category_id_level_3", StringType(), nullable=True),
        StructField("category_id_level_4", StringType(), nullable=True),
    ]
)

CREATE_IS_CURRENT_FLAG_SCHEMA = StructType(
    [
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
        StructField("product_version_id", StringType(), nullable=True),
        StructField("valid_to", DateType(), nullable=True),
        StructField("is_current", BooleanType(), nullable=True),
    ]
)

SELECT_CATEGORY_PATH_DIMENSIONS_SCHEMA = StructType(
    [
        StructField("category_id", StringType(), nullable=True),
        StructField("category_id_level_1", StringType(), nullable=True),
        StructField("category_id_level_2", StringType(), nullable=True),
        StructField("category_id_level_3", StringType(), nullable=True),
        StructField("category_id_level_4", StringType(), nullable=True),
    ]
)


def test_create_valid_to_column(spark: SparkSession) -> None:
    """create_valid_to_column returns exactly its reviewed expected rows."""
    assert inputs.FLATTEN_CATEGORY_TREE_REVIEWED, review_message(
        "categories",
        "FLATTEN_CATEGORY_TREE",
    )
    assert inputs.CREATE_PRODUCT_VERSION_ID_REVIEWED, review_message(
        "products",
        "CREATE_PRODUCT_VERSION_ID",
    )
    assert expected.CREATE_VALID_TO_COLUMN_REVIEWED, review_message(
        "dim_product",
        "CREATE_VALID_TO_COLUMN",
    )
    df_actual = job.create_valid_to_column(
        rows_to_df(spark, inputs.PRODUCTS_ROWS, schema_definitions.products_schema),
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


def test_filter_latest_category_snapshot(spark: SparkSession) -> None:
    """filter_latest_category_snapshot returns exactly its reviewed expected rows."""
    assert inputs.FLATTEN_CATEGORY_TREE_REVIEWED, review_message(
        "categories",
        "FLATTEN_CATEGORY_TREE",
    )
    assert inputs.CREATE_PRODUCT_VERSION_ID_REVIEWED, review_message(
        "products",
        "CREATE_PRODUCT_VERSION_ID",
    )
    assert expected.FILTER_LATEST_CATEGORY_SNAPSHOT_REVIEWED, review_message(
        "dim_product",
        "FILTER_LATEST_CATEGORY_SNAPSHOT",
    )
    df_actual = job.filter_latest_category_snapshot(
        rows_to_df(spark, inputs.CATEGORIES_ROWS, schema_definitions.categories_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.FILTER_LATEST_CATEGORY_SNAPSHOT_ROWS,
        FILTER_LATEST_CATEGORY_SNAPSHOT_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_create_is_current_flag(spark: SparkSession) -> None:
    """create_is_current_flag returns exactly its reviewed expected rows."""
    assert inputs.FLATTEN_CATEGORY_TREE_REVIEWED, review_message(
        "categories",
        "FLATTEN_CATEGORY_TREE",
    )
    assert inputs.CREATE_PRODUCT_VERSION_ID_REVIEWED, review_message(
        "products",
        "CREATE_PRODUCT_VERSION_ID",
    )
    assert expected.CREATE_IS_CURRENT_FLAG_REVIEWED, review_message(
        "dim_product",
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


def test_select_category_path_dimensions(spark: SparkSession) -> None:
    """select_category_path_dimensions returns exactly its reviewed expected rows."""
    assert inputs.FLATTEN_CATEGORY_TREE_REVIEWED, review_message(
        "categories",
        "FLATTEN_CATEGORY_TREE",
    )
    assert inputs.CREATE_PRODUCT_VERSION_ID_REVIEWED, review_message(
        "products",
        "CREATE_PRODUCT_VERSION_ID",
    )
    assert expected.SELECT_CATEGORY_PATH_DIMENSIONS_REVIEWED, review_message(
        "dim_product",
        "SELECT_CATEGORY_PATH_DIMENSIONS",
    )
    df_actual = job.select_category_path_dimensions(
        rows_to_df(
            spark,
            expected.FILTER_LATEST_CATEGORY_SNAPSHOT_ROWS,
            FILTER_LATEST_CATEGORY_SNAPSHOT_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.SELECT_CATEGORY_PATH_DIMENSIONS_ROWS,
        SELECT_CATEGORY_PATH_DIMENSIONS_SCHEMA,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)


def test_join_category_path(spark: SparkSession) -> None:
    """join_category_path returns exactly its reviewed expected rows."""
    assert inputs.FLATTEN_CATEGORY_TREE_REVIEWED, review_message(
        "categories",
        "FLATTEN_CATEGORY_TREE",
    )
    assert inputs.CREATE_PRODUCT_VERSION_ID_REVIEWED, review_message(
        "products",
        "CREATE_PRODUCT_VERSION_ID",
    )
    assert expected.JOIN_CATEGORY_PATH_REVIEWED, review_message("dim_product", "JOIN_CATEGORY_PATH")
    df_actual = job.join_category_path(
        rows_to_df(spark, expected.CREATE_IS_CURRENT_FLAG_ROWS, CREATE_IS_CURRENT_FLAG_SCHEMA),
        rows_to_df(
            spark,
            expected.SELECT_CATEGORY_PATH_DIMENSIONS_ROWS,
            SELECT_CATEGORY_PATH_DIMENSIONS_SCHEMA,
        ),
    )
    df_expected = rows_to_df(
        spark,
        expected.JOIN_CATEGORY_PATH_ROWS,
        schema_definitions.dim_product_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
