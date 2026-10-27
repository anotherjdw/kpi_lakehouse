"""Unit tests of the categories transformations.

One test per transformation: it runs the transformation on its input rows and compares the
result with its reviewed expected rows, both from this job's modules in tests/fixtures/. An
input that is another transformation's output is that transformation's expected rows.
"""

from pyspark.sql import SparkSession

from kpi_lakehouse.jobs.categories import (
    categories_transformations as job,
)
from kpi_lakehouse.utils.schema import schema_definitions
from tests.fixtures.categories import expected, inputs
from tests.helpers import (
    column_types,
    inputs_message,
    review_message,
    row_key,
    rows_to_df,
)


def test_flatten_category_tree(spark: SparkSession) -> None:
    """flatten_category_tree returns exactly its reviewed expected rows."""
    assert inputs.INPUTS_FILLED, inputs_message("categories")
    assert expected.FLATTEN_CATEGORY_TREE_REVIEWED, review_message(
        "categories",
        "FLATTEN_CATEGORY_TREE",
    )
    df_actual = job.flatten_category_tree(
        rows_to_df(spark, inputs.CATEGORIES_ROWS, schema_definitions.categories_raw_schema),
    )
    df_expected = rows_to_df(
        spark,
        expected.FLATTEN_CATEGORY_TREE_ROWS,
        schema_definitions.categories_schema,
    )
    columns = df_expected.columns
    assert column_types(df_actual.schema) == column_types(df_expected.schema)
    actual = df_actual.select(columns).collect()
    expected_rows = df_expected.collect()
    assert sorted(actual, key=row_key) == sorted(expected_rows, key=row_key)
