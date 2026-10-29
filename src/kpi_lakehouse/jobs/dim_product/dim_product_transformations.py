"""Transformations and run steps of the dim_product ETL job.

parse_arguments, read, transform and write are scaffold-owned: `generate create-job` renders
them from the job spec, and `generate codegen` never changes them. Every other function is a
transformation whose body `generate codegen` writes.
"""

import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from kpi_lakehouse.utils.catalog import register_partitions
from kpi_lakehouse.utils.dataframes import (
    create_pyspark_dataframe,
    validate_pyspark_dataframe,
    write_pyspark_dataframe,
)
from kpi_lakehouse.utils.schema import schema_definitions
from kpi_lakehouse.utils.spark import spark_session


def create_valid_to_column(products: DataFrame) -> DataFrame:
    """Creates a valid_to column based on the lead of valid_from within each product partition.

    Computes the valid_to date for each product version by taking the lead value
    of valid_from partitioned by product_id and ordered by valid_from. The most
    recent version for each product will have a null valid_to value.

    Args:
        products: DataFrame containing product versioning data with valid_from
            and product_id columns among others.

    Returns:
        DataFrame with the same columns as the input plus a valid_to date column
        representing the start date of the next product version, or null for the
        latest version.
    """
    window = Window.partitionBy(F.col("product_id")).orderBy(F.col("valid_from"))
    return products.withColumn("valid_to", F.lead(F.col("valid_from")).over(window))


def filter_latest_category_snapshot(categories: DataFrame) -> DataFrame:
    """Filter categories to retain only rows from the latest snapshot date.

    Args:
        categories: DataFrame containing category snapshot data with a
            snapshot_date column used to identify the most recent snapshot.

    Returns:
        DataFrame containing only the rows whose snapshot_date equals the
        maximum snapshot_date present in the input DataFrame, with the same
        schema as the input.
    """
    max_snapshot_date = categories.agg(F.max("snapshot_date")).collect()[0][0]
    return categories.filter(F.col("snapshot_date") == max_snapshot_date)


def create_is_current_flag(create_valid_to_column_result: DataFrame) -> DataFrame:
    """Creates an is_current boolean flag based on the valid_to column.

    Args:
        create_valid_to_column_result: DataFrame containing product version records
            with a valid_to date column indicating record expiry.

    Returns:
        DataFrame with the snapshot_date column removed and an is_current boolean
        column added, where is_current is True when valid_to is null and False otherwise.
    """
    return create_valid_to_column_result.withColumn(
        "is_current", F.col("valid_to").isNull()
    ).select(
        "product_id",
        "sku",
        "product_name",
        "brand",
        "category_id",
        "list_price_eur",
        "cost_price_eur",
        "is_active",
        "launched_date",
        "valid_from",
        "product_version_id",
        "valid_to",
        "is_current",
    )


def select_category_path_dimensions(filter_latest_category_snapshot_result: DataFrame) -> DataFrame:
    """Select category path dimension columns from the category snapshot DataFrame.

    Args:
        filter_latest_category_snapshot_result: DataFrame containing category snapshot data
            with hierarchical level columns.

    Returns:
        DataFrame containing only category_id and its four hierarchical level columns.
    """
    return filter_latest_category_snapshot_result.select(
        F.col("category_id"),
        F.col("category_id_level_1"),
        F.col("category_id_level_2"),
        F.col("category_id_level_3"),
        F.col("category_id_level_4"),
    )


def join_category_path(
    create_is_current_flag_result: DataFrame, select_category_path_dimensions_result: DataFrame
) -> DataFrame:
    """Join category path dimensions onto product data using a left join on category_id.

    Args:
        create_is_current_flag_result: DataFrame containing product records with
            versioning and currency flags.
        select_category_path_dimensions_result: DataFrame containing category
            hierarchy levels keyed by category_id.

    Returns:
        DataFrame with all product rows enriched with category path level columns,
        retaining products whose category_id has no matching category record.
    """
    return create_is_current_flag_result.join(
        select_category_path_dimensions_result, on="category_id", how="left"
    ).select(
        F.col("valid_from"),
        F.col("product_id"),
        F.col("sku"),
        F.col("product_name"),
        F.col("brand"),
        F.col("category_id"),
        F.col("list_price_eur"),
        F.col("cost_price_eur"),
        F.col("is_active"),
        F.col("launched_date"),
        F.col("product_version_id"),
        F.col("valid_to"),
        F.col("is_current"),
        F.col("category_id_level_1"),
        F.col("category_id_level_2"),
        F.col("category_id_level_3"),
        F.col("category_id_level_4"),
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--PRODUCTS_INPUT_PATH", type=str)
    parser.add_argument("--CATEGORIES_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--PRODUCTS_LOOKBACK_DAYS", type=int)
    parser.add_argument("--PRODUCTS_START_DATE", type=str)
    parser.add_argument("--PRODUCTS_END_DATE", type=str)
    parser.add_argument("--CATEGORIES_LOOKBACK_DAYS", type=int)
    parser.add_argument("--CATEGORIES_START_DATE", type=str)
    parser.add_argument("--CATEGORIES_END_DATE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "products": create_pyspark_dataframe(
            spark=spark,
            file_path=args.PRODUCTS_INPUT_PATH,
            schema=schema_definitions.products_schema,
            file_format="parquet",
            date_ranges=processing_dates["products"],
            partition_key="snapshot_date",
        ),
        "categories": create_pyspark_dataframe(
            spark=spark,
            file_path=args.CATEGORIES_INPUT_PATH,
            schema=schema_definitions.categories_schema,
            file_format="parquet",
            date_ranges=processing_dates["categories"],
            partition_key="snapshot_date",
        ),
    }
    if sources["products"].isEmpty():
        raise ValueError(
            f"Source 'products' read no rows for "
            f"{processing_dates['products']}. "
            "To allow this, set allow_empty: true on this source in "
            "dim_product.yaml."
        )
    if sources["categories"].isEmpty():
        raise ValueError(
            f"Source 'categories' read no rows for "
            f"{processing_dates['categories']}. "
            "To allow this, set allow_empty: true on this source in "
            "dim_product.yaml."
        )
    return sources


def transform(products: DataFrame, categories: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    create_valid_to_column_result = create_valid_to_column(products)
    filter_latest_category_snapshot_result = filter_latest_category_snapshot(categories)
    create_is_current_flag_result = create_is_current_flag(create_valid_to_column_result)
    select_category_path_dimensions_result = select_category_path_dimensions(
        filter_latest_category_snapshot_result,
    )
    join_category_path_result = join_category_path(
        create_is_current_flag_result,
        select_category_path_dimensions_result,
    )
    return join_category_path_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.dim_product_schema,
            unique_key=["product_id", "valid_from"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key="valid_from",
        )
        # Register after a successful write, while the DataFrame is still cached.
        register_partitions(
            data=output,
            table=output_table,
            partition_key="valid_from",
            output_file_path=output_file_path,
        )
    finally:
        output.unpersist()
