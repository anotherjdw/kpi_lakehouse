"""Transformations and run steps of the products ETL job.

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


def create_tracked_columns(products: DataFrame) -> DataFrame:
    """Add a version-start flag based on changes in tracked columns per product.

    Compares a struct of tracked columns (list_price_eur, cost_price_eur,
    category_id, is_active) against the previous row's struct within each
    product's timeline ordered by snapshot_date. The first row per product and
    any row where at least one tracked column differs from the previous row
    (including null-to-value transitions) are marked as version starts.

    Args:
        products: DataFrame containing product snapshot data with columns
            snapshot_date, product_id, sku, product_name, brand, category_id,
            list_price_eur, cost_price_eur, is_active, and launched_date.

    Returns:
        DataFrame with all input columns plus is_version_start (boolean), which
        is True when the tracked columns differ from the previous snapshot for
        the same product or when it is the product's first snapshot row.
    """
    tracked_cols = ["list_price_eur", "cost_price_eur", "category_id", "is_active"]
    window = Window.partitionBy("product_id").orderBy("snapshot_date")
    current_struct = F.struct(*[F.col(c) for c in tracked_cols])
    prev_struct = F.lag(current_struct).over(window)
    is_version_start = ~current_struct.eqNullSafe(prev_struct)
    return products.withColumn("is_version_start", is_version_start)


def filter_version_starts(create_tracked_columns_result: DataFrame) -> DataFrame:
    """Filter rows where a new version starts, excluding the earliest snapshot date.

    Filters the DataFrame to retain only rows where `is_version_start` is True,
    drops the `is_version_start` column, then removes rows belonging to the
    earliest snapshot_date in the DataFrame — unless the DataFrame contains
    only a single distinct snapshot_date.

    Args:
        create_tracked_columns_result: DataFrame containing product snapshot data
            with an `is_version_start` boolean column indicating version boundaries.

    Returns:
        DataFrame with version-start rows only, without the `is_version_start`
        column, and with the earliest snapshot_date removed when multiple
        snapshot dates exist.
    """
    filtered = create_tracked_columns_result.filter(F.col("is_version_start")).drop(
        "is_version_start"
    )
    min_date = create_tracked_columns_result.agg(
        F.min("snapshot_date").alias("min_date")
    ).collect()[0]["min_date"]
    max_date = create_tracked_columns_result.agg(
        F.max("snapshot_date").alias("max_date")
    ).collect()[0]["max_date"]
    if min_date == max_date:
        return filtered
    return filtered.filter(F.col("snapshot_date") != F.lit(min_date))


def add_validity_window(filter_version_starts_result: DataFrame) -> DataFrame:
    """Adds a valid_from column derived from snapshot_date.

    Args:
        filter_version_starts_result: DataFrame containing product snapshot data
            with a snapshot_date column.

    Returns:
        DataFrame with an additional valid_from column set equal to snapshot_date.
    """
    return filter_version_starts_result.withColumn("valid_from", F.col("snapshot_date"))


def create_product_version_id(add_validity_window_result: DataFrame) -> DataFrame:
    """Creates a SHA256-hashed product_version_id column from product_id and valid_from.

    Args:
        add_validity_window_result: DataFrame containing product records with
            product_id and valid_from columns used to generate the version identifier.

    Returns:
        DataFrame with all original columns plus a product_version_id column
        containing the SHA256 hash of the concatenation of product_id and valid_from.
    """
    return add_validity_window_result.withColumn(
        "product_version_id",
        F.sha2(F.concat(F.col("product_id"), F.col("valid_from").cast("string")), 256),
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--PRODUCTS_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--PRODUCTS_LOOKBACK_DAYS", type=int)
    parser.add_argument("--PRODUCTS_START_DATE", type=str)
    parser.add_argument("--PRODUCTS_END_DATE", type=str)
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
            schema=schema_definitions.products_raw_schema,
            file_format="json",
            date_ranges=processing_dates["products"],
            partition_key="snapshot_date",
        ),
    }
    if sources["products"].isEmpty():
        raise ValueError(
            f"Source 'products' read no rows for "
            f"{processing_dates['products']}. "
            "To allow this, set allow_empty: true on this source in "
            "products.yaml."
        )
    return sources


def transform(products: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    create_tracked_columns_result = create_tracked_columns(products)
    filter_version_starts_result = filter_version_starts(create_tracked_columns_result)
    add_validity_window_result = add_validity_window(filter_version_starts_result)
    create_product_version_id_result = create_product_version_id(add_validity_window_result)
    return create_product_version_id_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.products_schema,
            unique_key=["snapshot_date", "product_id"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key="snapshot_date",
        )
        # Register after a successful write, while the DataFrame is still cached.
        register_partitions(
            data=output,
            table=output_table,
            partition_key="snapshot_date",
            output_file_path=output_file_path,
        )
    finally:
        output.unpersist()
