"""Transformations and run steps of the categories ETL job.

parse_arguments, read, transform and write are scaffold-owned: `generate create-job` renders
them from the job spec, and `generate codegen` never changes them. Every other function is a
transformation whose body `generate codegen` writes.
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kpi_lakehouse.utils.catalog import register_partitions
from kpi_lakehouse.utils.dataframes import (
    create_pyspark_dataframe,
    validate_pyspark_dataframe,
    write_pyspark_dataframe,
)
from kpi_lakehouse.utils.schema import schema_definitions
from kpi_lakehouse.utils.spark import spark_session


def flatten_category_tree(categories: DataFrame) -> DataFrame:
    """Resolve each category's ancestor path from root to itself across up to four levels.

    Performs iterative self-joins on parent_category_id to walk up the category
    tree, assigning each ancestor's category_id to the appropriate level column.
    Raises a RuntimeError if any parent_category_id references a missing category
    or if a cycle is detected in the tree.

    Args:
        categories: DataFrame containing category hierarchy data with columns
            snapshot_date, category_id, category_name, parent_category_id,
            is_active, and updated_at.

    Returns:
        DataFrame with the original columns plus category_id_level_1 through
        category_id_level_4, where level_1 is the root ancestor and the
        category's own id fills the column at its depth; deeper levels are null.
    """
    all_ids = categories.select(F.col("category_id")).distinct()
    non_root = categories.filter(F.col("parent_category_id").isNotNull())
    missing_parents = non_root.join(
        all_ids, non_root["parent_category_id"] == all_ids["category_id"], "left_anti"
    )
    missing_count = missing_parents.count()
    if missing_count > 0:
        raise RuntimeError(f"Found {missing_count} category(ies) with missing parent references.")
    base = categories.select(
        F.col("category_id").alias("id"), F.col("parent_category_id").alias("p1")
    )
    lookup = categories.select(
        F.col("category_id").alias("lid"), F.col("parent_category_id").alias("lp")
    )
    step2 = base.join(lookup, base["p1"] == lookup["lid"], "left").select(
        F.col("id"), F.col("p1"), F.col("lp").alias("p2")
    )
    step3 = step2.join(lookup, step2["p2"] == lookup["lid"], "left").select(
        F.col("id"), F.col("p1"), F.col("p2"), F.col("lp").alias("p3")
    )
    step4 = step3.join(lookup, step3["p3"] == lookup["lid"], "left").select(
        F.col("id"), F.col("p1"), F.col("p2"), F.col("p3"), F.col("lp").alias("p4")
    )
    cycle_check = step4.filter(
        (F.col("p1") == F.col("id"))
        | (F.col("p2") == F.col("id"))
        | (F.col("p3") == F.col("id"))
        | (F.col("p4") == F.col("id"))
    )
    cycle_count = cycle_check.count()
    if cycle_count > 0:
        raise RuntimeError(f"Cycle detected in category tree for {cycle_count} category(ies).")
    deep_check = step4.filter(F.col("p4").isNotNull())
    deep_count = deep_check.count()
    if deep_count > 0:
        raise RuntimeError(
            f"Category tree exceeds 4 levels for {deep_count} category(ies); "
            f"possible cycle or unsupported depth."
        )
    levels = step4.select(
        F.col("id"),
        F.when(F.col("p1").isNull(), F.col("id"))
        .when(F.col("p2").isNull(), F.col("p1"))
        .when(F.col("p3").isNull(), F.col("p2"))
        .otherwise(F.col("p3"))
        .alias("category_id_level_1"),
        F.when(F.col("p1").isNull(), F.lit(None).cast("string"))
        .when(F.col("p2").isNull(), F.col("id"))
        .when(F.col("p3").isNull(), F.col("p1"))
        .otherwise(F.col("p2"))
        .alias("category_id_level_2"),
        F.when(F.col("p2").isNull(), F.lit(None).cast("string"))
        .when(F.col("p3").isNull(), F.col("id"))
        .otherwise(F.col("p1"))
        .alias("category_id_level_3"),
        F.when(F.col("p3").isNull(), F.lit(None).cast("string"))
        .otherwise(F.col("id"))
        .alias("category_id_level_4"),
    )
    result = categories.join(levels, categories["category_id"] == levels["id"], "left").select(
        F.col("snapshot_date"),
        F.col("category_id"),
        F.col("category_name"),
        F.col("parent_category_id"),
        F.col("is_active"),
        F.col("updated_at"),
        F.col("category_id_level_1"),
        F.col("category_id_level_2"),
        F.col("category_id_level_3"),
        F.col("category_id_level_4"),
    )
    return result


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--CATEGORIES_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
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
        "categories": create_pyspark_dataframe(
            spark=spark,
            file_path=args.CATEGORIES_INPUT_PATH,
            schema=schema_definitions.categories_raw_schema,
            file_format="json",
            date_ranges=processing_dates["categories"],
            partition_key="snapshot_date",
        ),
    }
    if sources["categories"].isEmpty():
        raise ValueError(
            f"Source 'categories' read no rows for "
            f"{processing_dates['categories']}. "
            "To allow this, set allow_empty: true on this source in "
            "categories.yaml."
        )
    return sources


def transform(categories: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    flatten_category_tree_result = flatten_category_tree(categories)
    return flatten_category_tree_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.categories_schema,
            unique_key=["category_id"],
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
