"""Transformations and run steps of the dim_customer ETL job.

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


def create_valid_to_column(customers: DataFrame) -> DataFrame:
    """Create a valid_to column based on the lead of valid_from per customer.

    Computes the valid_to date for each customer record by taking the lead value
    of valid_from within a window partitioned by customer_id and ordered by
    valid_from, leaving the most recent record with a null valid_to.

    Args:
        customers: DataFrame containing customer records with valid_from and
            customer_id columns, among others.

    Returns:
        DataFrame with the same schema as the input plus a valid_to date column
        representing the start date of the next version for each customer, or
        null for the latest version.
    """
    window = Window.partitionBy(F.col("customer_id")).orderBy(F.col("valid_from"))
    return customers.withColumn("valid_to", F.lead(F.col("valid_from")).over(window))


def create_is_current_flag(create_valid_to_column_result: DataFrame) -> DataFrame:
    """Creates an is_current boolean flag column based on the valid_to column.

    Args:
        create_valid_to_column_result: DataFrame containing SCD2-style customer
            records with a valid_to date column indicating record expiry.

    Returns:
        DataFrame with an additional is_current boolean column that is True
        when valid_to is null, and False otherwise.
    """
    return create_valid_to_column_result.withColumn(
        "is_current", F.when(F.col("valid_to").isNull(), F.lit(True)).otherwise(F.lit(False))
    )


def select_dimensions(create_is_current_flag_result: DataFrame) -> DataFrame:
    """Select dimension columns from the is_current flag result DataFrame.

    Selects a subset of customer dimension columns, dropping snapshot and
    personally identifiable fields while retaining versioning and validity
    metadata.

    Args:
        create_is_current_flag_result: DataFrame containing customer snapshot
            data with SCD2 validity and currency flags.

    Returns:
        DataFrame containing only the selected dimension columns with the
        required output schema.
    """
    return create_is_current_flag_result.select(
        F.col("valid_from"),
        F.col("customer_id"),
        F.col("country_code"),
        F.col("postal_code"),
        F.col("customer_segment"),
        F.col("marketing_opt_in"),
        F.col("customer_version_id"),
        F.col("birth_year"),
        F.col("valid_to"),
        F.col("is_current"),
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--CUSTOMERS_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--CUSTOMERS_LOOKBACK_DAYS", type=int)
    parser.add_argument("--CUSTOMERS_START_DATE", type=str)
    parser.add_argument("--CUSTOMERS_END_DATE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "customers": create_pyspark_dataframe(
            spark=spark,
            file_path=args.CUSTOMERS_INPUT_PATH,
            schema=schema_definitions.customers_schema,
            file_format="parquet",
            date_ranges=processing_dates["customers"],
            partition_key="snapshot_date",
        ),
    }
    if sources["customers"].isEmpty():
        raise ValueError(
            f"Source 'customers' read no rows for "
            f"{processing_dates['customers']}. "
            "To allow this, set allow_empty: true on this source in "
            "dim_customer.yaml."
        )
    return sources


def transform(customers: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    create_valid_to_column_result = create_valid_to_column(customers)
    create_is_current_flag_result = create_is_current_flag(create_valid_to_column_result)
    select_dimensions_result = select_dimensions(create_is_current_flag_result)
    return select_dimensions_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.dim_customer_schema,
            unique_key=["valid_from", "customer_id"],
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
