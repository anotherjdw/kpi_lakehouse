"""Transformations and run steps of the customers ETL job.

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


def create_tracked_columns(customers: DataFrame) -> DataFrame:
    """Add a boolean flag indicating the start of a new version for tracked fields.

    Compares a struct of tracked fields (customer_segment, country_code,
    marketing_opt_in, is_deleted) against the previous row per customer
    (ordered by snapshot_date) using eqNullSafe. The flag is True when the
    tracked struct differs from the lagged struct, including the first row per
    customer and null-to-value transitions.

    Args:
        customers: DataFrame containing customer snapshot records with fields
            including snapshot_date, customer_id, customer_segment,
            country_code, marketing_opt_in, and is_deleted.

    Returns:
        DataFrame with all original columns plus is_version_start (boolean),
        which is True when any tracked field has changed from the previous
        snapshot for that customer.
    """
    tracked_cols = ["customer_segment", "country_code", "marketing_opt_in", "is_deleted"]
    window = Window.partitionBy("customer_id").orderBy("snapshot_date")
    current_struct = F.struct(*[F.col(c) for c in tracked_cols])
    lagged_struct = F.lag(current_struct).over(window)
    is_version_start = ~current_struct.eqNullSafe(lagged_struct)
    return customers.withColumn("is_version_start", is_version_start)


def filter_version_starts(create_tracked_columns_result: DataFrame) -> DataFrame:
    """Filter rows where is_version_start is true and remove version-start metadata column.

    Keeps only rows where is_version_start is True, drops the is_version_start column,
    and removes rows from the earliest snapshot_date unless the DataFrame contains
    only a single distinct snapshot_date.

    Args:
        create_tracked_columns_result: DataFrame containing customer snapshot data
            with an is_version_start boolean column indicating version boundaries.

    Returns:
        DataFrame with version-start rows only, is_version_start column dropped,
        and earliest snapshot_date rows excluded when multiple dates are present.
    """
    filtered = create_tracked_columns_result.filter(F.col("is_version_start")).drop(
        "is_version_start"
    )
    distinct_dates = create_tracked_columns_result.select("snapshot_date").distinct()
    date_count = distinct_dates.count()
    if date_count <= 1:
        return filtered
    min_date = distinct_dates.agg(F.min("snapshot_date")).collect()[0][0]
    return filtered.filter(F.col("snapshot_date") != min_date)


def add_validity_window(filter_version_starts_result: DataFrame) -> DataFrame:
    """Adds a valid_from column derived from snapshot_date.

    Args:
        filter_version_starts_result: DataFrame containing customer snapshot data
            with a snapshot_date column.

    Returns:
        DataFrame with an additional valid_from column set equal to snapshot_date.
    """
    return filter_version_starts_result.withColumn("valid_from", F.col("snapshot_date"))


def create_customer_version_id(add_validity_window_result: DataFrame) -> DataFrame:
    """Creates a SHA256-hashed customer_version_id column from customer_id and valid_from.

    Args:
        add_validity_window_result: DataFrame containing customer records with
            customer_id and valid_from columns among others.

    Returns:
        DataFrame with all original columns plus a new customer_version_id column
        containing the SHA256 hash of the concatenation of customer_id and valid_from.
    """
    return add_validity_window_result.withColumn(
        "customer_version_id",
        F.sha2(F.concat(F.col("customer_id"), F.col("valid_from").cast("string")), 256),
    )


def pseudonymise_email_data(create_customer_version_id_result: DataFrame) -> DataFrame:
    """Pseudonymise the email column by replacing it with its SHA256 hash.

    Args:
        create_customer_version_id_result: DataFrame containing customer data
            with an email column to be pseudonymised.

    Returns:
        DataFrame with the same schema where the email column values have been
        replaced with their SHA256 hashed equivalents.
    """
    return create_customer_version_id_result.withColumn("email", F.sha2(F.col("email"), 256))


def drop_pii_columns(pseudonymise_email_data_result: DataFrame) -> DataFrame:
    """Drops personally identifiable columns from the customer DataFrame.

    Args:
        pseudonymise_email_data_result: DataFrame containing customer data with
            PII columns including first_name, last_name, and city.

    Returns:
        DataFrame with first_name, last_name, and city columns removed.
    """
    return pseudonymise_email_data_result.drop("first_name", "last_name", "city")


def create_birth_year(drop_pii_columns_result: DataFrame) -> DataFrame:
    """Extracts birth year from date_of_birth and removes the original column.

    Args:
        drop_pii_columns_result: DataFrame containing customer data including
            a date_of_birth column from which the year is extracted.

    Returns:
        DataFrame with a new birth_year integer column derived from date_of_birth,
        and the date_of_birth column removed.
    """
    return drop_pii_columns_result.withColumn("birth_year", F.year(F.col("date_of_birth"))).drop(
        "date_of_birth"
    )


def shorten_postal_code(create_birth_year_result: DataFrame) -> DataFrame:
    """Shortens postal_code values to their first two characters.

    Args:
        create_birth_year_result: DataFrame containing customer records with
            a postal_code column among other fields.

    Returns:
        DataFrame with the same schema where postal_code values are truncated
        to the first two characters.
    """
    return create_birth_year_result.withColumn(
        "postal_code", F.substring(F.col("postal_code"), 1, 2)
    )


def remove_deleted_customer_records(shorten_postal_code_result: DataFrame) -> DataFrame:
    """Nulls PII fields for deleted customer records while preserving all other columns.

    For rows where is_deleted is true, sets email, postal_code, birth_year, and
    country_code to null. All other columns remain unchanged on every row.
    Rows where is_deleted is false are returned as-is.

    Args:
        shorten_postal_code_result: DataFrame containing customer records with an
            is_deleted flag and associated PII fields.

    Returns:
        DataFrame with the same schema where deleted records have their PII fields
        nulled out but all other fields preserved exactly as provided.
    """
    return (
        shorten_postal_code_result.withColumn(
            "email",
            F.when(F.col("is_deleted"), F.lit(None).cast("string")).otherwise(F.col("email")),
        )
        .withColumn(
            "postal_code",
            F.when(F.col("is_deleted"), F.lit(None).cast("string")).otherwise(F.col("postal_code")),
        )
        .withColumn(
            "birth_year",
            F.when(F.col("is_deleted"), F.lit(None).cast("int")).otherwise(F.col("birth_year")),
        )
        .withColumn(
            "country_code",
            F.when(F.col("is_deleted"), F.lit(None).cast("string")).otherwise(
                F.col("country_code")
            ),
        )
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
            schema=schema_definitions.customers_raw_schema,
            file_format="json",
            date_ranges=processing_dates["customers"],
            partition_key="snapshot_date",
        ),
    }
    if sources["customers"].isEmpty():
        raise ValueError(
            f"Source 'customers' read no rows for "
            f"{processing_dates['customers']}. "
            "To allow this, set allow_empty: true on this source in "
            "customers.yaml."
        )
    return sources


def transform(customers: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    create_tracked_columns_result = create_tracked_columns(customers)
    filter_version_starts_result = filter_version_starts(create_tracked_columns_result)
    add_validity_window_result = add_validity_window(filter_version_starts_result)
    create_customer_version_id_result = create_customer_version_id(add_validity_window_result)
    pseudonymise_email_data_result = pseudonymise_email_data(create_customer_version_id_result)
    drop_pii_columns_result = drop_pii_columns(pseudonymise_email_data_result)
    create_birth_year_result = create_birth_year(drop_pii_columns_result)
    shorten_postal_code_result = shorten_postal_code(create_birth_year_result)
    remove_deleted_customer_records_result = remove_deleted_customer_records(
        shorten_postal_code_result,
    )
    return remove_deleted_customer_records_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.customers_schema,
            unique_key=["snapshot_date", "customer_id"],
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
