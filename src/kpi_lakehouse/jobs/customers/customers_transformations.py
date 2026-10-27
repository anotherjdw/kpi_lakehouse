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
    """Add a boolean column indicating the start of a new version for tracked fields.

    Compares tracked fields (customer_segment, country_code, marketing_opt_in) against
    their lagged values within each customer's history ordered by snapshot_date. A row is
    marked as a version start when any tracked field changes (including null-to-value
    transitions) or when it is the customer's first recorded row.

    Args:
        customers: DataFrame containing customer snapshot records with fields including
            customer_id, snapshot_date, customer_segment, country_code, and
            marketing_opt_in.

    Returns:
        DataFrame with all original columns plus is_version_start (boolean), which is
        True when the tracked fields differ from the previous snapshot for that customer.
    """
    tracked = ["customer_segment", "country_code", "marketing_opt_in"]
    window = Window.partitionBy("customer_id").orderBy("snapshot_date")
    current_struct = F.struct(*[F.col(c) for c in tracked])
    lagged_struct = F.lag(current_struct).over(window)
    is_version_start = ~current_struct.eqNullSafe(lagged_struct) | lagged_struct.isNull()
    return customers.withColumn("is_version_start", is_version_start)


def filter_version_starts(create_tracked_columns_result: DataFrame) -> DataFrame:
    """Filter rows where is_version_start is True and drop the is_version_start column.

    Args:
        create_tracked_columns_result: DataFrame containing customer snapshot data
            with an is_version_start boolean column indicating version boundaries.

    Returns:
        DataFrame containing only rows where is_version_start is True,
        with the is_version_start column removed.
    """
    return create_tracked_columns_result.filter(F.col("is_version_start")).drop("is_version_start")


def add_validity_window(filter_version_starts_result: DataFrame) -> DataFrame:
    """Add valid_from and valid_to columns defining each record's validity window.

    Computes valid_from as the snapshot_date and valid_to as the next snapshot_date
    for the same customer, using a window function partitioned by customer_id and
    ordered by snapshot_date.

    Args:
        filter_version_starts_result: DataFrame containing customer snapshot records
            with snapshot_date and customer_id columns among others.

    Returns:
        DataFrame with the original columns plus valid_from (equal to snapshot_date)
        and valid_to (equal to the next snapshot_date for the same customer, or null
        if there is no subsequent record).
    """
    window_spec = Window.partitionBy("customer_id").orderBy("snapshot_date")
    return filter_version_starts_result.withColumn("valid_from", F.col("snapshot_date")).withColumn(
        "valid_to", F.lead("snapshot_date").over(window_spec)
    )


def create_customer_version_id(add_validity_window_result: DataFrame) -> DataFrame:
    """Creates a SHA256-hashed customer_version_id column from customer_id and valid_from.

    Args:
        add_validity_window_result: DataFrame containing customer records with validity
            window columns, including customer_id and valid_from.

    Returns:
        DataFrame with all original columns plus a new customer_version_id column
        containing the SHA256 hash of the concatenation of customer_id and valid_from.
    """
    return add_validity_window_result.withColumn(
        "customer_version_id",
        F.sha2(F.concat(F.col("customer_id"), F.col("valid_from").cast("string")), 256),
    )


def pseudonymise_email_data(create_customer_version_id_result: DataFrame) -> DataFrame:
    """Replaces email values with their SHA256 hashed equivalents.

    Args:
        create_customer_version_id_result: DataFrame containing customer data
            with an email column to be pseudonymised.

    Returns:
        DataFrame with the same schema where the email column values have been
        replaced with their SHA256 hashed representations.
    """
    return create_customer_version_id_result.withColumn("email", F.sha2(F.col("email"), 256))


def drop_pii_columns(pseudonymise_email_data_result: DataFrame) -> DataFrame:
    """Drop personally identifiable columns from the customer DataFrame.

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
            a date_of_birth column from which the birth year is extracted.

    Returns:
        DataFrame with a new integer birth_year column derived from date_of_birth,
        and the date_of_birth column removed.
    """
    return drop_pii_columns_result.withColumn(
        "birth_year", F.year(F.col("date_of_birth")).cast("int")
    ).drop("date_of_birth")


def shorten_postal_code(create_birth_year_result: DataFrame) -> DataFrame:
    """Shorten postal_code values to their first two characters.

    Args:
        create_birth_year_result: DataFrame containing customer records with a
            postal_code column among other fields.

    Returns:
        DataFrame with the same schema where postal_code values are truncated
        to the first two characters.
    """
    return create_birth_year_result.withColumn(
        "postal_code", F.substring(F.col("postal_code"), 1, 2)
    )


def remove_deleted_customer_records(shorten_postal_code_result: DataFrame) -> DataFrame:
    """Remove PII from customers whose latest record is marked as deleted.

    Identifies customers where the open-ended record (valid_to is null) has
    is_deleted set to true, then nullifies email, postal_code, birth_year, and
    country_code for all versions of those customers. All other columns remain
    unchanged.

    Args:
        shorten_postal_code_result: DataFrame containing customer records with
            SCD2-style versioning, including is_deleted and valid_to flags.

    Returns:
        DataFrame with the same schema where PII columns are nulled out for
        customers whose current record (valid_to is null) is marked deleted.
    """
    deleted_customers = (
        shorten_postal_code_result.filter(F.col("valid_to").isNull() & F.col("is_deleted"))
        .select("customer_id")
        .distinct()
    )
    joined = shorten_postal_code_result.join(
        deleted_customers.withColumn("_is_deleted_customer", F.lit(True)),
        on="customer_id",
        how="left",
    )
    result = joined.select(
        F.col("snapshot_date"),
        F.col("customer_id"),
        F.when(F.col("_is_deleted_customer"), F.lit(None).cast("string"))
        .otherwise(F.col("email"))
        .alias("email"),
        F.col("signup_ts"),
        F.when(F.col("_is_deleted_customer"), F.lit(None).cast("string"))
        .otherwise(F.col("country_code"))
        .alias("country_code"),
        F.when(F.col("_is_deleted_customer"), F.lit(None).cast("string"))
        .otherwise(F.col("postal_code"))
        .alias("postal_code"),
        F.col("customer_segment"),
        F.col("marketing_opt_in"),
        F.col("is_deleted"),
        F.col("valid_from"),
        F.col("valid_to"),
        F.col("customer_version_id"),
        F.when(F.col("_is_deleted_customer"), F.lit(None).cast("int"))
        .otherwise(F.col("birth_year"))
        .alias("birth_year"),
    )
    return result


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
