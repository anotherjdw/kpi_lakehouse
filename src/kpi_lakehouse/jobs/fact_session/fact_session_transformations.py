"""Transformations and run steps of the fact_session ETL job.

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


def join_customer_data(sessions: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """Left join dim_customer onto sessions using customer_id and date range validity.

    Joins the customer dimension table to session data based on customer_id and
    the session's event_date falling within the customer record's valid_from/valid_to
    range. Sessions without a matching customer record retain a null customer_version_id.

    Args:
        sessions: DataFrame containing session-level event data with customer_id
            and event_date fields.
        dim_customer: DataFrame containing versioned customer dimension records
            with validity date ranges (valid_from, valid_to).

    Returns:
        DataFrame with the original session columns plus customer_version_id from
        the matched dim_customer record, or null if no match exists.
    """
    join_condition = (
        (sessions["customer_id"] == dim_customer["customer_id"])
        & (dim_customer["valid_from"] <= sessions["event_date"])
        & (dim_customer["valid_to"].isNull() | (sessions["event_date"] < dim_customer["valid_to"]))
    )
    result = (
        sessions.join(
            dim_customer.select("customer_id", "customer_version_id", "valid_from", "valid_to"),
            on=join_condition,
            how="left",
        )
        .drop(dim_customer["customer_id"])
        .drop("valid_from", "valid_to")
        .select(
            sessions["session_id"],
            sessions["event_date"],
            sessions["first_event_time"],
            sessions["last_event_time"],
            sessions["first_mkt_channel"],
            sessions["customer_id"],
            sessions["last_event_type"],
            sessions["order_id"],
            F.col("customer_version_id"),
        )
    )
    return result


def create_date_key(join_customer_data_result: DataFrame) -> DataFrame:
    """Creates a date_key column from the event_date column.

    Formats event_date as yyyyMMdd and casts it to an integer, appending
    the result as a new date_key column to the input DataFrame.

    Args:
        join_customer_data_result: DataFrame containing session and customer
            event data including an event_date column of type date.

    Returns:
        DataFrame with all original columns plus a date_key column of type
        int representing event_date formatted as yyyyMMdd.
    """
    return join_customer_data_result.withColumn(
        "date_key", F.date_format(F.col("event_date"), "yyyyMMdd").cast("int")
    )


def create_converted_customer_flag(create_date_key_result: DataFrame) -> DataFrame:
    """Creates a boolean flag indicating whether a customer has converted.

    Adds a converted_customer column to the input DataFrame that is True when
    order_id is not null, and False otherwise.

    Args:
        create_date_key_result: DataFrame containing session and customer event
            data including an order_id column.

    Returns:
        DataFrame with all original columns plus a converted_customer boolean
        column that is True when order_id is not null, False otherwise.
    """
    return create_date_key_result.withColumn("converted_customer", F.col("order_id").isNotNull())


def rename_marketing_channel(create_converted_customer_flag_result: DataFrame) -> DataFrame:
    """Renames the first_mkt_channel column to mkt_channel in the input DataFrame.

    Args:
        create_converted_customer_flag_result: DataFrame containing session and marketing
            channel data with a first_mkt_channel column.

    Returns:
        DataFrame with first_mkt_channel renamed to mkt_channel and all other columns
        retained in the specified output schema order.
    """
    return create_converted_customer_flag_result.withColumnRenamed(
        "first_mkt_channel", "mkt_channel"
    ).select(
        "session_id",
        "event_date",
        "first_event_time",
        "last_event_time",
        "customer_id",
        "last_event_type",
        "order_id",
        "customer_version_id",
        "date_key",
        "converted_customer",
        "mkt_channel",
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--SESSIONS_INPUT_PATH", type=str)
    parser.add_argument("--DIM_CUSTOMER_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--SESSIONS_LOOKBACK_DAYS", type=int)
    parser.add_argument("--SESSIONS_START_DATE", type=str)
    parser.add_argument("--SESSIONS_END_DATE", type=str)
    parser.add_argument("--DIM_CUSTOMER_LOOKBACK_DAYS", type=int)
    parser.add_argument("--DIM_CUSTOMER_START_DATE", type=str)
    parser.add_argument("--DIM_CUSTOMER_END_DATE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "sessions": create_pyspark_dataframe(
            spark=spark,
            file_path=args.SESSIONS_INPUT_PATH,
            schema=schema_definitions.sessions_schema,
            file_format="parquet",
            date_ranges=processing_dates["sessions"],
            partition_key="event_date",
        ),
        "dim_customer": create_pyspark_dataframe(
            spark=spark,
            file_path=args.DIM_CUSTOMER_INPUT_PATH,
            schema=schema_definitions.dim_customer_schema,
            file_format="parquet",
            date_ranges=processing_dates["dim_customer"],
            partition_key="valid_from",
        ),
    }
    if sources["sessions"].isEmpty():
        raise ValueError(
            f"Source 'sessions' read no rows for "
            f"{processing_dates['sessions']}. "
            "To allow this, set allow_empty: true on this source in "
            "fact_session.yaml."
        )
    if sources["dim_customer"].isEmpty():
        raise ValueError(
            f"Source 'dim_customer' read no rows for "
            f"{processing_dates['dim_customer']}. "
            "To allow this, set allow_empty: true on this source in "
            "fact_session.yaml."
        )
    return sources


def transform(sessions: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    join_customer_data_result = join_customer_data(sessions, dim_customer)
    create_date_key_result = create_date_key(join_customer_data_result)
    create_converted_customer_flag_result = create_converted_customer_flag(create_date_key_result)
    rename_marketing_channel_result = rename_marketing_channel(
        create_converted_customer_flag_result,
    )
    return rename_marketing_channel_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.fact_session_schema,
            unique_key=["session_id", "event_date"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key="event_date",
        )
        # Register after a successful write, while the DataFrame is still cached.
        register_partitions(
            data=output,
            table=output_table,
            partition_key="event_date",
            output_file_path=output_file_path,
        )
    finally:
        output.unpersist()
