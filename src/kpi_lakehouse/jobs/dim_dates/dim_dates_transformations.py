"""Transformations and run steps of the dim_dates ETL job.

parse_arguments, read, transform and write are scaffold-owned: `generate create-job` renders
them from the job spec, and `generate codegen` never changes them. Every other function is a
transformation whose body `generate codegen` writes.
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kpi_lakehouse.utils.dataframes import (
    create_pyspark_dataframe,
    validate_pyspark_dataframe,
    write_pyspark_dataframe,
)
from kpi_lakehouse.utils.schema import schema_definitions
from kpi_lakehouse.utils.spark import spark_session


def create_date_key(dates: DataFrame) -> DataFrame:
    """Converts each date into an integer in yyyymmdd format to use as the table's primary key.

    Args:
        dates: DataFrame containing a date column of type date.

    Returns:
        DataFrame with the original date column and a new date_key column
        containing the date represented as an integer in yyyymmdd format.
    """
    return dates.withColumn("date_key", F.date_format(F.col("date"), "yyyyMMdd").cast("int"))


def create_year_column(create_date_key_result: DataFrame) -> DataFrame:
    """Extracts the year from the date column and adds it as a new integer column.

    Args:
        create_date_key_result: DataFrame containing date and date_key columns.

    Returns:
        DataFrame with the original columns plus a year column extracted from date.
    """
    return create_date_key_result.withColumn("year", F.year(F.col("date")).cast("int"))


def create_quarter_column(create_year_column_result: DataFrame) -> DataFrame:
    """Create a quarter column derived from the date column.

    Args:
        create_year_column_result: DataFrame containing date, date_key, and year columns.

    Returns:
        DataFrame with an additional quarter column representing the quarter of the year (1 to 4).
    """
    return create_year_column_result.withColumn("quarter", F.quarter(F.col("date")))


def create_month_column(create_quarter_column_result: DataFrame) -> DataFrame:
    """Create a month column derived from the date column.

    Args:
        create_quarter_column_result: DataFrame containing date, date_key, year,
            and quarter columns.

    Returns:
        DataFrame with an additional month column representing the month of the
        date value, ranging from 1 to 12.
    """
    return create_quarter_column_result.withColumn("month", F.month(F.col("date")))


def create_iso_week_column(create_month_column_result: DataFrame) -> DataFrame:
    """Creates an iso_week column containing the ISO 8601 week of the year.

    Args:
        create_month_column_result: DataFrame containing date, date_key, year,
            quarter, and month columns.

    Returns:
        DataFrame with an additional iso_week column representing the ISO 8601
        week number of the year derived from the date column.
    """
    return create_month_column_result.withColumn("iso_week", F.weekofyear(F.col("date")))


def create_weekday_column(create_iso_week_column_result: DataFrame) -> DataFrame:
    """Creates a weekday column representing the ISO day of the week.

    Adds a weekday column derived from the date column, where weekday values
    range from 1 (Monday) to 7 (Sunday), following the ISO day of the week
    standard.

    Args:
        create_iso_week_column_result: DataFrame containing date, date_key,
            year, quarter, month, and iso_week columns.

    Returns:
        DataFrame with an additional weekday column representing the ISO day
        of the week (1=Monday, 7=Sunday).
    """
    return create_iso_week_column_result.withColumn("weekday", F.weekday(F.col("date")) + 1)


def create_weekend_flag(create_weekday_column_result: DataFrame) -> DataFrame:
    """Create a boolean flag indicating whether a date falls on a weekend.

    Args:
        create_weekday_column_result: DataFrame containing date dimension columns
            including a weekday column with ISO weekday numbers (1=Monday, 7=Sunday).

    Returns:
        DataFrame with all input columns plus an is_weekend boolean column that
        is True when weekday is 6 (Saturday) or 7 (Sunday), otherwise False.
    """
    return create_weekday_column_result.withColumn("is_weekend", F.col("weekday").isin(6, 7))


def create_month_start_column(create_weekend_flag_result: DataFrame) -> DataFrame:
    """Creates a month_start column representing the first day of the month for each date.

    Args:
        create_weekend_flag_result: DataFrame containing date and related calendar columns,
            including a date column from which the month start date is derived.

    Returns:
        DataFrame with all original columns plus a month_start column of type date,
        representing the first day of the month for each corresponding date value.
    """
    return create_weekend_flag_result.withColumn("month_start", F.trunc(F.col("date"), "month"))


def create_month_end_column(create_month_start_column_result: DataFrame) -> DataFrame:
    """Adds a month_end column representing the last date of the month for each row.

    Args:
        create_month_start_column_result: DataFrame containing date dimension columns
            including a date column and month_start column.

    Returns:
        DataFrame with an additional month_end column of type date, representing
        the last calendar date of the month corresponding to each row's date value.
    """
    return create_month_start_column_result.withColumn("month_end", F.last_day(F.col("date")))


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--DATES_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "dates": create_pyspark_dataframe(
            spark=spark,
            file_path=args.DATES_INPUT_PATH,
            schema=schema_definitions.dates_schema,
            file_format="json",
        ),
    }
    if sources["dates"].isEmpty():
        raise ValueError(
            "Source 'dates' read no rows from its whole dataset. "
            "To allow this, set allow_empty: true on this source in "
            "dim_dates.yaml."
        )
    return sources


def transform(dates: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    create_date_key_result = create_date_key(dates)
    create_year_column_result = create_year_column(create_date_key_result)
    create_quarter_column_result = create_quarter_column(create_year_column_result)
    create_month_column_result = create_month_column(create_quarter_column_result)
    create_iso_week_column_result = create_iso_week_column(create_month_column_result)
    create_weekday_column_result = create_weekday_column(create_iso_week_column_result)
    create_weekend_flag_result = create_weekend_flag(create_weekday_column_result)
    create_month_start_column_result = create_month_start_column(create_weekend_flag_result)
    create_month_end_column_result = create_month_end_column(create_month_start_column_result)
    return create_month_end_column_result


def write(output: DataFrame, output_file_path: str) -> None:
    """Check the output against its contract, then write it as parquet."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.dim_dates_schema,
            unique_key=["date"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key=None,
        )
    finally:
        output.unpersist()
