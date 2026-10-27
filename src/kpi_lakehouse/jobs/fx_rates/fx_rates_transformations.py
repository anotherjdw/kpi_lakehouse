"""Transformations and run steps of the fx_rates ETL job.

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


def join_fx_rates_to_date(fx_rates: DataFrame, dates: DataFrame) -> DataFrame:
    """Left join FX rates to a date spine on matching dates.

    Args:
        fx_rates: DataFrame containing FX rate records with rate_date, base_currency,
            quote_currency, and rate columns.
        dates: DataFrame containing a date spine with a single date column.

    Returns:
        DataFrame with all FX rate columns plus the date column from the date spine,
        produced by a left join on dates.date == fx_rates.rate_date.
    """
    result = dates.join(fx_rates, dates["date"] == fx_rates["rate_date"], how="left")
    return result.select(
        fx_rates["rate_date"],
        fx_rates["base_currency"],
        fx_rates["quote_currency"],
        fx_rates["rate"],
        dates["date"],
    )


def fill_weekends_and_holidays(join_fx_rates_to_date_result: DataFrame) -> DataFrame:
    """Fill weekend and holiday gaps in FX rates by carrying forward the last known rate.

    Cross joins every distinct date with every distinct quote_currency, left joins
    published rates back on date and quote_currency, then forward-fills the rate
    column per currency ordered by date. Appends an EUR-to-EUR row per date with
    rate 1.0 and null rate_date.

    Args:
        join_fx_rates_to_date_result: DataFrame with columns rate_date, base_currency,
            quote_currency, rate, and date, where weekend/holiday dates appear as
            all-null rows from the upstream join.

    Returns:
        DataFrame with columns rate_date, base_currency, quote_currency, rate, and date,
        one row per date and currency pair, with gaps forward-filled and an EUR identity
        row appended per date.
    """
    distinct_dates = join_fx_rates_to_date_result.select(F.col("date")).distinct()
    distinct_currencies = (
        join_fx_rates_to_date_result.filter(F.col("quote_currency").isNotNull())
        .select(F.col("quote_currency"))
        .distinct()
    )
    full_grid = distinct_dates.crossJoin(distinct_currencies)
    published_rates = join_fx_rates_to_date_result.filter(F.col("rate_date").isNotNull()).select(
        "date", "quote_currency", "rate_date", "base_currency", "rate"
    )
    filled = full_grid.join(published_rates, on=["date", "quote_currency"], how="left")
    window_ffill = (
        Window.partitionBy("quote_currency")
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    filled = filled.withColumn("rate", F.last(F.col("rate"), ignorenulls=True).over(window_ffill))
    filled = filled.withColumn(
        "base_currency",
        F.when(F.col("base_currency").isNotNull(), F.col("base_currency")).otherwise(F.lit("EUR")),
    )
    filled = filled.select(
        F.col("rate_date"),
        F.col("base_currency"),
        F.col("quote_currency"),
        F.col("rate").cast("decimal(18,8)"),
        F.col("date"),
    )
    eur_rows = (
        distinct_dates.withColumn("rate_date", F.lit(None).cast("date"))
        .withColumn("base_currency", F.lit("EUR"))
        .withColumn("quote_currency", F.lit("EUR"))
        .withColumn("rate", F.lit(1.0).cast("decimal(18,8)"))
        .select("rate_date", "base_currency", "quote_currency", "rate", "date")
    )
    result = filled.unionByName(eur_rows)
    return result


def coalesce_rate_date(fill_weekends_and_holidays_result: DataFrame) -> DataFrame:
    """Coalesce rate_date and date columns into rate_date, dropping the date column.

    Args:
        fill_weekends_and_holidays_result: DataFrame containing rate_date, base_currency,
            quote_currency, rate, and date columns.

    Returns:
        DataFrame with rate_date coalesced from rate_date and date columns, with the
        date column dropped.
    """
    return fill_weekends_and_holidays_result.withColumn(
        "rate_date", F.coalesce(F.col("rate_date"), F.col("date"))
    ).drop("date")


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--FX_RATES_INPUT_PATH", type=str)
    parser.add_argument("--DATES_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--FX_RATES_LOOKBACK_DAYS", type=int)
    parser.add_argument("--FX_RATES_START_DATE", type=str)
    parser.add_argument("--FX_RATES_END_DATE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "fx_rates": create_pyspark_dataframe(
            spark=spark,
            file_path=args.FX_RATES_INPUT_PATH,
            schema=schema_definitions.fx_rates_raw_schema,
            file_format="json",
            date_ranges=processing_dates["fx_rates"],
            partition_key="rate_date",
        ),
        "dates": create_pyspark_dataframe(
            spark=spark,
            file_path=args.DATES_INPUT_PATH,
            schema=schema_definitions.dates_schema,
            file_format="json",
        ),
    }
    if sources["fx_rates"].isEmpty():
        raise ValueError(
            f"Source 'fx_rates' read no rows for "
            f"{processing_dates['fx_rates']}. "
            "To allow this, set allow_empty: true on this source in "
            "fx_rates.yaml."
        )
    if sources["dates"].isEmpty():
        raise ValueError(
            "Source 'dates' read no rows from its whole dataset. "
            "To allow this, set allow_empty: true on this source in "
            "fx_rates.yaml."
        )
    return sources


def transform(fx_rates: DataFrame, dates: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    join_fx_rates_to_date_result = join_fx_rates_to_date(fx_rates, dates)
    fill_weekends_and_holidays_result = fill_weekends_and_holidays(join_fx_rates_to_date_result)
    coalesce_rate_date_result = coalesce_rate_date(fill_weekends_and_holidays_result)
    return coalesce_rate_date_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.fx_rates_schema,
            unique_key=["rate_date", "quote_currency"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key="rate_date",
        )
        # Register after a successful write, while the DataFrame is still cached.
        register_partitions(
            data=output,
            table=output_table,
            partition_key="rate_date",
            output_file_path=output_file_path,
        )
    finally:
        output.unpersist()
