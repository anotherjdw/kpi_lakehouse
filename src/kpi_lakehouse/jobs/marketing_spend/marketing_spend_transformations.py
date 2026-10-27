"""Transformations and run steps of the marketing_spend ETL job.

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


def join_fx_rates(fx_rates: DataFrame, marketing_spend: DataFrame) -> DataFrame:
    """Join FX rates onto marketing spend data by date and currency.

    Performs a left join from marketing_spend to fx_rates on spend_date matching
    rate_date and currency_code matching base_currency, retaining all marketing
    spend rows and enriching with FX rate information where available.

    Args:
        fx_rates: DataFrame containing exchange rate data with rate_date,
            base_currency, quote_currency, and rate columns.
        marketing_spend: DataFrame containing marketing spend data with
            spend_date, currency_code, and associated campaign metrics.

    Returns:
        DataFrame with the combined schema containing FX rate fields
        (rate_date, quote_currency, rate) alongside all marketing spend fields,
        excluding the base_currency column from fx_rates.
    """
    result = marketing_spend.join(
        fx_rates.select(
            F.col("rate_date"), F.col("base_currency"), F.col("quote_currency"), F.col("rate")
        ),
        on=[
            marketing_spend["spend_date"] == fx_rates["rate_date"],
            marketing_spend["currency_code"] == fx_rates["quote_currency"],
        ],
        how="left",
    ).select(
        F.col("rate_date"),
        F.col("quote_currency"),
        F.col("rate"),
        F.col("spend_date"),
        F.col("platform"),
        F.col("campaign_id"),
        F.col("campaign_name"),
        F.col("utm_source"),
        F.col("utm_medium"),
        F.col("utm_campaign"),
        F.col("impressions"),
        F.col("clicks"),
        F.col("spend_amount"),
        F.col("currency_code"),
        F.col("reported_at"),
    )
    return result


def keep_latest_restatement(join_fx_rates_result: DataFrame) -> DataFrame:
    """Keep only the row with the latest reported_at per spend_date, platform, and campaign_id.

    Args:
        join_fx_rates_result: DataFrame containing FX-joined spend records with
            potentially multiple restatements per spend_date/platform/campaign_id.

    Returns:
        DataFrame with the same schema, retaining only the row with the maximum
        reported_at timestamp for each (spend_date, platform, campaign_id) group.
    """
    window = Window.partitionBy("spend_date", "platform", "campaign_id").orderBy(
        F.col("reported_at").desc()
    )
    return (
        join_fx_rates_result.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def convert_spend_amount_to_euros(keep_latest_restatement_result: DataFrame) -> DataFrame:
    """Convert spend amounts to euros using exchange rates.

    Divides spend_amount by rate to produce spend_amount_eur, then drops
    intermediate columns to match the required output schema.

    Args:
        keep_latest_restatement_result: DataFrame containing spend data with
            exchange rate information joined in, including spend_amount,
            rate, and associated campaign metadata.

    Returns:
        DataFrame with spend_amount converted to euros as spend_amount_eur,
        with rate-related and raw spend_amount columns removed.
    """
    return keep_latest_restatement_result.withColumn(
        "spend_amount_eur", (F.col("spend_amount") / F.col("rate")).cast("decimal(12,2)")
    ).select(
        "spend_date",
        "platform",
        "campaign_id",
        "campaign_name",
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "impressions",
        "clicks",
        "spend_amount_eur",
        "currency_code",
        "reported_at",
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--MARKETING_SPEND_INPUT_PATH", type=str)
    parser.add_argument("--FX_RATES_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--MARKETING_SPEND_LOOKBACK_DAYS", type=int)
    parser.add_argument("--MARKETING_SPEND_START_DATE", type=str)
    parser.add_argument("--MARKETING_SPEND_END_DATE", type=str)
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
        "marketing_spend": create_pyspark_dataframe(
            spark=spark,
            file_path=args.MARKETING_SPEND_INPUT_PATH,
            schema=schema_definitions.marketing_spend_raw_schema,
            file_format="json",
            date_ranges=processing_dates["marketing_spend"],
            partition_key="spend_date",
        ),
        "fx_rates": create_pyspark_dataframe(
            spark=spark,
            file_path=args.FX_RATES_INPUT_PATH,
            schema=schema_definitions.fx_rates_schema,
            file_format="parquet",
            date_ranges=processing_dates["fx_rates"],
            partition_key="rate_date",
        ),
    }
    if sources["marketing_spend"].isEmpty():
        raise ValueError(
            f"Source 'marketing_spend' read no rows for "
            f"{processing_dates['marketing_spend']}. "
            "To allow this, set allow_empty: true on this source in "
            "marketing_spend.yaml."
        )
    if sources["fx_rates"].isEmpty():
        raise ValueError(
            f"Source 'fx_rates' read no rows for "
            f"{processing_dates['fx_rates']}. "
            "To allow this, set allow_empty: true on this source in "
            "marketing_spend.yaml."
        )
    return sources


def transform(marketing_spend: DataFrame, fx_rates: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    join_fx_rates_result = join_fx_rates(fx_rates, marketing_spend)
    keep_latest_restatement_result = keep_latest_restatement(join_fx_rates_result)
    convert_spend_amount_to_euros_result = convert_spend_amount_to_euros(
        keep_latest_restatement_result,
    )
    return convert_spend_amount_to_euros_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.marketing_spend_schema,
            unique_key=["spend_date", "platform", "campaign_id", "reported_at"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key="spend_date",
        )
        # Register after a successful write, while the DataFrame is still cached.
        register_partitions(
            data=output,
            table=output_table,
            partition_key="spend_date",
            output_file_path=output_file_path,
        )
    finally:
        output.unpersist()
