"""Transformations and run steps of the kpi_channel_performance ETL job.

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


def aggregate_sessions(fact_session: DataFrame) -> DataFrame:
    """Aggregate session data by event date and marketing channel.

    Groups fact_session by event_date and mkt_channel, computing total session
    counts and converted session counts. Renames event_date to date in the output.

    Args:
        fact_session: DataFrame containing session-level data with event_date,
            mkt_channel, and converted_customer columns among others.

    Returns:
        DataFrame with one row per (date, mkt_channel) combination, containing
        total session counts and counts of sessions where converted_customer is true.
    """
    return (
        fact_session.groupBy(F.col("event_date"), F.col("mkt_channel"))
        .agg(
            F.count("*").alias("sessions"),
            F.count(F.when(F.col("converted_customer"), 1)).alias("converted_sessions"),
        )
        .withColumnRenamed("event_date", "date")
        .select(F.col("date"), F.col("mkt_channel"), F.col("sessions"), F.col("converted_sessions"))
    )


def filter_completed_orders(fact_order_line: DataFrame) -> DataFrame:
    """Filter out cancelled orders from the fact order line table.

    Args:
        fact_order_line: DataFrame containing order line facts with order status information.

    Returns:
        DataFrame with the same schema as the input, excluding rows where order_status
         is 'cancelled'.
    """
    return fact_order_line.filter(F.col("order_status") != "cancelled")


def aggregate_marketing_spend(marketing_spend: DataFrame) -> DataFrame:
    """Aggregate marketing spend by date and channel.

    Groups the marketing spend data by spend_date and platform (used as
    mkt_channel), summing spend_amount_eur into spend_eur.

    Args:
        marketing_spend: DataFrame containing raw marketing spend records with
            spend_date, platform, and spend_amount_eur columns among others.

    Returns:
        DataFrame with columns date (date), mkt_channel (string), and
        spend_eur (decimal(12,2)), aggregated by date and marketing channel.
    """
    return (
        marketing_spend.withColumnRenamed("platform", "mkt_channel")
        .groupBy(F.col("spend_date"), F.col("mkt_channel"))
        .agg(F.sum("spend_amount_eur").cast("decimal(12,2)").alias("spend_eur"))
        .withColumnRenamed("spend_date", "date")
        .select("date", "mkt_channel", "spend_eur")
    )


def aggregate_attributed_revenue(filter_completed_orders_result: DataFrame) -> DataFrame:
    """Aggregate net revenue after returns by order date and marketing channel.

    Args:
        filter_completed_orders_result: DataFrame containing completed order line
            items with revenue, return, and attribution details.

    Returns:
        DataFrame with one row per (date, mkt_channel) combination, containing
        the sum of net_revenue_after_returns_eur as attributed_net_revenue_eur,
        cast to decimal(12,2).
    """
    return (
        filter_completed_orders_result.groupBy(F.col("order_date"), F.col("mkt_channel"))
        .agg(
            F.sum("net_revenue_after_returns_eur")
            .cast("decimal(12,2)")
            .alias("attributed_net_revenue_eur")
        )
        .withColumnRenamed("order_date", "date")
        .select(F.col("date"), F.col("mkt_channel"), F.col("attributed_net_revenue_eur"))
    )


def combine_channel_metrics(
    aggregate_sessions_result: DataFrame,
    aggregate_attributed_revenue_result: DataFrame,
    aggregate_marketing_spend_result: DataFrame,
) -> DataFrame:
    """Combines session, attributed revenue, and marketing spend metrics into a single DataFrame.

    Performs full outer joins across all three input DataFrames on date and
    mkt_channel, preserving every date and mkt_channel combination that appears
    in any of the inputs.

    Args:
        aggregate_sessions_result: DataFrame containing session and converted
            session counts per date and marketing channel.
        aggregate_attributed_revenue_result: DataFrame containing attributed net
            revenue per date and marketing channel.
        aggregate_marketing_spend_result: DataFrame containing marketing spend
            per date and marketing channel.

    Returns:
        A DataFrame with one row per unique date and mkt_channel combination
        found across all three inputs, with columns for sessions,
        converted_sessions, attributed_net_revenue_eur, and spend_eur.
    """
    join_keys = ["date", "mkt_channel"]
    combined = aggregate_sessions_result.join(
        aggregate_attributed_revenue_result, on=join_keys, how="full"
    ).join(aggregate_marketing_spend_result, on=join_keys, how="full")
    return combined.select(
        F.col("date"),
        F.col("mkt_channel"),
        F.col("sessions"),
        F.col("converted_sessions"),
        F.col("attributed_net_revenue_eur"),
        F.col("spend_eur"),
    )


def fill_missing_metrics(combine_channel_metrics_result: DataFrame) -> DataFrame:
    """Fill missing metric values with appropriate defaults.

    Fills null values in sessions and converted_sessions with 0 (integer),
    and null values in attributed_net_revenue_eur and spend_eur with 0.00
    (decimal(12,2)).

    Args:
        combine_channel_metrics_result: DataFrame containing channel metrics
            with potential null values in numeric columns.

    Returns:
        DataFrame with the same schema where null numeric metric values have
        been replaced with zero defaults.
    """
    return (
        combine_channel_metrics_result.withColumn(
            "sessions", F.coalesce(F.col("sessions"), F.lit(0).cast("bigint"))
        )
        .withColumn(
            "converted_sessions", F.coalesce(F.col("converted_sessions"), F.lit(0).cast("bigint"))
        )
        .withColumn(
            "attributed_net_revenue_eur",
            F.coalesce(F.col("attributed_net_revenue_eur"), F.lit(0.0).cast("decimal(12,2)")),
        )
        .withColumn("spend_eur", F.coalesce(F.col("spend_eur"), F.lit(0.0).cast("decimal(12,2)")))
    )


def create_date_key(fill_missing_metrics_result: DataFrame) -> DataFrame:
    """Creates a date_key column from the date column formatted as yyyyMMdd integer.

    Args:
        fill_missing_metrics_result: DataFrame containing date and marketing metrics columns.

    Returns:
        DataFrame with an additional date_key column representing the date as an integer
        in yyyyMMdd format.
    """
    return fill_missing_metrics_result.withColumn(
        "date_key", F.date_format(F.col("date"), "yyyyMMdd").cast("int")
    )


def create_conversion_rate(create_date_key_result: DataFrame) -> DataFrame:
    """Adds a conversion_rate column computed as converted_sessions divided by sessions.

    Args:
        create_date_key_result: DataFrame containing session and conversion data
            with a date_key column already added.

    Returns:
        DataFrame with all original columns plus a conversion_rate column of
        type decimal(5,4), set to null when sessions is 0.
    """
    return create_date_key_result.withColumn(
        "conversion_rate",
        F.when(F.col("sessions") == 0, F.lit(None).cast("decimal(5,4)")).otherwise(
            (F.col("converted_sessions") / F.col("sessions")).cast("decimal(5,4)")
        ),
    )


def create_return_on_ad_spend(create_conversion_rate_result: DataFrame) -> DataFrame:
    """Adds a return on ad spend (ROAS) column to the input DataFrame.

    Computes ROAS as attributed_net_revenue_eur divided by spend_eur,
    returning null when spend_eur is zero to avoid division by zero errors.

    Args:
        create_conversion_rate_result: DataFrame containing marketing channel
            performance data including attributed net revenue and spend columns.

    Returns:
        DataFrame with all original columns plus a roas column of type
        decimal(12,2), representing attributed_net_revenue_eur / spend_eur,
        or null when spend_eur is 0.
    """
    return create_conversion_rate_result.withColumn(
        "roas",
        F.when(F.col("spend_eur") == 0, None).otherwise(
            (F.col("attributed_net_revenue_eur") / F.col("spend_eur")).cast("decimal(12,2)")
        ),
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--FACT_SESSION_INPUT_PATH", type=str)
    parser.add_argument("--FACT_ORDER_LINE_INPUT_PATH", type=str)
    parser.add_argument("--MARKETING_SPEND_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--FACT_SESSION_LOOKBACK_DAYS", type=int)
    parser.add_argument("--FACT_SESSION_START_DATE", type=str)
    parser.add_argument("--FACT_SESSION_END_DATE", type=str)
    parser.add_argument("--FACT_ORDER_LINE_LOOKBACK_DAYS", type=int)
    parser.add_argument("--FACT_ORDER_LINE_START_DATE", type=str)
    parser.add_argument("--FACT_ORDER_LINE_END_DATE", type=str)
    parser.add_argument("--MARKETING_SPEND_LOOKBACK_DAYS", type=int)
    parser.add_argument("--MARKETING_SPEND_START_DATE", type=str)
    parser.add_argument("--MARKETING_SPEND_END_DATE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "fact_session": create_pyspark_dataframe(
            spark=spark,
            file_path=args.FACT_SESSION_INPUT_PATH,
            schema=schema_definitions.fact_session_schema,
            file_format="parquet",
            date_ranges=processing_dates["fact_session"],
            partition_key="event_date",
        ),
        "fact_order_line": create_pyspark_dataframe(
            spark=spark,
            file_path=args.FACT_ORDER_LINE_INPUT_PATH,
            schema=schema_definitions.fact_order_line_schema,
            file_format="parquet",
            date_ranges=processing_dates["fact_order_line"],
            partition_key="order_date",
        ),
        "marketing_spend": create_pyspark_dataframe(
            spark=spark,
            file_path=args.MARKETING_SPEND_INPUT_PATH,
            schema=schema_definitions.marketing_spend_schema,
            file_format="parquet",
            date_ranges=processing_dates["marketing_spend"],
            partition_key="spend_date",
        ),
    }
    if sources["fact_session"].isEmpty():
        raise ValueError(
            f"Source 'fact_session' read no rows for "
            f"{processing_dates['fact_session']}. "
            "To allow this, set allow_empty: true on this source in "
            "kpi_channel_performance.yaml."
        )
    if sources["fact_order_line"].isEmpty():
        raise ValueError(
            f"Source 'fact_order_line' read no rows for "
            f"{processing_dates['fact_order_line']}. "
            "To allow this, set allow_empty: true on this source in "
            "kpi_channel_performance.yaml."
        )
    if sources["marketing_spend"].isEmpty():
        raise ValueError(
            f"Source 'marketing_spend' read no rows for "
            f"{processing_dates['marketing_spend']}. "
            "To allow this, set allow_empty: true on this source in "
            "kpi_channel_performance.yaml."
        )
    return sources


def transform(
    fact_session: DataFrame,
    fact_order_line: DataFrame,
    marketing_spend: DataFrame,
) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    aggregate_sessions_result = aggregate_sessions(fact_session)
    filter_completed_orders_result = filter_completed_orders(fact_order_line)
    aggregate_marketing_spend_result = aggregate_marketing_spend(marketing_spend)
    aggregate_attributed_revenue_result = aggregate_attributed_revenue(
        filter_completed_orders_result,
    )
    combine_channel_metrics_result = combine_channel_metrics(
        aggregate_sessions_result,
        aggregate_attributed_revenue_result,
        aggregate_marketing_spend_result,
    )
    fill_missing_metrics_result = fill_missing_metrics(combine_channel_metrics_result)
    create_date_key_result = create_date_key(fill_missing_metrics_result)
    create_conversion_rate_result = create_conversion_rate(create_date_key_result)
    create_return_on_ad_spend_result = create_return_on_ad_spend(create_conversion_rate_result)
    return create_return_on_ad_spend_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.kpi_channel_performance_schema,
            unique_key=["date", "mkt_channel"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key="date",
        )
        # Register after a successful write, while the DataFrame is still cached.
        register_partitions(
            data=output,
            table=output_table,
            partition_key="date",
            output_file_path=output_file_path,
        )
    finally:
        output.unpersist()
