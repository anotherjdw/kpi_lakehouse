"""Transformations and run steps of the session_events ETL job.

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


def deduplicate_session_data(session_events: DataFrame) -> DataFrame:
    """Deduplicate session events by keeping one row per event_id.

    Orders the data by event_id and event_ts descending, then retains only
    the first (most recent) row for each unique event_id.

    Args:
        session_events: DataFrame containing session event records with
            event_id, event_ts, and associated session/user attributes.

    Returns:
        DataFrame with the same schema as the input, containing exactly one
        row per event_id, selected based on descending event_ts ordering.
    """
    window = Window.partitionBy(F.col("event_id")).orderBy(
        F.col("event_id").desc(), F.col("event_ts").desc()
    )
    return (
        session_events.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def derive_marketing_channel(deduplicate_session_data_result: DataFrame) -> DataFrame:
    """Classify each event into a marketing channel based on UTM parameters or referrer domain.

    Derives a marketing channel label for each event by inspecting utm_source and
    utm_medium first, then falling back to referrer_domain when UTM fields are absent.
    The resulting channel is one of: paid_search, paid_social, affiliate, email,
    organic_search, referral, or direct.

    Args:
        deduplicate_session_data_result: DataFrame containing deduplicated session
            events with UTM parameters and referrer domain columns.

    Returns:
        DataFrame with all original columns plus a new `mkt_channel` string column
        containing the classified marketing channel for each event.
    """
    paid_search_sources = ["google", "bing", "yahoo", "baidu", "duckduckgo", "yandex"]
    paid_social_sources = [
        "facebook",
        "instagram",
        "twitter",
        "linkedin",
        "tiktok",
        "pinterest",
        "snapchat",
        "youtube",
    ]
    affiliate_mediums = ["affiliate", "partner", "cpa"]
    email_mediums = ["email", "newsletter", "e-mail", "mail"]
    paid_mediums = ["cpc", "ppc", "paid", "paid_search", "paidsearch", "paid search"]
    social_mediums = ["social", "social-media", "social_media", "paid_social", "paidsocial"]
    organic_search_domains = [
        "google.com",
        "bing.com",
        "yahoo.com",
        "baidu.com",
        "duckduckgo.com",
        "yandex.com",
        "yandex.ru",
    ]
    social_domains = [
        "facebook.com",
        "instagram.com",
        "twitter.com",
        "linkedin.com",
        "tiktok.com",
        "pinterest.com",
        "snapchat.com",
        "youtube.com",
        "t.co",
    ]
    src = F.lower(F.col("utm_source"))
    med = F.lower(F.col("utm_medium"))
    ref = F.lower(F.col("referrer_domain"))
    has_utm = F.col("utm_source").isNotNull() & (F.trim(F.col("utm_source")) != "")
    utm_channel = (
        F.when(med.isin(email_mediums), "email")
        .when(med.isin(affiliate_mediums), "affiliate")
        .when(med.isin(paid_mediums) & src.isin(paid_search_sources), "paid_search")
        .when(
            med.isin(paid_mediums) | med.isin(social_mediums),
            F.when(src.isin(paid_social_sources), "paid_social").otherwise("paid_search"),
        )
        .when(src.isin(paid_social_sources) & med.isin(social_mediums), "paid_social")
        .when(src.isin(paid_search_sources), "paid_search")
        .when(src.isin(paid_social_sources), "paid_social")
        .otherwise("referral")
    )
    referrer_channel = (
        F.when(ref.isNull() | (F.trim(ref) == ""), "direct")
        .when(ref.isin(organic_search_domains), "organic_search")
        .when(ref.isin(social_domains), "referral")
        .otherwise("referral")
    )
    mkt_channel = F.when(has_utm, utm_channel).otherwise(referrer_channel)
    return deduplicate_session_data_result.withColumn("mkt_channel", mkt_channel)


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--SESSION_EVENTS_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--SESSION_EVENTS_LOOKBACK_DAYS", type=int)
    parser.add_argument("--SESSION_EVENTS_START_DATE", type=str)
    parser.add_argument("--SESSION_EVENTS_END_DATE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "session_events": create_pyspark_dataframe(
            spark=spark,
            file_path=args.SESSION_EVENTS_INPUT_PATH,
            schema=schema_definitions.session_events_raw_schema,
            file_format="parquet",
            date_ranges=processing_dates["session_events"],
            partition_key="event_date",
        ),
    }
    if sources["session_events"].isEmpty():
        raise ValueError(
            f"Source 'session_events' read no rows for "
            f"{processing_dates['session_events']}. "
            "To allow this, set allow_empty: true on this source in "
            "session_events.yaml."
        )
    return sources


def transform(session_events: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    deduplicate_session_data_result = deduplicate_session_data(session_events)
    derive_marketing_channel_result = derive_marketing_channel(deduplicate_session_data_result)
    return derive_marketing_channel_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.session_events_schema,
            unique_key=["event_id"],
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
