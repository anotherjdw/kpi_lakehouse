"""Transformations and run steps of the sessions ETL job.

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


def aggregate_session_events(session_events: DataFrame) -> DataFrame:
    """Aggregates session events into session-level summaries.

    Groups session event data by event_date and session_id, computing
    first/last timestamps, first marketing channel, last event type,
    last non-null customer_id, and the order_id associated with a purchase.

    Args:
        session_events: DataFrame containing raw session event records with
            fields including session_id, event_date, event_ts, mkt_channel,
            event_type, customer_id, and order_id.

    Returns:
        DataFrame with one row per (event_date, session_id) combination,
        containing aggregated session-level metrics.
    """

    return (
        session_events.groupBy("event_date", "session_id")
        .agg(
            F.min("event_ts").alias("first_event_time"),
            F.max("event_ts").alias("last_event_time"),
            F.min_by("mkt_channel", "event_ts").alias("first_mkt_channel"),
            F.max_by("event_type", "event_ts").alias("last_event_type"),
            F.max_by(
                "customer_id", F.when(F.col("customer_id").isNotNull(), F.col("event_ts"))
            ).alias("customer_id"),
            F.max(F.when(F.col("order_id").isNotNull(), F.col("order_id"))).alias("order_id"),
        )
        .select(
            F.col("session_id"),
            F.col("event_date"),
            F.col("first_event_time"),
            F.col("last_event_time"),
            F.col("first_mkt_channel"),
            F.col("customer_id"),
            F.col("last_event_type"),
            F.col("order_id"),
        )
    )


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
            schema=schema_definitions.session_events_schema,
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
            "sessions.yaml."
        )
    return sources


def transform(session_events: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    aggregate_session_events_result = aggregate_session_events(session_events)
    return aggregate_session_events_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.sessions_schema,
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
