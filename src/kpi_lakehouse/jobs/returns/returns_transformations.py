"""Transformations and run steps of the returns ETL job.

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


def create_updated_date_column(returns: DataFrame) -> DataFrame:
    """Extract the date from the updated_at timestamp and add it as a new column.

    Args:
        returns: DataFrame containing return records with an updated_at timestamp column.

    Returns:
        DataFrame with all original columns plus an update_date column of type date,
        derived from the updated_at timestamp.
    """
    return returns.withColumn("update_date", F.to_date(F.col("updated_at")))


def keep_latest_status(create_updated_date_column_result: DataFrame) -> DataFrame:
    """Keep the latest status per return_id within a 45-day lookback window,
    excluding requested and rejected returns.

    Filters rows to retain only the most recently updated record per return_id
    within the last 45 days, then removes any returns with a 'requested' or
    'rejected' status, keeping only received or refunded returns.

    Args:
        create_updated_date_column_result: DataFrame containing return records with
            export_date, return_id, order_id, line_number, product_id, requested_ts,
            quantity_returned, refund_amount, return_reason, return_status, updated_at,
            and update_date columns.

    Returns:
        DataFrame with the same schema, containing only the latest received or
        refunded return record per return_id within the 45-day lookback window.
    """
    max_export_date = create_updated_date_column_result.agg(F.max("export_date")).collect()[0][0]
    cutoff_date = F.date_sub(F.lit(max_export_date), 45)
    filtered_df = create_updated_date_column_result.filter(F.col("export_date") >= cutoff_date)
    window_spec = Window.partitionBy("return_id").orderBy(F.col("updated_at").desc())
    ranked_df = filtered_df.withColumn("_rank", F.rank().over(window_spec))
    latest_df = ranked_df.filter(F.col("_rank") == 1).drop("_rank")
    result_df = latest_df.filter(~F.lower(F.col("return_status")).isin(["requested", "rejected"]))
    return result_df


def join_order_items_data(
    keep_latest_status_result: DataFrame, order_items: DataFrame
) -> DataFrame:
    """Join order items data to returns data on order_id and line_number.

    Performs a left join of order_items onto keep_latest_status_result, retaining
    tax_rate and rate from order_items. All returns are preserved, including those
    without a matching order item.

    Args:
        keep_latest_status_result: DataFrame containing return records with status
            and refund information.
        order_items: DataFrame containing order line item details including tax_rate
            and rate fields.

    Returns:
        DataFrame with all return records enriched with tax_rate and rate from
        order_items where a match exists on order_id and line_number.
    """
    order_items_selected = order_items.select(
        F.col("order_id"), F.col("line_number"), F.col("rate"), F.col("tax_rate")
    )
    result = keep_latest_status_result.join(
        order_items_selected, on=["order_id", "line_number"], how="left"
    )
    return result.select(
        F.col("export_date"),
        F.col("return_id"),
        F.col("order_id"),
        F.col("line_number"),
        F.col("product_id"),
        F.col("requested_ts"),
        F.col("quantity_returned"),
        F.col("refund_amount"),
        F.col("return_reason"),
        F.col("return_status"),
        F.col("updated_at"),
        F.col("update_date"),
        F.col("tax_rate"),
        F.col("rate"),
    )


def convert_refund_amount(join_order_items_data_result: DataFrame) -> DataFrame:
    """Converts refund_amount to EUR by dividing by the exchange rate.

    Args:
        join_order_items_data_result: DataFrame containing refund and order item
            data including refund_amount in original currency and rate for
            conversion to EUR.

    Returns:
        DataFrame with refund_amount replaced by refund_amount_eur, computed
        as refund_amount divided by rate, cast to decimal(12,2).
    """
    return join_order_items_data_result.withColumn(
        "refund_amount_eur", (F.col("refund_amount") / F.col("rate")).cast("decimal(12,2)")
    ).select(
        "export_date",
        "return_id",
        "order_id",
        "line_number",
        "product_id",
        "requested_ts",
        "quantity_returned",
        "return_reason",
        "return_status",
        "updated_at",
        "update_date",
        "tax_rate",
        "refund_amount_eur",
    )


def compute_net_refund(convert_refund_amount_result: DataFrame) -> DataFrame:
    """Computes the net refund amount in EUR by applying tax rate to the refund amount.

    Args:
        convert_refund_amount_result: DataFrame containing refund data with refund_amount_eur
            and tax_rate columns used to compute the net refund.

    Returns:
        DataFrame with the original columns plus a net_refund_eur column computed as
        refund_amount_eur multiplied by (1 + tax_rate), cast to decimal(12,2).
    """
    return convert_refund_amount_result.withColumn(
        "net_refund_eur",
        (F.col("refund_amount_eur") / (F.lit(1) + F.col("tax_rate"))).cast("decimal(12,2)"),
    ).select(
        "update_date",
        "export_date",
        "return_id",
        "order_id",
        "line_number",
        "product_id",
        "requested_ts",
        "quantity_returned",
        "return_reason",
        "return_status",
        "updated_at",
        "tax_rate",
        "refund_amount_eur",
        "net_refund_eur",
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--RETURNS_INPUT_PATH", type=str)
    parser.add_argument("--ORDER_ITEMS_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--RETURNS_LOOKBACK_DAYS", type=int)
    parser.add_argument("--RETURNS_START_DATE", type=str)
    parser.add_argument("--RETURNS_END_DATE", type=str)
    parser.add_argument("--ORDER_ITEMS_LOOKBACK_DAYS", type=int)
    parser.add_argument("--ORDER_ITEMS_START_DATE", type=str)
    parser.add_argument("--ORDER_ITEMS_END_DATE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "returns": create_pyspark_dataframe(
            spark=spark,
            file_path=args.RETURNS_INPUT_PATH,
            schema=schema_definitions.returns_raw_schema,
            file_format="json",
            date_ranges=processing_dates["returns"],
            partition_key="export_date",
        ),
        "order_items": create_pyspark_dataframe(
            spark=spark,
            file_path=args.ORDER_ITEMS_INPUT_PATH,
            schema=schema_definitions.order_items_schema,
            file_format="parquet",
            date_ranges=processing_dates["order_items"],
            partition_key="order_date",
        ),
    }
    if sources["returns"].isEmpty():
        raise ValueError(
            f"Source 'returns' read no rows for "
            f"{processing_dates['returns']}. "
            "To allow this, set allow_empty: true on this source in "
            "returns.yaml."
        )
    if sources["order_items"].isEmpty():
        raise ValueError(
            f"Source 'order_items' read no rows for "
            f"{processing_dates['order_items']}. "
            "To allow this, set allow_empty: true on this source in "
            "returns.yaml."
        )
    return sources


def transform(returns: DataFrame, order_items: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    create_updated_date_column_result = create_updated_date_column(returns)
    keep_latest_status_result = keep_latest_status(create_updated_date_column_result)
    join_order_items_data_result = join_order_items_data(keep_latest_status_result, order_items)
    convert_refund_amount_result = convert_refund_amount(join_order_items_data_result)
    compute_net_refund_result = compute_net_refund(convert_refund_amount_result)
    return compute_net_refund_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.returns_schema,
            unique_key=["return_id", "updated_at"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key="update_date",
        )
        # Register after a successful write, while the DataFrame is still cached.
        register_partitions(
            data=output,
            table=output_table,
            partition_key="update_date",
            output_file_path=output_file_path,
        )
    finally:
        output.unpersist()
