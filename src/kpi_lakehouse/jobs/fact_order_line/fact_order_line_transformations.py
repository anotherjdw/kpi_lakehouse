"""Transformations and run steps of the fact_order_line ETL job.

parse_arguments, read, transform and write are scaffold-owned: `generate create-job` renders
them from the job spec, and `generate codegen` never changes them. Every other function is a
transformation whose body `generate codegen` writes.
"""

import argparse

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from kpi_lakehouse.utils.catalog import register_partitions
from kpi_lakehouse.utils.dataframes import (
    create_pyspark_dataframe,
    validate_pyspark_dataframe,
    write_pyspark_dataframe,
)
from kpi_lakehouse.utils.schema import schema_definitions
from kpi_lakehouse.utils.spark import spark_session


def select_order_columns(orders: DataFrame) -> DataFrame:
    """Select a subset of columns from the orders DataFrame.

    Args:
        orders: DataFrame containing order data with multiple columns including
            order_date, order_id, customer_id, session_id, order_ts, order_status,
            and additional columns to be dropped.

    Returns:
        DataFrame containing only order_date, order_id, customer_id, session_id,
        order_ts, and order_status columns.
    """
    return orders.select(
        F.col("order_date"),
        F.col("order_id"),
        F.col("customer_id"),
        F.col("session_id"),
        F.col("order_ts"),
        F.col("order_status"),
    )


def keep_latest_return_status(returns: DataFrame) -> DataFrame:
    """Keep only the row with the latest updated_at timestamp per return_id.

    Args:
        returns: DataFrame containing return records with one or more rows
            per return_id, each with an updated_at timestamp.

    Returns:
        DataFrame with the same schema as the input, retaining only the row
        with the maximum updated_at value for each return_id.
    """
    window = Window.partitionBy("return_id").orderBy(F.col("updated_at").desc())
    return (
        returns.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def keep_session_landing_row(sessions: DataFrame) -> DataFrame:
    """Keep the session row with the earliest first_event_time per session_id.

    Args:
        sessions: DataFrame containing session-level data with one or more rows
            per session_id, including event timing and marketing attribution fields.

    Returns:
        DataFrame with exactly one row per session_id, retaining the row that
        has the earliest first_event_time for each session_id.
    """
    window = Window.partitionBy("session_id").orderBy(F.col("first_event_time").asc())
    return (
        sessions.withColumn("_row_rank", F.row_number().over(window))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank")
    )


def join_order_items_to_orders(
    select_order_columns_result: DataFrame, order_items: DataFrame
) -> DataFrame:
    """Join order items to orders on order_id and order_date.

    Args:
        select_order_columns_result: DataFrame containing order-level columns
            including order_date, order_id, customer_id, session_id, order_ts,
            and order_status.
        order_items: DataFrame containing order item-level columns including
            order_date, order_id, line_number, product_id, quantity, unit_price,
            discount_amount, tax_rate, rate, gross_amount_eur, and net_amount_eur.

    Returns:
        DataFrame with one row per order line item, combining order-level and
        item-level columns, excluding the rate column, joined on order_id and
        order_date.
    """
    joined = order_items.join(
        select_order_columns_result, on=["order_id", "order_date"], how="inner"
    )
    return joined.select(
        F.col("order_date"),
        F.col("order_id"),
        F.col("customer_id"),
        F.col("session_id"),
        F.col("order_ts"),
        F.col("order_status"),
        F.col("line_number"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("discount_amount"),
        F.col("tax_rate"),
        F.col("gross_amount_eur"),
        F.col("net_amount_eur"),
    )


def aggregate_returns(keep_latest_return_status_result: DataFrame) -> DataFrame:
    """Aggregate return records by order and line number.

    Groups the input DataFrame by order_id and line_number, summing
    quantity_returned into returned_quantity and net_refund_eur (treating
    nulls as 0) into refund_net_eur.

    Args:
        keep_latest_return_status_result: DataFrame containing return records
            with columns including order_id, line_number, quantity_returned,
            and net_refund_eur.

    Returns:
        DataFrame with one row per (order_id, line_number) pair containing
        aggregated returned_quantity and refund_net_eur totals.
    """
    return keep_latest_return_status_result.groupBy("order_id", "line_number").agg(
        F.sum("quantity_returned").cast("int").alias("returned_quantity"),
        F.sum(F.coalesce(F.col("net_refund_eur"), F.lit(0).cast("decimal(12,2)")))
        .cast("decimal(12,2)")
        .alias("refund_net_eur"),
    )


def join_customer_version_to_order_data(
    join_order_items_to_orders_result: DataFrame, dim_customer: DataFrame
) -> DataFrame:
    """Left join dim_customer onto orders data to add customer_version_id.

    Joins dim_customer to order data on customer_id with a validity window check,
    ensuring the order_date falls within the customer record's valid_from and valid_to
    range, and adds customer_version_id to the result.

    Args:
        join_order_items_to_orders_result: DataFrame containing order and order item data
            with customer_id and order_date columns.
        dim_customer: DataFrame containing customer dimension data with validity period
            columns (valid_from, valid_to) and customer_version_id.

    Returns:
        DataFrame with all original order columns plus customer_version_id from the
        matched customer dimension record.
    """
    dim_customer_slim = dim_customer.select(
        F.col("customer_id").alias("dc_customer_id"),
        F.col("valid_from"),
        F.col("valid_to"),
        F.col("customer_version_id"),
    )
    join_condition = (
        (F.col("customer_id") == F.col("dc_customer_id"))
        & (F.col("valid_from") <= F.col("order_date"))
        & (F.col("valid_to").isNull() | (F.col("order_date") < F.col("valid_to")))
    )
    result = join_order_items_to_orders_result.join(
        dim_customer_slim, on=join_condition, how="left"
    ).drop("dc_customer_id", "valid_from", "valid_to")
    return result.select(
        F.col("order_date"),
        F.col("order_id"),
        F.col("customer_id"),
        F.col("session_id"),
        F.col("order_ts"),
        F.col("order_status"),
        F.col("line_number"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("discount_amount"),
        F.col("tax_rate"),
        F.col("gross_amount_eur"),
        F.col("net_amount_eur"),
        F.col("customer_version_id"),
    )


def join_product_version_to_orders_data(
    join_customer_version_to_order_data_result: DataFrame, dim_product: DataFrame
) -> DataFrame:
    """Join dim_product to orders data to add product version and cost price columns.

    Performs a left join between orders data and dim_product on product_id,
    filtering to the valid product version for each order date (where valid_from
    is on or before order_date and valid_to is null or after order_date). Only
    product_version_id and cost_price_eur are added to the result.

    Args:
        join_customer_version_to_order_data_result: DataFrame containing order
            lines with customer version information.
        dim_product: DataFrame containing product dimension data with SCD2
            validity columns (valid_from, valid_to).

    Returns:
        DataFrame with the original orders columns plus cost_price_eur and
        product_version_id from the matching product dimension record.
    """
    dim_product_slim = dim_product.select(
        F.col("product_id").alias("dp_product_id"),
        F.col("valid_from").alias("dp_valid_from"),
        F.col("valid_to").alias("dp_valid_to"),
        F.col("product_version_id"),
        F.col("cost_price_eur"),
    )
    join_condition = (
        (F.col("product_id") == F.col("dp_product_id"))
        & (F.col("dp_valid_from") <= F.col("order_date"))
        & (F.col("dp_valid_to").isNull() | (F.col("order_date") < F.col("dp_valid_to")))
    )
    result = join_customer_version_to_order_data_result.join(
        dim_product_slim, on=join_condition, how="left"
    ).select(
        F.col("order_date"),
        F.col("order_id"),
        F.col("customer_id"),
        F.col("session_id"),
        F.col("order_ts"),
        F.col("order_status"),
        F.col("line_number"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("discount_amount"),
        F.col("tax_rate"),
        F.col("gross_amount_eur"),
        F.col("net_amount_eur"),
        F.col("customer_version_id"),
        F.col("cost_price_eur"),
        F.col("product_version_id"),
    )
    return result


def create_cost_column(join_product_version_to_orders_data_result: DataFrame) -> DataFrame:
    """Creates a cost_eur column by multiplying quantity by cost_price_eur.

    Args:
        join_product_version_to_orders_data_result: DataFrame containing order line
            data with quantity and cost_price_eur columns.

    Returns:
        DataFrame with the original columns (excluding cost_price_eur) plus a new
        cost_eur column of type decimal(12,2) representing quantity * cost_price_eur.
    """
    return join_product_version_to_orders_data_result.withColumn(
        "cost_eur", (F.col("quantity") * F.col("cost_price_eur")).cast(DecimalType(12, 2))
    ).select(
        "order_date",
        "order_id",
        "customer_id",
        "session_id",
        "order_ts",
        "order_status",
        "line_number",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "tax_rate",
        "gross_amount_eur",
        "net_amount_eur",
        "customer_version_id",
        "product_version_id",
        "cost_eur",
    )


def create_gross_margin_column(create_cost_column_result: DataFrame) -> DataFrame:
    """Creates a gross_margin_eur column as the difference between net_amount_eur and cost_eur.

    Args:
        create_cost_column_result: DataFrame containing order line items with net_amount_eur
            and cost_eur columns expressed in EUR.

    Returns:
        DataFrame with all original columns plus gross_margin_eur as decimal(12,2),
        computed as net_amount_eur minus cost_eur.
    """
    return create_cost_column_result.withColumn(
        "gross_margin_eur", (F.col("net_amount_eur") - F.col("cost_eur")).cast(DecimalType(12, 2))
    )


def join_aggregated_returns_to_order_data(
    aggregate_returns_result: DataFrame, create_gross_margin_column_result: DataFrame
) -> DataFrame:
    """Join aggregated returns data to order line data via a left join.

    Performs a left join from order data onto aggregated returns on order_id
    and line_number, preserving all order lines regardless of whether a return
    exists.

    Args:
        aggregate_returns_result: DataFrame containing aggregated return
            quantities and refund amounts per order line.
        create_gross_margin_column_result: DataFrame containing order line
            details including gross margin calculations.

    Returns:
        DataFrame with all order lines enriched with return information where
        available, containing the combined schema of both inputs minus
        duplicate join keys.
    """
    return create_gross_margin_column_result.join(
        aggregate_returns_result, on=["order_id", "line_number"], how="left"
    ).select(
        F.col("order_id"),
        F.col("line_number"),
        F.col("returned_quantity"),
        F.col("refund_net_eur"),
        F.col("order_date"),
        F.col("customer_id"),
        F.col("session_id"),
        F.col("order_ts"),
        F.col("order_status"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("discount_amount"),
        F.col("tax_rate"),
        F.col("gross_amount_eur"),
        F.col("net_amount_eur"),
        F.col("customer_version_id"),
        F.col("product_version_id"),
        F.col("cost_eur"),
        F.col("gross_margin_eur"),
    )


def fill_missing_returns(join_aggregated_returns_to_order_data_result: DataFrame) -> DataFrame:
    """Fill null values in return-related columns with default zero values.

    Args:
        join_aggregated_returns_to_order_data_result: DataFrame containing order data
            joined with aggregated returns, where some rows may have null values in
            returned_quantity and refund_net_eur columns.

    Returns:
        DataFrame with the same schema where returned_quantity nulls are replaced
        with 0 and refund_net_eur nulls are replaced with 0.00 as decimal(12,2).
    """
    return join_aggregated_returns_to_order_data_result.withColumn(
        "returned_quantity", F.coalesce(F.col("returned_quantity"), F.lit(0))
    ).withColumn(
        "refund_net_eur", F.coalesce(F.col("refund_net_eur"), F.lit(0).cast("decimal(12,2)"))
    )


def create_net_revenue_after_returns(fill_missing_returns_result: DataFrame) -> DataFrame:
    """Adds a net revenue after returns column to the order line DataFrame.

    Computes net_revenue_after_returns_eur as the difference between
    net_amount_eur and refund_net_eur, cast to decimal(12,2).

    Args:
        fill_missing_returns_result: DataFrame containing order line data with
            net_amount_eur and refund_net_eur columns.

    Returns:
        DataFrame with all original columns plus net_revenue_after_returns_eur
        of type decimal(12,2).
    """
    return fill_missing_returns_result.withColumn(
        "net_revenue_after_returns_eur",
        (F.col("net_amount_eur") - F.col("refund_net_eur")).cast("decimal(12,2)"),
    )


def join_session_channel(
    create_net_revenue_after_returns_result: DataFrame, keep_session_landing_row_result: DataFrame
) -> DataFrame:
    """Left join session data onto orders data to add session channel information.

    Performs a left join between order lines and session data on session_id,
    enriching each order line with the first_mkt_channel from the session,
    aliased as session_channel. Order lines with no matching session or a null
    session_id will have a null session_channel.

    Args:
        create_net_revenue_after_returns_result: DataFrame containing order line
            data including net revenue after returns calculations.
        keep_session_landing_row_result: DataFrame containing session-level data
            including the first marketing channel per session.

    Returns:
        DataFrame with all original order line columns plus session_channel
        derived from first_mkt_channel in the session data.
    """
    session_df = keep_session_landing_row_result.select(
        F.col("session_id"), F.col("first_mkt_channel").alias("session_channel")
    )
    result = create_net_revenue_after_returns_result.join(session_df, on="session_id", how="left")
    return result.select(
        F.col("order_id"),
        F.col("line_number"),
        F.col("returned_quantity"),
        F.col("refund_net_eur"),
        F.col("order_date"),
        F.col("customer_id"),
        F.col("session_id"),
        F.col("order_ts"),
        F.col("order_status"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("discount_amount"),
        F.col("tax_rate"),
        F.col("gross_amount_eur"),
        F.col("net_amount_eur"),
        F.col("customer_version_id"),
        F.col("product_version_id"),
        F.col("cost_eur"),
        F.col("gross_margin_eur"),
        F.col("net_revenue_after_returns_eur"),
        F.col("session_channel"),
    )


def join_sessions_for_fallback_channel(
    join_session_channel_result: DataFrame, sessions: DataFrame
) -> DataFrame:
    """Join sessions to order lines to derive a fallback marketing channel.

    Performs a left join of sessions onto order lines by customer_id, filtering
    sessions where first_mkt_channel is not 'direct' and first_event_time falls
    within [order_ts - 30 days, order_ts). Among multiple matching sessions per
    order line, retains the one with the latest first_event_time (ties broken by
    highest session_id). Unmatched lines retain a single row with fallback_channel
    set to null.

    Args:
        join_session_channel_result: DataFrame containing order line data with
            session and channel information.
        sessions: DataFrame containing session-level data including marketing
            channel and event timing.

    Returns:
        DataFrame with the same order line columns plus a fallback_channel column
        derived from the best-matching session's first_mkt_channel.
    """
    sessions_filtered = sessions.filter(
        (F.col("first_mkt_channel") != "direct") & F.col("first_mkt_channel").isNotNull()
    ).select(
        F.col("session_id").alias("fb_session_id"),
        F.col("customer_id").alias("fb_customer_id"),
        F.col("first_mkt_channel").alias("fallback_channel"),
        F.col("first_event_time").alias("fb_first_event_time"),
    )
    join_condition = (
        (F.col("customer_id") == F.col("fb_customer_id"))
        & (F.col("fb_first_event_time") >= F.col("order_ts") - F.expr("INTERVAL 30 DAYS"))
        & (F.col("fb_first_event_time") < F.col("order_ts"))
    )
    joined = join_session_channel_result.join(sessions_filtered, join_condition, "left")
    window = Window.partitionBy("order_id", "line_number").orderBy(
        F.col("fb_first_event_time").desc_nulls_last(), F.col("fb_session_id").desc_nulls_last()
    )
    ranked = joined.withColumn("_rank", F.row_number().over(window))
    result = ranked.filter(F.col("_rank") == 1).drop(
        "_rank", "fb_session_id", "fb_customer_id", "fb_first_event_time"
    )
    output_columns = [
        "order_id",
        "line_number",
        "returned_quantity",
        "refund_net_eur",
        "order_date",
        "customer_id",
        "session_id",
        "order_ts",
        "order_status",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "tax_rate",
        "gross_amount_eur",
        "net_amount_eur",
        "customer_version_id",
        "product_version_id",
        "cost_eur",
        "gross_margin_eur",
        "net_revenue_after_returns_eur",
        "session_channel",
        "fallback_channel",
    ]
    return result.select(output_columns)


def create_mkt_channel(join_sessions_for_fallback_channel_result: DataFrame) -> DataFrame:
    """Creates a mkt_channel column from session_channel and fallback_channel with a
    'direct' default.

    Derives the marketing channel by coalescing session_channel and fallback_channel,
    then filling any remaining nulls with 'direct'. Drops the original channel columns
    and returns the result as mkt_channel.

    Args:
        join_sessions_for_fallback_channel_result: DataFrame containing order line data
            with session_channel and fallback_channel columns.

    Returns:
        DataFrame with the same columns as the input except session_channel and
        fallback_channel are replaced by a single mkt_channel string column.
    """
    return (
        join_sessions_for_fallback_channel_result.withColumn(
            "mkt_channel", F.coalesce(F.col("session_channel"), F.col("fallback_channel"))
        )
        .withColumn(
            "mkt_channel",
            F.when(F.col("mkt_channel").isNull(), F.lit("direct")).otherwise(F.col("mkt_channel")),
        )
        .drop("session_channel", "fallback_channel")
    )


def create_date_key(create_mkt_channel_result: DataFrame) -> DataFrame:
    """Creates a date_key column derived from order_date and reorders columns.

    Args:
        create_mkt_channel_result: DataFrame containing order data with an order_date column.

    Returns:
        DataFrame with the required output schema including a date_key column
        representing order_date formatted as yyyyMMdd cast to integer.
    """
    return create_mkt_channel_result.withColumn(
        "date_key", F.date_format(F.col("order_date"), "yyyyMMdd").cast("int")
    ).select(
        "order_date",
        "order_id",
        "line_number",
        "customer_id",
        "session_id",
        "order_ts",
        "order_status",
        "product_id",
        "quantity",
        "unit_price",
        "discount_amount",
        "tax_rate",
        "gross_amount_eur",
        "net_amount_eur",
        "customer_version_id",
        "product_version_id",
        "cost_eur",
        "gross_margin_eur",
        "returned_quantity",
        "refund_net_eur",
        "net_revenue_after_returns_eur",
        "mkt_channel",
        "date_key",
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--ORDERS_INPUT_PATH", type=str)
    parser.add_argument("--ORDER_ITEMS_INPUT_PATH", type=str)
    parser.add_argument("--RETURNS_INPUT_PATH", type=str)
    parser.add_argument("--DIM_CUSTOMER_INPUT_PATH", type=str)
    parser.add_argument("--DIM_PRODUCT_INPUT_PATH", type=str)
    parser.add_argument("--SESSIONS_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--ORDERS_LOOKBACK_DAYS", type=int)
    parser.add_argument("--ORDERS_START_DATE", type=str)
    parser.add_argument("--ORDERS_END_DATE", type=str)
    parser.add_argument("--ORDER_ITEMS_LOOKBACK_DAYS", type=int)
    parser.add_argument("--ORDER_ITEMS_START_DATE", type=str)
    parser.add_argument("--ORDER_ITEMS_END_DATE", type=str)
    parser.add_argument("--RETURNS_LOOKBACK_DAYS", type=int)
    parser.add_argument("--RETURNS_START_DATE", type=str)
    parser.add_argument("--RETURNS_END_DATE", type=str)
    parser.add_argument("--DIM_CUSTOMER_LOOKBACK_DAYS", type=int)
    parser.add_argument("--DIM_CUSTOMER_START_DATE", type=str)
    parser.add_argument("--DIM_CUSTOMER_END_DATE", type=str)
    parser.add_argument("--DIM_PRODUCT_LOOKBACK_DAYS", type=int)
    parser.add_argument("--DIM_PRODUCT_START_DATE", type=str)
    parser.add_argument("--DIM_PRODUCT_END_DATE", type=str)
    parser.add_argument("--SESSIONS_LOOKBACK_DAYS", type=int)
    parser.add_argument("--SESSIONS_START_DATE", type=str)
    parser.add_argument("--SESSIONS_END_DATE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "orders": create_pyspark_dataframe(
            spark=spark,
            file_path=args.ORDERS_INPUT_PATH,
            schema=schema_definitions.orders_schema,
            file_format="parquet",
            date_ranges=processing_dates["orders"],
            partition_key="order_date",
        ),
        "order_items": create_pyspark_dataframe(
            spark=spark,
            file_path=args.ORDER_ITEMS_INPUT_PATH,
            schema=schema_definitions.order_items_schema,
            file_format="parquet",
            date_ranges=processing_dates["order_items"],
            partition_key="order_date",
        ),
        "returns": create_pyspark_dataframe(
            spark=spark,
            file_path=args.RETURNS_INPUT_PATH,
            schema=schema_definitions.returns_schema,
            file_format="parquet",
            date_ranges=processing_dates["returns"],
            partition_key="update_date",
        ),
        "dim_customer": create_pyspark_dataframe(
            spark=spark,
            file_path=args.DIM_CUSTOMER_INPUT_PATH,
            schema=schema_definitions.dim_customer_schema,
            file_format="parquet",
            date_ranges=processing_dates["dim_customer"],
            partition_key="valid_from",
        ),
        "dim_product": create_pyspark_dataframe(
            spark=spark,
            file_path=args.DIM_PRODUCT_INPUT_PATH,
            schema=schema_definitions.dim_product_schema,
            file_format="parquet",
            date_ranges=processing_dates["dim_product"],
            partition_key="valid_from",
        ),
        "sessions": create_pyspark_dataframe(
            spark=spark,
            file_path=args.SESSIONS_INPUT_PATH,
            schema=schema_definitions.sessions_schema,
            file_format="parquet",
            date_ranges=processing_dates["sessions"],
            partition_key="event_date",
        ),
    }
    if sources["orders"].isEmpty():
        raise ValueError(
            f"Source 'orders' read no rows for "
            f"{processing_dates['orders']}. "
            "To allow this, set allow_empty: true on this source in "
            "fact_order_line.yaml."
        )
    if sources["order_items"].isEmpty():
        raise ValueError(
            f"Source 'order_items' read no rows for "
            f"{processing_dates['order_items']}. "
            "To allow this, set allow_empty: true on this source in "
            "fact_order_line.yaml."
        )
    if sources["returns"].isEmpty():
        raise ValueError(
            f"Source 'returns' read no rows for "
            f"{processing_dates['returns']}. "
            "To allow this, set allow_empty: true on this source in "
            "fact_order_line.yaml."
        )
    if sources["dim_customer"].isEmpty():
        raise ValueError(
            f"Source 'dim_customer' read no rows for "
            f"{processing_dates['dim_customer']}. "
            "To allow this, set allow_empty: true on this source in "
            "fact_order_line.yaml."
        )
    if sources["dim_product"].isEmpty():
        raise ValueError(
            f"Source 'dim_product' read no rows for "
            f"{processing_dates['dim_product']}. "
            "To allow this, set allow_empty: true on this source in "
            "fact_order_line.yaml."
        )
    if sources["sessions"].isEmpty():
        raise ValueError(
            f"Source 'sessions' read no rows for "
            f"{processing_dates['sessions']}. "
            "To allow this, set allow_empty: true on this source in "
            "fact_order_line.yaml."
        )
    return sources


def transform(
    orders: DataFrame,
    order_items: DataFrame,
    returns: DataFrame,
    dim_customer: DataFrame,
    dim_product: DataFrame,
    sessions: DataFrame,
) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    select_order_columns_result = select_order_columns(orders)
    keep_latest_return_status_result = keep_latest_return_status(returns)
    keep_session_landing_row_result = keep_session_landing_row(sessions)
    join_order_items_to_orders_result = join_order_items_to_orders(
        select_order_columns_result,
        order_items,
    )
    aggregate_returns_result = aggregate_returns(keep_latest_return_status_result)
    join_customer_version_to_order_data_result = join_customer_version_to_order_data(
        join_order_items_to_orders_result,
        dim_customer,
    )
    join_product_version_to_orders_data_result = join_product_version_to_orders_data(
        join_customer_version_to_order_data_result,
        dim_product,
    )
    create_cost_column_result = create_cost_column(join_product_version_to_orders_data_result)
    create_gross_margin_column_result = create_gross_margin_column(create_cost_column_result)
    join_aggregated_returns_to_order_data_result = join_aggregated_returns_to_order_data(
        aggregate_returns_result,
        create_gross_margin_column_result,
    )
    fill_missing_returns_result = fill_missing_returns(join_aggregated_returns_to_order_data_result)
    create_net_revenue_after_returns_result = create_net_revenue_after_returns(
        fill_missing_returns_result,
    )
    join_session_channel_result = join_session_channel(
        create_net_revenue_after_returns_result,
        keep_session_landing_row_result,
    )
    join_sessions_for_fallback_channel_result = join_sessions_for_fallback_channel(
        join_session_channel_result,
        sessions,
    )
    create_mkt_channel_result = create_mkt_channel(join_sessions_for_fallback_channel_result)
    create_date_key_result = create_date_key(create_mkt_channel_result)
    return create_date_key_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.fact_order_line_schema,
            unique_key=["order_id", "line_number"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key="order_date",
        )
        # Register after a successful write, while the DataFrame is still cached.
        register_partitions(
            data=output,
            table=output_table,
            partition_key="order_date",
            output_file_path=output_file_path,
        )
    finally:
        output.unpersist()
