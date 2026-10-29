"""Transformations and run steps of the order_items ETL job.

parse_arguments, read, transform and write are scaffold-owned: `generate create-job` renders
them from the job spec, and `generate codegen` never changes them. Every other function is a
transformation whose body `generate codegen` writes.
"""

import argparse

from pyspark.sql import DataFrame
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


def join_order_data(orders: DataFrame, order_items: DataFrame) -> DataFrame:
    """Join orders with order_items, keeping all order_items rows.

    Selects order_date, order_id, and rate from orders, then left joins
    order_items onto the orders subset using order_id and order_date as keys,
    preserving every row from order_items.

    Args:
        orders: DataFrame containing order-level data including rate information.
        order_items: DataFrame containing line-item data for each order.

    Returns:
        DataFrame with order_date, order_id, rate from orders combined with
        line-item columns from order_items, with all order_items rows retained.
    """
    orders_subset = orders.select(F.col("order_date"), F.col("order_id"), F.col("rate"))
    result = order_items.join(orders_subset, on=["order_id", "order_date"], how="left").select(
        F.col("order_date"),
        F.col("order_id"),
        F.col("rate"),
        F.col("line_number"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("discount_amount"),
        F.col("tax_rate"),
    )
    return result


def create_gross_amount(join_order_data_result: DataFrame) -> DataFrame:
    """Adds a gross_amount column computed from quantity, unit_price, and discount_amount.

    Args:
        join_order_data_result: DataFrame containing order line data with quantity,
            unit_price, and discount_amount columns.

    Returns:
        DataFrame with all original columns plus gross_amount, calculated as
        (quantity * unit_price) - discount_amount, cast to decimal(12,2).
    """
    return join_order_data_result.withColumn(
        "gross_amount",
        (F.col("quantity") * F.col("unit_price") - F.col("discount_amount")).cast("decimal(12,2)"),
    )


def create_net_amount(create_gross_amount_result: DataFrame) -> DataFrame:
    """Adds a net_amount column derived from gross_amount divided by (1 + tax_rate).

    Args:
        create_gross_amount_result: DataFrame containing order line items with
            gross_amount and tax_rate columns.

    Returns:
        DataFrame with all original columns plus net_amount as decimal(12,2),
        computed as gross_amount / (1 + tax_rate).
    """
    return create_gross_amount_result.withColumn(
        "net_amount",
        (F.col("gross_amount") / (F.lit(1) + F.col("tax_rate"))).cast(DecimalType(12, 2)),
    )


def convert_gross_amount_to_euros(create_net_amount_result: DataFrame) -> DataFrame:
    """Converts gross_amount to euros by dividing by the exchange rate.

    Args:
        create_net_amount_result: DataFrame containing order line items with
            gross_amount in original currency and a rate for EUR conversion.

    Returns:
        DataFrame with gross_amount replaced by gross_amount_eur, computed as
        gross_amount divided by rate, cast to decimal(12,2).
    """
    return create_net_amount_result.select(
        F.col("order_date"),
        F.col("order_id"),
        F.col("rate"),
        F.col("line_number"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("discount_amount"),
        F.col("tax_rate"),
        F.col("net_amount"),
        (F.col("gross_amount") / F.col("rate")).cast("decimal(12,2)").alias("gross_amount_eur"),
    )


def convert_net_amount_to_euros(convert_gross_amount_to_euros_result: DataFrame) -> DataFrame:
    """Converts net_amount to euros by dividing by the exchange rate.

    Args:
        convert_gross_amount_to_euros_result: DataFrame containing order lines with
            net_amount in original currency and a rate column for conversion.

    Returns:
        DataFrame with net_amount_eur computed as net_amount divided by rate,
        and columns reordered to match the required output schema.
    """
    return convert_gross_amount_to_euros_result.withColumn(
        "net_amount_eur", F.round(F.col("net_amount") / F.col("rate"), 2).cast("decimal(12,2)")
    ).select(
        F.col("order_date"),
        F.col("order_id"),
        F.col("line_number"),
        F.col("product_id"),
        F.col("quantity"),
        F.col("unit_price"),
        F.col("discount_amount"),
        F.col("tax_rate"),
        F.col("rate"),
        F.col("gross_amount_eur"),
        F.col("net_amount_eur"),
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--ORDER_ITEMS_INPUT_PATH", type=str)
    parser.add_argument("--ORDERS_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--ORDER_ITEMS_LOOKBACK_DAYS", type=int)
    parser.add_argument("--ORDER_ITEMS_START_DATE", type=str)
    parser.add_argument("--ORDER_ITEMS_END_DATE", type=str)
    parser.add_argument("--ORDERS_LOOKBACK_DAYS", type=int)
    parser.add_argument("--ORDERS_START_DATE", type=str)
    parser.add_argument("--ORDERS_END_DATE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "order_items": create_pyspark_dataframe(
            spark=spark,
            file_path=args.ORDER_ITEMS_INPUT_PATH,
            schema=schema_definitions.order_items_raw_schema,
            file_format="json",
            date_ranges=processing_dates["order_items"],
            partition_key="order_date",
        ),
        "orders": create_pyspark_dataframe(
            spark=spark,
            file_path=args.ORDERS_INPUT_PATH,
            schema=schema_definitions.orders_schema,
            file_format="parquet",
            date_ranges=processing_dates["orders"],
            partition_key="order_date",
        ),
    }
    if sources["order_items"].isEmpty():
        raise ValueError(
            f"Source 'order_items' read no rows for "
            f"{processing_dates['order_items']}. "
            "To allow this, set allow_empty: true on this source in "
            "order_items.yaml."
        )
    if sources["orders"].isEmpty():
        raise ValueError(
            f"Source 'orders' read no rows for "
            f"{processing_dates['orders']}. "
            "To allow this, set allow_empty: true on this source in "
            "order_items.yaml."
        )
    return sources


def transform(order_items: DataFrame, orders: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    join_order_data_result = join_order_data(orders, order_items)
    create_gross_amount_result = create_gross_amount(join_order_data_result)
    create_net_amount_result = create_net_amount(create_gross_amount_result)
    convert_gross_amount_to_euros_result = convert_gross_amount_to_euros(create_net_amount_result)
    convert_net_amount_to_euros_result = convert_net_amount_to_euros(
        convert_gross_amount_to_euros_result,
    )
    return convert_net_amount_to_euros_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.order_items_schema,
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
