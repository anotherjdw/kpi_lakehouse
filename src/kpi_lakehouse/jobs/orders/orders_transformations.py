"""Transformations and run steps of the orders ETL job.

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


def create_order_date(orders: DataFrame) -> DataFrame:
    """Extracts the date from order_ts and adds it as order_date column.

    Args:
        orders: DataFrame containing order records with an order_ts timestamp column.

    Returns:
        DataFrame with all original columns plus an order_date column of type date,
        derived from the date portion of order_ts.
    """
    return orders.withColumn("order_date", F.to_date(F.col("order_ts")))


def keep_latest_status(create_order_date_result: DataFrame) -> DataFrame:
    """Keep the latest status row per order_id within a 14-day lookback window.

    Filters out test orders, retains only the row with the maximum updated_at
    per order_id (considering export dates within the last 14 days), and drops
    the is_test_order column.

    Args:
        create_order_date_result: DataFrame containing order records with status
            and audit fields including export_date, order_id, updated_at, and
            is_test_order.

    Returns:
        DataFrame with one row per order_id representing the latest status,
        with is_test_order column removed and test orders excluded.
    """
    max_export_date = create_order_date_result.agg(F.max("export_date")).collect()[0][0]
    lookback_start = F.date_sub(F.lit(max_export_date), 14)
    filtered = create_order_date_result.filter(~F.col("is_test_order")).filter(
        F.col("export_date") >= lookback_start
    )
    window = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
    result = (
        filtered.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "is_test_order")
    )
    return result


def join_fx_rates(fx_rates: DataFrame, keep_latest_status_result: DataFrame) -> DataFrame:
    """Join FX rates data to order data on matching date and currency.

    Args:
        fx_rates: DataFrame containing foreign exchange rate information
            with rate_date, base_currency, quote_currency, and rate columns.
        keep_latest_status_result: DataFrame containing order data with
            order_date and currency_code columns among others.

    Returns:
        DataFrame with all columns from both inputs, produced by a left join
        of fx_rates onto order data where order_date matches rate_date and
        currency_code matches quote_currency.
    """
    return keep_latest_status_result.join(
        fx_rates,
        on=[
            keep_latest_status_result["order_date"] == fx_rates["rate_date"],
            keep_latest_status_result["currency_code"] == fx_rates["quote_currency"],
        ],
        how="left",
    ).select(
        fx_rates["rate_date"],
        fx_rates["base_currency"],
        fx_rates["quote_currency"],
        fx_rates["rate"],
        keep_latest_status_result["export_date"],
        keep_latest_status_result["order_id"],
        keep_latest_status_result["customer_id"],
        keep_latest_status_result["session_id"],
        keep_latest_status_result["order_ts"],
        keep_latest_status_result["order_status"],
        keep_latest_status_result["updated_at"],
        keep_latest_status_result["currency_code"],
        keep_latest_status_result["shipping_fee"],
        keep_latest_status_result["voucher_code"],
        keep_latest_status_result["payment_method"],
        keep_latest_status_result["shipping_country"],
        keep_latest_status_result["order_date"],
    )


def convert_shipping_fee(join_fx_rates_result: DataFrame) -> DataFrame:
    """Converts shipping fee to EUR using the FX rate.

    Divides the shipping_fee by the rate to produce shipping_fee_eur,
    then selects and returns only the columns defined in the output schema.

    Args:
        join_fx_rates_result: DataFrame containing order data joined with FX
            rates, including shipping_fee and rate columns.

    Returns:
        DataFrame with shipping_fee converted to EUR as shipping_fee_eur,
        containing only the columns specified in the output schema.
    """
    return join_fx_rates_result.withColumn(
        "shipping_fee_eur", (F.col("shipping_fee") / F.col("rate")).cast("decimal(12,2)")
    ).select(
        "order_date",
        "export_date",
        "order_id",
        "customer_id",
        "session_id",
        "order_ts",
        "order_status",
        "updated_at",
        "currency_code",
        "voucher_code",
        "payment_method",
        "shipping_country",
        "rate_date",
        "base_currency",
        "quote_currency",
        "rate",
        "shipping_fee_eur",
    )


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--ORDERS_INPUT_PATH", type=str)
    parser.add_argument("--FX_RATES_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--ORDERS_LOOKBACK_DAYS", type=int)
    parser.add_argument("--ORDERS_START_DATE", type=str)
    parser.add_argument("--ORDERS_END_DATE", type=str)
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
        "orders": create_pyspark_dataframe(
            spark=spark,
            file_path=args.ORDERS_INPUT_PATH,
            schema=schema_definitions.orders_raw_schema,
            file_format="json",
            date_ranges=processing_dates["orders"],
            partition_key="export_date",
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
    if sources["orders"].isEmpty():
        raise ValueError(
            f"Source 'orders' read no rows for "
            f"{processing_dates['orders']}. "
            "To allow this, set allow_empty: true on this source in "
            "orders.yaml."
        )
    if sources["fx_rates"].isEmpty():
        raise ValueError(
            f"Source 'fx_rates' read no rows for "
            f"{processing_dates['fx_rates']}. "
            "To allow this, set allow_empty: true on this source in "
            "orders.yaml."
        )
    return sources


def transform(orders: DataFrame, fx_rates: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    create_order_date_result = create_order_date(orders)
    keep_latest_status_result = keep_latest_status(create_order_date_result)
    join_fx_rates_result = join_fx_rates(fx_rates, keep_latest_status_result)
    convert_shipping_fee_result = convert_shipping_fee(join_fx_rates_result)
    return convert_shipping_fee_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.orders_schema,
            unique_key=["order_id", "updated_at"],
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
