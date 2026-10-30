"""Transformations and run steps of the kpi_daily_sales ETL job.

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


def aggregate_daily_totals(fact_order_line: DataFrame) -> DataFrame:
    """Aggregate order line facts to daily totals by order_date.

    Groups by order_date and computes counts of active and cancelled orders,
    plus summed metrics (units, GMV, net sales, net revenue, gross margin)
    restricted to non-cancelled lines.

    Args:
        fact_order_line: DataFrame containing order line facts with order_date,
            order_id, order_status, quantity, gross_amount_eur, net_amount_eur,
            net_revenue_after_returns_eur, and gross_margin_eur columns.

    Returns:
        DataFrame with one row per order_date containing aggregated daily
        totals: orders, cancelled_orders, units, gmv_eur, net_sales_eur,
        net_revenue_eur, and gross_margin_eur.
    """
    is_cancelled = F.col("order_status") == F.lit("cancelled")
    is_active = ~is_cancelled
    result = fact_order_line.groupBy("order_date").agg(
        F.countDistinct(F.when(is_active, F.col("order_id"))).cast("int").alias("orders"),
        F.countDistinct(F.when(is_cancelled, F.col("order_id")))
        .cast("int")
        .alias("cancelled_orders"),
        F.sum(F.when(is_active, F.col("quantity")).otherwise(F.lit(0))).cast("int").alias("units"),
        F.sum(F.when(is_active, F.col("gross_amount_eur")).otherwise(F.lit(0)))
        .cast("decimal(12,2)")
        .alias("gmv_eur"),
        F.sum(F.when(is_active, F.col("net_amount_eur")).otherwise(F.lit(0)))
        .cast("decimal(12,2)")
        .alias("net_sales_eur"),
        F.sum(F.when(is_active, F.col("net_revenue_after_returns_eur")).otherwise(F.lit(0)))
        .cast("decimal(12,2)")
        .alias("net_revenue_eur"),
        F.sum(F.when(is_active, F.col("gross_margin_eur")))
        .cast("decimal(12,2)")
        .alias("gross_margin_eur"),
    )
    return result


def create_date_key(aggregate_daily_totals_result: DataFrame) -> DataFrame:
    """Creates a date_key column from the order_date column.

    Formats order_date as yyyyMMdd and casts it to an integer to produce
    a date_key column appended to the input DataFrame.

    Args:
        aggregate_daily_totals_result: DataFrame containing order_date and
            daily aggregate metrics.

    Returns:
        DataFrame with all original columns plus a date_key column of type
        int representing order_date in yyyyMMdd format.
    """
    return aggregate_daily_totals_result.withColumn(
        "date_key", F.date_format(F.col("order_date"), "yyyyMMdd").cast("int")
    )


def create_average_order_value_column(create_date_key_result: DataFrame) -> DataFrame:
    """Creates an average order value column (aov_eur) from net_revenue_eur divided by orders.

    Args:
        create_date_key_result: DataFrame containing order metrics including net_revenue_eur,
            orders, and other sales-related columns with a date_key.

    Returns:
        DataFrame with all original columns plus aov_eur (decimal(12,2)), which is
        net_revenue_eur divided by orders, or null when orders is 0.
    """
    return create_date_key_result.withColumn(
        "aov_eur",
        F.when(
            F.col("orders") != 0, (F.col("net_revenue_eur") / F.col("orders")).cast("decimal(12,2)")
        ).otherwise(F.lit(None).cast("decimal(12,2)")),
    )


def create_gross_margin_percentage_column(
    create_average_order_value_column_result: DataFrame,
) -> DataFrame:
    """Creates a gross_margin_pct column as the ratio of gross_margin_eur to net_sales_eur.

    Args:
        create_average_order_value_column_result: DataFrame containing order metrics including
            gross_margin_eur and net_sales_eur columns.

    Returns:
        DataFrame with an additional gross_margin_pct column of type decimal(2,2), representing
        gross_margin_eur divided by net_sales_eur, or null when net_sales_eur is zero.
    """
    return create_average_order_value_column_result.withColumn(
        "gross_margin_pct",
        F.when(F.col("net_sales_eur") == 0, F.lit(None)).otherwise(
            (F.col("gross_margin_eur") / F.col("net_sales_eur")).cast("decimal(2,2)")
        ),
    )


def create_cancellation_rate_column(
    create_gross_margin_percentage_column_result: DataFrame,
) -> DataFrame:
    """Creates a cancellation_rate column from cancelled_orders and orders.

    Computes cancellation_rate as cancelled_orders / (orders + cancelled_orders),
    returning null when the denominator is zero.

    Args:
        create_gross_margin_percentage_column_result: DataFrame containing order_date,
            orders, cancelled_orders, units, gmv_eur, net_sales_eur, net_revenue_eur,
            gross_margin_eur, date_key, aov_eur, and gross_margin_pct columns.

    Returns:
        DataFrame with all input columns plus a cancellation_rate column of type
        decimal(2,2), representing the ratio of cancelled_orders to total orders
        (cancelled + non-cancelled), or null if the denominator is zero.
    """
    df = create_gross_margin_percentage_column_result
    denominator = F.col("orders") + F.col("cancelled_orders")
    cancellation_rate = (
        F.when(denominator == 0, F.lit(None))
        .otherwise(F.col("cancelled_orders") / denominator)
        .cast("decimal(2,2)")
    )
    return df.withColumn("cancellation_rate", cancellation_rate)


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--FACT_ORDER_LINE_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--FACT_ORDER_LINE_LOOKBACK_DAYS", type=int)
    parser.add_argument("--FACT_ORDER_LINE_START_DATE", type=str)
    parser.add_argument("--FACT_ORDER_LINE_END_DATE", type=str)
    parser.add_argument("--JOB_NAME", type=str)
    parser.add_argument("--JOB_RUN_ID", type=str)
    parser.add_argument("--environment", type=str)
    return parser.parse_known_args(argv)[0]


def read(args: argparse.Namespace, processing_dates: dict[str, list[str]]) -> dict[str, DataFrame]:
    """Read every source, each for its own dates; an empty source fails unless allowed."""
    spark = spark_session()
    sources = {
        "fact_order_line": create_pyspark_dataframe(
            spark=spark,
            file_path=args.FACT_ORDER_LINE_INPUT_PATH,
            schema=schema_definitions.fact_order_line_schema,
            file_format="parquet",
            date_ranges=processing_dates["fact_order_line"],
            partition_key="order_date",
        ),
    }
    if sources["fact_order_line"].isEmpty():
        raise ValueError(
            f"Source 'fact_order_line' read no rows for "
            f"{processing_dates['fact_order_line']}. "
            "To allow this, set allow_empty: true on this source in "
            "kpi_daily_sales.yaml."
        )
    return sources


def transform(fact_order_line: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    aggregate_daily_totals_result = aggregate_daily_totals(fact_order_line)
    create_date_key_result = create_date_key(aggregate_daily_totals_result)
    create_average_order_value_column_result = create_average_order_value_column(
        create_date_key_result,
    )
    create_gross_margin_percentage_column_result = create_gross_margin_percentage_column(
        create_average_order_value_column_result,
    )
    create_cancellation_rate_column_result = create_cancellation_rate_column(
        create_gross_margin_percentage_column_result,
    )
    return create_cancellation_rate_column_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.kpi_daily_sales_schema,
            unique_key=["order_date"],
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
