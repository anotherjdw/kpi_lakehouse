"""Transformations and run steps of the kpi_return_rate ETL job.

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


def select_category_level(dim_product: DataFrame) -> DataFrame:
    """Select product version ID and top-level category ID from the product dimension.

    Args:
        dim_product: DataFrame containing product dimension data with version IDs
            and hierarchical category level columns.

    Returns:
        DataFrame containing only product_version_id and category_id_level_1 columns.
    """
    return dim_product.select(F.col("product_version_id"), F.col("category_id_level_1"))


def join_category_data_to_fact_order_line(
    select_category_level_result: DataFrame, fact_order_line: DataFrame
) -> DataFrame:
    """Join category level data onto fact order line records using product_version_id.

    Performs a left join of category information onto the fact order line DataFrame,
    preserving all order line records regardless of whether a matching category exists.

    Args:
        select_category_level_result: DataFrame containing product version to category
            level 1 mappings with columns product_version_id and category_id_level_1.
        fact_order_line: DataFrame containing order line fact records with full order
            and product details.

    Returns:
        DataFrame with all fact_order_line records enriched with category_id_level_1,
        containing exactly the columns defined in the required output schema.
    """
    return fact_order_line.join(
        select_category_level_result, on="product_version_id", how="left"
    ).select(
        F.col("product_version_id"),
        F.col("category_id_level_1"),
        F.col("order_date"),
        F.col("order_id"),
        F.col("line_number"),
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
        F.col("cost_eur"),
        F.col("gross_margin_eur"),
        F.col("returned_quantity"),
        F.col("refund_net_eur"),
        F.col("net_revenue_after_returns_eur"),
        F.col("mkt_channel"),
        F.col("date_key"),
    )


def filter_completed_orders(join_category_data_to_fact_order_line_result: DataFrame) -> DataFrame:
    """Filter out cancelled orders from the order line data.

    Args:
        join_category_data_to_fact_order_line_result: DataFrame containing order line data
            with category information joined, including an order_status column.

    Returns:
        DataFrame with the same schema as the input, containing only rows where
        order_status is not 'cancelled'.
    """
    return join_category_data_to_fact_order_line_result.filter(F.col("order_status") != "cancelled")


def create_month_column(filter_completed_orders_result: DataFrame) -> DataFrame:
    """Creates a month column representing the first date of the calendar month.

    Args:
        filter_completed_orders_result: DataFrame containing order data with an order_date column.

    Returns:
        DataFrame with all original columns plus a month column set to the first day
        of the calendar month derived from order_date.
    """
    return filter_completed_orders_result.withColumn(
        "month", F.trunc(F.col("order_date"), "MM").cast("date")
    )


def filter_complete_months(create_month_column_result: DataFrame) -> DataFrame:
    """Filter rows to only include months fully covered by the data's date range.

    Computes the earliest order_date across the entire DataFrame and retains
    only rows whose month value is greater than or equal to that minimum date,
    effectively excluding any month that the lookback period only partially covers.

    Args:
        create_month_column_result: DataFrame containing order data with a
            pre-computed `month` column (first day of each order's month) and
            an `order_date` column used to determine the earliest date present.

    Returns:
        DataFrame with the same schema as the input, filtered to rows where
        `month` is greater than or equal to the minimum `order_date` found
        in the full dataset.
    """
    min_order_date = create_month_column_result.agg(
        F.min("order_date").alias("min_order_date")
    ).collect()[0]["min_order_date"]
    return create_month_column_result.filter(F.col("month") >= F.lit(min_order_date))


def aggregate_monthly_returns(filter_complete_months_result: DataFrame) -> DataFrame:
    """Aggregate sales and return metrics by month and category.

    Groups the input DataFrame by month and category_id_level_1, computing
    total units sold, units returned, and net refunds in EUR.

    Args:
        filter_complete_months_result: DataFrame containing order line data
            with quantity, returned_quantity, and refund_net_eur columns.

    Returns:
        DataFrame with one row per (month, category_id_level_1) combination,
        containing aggregated units_sold, units_returned, and net_refunds_eur.
    """
    return (
        filter_complete_months_result.groupBy("month", "category_id_level_1")
        .agg(
            F.sum("quantity").cast("int").alias("units_sold"),
            F.sum("returned_quantity").cast("int").alias("units_returned"),
            F.sum("refund_net_eur").cast("decimal(12,2)").alias("net_refunds_eur"),
        )
        .select(
            F.col("month"),
            F.col("category_id_level_1"),
            F.col("units_sold"),
            F.col("units_returned"),
            F.col("net_refunds_eur"),
        )
    )


def create_return_rate_column(aggregate_monthly_returns_result: DataFrame) -> DataFrame:
    """Create a return_rate column as units_returned divided by units_sold.

    Args:
        aggregate_monthly_returns_result: DataFrame containing monthly return
            aggregates with units_sold and units_returned columns.

    Returns:
        DataFrame with the original columns plus a return_rate column of type
        decimal(5,4), computed as units_returned / units_sold, or null when
        units_sold is 0.
    """
    return aggregate_monthly_returns_result.withColumn(
        "return_rate",
        F.when(
            F.col("units_sold") != 0,
            (F.col("units_returned") / F.col("units_sold")).cast("decimal(5,4)"),
        ).otherwise(F.lit(None).cast("decimal(5,4)")),
    )


def create_is_final_column(
    create_return_rate_column_result: DataFrame, filter_completed_orders_result: DataFrame
) -> DataFrame:
    """Creates an is_final boolean column based on whether max order date is more than 45 days
    after end of month.

    Args:
        create_return_rate_column_result: DataFrame containing monthly return rate metrics
        per category. filter_completed_orders_result: DataFrame containing completed
        orders used to derive max order date.

    Returns:
        DataFrame with the original return rate columns plus an is_final boolean column indicating
        whether the max order date is more than 45 days after the last day of the month.
    """
    max_order_date_df = filter_completed_orders_result.agg(
        F.max("order_date").alias("max_order_date")
    )
    joined = create_return_rate_column_result.crossJoin(max_order_date_df)
    last_day_of_month = F.last_day(F.col("month"))
    result = joined.withColumn(
        "is_final", F.datediff(F.col("max_order_date"), last_day_of_month) > 45
    ).drop("max_order_date")
    return result


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this job's Glue arguments, ignoring the ones Glue adds itself."""
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--FACT_ORDER_LINE_INPUT_PATH", type=str)
    parser.add_argument("--DIM_PRODUCT_INPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_PATH", type=str)
    parser.add_argument("--OUTPUT_TABLE", type=str)
    parser.add_argument("--PROCESSING_TYPE", type=str)
    parser.add_argument("--FACT_ORDER_LINE_LOOKBACK_DAYS", type=int)
    parser.add_argument("--FACT_ORDER_LINE_START_DATE", type=str)
    parser.add_argument("--FACT_ORDER_LINE_END_DATE", type=str)
    parser.add_argument("--DIM_PRODUCT_LOOKBACK_DAYS", type=int)
    parser.add_argument("--DIM_PRODUCT_START_DATE", type=str)
    parser.add_argument("--DIM_PRODUCT_END_DATE", type=str)
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
        "dim_product": create_pyspark_dataframe(
            spark=spark,
            file_path=args.DIM_PRODUCT_INPUT_PATH,
            schema=schema_definitions.dim_product_schema,
            file_format="parquet",
            date_ranges=processing_dates["dim_product"],
            partition_key="valid_from",
        ),
    }
    if sources["fact_order_line"].isEmpty():
        raise ValueError(
            f"Source 'fact_order_line' read no rows for "
            f"{processing_dates['fact_order_line']}. "
            "To allow this, set allow_empty: true on this source in "
            "kpi_return_rate.yaml."
        )
    if sources["dim_product"].isEmpty():
        raise ValueError(
            f"Source 'dim_product' read no rows for "
            f"{processing_dates['dim_product']}. "
            "To allow this, set allow_empty: true on this source in "
            "kpi_return_rate.yaml."
        )
    return sources


def transform(fact_order_line: DataFrame, dim_product: DataFrame) -> DataFrame:
    """Run the transformations in dependency order and return the job's output."""
    select_category_level_result = select_category_level(dim_product)
    join_category_data_to_fact_order_line_result = join_category_data_to_fact_order_line(
        select_category_level_result,
        fact_order_line,
    )
    filter_completed_orders_result = filter_completed_orders(
        join_category_data_to_fact_order_line_result,
    )
    create_month_column_result = create_month_column(filter_completed_orders_result)
    filter_complete_months_result = filter_complete_months(create_month_column_result)
    aggregate_monthly_returns_result = aggregate_monthly_returns(filter_complete_months_result)
    create_return_rate_column_result = create_return_rate_column(aggregate_monthly_returns_result)
    create_is_final_column_result = create_is_final_column(
        create_return_rate_column_result,
        filter_completed_orders_result,
    )
    return create_is_final_column_result


def write(output: DataFrame, output_file_path: str, output_table: str) -> None:
    """Check the output against its contract, write it as parquet, then register its partitions."""
    output = output.persist()
    try:
        validate_pyspark_dataframe(
            output,
            schema_definitions.kpi_return_rate_schema,
            unique_key=["month", "category_id_level_1"],
        )
        write_pyspark_dataframe(
            output_dataframe=output,
            output_file_path=output_file_path,
            file_format="parquet",
            partition_key="month",
        )
        # Register after a successful write, while the DataFrame is still cached.
        register_partitions(
            data=output,
            table=output_table,
            partition_key="month",
            output_file_path=output_file_path,
        )
    finally:
        output.unpersist()
