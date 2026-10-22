"""register_partitions against a real (local) catalog table.

The catalog_table fixture is an external table at the test's folder with no partitions
registered, standing in for a job's Glue table. Reading it shows only the data in
registered partitions, which is exactly what Athena sees.
"""

from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import DateType, DecimalType, IntegerType, StructField, StructType

from kpi_lakehouse.utils.catalog import register_partitions
from kpi_lakehouse.utils.dataframes import write_pyspark_dataframe
from tests.helpers import create_catalog_table, registered_partitions

SCHEMA = StructType(
    [
        StructField("date_utc", DateType(), nullable=False),
        StructField("shop_id", IntegerType(), nullable=False),
        StructField("orders", IntegerType(), nullable=True),
        StructField("earnings_eur", DecimalType(10, 2), nullable=True),
    ]
)
PARTITION_KEY = "date_utc"


@pytest.fixture
def catalog_table(spark: SparkSession, test_database: str, test_dir: Path) -> Iterator[str]:
    """A local table at `test_dir` with no partitions registered, like a new Glue table.

    Yields the fully qualified table name, like a job's --OUTPUT_TABLE.
    """
    table = f"{test_database}.orders_per_shop"
    create_catalog_table(spark, table, SCHEMA, PARTITION_KEY, test_dir)
    yield table
    # External table: dropping it removes the catalog entry, not the files in test_dir.
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def orders_per_shop(spark: SparkSession, *days: date) -> DataFrame:
    """Two rows per day, one for each of two shops."""
    return spark.createDataFrame(
        [
            Row(date_utc=day, shop_id=shop_id, orders=1, earnings_eur=Decimal("10.00"))
            for day in days
            for shop_id in (1, 2)
        ],
        schema=SCHEMA,
    )


def write_and_register(data: DataFrame, table: str, path: Path) -> None:
    """Write `data` partitioned by date under `path`, then register what was written."""
    write_pyspark_dataframe(
        output_dataframe=data,
        output_file_path=str(path),
        file_format="parquet",
        partition_key=PARTITION_KEY,
    )
    register_partitions(
        data=data,
        table=table,
        partition_key=PARTITION_KEY,
        output_file_path=str(path),
    )


def test_written_data_is_only_visible_once_registered(
    spark: SparkSession, test_dir: Path, catalog_table: str
) -> None:
    """Files written to the table's folder appear in the table only once registered."""
    data = orders_per_shop(spark, date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3))

    write_pyspark_dataframe(
        output_dataframe=data,
        output_file_path=str(test_dir),
        file_format="parquet",
        partition_key=PARTITION_KEY,
    )
    assert spark.table(catalog_table).count() == 0

    register_partitions(
        data=data,
        table=catalog_table,
        partition_key=PARTITION_KEY,
        output_file_path=str(test_dir),
    )

    assert registered_partitions(spark, catalog_table) == [
        "date_utc=2026-01-01",
        "date_utc=2026-01-02",
        "date_utc=2026-01-03",
    ]
    assert spark.table(catalog_table).count() == data.count()


def test_rerun_of_an_existing_partition_is_idempotent(
    spark: SparkSession, test_dir: Path, catalog_table: str
) -> None:
    """Re-registering a date already registered adds nothing and fails on nothing."""
    write_and_register(
        orders_per_shop(spark, date(2026, 1, 1), date(2026, 1, 2)), catalog_table, test_dir
    )

    # A backfill that rewrites 2026-01-02 and adds 2026-01-03 must not fail on the
    # already-registered partition, and must only add the new one.
    write_and_register(
        orders_per_shop(spark, date(2026, 1, 2), date(2026, 1, 3)), catalog_table, test_dir
    )

    assert registered_partitions(spark, catalog_table) == [
        "date_utc=2026-01-01",
        "date_utc=2026-01-02",
        "date_utc=2026-01-03",
    ]
    assert spark.table(catalog_table).count() == 6
