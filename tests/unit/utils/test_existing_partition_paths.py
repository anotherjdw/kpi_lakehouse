"""Existence filtering of requested partition paths before a partitioned read.

_existing_partition_paths uses the Hadoop FileSystem bound to the path's scheme,
so the same code that lists S3 in Glue lists the local filesystem here — no S3
and no mocking required.
"""

from pathlib import Path

from pyspark.sql import SparkSession

from kpi_lakehouse.utils.dataframes import _existing_partition_paths


def test_returns_only_existing_requested_partitions(spark: SparkSession, test_dir: Path) -> None:
    """A requested partition that exists is kept; one that is absent is dropped."""
    base = test_dir / "orders"
    (base / "OrderDate=2026-01-01").mkdir(parents=True)
    (base / "OrderDate=2026-01-02").mkdir(parents=True)

    result = _existing_partition_paths(spark, str(base), "OrderDate", ["2026-01-01", "2026-01-03"])

    # 2026-01-01 exists -> kept; 2026-01-03 absent -> dropped; 2026-01-02 not requested.
    assert result == [f"{base}/OrderDate=2026-01-01"]


def test_nonexistent_base_returns_empty(spark: SparkSession, test_dir: Path) -> None:
    """A base directory that does not exist yields an empty list, not an error."""
    base = test_dir / "missing"

    assert _existing_partition_paths(spark, str(base), "OrderDate", ["2026-01-01"]) == []


def test_empty_date_ranges_returns_empty(spark: SparkSession, test_dir: Path) -> None:
    """No requested dates yields an empty list even when partitions exist."""
    base = test_dir / "orders"
    (base / "OrderDate=2026-01-01").mkdir(parents=True)

    assert _existing_partition_paths(spark, str(base), "OrderDate", []) == []
