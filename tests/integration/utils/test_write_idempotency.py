"""Idempotency guarantees of write_pyspark_dataframe's partitioned overwrite.

The write path uses mode="overwrite" with partitionOverwriteMode=dynamic, so
re-running a job for one date must replace only that date's partition, leaving
sibling partitions untouched and never duplicating rows on a repeated run.
"""

from datetime import date
from pathlib import Path

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType

from kpi_lakehouse.utils.dataframes import write_pyspark_dataframe

SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=True),
        StructField("date_utc", DateType(), nullable=True),
    ]
)

DAY_A = date(2026, 1, 1)
DAY_B = date(2026, 1, 2)


def _write(df: DataFrame, path: Path) -> None:
    write_pyspark_dataframe(
        output_dataframe=df,
        output_file_path=str(path),
        file_format="parquet",
        partition_key="date_utc",
    )


def test_partitioned_overwrite_is_idempotent_and_preserves_siblings(
    spark: SparkSession, test_dir: Path
) -> None:
    """Re-running one partition replaces it in place and leaves the others intact."""
    initial = spark.createDataFrame(
        [Row(id="a1", date_utc=DAY_A), Row(id="b1", date_utc=DAY_B)],
        schema=SCHEMA,
    )
    _write(initial, test_dir)

    # Re-run DAY_B only, twice: a repeated run must not duplicate rows.
    rerun_b = spark.createDataFrame([Row(id="b2", date_utc=DAY_B)], schema=SCHEMA)
    _write(rerun_b, test_dir)
    _write(rerun_b, test_dir)

    result = spark.read.parquet(str(test_dir))

    # Sibling partition DAY_A is untouched by the DAY_B re-runs.
    assert result.filter(f.col("date_utc") == DAY_A).count() == 1

    # DAY_B is replaced, not appended: only the re-run row remains (b1 is gone).
    day_b_ids = [row["id"] for row in result.filter(f.col("date_utc") == DAY_B).collect()]
    assert day_b_ids == ["b2"]

    # Total row count is stable across the identical repeated write (no duplication).
    assert result.count() == 2
