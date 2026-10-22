"""Read, validate and write the PySpark DataFrames every job in this project handles."""

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from .data_quality import check_no_duplicate_rows
from .python import validate_dates


def validate_pyspark_dataframe(
    data: DataFrame, expected_schema: t.StructType, unique_key: list[str]
) -> None:
    """Check an output DataFrame against its contract before it is written.

    Runs in each job's write(), so a job never writes data its contract does not
    describe. Three checks run in order, and the first one that fails raises:

    * schema: missing and unexpected columns, and the exact type of every shared
      column (decimal precision and scale count; nullable is ignored). Nothing is
      cast to make a column fit;
    * nulls: one pass over the columns the expected schema marks non-nullable;
    * uniqueness: no two rows share the same `unique_key`.

    An empty DataFrame has no nulls and no duplicates, so it passes.

    Args:
        data: The DataFrame about to be written.
        expected_schema: The output contract's schema.
        unique_key: Columns that together identify one row of the output.

    Raises:
        ValueError: "Schema validation failed: ..." listing every schema problem at
            once, or "Data validation failed: ..." for nulls in non-nullable columns
            or duplicate keys.
    """
    expected_column_names = set(expected_schema.fieldNames())
    expected_column_types = {field.name: field.dataType for field in expected_schema.fields}

    actual_column_names = set(data.columns)
    actual_column_types = {field.name: field.dataType for field in data.schema.fields}

    columns_missing_from_expected_schema = expected_column_names - actual_column_names
    unexpected_columns_in_actual_schema = actual_column_names - expected_column_names

    validation_errors: list[str] = []

    # collect columns missing from expected schema
    if columns_missing_from_expected_schema:
        validation_errors.append(f"Missing columns: {columns_missing_from_expected_schema}")

    # collect unexpected columns coming from actual dataframe
    if unexpected_columns_in_actual_schema:
        validation_errors.append(f"Unexpected columns: {unexpected_columns_in_actual_schema}")

    # identify data type errors
    type_errors = []
    for col_name in expected_column_names & actual_column_names:
        expected_type = expected_column_types[col_name]
        actual_type = actual_column_types[col_name]
        if actual_type != expected_type:
            type_errors.append(
                f"'{col_name}': Expected {expected_type.simpleString()}, "
                f"Received {actual_type.simpleString()}"
            )

    # collect data type errors
    if type_errors:
        validation_errors.append(f"Type mismatches: {', '.join(type_errors)}")

    if validation_errors:
        error_message = "Schema validation failed:\n- " + "\n- ".join(validation_errors)
        raise ValueError(error_message)

    non_nullable = [field.name for field in expected_schema.fields if not field.nullable]
    if non_nullable:
        null_counts = data.select(
            [f.count(f.when(f.col(c).isNull(), 1)).alias(c) for c in non_nullable]
        ).first()
        # A global aggregation always returns exactly one row, even on an empty DataFrame.
        assert null_counts is not None
        columns_with_nulls = {c: null_counts[c] for c in non_nullable if null_counts[c] > 0}
        if columns_with_nulls:
            raise ValueError(
                f"Data validation failed: nulls in non-nullable columns {columns_with_nulls}"
            )

    uniqueness = check_no_duplicate_rows(data, subset=unique_key)
    if not uniqueness.passed:
        raise ValueError(f"Data validation failed: {uniqueness.message}")


def create_pyspark_dataframe(
    spark: SparkSession,
    file_path: str,
    schema: StructType | None = None,
    file_format: str = "json",
    date_ranges: list[str] | None = None,
    partition_key: str | None = None,
) -> DataFrame:
    """Read one source dataset from files, limited to the requested date partitions.

    Every job's read() calls this once per source. JSON is read with `multiLine`, so
    a file holding one JSON array is read whole. For a partitioned source, only the
    `{partition_key}={day}` folders that exist are loaded, with `basePath` set so the
    partition column stays in the result.

    Args:
        spark: The active SparkSession.
        file_path: The dataset's root folder, e.g. s3://bucket/orders/.
        schema: The schema to read with; None lets Spark infer one.
        file_format: The files' format, e.g. json or parquet.
        date_ranges: Partition values to read, as YYYY-MM-DD strings.
        partition_key: The dataset's date partition column; None for an unpartitioned
            dataset, which is read whole.

    Returns:
        The rows read. For a partitioned read where none of the requested partitions
        exist, an empty DataFrame with `schema`.

    Raises:
        ValueError: If a value in `date_ranges` is not a YYYY-MM-DD string.
    """
    reader = spark.read.format(file_format).option("multiLine", "true")
    if schema is not None:
        reader = reader.schema(schema)

    if partition_key and date_ranges:
        validate_dates(date_ranges)
        base = file_path.rstrip("/")
        paths = _existing_partition_paths(spark, base, partition_key, date_ranges)
        if not paths:
            # None of the requested partitions exist. Loading a missing directory
            # (or an empty path list) raises AnalysisException, so return an empty
            # frame with the read schema to preserve the "no data" outcome.
            return spark.createDataFrame([], schema)
        return reader.option("basePath", base).load(paths)

    return reader.load(file_path)


def _existing_partition_paths(
    spark: SparkSession,
    base: str,
    partition_key: str,
    date_ranges: list[str],
) -> list[str]:
    """Return the requested partition paths under `base` that actually exist.

    Loading a non-existent partition directory raises AnalysisException and fails
    the whole read, so requested partitions are filtered down to those present in
    storage before `create_pyspark_dataframe` loads them. The base directory is
    listed once via the Hadoop FileSystem bound to its scheme — the same
    filesystem Spark is configured with, so this works against S3 in Glue and the
    local filesystem in tests with no extra dependencies.

    Args:
        spark: Active SparkSession, used to reach the Hadoop configuration.
        base: Partition root without a trailing slash, e.g. s3://bucket/orders.
        partition_key: The Hive partition column name, e.g. OrderDate.
        date_ranges: Requested partition values as YYYY-MM-DD strings.

    Returns:
        The subset of `base/{partition_key}={day}` paths that exist, in request
        order; an empty list if `base` itself is absent or nothing matches.
    """
    jvm = spark._jvm
    # Set whenever the session runs on a JVM, which every session here does.
    assert jvm is not None
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path_cls = jvm.org.apache.hadoop.fs.Path
    base_path = path_cls(base)
    fs = base_path.getFileSystem(hadoop_conf)

    if not fs.exists(base_path):
        return []

    present = {status.getPath().getName() for status in fs.listStatus(base_path)}
    return [
        f"{base}/{partition_key}={day}"
        for day in date_ranges
        if f"{partition_key}={day}" in present
    ]


def write_pyspark_dataframe(
    output_dataframe: DataFrame,
    output_file_path: str,
    file_format: str = "parquet",
    partition_key: str | None = None,
) -> None:
    """Write a job's output, replacing only the partitions the DataFrame holds.

    Always overwrites. A partitioned write uses dynamic partition overwrite, so
    re-running a job for one date replaces that date's folder and leaves every other
    date untouched; an unpartitioned write replaces the whole dataset.

    Args:
        output_dataframe: The validated output rows.
        output_file_path: The dataset's root folder, e.g. s3://bucket/orders_clean/.
        file_format: The format to write, e.g. parquet.
        partition_key: The column to partition by; None for an unpartitioned dataset.
    """
    if partition_key is None:
        output_dataframe.write.mode("overwrite").format(file_format).save(output_file_path)

    else:
        (
            output_dataframe.write.mode("overwrite")
            .option("partitionOverwriteMode", "dynamic")
            .format(file_format)
            .partitionBy(partition_key)
            .save(output_file_path)
        )
