"""Read, validate and write the PySpark DataFrames every job in this project handles."""

from collections.abc import Callable
from datetime import UTC, date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import StructType

from .data_quality import check_no_duplicate_rows
from .python import validate_dates


def missing_columns(actual: StructType, expected: StructType) -> list[str]:
    """List the contract's columns that the data doesn't have.

    Args:
        actual: The schema of the data being checked.
        expected: The contract's schema.

    Returns:
        The missing column names, sorted; empty when none is missing.
    """
    return sorted(set(expected.fieldNames()) - set(actual.fieldNames()))


def unexpected_columns(actual: StructType, expected: StructType) -> list[str]:
    """List the data's columns that the contract doesn't have.

    Args:
        actual: The schema of the data being checked.
        expected: The contract's schema.

    Returns:
        The unexpected column names, sorted; empty when there is none.
    """
    return sorted(set(actual.fieldNames()) - set(expected.fieldNames()))


def type_mismatches(actual: StructType, expected: StructType) -> list[str]:
    """List the columns both schemas have whose types differ.

    Decimal precision and scale count; nullable and metadata are ignored. Nothing is
    cast to make a column fit.

    Args:
        actual: The schema of the data being checked.
        expected: The contract's schema.

    Returns:
        One message per mismatched column, in the contract's column order.
    """
    actual_types = {field.name: field.dataType for field in actual.fields}
    return [
        f"'{field.name}': Expected {field.dataType.simpleString()}, "
        f"Received {actual_types[field.name].simpleString()}"
        for field in expected.fields
        if field.name in actual_types and actual_types[field.name] != field.dataType
    ]


def _count(breaks: Column) -> Column:
    """Count the rows where `breaks` is true.

    A null value makes `breaks` null rather than true, so a null never counts against a
    rule; whether a column may hold nulls is the null check's job alone.
    """
    return f.count(f.when(breaks, 1))


def null_violations(schema: StructType) -> dict[str, Column]:
    """Count the nulls in each column the schema marks non-nullable.

    Args:
        schema: The contract's schema.

    Returns:
        One count per non-nullable column, keyed by column name. The counts are Spark
        expressions, not yet run.
    """
    return {
        field.name: _count(f.col(field.name).isNull())
        for field in schema.fields
        if not field.nullable
    }


def enum_violations(schema: StructType) -> dict[str, Column]:
    """Count the values outside the allowed values of each column whose metadata sets `enum`.

    The allowed values of a decimal, date or timestamp column are written as text, since
    metadata is JSON, and are compared as values of the column's type.

    Args:
        schema: The contract's schema.

    Returns:
        One count per such column, keyed by column name, as Spark expressions not yet run.
    """
    return {
        field.name: _count(~f.col(field.name).isin(_allowed_values(field)))
        for field in schema.fields
        if "enum" in field.metadata
    }


def _allowed_values(field: t.StructField) -> list[object]:
    """Read a column's `enum` as values of the column's type.

    Args:
        field: A field whose metadata sets `enum`.

    Returns:
        The allowed values: decimals, dates and timestamps turned from their text, every
        other value as written.
    """
    if isinstance(field.dataType, t.DecimalType | t.DateType | t.TimestampType):
        return [_typed_value(text, field) for text in field.metadata["enum"]]
    values: list[object] = field.metadata["enum"]
    return values


# What breaks each bound: the value compared with the bound.
_BOUND_BREAKS: dict[str, Callable[[Column, Column], Column]] = {
    "minimum": lambda value, bound: value < bound,
    "exclusiveMinimum": lambda value, bound: value <= bound,
    "maximum": lambda value, bound: value > bound,
    "exclusiveMaximum": lambda value, bound: value >= bound,
}


def _typed_value(text: str, field: t.StructField) -> Decimal | int | float | date | datetime:
    """Turn a bound, or an allowed value, written as text into a value of its column's type.

    Every bound is written as text in the metadata, and so is every allowed value of a
    decimal, date or timestamp column, so a decimal stays exact. A timestamp written
    without an offset is read in the column's `defaultTimezone`, or in UTC when it sets
    none, so it means the same instant wherever the job runs: Spark would otherwise read
    it in the time zone of the machine running the job.

    Args:
        text: The value as written, e.g. "0", "2020-01-01" or "2026-01-01T00:00:00".
        field: The column, whose type the value takes.

    Returns:
        The value as a decimal, whole number, float, date or timestamp with a time zone.

    Raises:
        ValueError: If the column is not a number, date or timestamp.
    """
    data_type = field.dataType
    if isinstance(data_type, t.DecimalType):
        return Decimal(text)
    if isinstance(data_type, t.IntegralType):
        return int(text)
    if isinstance(data_type, t.FractionalType):
        return float(text)
    if isinstance(data_type, t.TimestampType):
        value = datetime.fromisoformat(text)
        if value.tzinfo is not None:
            return value
        zone = field.metadata.get("defaultTimezone")
        return value.replace(tzinfo=ZoneInfo(zone) if zone else UTC)
    if isinstance(data_type, t.DateType):
        return date.fromisoformat(text)
    raise ValueError(
        f"A bound needs a number, date or timestamp column; got {data_type.simpleString()}."
    )


def bound_violations(schema: StructType) -> dict[str, Column]:
    """Count the values that break each bound a column's metadata sets.

    Args:
        schema: The contract's schema.

    Returns:
        One count per column and bound, keyed like "shipping_fee (minimum 0)", as Spark
        expressions not yet run.

    Raises:
        ValueError: If a bound is set on a column that is not a number, date or timestamp.
    """
    counts = {}
    for field in schema.fields:
        for rule, breaks in _BOUND_BREAKS.items():
            if rule in field.metadata:
                bound = f.lit(_typed_value(field.metadata[rule], field))
                key = f"{field.name} ({rule} {field.metadata[rule]})"
                counts[key] = _count(breaks(f.col(field.name), bound))
    return counts


def length_violations(schema: StructType) -> dict[str, Column]:
    """Count the strings shorter than `minLength` or longer than `maxLength`.

    Args:
        schema: The contract's schema.

    Returns:
        One count per column and rule, keyed like "order_id (maxLength 13)", as Spark
        expressions not yet run.
    """
    counts = {}
    for field in schema.fields:
        length = f.length(f.col(field.name))
        if "minLength" in field.metadata:
            minimum = field.metadata["minLength"]
            counts[f"{field.name} (minLength {minimum})"] = _count(length < minimum)
        if "maxLength" in field.metadata:
            maximum = field.metadata["maxLength"]
            counts[f"{field.name} (maxLength {maximum})"] = _count(length > maximum)
    return counts


def pattern_violations(schema: StructType) -> dict[str, Column]:
    """Count the strings that don't match their column's `pattern`.

    Spark's `rlike` finds the pattern anywhere in the value, so a pattern must start
    with `^` and end with `$` to describe the whole value.

    Args:
        schema: The contract's schema.

    Returns:
        One count per column with a pattern, keyed by column name, as Spark expressions
        not yet run.
    """
    return {
        field.name: _count(~f.col(field.name).rlike(field.metadata["pattern"]))
        for field in schema.fields
        if "pattern" in field.metadata
    }


def primary_key(schema: StructType) -> list[str]:
    """List the columns whose metadata marks them `primaryKey`, in the schema's order.

    Args:
        schema: The contract's schema.

    Returns:
        The primary-key column names; empty when the schema marks none.
    """
    return [field.name for field in schema.fields if field.metadata.get("primaryKey")]


def validate_pyspark_dataframe(
    data: DataFrame, expected_schema: t.StructType, check_duplicates: bool = True
) -> None:
    """Check a DataFrame against its contract, reporting every problem found.

    Runs in each job's write() on the output, and in read() on a source with its own
    contract, so a job neither reads nor writes data its contract does not describe.
    read() leaves out the duplicate check: a botched ingestion can repeat files or rows,
    and the job's own transformations can remove them.
    First the schema: missing and unexpected columns, and the exact type of every
    shared column. If any of these fail, it raises without reading the data, since the
    other checks need the columns to be there and typed right. Then the data: nulls in
    non-nullable columns, allowed values, bounds, string lengths and patterns, counted
    together in one pass, and duplicates over the columns the metadata marks
    `primaryKey`, or over whole rows when it marks none. An empty DataFrame passes.

    Args:
        data: The DataFrame to check.
        expected_schema: The contract's schema; the rules are in its fields' metadata.
        check_duplicates: Whether to check for duplicates; read() passes False.

    Raises:
        ValueError: "Schema validation failed: ..." listing every schema problem, or
            "Data validation failed: ..." listing every rule broken, with the number of
            rows breaking it.
    """
    schema_problems = []
    if missing := missing_columns(data.schema, expected_schema):
        schema_problems.append(f"Missing columns: {missing}")
    if unexpected := unexpected_columns(data.schema, expected_schema):
        schema_problems.append(f"Unexpected columns: {unexpected}")
    if mismatches := type_mismatches(data.schema, expected_schema):
        schema_problems.append(f"Type mismatches: {', '.join(mismatches)}")
    if schema_problems:
        raise ValueError("Schema validation failed:\n- " + "\n- ".join(schema_problems))

    rule_counts = {
        "nulls in non-nullable columns": null_violations(expected_schema),
        "values outside the allowed values": enum_violations(expected_schema),
        "values out of bounds": bound_violations(expected_schema),
        "strings of the wrong length": length_violations(expected_schema),
        "strings not matching the pattern": pattern_violations(expected_schema),
    }
    checks = [
        (problem, name, count)
        for problem, counts in rule_counts.items()
        for name, count in counts.items()
    ]
    data_problems = []
    if checks:
        row = data.select([count.alias(f"c{i}") for i, (_, _, count) in enumerate(checks)])
        totals = row.first()
        # A global aggregation always returns exactly one row, even on an empty DataFrame.
        assert totals is not None
        found: dict[str, dict[str, int]] = {}
        for i, (problem, name, _) in enumerate(checks):
            if totals[i] > 0:
                found.setdefault(problem, {})[name] = totals[i]
        data_problems = [f"{problem} {counts}" for problem, counts in found.items()]

    if check_duplicates:
        uniqueness = check_no_duplicate_rows(data, subset=primary_key(expected_schema) or None)
        if not uniqueness.passed:
            data_problems.append(uniqueness.message)
    if data_problems:
        raise ValueError("Data validation failed: " + "; ".join(data_problems))


def _json_options(schema: StructType) -> dict[str, str]:
    """Build the JSON reader options a contract's schema asks for.

    The source is read in FAILFAST mode, so a value that doesn't fit its column's type,
    or a record that isn't valid JSON, fails the read instead of becoming null. The
    `format` of the date and timestamp columns sets how Spark reads their text, and
    `defaultTimezone` the zone of values that carry none. Spark takes one layout per read,
    so all date columns must share one `format`, and all timestamp columns another.

    Args:
        schema: The source contract's schema, with each column's rules in its metadata.

    Returns:
        The options to pass to Spark's JSON reader.

    Raises:
        ValueError: If two date columns, or two timestamp columns, set different formats,
            or two columns set different default time zones.
    """
    options = {"mode": "FAILFAST"}
    for option, key, kind in (
        ("dateFormat", "format", t.DateType),
        ("timestampFormat", "format", t.TimestampType),
        ("timeZone", "defaultTimezone", t.TimestampType),
    ):
        values = {
            field.metadata[key]
            for field in schema.fields
            if isinstance(field.dataType, kind) and key in field.metadata
        }
        if len(values) > 1:
            raise ValueError(
                f"Spark reads one {option} per source; the schema sets {sorted(values)}."
            )
        if values:
            options[option] = values.pop()
    return options


def create_pyspark_dataframe(
    spark: SparkSession,
    file_path: str,
    schema: StructType | None = None,
    file_format: str = "json",
    date_ranges: list[str] | None = None,
    partition_key: str | None = None,
) -> DataFrame:
    """Read one source dataset from files, limited to the requested date partitions.

    Every job's read() calls this once per source. A `json` file is one JSON document,
    such as an array of records, read with `multiLine`; a `jsonl` file holds one JSON
    record per line (JSON Lines). With a `schema`, both are also read in FAILFAST mode
    with the date and timestamp formats from the schema's metadata (see `_json_options`).
    For a partitioned source, only the
    `{partition_key}={day}` folders that exist are loaded, with `basePath` set so the
    partition column stays in the result.

    Args:
        spark: The active SparkSession.
        file_path: The dataset's root folder, e.g. s3://bucket/orders/.
        schema: The schema to read with; None lets Spark infer one.
        file_format: The files' format: json, jsonl or parquet.
        date_ranges: Partition values to read, as YYYY-MM-DD strings.
        partition_key: The dataset's date partition column; None for an unpartitioned
            dataset, which is read whole.

    Returns:
        The rows read. For a partitioned read where none of the requested partitions
        exist, an empty DataFrame with `schema`.

    Raises:
        ValueError: If a value in `date_ranges` is not a YYYY-MM-DD string, or if the
            schema's date or timestamp columns set more than one format (see
            `_json_options`).
    """
    # Spark reads both JSON shapes with its json reader; multiLine tells them apart.
    spark_format = {"json": "json", "jsonl": "json", "parquet": "parquet"}[file_format]
    multi_line = "true" if file_format == "json" else "false"
    reader = spark.read.format(spark_format).option("multiLine", multi_line)
    if schema is not None:
        reader = reader.schema(schema)
        if spark_format == "json":
            reader = reader.options(**_json_options(schema))

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
