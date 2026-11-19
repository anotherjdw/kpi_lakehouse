"""Read, validate and write the PySpark DataFrames every job in this project handles."""

from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal

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

    Args:
        schema: The contract's schema.

    Returns:
        One count per such column, keyed by column name, as Spark expressions not yet run.
    """
    return {
        field.name: _count(~f.col(field.name).isin(field.metadata["enum"]))
        for field in schema.fields
        if "enum" in field.metadata
    }


# What breaks each bound: the value compared with the bound.
_BOUND_BREAKS: dict[str, Callable[[Column, Column], Column]] = {
    "minimum": lambda value, bound: value < bound,
    "exclusiveMinimum": lambda value, bound: value <= bound,
    "maximum": lambda value, bound: value > bound,
    "exclusiveMaximum": lambda value, bound: value >= bound,
}


def _bound_value(text: str, data_type: t.DataType) -> Decimal | int | float | date | datetime:
    """Turn a bound written as text into a value of its column's type.

    Every bound is written as text in the metadata, so a decimal bound stays exact.

    Args:
        text: The bound as written, e.g. "0" or "2020-01-01".
        data_type: The column's type.

    Returns:
        The bound as a decimal, whole number, float, date or timestamp.

    Raises:
        ValueError: If the column is not a number, date or timestamp.
    """
    if isinstance(data_type, t.DecimalType):
        return Decimal(text)
    if isinstance(data_type, t.IntegralType):
        return int(text)
    if isinstance(data_type, t.FractionalType):
        return float(text)
    if isinstance(data_type, t.TimestampType):
        return datetime.fromisoformat(text)
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
                bound = f.lit(_bound_value(field.metadata[rule], field.dataType))
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
    data: DataFrame, expected_schema: t.StructType, unique_key: list[str] | None = None
) -> None:
    """Check a DataFrame against its contract, reporting every problem found.

    Runs in each job's write() on the output, and in read() on a source with its own
    contract, so a job neither reads nor writes data its contract does not describe.
    First the schema: missing and unexpected columns, and the exact type of every
    shared column. If any of these fail, it raises without reading the data, since the
    other checks need the columns to be there and typed right. Then the data: nulls in
    non-nullable columns, allowed values, bounds, string lengths and patterns, counted
    together in one pass, and duplicate keys. An empty DataFrame passes.

    Args:
        data: The DataFrame to check.
        expected_schema: The contract's schema; the rules are in its fields' metadata.
        unique_key: The key columns, for a schema whose metadata marks no `primaryKey`.
            The metadata wins when it marks one. With neither, the duplicate check
            covers whole rows.

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

    uniqueness = check_no_duplicate_rows(data, subset=primary_key(expected_schema) or unique_key)
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
    check_files: bool = False,
) -> DataFrame:
    """Read one source dataset from files, limited to the requested date partitions.

    Every job's read() calls this once per source. JSON is read with `multiLine` set
    to false, so each line of a file is one JSON record (JSON Lines), as the extract
    Lambda writes raw files. For a partitioned source, only the
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
        check_files: Hold the files to the contract `schema` describes: JSON is read in
            FAILFAST mode with the date and timestamp formats from the schema's metadata.
            Only a source whose contract is in the ODCS format sets it; it needs `schema`.

    Returns:
        The rows read. For a partitioned read where none of the requested partitions
        exist, an empty DataFrame with `schema`.

    Raises:
        ValueError: If a value in `date_ranges` is not a YYYY-MM-DD string; or, with
            `check_files`, if `schema` is None, or if the schema's date or timestamp
            columns set more than one format (see `_json_options`).
    """
    if check_files and schema is None:
        raise ValueError("check_files needs the contract's schema.")
    reader = spark.read.format(file_format).option("multiLine", "false")
    if schema is not None:
        reader = reader.schema(schema)
        if check_files and file_format == "json":
            reader = reader.options(**_json_options(schema))

    paths = [file_path]
    if partition_key and date_ranges:
        validate_dates(date_ranges)
        base = file_path.rstrip("/")
        paths = _existing_partition_paths(spark, base, partition_key, date_ranges)
        if not paths:
            # None of the requested partitions exist. Loading a missing directory
            # (or an empty path list) raises AnalysisException, so return an empty
            # frame with the read schema to preserve the "no data" outcome.
            return spark.createDataFrame([], schema)
        reader = reader.option("basePath", base)

    return reader.load(paths)


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
