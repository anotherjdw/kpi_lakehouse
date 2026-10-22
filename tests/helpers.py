"""Helpers shared across test levels: assertions, fixture rows and files, and catalog tables."""

import base64
import json
from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, NamedTuple

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import DataType, StructType


class SourceFixture(NamedTuple):
    """How a job reads one source, and the input rows its tests write for it.

    Attributes:
        argument: The Glue argument holding the source's path, e.g. ORDERS_INPUT_PATH.
        rows: The source's input rows.
        schema: The source's StructType.
        file_format: How the source's files are stored: json or parquet.
        partition_key: The source's date partition column; None when it is unpartitioned.
    """

    argument: str
    rows: list[Row]
    schema: StructType
    file_format: str
    partition_key: str | None


def column_types(schema: StructType) -> dict[str, DataType]:
    """Map each column name to its data type, ignoring nullable.

    Comparing these dicts is the schema check for DataFrames read back from files:
    Spark marks every column read from a file as nullable, so comparing StructFields
    directly fails whenever the expected schema has a non-nullable field.
    """
    return {field.name: field.dataType for field in schema.fields}


def row_key(row: Row) -> tuple[tuple[bool, Any], ...]:
    """Sort key for comparing collected rows regardless of order.

    Pairs each value with whether it is None, so rows holding nulls sort without
    ever comparing None to a value.
    """
    return tuple((value is None, value) for value in row)


def rows_to_df(spark: SparkSession, rows: list[Row], schema: StructType) -> DataFrame:
    """Build a DataFrame from fixture rows, matching fields to the schema by name.

    `createDataFrame` maps a Row's fields to the schema by position, so a row whose
    fields are in a different order would fail, or with matching types silently swap
    values. Converting each Row to a dict first makes the match by name.
    """
    return spark.createDataFrame([row.asDict() for row in rows], schema)


def _json_value(value: Any) -> str:
    """Write a value the json module can't as the string Spark's JSON reader parses back.

    Dates and timestamps become ISO strings, decimals their exact digits and binary values
    base64, which is how Spark reads a JSON string into each of those types.
    """
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes | bytearray):
        return base64.b64encode(value).decode("ascii")
    raise TypeError(f"Can't write {value!r} to a JSON fixture file.")


def write_source_fixture(
    spark: SparkSession,
    rows: list[Row],
    schema: StructType,
    path: Path,
    file_format: str,
    partition_key: str | None,
) -> None:
    """Write a source's input rows to disk in the source's file format and partition layout.

    Every value is first checked against `schema`. JSON is written as one JSON array per
    partition folder with the json module, and the job reads each file whole (multiLine):
    Spark's JSON writer puts one object per line, of which such a read keeps only the
    first. The partition column is left out of the JSON objects, since the folder name
    holds its value. Parquet is written by Spark, partitioned by `partition_key` when set.
    """
    df = rows_to_df(spark, rows, schema)
    if file_format == "parquet":
        writer = df.write
        if partition_key is not None:
            writer = writer.partitionBy(partition_key)
        writer.parquet(str(path))
        return
    files: dict[Path, list[dict[str, Any]]] = {}
    for row in rows:
        values = row.asDict()
        folder = path
        if partition_key is not None:
            folder = path / f"{partition_key}={values.pop(partition_key).isoformat()}"
        files.setdefault(folder, []).append(values)
    for folder, objects in files.items():
        folder.mkdir(parents=True, exist_ok=True)
        text = json.dumps(objects, default=_json_value)
        (folder / "part-00000.json").write_text(text, encoding="utf-8")


def write_sources(
    spark: SparkSession, sources: Mapping[str, SourceFixture], root: Path
) -> list[str]:
    """Write every source's input rows under `root`, in a folder named after its alias.

    Returns:
        The job's input path arguments for what was written, e.g.
        ["--ORDERS_INPUT_PATH", "<root>/orders"].
    """
    argv = []
    for alias, source in sources.items():
        path = root / alias
        write_source_fixture(
            spark, source.rows, source.schema, path, source.file_format, source.partition_key
        )
        argv += [f"--{source.argument}", str(path)]
    return argv


def registered_partitions(spark: SparkSession, table: str) -> list[str]:
    """The partitions the catalog lists for `table`, e.g. ["date_utc=2026-01-01"]."""
    return sorted(row[0] for row in spark.sql(f"SHOW PARTITIONS {table}").collect())


def create_catalog_table(
    spark: SparkSession, table: str, schema: StructType, partition_key: str, location: Path
) -> None:
    """Create a local external parquet table shaped like a job's Glue table, with no partitions."""
    columns = ", ".join(f"{field.name} {field.dataType.simpleString()}" for field in schema.fields)
    spark.sql(
        f"CREATE TABLE {table} ({columns}) USING parquet "
        f"PARTITIONED BY ({partition_key}) LOCATION '{location}'"
    )


def fixture_dates(rows: list[Row], partition_key: str) -> list[str]:
    """List the partition values rows fall on, as YYYY-MM-DD strings, sorted and unique."""
    return sorted({row[partition_key].isoformat() for row in rows})


def processing_dates(sources: Mapping[str, SourceFixture]) -> dict[str, list[str]]:
    """List, per partitioned source, the dates its input rows fall on: what a run reads."""
    return {
        alias: fixture_dates(source.rows, source.partition_key)
        for alias, source in sources.items()
        if source.partition_key is not None
    }


def inputs_message(job: str) -> str:
    """Say what to do before a test can use a job's input rows: fill them in."""
    return (
        f"tests/fixtures/{job}/inputs.py still holds example rows. Replace them with rows "
        "that exercise the transformations and set INPUTS_FILLED = True, then run: "
        f"engineering-automation generate expected-outputs . {job}"
    )


def review_message(job: str, name: str) -> str:
    """Say what to do before a test can use a transformation's expected rows: review them."""
    return (
        f"tests/fixtures/{job}/expected.py: {name}_ROWS is not reviewed. If it is empty, "
        f"run: engineering-automation generate expected-outputs . {job}. Check and correct "
        f"every row, then set {name}_REVIEWED = True."
    )
