"""Register the partitions a job wrote in its Hive table in the Glue Data Catalog."""

from pyspark.sql import DataFrame

# Glue's BatchCreatePartition accepts at most 100 partitions per call.
GLUE_MAX_PARTITIONS_PER_CALL = 100


def build_add_partition_statements(
    table: str,
    partition_key: str,
    partition_values: list[str],
    output_file_path: str,
    batch_size: int = GLUE_MAX_PARTITIONS_PER_CALL,
) -> list[str]:
    """Builds the ALTER TABLE statements that register partitions in the catalog.

    Each statement adds up to `batch_size` partitions, each pointing at the
    `{partition_key}={value}/` directory Spark's partitionBy writes under
    `output_file_path`. IF NOT EXISTS makes the statements idempotent, so reruns
    and backfills that overwrite an existing partition leave the catalog unchanged.

    Args:
        table: Fully qualified table name, <database>.<table>.
        partition_key: The Hive partition column name, e.g. date_utc.
        partition_values: Partition values as written in the directory names,
            e.g. 2026-01-01.
        output_file_path: The table's S3 location the data was written to.
        batch_size: Maximum number of partitions per statement.

    Returns:
        One statement per batch, in ascending partition value order; an empty list
        if there are no partition values.
    """
    base = output_file_path.rstrip("/")
    values = sorted(set(partition_values))

    statements: list[str] = []
    for i in range(0, len(values), batch_size):
        clauses = "\n".join(
            f"PARTITION ({partition_key} = '{value}') LOCATION '{base}/{partition_key}={value}/'"
            for value in values[i : i + batch_size]
        )
        statements.append(f"ALTER TABLE {table} ADD IF NOT EXISTS\n{clauses}")

    return statements


def register_partitions(
    data: DataFrame,
    table: str,
    partition_key: str,
    output_file_path: str,
) -> None:
    """Registers the partitions present in `data` in the table's catalog entry.

    Call directly after writing `data` with partitionBy(partition_key), so exactly
    the partitions that were written get registered. Runs on the DataFrame's own
    SparkSession, which in Glue uses the Glue Data Catalog as its metastore
    (--enable-glue-datacatalog).

    Args:
        data: The DataFrame that was written; persist it beforehand to avoid
            recomputing it for the distinct partition values.
        table: Fully qualified table name, <database>.<table>.
        partition_key: The Hive partition column name, e.g. date_utc.
        output_file_path: The table's S3 location the data was written to.
    """
    partition_values = [
        str(row[partition_key]) for row in data.select(partition_key).distinct().collect()
    ]

    for statement in build_add_partition_statements(
        table=table,
        partition_key=partition_key,
        partition_values=partition_values,
        output_file_path=output_file_path,
    ):
        data.sparkSession.sql(statement)
