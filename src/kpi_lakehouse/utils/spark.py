"""The SparkSession every job in this project runs on."""

from pyspark.sql import SparkSession


def spark_session() -> SparkSession:
    """Return the active SparkSession, creating one if none exists.

    In Glue this picks up the session Glue has configured. Jobs read and write files
    by path. In Glue, `--enable-glue-datacatalog` makes the Glue Data Catalog this
    session's catalog, which a job with a table uses only to register the partitions
    it wrote; no Hive support is enabled here.

    Returns:
        The active SparkSession.
    """
    return SparkSession.builder.getOrCreate()
