"""The ALTER TABLE statements that register a job's partitions in its Hive table.

build_add_partition_statements is pure string building, so these tests need no Spark
session and no catalog.
"""

from datetime import date, timedelta

from kpi_lakehouse.utils.catalog import build_add_partition_statements

TABLE = "db.orders_per_shop"
LOCATION = "s3://bucket/orders_per_shop"


def days(start: date, count: int) -> list[str]:
    """`count` consecutive dates from `start`, as YYYY-MM-DD strings."""
    return [str(start + timedelta(days=i)) for i in range(count)]


def test_single_partition_statement() -> None:
    """One date gives one statement adding one partition at its folder."""
    statements = build_add_partition_statements(TABLE, "date_utc", ["2026-01-01"], LOCATION)

    assert statements == [
        "ALTER TABLE db.orders_per_shop ADD IF NOT EXISTS\n"
        "PARTITION (date_utc = '2026-01-01') "
        "LOCATION 's3://bucket/orders_per_shop/date_utc=2026-01-01/'"
    ]


def test_lookback_week_fits_in_one_statement_in_ascending_order() -> None:
    """A week of dates is one statement, its partitions in date order."""
    values = days(date(2026, 1, 1), 7)

    statements = build_add_partition_statements(TABLE, "date_utc", values, LOCATION)

    assert len(statements) == 1
    positions = [statements[0].index(f"date_utc = '{value}'") for value in values]
    assert positions == sorted(positions)


def test_backfill_is_split_into_batches_of_100() -> None:
    """A backfill of 250 dates is split into statements of at most 100 partitions."""
    values = days(date(2026, 1, 1), 250)

    statements = build_add_partition_statements(TABLE, "date_utc", values, LOCATION)

    assert [s.count("PARTITION (") for s in statements] == [100, 100, 50]
    assert f"date_utc = '{values[99]}'" in statements[0]
    assert f"date_utc = '{values[100]}'" in statements[1]
    assert f"date_utc = '{values[249]}'" in statements[2]


def test_duplicate_and_unsorted_values_are_registered_once_in_order() -> None:
    """A date listed twice is added once, and dates are sorted before batching."""
    statements = build_add_partition_statements(
        TABLE, "date_utc", ["2026-01-02", "2026-01-01", "2026-01-02"], LOCATION
    )

    assert statements == build_add_partition_statements(
        TABLE, "date_utc", ["2026-01-01", "2026-01-02"], LOCATION
    )
    assert statements[0].count("PARTITION (") == 2


def test_trailing_slash_on_output_path_does_not_change_location() -> None:
    """The partition location is the same with or without a trailing slash on the path."""
    with_slash = build_add_partition_statements(TABLE, "date_utc", ["2026-01-01"], f"{LOCATION}/")
    without_slash = build_add_partition_statements(TABLE, "date_utc", ["2026-01-01"], LOCATION)

    assert with_slash == without_slash


def test_no_partition_values_produce_no_statements() -> None:
    """No dates, no statements: an empty write registers nothing."""
    assert build_add_partition_statements(TABLE, "date_utc", [], LOCATION) == []
