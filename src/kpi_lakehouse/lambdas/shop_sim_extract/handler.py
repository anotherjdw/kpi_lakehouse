"""The extract Lambda: Postgres → raw-layer files in S3, once a day shortly after midnight UTC.

  orders, returns   rows changed since the last run (by _ingested_at), partitioned by the
                    date of updated_at as export_date. Only the latest version of a row is in
                    Postgres, so two status changes between runs export only the second: the
                    same trade-off a real batch export makes.
  order_items       rows inserted since the last run, partitioned by order_date
  customers,        full snapshot, snapshot_date = yesterday (the day the snapshot closes)
  products
  categories        full export, replacing the previous one

The upper bound is EXTRACT_LAG_MINUTES (2) before the run,
so rows still being committed are picked up
next time; the watermark advances only after the files are written.

Environment variables: DATA_BUCKET (required), RAW_PREFIX (raw), EXTRACT_LAG_MINUTES (2), plus
the DB_* variables of db.py. Invoke with {"snapshot_date": "YYYY-MM-DD"}
to override the snapshot date.
"""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import boto3

from kpi_lakehouse.lambdas.common.contracts import Contract, fmt_ts, load_contracts
from kpi_lakehouse.lambdas.common.db import connect
from kpi_lakehouse.lambdas.common.run_record import lambda_run, phase
from kpi_lakehouse.lambdas.common.sinks import RawWriter, S3Store

from .tables import EXPORT_ONLY, table_columns

INCREMENTAL = ("orders", "order_items", "returns")
SNAPSHOT = ("customers", "products")


def to_wire(value: Any) -> Any:
    """Converts one database value into its JSON-safe form.

    This lets rows read from Postgres be checked against the contracts and written as JSON.

    Args:
        value: A value from a Postgres row.

    Returns:
        Timestamps and dates as ISO strings, decimals as strings, anything else unchanged.
    """
    if isinstance(value, datetime):
        return fmt_ts(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def select_rows(
    conn: Any, dataset: str, contracts: dict[str, Contract], where: str = "", **params: Any
) -> list[dict[str, Any]]:
    """Runs one SELECT on a dataset's table and returns its rows as dictionaries.

    This is the single place the extract reads data from the shop's database.

    Args:
        conn: An open database connection.
        dataset: The table to read, e.g. "orders".
        contracts: The raw data contracts, keyed by dataset.
        where: An optional WHERE clause, with named parameters.
        **params: Values for the WHERE clause's named parameters.

    Returns:
        One dictionary per row, keyed by column name, with values in JSON-safe form.
    """
    names = [c.name for c in table_columns(contracts[dataset])]
    result = conn.run(f"SELECT {', '.join(names)} FROM {dataset} {where}", **params)
    return [{n: to_wire(v) for n, v in zip(names, row, strict=True)} for row in result]


def extract_settings_from_env(event: dict[str, Any] | None) -> tuple[datetime, datetime, str]:
    """Works out the run time, the export cut-off and the snapshot date for this run.

    This fixes the time window and snapshot date that every dataset in the run uses.

    Args:
        event: The Lambda event; it may set "snapshot_date" as YYYY-MM-DD.

    Returns:
        The run time, the cut-off for incremental rows, and the snapshot date.
    """
    run_at = datetime.now(UTC).replace(tzinfo=None)
    upper = run_at - timedelta(minutes=int(os.environ.get("EXTRACT_LAG_MINUTES", "2")))
    yesterday = (run_at - timedelta(days=1)).date().isoformat()
    snapshot_date = (event or {}).get("snapshot_date") or yesterday

    return run_at, upper, snapshot_date


def read_watermark(conn: Any, dataset: str) -> datetime:
    """Reads the time up to which a dataset was last exported.

    This lets each run export only the rows added or changed since the previous run.

    Args:
        conn: An open database connection.
        dataset: The dataset to look up, e.g. "orders".

    Returns:
        The stored watermark, or 1970-01-01 if the dataset has never been exported.
    """
    found = conn.run("SELECT high FROM _extract_watermark WHERE dataset = :d", d=dataset)
    lower = found[0][0] if found else datetime(1970, 1, 1)

    return lower


def extract_incremental(
    conn: Any, dataset: str, contracts: dict[str, Contract], lower: datetime, upper: datetime
) -> list[dict[str, Any]]:
    """Reads a dataset's rows ingested after the lower bound and up to the upper bound.

    This exports only what changed since the last run, rather than the whole table.

    Args:
        conn: An open database connection.
        dataset: The dataset to read, e.g. "orders".
        contracts: The raw data contracts, keyed by dataset.
        lower: The previous watermark; rows ingested after it are included.
        upper: This run's cut-off; rows ingested up to it are included.

    Returns:
        The changed rows, as dictionaries.
    """
    return select_rows(
        conn,
        dataset,
        contracts,
        "WHERE _ingested_at > :lower AND _ingested_at <= :upper",
        lower=lower,
        upper=upper,
    )


def with_export_date(dataset: str, row: dict[str, Any]) -> dict[str, Any]:
    """Adds the export date, taken from updated_at, to a row of a dataset that needs one.

    This gives orders and returns the partition column their raw files are organised by.

    Args:
        dataset: The row's dataset, e.g. "orders".
        row: One row, as a dictionary.

    Returns:
        The row with export_date added, or unchanged for datasets without one.
    """
    if dataset in EXPORT_ONLY:
        row = {EXPORT_ONLY[dataset]: row["updated_at"][:10], **row}

    return row


def write_rows(
    contracts: dict[str, Contract], dataset: str, row: dict[str, Any], raw: RawWriter
) -> None:
    """Checks one row against its contract and adds it to the files waiting to be written.

    This makes sure no row that breaks its contract reaches the raw layer.

    Args:
        contracts: The raw data contracts, keyed by dataset.
        dataset: The row's dataset, e.g. "orders".
        row: One row, as a dictionary.
        raw: The writer that collects rows until they are flushed to S3.
    """
    contracts[dataset].check(row)
    raw.add(dataset, row)


def extract_snapshot(
    conn: Any, dataset: str, contracts: dict[str, Contract]
) -> list[dict[str, Any]]:
    """Reads every row of a dataset's table.

    This captures the full current state of datasets exported as daily snapshots.

    Args:
        conn: An open database connection.
        dataset: The dataset to read, e.g. "customers".
        contracts: The raw data contracts, keyed by dataset.

    Returns:
        All rows, as dictionaries.
    """
    return select_rows(conn, dataset, contracts)


def with_snapshot_date(snapshot_date: str, row: dict[str, Any]) -> dict[str, Any]:
    """Adds the snapshot date to a row.

    This gives snapshot rows the partition column their raw files are organised by.

    Args:
        snapshot_date: The day the snapshot reflects, as YYYY-MM-DD.
        row: One row, as a dictionary.

    Returns:
        The row with snapshot_date added.
    """
    return {"snapshot_date": snapshot_date, **row}


def extract_full_categories(conn: Any, contracts: dict[str, Contract]) -> list[dict[str, Any]]:
    """Reads every row of the categories table.

    This exports the complete category list, which replaces the previous export each run.

    Args:
        conn: An open database connection.
        contracts: The raw data contracts, keyed by dataset.

    Returns:
        All category rows, as dictionaries.
    """
    return select_rows(conn, "categories", contracts)


def save_watermarks(watermarks: dict[str, datetime], conn: Any) -> None:
    """Stores this run's cut-off as the new watermark for each incremental dataset.

    This makes the next run start where this one ended, so no rows are missed or repeated.

    Args:
        watermarks: The new watermark per dataset.
        conn: An open database connection.
    """
    for dataset, high in watermarks.items():
        conn.run(
            "INSERT INTO _extract_watermark (dataset, high) VALUES (:d, :h) "
            "ON CONFLICT (dataset) DO UPDATE SET high = EXCLUDED.high",
            d=dataset,
            h=high,
        )


def handler(event: dict[str, Any] | None, context: object) -> dict[str, int]:
    """Export the shop's changed rows and snapshots to raw files, then move the watermarks.

    Runs inside `lambda_run`, which prints one run record when the invocation ends, with
    the rows written per dataset in its `metrics`.

    Args:
        event: The invocation's payload; `snapshot_date` overrides the snapshot date.
        context: Lambda's context object; None in local runs and tests.

    Returns:
        The rows written per dataset.
    """
    with lambda_run(context, local_name="shop-sim-extract") as record:
        summary: dict[str, int] = {}
        record.metrics["rows"] = summary

        with phase(record, "connect"):
            contracts = load_contracts()
            run_at, upper, snapshot_date = extract_settings_from_env(event)
            store = S3Store(
                boto3.client("s3"), os.environ["DATA_BUCKET"], os.environ.get("RAW_PREFIX", "raw")
            )
            raw = RawWriter(store, contracts)
            conn = connect()

        watermarks: dict[str, datetime] = {}
        with phase(record, "incremental"):
            for dataset in INCREMENTAL:
                lower = read_watermark(conn, dataset)
                rows = extract_incremental(conn, dataset, contracts, lower, upper)

                for row in rows:
                    row = with_export_date(dataset, row)
                    write_rows(contracts, dataset, row, raw)

                watermarks[dataset] = upper
                summary[dataset] = len(rows)

        with phase(record, "snapshot"):
            for dataset in SNAPSHOT:
                rows = extract_snapshot(conn, dataset, contracts)
                for row in rows:
                    row = with_snapshot_date(snapshot_date, row)
                    write_rows(contracts, dataset, row, raw)

                summary[dataset] = len(rows)

        with phase(record, "categories"):
            categories = extract_full_categories(conn, contracts)

            for row in categories:
                write_rows(contracts, "categories", row, raw)

            summary["categories"] = len(categories)

        with phase(record, "write"):
            raw.flush(tag=f"extract-{run_at:%Y%m%dT%H%M}")

        with phase(record, "watermarks"):
            save_watermarks(watermarks, conn)

        conn.close()
        return summary
