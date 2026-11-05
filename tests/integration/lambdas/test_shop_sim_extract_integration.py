"""Integration tests of the shop_sim_extract Lambda against a real local Postgres.

pytest-postgresql starts a throwaway Postgres server when the first test here needs it, and
stops it and deletes its data when the session ends. The server binaries come from pgserver, a
test dependency in pyproject.toml, so `pip install -e ".[test]"` is the only setup. The shop's
tables come from tests/fixtures/lambdas/shop_schema.sql and are emptied before every test. S3 is
replaced by an in-memory store; no Docker and no AWS access are needed.

Without a working PostgreSQL (pgserver's, then pg_ctl on PATH, Homebrew or /usr/lib/postgresql)
these tests are skipped locally, but fail in CI (where $CI is set), so they never silently stop
running there.
"""

import glob
import importlib.util
import os
import shutil
import subprocess
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pg8000.native
import pytest
from pytest_postgresql import factories
from pytest_postgresql.executors import PostgreSQLExecutor

from kpi_lakehouse.lambdas.common.contracts import Column, Contract, load_contracts
from kpi_lakehouse.lambdas.common.db import connect
from kpi_lakehouse.lambdas.common.sinks import RawWriter
from kpi_lakehouse.lambdas.shop_sim_extract import handler as extract

SCHEMA = Path(__file__).parents[2] / "fixtures" / "lambdas" / "shop_schema.sql"
TABLES = ["categories", "products", "customers", "orders", "order_items", "returns"]
DB_NAME = "shop_test"
DB_PASSWORD = "test"


def find_pg_ctl() -> str | None:
    """The pg_ctl of a PostgreSQL install whose server binary actually runs, or None.

    The server pgserver installs with the test dependencies comes first, so the tests use the
    same PostgreSQL wherever `pip install -e ".[test]"` ran; a system install is the fallback.
    """
    spec = importlib.util.find_spec("pgserver")
    bundled = Path(spec.origin).parent / "pginstall" / "bin" / "pg_ctl" if spec else None
    candidates = [str(bundled) if bundled else None, shutil.which("pg_ctl")]
    candidates += sorted(glob.glob("/opt/homebrew/opt/postgresql*/bin/pg_ctl"), reverse=True)
    candidates += sorted(glob.glob("/usr/local/opt/postgresql*/bin/pg_ctl"), reverse=True)
    candidates += sorted(glob.glob("/usr/lib/postgresql/*/bin/pg_ctl"), reverse=True)
    for pg_ctl in candidates:
        if not pg_ctl:
            continue
        postgres = Path(pg_ctl).with_name("postgres")
        try:
            result = subprocess.run(
                [str(postgres), "--version"], capture_output=True, check=False, timeout=10
            )
        except OSError:
            continue
        if result.returncode == 0:
            return pg_ctl
    return None


PG_CTL = find_pg_ctl()
NO_POSTGRES = "no working PostgreSQL install found (pg_ctl and postgres)"

if PG_CTL is None:

    @pytest.fixture(scope="session")
    def shop_postgresql_proc() -> PostgreSQLExecutor:
        """Stands in for the server when PostgreSQL is missing: fail in CI, skip elsewhere."""
        if os.environ.get("CI"):
            pytest.fail(NO_POSTGRES)
        pytest.skip(NO_POSTGRES)

else:
    shop_postgresql_proc = factories.postgresql_proc(
        executable=PG_CTL, port=None, password=DB_PASSWORD
    )


@pytest.fixture(scope="session")
def shop_database(shop_postgresql_proc: PostgreSQLExecutor) -> Iterator[None]:
    """Creates the shop database and its tables, and points common.db.connect() at it."""
    server = shop_postgresql_proc
    admin = pg8000.native.Connection(
        user=server.user,
        host=server.host,
        port=server.port,
        database="postgres",
        password=DB_PASSWORD,
    )
    admin.run(f"CREATE DATABASE {DB_NAME}")
    admin.close()
    with pytest.MonkeyPatch.context() as env:
        env.setenv("DB_HOST", server.host)
        env.setenv("DB_PORT", str(server.port))
        env.setenv("DB_USER", server.user)
        env.setenv("DB_PASSWORD", DB_PASSWORD)
        env.setenv("DB_NAME", DB_NAME)
        env.setenv("DB_SSL", "false")
        conn = connect()
        for statement in SCHEMA.read_text().split(";"):
            sql = "\n".join(
                line for line in statement.splitlines() if not line.startswith("--")
            ).strip()
            if sql:
                conn.run(sql)
        conn.close()
        yield


@pytest.fixture
def conn(shop_database: None) -> Iterator[pg8000.native.Connection]:
    """A connection from common.db.connect() to empty shop tables."""
    connection = connect()
    connection.run(f"TRUNCATE {', '.join(TABLES)}, _extract_watermark")
    yield connection
    connection.close()


@pytest.fixture(scope="module")
def contracts() -> dict[str, Contract]:
    """The raw data contracts from src/kpi_lakehouse/contracts/."""
    return load_contracts()


class FakeS3:
    """Records put_object calls, instead of writing to S3."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:  # noqa: N803
        self.objects[Key] = Body


def db_value(column: Column | None, data_type: str, n: int) -> Any:
    """A value for one database column, valid for its contract column when there is one."""
    kind = column.type if column else data_type
    if kind in ("string", "text"):
        return f"v{n}"
    if kind in ("int", "bigint", "smallint", "tinyint", "integer"):
        return column.min if column and column.min is not None else n
    if kind in ("double", "float", "double precision"):
        return 1.5
    if kind == "boolean":
        return True
    if kind == "date":
        return date(2026, 11, 2)
    if kind in ("timestamp", "timestamp without time zone"):
        return datetime(2026, 11, 2, 10, 0, n % 60)
    return Decimal("1.00")  # decimal(p,s) / numeric


def insert(
    conn: pg8000.native.Connection,
    contracts: dict[str, Contract],
    table: str,
    n: int,
    ingested_at: datetime,
) -> None:
    """Inserts the n-th test row into a shop table, ingested at the given time."""
    columns = conn.run(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = :t AND column_name <> '_ingested_at' ORDER BY ordinal_position",
        t=table,
    )
    by_name = {c.name: c for c in contracts[table].columns}
    values = {name: db_value(by_name.get(name), data_type, n) for name, data_type in columns}
    names = [*values, "_ingested_at"]
    params = {f"p{i}": v for i, v in enumerate([*values.values(), ingested_at])}
    conn.run(
        f"INSERT INTO {table} ({', '.join(names)}) VALUES ({', '.join(':' + p for p in params)})",
        **params,
    )


def utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


# --- select_rows -------------------------------------------------------------------------


def test_select_rows_returns_wire_rows(
    conn: pg8000.native.Connection, contracts: dict[str, Contract]
) -> None:
    """select_rows reads every backend column, with values converted to wire format."""
    insert(conn, contracts, "orders", 1, utc_now())
    rows = extract.select_rows(conn, "orders", contracts)
    assert len(rows) == 1
    row = rows[0]
    assert "export_date" not in row
    assert "_ingested_at" not in row
    assert row["updated_at"] == "2026-11-02T10:00:01.000Z"
    contracts["orders"].check(extract.with_export_date("orders", row))


# --- read_watermark / save_watermarks ----------------------------------------------------


def test_read_watermark_without_stored_value(conn: pg8000.native.Connection) -> None:
    """read_watermark returns 1970-01-01 for a dataset that was never exported."""
    assert extract.read_watermark(conn, "orders") == datetime(1970, 1, 1)


def test_save_watermarks_inserts_then_updates(conn: pg8000.native.Connection) -> None:
    """save_watermarks stores a new watermark and replaces it on the next save."""
    first = datetime(2026, 11, 2, 0, 28)
    second = datetime(2026, 11, 3, 0, 28)
    extract.save_watermarks({"orders": first}, conn)
    assert extract.read_watermark(conn, "orders") == first
    extract.save_watermarks({"orders": second}, conn)
    assert extract.read_watermark(conn, "orders") == second
    assert conn.run("SELECT count(*) FROM _extract_watermark")[0][0] == 1


# --- extract_incremental / extract_snapshot / extract_full_categories ---------------------


def test_extract_incremental_reads_only_the_window(
    conn: pg8000.native.Connection, contracts: dict[str, Contract]
) -> None:
    """extract_incremental returns rows ingested after lower and up to upper, inclusive."""
    lower = datetime(2026, 11, 2, 0, 0)
    upper = datetime(2026, 11, 3, 0, 0)
    insert(conn, contracts, "orders", 1, lower)  # at lower: excluded
    insert(conn, contracts, "orders", 2, lower + timedelta(hours=1))  # inside
    insert(conn, contracts, "orders", 3, upper)  # at upper: included
    insert(conn, contracts, "orders", 4, upper + timedelta(seconds=1))  # after upper: excluded
    rows = extract.extract_incremental(conn, "orders", contracts, lower, upper)
    assert sorted(r["order_id"] for r in rows) == ["v2", "v3"]


def test_extract_snapshot_reads_every_row(
    conn: pg8000.native.Connection, contracts: dict[str, Contract]
) -> None:
    """extract_snapshot returns every row, whenever it was ingested."""
    insert(conn, contracts, "customers", 1, datetime(2025, 1, 1))
    insert(conn, contracts, "customers", 2, utc_now())
    rows = extract.extract_snapshot(conn, "customers", contracts)
    assert sorted(r["customer_id"] for r in rows) == ["v1", "v2"]


def test_extract_full_categories_reads_every_row(
    conn: pg8000.native.Connection, contracts: dict[str, Contract]
) -> None:
    """extract_full_categories returns every category, unchanged."""
    insert(conn, contracts, "categories", 1, utc_now())
    insert(conn, contracts, "categories", 2, utc_now())
    rows = extract.extract_full_categories(conn, contracts)
    assert sorted(r["category_id"] for r in rows) == ["v1", "v2"]


# --- handler -----------------------------------------------------------------------------


@pytest.fixture
def lambda_env(monkeypatch: pytest.MonkeyPatch) -> FakeS3:
    """The extract's environment, with S3 replaced by an in-memory FakeS3."""
    s3 = FakeS3()
    monkeypatch.setattr(extract.boto3, "client", lambda name: s3)
    monkeypatch.setenv("DATA_BUCKET", "test-bucket")
    monkeypatch.setenv("RAW_PREFIX", "/")
    monkeypatch.setenv("EXTRACT_LAG_MINUTES", "0")
    return s3


def fill_shop(conn: pg8000.native.Connection, contracts: dict[str, Contract]) -> None:
    """Two rows in every shop table, ingested an hour ago."""
    ingested = utc_now() - timedelta(hours=1)
    for table in TABLES:
        for n in (1, 2):
            insert(conn, contracts, table, n, ingested)


def test_handler_writes_every_dataset_then_saves_watermarks(
    conn: pg8000.native.Connection, contracts: dict[str, Contract], lambda_env: FakeS3
) -> None:
    """handler exports all six datasets to their raw folders and advances the watermarks."""
    fill_shop(conn, contracts)
    summary = extract.handler({"snapshot_date": "2026-11-02"}, None)
    assert summary == dict.fromkeys(TABLES, 2)
    folders = sorted(key.rsplit("/", 1)[0] for key in lambda_env.objects)
    assert folders == [
        "categories",
        "customers/snapshot_date=2026-11-02",
        "order_items/order_date=2026-11-02",
        "orders/export_date=2026-11-02",
        "products/snapshot_date=2026-11-02",
        "returns/export_date=2026-11-02",
    ]
    assert "categories/categories.json" in lambda_env.objects
    saved = dict(conn.run("SELECT dataset, high FROM _extract_watermark"))
    assert sorted(saved) == ["order_items", "orders", "returns"]


def test_handler_second_run_exports_no_old_incremental_rows(
    conn: pg8000.native.Connection, contracts: dict[str, Contract], lambda_env: FakeS3
) -> None:
    """After a run, the next run skips incremental rows already exported."""
    fill_shop(conn, contracts)
    extract.handler({}, None)
    summary = extract.handler({}, None)
    assert summary["orders"] == summary["order_items"] == summary["returns"] == 0
    assert summary["customers"] == summary["products"] == summary["categories"] == 2


def test_handler_keeps_watermarks_when_writing_fails(
    conn: pg8000.native.Connection,
    contracts: dict[str, Contract],
    lambda_env: FakeS3,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If writing the files fails, no watermark moves, so the next run exports the rows again."""
    fill_shop(conn, contracts)

    def failing_flush(self: RawWriter, tag: str) -> int:
        raise OSError("S3 unavailable")

    monkeypatch.setattr(RawWriter, "flush", failing_flush)
    with pytest.raises(OSError, match="S3 unavailable"):
        extract.handler({}, None)
    assert conn.run("SELECT count(*) FROM _extract_watermark")[0][0] == 0
