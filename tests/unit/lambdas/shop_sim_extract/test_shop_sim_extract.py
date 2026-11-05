"""Unit tests of the shop_sim_extract Lambda's pure and write functions.

These run without a database or AWS: each test calls one function from the handler module with
plain Python values. The functions that read or write the shop database are tested against a
real local Postgres in tests/integration/lambdas/test_shop_sim_extract_integration.py.
"""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from kpi_lakehouse.lambdas.common.contracts import (
    Column,
    Contract,
    ContractError,
    load_contracts,
)
from kpi_lakehouse.lambdas.common.sinks import RawWriter
from kpi_lakehouse.lambdas.shop_sim_extract import handler as extract
from kpi_lakehouse.lambdas.shop_sim_extract.tables import table_columns

FIXED_NOW = datetime(2026, 11, 3, 0, 30, tzinfo=UTC)


class FakeStore:
    """Records what RawWriter writes, instead of writing to S3."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put(self, key: str, body: bytes) -> None:
        self.objects[key] = body


class FixedDatetime(datetime):
    """datetime whose now() is FIXED_NOW, so the settings can be checked exactly."""

    @classmethod
    def now(cls, tz: Any = None) -> "FixedDatetime":
        return cls.fromtimestamp(FIXED_NOW.timestamp(), tz)


def wire_value(column: Column) -> Any:
    """One valid wire-format value for a contract column."""
    if column.type == "string":
        return f"{column.name}-1"
    if column.type in ("int", "bigint", "smallint", "tinyint"):
        return column.min if column.min is not None else 1
    if column.type in ("double", "float"):
        return 1.5
    if column.type == "boolean":
        return True
    if column.type == "date":
        return "2026-11-02"
    if column.type == "timestamp":
        return "2026-11-02T10:00:00.000Z"
    return "1.00"  # decimal(p,s)


def backend_row(contract: Contract) -> dict[str, Any]:
    """A valid row as select_rows returns it: the backend columns only, in wire format."""
    return {c.name: wire_value(c) for c in table_columns(contract)}


@pytest.fixture(scope="module")
def contracts() -> dict[str, Contract]:
    """The raw data contracts from src/kpi_lakehouse/contracts/."""
    return load_contracts()


# --- to_wire -----------------------------------------------------------------------------


def test_to_wire_formats_timestamps() -> None:
    """to_wire writes a datetime as a UTC wire timestamp with milliseconds."""
    assert extract.to_wire(datetime(2026, 11, 2, 10, 5, 6, 789000)) == "2026-11-02T10:05:06.789Z"


def test_to_wire_formats_dates() -> None:
    """to_wire writes a date as YYYY-MM-DD."""
    assert extract.to_wire(date(2026, 11, 2)) == "2026-11-02"


def test_to_wire_formats_decimals_as_strings() -> None:
    """to_wire keeps a decimal's exact digits by writing it as a string."""
    assert extract.to_wire(Decimal("12.50")) == "12.50"


@pytest.mark.parametrize("value", ["text", 7, 1.5, True, None])
def test_to_wire_leaves_other_values_unchanged(value: Any) -> None:
    """to_wire returns strings, numbers, booleans and None as they are."""
    assert extract.to_wire(value) == value


# --- with_export_date / with_snapshot_date -----------------------------------------------


@pytest.mark.parametrize("dataset", ["orders", "returns"])
def test_with_export_date_adds_date_of_updated_at(dataset: str) -> None:
    """with_export_date adds export_date, the date part of updated_at, to orders and returns."""
    row = {"updated_at": "2026-11-02T23:59:59.000Z", "status": "shipped"}
    result = extract.with_export_date(dataset, row)
    assert result == {"export_date": "2026-11-02", **row}
    assert row == {"updated_at": "2026-11-02T23:59:59.000Z", "status": "shipped"}


def test_with_export_date_leaves_order_items_unchanged() -> None:
    """with_export_date returns rows of datasets without an export_date as they are."""
    row = {"order_date": "2026-11-02", "line_number": 1}
    assert extract.with_export_date("order_items", row) == row


def test_with_snapshot_date_adds_snapshot_date() -> None:
    """with_snapshot_date adds the snapshot date and keeps every other column."""
    row = {"customer_id": "c-1"}
    assert extract.with_snapshot_date("2026-11-02", row) == {
        "snapshot_date": "2026-11-02",
        "customer_id": "c-1",
    }


# --- extract_settings_from_env -----------------------------------------------------------


def test_settings_default_lag_and_snapshot_date(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without overrides, the cut-off is 2 minutes before the run and the snapshot is yesterday."""
    monkeypatch.setattr(extract, "datetime", FixedDatetime)
    monkeypatch.delenv("EXTRACT_LAG_MINUTES", raising=False)
    run_at, upper, snapshot_date = extract.extract_settings_from_env(None)
    assert run_at == datetime(2026, 11, 3, 0, 30)
    assert run_at.tzinfo is None
    assert upper == run_at - timedelta(minutes=2)
    assert snapshot_date == "2026-11-02"


def test_settings_lag_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """EXTRACT_LAG_MINUTES sets how far before the run the cut-off is."""
    monkeypatch.setattr(extract, "datetime", FixedDatetime)
    monkeypatch.setenv("EXTRACT_LAG_MINUTES", "15")
    run_at, upper, _ = extract.extract_settings_from_env({})
    assert upper == run_at - timedelta(minutes=15)


def test_settings_snapshot_date_from_event(monkeypatch: pytest.MonkeyPatch) -> None:
    """A snapshot_date in the event overrides yesterday."""
    monkeypatch.setattr(extract, "datetime", FixedDatetime)
    _, _, snapshot_date = extract.extract_settings_from_env({"snapshot_date": "2026-10-01"})
    assert snapshot_date == "2026-10-01"


# --- write_rows --------------------------------------------------------------------------


def test_write_rows_buffers_valid_row(contracts: dict[str, Contract]) -> None:
    """write_rows adds a valid row, which flush then writes to its partition folder."""
    store = FakeStore()
    raw = RawWriter(store, contracts)
    row = extract.with_export_date("orders", backend_row(contracts["orders"]))
    extract.write_rows(contracts, "orders", row, raw)
    assert raw.flush(tag="test") == 1
    assert list(store.objects) == ["orders/export_date=2026-11-02/part-test.json"]


def test_write_rows_rejects_row_breaking_contract(contracts: dict[str, Contract]) -> None:
    """write_rows raises ContractError for a row missing a column, and buffers nothing."""
    store = FakeStore()
    raw = RawWriter(store, contracts)
    row = extract.with_export_date("orders", backend_row(contracts["orders"]))
    del row["updated_at"]
    with pytest.raises(ContractError):
        extract.write_rows(contracts, "orders", row, raw)
    assert raw.flush(tag="test") == 0
    assert store.objects == {}
