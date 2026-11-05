"""Load the raw data contracts and check every row the simulator emits against them.

Rows move through the simulator in *wire format*: JSON-native values only (dates and
timestamps as ISO strings, decimals as strings), so they can be kept in the state file, sent
through SQS and written as JSON without conversion. `Contract.check` parses a wire row into
typed Python values and fails on anything the contract does not allow, so the simulator can
never write data that drifts away from the contracts.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import yaml

_DECIMAL_RE = re.compile(r"decimal\((\d+),(\d+)\)")
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
# The repo's contracts: this file is src/kpi_lakehouse/lambdas/common/contracts.py
REPO_CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "contracts"


class ContractError(ValueError):
    """A row does not match its contract."""


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool
    min: int | None = None
    max: int | None = None


@dataclass(frozen=True)
class Contract:
    dataset: str
    file_format: str
    partition_key: str | None
    columns: tuple[Column, ...]

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def file_columns(self) -> list[Column]:
        """Columns stored inside the files; the partition column lives in the folder name."""
        return [c for c in self.columns if c.name != self.partition_key]

    def check(self, row: dict[str, Any]) -> dict[str, Any]:
        """Parse a wire-format row into typed values, raising ContractError on any mismatch.

        Args:
            row: One row in wire format, holding exactly the contract's columns.

        Returns:
            The row with dates, timestamps and decimals parsed.
        """
        missing = [n for n in self.names if n not in row]
        extra = [k for k in row if k not in self.names]
        if missing or extra:
            raise ContractError(f"{self.dataset}: missing columns {missing}, unexpected {extra}")
        typed: dict[str, Any] = {}
        for col in self.columns:
            value = row[col.name]
            if value is None:
                if not col.nullable:
                    raise ContractError(f"{self.dataset}.{col.name} is null but not nullable")
                typed[col.name] = None
                continue
            typed[col.name] = _parse(self.dataset, col, value)
        return typed


def _parse(dataset: str, col: Column, value: Any) -> Any:
    where = f"{dataset}.{col.name}={value!r}"
    kind = col.type
    try:
        if kind == "string":
            if not isinstance(value, str):
                raise ContractError(f"{where}: expected a string")
            return value
        if kind in ("int", "bigint", "smallint", "tinyint"):
            if isinstance(value, bool) or not isinstance(value, int):
                raise ContractError(f"{where}: expected an integer")
            if col.min is not None and value < col.min:
                raise ContractError(f"{where}: below minimum {col.min}")
            if col.max is not None and value > col.max:
                raise ContractError(f"{where}: above maximum {col.max}")
            return value
        if kind == "boolean":
            if not isinstance(value, bool):
                raise ContractError(f"{where}: expected a boolean")
            return value
        if kind in ("double", "float"):
            if isinstance(value, bool) or not isinstance(value, int | float):
                raise ContractError(f"{where}: expected a number")
            return float(value)
        if kind == "date":
            return date.fromisoformat(value)
        if kind == "timestamp":
            return parse_ts(value)
        match = _DECIMAL_RE.fullmatch(kind)
        if match:
            precision, scale = int(match[1]), int(match[2])
            number = Decimal(str(value))
            exponent = number.as_tuple().exponent
            if isinstance(exponent, int) and -exponent > scale:
                raise ContractError(f"{where}: more than {scale} decimal places")
            if len(number.quantize(Decimal(1).scaleb(-scale)).as_tuple().digits) > precision:
                raise ContractError(f"{where}: does not fit {kind}")
            return number
    except (ValueError, TypeError, InvalidOperation) as exc:
        if isinstance(exc, ContractError):
            raise
        raise ContractError(f"{where}: cannot be read as {kind}") from exc
    raise ContractError(f"{where}: type {kind} is not supported by the simulator")


def parse_ts(value: str) -> datetime:
    """Read a wire timestamp (ISO 8601, UTC) into an aware datetime."""
    ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return ts if ts.tzinfo else ts.replace(tzinfo=UTC)


def fmt_ts(ts: datetime) -> str:
    """Write an aware or naive-UTC datetime as a wire timestamp with millisecond precision."""
    if ts.tzinfo:
        ts = ts.astimezone(UTC)
    return ts.strftime(TIMESTAMP_FORMAT)[:-4] + "Z"


def default_contracts_dir() -> Path:
    """Find the contracts: $CONTRACTS_DIR, the copy bundled into a Lambda zip, or the repo's."""
    if os.environ.get("CONTRACTS_DIR"):
        return Path(os.environ["CONTRACTS_DIR"])
    bundled = Path(__file__).parent / "contracts"
    if bundled.is_dir():
        return bundled
    return REPO_CONTRACTS_DIR


def load_contracts(directory: Path | None = None) -> dict[str, Contract]:
    """Load every `*_raw_data_contract.yaml`, keyed by dataset name (e.g. `orders`)."""
    directory = directory or default_contracts_dir()
    contracts = {}
    for path in sorted(directory.glob("*_raw_data_contract.yaml")):
        spec = yaml.safe_load(path.read_text())["contract"]
        columns = tuple(
            Column(
                name=c["name"],
                type=c["type"].replace(" ", ""),
                nullable=bool(c["nullable"]),
                min=(c.get("constraints") or {}).get("min"),
                max=(c.get("constraints") or {}).get("max"),
            )
            for c in spec["schema"]
        )
        contract = Contract(
            dataset=spec["dataset"],
            file_format=spec.get("file_format", "json"),
            partition_key=spec.get("partition_key"),
            columns=columns,
        )
        contracts[contract.dataset] = contract
    if not contracts:
        raise FileNotFoundError(f"No *_raw_data_contract.yaml files in {directory}")
    return contracts
