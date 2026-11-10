"""Where events go: raw-layer json lines files in s3.

Raw files follow the layout engineering-automation reads: `<dataset>/<partition_key>=<YYYY-MM-DD>/`
with JSON Lines files inside, and the partition column only in the folder name.
Unpartitioned datasets are one file, replaced on every export.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from .contracts import Contract


class S3Store:
    def __init__(self, s3: Any, bucket: str, prefix: str = ""):
        self.s3, self.bucket, self.prefix = s3, bucket, prefix.strip("/")

    def put(self, key: str, body: bytes) -> None:
        full = f"{self.prefix}/{key}" if self.prefix else key
        self.s3.put_object(Bucket=self.bucket, Key=full, Body=body)


def json_lines(contract: Contract, rows: Iterable[dict[str, Any]]) -> bytes:
    """Serialise wire rows as JSON Lines; decimals are written as JSON numbers."""
    decimals = {c.name for c in contract.columns if c.type.startswith("decimal")}
    lines = []
    for row in rows:
        out = {k: (float(v) if k in decimals and v is not None else v) for k, v in row.items()}
        lines.append(json.dumps(out, ensure_ascii=False))
    return ("\n".join(lines) + "\n").encode()


class RawWriter:
    """Buffers rows per dataset and partition, and writes one file per partition on flush."""

    def __init__(self, store: Any, contracts: dict[str, Contract]):
        self.store, self.contracts = store, contracts
        self.buffer: dict[tuple[str, str | None], list[dict[str, Any]]] = {}

    def add(self, dataset: str, row: dict[str, Any]) -> None:
        key = self.contracts[dataset].partition_key
        self.buffer.setdefault((dataset, row[key] if key else None), []).append(row)

    def flush(self, tag: str) -> int:
        """Write everything buffered; `tag` makes file names unique per run.
        Returns rows written."""
        written = 0
        for (dataset, value), rows in sorted(
            self.buffer.items(), key=lambda kv: (kv[0][0], kv[0][1] or "")
        ):
            contract = self.contracts[dataset]
            if contract.file_format != "json":
                raise ValueError(
                    f"{dataset}: only JSON contracts can be written, not {contract.file_format}"
                )
            key = contract.partition_key
            if key:
                folder, name = f"{dataset}/{key}={value}", f"part-{tag}"
            else:
                folder, name = dataset, dataset
            body = (json_lines(contract, [{k: v for k, v in r.items() if k != key} for r in rows]),)
            self.store.put(f"{folder}/{name}.json", body)
            written += len(rows)
        self.buffer.clear()
        return written
