"""The shop's backend tables, as the extract reads them"""

from __future__ import annotations

from typing import Any

from kpi_lakehouse.lambdas.common.contracts import Column, Contract

# Tables of the shop backend: primary key, and the column that must not move backwards.
TABLES: dict[str, dict[str, Any]] = {
    "categories": {"key": ["category_id"], "guard": "updated_at"},
    "products": {"key": ["product_id"], "guard": None},
    "customers": {"key": ["customer_id"], "guard": None},
    "orders": {"key": ["order_id"], "guard": "updated_at"},
    "order_items": {"key": ["order_id", "line_number"], "guard": None},
    "returns": {"key": ["return_id"], "guard": "updated_at"},
}
# Partition columns that exist only in the raw layer, not in the backend.
EXPORT_ONLY = {
    "orders": "export_date",
    "returns": "export_date",
    "customers": "snapshot_date",
    "products": "snapshot_date",
}


def table_columns(contract: Contract) -> list[Column]:
    skip = EXPORT_ONLY.get(contract.dataset)
    return [c for c in contract.columns if c.name != skip]
