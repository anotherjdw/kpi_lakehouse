"""Input rows for the returns tests, one list per source.

Replace the example rows with 3-5 rows that exercise the transformations, then set
INPUTS_FILLED = True. Column names and types follow each source's contract.

A source that reads another job's output takes that job's expected output rows, imported
from its fixtures below with their REVIEWED flag; the tests fail until they are reviewed.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

from tests.fixtures.order_items.expected import CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED
from tests.fixtures.order_items.expected import CONVERT_NET_AMOUNT_TO_EUROS_ROWS as ORDER_ITEMS_ROWS

INPUTS_FILLED = True

# Three returns on the two orders delivered on 18 October 2026, shaped like shop_simulator's
# engine.py: a row per status version, exported on the day of updated_at, return_ids numbered
# in order of requested_ts, and gaps from config.yaml (request 1-28 days after delivery,
# requested_to_received 48-192h, received_to_closed 24-72h). refund_amount is null until
# refunded, then unit_price x quantity_returned - discount_amount x returned share.
RETURNS_ROWS = [
    # ORD-000004814 line 1 (CHF): rejected, so keep latest status drops the return.
    Row(
        export_date=date(2026, 10, 19),
        return_id="RET-00000391",
        order_id="ORD-000004814",
        line_number=1,
        product_id="P00058",
        requested_ts=datetime(2026, 10, 19, 20, 31, 9, 512000),
        quantity_returned=1,
        refund_amount=None,
        return_reason="wrong_size",
        return_status="requested",
        updated_at=datetime(2026, 10, 19, 20, 31, 9, 512000),
    ),
    Row(
        export_date=date(2026, 10, 22),
        return_id="RET-00000391",
        order_id="ORD-000004814",
        line_number=1,
        product_id="P00058",
        requested_ts=datetime(2026, 10, 19, 20, 31, 9, 512000),
        quantity_returned=1,
        refund_amount=None,
        return_reason="wrong_size",
        return_status="received",
        updated_at=datetime(2026, 10, 22, 14, 8, 33, 64000),
    ),
    Row(
        export_date=date(2026, 10, 24),
        return_id="RET-00000391",
        order_id="ORD-000004814",
        line_number=1,
        product_id="P00058",
        requested_ts=datetime(2026, 10, 19, 20, 31, 9, 512000),
        quantity_returned=1,
        refund_amount=None,
        return_reason="wrong_size",
        return_status="rejected",
        updated_at=datetime(2026, 10, 24, 10, 26, 50, 917000),
    ),
    # ORD-000004812 line 1 (EUR, quantity 2, 3.00 discount): one unit returned,
    # still received, so refund_amount stays null.
    Row(
        export_date=date(2026, 10, 20),
        return_id="RET-00000392",
        order_id="ORD-000004812",
        line_number=1,
        product_id="P00117",
        requested_ts=datetime(2026, 10, 20, 18, 5, 12, 733000),
        quantity_returned=1,
        refund_amount=None,
        return_reason="changed_mind",
        return_status="requested",
        updated_at=datetime(2026, 10, 20, 18, 5, 12, 733000),
    ),
    Row(
        export_date=date(2026, 10, 23),
        return_id="RET-00000392",
        order_id="ORD-000004812",
        line_number=1,
        product_id="P00117",
        requested_ts=datetime(2026, 10, 20, 18, 5, 12, 733000),
        quantity_returned=1,
        refund_amount=None,
        return_reason="changed_mind",
        return_status="received",
        updated_at=datetime(2026, 10, 23, 9, 41, 57, 208000),
    ),
    # ORD-000004814 line 2 (CHF): refunded 26.99 - 2.70 = 24.29 CHF, converted at the
    # order's rate of 0.9445.
    Row(
        export_date=date(2026, 10, 21),
        return_id="RET-00000393",
        order_id="ORD-000004814",
        line_number=2,
        product_id="P00271",
        requested_ts=datetime(2026, 10, 21, 8, 12, 44, 381000),
        quantity_returned=1,
        refund_amount=None,
        return_reason="damaged",
        return_status="requested",
        updated_at=datetime(2026, 10, 21, 8, 12, 44, 381000),
    ),
    Row(
        export_date=date(2026, 10, 23),
        return_id="RET-00000393",
        order_id="ORD-000004814",
        line_number=2,
        product_id="P00271",
        requested_ts=datetime(2026, 10, 21, 8, 12, 44, 381000),
        quantity_returned=1,
        refund_amount=None,
        return_reason="damaged",
        return_status="received",
        updated_at=datetime(2026, 10, 23, 16, 47, 5, 722000),
    ),
    Row(
        export_date=date(2026, 10, 25),
        return_id="RET-00000393",
        order_id="ORD-000004814",
        line_number=2,
        product_id="P00271",
        requested_ts=datetime(2026, 10, 21, 8, 12, 44, 381000),
        quantity_returned=1,
        refund_amount=Decimal("24.29"),
        return_reason="damaged",
        return_status="refunded",
        updated_at=datetime(2026, 10, 25, 11, 3, 29, 146000),
    ),
]
