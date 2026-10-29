"""Expected output rows of the sessions transformations.

One list per transformation. `engineering-automation generate expected-outputs` drafts
each empty list from the transformation's description and input rows, without seeing its
code. Check every row, correct it, then set that transformation's REVIEWED flag to True.
Its test fails until you do.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

AGGREGATE_SESSION_EVENTS_REVIEWED = True
AGGREGATE_SESSION_EVENTS_ROWS: list[Row] = [
    Row(
        session_id="3f9c2a7e51d84b06a1e7c4d29b8f6053",
        event_date=date(2026, 10, 15),
        first_event_time=datetime(2026, 10, 15, 10, 9, 41, 884000),
        last_event_time=datetime(2026, 10, 15, 10, 12, 30, 120000),
        first_mkt_channel="paid_search",
        customer_id="C0000417",
        last_event_type="purchase",
        order_id="ORD-000004812",
    ),
    Row(
        session_id="a84e1d6c0b9f42e7953c2f18d7a6b04e",
        event_date=date(2026, 10, 16),
        first_event_time=datetime(2026, 10, 16, 19, 17, 2, 418000),
        last_event_time=datetime(2026, 10, 16, 19, 20, 11, 305000),
        first_mkt_channel="affiliate",
        customer_id="C0000953",
        last_event_type="purchase",
        order_id="ORD-000004814",
    ),
    Row(
        session_id="c51b8e2f7a3d49c0b6e1f4a92d7c8e35",
        event_date=date(2026, 10, 15),
        first_event_time=datetime(2026, 10, 15, 21, 2, 20, 139000),
        last_event_time=datetime(2026, 10, 15, 21, 5, 42, 610000),
        first_mkt_channel="organic_search",
        customer_id="C0001502",
        last_event_type="purchase",
        order_id="ORD-000004815",
    ),
    Row(
        session_id="9719141f1b1f3f46adfa67bad1d4d80f",
        event_date=date(2026, 10, 16),
        first_event_time=datetime(2026, 10, 16, 8, 30, 12, 345000),
        last_event_time=datetime(2026, 10, 16, 8, 31, 5, 678000),
        first_mkt_channel="email",
        customer_id="C0001288",
        last_event_type="product_view",
        order_id=None,
    ),
    Row(
        session_id="5fbc010fecf1bbba68cf4d2fac579005",
        event_date=date(2026, 10, 17),
        first_event_time=datetime(2026, 10, 17, 12, 0, 3, 210000),
        last_event_time=datetime(2026, 10, 17, 12, 1, 48, 903000),
        first_mkt_channel="direct",
        customer_id="C0000417",
        last_event_type="product_view",
        order_id=None,
    ),
    Row(
        session_id="2fb1576860038b627f325abff433c321",
        event_date=date(2026, 10, 16),
        first_event_time=datetime(2026, 10, 16, 14, 22, 9, 4000),
        last_event_time=datetime(2026, 10, 16, 14, 22, 9, 4000),
        first_mkt_channel="referral",
        customer_id=None,
        last_event_type="page_view",
        order_id=None,
    ),
]
