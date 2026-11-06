"""Input rows for the orders tests, one list per source.

Replace the example rows with 3-5 rows that exercise the transformations, then set
INPUTS_FILLED = True. Column names and types follow each source's contract.

A source that reads another job's output takes that job's expected output rows, imported
from its fixtures below with their REVIEWED flag; the tests fail until they are reviewed.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

from tests.fixtures.fx_rates.expected import COALESCE_RATE_DATE_REVIEWED
from tests.fixtures.fx_rates.expected import COALESCE_RATE_DATE_ROWS as FX_RATES_ROWS

INPUTS_FILLED = True

# Five orders between 15 and 18 October 2026, one per currency plus a test order, shaped like
# shop_simulator's engine.py: a row per status version, exported on the day of updated_at, with
# lifecycle gaps from config.yaml (placed_to_paid 0.05-0.5h, paid_to_shipped 18-48h,
# shipped_to_delivered 24-96h, paid_to_cancelled 1-20h).
ORDERS_ROWS = [
    # EUR, placed, paid, shipped, then delivered on the 18th: keep latest status keeps only the
    # delivered version. Line 1 is returned (see the returns fixture).
    Row(
        export_date=date(2026, 10, 15),
        order_id="ORD-000004812",
        customer_id="C0000417",
        session_id="3f9c2a7e51d84b06a1e7c4d29b8f6053",
        order_ts=datetime(2026, 10, 15, 10, 12, 30, 120000),
        order_status="placed",
        updated_at=datetime(2026, 10, 15, 10, 12, 30, 120000),
        currency_code="EUR",
        shipping_fee=Decimal("4.95"),
        voucher_code=None,
        payment_method="card",
        shipping_country="DE",
        is_test_order=False,
    ),
    Row(
        export_date=date(2026, 10, 15),
        order_id="ORD-000004812",
        customer_id="C0000417",
        session_id="3f9c2a7e51d84b06a1e7c4d29b8f6053",
        order_ts=datetime(2026, 10, 15, 10, 12, 30, 120000),
        order_status="paid",
        updated_at=datetime(2026, 10, 15, 10, 31, 5, 480000),
        currency_code="EUR",
        shipping_fee=Decimal("4.95"),
        voucher_code=None,
        payment_method="card",
        shipping_country="DE",
        is_test_order=False,
    ),
    Row(
        export_date=date(2026, 10, 16),
        order_id="ORD-000004812",
        customer_id="C0000417",
        session_id="3f9c2a7e51d84b06a1e7c4d29b8f6053",
        order_ts=datetime(2026, 10, 15, 10, 12, 30, 120000),
        order_status="shipped",
        updated_at=datetime(2026, 10, 16, 14, 2, 44, 910000),
        currency_code="EUR",
        shipping_fee=Decimal("4.95"),
        voucher_code=None,
        payment_method="card",
        shipping_country="DE",
        is_test_order=False,
    ),
    Row(
        export_date=date(2026, 10, 18),
        order_id="ORD-000004812",
        customer_id="C0000417",
        session_id="3f9c2a7e51d84b06a1e7c4d29b8f6053",
        order_ts=datetime(2026, 10, 15, 10, 12, 30, 120000),
        order_status="delivered",
        updated_at=datetime(2026, 10, 18, 11, 37, 21, 455000),
        currency_code="EUR",
        shipping_fee=Decimal("4.95"),
        voucher_code=None,
        payment_method="card",
        shipping_country="DE",
        is_test_order=False,
    ),
    # PLN, placed on Saturday by customer service (no session): joins the rate carried from Friday.
    Row(
        export_date=date(2026, 10, 17),
        order_id="ORD-000004813",
        customer_id="C0001288",
        session_id=None,
        order_ts=datetime(2026, 10, 17, 9, 45, 0),
        order_status="placed",
        updated_at=datetime(2026, 10, 17, 9, 45, 0),
        currency_code="PLN",
        shipping_fee=Decimal("20.30"),
        voucher_code=None,
        payment_method="paypal",
        shipping_country="PL",
        is_test_order=False,
    ),
    # CHF, with a voucher: placed and paid on the 16th, shipped on the 17th, delivered on the
    # 18th. Both lines are returned (see the returns fixture).
    Row(
        export_date=date(2026, 10, 16),
        order_id="ORD-000004814",
        customer_id="C0000953",
        session_id="a84e1d6c0b9f42e7953c2f18d7a6b04e",
        order_ts=datetime(2026, 10, 16, 19, 20, 11, 305000),
        order_status="placed",
        updated_at=datetime(2026, 10, 16, 19, 20, 11, 305000),
        currency_code="CHF",
        shipping_fee=Decimal("5.20"),
        voucher_code="WELCOME10",
        payment_method="klarna_invoice",
        shipping_country="CH",
        is_test_order=False,
    ),
    Row(
        export_date=date(2026, 10, 16),
        order_id="ORD-000004814",
        customer_id="C0000953",
        session_id="a84e1d6c0b9f42e7953c2f18d7a6b04e",
        order_ts=datetime(2026, 10, 16, 19, 20, 11, 305000),
        order_status="paid",
        updated_at=datetime(2026, 10, 16, 19, 28, 40, 770000),
        currency_code="CHF",
        shipping_fee=Decimal("5.20"),
        voucher_code="WELCOME10",
        payment_method="klarna_invoice",
        shipping_country="CH",
        is_test_order=False,
    ),
    Row(
        export_date=date(2026, 10, 17),
        order_id="ORD-000004814",
        customer_id="C0000953",
        session_id="a84e1d6c0b9f42e7953c2f18d7a6b04e",
        order_ts=datetime(2026, 10, 16, 19, 20, 11, 305000),
        order_status="shipped",
        updated_at=datetime(2026, 10, 17, 15, 52, 6, 130000),
        currency_code="CHF",
        shipping_fee=Decimal("5.20"),
        voucher_code="WELCOME10",
        payment_method="klarna_invoice",
        shipping_country="CH",
        is_test_order=False,
    ),
    Row(
        export_date=date(2026, 10, 18),
        order_id="ORD-000004814",
        customer_id="C0000953",
        session_id="a84e1d6c0b9f42e7953c2f18d7a6b04e",
        order_ts=datetime(2026, 10, 16, 19, 20, 11, 305000),
        order_status="delivered",
        updated_at=datetime(2026, 10, 18, 17, 14, 48, 390000),
        currency_code="CHF",
        shipping_fee=Decimal("5.20"),
        voucher_code="WELCOME10",
        payment_method="klarna_invoice",
        shipping_country="CH",
        is_test_order=False,
    ),
    # CZK, placed then paid then cancelled before shipping. CZ is not a simulator market;
    # its fee assumes a price_factor of 25.00 (4.95 EUR -> 123.75 CZK).
    Row(
        export_date=date(2026, 10, 15),
        order_id="ORD-000004815",
        customer_id="C0001502",
        session_id="c51b8e2f7a3d49c0b6e1f4a92d7c8e35",
        order_ts=datetime(2026, 10, 15, 21, 5, 42, 610000),
        order_status="placed",
        updated_at=datetime(2026, 10, 15, 21, 5, 42, 610000),
        currency_code="CZK",
        shipping_fee=Decimal("123.75"),
        voucher_code=None,
        payment_method="card",
        shipping_country="CZ",
        is_test_order=False,
    ),
    Row(
        export_date=date(2026, 10, 15),
        order_id="ORD-000004815",
        customer_id="C0001502",
        session_id="c51b8e2f7a3d49c0b6e1f4a92d7c8e35",
        order_ts=datetime(2026, 10, 15, 21, 5, 42, 610000),
        order_status="paid",
        updated_at=datetime(2026, 10, 15, 21, 20, 18, 45000),
        currency_code="CZK",
        shipping_fee=Decimal("123.75"),
        voucher_code=None,
        payment_method="card",
        shipping_country="CZ",
        is_test_order=False,
    ),
    Row(
        export_date=date(2026, 10, 16),
        order_id="ORD-000004815",
        customer_id="C0001502",
        session_id="c51b8e2f7a3d49c0b6e1f4a92d7c8e35",
        order_ts=datetime(2026, 10, 15, 21, 5, 42, 610000),
        order_status="cancelled",
        updated_at=datetime(2026, 10, 16, 8, 41, 3, 220000),
        currency_code="CZK",
        shipping_fee=Decimal("123.75"),
        voucher_code=None,
        payment_method="card",
        shipping_country="CZ",
        is_test_order=False,
    ),
    # EUR internal QA order, placed on Sunday: keep latest status drops it.
    Row(
        export_date=date(2026, 10, 18),
        order_id="ORD-000004816",
        customer_id="C0000088",
        session_id="e27d9b4a6c1f40389a5e0b7d3c6f2a91",
        order_ts=datetime(2026, 10, 18, 11, 3, 57, 840000),
        order_status="placed",
        updated_at=datetime(2026, 10, 18, 11, 3, 57, 840000),
        currency_code="EUR",
        shipping_fee=Decimal("4.95"),
        voucher_code=None,
        payment_method="card",
        shipping_country="NL",
        is_test_order=True,
    ),
]
