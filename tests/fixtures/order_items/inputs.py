"""Input rows for the order_items tests, one list per source.

Replace the example rows with 3-5 rows that exercise the transformations, then set
INPUTS_FILLED = True. Column names and types follow each source's contract.

A source that reads another job's output takes that job's expected output rows, imported
from its fixtures below with their REVIEWED flag; the tests fail until they are reviewed.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

from tests.fixtures.orders.expected import CONVERT_SHIPPING_FEE_REVIEWED
from tests.fixtures.orders.expected import CONVERT_SHIPPING_FEE_ROWS as ORDERS_ROWS

INPUTS_FILLED = True

# The lines of the four orders in the orders job's output, priced like shop_simulator's
# engine.py: local price from list_price_eur and the market's price_factor, a 15% line promo
# and a 10% voucher on what is left, VAT from the shipping country. Every order's subtotal
# stays under free_shipping_from_eur, matching the shipping fee on its order.
ORDER_ITEMS_ROWS = [
    # ORD-000004812, EUR, DE: three lines. Line 1 has quantity 2 and a promo of 15% on the line
    # total (19.98 -> 3.00 off), so discount_amount is per line, not per unit.
    Row(
        order_date=date(2026, 10, 15),
        order_id="ORD-000004812",
        line_number=1,
        product_id="P00117",
        quantity=2,
        unit_price=Decimal("9.99"),
        discount_amount=Decimal("3.00"),
        tax_rate=Decimal("0.1900"),
    ),
    Row(
        order_date=date(2026, 10, 15),
        order_id="ORD-000004812",
        line_number=2,
        product_id="P00342",
        quantity=1,
        unit_price=Decimal("12.99"),
        discount_amount=Decimal("0.00"),
        tax_rate=Decimal("0.1900"),
    ),
    Row(
        order_date=date(2026, 10, 15),
        order_id="ORD-000004812",
        line_number=3,
        product_id="P00086",
        quantity=1,
        unit_price=Decimal("14.99"),
        discount_amount=Decimal("0.00"),
        tax_rate=Decimal("0.1900"),
    ),
    # ORD-000004813, PLN, PL: list price 34.99 EUR x 4.10, rounded to a whole price ending in 9.
    Row(
        order_date=date(2026, 10, 17),
        order_id="ORD-000004813",
        line_number=1,
        product_id="P00203",
        quantity=1,
        unit_price=Decimal("139.00"),
        discount_amount=Decimal("0.00"),
        tax_rate=Decimal("0.2300"),
    ),
    # ORD-000004814, CHF, CH: two lines, each 10% off from the order's WELCOME10 voucher.
    Row(
        order_date=date(2026, 10, 16),
        order_id="ORD-000004814",
        line_number=1,
        product_id="P00058",
        quantity=1,
        unit_price=Decimal("20.99"),
        discount_amount=Decimal("2.10"),
        tax_rate=Decimal("0.0810"),
    ),
    Row(
        order_date=date(2026, 10, 16),
        order_id="ORD-000004814",
        line_number=2,
        product_id="P00271",
        quantity=1,
        unit_price=Decimal("26.99"),
        discount_amount=Decimal("2.70"),
        tax_rate=Decimal("0.0810"),
    ),
    # ORD-000004815, CZK, CZ: CZ is not a simulator market; this assumes a price_factor of 25.00
    # (29.99 EUR -> 749.00 CZK) and Czech VAT of 21%.
    Row(
        order_date=date(2026, 10, 15),
        order_id="ORD-000004815",
        line_number=1,
        product_id="P00164",
        quantity=1,
        unit_price=Decimal("749.00"),
        discount_amount=Decimal("0.00"),
        tax_rate=Decimal("0.2100"),
    ),
]
