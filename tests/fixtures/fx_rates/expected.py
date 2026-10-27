"""Expected output rows of the fx_rates transformations.

One list per transformation. `engineering-automation generate expected-outputs` drafts
each empty list from the transformation's description and input rows, without seeing its
code. Check every row, correct it, then set that transformation's REVIEWED flag to True.
Its test fails until you do.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

JOIN_FX_RATES_TO_DATE_REVIEWED = True
JOIN_FX_RATES_TO_DATE_ROWS: list[Row] = [
    Row(
        rate_date=date(2026, 10, 15),
        base_currency="EUR",
        quote_currency="CHF",
        rate=Decimal("0.93970000"),
        date=date(2026, 10, 15),
    ),
    Row(
        rate_date=date(2026, 10, 15),
        base_currency="EUR",
        quote_currency="CZK",
        rate=Decimal("25.24190000"),
        date=date(2026, 10, 15),
    ),
    Row(
        rate_date=date(2026, 10, 15),
        base_currency="EUR",
        quote_currency="PLN",
        rate=Decimal("4.30100000"),
        date=date(2026, 10, 15),
    ),
    Row(
        rate_date=date(2026, 10, 16),
        base_currency="EUR",
        quote_currency="CHF",
        rate=Decimal("0.94450000"),
        date=date(2026, 10, 16),
    ),
    Row(
        rate_date=date(2026, 10, 16),
        base_currency="EUR",
        quote_currency="CZK",
        rate=Decimal("25.15670000"),
        date=date(2026, 10, 16),
    ),
    Row(
        rate_date=date(2026, 10, 16),
        base_currency="EUR",
        quote_currency="PLN",
        rate=Decimal("4.29800000"),
        date=date(2026, 10, 16),
    ),
    Row(
        rate_date=None, base_currency=None, quote_currency=None, rate=None, date=date(2026, 10, 17)
    ),
    Row(
        rate_date=None, base_currency=None, quote_currency=None, rate=None, date=date(2026, 10, 18)
    ),
]

FILL_WEEKENDS_AND_HOLIDAYS_REVIEWED = True
FILL_WEEKENDS_AND_HOLIDAYS_ROWS: list[Row] = [
    Row(
        rate_date=date(2026, 10, 15),
        base_currency="EUR",
        quote_currency="CHF",
        rate=Decimal("0.93970000"),
        date=date(2026, 10, 15),
    ),
    Row(
        rate_date=date(2026, 10, 15),
        base_currency="EUR",
        quote_currency="CZK",
        rate=Decimal("25.24190000"),
        date=date(2026, 10, 15),
    ),
    Row(
        rate_date=date(2026, 10, 15),
        base_currency="EUR",
        quote_currency="PLN",
        rate=Decimal("4.30100000"),
        date=date(2026, 10, 15),
    ),
    Row(
        rate_date=date(2026, 10, 16),
        base_currency="EUR",
        quote_currency="CHF",
        rate=Decimal("0.94450000"),
        date=date(2026, 10, 16),
    ),
    Row(
        rate_date=date(2026, 10, 16),
        base_currency="EUR",
        quote_currency="CZK",
        rate=Decimal("25.15670000"),
        date=date(2026, 10, 16),
    ),
    Row(
        rate_date=date(2026, 10, 16),
        base_currency="EUR",
        quote_currency="PLN",
        rate=Decimal("4.29800000"),
        date=date(2026, 10, 16),
    ),
    Row(
        rate_date=None,
        base_currency="EUR",
        quote_currency="CHF",
        rate=Decimal("0.94450000"),
        date=date(2026, 10, 17),
    ),
    Row(
        rate_date=None,
        base_currency="EUR",
        quote_currency="CZK",
        rate=Decimal("25.15670000"),
        date=date(2026, 10, 17),
    ),
    Row(
        rate_date=None,
        base_currency="EUR",
        quote_currency="PLN",
        rate=Decimal("4.29800000"),
        date=date(2026, 10, 17),
    ),
    Row(
        rate_date=None,
        base_currency="EUR",
        quote_currency="CHF",
        rate=Decimal("0.94450000"),
        date=date(2026, 10, 18),
    ),
    Row(
        rate_date=None,
        base_currency="EUR",
        quote_currency="CZK",
        rate=Decimal("25.15670000"),
        date=date(2026, 10, 18),
    ),
    Row(
        rate_date=None,
        base_currency="EUR",
        quote_currency="PLN",
        rate=Decimal("4.29800000"),
        date=date(2026, 10, 18),
    ),
    Row(
        rate_date=None,
        base_currency="EUR",
        quote_currency="EUR",
        rate=Decimal("1.00000000"),
        date=date(2026, 10, 15),
    ),
    Row(
        rate_date=None,
        base_currency="EUR",
        quote_currency="EUR",
        rate=Decimal("1.00000000"),
        date=date(2026, 10, 16),
    ),
    Row(
        rate_date=None,
        base_currency="EUR",
        quote_currency="EUR",
        rate=Decimal("1.00000000"),
        date=date(2026, 10, 17),
    ),
    Row(
        rate_date=None,
        base_currency="EUR",
        quote_currency="EUR",
        rate=Decimal("1.00000000"),
        date=date(2026, 10, 18),
    ),
]

COALESCE_RATE_DATE_REVIEWED = True
COALESCE_RATE_DATE_ROWS: list[Row] = [
    Row(
        rate_date=date(2026, 10, 15),
        base_currency="EUR",
        quote_currency="CHF",
        rate=Decimal("0.93970000"),
    ),
    Row(
        rate_date=date(2026, 10, 15),
        base_currency="EUR",
        quote_currency="CZK",
        rate=Decimal("25.24190000"),
    ),
    Row(
        rate_date=date(2026, 10, 15),
        base_currency="EUR",
        quote_currency="PLN",
        rate=Decimal("4.30100000"),
    ),
    Row(
        rate_date=date(2026, 10, 16),
        base_currency="EUR",
        quote_currency="CHF",
        rate=Decimal("0.94450000"),
    ),
    Row(
        rate_date=date(2026, 10, 16),
        base_currency="EUR",
        quote_currency="CZK",
        rate=Decimal("25.15670000"),
    ),
    Row(
        rate_date=date(2026, 10, 16),
        base_currency="EUR",
        quote_currency="PLN",
        rate=Decimal("4.29800000"),
    ),
    Row(
        rate_date=date(2026, 10, 17),
        base_currency="EUR",
        quote_currency="CHF",
        rate=Decimal("0.94450000"),
    ),
    Row(
        rate_date=date(2026, 10, 17),
        base_currency="EUR",
        quote_currency="CZK",
        rate=Decimal("25.15670000"),
    ),
    Row(
        rate_date=date(2026, 10, 17),
        base_currency="EUR",
        quote_currency="PLN",
        rate=Decimal("4.29800000"),
    ),
    Row(
        rate_date=date(2026, 10, 18),
        base_currency="EUR",
        quote_currency="CHF",
        rate=Decimal("0.94450000"),
    ),
    Row(
        rate_date=date(2026, 10, 18),
        base_currency="EUR",
        quote_currency="CZK",
        rate=Decimal("25.15670000"),
    ),
    Row(
        rate_date=date(2026, 10, 18),
        base_currency="EUR",
        quote_currency="PLN",
        rate=Decimal("4.29800000"),
    ),
    Row(
        rate_date=date(2026, 10, 15),
        base_currency="EUR",
        quote_currency="EUR",
        rate=Decimal("1.00000000"),
    ),
    Row(
        rate_date=date(2026, 10, 16),
        base_currency="EUR",
        quote_currency="EUR",
        rate=Decimal("1.00000000"),
    ),
    Row(
        rate_date=date(2026, 10, 17),
        base_currency="EUR",
        quote_currency="EUR",
        rate=Decimal("1.00000000"),
    ),
    Row(
        rate_date=date(2026, 10, 18),
        base_currency="EUR",
        quote_currency="EUR",
        rate=Decimal("1.00000000"),
    ),
]
