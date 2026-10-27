"""Input rows for the fx_rates tests, one list per source.

Replace the example rows with 3-5 rows that exercise the transformations, then set
INPUTS_FILLED = True. Column names and types follow each source's contract.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

INPUTS_FILLED = True

# ECB rates for Thursday 15 and Friday 16 October 2026, from shop_simulator's fx.synthetic
# (seed 42, fallbacks CHF 0.94, CZK 25.30, PLN 4.30). Weekdays only, like the real feed.
FX_RATES_ROWS = [
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
]

# Thursday 15 to Sunday 18 October 2026: the weekend has no rates to join, so
# fill_weekends_and_holidays must carry Friday's rates forward.
DATES_ROWS = [
    Row(date=date(2026, 10, 15)),
    Row(date=date(2026, 10, 16)),
    Row(date=date(2026, 10, 17)),
    Row(date=date(2026, 10, 18)),
]
