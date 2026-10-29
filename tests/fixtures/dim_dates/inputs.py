"""Input rows for the dim_dates tests, one list per source.

Replace the example rows with 3-5 rows that exercise the transformations, then set
INPUTS_FILLED = True. Column names and types follow each source's contract.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

INPUTS_FILLED = True

# One date per month of 2026, covering every quarter, weekends and the first and last days
# of months, plus 31 December 2025, which falls in ISO week 1 of the following year.
DATES_ROWS = [
    # Thursday, first day of the year and of its month
    Row(date=date(2026, 1, 1)),
    # Saturday (weekend); February ends on the 28th
    Row(date=date(2026, 2, 14)),
    # Tuesday, last day of its month
    Row(date=date(2026, 3, 31)),
    # Wednesday
    Row(date=date(2026, 4, 15)),
    # Friday, first day of its month
    Row(date=date(2026, 5, 1)),
    # Sunday (weekend)
    Row(date=date(2026, 6, 21)),
    # Wednesday
    Row(date=date(2026, 7, 15)),
    # Monday, last day of its month
    Row(date=date(2026, 8, 31)),
    # Wednesday, last day of its month
    Row(date=date(2026, 9, 30)),
    # Thursday
    Row(date=date(2026, 10, 15)),
    # Monday, last day of its month
    Row(date=date(2026, 11, 30)),
    # Friday
    Row(date=date(2026, 12, 25)),
    # Wednesday in year 2025 but ISO week 1 of 2026: its week continues into January
    Row(date=date(2025, 12, 31)),
]
