"""Input rows for the kpi_daily_sales tests, one list per source.

Every source reads another job's output, so its rows are that job's expected output rows,
imported from its fixtures below with their REVIEWED flag. There is nothing to fill in
here; the tests fail until those rows are reviewed.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

from tests.fixtures.fact_order_line.expected import CREATE_DATE_KEY_REVIEWED
from tests.fixtures.fact_order_line.expected import CREATE_DATE_KEY_ROWS as FACT_ORDER_LINE_ROWS
