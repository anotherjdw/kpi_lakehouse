"""Input rows for the fact_session tests, one list per source.

Every source reads another job's output, so its rows are that job's expected output rows,
imported from its fixtures below with their REVIEWED flag. There is nothing to fill in
here; the tests fail until those rows are reviewed.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

from tests.fixtures.dim_customer.expected import SELECT_DIMENSIONS_REVIEWED
from tests.fixtures.dim_customer.expected import SELECT_DIMENSIONS_ROWS as DIM_CUSTOMER_ROWS
from tests.fixtures.sessions.expected import AGGREGATE_SESSION_EVENTS_REVIEWED
from tests.fixtures.sessions.expected import AGGREGATE_SESSION_EVENTS_ROWS as SESSIONS_ROWS
