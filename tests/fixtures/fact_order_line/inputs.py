"""Input rows for the fact_order_line tests, one list per source.

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
from tests.fixtures.dim_product.expected import JOIN_CATEGORY_PATH_REVIEWED
from tests.fixtures.dim_product.expected import JOIN_CATEGORY_PATH_ROWS as DIM_PRODUCT_ROWS
from tests.fixtures.order_items.expected import CONVERT_NET_AMOUNT_TO_EUROS_REVIEWED
from tests.fixtures.order_items.expected import CONVERT_NET_AMOUNT_TO_EUROS_ROWS as ORDER_ITEMS_ROWS
from tests.fixtures.orders.expected import CONVERT_SHIPPING_FEE_REVIEWED
from tests.fixtures.orders.expected import CONVERT_SHIPPING_FEE_ROWS as ORDERS_ROWS
from tests.fixtures.returns.expected import COMPUTE_NET_REFUND_REVIEWED
from tests.fixtures.returns.expected import COMPUTE_NET_REFUND_ROWS as RETURNS_ROWS
from tests.fixtures.sessions.expected import AGGREGATE_SESSION_EVENTS_REVIEWED
from tests.fixtures.sessions.expected import AGGREGATE_SESSION_EVENTS_ROWS as SESSIONS_ROWS
