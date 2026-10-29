"""Input rows for the dim_product tests, one list per source.

Every source reads another job's output, so its rows are that job's expected output rows,
imported from its fixtures below with their REVIEWED flag. There is nothing to fill in
here; the tests fail until those rows are reviewed.
"""

from datetime import date, datetime
from decimal import Decimal
from hashlib import md5, sha256

from pyspark.sql import Row

from tests.fixtures.categories.expected import FLATTEN_CATEGORY_TREE_REVIEWED
from tests.fixtures.categories.expected import FLATTEN_CATEGORY_TREE_ROWS as CATEGORIES_ROWS
from tests.fixtures.products.expected import CREATE_PRODUCT_VERSION_ID_REVIEWED
from tests.fixtures.products.expected import CREATE_PRODUCT_VERSION_ID_ROWS as PRODUCTS_ROWS
