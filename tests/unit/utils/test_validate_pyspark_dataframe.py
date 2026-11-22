"""Validation of a DataFrame against its contract's schema and the rules in its metadata.

validate_pyspark_dataframe runs in each job's write(), before the file write, and in read()
on a source with its own contract. It checks exact column types (decimal precision/scale
included, nullable ignored), then, in one pass, nulls in non-nullable columns and the
allowed values, bounds, lengths and patterns the fields' metadata sets, then uniqueness of
the key, and reports every broken rule at once.
"""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kpi_lakehouse.utils.dataframes import validate_pyspark_dataframe

# The key is (date_utc, shop_id), marked in the metadata as the StructType module writes it.
EXPECTED_SCHEMA = StructType(
    [
        StructField("date_utc", DateType(), nullable=False, metadata={"primaryKey": True}),
        StructField("shop_id", IntegerType(), nullable=False, metadata={"primaryKey": True}),
        StructField("earnings_eur", DecimalType(10, 2), nullable=True),
    ]
)

# Same names and types, all nullable: lets a test build rows the expected schema forbids.
NULLABLE_SCHEMA = StructType(
    [StructField(field.name, field.dataType, nullable=True) for field in EXPECTED_SCHEMA]
)

DAY = date(2026, 1, 1)


def test_valid_dataframe_passes(spark: SparkSession) -> None:
    """Correct types, no nulls in non-nullable columns and a unique key raise nothing."""
    df = spark.createDataFrame(
        [
            Row(date_utc=DAY, shop_id=1, earnings_eur=Decimal("10.00")),
            Row(date_utc=DAY, shop_id=2, earnings_eur=None),
        ],
        schema=NULLABLE_SCHEMA,
    )

    validate_pyspark_dataframe(df, EXPECTED_SCHEMA)


def test_empty_dataframe_passes(spark: SparkSession) -> None:
    """An empty DataFrame has no nulls and no duplicates, so it passes."""
    df = spark.createDataFrame([], schema=NULLABLE_SCHEMA)

    validate_pyspark_dataframe(df, EXPECTED_SCHEMA)


def test_decimal_precision_mismatch_fails(spark: SparkSession) -> None:
    """decimal(20,2) is not decimal(10,2): precision and scale are part of the type."""
    schema = StructType(
        [
            StructField("date_utc", DateType()),
            StructField("shop_id", IntegerType()),
            StructField("earnings_eur", DecimalType(20, 2)),
        ]
    )
    df = spark.createDataFrame([Row(date_utc=DAY, shop_id=1, earnings_eur=Decimal("1.00"))], schema)

    with pytest.raises(ValueError, match=r"Expected decimal\(10,2\), Received decimal\(20,2\)"):
        validate_pyspark_dataframe(df, EXPECTED_SCHEMA)


def test_missing_column_fails(spark: SparkSession) -> None:
    """A column in the expected schema but not in the DataFrame fails schema validation."""
    df = spark.createDataFrame([Row(date_utc=DAY, shop_id=1)], "date_utc date, shop_id int")

    with pytest.raises(
        ValueError, match="(?s)Schema validation failed.*Missing columns.*earnings_eur"
    ):
        validate_pyspark_dataframe(df, EXPECTED_SCHEMA)


def test_unexpected_column_fails(spark: SparkSession) -> None:
    """A column in the DataFrame but not in the expected schema fails schema validation."""
    schema = StructType([*NULLABLE_SCHEMA.fields, StructField("extra", StringType())])
    df = spark.createDataFrame(
        [Row(date_utc=DAY, shop_id=1, earnings_eur=Decimal("1.00"), extra="x")], schema
    )

    with pytest.raises(ValueError, match="(?s)Schema validation failed.*Unexpected columns.*extra"):
        validate_pyspark_dataframe(df, EXPECTED_SCHEMA)


def test_null_in_non_nullable_column_fails(spark: SparkSession) -> None:
    """A null in a column the expected schema marks non-nullable fails data validation."""
    df = spark.createDataFrame(
        [Row(date_utc=DAY, shop_id=None, earnings_eur=Decimal("1.00"))], schema=NULLABLE_SCHEMA
    )

    with pytest.raises(ValueError, match="Data validation failed: nulls.*shop_id"):
        validate_pyspark_dataframe(df, EXPECTED_SCHEMA)


def test_duplicate_key_fails(spark: SparkSession) -> None:
    """Two rows with the same composite key fail data validation."""
    df = spark.createDataFrame(
        [
            Row(date_utc=DAY, shop_id=1, earnings_eur=Decimal("1.00")),
            Row(date_utc=DAY, shop_id=1, earnings_eur=Decimal("2.00")),
        ],
        schema=NULLABLE_SCHEMA,
    )

    with pytest.raises(ValueError, match="Data validation failed: 1 duplicate row"):
        validate_pyspark_dataframe(df, EXPECTED_SCHEMA)


def test_composite_key_is_checked_as_a_whole(spark: SparkSession) -> None:
    """Repeated values in one key column are fine while the full key stays unique."""
    df = spark.createDataFrame(
        [
            Row(date_utc=DAY, shop_id=1, earnings_eur=Decimal("1.00")),
            Row(date_utc=DAY, shop_id=2, earnings_eur=Decimal("1.00")),
            Row(date_utc=date(2026, 1, 2), shop_id=1, earnings_eur=Decimal("1.00")),
        ],
        schema=NULLABLE_SCHEMA,
    )

    validate_pyspark_dataframe(df, EXPECTED_SCHEMA)


# One column per rule kind, each rule in the field's metadata as the contract writes it.
RULES_SCHEMA = StructType(
    [
        StructField(
            "code",
            StringType(),
            nullable=False,
            metadata={
                "primaryKey": True,
                "minLength": 5,
                "maxLength": 5,
                "pattern": "^AB-[0-9]{2}$",
            },
        ),
        StructField("status", StringType(), nullable=True, metadata={"enum": ["open", "done"]}),
        StructField(
            "amount",
            DecimalType(10, 2),
            nullable=True,
            metadata={"minimum": "0", "maximum": "100.00"},
        ),
        StructField("quantity", LongType(), nullable=True, metadata={"exclusiveMaximum": "10"}),
        StructField("day", DateType(), nullable=True, metadata={"exclusiveMinimum": "2026-01-01"}),
    ]
)

RULES_NULLABLE_SCHEMA = StructType(
    [StructField(field.name, field.dataType, nullable=True) for field in RULES_SCHEMA]
)

GOOD = {
    "code": "AB-01",
    "status": "open",
    "amount": Decimal("100.00"),
    "quantity": 9,
    "day": date(2026, 1, 2),
}


def rules_df(spark: SparkSession, *changes: dict[str, object]) -> DataFrame:
    """Build one row per entry of `changes`, each GOOD with that entry's values replaced."""
    return spark.createDataFrame(
        [Row(**{**GOOD, **change}) for change in changes], schema=RULES_NULLABLE_SCHEMA
    )


def test_rows_meeting_every_rule_pass(spark: SparkSession) -> None:
    """Values on the inclusive bounds pass, and a null passes every rule but the null check."""
    df = rules_df(
        spark,
        {},
        {"code": "AB-02", "amount": Decimal("0.00")},
        {"code": "AB-03", "status": None, "amount": None, "quantity": None, "day": None},
    )

    validate_pyspark_dataframe(df, RULES_SCHEMA)


def test_value_outside_the_allowed_values_fails(spark: SparkSession) -> None:
    """A value missing from the field's `enum` fails, counted per column."""
    df = rules_df(spark, {"status": "lost"})

    with pytest.raises(ValueError, match=r"values outside the allowed values \{'status': 1\}"):
        validate_pyspark_dataframe(df, RULES_SCHEMA)


def test_values_breaking_bounds_fail(spark: SparkSession) -> None:
    """Each bound is compared in its column's type: decimal, whole number and date."""
    df = rules_df(
        spark,
        {"code": "AB-01", "amount": Decimal("-0.01")},
        {"code": "AB-02", "amount": Decimal("100.01"), "quantity": 10, "day": date(2026, 1, 1)},
    )

    with pytest.raises(ValueError) as raised:
        validate_pyspark_dataframe(df, RULES_SCHEMA)

    assert (
        "values out of bounds {'amount (minimum 0)': 1, 'amount (maximum 100.00)': 1, "
        "'quantity (exclusiveMaximum 10)': 1, 'day (exclusiveMinimum 2026-01-01)': 1}"
    ) in str(raised.value)


def test_strings_of_the_wrong_length_fail(spark: SparkSession) -> None:
    """A string shorter than minLength or longer than maxLength fails."""
    df = rules_df(spark, {"code": "AB-1"}, {"code": "AB-123"})

    with pytest.raises(ValueError) as raised:
        validate_pyspark_dataframe(df, RULES_SCHEMA)

    assert "strings of the wrong length {'code (minLength 5)': 1, 'code (maxLength 5)': 1}" in str(
        raised.value
    )


def test_allowed_values_written_as_text_are_compared_in_their_column_type(
    spark: SparkSession,
) -> None:
    """A decimal or date column's allowed values are text in the metadata, read as its type."""
    schema = StructType(
        [
            StructField("fee", DecimalType(10, 2), metadata={"enum": ["0.00", "4.50"]}),
            StructField("day", DateType(), metadata={"enum": ["2026-01-01"]}),
        ]
    )
    df = spark.createDataFrame(
        [
            Row(fee=Decimal("4.50"), day=date(2026, 1, 1)),
            Row(fee=Decimal("4.51"), day=date(2026, 1, 2)),
        ],
        schema,
    )

    with pytest.raises(
        ValueError, match=r"values outside the allowed values \{'fee': 1, 'day': 1\}"
    ):
        validate_pyspark_dataframe(df, schema)


def test_timestamp_rules_without_an_offset_are_read_in_the_column_zone(
    spark: SparkSession,
) -> None:
    """Without an offset, a bound or allowed value is in defaultTimezone, else in UTC.

    Each column allows exactly the instant 2026-01-01T00:00:00Z, so the check gives the
    same result wherever it runs.
    """
    schema = StructType(
        [
            StructField(
                "local_ts",
                TimestampType(),
                metadata={"minimum": "2026-01-01T01:00:00", "defaultTimezone": "Europe/Warsaw"},
            ),
            StructField("utc_ts", TimestampType(), metadata={"enum": ["2026-01-01T00:00:00"]}),
        ]
    )
    instant = datetime(2026, 1, 1, tzinfo=UTC)
    before = datetime(2025, 12, 31, 23, 59, 59, tzinfo=UTC)
    validate_pyspark_dataframe(
        spark.createDataFrame([Row(local_ts=instant, utc_ts=instant)], schema), schema
    )

    with pytest.raises(ValueError) as raised:
        validate_pyspark_dataframe(
            spark.createDataFrame([Row(local_ts=before, utc_ts=before)], schema), schema
        )

    assert "values outside the allowed values {'utc_ts': 1}" in str(raised.value)
    assert "values out of bounds {'local_ts (minimum 2026-01-01T01:00:00)': 1}" in str(raised.value)


def test_string_not_matching_the_pattern_fails(spark: SparkSession) -> None:
    """A string of the right length that doesn't match the pattern fails."""
    df = rules_df(spark, {"code": "XY-01"})

    with pytest.raises(ValueError, match=r"strings not matching the pattern \{'code': 1\}"):
        validate_pyspark_dataframe(df, RULES_SCHEMA)


def test_every_broken_rule_is_reported_at_once(spark: SparkSession) -> None:
    """Nulls, rule violations and duplicate keys are all listed in one error."""
    df = rules_df(spark, {"code": None}, {"status": "lost"}, {"status": "done"})

    with pytest.raises(ValueError) as raised:
        validate_pyspark_dataframe(df, RULES_SCHEMA)

    message = str(raised.value)
    assert message.startswith("Data validation failed: nulls in non-nullable columns {'code': 1}")
    assert "values outside the allowed values {'status': 1}" in message
    assert "1 duplicate row(s) detected across ['code']" in message


def test_schema_problem_stops_before_the_data_checks(spark: SparkSession) -> None:
    """A wrong type is reported alone: the value checks need the columns typed right."""
    df = spark.createDataFrame(
        [Row(date_utc=DAY, shop_id=None, earnings_eur="1.00")],
        "date_utc date, shop_id int, earnings_eur string",
    )

    with pytest.raises(ValueError) as raised:
        validate_pyspark_dataframe(df, EXPECTED_SCHEMA)

    assert str(raised.value).startswith("Schema validation failed")
    assert "nulls" not in str(raised.value)


def test_schema_without_a_key_is_checked_for_duplicate_rows(spark: SparkSession) -> None:
    """With no primaryKey column, two identical rows fail, and rows that differ pass."""
    schema = StructType([StructField("name", StringType()), StructField("size", IntegerType())])
    validate_pyspark_dataframe(
        spark.createDataFrame([Row(name="a", size=1), Row(name="a", size=2)], schema), schema
    )

    with pytest.raises(ValueError, match="1 duplicate row"):
        validate_pyspark_dataframe(
            spark.createDataFrame([Row(name="a", size=1), Row(name="a", size=1)], schema), schema
        )


def test_bound_on_a_string_column_raises(spark: SparkSession) -> None:
    """A bound needs a number, date or timestamp column to compare against."""
    schema = StructType([StructField("name", StringType(), metadata={"minimum": "a"})])
    df = spark.createDataFrame([Row(name="b")], schema)

    with pytest.raises(ValueError, match="A bound needs a number, date or timestamp column"):
        validate_pyspark_dataframe(df, schema)
