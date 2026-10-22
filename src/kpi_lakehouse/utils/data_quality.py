"""Data quality checks that return a result instead of raising.

Each check returns a CheckResult, so the caller decides whether a failure halts the
job. validate_pyspark_dataframe (utils/dataframes.py) raises on a failed uniqueness
check before every write.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame

# ---------------------------------------------------------------------------
# Result contract
# ---------------------------------------------------------------------------


@dataclass
class CheckResult:
    """Encapsulates the outcome of a single data quality check.

    Attributes:
        check_name: Human-readable identifier for the check
            (e.g. "no_nulls[order_id]").
        passed: True if the check passed; False if a violation was detected.
        message: A concise description of the outcome, including violation
            counts where relevant.
        details: Optional structured data for downstream logging or alerting
            (e.g. the list of violating values, column name, row counts).
    """

    check_name: str
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """Render the result as one log line, e.g. "[FAIL] no_duplicate_rows[...]: ...".

        Returns:
            The status, check name and message on one line.
        """
        status = "PASS" if self.passed else "FAIL"
        return f"[{status}] {self.check_name}: {self.message}"


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def check_no_duplicate_rows(
    df: DataFrame,
    subset: list[str] | None = None,
) -> CheckResult:
    """Asserts that no duplicate rows exist in the DataFrame.

    When `subset` is provided, uniqueness is enforced only across those
    columns — useful for composite business key checks (e.g. one active
    record per customer_id + product_id). When None, all columns are used
    for a full-row deduplication check.

    Args:
        df: The DataFrame to validate.
        subset: Column names to consider for duplicate detection. When None,
            all columns are used. Defaults to None.

    Returns:
        A CheckResult that passes only if no duplicate rows are found
        across the specified columns.
    """
    check_name = f"no_duplicate_rows[{subset or 'all_columns'}]"
    total_rows = df.count()
    distinct_rows = df.dropDuplicates(subset).count() if subset else df.distinct().count()
    duplicate_count = total_rows - distinct_rows
    passed = duplicate_count == 0

    return CheckResult(
        check_name=check_name,
        passed=passed,
        message=(
            f"No duplicate rows found. Total rows: {total_rows}."
            if passed
            else f"{duplicate_count} duplicate row(s) detected across "
            f"{subset or 'all columns'}. Total: {total_rows}, distinct: {distinct_rows}."
        ),
        details={
            "subset": subset or df.columns,
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicate_count": duplicate_count,
        },
    )
