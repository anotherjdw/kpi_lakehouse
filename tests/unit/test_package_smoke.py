"""Scaffold-owned smoke test: the package imports and declares a version.

Catches a broken package __init__ or a failed editable install before any other test
imports from the package.
"""

import kpi_lakehouse


def test_package_imports_and_declares_a_version() -> None:
    """The package is importable and exposes a non-empty __version__."""
    assert kpi_lakehouse.__version__
