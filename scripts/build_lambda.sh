#!/usr/bin/env bash
# Build build/lambdas.zip: the kpi_lakehouse.lambdas package, the raw data contracts and the
# Lambda dependencies for Linux x86_64 / Python 3.11. One zip serves every Lambda function; each
# function picks its own handler, e.g. kpi_lakehouse.lambdas.shop_sim_extract.handler.handler.
#
# The zip is byte-for-byte reproducible: the same sources and requirements give the same file,
# so Terraform re-uploads it and updates the functions only when something actually changed.
# Run from anywhere: `./scripts/build_lambda.sh`. CI runs it before `terraform plan`.
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON_VERSION="3.11"
BUILD_DIR="build/lambda"
PKG="$BUILD_DIR/package"
ZIP="build/lambdas.zip"

rm -rf "$BUILD_DIR" "$ZIP"
mkdir -p "$PKG"

# Dependencies, as prebuilt Linux wheels for the Lambda runtime; no .pyc, so no build timestamps.
python3 -m pip install --quiet --disable-pip-version-check --no-compile --target "$PKG" \
  --platform manylinux2014_x86_64 --implementation cp --python-version "$PYTHON_VERSION" \
  --only-binary=:all: -r src/kpi_lakehouse/lambdas/requirements.txt

# The package: kpi_lakehouse/__init__.py and kpi_lakehouse/lambdas/, without caches or the
# requirements file. The Glue code beside it is not needed.
mkdir -p "$PKG/kpi_lakehouse"
cp src/kpi_lakehouse/__init__.py "$PKG/kpi_lakehouse/"
(cd src/kpi_lakehouse && find lambdas -type f -not -path '*/__pycache__/*' -not -name '*.pyc' \
  -not -name 'requirements.txt' | while read -r f; do
    mkdir -p "../../$PKG/kpi_lakehouse/$(dirname "$f")"
    cp "$f" "../../$PKG/kpi_lakehouse/$f"
  done)

# Every raw data contract, where common/contracts.py looks for the bundled copy.
mkdir -p "$PKG/kpi_lakehouse/lambdas/common/contracts"
cp src/kpi_lakehouse/contracts/*_raw_data_contract.yaml "$PKG/kpi_lakehouse/lambdas/common/contracts/"

# Reproducible zip: fixed timestamps and permissions, files in a fixed order, no extra attributes.
find "$PKG" -type d -exec chmod 755 {} +
find "$PKG" -type f -exec chmod 644 {} +
find "$PKG" -exec touch -h -t 198001010000 {} +
(cd "$PKG" && find . -type f | LC_ALL=C sort | zip -q -X -D "../../lambdas.zip" -@)

echo "built $ZIP ($(du -h "$ZIP" | cut -f1))"
