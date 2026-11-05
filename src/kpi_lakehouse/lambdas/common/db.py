"""Postgres connection and table layout for the shop backend (writer and extract Lambdas).

On AWS the Lambdas log in with IAM database authentication: the token is signed locally from
the function's role, so no secret has to be fetched and no Secrets Manager endpoint is needed.
For a local Postgres (Docker), set DB_PASSWORD and DB_SSL=false instead.

Environment variables: DB_HOST (required), DB_PORT (5432), DB_NAME (shop), DB_USER (simulator),
DB_PASSWORD (local only), DB_SSL (true).
"""

from __future__ import annotations

import os
import ssl
from pathlib import Path

import pg8000.native


def connect() -> pg8000.native.Connection:

    host = os.environ["DB_HOST"]
    port = int(os.environ.get("DB_PORT", "5432"))
    user = os.environ.get("DB_USER", "simulator")
    password = os.environ.get("DB_PASSWORD")
    ssl_context = None
    if os.environ.get("DB_SSL", "true").lower() != "false":
        bundle = Path(__file__).parent / "rds-global-bundle.pem"
        ssl_context = ssl.create_default_context(cafile=str(bundle) if bundle.exists() else None)
    if not password:
        import boto3

        region = os.environ.get("AWS_REGION", "eu-central-1")
        password = boto3.client("rds", region_name=region).generate_db_auth_token(
            DBHostname=host, Port=port, DBUsername=user, Region=region
        )
    # A paused Aurora Serverless v2 cluster takes about 15 seconds to resume on first connect.
    return pg8000.native.Connection(
        user=user,
        host=host,
        port=port,
        database=os.environ.get("DB_NAME", "shop"),
        password=password,
        ssl_context=ssl_context,
        timeout=60,
    )
