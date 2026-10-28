import os
from collections.abc import Generator
from contextlib import contextmanager

from databricks import sql


def _connect():
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


@contextmanager
def get_cursor() -> Generator:
    conn = _connect()
    try:
        with conn.cursor() as cursor:
            yield cursor
    finally:
        conn.close()


def catalog() -> str:
    return os.environ.get("DATABRICKS_CATALOG", "nexus_tributario_prod")
