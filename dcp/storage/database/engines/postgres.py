from __future__ import annotations

from contextlib import contextmanager
from io import IOBase
from typing import Dict, Iterator, List, Optional, Callable
from commonmodel.base import Schema

from dcp.storage.database.api import (
    DatabaseApi,
    DatabaseStorageApi,
    create_db,
    dispose_all,
    drop_db,
)
from dcp.storage.database.utils import (
    columns_from_records,
    compile_jinja_sql_template,
)
from dcp.utils.common import rand_str
from dcp.utils.data import conform_records_for_insert
from loguru import logger
from sqlalchemy.engine.base import Engine

POSTGRES_SUPPORTED = False
try:
    from psycopg2.extras import execute_values

    POSTGRES_SUPPORTED = True
except ImportError:

    def execute_values(*args):
        raise ImportError("Psycopg2 not installed")


def bulk_insert(*args, **kwargs):
    kwargs["update"] = False
    return bulk_upsert(*args, **kwargs)


def bulk_upsert(
    eng: Engine,
    table_name: str,
    records: List[Dict],
    unique_on_column: str = None,
    ignore_duplicates: bool = False,
    update: bool = True,
    columns: List[str] = None,
    adapt_objects_to_json: bool = True,
    page_size: int = 5000,
    schema: Optional[Schema] = None,
):
    if not records:
        return
    if update and not unique_on_column:
        raise Exception("Must specify unique_on_column when updating")
    if schema:
        columns = schema.field_names()
    else:
        columns = columns_from_records(records)
    records = conform_records_for_insert(records, columns, adapt_objects_to_json)
    if update:
        tmpl = "bulk_upsert.sql"
    else:
        tmpl = "bulk_insert.sql"
    jinja_ctx = {
        "table_name": table_name,
        "columns": columns,
        "records": records,
        "unique_on_column": unique_on_column,
        "ignore_duplicates": ignore_duplicates,
    }
    sql = compile_jinja_sql_template(tmpl, jinja_ctx)
    logger.debug("SQL", sql)
    pg_execute_values(eng, sql, records, page_size=page_size)


def pg_execute_values(
    eng: Engine, sql: str, records: List[Dict], page_size: int = 5000
):
    conn = eng.raw_connection()
    try:
        with conn.cursor() as curs:
            execute_values(
                curs,
                sql,
                records,
                template=None,
                page_size=page_size,
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


class PostgresDatabaseApi(DatabaseApi):
    def __init__(
        self,
        url: str,
        json_serializer: Callable = None,
    ):
        if url.startswith("postgres:"):
            url = (
                "postgresql" + url[8:]
            )  # sqlalchemy now only works with postgresql scheme
        super().__init__(url, json_serializer)

    @classmethod
    def dialect_is_supported(cls) -> bool:
        return POSTGRES_SUPPORTED

    def _bulk_insert(
        self,
        table_name: str,
        records: List[Dict],
        schema: Optional[Schema] = None,
        **kwargs,
    ):
        bulk_insert(
            eng=self.get_engine(),
            table_name=table_name,
            records=records,
            schema=schema,
            **kwargs,
        )

    def bulk_insert_file(self, name: str, f: IOBase, schema: Optional[Schema] = None):
        cols = ""
        if schema is not None:
            cols = (
                "("
                + ",".join(self.get_quoted_identifier(f) for f in schema.field_names())
                + ")"
            )
        conn = self.get_engine().raw_connection()
        try:
            with conn.cursor() as curs:
                # TODO: swap for copy_expert at some point
                sql = f"""
                COPY {self.get_quoted_identifier(name)} {cols}
                FROM STDIN
                csv header;
                """
                curs.copy_expert(sql, f)
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    @classmethod
    @contextmanager
    def temp_local_database(cls, conn_url: str = None, **kwargs) -> Iterator[str]:
        test_db = f"__tmp_dcp_{rand_str(8).lower()}"
        url = conn_url or "postgresql://localhost"
        pg_url = f"{url}/postgres"
        create_db(pg_url, test_db)
        test_url = f"{url}/{test_db}"
        try:
            yield test_url
        finally:
            dispose_all(test_db)
            drop_db(pg_url, test_db)


class PostgresDatabaseStorageApi(DatabaseStorageApi, PostgresDatabaseApi):
    pass
