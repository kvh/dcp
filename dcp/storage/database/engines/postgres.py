from __future__ import annotations

from contextlib import contextmanager
from io import IOBase
from typing import Dict, Iterator, List, Optional, Callable
from commonmodel.base import Schema
from sqlalchemy.exc import OperationalError

from dcp.storage.base import StorageObject, FullPath, ensure_storage_object
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
from loguru import logger
from sqlalchemy.engine.base import Engine

from dcp.utils.data import conform_records_for_insert

POSTGRES_SUPPORTED = False
try:
    from psycopg2.extras import execute_values

    POSTGRES_SUPPORTED = True
except ImportError:

    def execute_values(*args):
        raise ImportError("Psycopg2 not installed")


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

    def get_default_storage_path(self) -> list[str]:
        try:
            with self.execute_sql_result("select current_schema()") as r:
                return [list(r)[0][0]]
        except OperationalError:
            # Database is offline or unavailable
            return ["public"]  # Default to postgres default?

    ### Overrides

    def _exists(self, obj: StorageObject) -> bool:
        """MUST also check for views"""
        meta_tables = ["information_schema.tables", "information_schema.views"]
        sql = f"select table_name from %s where table_name = '{obj.full_path.name}'"
        if obj.full_path.path:
            assert (
                len(obj.full_path.path) == 1
            ), f"Database table path must have length one {obj.full_path}"
            sql += f" and table_schema = '{obj.full_path.path[0]}'"

        sql = " union all ".join([sql % m for m in meta_tables])
        with self.execute_sql_result(sql) as res:
            table_cnt = len(list(res))
            return table_cnt > 0

    def _bulk_insert(
        self, table: StorageObject, records: list[dict], schema: Optional[Schema] = None
    ):
        self._bulk_insert_postgres(
            table=table,
            records=records,
            schema=schema,
        )

    def _bulk_insert_postgres(self, *args, **kwargs):
        kwargs["update"] = False
        return self._bulk_upsert(*args, **kwargs)

    def _bulk_upsert(
        self,
        table: StorageObject,
        records: List[Dict],
        # unique_on_column: str = None,
        # ignore_duplicates: bool = False,
        update: bool = True,
        page_size: int = 5000,
        schema: Optional[Schema] = None,
    ):
        if update:
            # TODO: we don't use this anywhere? Let's deprecate
            raise NotImplementedError
        if not records:
            return
        if schema:
            columns = schema.field_names()
        else:
            columns = columns_from_records(records)
        records = self.conform_records_for_insert(records, columns)
        tmpl = "bulk_insert.sql"
        jinja_ctx = {
            "table_name": table.formatted_full_name,
            "columns": columns,
            "records": records,
            "unique_on_column": "",
            "ignore_duplicates": False,
        }
        sql = compile_jinja_sql_template(tmpl, jinja_ctx)
        logger.debug("SQL", sql)
        pg_execute_values(self.get_engine(), sql, records, page_size=page_size)

    def _bulk_insert_file(
        self, table: StorageObject, f: IOBase, schema: Optional[Schema] = None
    ):
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
                COPY {table.formatted_full_name} {cols}
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
