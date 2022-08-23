from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterator, List

from dcp.storage.base import StorageObject
from dcp.storage.database.api import DatabaseApi, DatabaseStorageApi
from dcp.storage.database.utils import get_tmp_sqlite_db_url


class SqliteDatabaseApi(DatabaseApi):
    def _remove(self, obj: StorageObject):
        self.execute_sql(f"drop table {obj.formatted_full_name}")

    def _dbapi_escape_sql(self, sql: str) -> str:
        # No escape chars in sqlite
        return sql

    def get_default_storage_path(self) -> list[str]:
        # No namespaces in sqlite
        return []

    @classmethod
    @contextmanager
    def temp_local_database(cls, conn_url: str = None, **kwargs) -> Iterator[str]:
        db_url = get_tmp_sqlite_db_url("__test_dcp_sqlite")
        yield db_url

    def _exists(self, obj: StorageObject) -> bool:
        with self.execute_sql_result(
            f"select name from sqlite_master where type in ('table', 'view') and name = '{obj.full_path.name}'"
        ) as res:
            table_cnt = len(list(res))
            return table_cnt > 0


class SqliteDatabaseStorageApi(DatabaseStorageApi, SqliteDatabaseApi):
    pass
