from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterator, List

from dcp.storage.database.api import DatabaseApi, DatabaseStorageApi
from dcp.storage.database.utils import get_tmp_sqlite_db_url


class SqliteDatabaseApi(DatabaseApi):
    def remove(self, name: str):
        self.execute_sql(f"drop table {name}")

    @classmethod
    @contextmanager
    def temp_local_database(cls, conn_url: str = None, **kwargs) -> Iterator[str]:
        db_url = get_tmp_sqlite_db_url("__test_dcp_sqlite")
        yield db_url

    def exists(self, table_name: str) -> bool:
        with self.execute_sql_result(f"select name from sqlite_master where type = 'table' and name = '{table_name}'") as res:
            table_cnt = len(list(res))
            return table_cnt > 0

class SqliteDatabaseStorageApi(DatabaseStorageApi, SqliteDatabaseApi):
    pass
