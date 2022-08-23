from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterator, List

import sqlalchemy
from sqlalchemy.engine import Inspector

from dcp.storage.database.api import (
    DatabaseApi,
    DatabaseStorageApi,
    create_db,
    dispose_all,
    drop_db,
)
from dcp.utils.common import rand_str

MYSQL_SUPPORTED = False
try:
    import MySQLdb

    MYSQL_SUPPORTED = True
except ImportError:
    pass


class MysqlDatabaseApi(DatabaseApi):
    def get_placeholder_char(self) -> str:
        return "%s"

    @classmethod
    def dialect_is_supported(cls) -> bool:
        return MYSQL_SUPPORTED

    def get_default_storage_path(self) -> list[str]:
        db = self.get_engine().url.database
        if db:
            return [self.get_engine().url.database]
        return []

    def get_schemas_and_table_names(self) -> dict[str, set[str]]:
        inspector: Inspector = sqlalchemy.inspect(self.get_engine())
        dbname = self.get_default_storage_path()[0]
        schemas_to_tables = {}
        schemas_to_tables[dbname] = set(inspector.get_table_names())
        return schemas_to_tables

    @classmethod
    @contextmanager
    def temp_local_database(cls, conn_url: str = None, **kwargs) -> Iterator[str]:
        test_db = f"__tmp_dcp_{rand_str(8).lower()}"
        url = conn_url or "mysql://root@localhost"
        create_db(url, test_db)
        test_url = f"{url}/{test_db}"
        try:
            yield test_url
        finally:
            dispose_all(test_db)
            drop_db(url, test_db)


class MysqlDatabaseStorageApi(DatabaseStorageApi, MysqlDatabaseApi):
    pass
