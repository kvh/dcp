from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterator, List

from dcp.storage.database.api import (
    DatabaseApi,
    DatabaseStorageApi,
    create_db,
    dispose_all,
    drop_db,
)
from dcp.utils.common import rand_str
from dcp.utils.data import conform_records_for_insert

MYSQL_SUPPORTED = False
try:
    import MySQLdb

    MYSQL_SUPPORTED = True
except ImportError:
    pass


class MysqlDatabaseApi(DatabaseApi):
    @classmethod
    def dialect_is_supported(cls) -> bool:
        return MYSQL_SUPPORTED

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
