from __future__ import annotations

from contextlib import contextmanager
from typing import Dict, Iterator, List

from datacopy.storage.database.api import DatabaseApi, DatabaseStorageApi
from datacopy.storage.database.utils import get_tmp_sqlite_db_url


class SqliteDatabaseApi(DatabaseApi):
    @classmethod
    @contextmanager
    def temp_local_database(cls) -> Iterator[str]:
        db_url = get_tmp_sqlite_db_url("__test_snapflow_sqlite")
        yield db_url


class SqliteDatabaseStorageApi(DatabaseStorageApi, SqliteDatabaseApi):
    pass
