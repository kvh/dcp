from __future__ import annotations

from typing import Type

import pytest
from datacopy.data_copy.base import Conversion, CopyRequest, StorageFormat
from datacopy.data_copy.copiers.to_memory.database_to_memory import copy_db_to_records
from datacopy.data_format.formats.database.base import DatabaseTableFormat
from datacopy.data_format.formats.memory.records import RecordsFormat
from datacopy.storage.base import (
    DatabaseStorageClass,
    LocalPythonStorageEngine,
    Storage,
)
from datacopy.storage.database.api import DatabaseApi, DatabaseStorageApi
from datacopy.storage.memory.engines.python import (
    PythonStorageApi,
    new_local_python_storage,
)
from tests.utils import test_records_schema


@pytest.mark.parametrize(
    "url",
    [
        "sqlite://",
        "postgresql://localhost",
        "mysql://",
    ],
)
def test_db_to_mem(url):
    s: Storage = Storage.from_url(url)
    api_cls: Type[DatabaseApi] = s.storage_engine.get_api_cls()
    mem_s = new_local_python_storage()
    mem_api: PythonStorageApi = mem_s.get_api()
    if not s.get_api().dialect_is_supported():
        return
    with api_cls.temp_local_database() as db_url:
        name = "_test"
        db_s = Storage.from_url(db_url)
        db_api: DatabaseStorageApi = db_s.get_api()
        db_api.execute_sql(f"create table {name} as select 1 a, 2 b")
        req = CopyRequest(name, db_s, name, RecordsFormat, mem_s, test_records_schema)
        copy_db_to_records.copy(req)
        assert mem_api.get(name) == [{"a": 1, "b": 2}]
