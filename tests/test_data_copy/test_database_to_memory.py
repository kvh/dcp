from __future__ import annotations

from typing import Type


import pytest
from pandas import DataFrame

from dcp.data_copy.base import Conversion, CopyRequest, StorageFormat
from dcp.data_copy.copiers.to_memory.database_to_memory import (
    DatabaseTableToRecords,
    DatabaseTableToRecordsIterator,
)
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.data_format.formats.memory.records import RecordsFormat
from dcp.data_format.formats.memory.records_iterator import RecordsIteratorFormat
from dcp.storage.base import DatabaseStorageClass, LocalPythonStorageEngine, Storage
from dcp.storage.database.api import DatabaseApi, DatabaseStorageApi
from dcp.storage.memory.engines.python import PythonStorageApi, new_local_python_storage
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
        db_api.execute_sql(
            f"create table {name} as select 1 a, 2 b union all select 3 a, 4 b"
        )
        records = [
            {"a": 1, "b": 2},
            {"a": 3, "b": 4},
        ]

        # Records
        to_name = name + "records"
        req = CopyRequest(
            name, db_s, to_name, mem_s, RecordsFormat, test_records_schema
        )
        DatabaseTableToRecords().copy(req)
        assert mem_api.get(to_name) == records

        # Records iterator
        to_name = name + "iterator"
        req = CopyRequest(
            name, db_s, to_name, mem_s, RecordsIteratorFormat, test_records_schema
        )
        DatabaseTableToRecordsIterator().copy(req)
        obj = mem_api.get(to_name)
        assert not isinstance(obj, list)
        assert list(obj) == records
        obj.close()
