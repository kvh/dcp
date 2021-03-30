from __future__ import annotations

import tempfile
import types
import warnings
from io import StringIO
from typing import Optional, Type

import pytest
from snapflow.core.data_block import DataBlockMetadata, create_data_block_from_records
from snapflow.storage.data_copy.base import (
    Conversion,
    DataCopier,
    NetworkToMemoryCost,
    NoOpCost,
    StorageFormat,
    datacopy,
    get_datacopy_lookup,
)
from snapflow.storage.data_copy.database_to_memory import copy_db_to_records
from snapflow.storage.data_copy.memory_to_database import (
    copy_records_iterator_to_db,
    copy_records_to_db,
)
from snapflow.storage.data_formats import (
    DatabaseCursorFormat,
    DatabaseTableFormat,
    DatabaseTableRefFormat,
    DataFrameFormat,
    DelimitedFileFormat,
    JsonLinesFileFormat,
    RecordsFormat,
    RecordsIteratorFormat,
)
from snapflow.storage.data_formats.data_frame import DataFrameIteratorFormat
from snapflow.storage.data_formats.delimited_file_object import (
    DelimitedFileObjectFormat,
)
from snapflow.storage.data_records import MemoryRecordsObject, as_records
from snapflow.storage.db.api import DatabaseApi, DatabaseStorageApi
from snapflow.storage.storage import (
    DatabaseStorageClass,
    FileSystemStorageClass,
    LocalPythonStorageEngine,
    PostgresStorageEngine,
    PythonStorageApi,
    MemoryStorageClass,
    Storage,
    clear_local_storage,
    new_local_python_storage,
)
from tests.utils import TestSchema1, TestSchema4

records = [{"f1": "hi", "f2": 1}, {"f1": "bye", "f2": 2}]
records_itr = (lambda: ([r] for r in records),)[0]


@pytest.mark.parametrize(
    "url", ["sqlite://", "postgresql://localhost", "mysql://",],
)
def test_records_to_db(url):
    s: Storage = Storage.from_url(url)
    api_cls: Type[DatabaseApi] = s.storage_engine.get_api_cls()
    if not s.get_api().dialect_is_supported():
        warnings.warn(
            f"Skipping tests for database engine {s.storage_engine.__name__} (client library not installed)"
        )
        return
    mem_api: PythonStorageApi = new_local_python_storage().get_api()
    with api_cls.temp_local_database() as db_url:
        name = "_test"
        db_api: DatabaseStorageApi = Storage.from_url(db_url).get_api()
        # Records
        mdr = as_records(records)
        mem_api.put(name, mdr)
        conversion = Conversion(
            StorageFormat(LocalPythonStorageEngine, RecordsFormat),
            StorageFormat(s.storage_engine, DatabaseTableFormat),
        )
        copy_records_to_db.copy(
            name, name, conversion, mem_api, db_api, schema=TestSchema4
        )
        with db_api.execute_sql_result(f"select * from {name}") as res:
            assert [dict(r) for r in res] == records


@pytest.mark.parametrize(
    "url", ["sqlite://", "postgresql://localhost", "mysql://",],
)
def test_records_iterator_to_db(url):
    s: Storage = Storage.from_url(url)
    api_cls: Type[DatabaseApi] = s.storage_engine.get_api_cls()
    if not s.get_api().dialect_is_supported():
        return
    mem_api: PythonStorageApi = new_local_python_storage().get_api()
    with api_cls.temp_local_database() as db_url:
        name = "_test"
        db_api: DatabaseStorageApi = Storage.from_url(db_url).get_api()
        # Records
        mdr = as_records(records_itr())
        mem_api.put(name, mdr)
        conversion = Conversion(
            StorageFormat(LocalPythonStorageEngine, RecordsIteratorFormat),
            StorageFormat(s.storage_engine, DatabaseTableFormat),
        )
        copy_records_iterator_to_db.copy(
            name, name, conversion, mem_api, db_api, schema=TestSchema4
        )
        with db_api.execute_sql_result(f"select * from {name}") as res:
            assert [dict(r) for r in res] == records
