from __future__ import annotations

import warnings
from copy import deepcopy
from typing import Type
import tempfile
from dcp.data_copy.copiers.to_database.file_to_database import CsvFileToDatabaseTable
from dcp.storage.file_system.engines.base import FileSystemStorageApi

import pytest
from dcp.data_copy.base import Conversion, CopyRequest, StorageFormat
from dcp.data_copy.copiers.to_database.memory_to_database import RecordsToDatabaseTable
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.storage.base import DatabaseStorageClass, LocalPythonStorageEngine, Storage
from dcp.storage.database.api import DatabaseApi, DatabaseStorageApi
from dcp.storage.memory.engines.python import PythonStorageApi, new_local_python_storage
from tests.utils import (
    conformed_test_records,
    csv_lines,
    test_records,
    test_records_schema,
)


@pytest.mark.parametrize(
    "url",
    [
        # "sqlite://",
        "postgresql://localhost",
        # "mysql://",
    ],
)
def test_file_to_db(url):
    dr = tempfile.gettempdir()
    from_s: Storage = Storage.from_url(f"file://{dr}")
    fs_api: FileSystemStorageApi = from_s.get_api()
    to_s: Storage = Storage.from_url(url)
    api_cls: Type[DatabaseApi] = to_s.storage_engine.get_api_cls()
    if not to_s.get_api().dialect_is_supported():
        warnings.warn(
            f"Skipping tests for database engine {s.storage_engine.__name__} (client library not installed)"
        )
        return
    name = "_test"
    fs_api.write_lines_to_file(
        name, [ln.strip("\n") for ln in csv_lines(conformed_test_records)]
    )
    with api_cls.temp_local_database() as db_url:
        name = "_test"
        db_s = Storage.from_url(db_url)
        db_api: DatabaseStorageApi = db_s.get_api()
        # Records
        req = CopyRequest(
            name, from_s, name, db_s, DatabaseTableFormat, test_records_schema
        )
        CsvFileToDatabaseTable().copy(req)
        with db_api.execute_sql_result(f"select * from {name}") as res:
            if url.startswith("sqlite"):
                assert [dict(r) for r in res] == test_records
            else:
                assert [dict(r) for r in res] == conformed_test_records
