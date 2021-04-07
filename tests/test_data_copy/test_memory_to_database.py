from __future__ import annotations
from datacopy.data_format.formats.database.base import DatabaseTableFormat
from datacopy.data_copy.copiers.to_database.memory_to_database import copy_records_to_db
import warnings
from datacopy.storage.database.api import DatabaseApi, DatabaseStorageApi
from datacopy.storage.base import (
    DatabaseStorageClass,
    LocalPythonStorageEngine,
    Storage,
)
from datacopy.data_copy.base import Conversion, CopyRequest, StorageFormat
from datacopy.storage.memory.engines.python import (
    PythonStorageApi,
    new_local_python_storage,
)

from typing import Type

import pytest
from tests.utils import (
    test_records_schema,
    conformed_test_records,
)


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
    mem_s = new_local_python_storage()
    mem_api: PythonStorageApi = mem_s.get_api()
    with api_cls.temp_local_database() as db_url:
        name = "_test"
        db_s = Storage.from_url(db_url)
        db_api: DatabaseStorageApi = db_s.get_api()
        # Records
        mem_api.put(name, conformed_test_records)
        req = CopyRequest(
            name, mem_s, name, DatabaseTableFormat, db_s, test_records_schema
        )
        copy_records_to_db.copy(req)
        with db_api.execute_sql_result(f"select * from {name}") as res:
            assert [dict(r) for r in res] == conformed_test_records

