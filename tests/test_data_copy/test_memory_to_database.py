from __future__ import annotations

import warnings
from copy import deepcopy
from typing import Type

import pytest

from dcp.data_copy.base import CopyRequest
from dcp.data_copy.copiers.to_database.memory_to_database import RecordsToDatabaseTable
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.storage.base import (
    Storage,
    ensure_storage_object,
)
from dcp.storage.database.api import DatabaseApi
from dcp.storage.memory.engines.python import new_local_python_storage
from tests.utils import (
    conformed_test_records,
    test_records,
    test_records_schema,
    conformed_test_records_json_str,
    test_records_json_str,
)


@pytest.mark.parametrize(
    "url",
    [
        "sqlite://",
        "postgresql://localhost",
        "mysql://",
    ],
)
def test_records_to_db(url):
    s: Storage = Storage.from_url(url)
    api_cls: Type[DatabaseApi] = s.storage_engine.get_api_cls()
    if not s.get_database_api().dialect_is_supported():
        warnings.warn(
            f"Skipping tests for database engine {s.storage_engine.__name__} (client library not installed)"
        )
        return
    mem_s = new_local_python_storage()
    mem_api = mem_s.get_memory_api()
    with api_cls.temp_local_database() as db_url:
        name = "_test"
        db_s = Storage.from_url(db_url)
        db_api = db_s.get_database_api()
        # Records
        mem_api.put(name, deepcopy(conformed_test_records))
        from_so = ensure_storage_object(name, storage=mem_s)
        to_so = ensure_storage_object(
            name,
            storage=db_s,
            _data_format=DatabaseTableFormat,
            _schema=test_records_schema,
        )
        req = CopyRequest(from_so, to_so)
        RecordsToDatabaseTable().copy(req)
        with db_api.execute_sql_result(f"select * from {name}") as res:
            if url.startswith("sqlite"):
                assert [dict(r) for r in res] == test_records_json_str
            else:
                assert [dict(r) for r in res] == conformed_test_records_json_str
