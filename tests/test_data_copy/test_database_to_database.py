from __future__ import annotations
from dcp.data_copy.copiers.to_database.database_to_database import (
    DatabaseTableToDatabaseTable,
    PostgresTableToPostgresTable,
)

import warnings
from copy import deepcopy
from typing import Type

import pytest
from dcp.data_copy.base import Conversion, CopyRequest, StorageFormat
from dcp.data_copy.copiers.to_database.memory_to_database import RecordsToDatabaseTable
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.storage.base import DatabaseStorageClass, LocalPythonStorageEngine, Storage
from dcp.storage.database.api import DatabaseApi, DatabaseStorageApi
from dcp.storage.memory.engines.python import PythonStorageApi, new_local_python_storage
from tests.utils import conformed_test_records, test_records, test_records_schema


@pytest.mark.parametrize(
    "url", ["sqlite://", "postgresql://localhost", "mysql://",],
)
def test_db_to_db(url):
    s: Storage = Storage.from_url(url)
    api_cls: Type[DatabaseApi] = s.storage_engine.get_api_cls()
    if not s.get_api().dialect_is_supported():
        warnings.warn(
            f"Skipping tests for database engine {s.storage_engine.__name__} (client library not installed)"
        )
        return
    with api_cls.temp_local_database() as from_url:
        name = "_test"
        from_s = Storage.from_url(from_url)
        from_api: DatabaseStorageApi = from_s.get_api()
        from_api.execute_sql(f"create table {name} as select 1 a, 2 b")
        to_name = "_test_to"

        # Test within same database
        req = CopyRequest(
            name, from_s, to_name, from_s, DatabaseTableFormat,  # test_records_schema
        )
        DatabaseTableToDatabaseTable().copy(req)
        with from_api.execute_sql_result(f"select * from {to_name}") as res:
            if url.startswith("sqlite"):
                assert [dict(r) for r in res] == [{"a": "1", "b": "2"}]
            else:
                assert [dict(r) for r in res] == [{"a": 1, "b": 2}]

        # Test between separate dbs
        with api_cls.temp_local_database() as to_url:
            to_s = Storage.from_url(to_url)
            to_api: DatabaseStorageApi = to_s.get_api()
            req = CopyRequest(
                name, from_s, to_name, to_s, DatabaseTableFormat,  # test_records_schema
            )
            if url.startswith("post"):
                PostgresTableToPostgresTable().copy(req)
            else:
                DatabaseTableToDatabaseTable().copy(req)
            with to_api.execute_sql_result(f"select * from {to_name}") as res:
                if url.startswith("sqlite"):
                    assert [dict(r) for r in res] == [{"a": "1", "b": "2"}]
                else:
                    assert [dict(r) for r in res] == [{"a": 1, "b": 2}]

