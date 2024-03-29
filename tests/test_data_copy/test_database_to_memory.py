from __future__ import annotations

from typing import Type

import pytest
from pandas import DataFrame

from dcp import DatabaseCursorToRecordsIterator
from dcp.data_copy.base import CopyRequest
from dcp.data_copy.copiers.to_memory.database_to_memory import (
    DatabaseTableToRecords,
    DatabaseTableToRecordsIterator,
)
from dcp.data_copy.graph import execute_copy_path, get_datacopy_lookup
from dcp.data_format.formats.memory.dataframe_iterator import DataFrameIteratorFormat
from dcp.data_format.formats.memory.records import RecordsFormat
from dcp.data_format.formats.memory.records_iterator import RecordsIteratorFormat
from dcp.storage.base import (
    Storage,
    ensure_storage_object,
)
from dcp.storage.database.api import DatabaseApi
from dcp.storage.memory.engines.python import new_local_python_storage
from dcp.utils.pandas import assert_dataframes_are_almost_equal
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
    mem_api = mem_s.get_memory_api()
    if not s.get_database_api().dialect_is_supported():
        return
    with api_cls.temp_local_database() as db_url:
        name = "_test"
        db_s = Storage.from_url(db_url)
        db_api = db_s.get_database_api()
        db_api.execute_sql(
            f"create table {name} as select '1' f1, 2 f2 union all select '3' f1, 4 f2"
        )
        records = [
            {"f1": "1", "f2": 2},
            {"f1": "3", "f2": 4},
        ]

        # Records
        to_name = name + "records"
        from_so = ensure_storage_object(name, storage=db_s)
        to_so = ensure_storage_object(
            to_name,
            storage=mem_s,
            _data_format=RecordsFormat,
            _schema=test_records_schema,
        )
        req = CopyRequest(from_so, to_so)
        DatabaseTableToRecords().copy(req)
        assert mem_api.get(to_name) == records

        # Records iterator
        to_name = name + "iterator"
        from_so = ensure_storage_object(name, storage=db_s)
        to_so = ensure_storage_object(
            to_name,
            storage=mem_s,
            _data_format=RecordsIteratorFormat,
            _schema=test_records_schema,
        )
        req = CopyRequest(from_so, to_so)
        DatabaseTableToRecordsIterator().copy(req)
        obj = mem_api.get(to_name)
        assert not isinstance(obj, list)
        assert list(obj) == records
        obj.close()

        # While we're here, test some memory to memory conversions
        to_name = name + "dfiterator"
        from_name = name
        from_so = ensure_storage_object(from_name, storage=db_s)
        to_so = ensure_storage_object(
            to_name,
            storage=mem_s,
            _data_format=DataFrameIteratorFormat,
            _schema=test_records_schema,
        )
        req = CopyRequest(from_so, to_so)
        pth = get_datacopy_lookup().get_lowest_cost_path(req.conversion)
        assert pth is not None
        execute_copy_path(req, pth)
        obj = mem_api.get(to_name)
        assert not isinstance(obj, list)
        dfs = list(obj.chunks(100))
        assert len(dfs) == 1
        assert_dataframes_are_almost_equal(dfs[0], DataFrame(records))
        obj.close()

        with db_api.execute_sql_result(f"select * from {name} limit 1") as res:
            from_name = name + "cursor"
            to_name = name + "cursor_records"
            mem_api.put(from_name, res)
            from_so = ensure_storage_object(from_name, storage=mem_s)
            to_so = ensure_storage_object(
                to_name,
                storage=mem_s,
                _data_format=RecordsIteratorFormat,
                _schema=test_records_schema,
            )
            req = CopyRequest(from_so, to_so)
            DatabaseCursorToRecordsIterator().copy(req)
            obj = mem_api.get(to_name)
            assert not isinstance(obj, list)
            assert list(obj) == records[:1]
            obj.close()
