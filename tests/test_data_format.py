from __future__ import annotations

from typing import Any

import pytest
from commonmodel.field_types import DEFAULT_FIELD_TYPE, Date, Integer, Text
from pandas.core.frame import DataFrame
from pandas.core.series import Series

from dcp.data_format.base import (
    ALL_DATA_FORMATS,
    DataFormat,
    DataFormatBase,
    UnknownFormat,
)
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.data_format.handler import get_handler
from dcp.storage.base import (
    Storage,
    StorageClass,
    StorageEngine,
    ensure_storage_object,
)
from dcp.storage.database.utils import get_tmp_sqlite_db_url
from dcp.utils.pandas import assert_dataframes_are_almost_equal
from tests.utils import test_data_format_objects, test_records, test_records_schema


def test_formats():
    for fmt in ALL_DATA_FORMATS:
        assert issubclass(fmt, DataFormatBase)
        if fmt is UnknownFormat:
            continue
        assert issubclass(fmt.get_natural_storage_class(), StorageClass)
        eng = fmt.get_natural_storage_engine()
        if eng is not None:
            assert issubclass(eng, StorageEngine)
        assert isinstance(fmt.is_storable(), bool)


def assert_objects_equal(o1: Any, o2: Any):
    if isinstance(o1, DataFrame):
        assert_dataframes_are_almost_equal(o1, o2)
    elif isinstance(o1, Series):
        assert list(o1) == list(o2)
    else:
        assert o1 == o2


@pytest.mark.parametrize("fmt,py_obj", test_data_format_objects)
def test_memory_handlers(fmt: DataFormat, py_obj: Any):
    s = Storage("python://test")
    name = "_test"
    s.get_memory_api().put(name, py_obj())
    obj = ensure_storage_object(name, storage=s)
    handler = get_handler(fmt, s.storage_engine)
    assert list(handler().infer_field_names(obj)) == list(test_records[0].keys())
    assert handler().infer_field_type(obj, "f1") == Text()
    assert handler().infer_field_type(obj, "f2") == Integer()
    assert handler().infer_field_type(obj, "f3") == DEFAULT_FIELD_TYPE
    assert handler().infer_field_type(obj, "f4") == Date()
    assert handler().infer_field_type(obj, "f5") == DEFAULT_FIELD_TYPE

    handler().cast_to_field_type(obj, "f4", Text())
    handler().cast_to_field_type(obj, "f4", Date())
    round_trip_object = s.get_memory_api().get(name)
    assert_objects_equal(round_trip_object, py_obj())


def test_database_handler():
    dburl = get_tmp_sqlite_db_url()
    s = Storage(dburl)
    name = "_test"
    handler = get_handler(DatabaseTableFormat, s.storage_engine)
    obj = ensure_storage_object(name, storage=s)
    handler().create_empty(obj, test_records_schema)
    s.get_database_api().bulk_insert_records(obj, test_records, test_records_schema)
    assert list(handler().infer_field_names(obj)) == list(test_records[0].keys())
    assert handler().infer_field_type(obj, "f1") == Text()
    assert handler().infer_field_type(obj, "f2") == Integer()
    assert handler().infer_field_type(obj, "f3") == DEFAULT_FIELD_TYPE
    assert handler().infer_field_type(obj, "f4") == Date()
    assert handler().infer_field_type(obj, "f5") == DEFAULT_FIELD_TYPE

    # TODO
    # handler().cast_to_field_type(name, s, "f4", Date())
    # handler().cast_to_field_type(name, s, "f4", Text())
    # round_trip_object = s.get_memory_api().get(name)
    # assert_objects_equal(round_trip_object, obj())
