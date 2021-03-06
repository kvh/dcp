from __future__ import annotations

from dataclasses import asdict
from io import StringIO
from typing import Any, Callable

import pytest
from commonmodel.field_types import DEFAULT_FIELD_TYPE, Date, DateTime, Integer, Text
from dcp import data_format
from dcp.data_format.base import (
    ALL_DATA_FORMATS,
    DataFormat,
    DataFormatBase,
    UnknownFormat,
)
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.data_format.handler import get_handler
from dcp.storage.base import Storage, StorageClass, StorageEngine
from dcp.storage.database.utils import get_tmp_sqlite_db_url
from dcp.utils.pandas import assert_dataframes_are_almost_equal
from pandas.core.frame import DataFrame
from pandas.core.series import Series
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


@pytest.mark.parametrize("fmt,obj", test_data_format_objects)
def test_memory_handlers(fmt: DataFormat, obj: Any):
    s = Storage("python://test")
    name = "_test"
    s.get_api().put(name, obj())
    handler = get_handler(fmt, s.storage_engine)
    assert list(handler().infer_field_names(name, s)) == list(test_records[0].keys())
    assert handler().infer_field_type(name, s, "f1") == Text()
    assert handler().infer_field_type(name, s, "f2") == Integer()
    assert handler().infer_field_type(name, s, "f3") == DEFAULT_FIELD_TYPE
    assert handler().infer_field_type(name, s, "f4") == Date()
    assert handler().infer_field_type(name, s, "f5") == DEFAULT_FIELD_TYPE

    handler().cast_to_field_type(name, s, "f4", Text())
    handler().cast_to_field_type(name, s, "f4", Date())
    round_trip_object = s.get_api().get(name)
    assert_objects_equal(round_trip_object, obj())


def test_database_handler():
    dburl = get_tmp_sqlite_db_url()
    s = Storage(dburl)
    name = "_test"
    handler = get_handler(DatabaseTableFormat, s.storage_engine)
    handler().create_empty(name, s, test_records_schema)
    s.get_api().bulk_insert_records(name, test_records, test_records_schema)
    assert list(handler().infer_field_names(name, s)) == list(test_records[0].keys())
    assert handler().infer_field_type(name, s, "f1") == Text()
    assert handler().infer_field_type(name, s, "f2") == Integer()
    assert handler().infer_field_type(name, s, "f3") == DEFAULT_FIELD_TYPE
    assert handler().infer_field_type(name, s, "f4") == Date()
    assert handler().infer_field_type(name, s, "f5") == DEFAULT_FIELD_TYPE

    # TODO
    # handler().cast_to_field_type(name, s, "f4", Date())
    # handler().cast_to_field_type(name, s, "f4", Text())
    # round_trip_object = s.get_api().get(name)
    # assert_objects_equal(round_trip_object, obj())
