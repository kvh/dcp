from __future__ import annotations

from dataclasses import asdict

from pandas.core.series import Series
from dcp.utils.pandas import assert_dataframes_are_almost_equal
from pandas.core.frame import DataFrame

from schemas.field_types import DEFAULT_FIELD_TYPE, Date, DateTime, Integer, Text
from dcp.data_format.handler import get_handler
from dcp.storage.memory.memory_records_object import as_records
from dcp import data_format
from io import StringIO
from typing import Any, Callable

from dcp.data_format.base import ALL_DATA_FORMATS, DataFormat, DataFormatBase
from dcp.storage.base import Storage, StorageClass, StorageEngine
import pytest
from tests.utils import test_data_format_objects, test_records


def test_formats():
    for fmt in ALL_DATA_FORMATS:
        assert issubclass(fmt, DataFormatBase)
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
def test_memory_handler(fmt: DataFormat, obj: Any):
    s = Storage("python://test")
    name = "_test"
    s.get_api().put(name, as_records(obj(), data_format=fmt))
    handler = get_handler(fmt, s.storage_engine)
    assert list(handler().infer_field_names("_test", s)) == list(test_records[0].keys())
    assert handler().infer_field_type("_test", s, "f1") == Text()
    assert handler().infer_field_type("_test", s, "f2") == Integer()
    assert handler().infer_field_type("_test", s, "f3") == DEFAULT_FIELD_TYPE
    assert handler().infer_field_type("_test", s, "f4") == Date()
    assert handler().infer_field_type("_test", s, "f5") == DEFAULT_FIELD_TYPE

    handler().cast_to_field_type(name, s, "f4", Date())
    handler().cast_to_field_type(name, s, "f4", Text())
    round_trip_object = s.get_api().get(name).records_object
    assert_objects_equal(round_trip_object, obj())
