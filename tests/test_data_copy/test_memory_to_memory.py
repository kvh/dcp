from __future__ import annotations
from tests.test_data_format import assert_objects_equal
from dcp.data_copy.graph import get_datacopy_lookup
from dcp.utils.pandas import assert_dataframes_are_almost_equal
from dcp.storage.base import LocalPythonStorageEngine
from dcp.data_copy.base import Conversion, CopyRequest, StorageFormat
from dcp.storage.memory.memory_records_object import as_records
from dcp.storage.memory.engines.python import PythonStorageApi, new_local_python_storage

import tempfile
import types
from io import StringIO
from itertools import product
from typing import Any, List, Optional, Tuple, Type

import pandas as pd

# import pyarrow as pa
import pytest
from tests.utils import rf, dff, test_records_schema


from_formats = [rf, dff]  # , af, dfif, rif, dlff]
to_formats = [rf, dff]  # , af]


@pytest.mark.parametrize(
    "from_fmt,to_fmt", product(from_formats, to_formats),
)
def test_mem_to_mem(from_fmt, to_fmt):
    from_fmt, obj = from_fmt
    to_fmt, expected = to_fmt
    if from_fmt == to_fmt:
        return
    s = new_local_python_storage()
    mem_api: PythonStorageApi = s.get_api()
    from_name = "_from_test"
    to_name = "_to_test"
    mem_api.put(from_name, as_records(obj(), data_format=from_fmt))
    req = CopyRequest(from_name, s, to_name, to_fmt, s, test_records_schema)
    pth = get_datacopy_lookup().get_lowest_cost_path(req.conversion)
    assert pth is not None
    for i, ce in enumerate(pth.edges):
        ce.copier.copy(req)
        from_name = to_name
        to_name = to_name + str(i)
    to_name = from_name
    assert_objects_equal(mem_api.get(to_name).records_object, expected())

