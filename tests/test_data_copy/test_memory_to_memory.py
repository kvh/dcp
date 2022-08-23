from __future__ import annotations

from itertools import product

import pytest

from dcp import ensure_storage_object
from dcp.data_copy.base import CopyRequest
from dcp.data_copy.graph import get_datacopy_lookup
from dcp.storage.memory.engines.python import PythonStorageApi, new_local_python_storage
from tests.test_data_format import assert_objects_equal
from tests.utils import dff, rf, rif, dfif, test_records_schema

from_formats = [rf, dff]  # , af, dfif, rif, dlff]
to_formats = [rf, dff]  # , af]


@pytest.mark.parametrize(
    "from_fmt,to_fmt",
    product(from_formats, to_formats),
)
def test_mem_to_mem(from_fmt, to_fmt):
    from_fmt, obj = from_fmt
    to_fmt, expected = to_fmt
    if from_fmt == to_fmt:
        return
    s = new_local_python_storage()
    mem_api: PythonStorageApi = s.get_memory_api()
    from_name = "_from_test"
    to_name = "_to_test"
    mem_api.put(from_name, obj())
    from_so = ensure_storage_object(from_name, storage=s)
    to_so = ensure_storage_object(
        to_name, storage=s, _data_format=to_fmt, _schema=test_records_schema
    )
    req = CopyRequest(from_so, to_so)
    pth = get_datacopy_lookup().get_lowest_cost_path(req.conversion)
    assert pth is not None
    for i, ce in enumerate(pth.edges):
        ce.copier.copy(req)
        from_name = to_name
        to_name = to_name + str(i)
    to_name = from_name
    assert_objects_equal(mem_api.get(to_name), expected())
