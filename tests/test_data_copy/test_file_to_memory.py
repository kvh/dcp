from __future__ import annotations

import tempfile

import pyarrow as pa

from dcp.data_copy.base import CopyRequest
from dcp.data_copy.copiers.to_memory.file_to_memory import (
    CsvFileToRecords,
    JsonLinesFileToArrowTable,
)
from dcp.data_format.formats.memory.arrow_table import ArrowTableFormat
from dcp.data_format.formats.memory.records import RecordsFormat
from dcp.storage.base import (
    Storage,
    ensure_storage_object,
)
from dcp.storage.memory.engines.python import new_local_python_storage
from tests.utils import test_records_schema


def test_file_to_mem():
    dr = tempfile.gettempdir()
    s: Storage = Storage.from_url(f"file://{dr}")
    fs_api = s.get_filesystem_api()
    mem_s = new_local_python_storage()
    mem_api = mem_s.get_memory_api()
    name = "_test"
    fs_api.write_lines_to_file(name, ["f1,f2", "hi,2"])
    # Records
    records_obj = [{"f1": "hi", "f2": 2}]
    from_so = ensure_storage_object(name, storage=s)
    to_so = ensure_storage_object(
        name, storage=mem_s, _data_format=RecordsFormat, _schema=test_records_schema
    )
    req = CopyRequest(from_so, to_so)
    CsvFileToRecords().copy(req)
    assert mem_api.get(name) == records_obj

    # # Json lines
    name = "_json_test"
    fs_api.write_lines_to_file(name, ['{"f1":"hi","f2":2}'])
    from_so = ensure_storage_object(name, storage=s)
    to_so = ensure_storage_object(
        name, storage=mem_s, _data_format=ArrowTableFormat, _schema=test_records_schema
    )
    req = CopyRequest(from_so, to_so)
    JsonLinesFileToArrowTable().copy(req)
    expected = pa.Table.from_pydict({"f1": ["hi"], "f2": [2]})
    assert mem_api.get(name) == expected
