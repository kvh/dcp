from __future__ import annotations

import tempfile
import warnings
from typing import Type

import pyarrow as pa
import pytest
from dcp.data_copy.base import Conversion, CopyRequest, StorageFormat
from dcp.data_copy.copiers.to_memory.file_to_memory import (
    CsvFileToRecords,
    JsonLinesFileToArrowTable,
)
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.data_format.formats.file_system.csv_file import CsvFileFormat
from dcp.data_format.formats.memory.arrow_table import ArrowTableFormat
from dcp.data_format.formats.memory.records import RecordsFormat
from dcp.data_format.handler import get_handler, get_handler_for_name
from dcp.storage.base import DatabaseStorageClass, LocalPythonStorageEngine, Storage
from dcp.storage.database.api import DatabaseApi, DatabaseStorageApi
from dcp.storage.file_system.engines.local import FileSystemStorageApi
from dcp.storage.memory.engines.python import PythonStorageApi, new_local_python_storage
from dcp.utils.data import read_csv
from tests.utils import conformed_test_records, test_records_schema


def test_file_to_mem():
    dr = tempfile.gettempdir()
    s: Storage = Storage.from_url(f"file://{dr}")
    fs_api: FileSystemStorageApi = s.get_api()
    mem_s = new_local_python_storage()
    mem_api: PythonStorageApi = mem_s.get_api()
    name = "_test"
    fs_api.write_lines_to_file(name, ["f1,f2", "hi,2"])
    # Records
    records_obj = [{"f1": "hi", "f2": 2}]
    req = CopyRequest(name, s, name, mem_s, RecordsFormat, test_records_schema)
    CsvFileToRecords().copy(req)
    assert mem_api.get(name) == records_obj

    # # Json lines
    name = "_json_test"
    fs_api.write_lines_to_file(name, ['{"f1":"hi","f2":2}'])
    req = CopyRequest(name, s, name, mem_s, ArrowTableFormat, test_records_schema)
    JsonLinesFileToArrowTable().copy(req)
    expected = pa.Table.from_pydict({"f1": ["hi"], "f2": [2]})
    assert mem_api.get(name) == expected
