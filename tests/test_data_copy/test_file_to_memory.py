from __future__ import annotations
from datacopy.data_format.formats.memory.arrow_table import ArrowTableFormat
from datacopy.data_copy.copiers.to_memory.file_to_memory import (
    copy_csv_file_to_records,
    copy_json_file_to_arrow,
)
from datacopy.data_format.handler import get_handler, get_handler_for_name
from datacopy.utils.data import read_csv
from datacopy.data_format.formats.file_system.csv_file import CsvFileFormat
from datacopy.data_copy.copiers.to_file.memory_to_file import copy_records_to_csv_file
from datacopy.data_format.formats.memory.records import RecordsFormat
from datacopy.storage.file_system.engines.local import FileSystemStorageApi
import tempfile
from datacopy.data_format.formats.database.base import DatabaseTableFormat
from datacopy.data_copy.copiers.to_database.memory_to_database import copy_records_to_db
import warnings
from datacopy.storage.database.api import DatabaseApi, DatabaseStorageApi
from datacopy.storage.base import (
    DatabaseStorageClass,
    LocalPythonStorageEngine,
    Storage,
)
from datacopy.data_copy.base import Conversion, CopyRequest, StorageFormat
from datacopy.storage.memory.engines.python import (
    PythonStorageApi,
    new_local_python_storage,
)

from typing import Type

import pytest
from tests.utils import (
    test_records_schema,
    conformed_test_records,
)

import pyarrow as pa


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
    req = CopyRequest(name, s, name, RecordsFormat, mem_s, test_records_schema)
    copy_csv_file_to_records.copy(req)
    assert mem_api.get(name) == records_obj

    # # Json lines
    name = "_json_test"
    fs_api.write_lines_to_file(name, ['{"f1":"hi","f2":2}'])
    req = CopyRequest(name, s, name, ArrowTableFormat, mem_s, test_records_schema)
    copy_json_file_to_arrow.copy(req)
    expected = pa.Table.from_pydict({"f1": ["hi"], "f2": [2]})
    assert mem_api.get(name) == expected
