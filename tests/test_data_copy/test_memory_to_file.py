from __future__ import annotations

import tempfile
import warnings
from typing import Type

import pytest
from dcp.data_copy.base import Conversion, CopyRequest, StorageFormat
from dcp.data_copy.copiers.to_file.memory_to_file import RecordsToCsvFile
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.data_format.formats.file_system.csv_file import CsvFileFormat
from dcp.data_format.formats.memory.records import RecordsFormat
from dcp.data_format.handler import get_handler, get_handler_for_name
from dcp.storage.base import DatabaseStorageClass, LocalPythonStorageEngine, Storage
from dcp.storage.database.api import DatabaseApi, DatabaseStorageApi
from dcp.storage.file_system.engines.local import FileSystemStorageApi
from dcp.storage.memory.engines.python import PythonStorageApi, new_local_python_storage
from dcp.utils.common import rand_str
from dcp.utils.data import read_csv
from tests.utils import conformed_test_records, test_records_schema


def test_records_to_file():
    dr = tempfile.gettempdir()
    s: Storage = Storage.from_url(f"file://{dr}")
    fs_api: FileSystemStorageApi = s.get_api()
    mem_s = new_local_python_storage()
    mem_api: PythonStorageApi = mem_s.get_api()
    name = f"_test_{rand_str()}"
    obj = [{"f1": "hi", "f2": 2}]
    mem_api.put(name, obj)
    req = CopyRequest(name, mem_s, name, s, CsvFileFormat)
    RecordsToCsvFile().copy(req)
    with fs_api.open(name, newline="") as f:
        recs = list(read_csv(f))
        handler = get_handler(RecordsFormat, mem_s.storage_engine)
        mem_api.put(
            "output",
            recs,
        )
        handler().cast_to_schema("output", mem_s, schema=test_records_schema)
        recs = mem_api.get("output")
        assert recs == obj


# def test_obj_to_file():
#     dr = tempfile.gettempdir()
#     s: Storage = Storage.from_url(f"file://{dr}")
#     fs_api: FileSystemStorageApi = s.get_api()
#     mem_api: PythonStorageApi = new_local_python_storage().get_api()
#     name = "_test"
#     fmt = DelimitedFileObjectFormat
#     obj = (lambda: StringIO("f1,f2\nhi,2"),)[0]
#     mdr = as_records(obj(), data_format=fmt)
#     mem_api.put(name, mdr)
#     conversion = Conversion(
#         StorageFormat(LocalPythonStorageEngine, fmt),
#         StorageFormat(s.storage_engine, DelimitedFileFormat),
#     )
#     copy_file_object_to_delim_file.copy(
#         name, name, conversion, mem_api, fs_api, schema=TestSchema4
#     )
#     with fs_api.open(name) as f:
#         assert f.read() == obj().read()


# def test_records_to_json():
#     dr = tempfile.gettempdir()
#     s: Storage = Storage.from_url(f"file://{dr}")
#     fs_api: FileSystemStorageApi = s.get_api()
#     mem_api: PythonStorageApi = new_local_python_storage().get_api()
#     name = "_test"
#     fmt = RecordsFormat
#     obj = [{"f1": "hi", "f2": 2}]
#     mdr = as_records(obj, data_format=fmt)
#     mem_api.put(name, mdr)
#     conversion = Conversion(
#         StorageFormat(LocalPythonStorageEngine, fmt),
#         StorageFormat(s.storage_engine, JsonLinesFileFormat),
#     )
#     copy_records_to_json_file.copy(
#         name, name, conversion, mem_api, fs_api, schema=TestSchema4
#     )
#     with fs_api.open(name) as f:
#         f.seek(0)
#         recs = [json.loads(ln) for ln in f.readlines()]
#         recs = RecordsFormat.conform_records_to_schema(recs, TestSchema4)
#         assert recs == obj
