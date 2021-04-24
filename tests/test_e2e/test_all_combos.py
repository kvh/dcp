import tempfile
from contextlib import contextmanager
from itertools import product
from typing import Iterator, Type
from dcp.data_format.formats.memory.csv_file_object import CsvFileObjectFormat

import pytest
from dcp.data_copy.base import CopyRequest, NameExistsError
from dcp.data_copy.graph import execute_copy_request, get_copy_path
from dcp.data_format.base import DataFormat
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.data_format.formats.file_system.csv_file import CsvFileFormat
from dcp.data_format.formats.file_system.json_lines_file import JsonLinesFileFormat
from dcp.data_format.formats.memory.arrow_table import ArrowTableFormat
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.data_format.formats.memory.records import RecordsFormat
from dcp.data_format.handler import get_handler, infer_format_for_name
from dcp.storage.base import (
    DatabaseStorageClass,
    FileSystemStorageClass,
    MemoryStorageClass,
    Storage,
)
from dcp.storage.database.api import DatabaseApi
from dcp.utils.common import rand_str, to_json
from dcp.utils.data import write_csv

from ..utils import get_test_records_for_format, test_records, test_records_schema

dr = tempfile.gettempdir()
python_url = f"python://{rand_str(10)}/"

all_storage_formats = [
    ["sqlite://", DatabaseTableFormat],
    ["postgresql://localhost", DatabaseTableFormat],
    ["mysql://", DatabaseTableFormat],
    [f"file://{dr}", CsvFileFormat],
    [f"file://{dr}", JsonLinesFileFormat],
    [python_url, RecordsFormat],
    [python_url, DataFrameFormat],
    [python_url, ArrowTableFormat],
    [python_url, CsvFileObjectFormat],
]


def put_records_in_storage_format(
    name: str, storage: Storage, fmt: DataFormat, records=test_records
):
    if storage.storage_engine.storage_class == MemoryStorageClass:
        obj = get_test_records_for_format(fmt)()
        storage.get_api().put(name, obj)
    elif storage.storage_engine.storage_class == FileSystemStorageClass:
        if fmt == CsvFileFormat:
            with storage.get_api().open(name, "w") as f:
                write_csv(records, f)
        if fmt == JsonLinesFileFormat:
            storage.get_api().write_lines_to_file(name, (to_json(r) for r in records))
    elif storage.storage_engine.storage_class == DatabaseStorageClass:
        get_handler(fmt, storage.storage_engine)().create_empty(
            name, storage, test_records_schema
        )
        storage.get_api().bulk_insert_records(name, records)
    else:
        raise NotImplementedError


@contextmanager
def make_storage(url: str) -> Iterator[Storage]:
    storage = Storage(url)
    if storage.storage_engine.storage_class == DatabaseStorageClass:
        with make_database_storage(storage) as s:
            yield s
        return
    yield storage


@contextmanager
def make_database_storage(storage: Storage) -> Iterator[Storage]:
    api_cls: Type[DatabaseApi] = storage.storage_engine.get_api_cls()
    with api_cls.temp_local_database() as db_url:
        yield Storage(db_url)


existence_options = ["error", "append", "replace"]


@pytest.mark.parametrize(
    "from_storage_fmt,to_storage_fmt,if_exists",
    product(all_storage_formats, all_storage_formats, existence_options),
)
def test_copy(from_storage_fmt, to_storage_fmt, if_exists):
    if from_storage_fmt == to_storage_fmt:
        return
    with make_storage(from_storage_fmt[0]) as from_storage:
        with make_storage(to_storage_fmt[0]) as to_storage:
            from_name = "test_" + rand_str().lower()
            to_name = "test_" + rand_str().lower()
            put_records_in_storage_format(from_name, from_storage, from_storage_fmt[1])
            assert infer_format_for_name(from_name, from_storage) == from_storage_fmt[1]
            req = CopyRequest(
                from_name,
                from_storage,
                to_name,
                to_storage,
                to_format=to_storage_fmt[1],
                available_storages=[Storage(python_url)],
                if_exists=if_exists,
            )
            try:
                pth = get_copy_path(req)
                assert pth is not None
                assert 1 <= len(pth.edges) <= 4  # Bring this 4 down!
                execute_copy_request(req)
            except NotImplementedError:
                return
            if if_exists == "error":
                with pytest.raises(NameExistsError):
                    execute_copy_request(req)
            elif if_exists == "append":
                execute_copy_request(req)
            elif if_exists == "replace":
                execute_copy_request(req)
