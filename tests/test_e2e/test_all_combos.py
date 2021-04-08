from contextlib import contextmanager
from datacopy.data_format.handler import get_handler
import tempfile
from datacopy.utils.data import write_csv
from datacopy.data_format.base import DataFormat
from datacopy.storage.database.api import DatabaseApi
from typing import Iterator, Type
from datacopy.storage.base import (
    DatabaseStorageClass,
    FileSystemStorageClass,
    MemoryStorageClass,
    Storage,
)
from datacopy.data_copy.base import CopyRequest
from datacopy.data_copy.graph import execute_copy_request
from itertools import product
import pytest

from contextlib import contextmanager

from datacopy.data_format.formats.memory.arrow_table import ArrowTableFormat
from datacopy.data_format.formats.memory.dataframe import DataFrameFormat
from datacopy.data_format.formats.memory.records import RecordsFormat
from datacopy.data_format.formats.file_system.json_lines_file import JsonLinesFileFormat
from datacopy.data_format.formats.file_system.csv_file import CsvFileFormat
from datacopy.data_format.formats.database.base import DatabaseTableFormat
from datacopy.utils.common import rand_str, to_json

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


@pytest.mark.parametrize(
    "from_storage_fmt,to_storage_fmt",
    product(all_storage_formats, all_storage_formats),
)
def test_copy(from_storage_fmt, to_storage_fmt):
    if from_storage_fmt == to_storage_fmt:
        return
    with make_storage(from_storage_fmt[0]) as from_storage:
        with make_storage(to_storage_fmt[0]) as to_storage:
            from_name = "test_" + rand_str().lower()
            to_name = "test_" + rand_str().lower()
            put_records_in_storage_format(from_name, from_storage, from_storage_fmt[1])
            req = CopyRequest(
                from_name,
                from_storage,
                to_name,
                to_storage_fmt[1],
                to_storage,
                available_storages=[Storage(python_url)],
            )
            execute_copy_request(req)
