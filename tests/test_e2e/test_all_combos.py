import os
import tempfile
from contextlib import contextmanager
from itertools import product
from typing import Iterator, Type

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
from dcp.data_format.handler import get_handler, infer_format
from dcp.storage.base import (
    DatabaseStorageClass,
    FileSystemStorageClass,
    MemoryStorageClass,
    Storage,
    StorageObject,
    ensure_storage_object,
)
from dcp.storage.database.api import DatabaseApi
from dcp.storage.database.engines.bigquery import BIGQUERY_SUPPORTED
from dcp.storage.file_system.engines.gcs import GOOGLE_CLOUD_STORAGE_SUPPORTED
from dcp.utils.common import rand_str, to_json
from dcp.utils.data import write_csv
from loguru import logger

from ..utils import (
    get_test_records_for_format,
    test_records,
    test_records_schema,
    gs_test_bucket,
    bigquery_url,
)

# from dcp.data_format.formats.memory.csv_lines_iterator import CsvLinesIteratorFormat


dr = tempfile.gettempdir()
python_url = f"python://{rand_str(10)}/"

all_storage_formats = [
    ["sqlite://", DatabaseTableFormat],
    ["postgresql://localhost", DatabaseTableFormat],
    ["mysql://root@localhost", DatabaseTableFormat],
    # ["bigquery://kvh-playground", DatabaseTableFormat],
    [f"file://{dr}", CsvFileFormat],
    [f"file://{dr}", JsonLinesFileFormat],
    [python_url, RecordsFormat],
    [python_url, DataFrameFormat],
    [python_url, ArrowTableFormat],
]

if BIGQUERY_SUPPORTED and bigquery_url:
    all_storage_formats.append([bigquery_url, DatabaseTableFormat])
else:
    logger.warning(
        "Google BigQuery skipped (provide DCP_BIGQUERY_TEST_URL"
        " and GOOGLE_APPLICATION_CREDENTIALS env vars)"
    )

if GOOGLE_CLOUD_STORAGE_SUPPORTED and gs_test_bucket:
    all_storage_formats += [
        [f"gs://{gs_test_bucket}", CsvFileFormat],
    ]
else:
    logger.warning(
        "Google Cloud Storage skipped (provide DCP_GS_TEST_BUCKET"
        " and GOOGLE_APPLICATION_CREDENTIALS env vars)"
    )


def put_records_in_storage_format(
    so: StorageObject, fmt: DataFormat, records=test_records
):
    if so.storage.storage_engine.storage_class == MemoryStorageClass:
        obj = get_test_records_for_format(fmt)()
        so.storage.get_memory_api().put(so, obj)
    elif so.storage.storage_engine.storage_class == FileSystemStorageClass:
        if fmt == CsvFileFormat:
            with so.storage.get_filesystem_api().open(so.formatted_full_name, "w") as f:
                write_csv(records, f)
        if fmt == JsonLinesFileFormat:
            so.storage.get_filesystem_api().write_lines_to_file(
                so.formatted_full_name, (to_json(r) for r in records)
            )
    elif so.storage.storage_engine.storage_class == DatabaseStorageClass:
        get_handler(fmt, so.storage.storage_engine)().create_empty(
            so, test_records_schema
        )
        so.storage.get_database_api().bulk_insert_records(
            so.formatted_full_name, records, test_records_schema
        )
    else:
        raise NotImplementedError


@contextmanager
def make_storage(url: str) -> Iterator[Storage]:
    storage = Storage(url)
    if storage.storage_engine.storage_class == DatabaseStorageClass:
        with make_database_storage(storage) as s:
            yield s
    else:
        yield storage


@contextmanager
def make_database_storage(storage: Storage) -> Iterator[Storage]:
    api_cls: Type[DatabaseApi] = storage.storage_engine.get_api_cls()
    with api_cls.temp_local_database(storage.url) as db_url:
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
            from_so = ensure_storage_object(from_name, storage=from_storage)
            to_so = ensure_storage_object(
                to_name, storage=to_storage, _data_format=to_storage_fmt[1]
            )
            put_records_in_storage_format(from_so, from_storage_fmt[1])
            assert infer_format(from_so) == from_storage_fmt[1]
            req = CopyRequest(
                from_so,
                to_so,
                available_storages=[Storage(python_url)],
                if_exists=if_exists,
            )
            try:
                pth = get_copy_path(req)
                assert pth is not None
                assert 1 <= len(pth.edges) <= 4  # Bring this 4 down!
                execute_copy_request(req)
            except NotImplementedError:
                pytest.skip(f"Skipping {from_storage} to {to_storage}, not implemented")
                return
            if if_exists == "error":
                with pytest.raises(NameExistsError):
                    execute_copy_request(req)
            elif if_exists == "append":
                execute_copy_request(req)
            elif if_exists == "replace":
                execute_copy_request(req)
            else:
                raise
