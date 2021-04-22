import json
from typing import Iterator, TypeVar
from dcp.data_format.formats.memory.csv_file_object import CsvFileObjectFormat

import pandas as pd
from dcp.data_copy.base import CopyRequest, create_empty_if_not_exists, datacopier
from dcp.data_copy.costs import (
    DiskToMemoryCost,
    FormatConversionCost,
    MemoryToMemoryCost,
)
from dcp.data_format.formats.file_system.csv_file import CsvFileFormat
from dcp.data_format.formats.file_system.json_lines_file import JsonLinesFileFormat
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.storage.base import FileSystemStorageClass, MemoryStorageClass, StorageApi
from dcp.storage.file_system.engines.local import FileSystemStorageApi
from dcp.storage.memory.engines.python import PythonStorageApi
from dcp.utils.common import DcpJsonEncoder
from dcp.utils.data import write_csv
from dcp.utils.pandas import dataframe_to_records


@datacopier(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[RecordsFormat],  # , RecordsIteratorFormat],
    to_storage_classes=[FileSystemStorageClass],
    to_data_formats=[CsvFileFormat],
    cost=DiskToMemoryCost + FormatConversionCost,
)
def copy_records_to_csv_file(req: CopyRequest):
    assert isinstance(req.from_storage_api, PythonStorageApi)
    assert isinstance(req.to_storage_api, FileSystemStorageApi)
    records_object = req.from_storage_api.get(req.from_name)
    records_iterator = records_object
    if not isinstance(records_object, Iterator):
        records_iterator = [records_iterator]
    create_empty_if_not_exists(req)
    with req.to_storage_api.open(req.to_name, "a") as f:
        for records in records_iterator:
            write_csv(records, f, append=True)  # Append because we created empty


@datacopier(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[CsvFileObjectFormat],
    to_storage_classes=[FileSystemStorageClass],
    to_data_formats=[CsvFileFormat],
    cost=DiskToMemoryCost,
)
def copy_csv_file_object_to_csv_file(req: CopyRequest):
    assert isinstance(req.from_storage_api, PythonStorageApi)
    assert isinstance(req.to_storage_api, FileSystemStorageApi)
    file_obj = req.from_storage_api.get(req.from_name)
    create_empty_if_not_exists(req)
    with req.to_storage_api.open(req.to_name, "a") as to_file:
        try:
            # Skip header, already written by `create_empty_...`
            next(file_obj)
        except StopIteration:
            return
        to_file.writelines((ln for ln in file_obj))


@datacopier(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[RecordsFormat],  # , RecordsIteratorFormat],
    to_storage_classes=[FileSystemStorageClass],
    to_data_formats=[JsonLinesFileFormat],
    cost=DiskToMemoryCost,  # TODO: not much format conversion cost, but some?
)
def copy_records_to_json_file(req: CopyRequest):
    assert isinstance(req.from_storage_api, PythonStorageApi)
    assert isinstance(req.to_storage_api, FileSystemStorageApi)
    records = req.from_storage_api.get(req.from_name)
    create_empty_if_not_exists(req)
    with req.to_storage_api.open(req.to_name, "a") as f:
        for r in records:
            s = json.dumps(r, cls=DcpJsonEncoder)
            f.write(s + "\n")
