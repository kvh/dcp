from datacopy.data_format.formats.file_system.json_lines_file import JsonLinesFileFormat
import json
from datacopy.utils.common import DcpJsonEncoder
from datacopy.utils.data import write_csv
from datacopy.data_format.formats.file_system.csv_file import CsvFileFormat
from datacopy.storage.file_system.engines.local import FileSystemStorageApi
from datacopy.utils.pandas import dataframe_to_records
from datacopy.storage.base import FileSystemStorageClass, MemoryStorageClass, StorageApi
from datacopy.data_copy.costs import (
    DiskToMemoryCost,
    FormatConversionCost,
    MemoryToMemoryCost,
)
from datacopy.data_format.formats.memory.dataframe import DataFrameFormat
from datacopy.data_format.formats.memory.records import Records, RecordsFormat
from datacopy.storage.memory.engines.python import PythonStorageApi
from datacopy.data_copy.base import CopyRequest, datacopy
from typing import Iterator, TypeVar

import pandas as pd


@datacopy(
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
    with req.to_storage_api.open(req.to_name, "w") as f:
        append = False
        for records in records_iterator:
            write_csv(records, f, append=append)
            append = True


# @datacopy(
#     from_storage_classes=[MemoryStorageClass],
#     from_data_formats=[DelimitedFileObjectFormat, DelimitedFileObjectIteratorFormat],
#     to_storage_classes=[FileSystemStorageClass],
#     to_data_formats=[DelimitedFileFormat],
#     cost=DiskToMemoryCost,
# )
# def copy_file_object_to_delim_file(
#     req: CopyRequest
# ):
#     assert isinstance(req.from_storage_api, PythonStorageApi)
#     assert isinstance(req.to_storage_api, FileSystemStorageApi)
#     records_object = req.from_storage_api.get(req.from_name)
#     file_obj_iterator = records_object
#     if isinstance(records_object, IOBase):
#         file_obj_iterator = [file_obj_iterator]
#     with req.to_storage_api.open(req.to_name, "w") as to_file:
#         for file_obj in file_obj_iterator:
#             to_file.write(file_obj)


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[RecordsFormat],  # , RecordsIteratorFormat],
    to_storage_classes=[FileSystemStorageClass],
    to_data_formats=[JsonLinesFileFormat],
    cost=DiskToMemoryCost,  # TODO: not much conversion cost, but some?
)
def copy_records_to_json_file(req: CopyRequest):
    assert isinstance(req.from_storage_api, PythonStorageApi)
    assert isinstance(req.to_storage_api, FileSystemStorageApi)
    records_object = req.from_storage_api.get(req.from_name)
    records_iterator = records_object
    if not isinstance(records_object, Iterator):
        records_iterator = [records_iterator]
    with req.to_storage_api.open(req.to_name, "w") as f:
        for records in records_iterator:
            for r in records:
                json.dump(r, f, cls=DcpJsonEncoder)
