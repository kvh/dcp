from datacopy.data_format.formats.memory.arrow_table import ArrowTableFormat
from datacopy.data_format.formats.file_system.json_lines_file import JsonLinesFileFormat
import json
from datacopy.utils.common import DcpJsonEncoder
from datacopy.utils.data import read_csv, write_csv
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
from datacopy.storage.memory.memory_records_object import as_records
from datacopy.data_format.formats.memory.records import Records, RecordsFormat
from datacopy.storage.memory.engines.python import PythonStorageApi
from datacopy.data_copy.base import CopyRequest, datacopy

from pyarrow import json as pa_json


@datacopy(
    from_storage_classes=[FileSystemStorageClass],
    from_data_formats=[CsvFileFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[RecordsFormat],
    cost=DiskToMemoryCost + FormatConversionCost,
)
def copy_csv_file_to_records(req: CopyRequest):
    assert isinstance(req.from_storage_api, FileSystemStorageApi)
    assert isinstance(req.to_storage_api, PythonStorageApi)
    with req.from_storage_api.open(req.from_name) as f:
        records = list(read_csv(f.readlines()))
        req.to_storage_api.put(req.to_name, records)
        # This cast step is necessary because CSVs preserve no logical type information
        req.to_format_handler.cast_to_schema(
            req.to_name, req.to_storage_api.storage, req.schema
        )


# @datacopy(
#     from_storage_classes=[FileSystemStorageClass],
#     from_data_formats=[DelimitedFileFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[DelimitedFileObjectFormat],
#     cost=DiskToBufferCost,
# )
# def copy_delim_file_to_file_object(
#     from_name: str,
#     to_name: str,
#     conversion: Conversion,
#     from_storage_api: StorageApi,
#     to_storage_api: StorageApi,
#     schema: Schema,
# ):
#     assert isinstance(from_storage_api, FileSystemStorageApi)
#     assert isinstance(to_storage_api, PythonStorageApi)
#     with from_storage_api.open(from_name) as f:
#         mdr = as_records(f, data_format=DelimitedFileObjectFormat, schema=schema)
#         mdr = mdr.conform_to_schema()
#         to_storage_api.put(to_name, mdr)


@datacopy(
    from_storage_classes=[FileSystemStorageClass],
    from_data_formats=[JsonLinesFileFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[ArrowTableFormat],
    cost=DiskToMemoryCost,  # TODO: conversion cost might be minimal cuz in C?
)
def copy_json_file_to_arrow(req: CopyRequest):
    assert isinstance(req.from_storage_api, FileSystemStorageApi)
    assert isinstance(req.to_storage_api, PythonStorageApi)
    pth = req.from_storage_api.get_path(req.from_name)
    at = pa_json.read_json(pth)
    req.to_storage_api.put(req.to_name, at)
    # This cast step is necessary because JSON preserves no logical type information
    req.to_format_handler.cast_to_schema(
        req.to_name, req.to_storage_api.storage, req.schema
    )
