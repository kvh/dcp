from dcp.data_format.formats.file_system.json_lines_file import JsonLinesFileFormat
import json
from dcp.utils.common import DcpJsonEncoder
from dcp.utils.data import read_csv, write_csv
from dcp.data_format.formats.file_system.csv_file import CsvFileFormat
from dcp.storage.file_system.engines.local import FileSystemStorageApi
from dcp.utils.pandas import dataframe_to_records
from dcp.storage.base import FileSystemStorageClass, MemoryStorageClass, StorageApi
from dcp.data_copy.costs import (
    DiskToMemoryCost,
    FormatConversionCost,
    MemoryToMemoryCost,
)
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.storage.memory.memory_records_object import as_records
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.storage.memory.engines.python import PythonStorageApi
from dcp.data_copy.base import CopyRequest, datacopy

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
        mdr = as_records(records, data_format=RecordsFormat, schema=req.schema)
        req.to_storage_api.put(req.to_name, mdr)
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


# @datacopy(
#     from_storage_classes=[FileSystemStorageClass],
#     from_data_formats=[JsonLinesFileFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[ArrowTableFormat],
#     cost=DiskToMemoryCost,  # TODO: conversion cost might be minimal cuz in C?
# )
# def copy_json_file_to_arrow(
#     from_name: str,
#     to_name: str,
#     conversion: Conversion,
#     from_storage_api: StorageApi,
#     to_storage_api: StorageApi,
#     schema: Schema,
# ):
#     assert isinstance(from_storage_api, FileSystemStorageApi)
#     assert isinstance(to_storage_api, PythonStorageApi)
#     pth = from_storage_api.get_path(from_name)
#     at = pa_json.read_json(pth)
#     mdr = as_records(at, data_format=ArrowTableFormat, schema=schema)
#     mdr = mdr.conform_to_schema()
#     to_storage_api.put(to_name, mdr)
