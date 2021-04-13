import json

from dcp.data_copy.base import CopyRequest, create_empty_if_not_exists, datacopier
from dcp.data_copy.costs import (
    DiskToMemoryCost,
    FormatConversionCost,
    MemoryToMemoryCost,
)
from dcp.data_format.formats.file_system.csv_file import CsvFileFormat
from dcp.data_format.formats.file_system.json_lines_file import JsonLinesFileFormat
from dcp.data_format.formats.memory.arrow_table import ArrowTable, ArrowTableFormat
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.storage.base import FileSystemStorageClass, MemoryStorageClass, StorageApi
from dcp.storage.file_system.engines.local import FileSystemStorageApi
from dcp.storage.memory.engines.python import PythonStorageApi
from dcp.utils.common import DcpJsonEncoder
from dcp.utils.data import read_csv, write_csv
from dcp.utils.pandas import dataframe_to_records
from pyarrow import Table
from pyarrow import json as pa_json


@datacopier(
    from_storage_classes=[FileSystemStorageClass],
    from_data_formats=[CsvFileFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[RecordsFormat],
    cost=DiskToMemoryCost + FormatConversionCost,
)
def copy_csv_file_to_records(req: CopyRequest):
    assert isinstance(req.from_storage_api, FileSystemStorageApi)
    assert isinstance(req.to_storage_api, PythonStorageApi)
    create_empty_if_not_exists(req)
    existing_records = req.to_storage_api.get(req.to_name)
    with req.from_storage_api.open(req.from_name) as f:
        records = list(read_csv(f.readlines()))
        req.to_storage_api.put(req.to_name, existing_records + records)
        # This cast step is necessary because CSVs preserve no logical type information
        req.to_format_handler.cast_to_schema(
            req.to_name, req.to_storage_api.storage, req.get_schema()
        )


# @datacopier(
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


@datacopier(
    from_storage_classes=[FileSystemStorageClass],
    from_data_formats=[JsonLinesFileFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[ArrowTableFormat],
    cost=DiskToMemoryCost,  # TODO: conversion cost might be minimal cuz in C?
)
def copy_json_file_to_arrow(req: CopyRequest):
    assert isinstance(req.from_storage_api, FileSystemStorageApi)
    assert isinstance(req.to_storage_api, PythonStorageApi)
    create_empty_if_not_exists(req)
    pth = req.from_storage_api.get_path(req.from_name)
    at = pa_json.read_json(pth)
    # TODO: this will almost always break as the "read" json schema will differ from existing
    #       may want to use ParseOptions(explicit_schema=)?
    existing_table: Table = req.to_storage_api.get(req.to_name)
    new_table = Table.from_batches(existing_table.to_batches() + at.to_batches())
    req.to_storage_api.put(req.to_name, new_table)
    # This cast step is necessary because JSON preserves no logical type information
    req.to_format_handler.cast_to_schema(
        req.to_name, req.to_storage_api.storage, req.get_schema()
    )
