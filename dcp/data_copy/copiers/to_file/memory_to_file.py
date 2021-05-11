import json
from io import IOBase
from typing import Any, Iterator, TypeVar

import pandas as pd
from dcp.data_copy.base import CopyRequest, DataCopierBase
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

# from dcp.data_format.formats.memory.csv_lines_iterator import CsvLinesIteratorFormat


class MemoryToFileMixin:
    from_storage_classes = [MemoryStorageClass]
    to_storage_classes = [FileSystemStorageClass]

    def append(self, req: CopyRequest):
        assert isinstance(req.from_storage_api, PythonStorageApi)
        assert isinstance(req.to_storage_api, FileSystemStorageApi)
        records = req.from_storage_api.get(req.from_name)
        with req.to_storage_api.open(req.to_name, "a") as f:
            self.write_object(f, records)

    def write_object(self, f: IOBase, obj: Any):
        raise NotImplementedError


class RecordsToCsvFile(MemoryToFileMixin, DataCopierBase):
    from_data_formats = [RecordsFormat]
    to_data_formats = [CsvFileFormat]
    cost = DiskToMemoryCost + FormatConversionCost
    requires_schema_cast = False

    def write_object(self, f: IOBase, obj: Any):
        write_csv(obj, f, append=True)


# @datacopier(
#     from_storage_classes=[MemoryStorageClass],
#     from_data_formats=[CsvLinesIteratorFormat],
#     to_storage_classes=[FileSystemStorageClass],
#     to_data_formats=[CsvFileFormat],
#     cost=DiskToMemoryCost,
# )
# def copy_csv_lines_to_csv_file(req: CopyRequest):
#     assert isinstance(req.from_storage_api, PythonStorageApi)
#     assert isinstance(req.to_storage_api, FileSystemStorageApi)
#     csv_lines = req.from_storage_api.get(req.from_name)
#     create_empty_if_not_exists(req)
#     with req.to_storage_api.open(req.to_name, "a") as to_file:
#         try:
#             # Skip header, already written by `create_empty_...`
#             next(csv_lines)
#         except StopIteration:
#             return
#         to_file.writelines(csv_lines)


class RecordsToJsonLinesFile(MemoryToFileMixin, DataCopierBase):
    from_data_formats = [RecordsFormat]
    to_data_formats = [JsonLinesFileFormat]
    cost = DiskToMemoryCost
    requires_schema_cast = False

    def write_object(self, f: IOBase, obj: Records):
        for r in obj:
            s = json.dumps(r, cls=DcpJsonEncoder)
            f.write(s + "\n")
