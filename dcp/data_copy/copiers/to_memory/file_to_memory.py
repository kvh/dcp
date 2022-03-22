from io import IOBase

import pandas as pd
from dcp.data_copy.base import CopyRequest, DataCopierBase
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

try:
    from pyarrow import Table
    from pyarrow import json as pa_json

    PYARROW_SUPPORTED = True
except ImportError:
    PYARROW_SUPPORTED = False
    Table = None
    pa_json = None


class FileToMemoryMixin:
    from_storage_classes = [FileSystemStorageClass]
    to_storage_classes = [MemoryStorageClass]

    def append(self, req: CopyRequest):
        assert isinstance(req.from_storage_api, FileSystemStorageApi)
        assert isinstance(req.to_storage_api, PythonStorageApi)
        existing = req.to_storage_api.get(req.to_name)
        with req.from_storage_api.open(req.from_name) as f:
            new = self.read_to_object(f)
        final = self.concat(existing, new)
        req.to_storage_api.put(req.to_name, final)

    def concat(self, existing, new):
        raise NotImplementedError

    def read_to_object(self, req: CopyRequest):
        raise NotImplementedError


class CsvFileToRecords(FileToMemoryMixin, DataCopierBase):
    from_data_formats = [CsvFileFormat]
    to_data_formats = [RecordsFormat]
    cost = DiskToMemoryCost + FormatConversionCost
    requires_schema_cast = True

    def concat(self, existing: Records, new: Records) -> Records:
        return existing + new

    def read_to_object(self, f: IOBase):
        records = list(read_csv(f.readlines()))
        return records


class JsonLinesFileToArrowTable(FileToMemoryMixin, DataCopierBase):
    from_data_formats = [JsonLinesFileFormat]
    to_data_formats = [ArrowTableFormat]
    cost = DiskToMemoryCost + FormatConversionCost
    requires_schema_cast = True

    def concat(self, existing: ArrowTable, new: ArrowTable) -> ArrowTable:
        if not PYARROW_SUPPORTED:
            raise ImportError("Pyarrow is not installed")
        return Table.from_batches(existing.to_batches() + new.to_batches())

    def read_to_object(self, f: IOBase):
        at = pa_json.read_json(f.name)
        return at
