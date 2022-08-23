from io import IOBase

from dcp.data_copy.base import CopyRequest, DataCopierBase
from dcp.data_copy.costs import (
    DiskToMemoryCost,
    FormatConversionCost,
)
from dcp.data_format.formats.file_system.csv_file import CsvFileFormat
from dcp.data_format.formats.file_system.json_lines_file import JsonLinesFileFormat
from dcp.data_format.formats.memory.arrow_table import ArrowTable, ArrowTableFormat
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.storage.base import FileSystemStorageClass, MemoryStorageClass
from dcp.utils.data import read_csv

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
        existing = req.to_obj.storage.get_memory_api().get(
            req.to_obj.formatted_full_name
        )
        with req.from_obj.storage.get_filesystem_api().open(
            req.from_obj.formatted_full_name
        ) as f:
            new = self.read_to_object(f)
        final = self.concat(existing, new)
        req.to_obj.storage.get_memory_api().put(req.to_obj.formatted_full_name, final)

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
