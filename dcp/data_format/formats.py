from __future__ import annotations
from io import IOBase
from typing import Any, Dict, List, TypeVar


from dcp.storage.base import (
    DatabaseStorageClass,
    FileSystemStorageClass,
    LocalPythonStorageEngine,
    MemoryStorageClass,
)
from dcp.data_format.base import DataFormatBase

Records = List[Dict[str, Any]]


class RecordsFormat(DataFormatBase[Records]):
    natural_storage_class = MemoryStorageClass
    nickname = "records"


try:
    from pandas import DataFrame
except ImportError:
    DataFrame = TypeVar("DataFrame")


class DataFrameFormat(DataFormatBase[DataFrame]):
    natural_storage_class = MemoryStorageClass
    natural_storage_engine = LocalPythonStorageEngine
    nickname = "dataframe"


try:
    import pyarrow as pa

    ArrowTable = pa.Table
except ImportError:
    ArrowTable = TypeVar("ArrowTable")


class ArrowTableFormat(DataFormatBase[ArrowTable]):
    natural_storage_class = MemoryStorageClass
    nickname = "arrow"


ArrowFile = TypeVar("ArrowFile")


class ArrowFileFormat(DataFormatBase[ArrowFile]):
    natural_storage_class = FileSystemStorageClass
    nickname = "arrowfile"


# TODO: abstract type?
DatabaseCursor = TypeVar("DatabaseCursor")


class DatabaseCursorFormat(DataFormatBase[DatabaseCursor]):
    natural_storage_class = MemoryStorageClass
    storable = False
    nickname = "cursor"


DatabaseTable = TypeVar("DatabaseTable")


class DatabaseTableFormat(DataFormatBase[DatabaseTable]):
    natural_storage_class = DatabaseStorageClass
    nickname = "table"


class FileObject(IOBase):
    pass


class CsvFileObject(FileObject):
    pass


class CsvFileObjectFormat(DataFormatBase[CsvFileObject]):
    natural_storage_class = MemoryStorageClass
    storable = False


CsvFile = TypeVar("CsvFile")


class CsvFileFormat(DataFormatBase[CsvFile]):
    natural_storage_class = FileSystemStorageClass
    nickname = "csv"


class JsonLinesFileObject(FileObject):
    pass


class JsonLinesFileObjectFormat(DataFormatBase[JsonLinesFileObject]):
    natural_storage_class = MemoryStorageClass
    storable = False


JsonLinesFile = TypeVar("JsonLinesFile")


class JsonLinesFileFormat(DataFormatBase[JsonLinesFile]):
    natural_storage_class = FileSystemStorageClass
    nickname = "jsonl"
