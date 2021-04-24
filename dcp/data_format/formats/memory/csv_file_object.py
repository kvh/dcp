from __future__ import annotations
from contextlib import contextmanager

import decimal
import traceback
from datetime import date, datetime, time
from io import IOBase, StringIO
from itertools import tee
from typing import (
    Any,
    AnyStr,
    Dict,
    Iterable,
    List,
    Optional,
    TextIO,
    Type,
    Union,
    cast,
)
from dcp.data_format.formats.file_system.csv_file import is_maybe_csv
from dcp.data_format.formats.memory.records import Records, select_field_type

import dcp.storage.base as storage
from dcp.storage.file_system.engines.local import FileSystemStorageApi
from dcp.storage.memory.iterator import SampleableIOBase
import pandas as pd
from commonmodel import (
    DEFAULT_FIELD_TYPE,
    Boolean,
    Date,
    DateTime,
    Field,
    FieldType,
    Float,
    Integer,
    Schema,
    Time,
)
from commonmodel.field_types import (
    Binary,
    Decimal,
    Json,
    LongBinary,
    LongText,
    Text,
    ensure_field_type,
)
from dateutil import parser
from dcp.data_format.base import DataFormat, DataFormatBase
from dcp.data_format.handler import FormatHandler
from dcp.utils.common import (
    ensure_bool,
    ensure_date,
    ensure_datetime,
    ensure_time,
    is_boolish,
    is_nullish,
    is_numberish,
)
from dcp.utils.data import read_csv, read_json, sample_lines
from loguru import logger
from pandas import DataFrame


class CsvFileObject(IOBase):
    pass


class CsvFileObjectFormat(DataFormatBase[CsvFileObject]):
    nickname = "csv_file_object"
    natural_storage_class = storage.MemoryStorageClass
    storable = False


SAMPLE_SIZE = 1024 * 10
SAMPLE_SIZE_LINES = 10


class PythonCsvFileObjectHandler(FormatHandler):
    for_data_formats = [CsvFileObjectFormat]
    for_storage_engines = [storage.LocalPythonStorageEngine]

    @contextmanager
    def with_file_object(self, name: str, storage: storage.Storage):
        obj = storage.get_api().get(name)
        assert isinstance(obj, SampleableIOBase)
        obj.seek(0)
        yield obj
        obj.seek(0)

    def get_sample_string(self, name: str, storage: storage.Storage) -> str:
        with self.with_file_object(name, storage) as obj:
            s = obj.read(SAMPLE_SIZE)
        if isinstance(s, bytes):
            s = s.decode("utf8")
        return s

    def get_sample_records(self, name: str, storage: storage.Storage) -> Records:
        s = self.get_sample_string(name, storage)
        for r in read_csv(s.split("\n")):
            yield r

    def infer_data_format(
        self, name: str, storage: storage.Storage
    ) -> Optional[DataFormat]:
        obj = storage.get_api().get(name)
        if isinstance(obj, SampleableIOBase):
            s = self.get_sample_string(name, storage)
            if is_maybe_csv(s):
                return CsvFileObjectFormat
        return None

    # TODO: get sample
    def infer_field_names(self, name, storage) -> List[str]:
        names = []
        for r in self.get_sample_records(name, storage):
            for k in r.keys():  # Ordered as of py 3.7
                if k not in names:
                    names.append(k)  # Keep order
        return names

    def infer_field_type(
        self, name: str, storage: storage.Storage, field: str
    ) -> FieldType:
        sample = []
        for r in self.get_sample_records(name, storage):
            if field in r:
                sample.append(r[field])
            if len(sample) >= self.sample_size:
                break
        ft = select_field_type(sample)
        return ft

    def cast_to_field_type(
        self, name: str, storage: storage.Storage, field: str, field_type: FieldType
    ):
        # TODO: no-op? not really types on strings...
        pass

    def create_empty(self, name, storage, schema: Schema):
        s = ",".join(schema.field_names()) + "\n"
        storage.get_api().put(name, StringIO(s))

    def get_record_count(self, name: str, storage: storage.Storage) -> Optional[int]:
        return None
