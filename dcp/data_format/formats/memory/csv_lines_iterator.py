from __future__ import annotations

import decimal
import traceback
from contextlib import contextmanager
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

import dcp.storage.base as storage
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
from dcp.data_format.formats.file_system.csv_file import is_maybe_csv
from dcp.data_format.formats.memory.records import Records, select_field_type
from dcp.data_format.handler import FormatHandler
from dcp.storage.file_system.engines.local import FileSystemStorageApi
from dcp.storage.memory.iterator import SampleableIterator
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


class CsvLinesIterator(IOBase):
    pass


class CsvLinesIteratorFormat(DataFormatBase[CsvLinesIterator]):
    nickname = "csv_lines"
    natural_storage_class = storage.MemoryStorageClass
    storable = False


SAMPLE_SIZE = 1024 * 10
SAMPLE_SIZE_LINES = 100


class PythonCsvLinesIteratorHandler(FormatHandler):
    for_data_formats = [CsvLinesIteratorFormat]
    for_storage_engines = [storage.LocalPythonStorageEngine]

    def get_sample_string(self, name: str, storage: storage.Storage) -> str:
        obj = storage.get_api().get(name)
        assert isinstance(obj, SampleableIterator)
        sample = obj.head(SAMPLE_SIZE_LINES)
        s = "".join(sample)
        return s

    def get_sample_records(self, name: str, storage: storage.Storage) -> Records:
        obj = storage.get_api().get(name)
        assert isinstance(obj, SampleableIterator)
        sample = obj.head(SAMPLE_SIZE_LINES)
        for r in read_csv(sample):
            yield r

    def infer_data_format(
        self, name: str, storage: storage.Storage
    ) -> Optional[DataFormat]:
        obj = storage.get_api().get(name)
        if isinstance(obj, SampleableIterator):
            s = self.get_sample_string(name, storage)
            if is_maybe_csv(s):
                return CsvLinesIteratorFormat
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
        storage.get_api().put(name, (ln for ln in [s]))

    def get_record_count(self, name: str, storage: storage.Storage) -> Optional[int]:
        return None
