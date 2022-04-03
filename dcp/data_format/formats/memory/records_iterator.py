from __future__ import annotations

import decimal
import traceback
from datetime import date, datetime, time
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
    Iterator,
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
from dcp.utils.data import read_json
from loguru import logger
from pandas import DataFrame


T = TypeVar("T")


class IteratorBase(Generic[T]):
    def __init__(self, iterator: Iterable[T], closeable: Callable = None):
        self.iterator = iterator
        self.closeable = closeable

    def __iter__(self) -> Iterator[T]:
        yield from self.iterator
    
    def chunks(self, chunksize: int) -> Iterator:
        try:
            chunk = []
            for record in self.iterator:
                chunk.append(record)
                if len(chunk) == chunksize:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk
        finally:
            self.close()
            
    def close(self):
        if self.closeable:
            self.closeable()

    def concat(self, append_other: IteratorBase):
        def f() -> Iterable[T]:
            yield from self.iterator
            yield from append_other.iterator

        def c():
            self.close()
            append_other.close()

        return type(self)(f(), c)


class RecordsIterator(IteratorBase[Dict[str, Any]]):
    pass


class RecordsIteratorFormat(DataFormatBase[RecordsIterator]):
    natural_storage_class = storage.MemoryStorageClass
    nickname = "records_iterator"


class PythonRecordsIteratorHandler(FormatHandler):
    for_data_formats = [RecordsIteratorFormat]
    for_storage_engines = [storage.LocalPythonStorageEngine]

    def infer_data_format(self, name, storage) -> Optional[DataFormat]:
        return None

    def infer_field_names(self, name, storage) -> List[str]:
        raise NotImplementedError

    def infer_field_type(
        self, name: str, storage: storage.Storage, field: str
    ) -> FieldType:
        raise NotImplementedError

    def cast_to_field_type(
        self, name: str, storage: storage.Storage, field: str, field_type: FieldType
    ):
        raise NotImplementedError

    def create_empty(self, name, storage, schema: Schema):
        def f():
            yield from []

        storage.get_api().put(name, RecordsIterator(f()))
