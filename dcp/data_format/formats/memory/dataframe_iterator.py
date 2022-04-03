from __future__ import annotations

from typing import Callable, Dict, Iterable, List, Optional, Type, cast
from dcp.data_format.formats.memory.dataframe import (
    PythonDataframeHandler,
    cast_series_to_field_type,
)
from dcp.data_format.formats.memory.records_iterator import RecordsIterator

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
from commonmodel.field_types import Binary, Decimal, Json, LongBinary, LongText, Text
from dateutil import parser
from dcp.data_format.base import DataFormat, DataFormatBase
from dcp.data_format.formats.memory.records import (
    cast_python_object_to_field_type,
    select_field_type,
)
from dcp.data_format.handler import FormatHandler
from loguru import logger
from pandas import DataFrame


class DataFrameIteratorFormat(DataFormatBase[DataFrame]):
    natural_storage_class = storage.MemoryStorageClass
    natural_storage_engine = storage.LocalPythonStorageEngine
    nickname = "dataframe_iterator"


class DataFrameIterator:
    def __init__(self, iterator: RecordsIterator, apply: list[Callable] = None):
        self.iterator = iterator
        self.apply = apply or []

    def chunks(self, size: int) -> Iterable[DataFrame]:
        try:
            chunk = []
            for record in self.iterator:
                chunk.append(record)
                if len(chunk) == size:
                    yield self._build_df(chunk)
                    chunk = []
            if chunk:
                yield self._build_df(chunk)
        finally:
            self.close()
    
    def all(self) -> DataFrame:
        return self._build_df(list(self.iterator))

    def _build_df(self, records: List[Dict]) -> DataFrame:
        df = DataFrame(records)
        for a in self.apply:
            df = a(df)
        return df

    def close(self):
        self.iterator.close()

    def concat(self, append_other: DataFrameIterator):
        return DataFrameIterator(
            self.iterator.concat(append_other.iterator), self.apply
        )


def cast_df_to_schema(df: DataFrame, field: str, field_type: FieldType) -> DataFrame:
    if field in df.columns:
        df[field] = cast_series_to_field_type(df[field], field_type)
    return df


class PythonDataframeIteratorHandler(FormatHandler):
    for_data_formats = [DataFrameIteratorFormat]
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
        df_iterator = storage.get_api().get(name)
        cast_df_iterator = DataFrameIterator(
            df_iterator.iterator,
            df_iterator.apply + [lambda df: cast_df_to_schema(df, field, field_type)],
        )
        storage.get_api().put(
            name,
            cast_df_iterator,
        )

    def create_empty(self, name, storage, schema: Schema):
        def f():
            yield from []

        storage.get_api().put(name, DataFrameIterator(RecordsIterator(f())))

    def supports(self, field_type) -> bool:
        raise NotImplementedError
