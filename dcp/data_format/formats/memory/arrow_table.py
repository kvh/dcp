from __future__ import annotations

import decimal
from datetime import date, datetime, time
from typing import Any, Dict, Iterable, List, Optional, Type, TypeVar, Union, cast

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
    DEFAULT_FIELD_TYPE_CLASS,
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
from dcp.utils.data import read_json
from loguru import logger
from pandas import DataFrame

try:
    import pyarrow as pa

    ArrowTable = pa.Table
except ImportError:
    ArrowTable = TypeVar("ArrowTable")


class ArrowTableFormat(DataFormatBase[ArrowTable]):
    natural_storage_class = storage.MemoryStorageClass
    nickname = "arrow"


class ArrowTableHandler(FormatHandler):
    for_data_formats = [ArrowTableFormat]
    for_storage_engines = [storage.LocalPythonStorageEngine]

    def infer_data_format(self, name, storage) -> Optional[DataFormat]:
        obj = storage.get_api().get(name)
        if isinstance(obj, pa.Table):
            return ArrowTableFormat
        return None

    def infer_field_names(self, name, storage) -> List[str]:
        table = storage.get_api().get(name)
        assert isinstance(table, ArrowTable)
        return [f.name for f in table.schema]

    def infer_field_type(
        self, name: str, storage: storage.Storage, field: str
    ) -> FieldType:
        table: ArrowTable = storage.get_api().get(name)
        return arrow_type_to_field_type(str(table.field(field).type))

    def cast_to_field_type(
        self, name: str, storage: storage.Storage, field: str, field_type: FieldType
    ):
        # TODO
        pass

    def create_empty(self, name, storage, schema: Schema):
        table = pa.Table.from_batches([], schema=schema_to_arrow_schema(schema))
        storage.get_api().put(name, table)


def schema_to_arrow_schema(schema: Schema) -> pa.Schema:
    fields = [(f.name, field_type_to_arrow_type(f.field_type)) for f in schema.fields]
    return pa.schema(fields)


def arrow_type_to_field_type(arrow_type: str) -> FieldType:
    """
    null
    bool_
    int8
    int16
    int32
    int64
    uint8
    uint16
    uint32
    uint64
    float16
    float32
    float64
    time32
    time64
    timestamp
    date32
    date64
    binary
    string
    utf8
    large_binary
    large_string
    large_utf8
    decimal128
    list_(value_type, int list_size=-1)
    large_list(value_type)
    map_(key_type, item_type[, keys_sorted])
    struct(fields)
    dictionary(index_type, value_type, …)
    field(name, type, bool nullable=True[, metadata])
    schema(fields[, metadata])
    """
    if arrow_type.startswith("bool"):
        return Boolean()
    if arrow_type.startswith("int") or arrow_type.startswith("uint"):
        return Integer()
    if arrow_type.startswith("float"):
        return Float()
    if arrow_type.startswith("decimal"):
        return Decimal()
    if arrow_type.startswith("binary"):
        return Binary()
    if arrow_type.startswith("large_binary"):
        return LongBinary()
    if arrow_type.startswith("utf8") or arrow_type.startswith("string"):
        return Text()
    if arrow_type.startswith("large_utf8") or arrow_type.startswith("large_string"):
        return LongText()
    if arrow_type.startswith("timestamp"):
        return DateTime()
    if arrow_type.startswith("time"):
        return Time()
    if arrow_type.startswith("date"):
        return Date()
    return DEFAULT_FIELD_TYPE


def field_type_to_arrow_type(field_type: FieldType) -> pa.DataType:
    types = {
        Boolean: pa.bool_,
        Integer: pa.int64,
        Float: pa.float64,
        Decimal: pa.decimal128,
        Binary: pa.binary,
        LongBinary: pa.large_binary,
        Text: pa.utf8,
        LongText: pa.large_utf8,
        Time: pa.time64,
        Date: pa.date32,
        DateTime: pa.timestamp,
    }
    pa_type = types[type(field_type)]
    if pa_type == pa.decimal128:
        pdt = pa_type(**field_type.get_parameters())
    elif pa_type in (pa.time64, pa.timestamp):
        pdt = pa_type("us")
    else:
        pdt = pa_type()
    return pdt
