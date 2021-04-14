from __future__ import annotations

from typing import Dict, List, Optional, Type, cast

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


class DataFrameFormat(DataFormatBase[DataFrame]):
    natural_storage_class = storage.MemoryStorageClass
    natural_storage_engine = storage.LocalPythonStorageEngine
    nickname = "dataframe"


class PythonDataframeHandler(FormatHandler):
    for_data_formats = [DataFrameFormat]
    for_storage_engines = [storage.LocalPythonStorageEngine]

    def infer_data_format(self, name, storage) -> Optional[DataFormat]:
        obj = storage.get_api().get(name)
        if isinstance(obj, pd.DataFrame):
            return DataFrameFormat
        return None

    def infer_field_names(self, name, storage) -> List[str]:
        return storage.get_api().get(name).columns

    def infer_field_type(
        self, name: str, storage: storage.Storage, field: str
    ) -> FieldType:
        df = storage.get_api().get(name)
        cast(DataFrame, df)
        series = df[field]
        ft = pandas_series_to_field_type(series)
        return ft

    def cast_to_field_type(
        self, name: str, storage: storage.Storage, field: str, field_type: FieldType
    ):
        df = storage.get_api().get(name)
        cast(DataFrame, df)
        if field in df.columns:
            df[field] = cast_series_to_field_type(df[field], field_type)
        storage.get_api().put(name, df)  # Unnecessary?

    def create_empty(self, name, storage, schema: Schema):
        df = DataFrame()
        for field in schema.fields:
            pd_type = field_type_to_pandas_dtype(field.field_type)
            df[field.name] = pd.Series(dtype=pd_type)
        storage.get_api().put(name, df)

    def supports(self, field_type) -> bool:
        raise NotImplementedError


def pandas_series_to_field_type(series: pd.Series) -> FieldType:
    """
    Cribbed from pandas.io.sql
    Changes:
        - strict datetime and JSON inference
        - No timezone handling
        - No single/32 numeric types
    """
    dtype = pd.api.types.infer_dtype(series, skipna=True).lower()
    if dtype == "datetime64" or dtype == "datetime":
        # GH 9086: TIMESTAMP is the suggested type if the column contains
        # timezone information
        # try:
        #     if col.dt.tz is not None:
        #         return TIMESTAMP(timezone=True)
        # except AttributeError:
        #     # The column is actually a DatetimeIndex
        #     if col.tz is not None:
        #         return TIMESTAMP(timezone=True)
        return DateTime()
    if dtype == "timedelta64":
        raise NotImplementedError  # TODO
    elif dtype.startswith("float"):
        return Float()
    elif dtype.startswith("int"):
        return Integer()
    elif dtype == "boolean":
        return Boolean()
    elif dtype == "date":
        return Date()
    elif dtype == "time":
        return Time()
    elif dtype == "complex":
        raise ValueError("Complex number datatype not supported")
    elif dtype == "empty":
        return DEFAULT_FIELD_TYPE
    else:
        # Handle object / string case as generic detection
        # try:
        return select_field_type(series.dropna().iloc[:100])
        # except ParserError:
        #     pass


def field_type_to_pandas_dtype(ft: FieldType) -> str:
    lookup: Dict[Type[FieldType], str] = {
        Boolean: "boolean",
        Integer: "Int64",
        Float: "float64",
        Decimal: "float64",
        Binary: "bytes",  # TODO: is this a thing?
        LongBinary: "bytes",
        Text: "string",
        LongText: "string",
        Date: "datetime64[ns]",
        Time: "time",
        DateTime: "datetime64[ns]",
        Json: "object",
    }
    return lookup.get(ft.__class__, "object")


def cast_series_to_field_type(s: pd.Series, field_type: FieldType) -> pd.Series:
    pd_type = field_type_to_pandas_dtype(field_type)
    if s.dtype.name == pd_type:
        return s
    if "datetime" in pd_type:
        s = pd.to_datetime(s)
        return s
    try:
        return s.astype(pd_type)
    except (TypeError, ValueError, parser.ParserError):
        pass
    # Fall back to parsing individual values (very slow...)
    return pd.Series([cast_python_object_to_field_type(v, field_type) for v in s])
