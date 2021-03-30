from __future__ import annotations

from sqlalchemy.sql.ddl import CreateTable
from dcp.data_format.formats.memory.records import (
    cast_python_object_to_field_type,
    select_field_type,
)
import sqlalchemy as sa
import sqlalchemy.types as satypes
from schemas.field_types import Binary, Decimal, Json, LongBinary, LongText, Text
from dcp.data_format.handler import FormatHandler
from dcp.data_format.base import DataFormat, DataFormatBase
from typing import Dict, List, Type, TypeVar, cast
from loguru import logger
from dateutil import parser
import pandas as pd
from pandas import DataFrame

import dcp.storage.base as storage
from schemas import (
    DEFAULT_FIELD_TYPE,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    Time,
    Field,
    FieldType,
    Schema,
)


DatabaseTable = TypeVar("DatabaseTable")


class DatabaseTableFormat(DataFormatBase[DatabaseTable]):
    natural_storage_class = storage.DatabaseStorageClass
    nickname = "table"


class GenericDatabaseTableHandler(FormatHandler):
    for_data_formats = [DatabaseTableFormat]
    for_storage_classes = [storage.DatabaseStorageClass]

    def infer_data_format(self, name, storage) -> DataFormat:
        return DatabaseTableFormat

    def infer_field_names(self, name, storage) -> List[str]:
        tble = storage.get_api().get_as_sqlalchemy_table(name)
        return [c.name for c in tble.columns]

    def infer_field_type(
        self, name: str, storage: storage.Storage, field: str
    ) -> FieldType:
        tble: sa.Table = storage.get_api().get_as_sqlalchemy_table(name)
        for c in tble.columns:
            if c.name == field:
                return sqlalchemy_type_to_field_type(c.type)
        raise ValueError(f"Field does not exist: {field}")

    def cast_to_field_type(
        self, name: str, storage: storage.Storage, field: str, field_type: FieldType
    ):
        # TODO
        pass

    def create_empty(self, name, storage, schema: Schema):
        table = schema_as_sqlalchemy_table(schema, name)
        storage.get_api().create_sqlalchemy_table(table)


def field_type_to_sqlalchemy_type(ft: FieldType) -> satypes.TypeEngine:
    return {
        "Boolean": satypes.Boolean,
        "Integer": satypes.BigInteger,
        "Float": satypes.Float,
        "Decimal": satypes.Numeric,
        "Date": satypes.Date,
        "Time": satypes.Time,
        "DateTime": satypes.DateTime,
        "Binary": satypes.BINARY,  # TODO
        "LongBinary": satypes.LargeBinary,
        "Text": satypes.Unicode,  # TODO: size mismatch here
        "LongText": satypes.UnicodeText,
        "Json": satypes.JSON,
    }[ft.__class__.__name__]


def field_as_sqlalchemy_column(f: Field) -> sa.Column:
    return sa.Column(f.name, field_type_to_sqlalchemy_type(f.field_type))


def schema_as_sqlalchemy_table(schema: Schema, name: str) -> sa.Table:
    columns: List[sa.Column] = []
    for field in schema.fields:
        c = field_as_sqlalchemy_column(field)
        columns.append(c)
    # TODO: table level constraints
    sa_table = sa.Table(name, sa.MetaData(), *columns)
    return sa_table


def sqlalchemy_type_to_field_type(sa_type: satypes.TypeEngine) -> FieldType:
    """
    Supports additional Sqlalchemy types and arrow types, as well as legacy names
    """
    s = repr(sa_type)
    satype_aliases = {
        # Sqlalchemy
        "int": Integer,
        "integer": Integer,
        "biginteger": Integer,
        "bigint": Integer,
        "numeric": Decimal,
        "real": Float,
        "date": Date,
        "datetime": DateTime,
        "time": Time,
        "text": Text,
        "varchar": Text,
        "unicode": Text,
        "unicodetext": Text,
        "json": Json,
    }
    try:
        ls = s.lower()
        ft = eval(ls, {"__builtins__": None}, satype_aliases)
        if isinstance(ft, type):
            ft = ft()
        return ft
    except (AttributeError, TypeError):
        raise NotImplementedError(s)
