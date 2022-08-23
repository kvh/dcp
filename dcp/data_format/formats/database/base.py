from __future__ import annotations

import json
from typing import Any, Dict, List, TypeVar

import sqlalchemy as sa
import sqlalchemy.types as satypes
from commonmodel import (
    Boolean,
    Date,
    DateTime,
    Field,
    FieldType,
    Float,
    Integer,
    Interval,
    Schema,
    Time,
)
from commonmodel.field_types import (
    Binary,
    Decimal,
    Json,
    Text,
    str_to_field_type,
    Array,
)
from sqlalchemy import TypeDecorator, UnicodeText

import dcp.storage.base as storage
from dcp.data_format.base import DataFormat, DataFormatBase
from dcp.data_format.handler import FormatHandler
from dcp.utils.common import to_json

DatabaseTable = TypeVar("DatabaseTable")


class DatabaseTableFormat(DataFormatBase[DatabaseTable]):
    natural_storage_class = storage.DatabaseStorageClass
    nickname = "table"


class GenericDatabaseTableHandler(FormatHandler):
    for_data_formats = [DatabaseTableFormat]
    for_storage_classes = [storage.DatabaseStorageClass]

    def infer_data_format(self, obj: storage.StorageObject) -> DataFormat:
        return DatabaseTableFormat

    def infer_field_names(self, so: storage.StorageObject) -> List[str]:
        tble = so.storage.get_database_api().get_as_sqlalchemy_table(so)
        return [c.name for c in tble.columns]

    def infer_field_type(self, so: storage.StorageObject, field: str) -> FieldType:
        tble: sa.Table = so.storage.get_database_api().get_as_sqlalchemy_table(so)
        for c in tble.columns:
            if c.name == field:
                return sqlalchemy_type_to_field_type(c.type)
        raise ValueError(f"Field does not exist: {field}")

    def cast_to_field_type(
        self, so: storage.StorageObject, field: str, field_type: FieldType
    ):
        # TODO
        pass

    def create_empty(self, so: storage.StorageObject, schema: Schema):
        table = schema_as_sqlalchemy_table(schema, so)
        so.storage.get_database_api().create_sqlalchemy_table(table)


class CustomJson(TypeDecorator):
    impl = UnicodeText
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            value = to_json(value)
        return value

    def process_result_value(self, value, dialect):
        if value is not None and isinstance(value, str):
            value = json.loads(value)
        return value


def field_type_to_sqlalchemy_type(
    ft: FieldType, field_type_parameter_defaults: Dict[str, Any] = None
) -> satypes.TypeEngine:
    types = {
        "Boolean": satypes.Boolean,
        "Integer": satypes.BigInteger,
        "Float": satypes.Float,
        "Decimal": satypes.Numeric,
        "Date": satypes.Date,
        "Time": satypes.Time,
        "DateTime": satypes.DateTime,
        "Interval": satypes.Interval,
        "Binary": satypes.BINARY,  # TODO
        "LongBinary": satypes.LargeBinary,
        "Text": satypes.Unicode,  # TODO: size mismatch here
        "LongText": satypes.UnicodeText,
        "Json": CustomJson,
    }
    sa_type = types[ft.name]
    if field_type_parameter_defaults:
        params = field_type_parameter_defaults.copy()
        params.update(ft.get_parameters())
    else:
        params = ft.get_parameters()
    return sa_type(**params)


def field_as_sqlalchemy_column(
    f: Field, field_type_parameter_defaults: Dict[str, Any] = None
) -> sa.Column:
    return sa.Column(
        f.name,
        field_type_to_sqlalchemy_type(f.field_type, field_type_parameter_defaults),
    )


def schema_as_sqlalchemy_table(
    schema: Schema,
    so: storage.StorageObject,
    field_type_parameter_defaults: Dict[str, Dict[str, Any]] = None,
) -> sa.Table:
    columns: List[sa.Column] = []
    for field in schema.fields:
        c = field_as_sqlalchemy_column(
            field, (field_type_parameter_defaults or {}).get(field.field_type.name)
        )
        columns.append(c)
    # TODO: table level constraints
    sa_table = sa.Table(
        so.full_path.name,
        sa.MetaData(),
        *columns,
        schema=so.full_path.get_last_path_element(),
    )
    return sa_table


all_aliases = {}
_satype_aliases = {
    # Sqlalchemy
    "Boolean": Boolean,
    "Int": Integer,
    "Integer": Integer,
    "BigInteger": Integer,
    "BigInt": Integer,
    "SmallInteger": Integer,
    "SmallInt": Integer,
    "Decimal": Decimal,
    "Numeric": Decimal,
    "Float": Float,
    "Real": Float,
    "Double": Float,
    "Double_Precision": Float,
    "Date": Date,
    "Datetime": DateTime,
    "Timestamp": DateTime,
    "Time": Time,
    "Interval": Interval,
    "Binary": Binary,
    "Text": Text,
    "Varchar": Text,
    "NVarchar": Text,
    "Unicode": Text,
    "UnicodeText": Text,
    "String": Text,
    "Json": Json,
    "JSONB": Json,
    "NullType": Text,  # TODO: What is nulltype?
    "Array": Json,  # TODO: properly support sql array at some point
}
for k, v in _satype_aliases.items():
    all_aliases[k.upper()] = v
    all_aliases[k] = v

ignore_args_sa_types = [
    "array",  # Ignore array sub-type arg
]


def sqlalchemy_type_to_field_type(sa_type: satypes.TypeEngine) -> FieldType:
    """
    Supports additional Sqlalchemy types and arrow types, as well as legacy names
    """
    s = repr(sa_type)
    parts = s.split("(")
    name = parts[0]
    cm_type = all_aliases[name]
    args = ")"
    if name.lower() not in ignore_args_sa_types:
        if len(parts) > 1:
            args = "(".join(parts[1:])
    cm_str = cm_type.name + "(" + args
    return str_to_field_type(cm_str)
