from __future__ import annotations

from typing import Any, Dict, List, Type, TypeVar

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
    Schema,
    Time,
)
from commonmodel.field_types import Binary, Decimal, Json, Text

import dcp.storage.base as storage
from dcp.data_format.base import DataFormat, DataFormatBase
from dcp.data_format.handler import FormatHandler

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
        "Binary": satypes.BINARY,  # TODO
        "LongBinary": satypes.LargeBinary,
        "Text": satypes.Unicode,  # TODO: size mismatch here
        "LongText": satypes.UnicodeText,
        "Json": satypes.JSON,
    }
    sa_type = types[ft.__class__.__name__]
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
    name: str,
    field_type_parameter_defaults: Dict[Type[FieldType], Dict[str, Any]] = None,
) -> sa.Table:
    columns: List[sa.Column] = []
    for field in schema.fields:
        c = field_as_sqlalchemy_column(
            field, (field_type_parameter_defaults or {}).get(type(field.field_type))
        )
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
        "Boolean": Boolean,
        "Int": Integer,
        "Integer": Integer,
        "BigInteger": Integer,
        "Bigint": Integer,
        "Smallint": Integer,
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
        "Binary": Binary,
        "Text": Text,
        "Varchar": Text,
        "Unicode": Text,
        "Unicodetext": Text,
        "Json": Json,
        "JSONB": Json,
        "NullType": Text,  # TODO: What is nulltype?
    }
    all_aliases = {}
    for k, v in satype_aliases.items():
        all_aliases[k.upper()] = v
        all_aliases[k] = v
    try:
        ft = eval(s, {"__builtins__": None}, all_aliases)
        if isinstance(ft, type):
            ft = ft()
        return ft
    except (AttributeError, TypeError):
        raise NotImplementedError(s)
