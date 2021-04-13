from __future__ import annotations


import dcp.storage.base as storage
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.types as satypes
from dcp.data_format.base import DataFormat, DataFormatBase
from dcp.data_format.formats.database.base import (
    GenericDatabaseTableHandler,
    schema_as_sqlalchemy_table,
    sqlalchemy_type_to_field_type,
)
from dcp.data_format.formats.memory.records import (
    cast_python_object_to_field_type,
    select_field_type,
)
from dcp.data_format.handler import FormatHandler
from dateutil import parser
from loguru import logger
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
from pandas import DataFrame
from sqlalchemy.sql.ddl import CreateTable

DEFAULT_MYSQL_VARCHAR_LENGTH = 255


class MysqlDatabaseTableHandler(GenericDatabaseTableHandler):
    for_storage_classes = []
    for_storage_engines = [storage.MysqlStorageEngine]

    def infer_field_type(
        self, name: str, storage: storage.Storage, field: str
    ) -> FieldType:
        ftype = super().infer_field_type(name, storage, field)
        ftype = parameterize_field_type_for_mysql(ftype)
        return ftype

    def create_empty(self, name: str, storage: storage.Storage, schema: Schema):
        table = schema_as_sqlalchemy_table(
            schema, name, field_type_parameter_defaults={Text: {"length": 255}}
        )
        storage.get_api().create_sqlalchemy_table(table)


def parameterize_field_type_for_mysql(ft: FieldType) -> FieldType:
    # Mysql requires a length for varchar fields
    if isinstance(ft, Text):
        ln = ft.get_parameter("length")
        if ln is None:
            ft.update_parameter("length", DEFAULT_MYSQL_VARCHAR_LENGTH)
    return ft
