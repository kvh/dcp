from __future__ import annotations

import dcp.storage.base as storage
from commonmodel import FieldType, Schema
from commonmodel.field_types import Text
from dcp.data_format.formats.database.base import (
    GenericDatabaseTableHandler,
    schema_as_sqlalchemy_table,
)

DEFAULT_MYSQL_VARCHAR_LENGTH = 255


class MysqlDatabaseTableHandler(GenericDatabaseTableHandler):
    for_storage_classes = []
    for_storage_engines = [storage.MysqlStorageEngine]

    def infer_field_type(self, so: storage.StorageObject, field: str) -> FieldType:
        ftype = super().infer_field_type(so, field)
        ftype = parameterize_field_type_for_mysql(ftype)
        return ftype

    def create_empty(self, so: storage.StorageObject, schema: Schema):
        table = schema_as_sqlalchemy_table(
            schema, so, field_type_parameter_defaults={"Text": {"length": 255}}
        )
        so.storage.get_database_api().create_sqlalchemy_table(table)


def parameterize_field_type_for_mysql(ft: FieldType) -> FieldType:
    # Mysql requires a length for varchar fields
    if ft.name == "Text":
        params = ft.get_parameters()
        ln = params.get("length")
        if ln is None:
            params["length"] = DEFAULT_MYSQL_VARCHAR_LENGTH
            ft = ft.copy(update={"parameters": params})
    return ft
