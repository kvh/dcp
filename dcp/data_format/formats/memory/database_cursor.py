from __future__ import annotations

from typing import List, Optional

from commonmodel import (
    FieldType,
    Schema,
)
from sqlalchemy.engine import ResultProxy

import dcp.storage.base as storage
from dcp.data_format.formats.database.base import sqlalchemy_type_to_field_type
from dcp.data_format.base import DataFormat, DataFormatBase
from dcp.data_format.handler import FormatHandler
from dcp.data_format.inference import generate_auto_schema


class DatabaseCursor:
    def __init__(self, result_proxy: ResultProxy | None):
        self.result_proxy = result_proxy

    def __iter__(self):
        if self.result_proxy is None:
            raise StopIteration
        yield from self.result_proxy

    def close(self):
        if self.result_proxy is None:
            return
        self.result_proxy.close()


class DatabaseCursorFormat(DataFormatBase[DatabaseCursor]):
    natural_storage_class = storage.MemoryStorageClass
    nickname = "database_cursor"


class DatabaseCursorHandler(FormatHandler):
    for_data_formats = [DatabaseCursorFormat]
    for_storage_engines = [storage.LocalPythonStorageEngine]

    def infer_data_format(self, so: storage.StorageObject) -> Optional[DataFormat]:
        obj = so.storage.get_memory_api().get(so)
        if isinstance(obj, ResultProxy):
            return DatabaseCursorFormat
        return None

    def infer_field_names(self, so: storage.StorageObject) -> List[str]:
        cur = so.storage.get_memory_api().get(so)
        return list(cur.keys())

    def infer_field_type(self, so: storage.StorageObject, field: str) -> FieldType:
        # TODO: this only works for ORM queries
        cur = so.storage.get_memory_api().get(so)
        types = {col.name: col.type for col in cur.context.compiled.statement.columns}
        return sqlalchemy_type_to_field_type(types[field])

    def infer_schema(self, so: storage.StorageObject) -> Schema:
        fields = []
        schema = generate_auto_schema(fields=fields)
        return schema

    def cast_to_field_type(
        self, so: storage.StorageObject, field: str, field_type: FieldType
    ):
        # TODO
        pass

    def create_empty(self, so: storage.StorageObject, schema: Schema):
        so.storage.get_memory_api().put(so, DatabaseCursor(None))
