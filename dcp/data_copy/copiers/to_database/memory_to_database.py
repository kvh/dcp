from __future__ import annotations

from typing import Any, Sequence

from commonmodel.base import Schema
from dcp.data_copy.base import CopyRequest, DataCopierBase, create_empty_if_not_exists
from dcp.data_copy.costs import (
    FormatConversionCost,
    MemoryToMemoryCost,
    NetworkToMemoryCost,
)
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.storage.base import DatabaseStorageClass, MemoryStorageClass, StorageApi
from dcp.storage.database.api import DatabaseStorageApi
from dcp.storage.memory.engines.python import PythonStorageApi


class MemoryToDatabaseMixin:
    from_storage_classes = [MemoryStorageClass]
    to_storage_classes = [DatabaseStorageClass]

    def append(self, req: CopyRequest):
        obj = req.from_obj.storage.get_memory_api().get(req.from_obj)
        self.insert_object(req, obj)

    def insert_object(self, req: CopyRequest, obj: Any):
        raise NotImplementedError


class RecordsToDatabaseTable(MemoryToDatabaseMixin, DataCopierBase):
    from_data_formats = [RecordsFormat]
    to_data_formats = [DatabaseTableFormat]
    cost = NetworkToMemoryCost
    requires_schema_cast = False

    def insert_object(self, req: CopyRequest, obj: Records):
        req.to_obj.storage.get_database_api().bulk_insert_records(
            req.to_obj, obj, req.get_to_schema()
        )


# @datacopier(
#     from_storage_classes=[MemoryStorageClass],
#     from_data_formats=[RecordsIteratorFormat],
#     to_storage_classes=[DatabaseStorageClass],
#     to_data_formats=[DatabaseTableFormat],
#     cost=NetworkToBufferCost,
# )
# def copy_records_iterator_to_db(
#     req: CopyRequest
# ):
#     assert isinstance(req.from_storage_api, PythonStorageApi)
#     assert isinstance(req.to_storage_api, DatabaseStorageApi)
#     mdr = req.from_storage_api.get(req.from_name)
#     req.to_format_handler.create_empty(
#         req.to_name, req.to_storage_api.storage, req.get_schema()
#     )
#     for records in mdr.records_object:
#         req.to_obj.storage.get_database_api().bulk_insert_records(req.to_name, records)
