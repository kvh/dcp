from dcp.storage.database.api import DatabaseStorageApi
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.storage.base import DatabaseStorageClass, MemoryStorageClass, StorageApi
from dcp.data_copy.costs import (
    FormatConversionCost,
    MemoryToMemoryCost,
    NetworkToMemoryCost,
)
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.storage.memory.memory_records_object import as_records
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.storage.memory.engines.python import PythonStorageApi
from schemas.base import Schema
from dcp.data_copy.base import CopyRequest, datacopy


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[RecordsFormat],
    to_storage_classes=[DatabaseStorageClass],
    to_data_formats=[DatabaseTableFormat],
    cost=NetworkToMemoryCost,
)
def copy_records_to_db(req: CopyRequest):
    assert isinstance(req.from_storage_api, PythonStorageApi)
    assert isinstance(req.to_storage_api, DatabaseStorageApi)
    mdr = req.from_storage_api.get(req.from_name)
    req.to_format_handler.create_empty(
        req.to_name, req.to_storage_api.storage, req.schema
    )
    req.to_storage_api.bulk_insert_records(req.to_name, mdr.records_object)


# @datacopy(
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
#         req.to_name, req.to_storage_api.storage, req.schema
#     )
#     for records in mdr.records_object:
#         req.to_storage_api.bulk_insert_records(req.to_name, records)
