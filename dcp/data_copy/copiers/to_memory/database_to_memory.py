from dcp.data_copy.base import CopyRequest, create_empty_if_not_exists, datacopier
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
from dcp.storage.database.utils import result_proxy_to_records
from dcp.storage.memory.engines.python import PythonStorageApi
from commonmodel.base import Schema


@datacopier(
    from_storage_classes=[DatabaseStorageClass],
    from_data_formats=[DatabaseTableFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[RecordsFormat],
    cost=NetworkToMemoryCost,
)
def copy_db_to_records(req: CopyRequest):
    assert isinstance(req.from_storage_api, DatabaseStorageApi)
    assert isinstance(req.to_storage_api, PythonStorageApi)
    select_sql = f"select * from {req.from_name}"
    create_empty_if_not_exists(req)
    existing_records = req.to_storage_api.get(req.to_name)
    with req.from_storage_api.execute_sql_result(select_sql) as r:
        records = result_proxy_to_records(r)
        req.to_storage_api.put(req.to_name, existing_records + records)


# @datacopier(
#     from_storage_classes=[DatabaseStorageClass],
#     from_data_formats=[DatabaseTableFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[RecordsIteratorFormat],
#     cost=NetworkToBufferCost,
# )
# def copy_db_to_records_iterator(
#     from_name: str,
#     to_name: str,
#     conversion: Conversion,
#     from_storage_api: StorageApi,
#     to_storage_api: StorageApi,
#     schema: Schema,
# ):
#     assert isinstance(from_storage_api, DatabaseStorageApi)
#     assert isinstance(to_storage_api, PythonStorageApi)
#     select_sql = f"select * from {from_name}"
#     conn = (
#         from_storage_api.get_engine().connect()
#     )  # Gonna leave this connection hanging... # TODO: add "closeable" to the MDR and handle?
#     r = conn.execute(select_sql)

#     def f():
#         while True:
#             # TODO: how to parameterize this chunk size? (it's approximate anyways for some dbs?)
#             rows = r.fetchmany(1000)
#             if not rows:
#                 return
#             records = result_proxy_to_records(r, rows=rows)
#             yield records

#     mdr = as_records(f(), data_format=RecordsIteratorFormat, schema=schema)
#     mdr = mdr.conform_to_schema()
#     mdr.closeable = conn.close
#     to_storage_api.put(to_name, mdr)


# @datacopier(
#     from_storage_classes=[DatabaseStorageClass],
#     from_data_formats=[DatabaseTableFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[DatabaseCursorFormat],
#     cost=NetworkToBufferCost,
# )
# def copy_db_to_cursor(
#     from_name: str,
#     to_name: str,
#     conversion: Conversion,
#     from_storage_api: StorageApi,
#     to_storage_api: StorageApi,
#     schema: Schema,
# ):
#     assert isinstance(from_storage_api, DatabaseStorageApi)
#     assert isinstance(to_storage_api, PythonStorageApi)
#     select_sql = f"select * from {from_name}"
#     conn = (
#         from_storage_api.get_engine().connect()
#     )  # Gonna leave this connection hanging... # TODO: add "closeable" to the MDR and handle?
#     r = conn.execute(select_sql)
#     mdr = as_records(r, data_format=DatabaseCursorFormat, schema=schema)
#     mdr = mdr.conform_to_schema()
#     mdr.closeable = conn.close
#     to_storage_api.put(to_name, mdr)


# # @datacopier(
# #     from_storage_classes=[DatabaseStorageClass],
# #     from_data_formats=[DatabaseTableFormat],
# #     to_storage_classes=[MemoryStorageClass],
# #     to_data_formats=[DatabaseTableFormat],
# #     cost=NoOpCost,
# # )
# # def copy_db_to_ref(
# #     from_name: str,
# #     to_name: str,
# #     conversion: Conversion,
# #     from_storage_api: StorageApi,
# #     to_storage_api: StorageApi,
# #     schema: Schema,
# # ):
# #     assert isinstance(from_storage_api, DatabaseStorageApi)
# #     assert isinstance(to_storage_api, PythonStorageApi)
# #     r = DatabaseTableRef(to_name, storage_url=from_storage_api.storage.url)
# #     mdr = as_records(r, data_format=DatabaseTableFormat, schema=schema)
# #     to_storage_api.put(to_name, mdr)
