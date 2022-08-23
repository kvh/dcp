from sqlalchemy.engine import Result

from dcp.data_copy.base import CopyRequest, DataCopierBase
from dcp.data_copy.costs import (
    NetworkToMemoryCost,
)
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.data_format.formats.memory.records_iterator import (
    RecordsIterator,
    RecordsIteratorFormat,
)
from dcp.storage.base import (
    DatabaseStorageClass,
    MemoryStorageClass,
)
from dcp.storage.database.utils import result_proxy_to_records


class DatabaseToMemoryMixin:
    from_storage_classes = [DatabaseStorageClass]
    to_storage_classes = [MemoryStorageClass]

    def append(self, req: CopyRequest):
        existing = req.to_obj.storage.get_memory_api().get(req.to_obj)
        select_sql = f"select * from {req.from_obj.formatted_full_name}"
        with req.from_obj.storage.get_database_api().execute_sql_result(
            select_sql
        ) as r:
            new = self.result_to_object(r)
        final = self.concat(existing, new)
        req.to_obj.storage.get_memory_api().put(req.to_obj, final)

    def concat(self, existing, new):
        raise NotImplementedError

    def result_to_object(self, res: Result):
        raise NotImplementedError


class DatabaseTableToRecords(DatabaseToMemoryMixin, DataCopierBase):
    from_data_formats = [DatabaseTableFormat]
    to_data_formats = [RecordsFormat]
    cost = NetworkToMemoryCost
    requires_schema_cast = False

    def concat(self, existing: Records, new: Records) -> Records:
        return existing + new

    def result_to_object(self, res: Result):
        records = result_proxy_to_records(res)
        return records


class DatabaseTableToRecordsIterator(DatabaseToMemoryMixin, DataCopierBase):
    from_data_formats = [DatabaseTableFormat]
    to_data_formats = [RecordsIteratorFormat]
    cost = NetworkToMemoryCost
    requires_schema_cast = False

    def append(self, req: CopyRequest):
        existing = req.to_obj.storage.get_memory_api().get(req.to_obj)
        select_sql = f"select * from {req.from_obj.formatted_full_name}"
        conn = req.from_obj.storage.get_database_api().get_engine().connect()
        res = conn.execute(select_sql)

        def c():
            res.close()
            conn.close()

        def f():
            while True:
                # TODO: how to parameterize this chunk size? (it's approximate anyways for some dbs?)
                rows = res.fetchmany(100)
                if not rows:
                    return
                records = result_proxy_to_records(res, rows=rows)
                for record in records:
                    yield record

        new = RecordsIterator(f(), c)
        final = existing.concat(new)
        req.to_obj.storage.get_memory_api().put(req.to_obj, final)


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
