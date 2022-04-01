from typing import Iterator, Dict

from commonmodel.base import Schema
from dcp.data_copy.base import CopyRequest, DataCopierBase
from dcp.data_copy.costs import (
    FormatConversionCost,
    MemoryToMemoryCost,
    NetworkToMemoryCost,
)
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.data_format.formats.memory.records_iterator import (
    RecordsIterator,
    RecordsIteratorFormat,
)
from dcp.storage.base import (
    DatabaseStorageClass,
    MemoryStorageClass,
    StorageApi,
)
from dcp.storage.database.api import DatabaseStorageApi
from dcp.storage.database.utils import result_proxy_to_records
from dcp.storage.memory.engines.python import PythonStorageApi
from sqlalchemy.engine import Result


class DatabaseToMemoryMixin:
    from_storage_classes = [DatabaseStorageClass]
    to_storage_classes = [MemoryStorageClass]

    def append(self, req: CopyRequest):
        assert isinstance(req.from_storage_api, DatabaseStorageApi)
        assert isinstance(req.to_storage_api, PythonStorageApi)
        existing = req.to_storage_api.get(req.to_name)
        quoted_from_name = req.from_storage_api.get_quoted_identifier(req.from_name)
        select_sql = f"select * from {quoted_from_name}"
        with req.from_storage_api.execute_sql_result(select_sql) as r:
            new = self.result_to_object(r)
        final = self.concat(existing, new)
        req.to_storage_api.put(req.to_name, final)

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
        assert isinstance(req.from_storage_api, DatabaseStorageApi)
        assert isinstance(req.to_storage_api, PythonStorageApi)
        existing = req.to_storage_api.get(req.to_name)
        quoted_from_name = req.from_storage_api.get_quoted_identifier(req.from_name)
        select_sql = f"select * from {quoted_from_name}"
        conn = req.from_storage_api.get_engine().connect()
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
        req.to_storage_api.put(req.to_name, final)


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
