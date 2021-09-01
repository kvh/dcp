from __future__ import annotations
from dcp.utils.data import read_json
from typing import Any
from dcp.storage.base import NameDoesNotExistError, Storage, StorageApi
from dcp.storage.database.api import DatabaseStorageApi
import sqlalchemy as sa


class KeyValueStorageApi(StorageApi):
    def get(self, name: str) -> Any:
        raise NotImplementedError

    def put(self, name: str, records_obj: Any):
        raise NotImplementedError


class KeyValueDatabaseStorageApi(KeyValueStorageApi):
    def __init__(self, storage: Storage, kv_store_name: str = "_kvstore"):
        super().__init__(storage)
        assert storage.url.startswith("kv+")
        db_url = storage.url[3:]
        self.db_api = Storage(db_url).get_api()
        self.kv_store_name = kv_store_name

    def get_kv_table(self) -> sa.Table:
        return sa.Table(
            self.kv_store_name,
            sa.MetaData(),
            sa.Column("key", sa.Unicode(256)),
            sa.Column("value", sa.JSON()),
        )

    def ensure_kv_store(self) -> sa.Table:
        # TODO: expensive check?
        table = self.get_kv_table()
        if self.db_api.exists(self.kv_store_name):
            return table
        self.db_api.create_sqlalchemy_table(table)
        return table

    def record_count(self, name: str) -> Optional[int]:
        return len(self.get(name))

    def exists(self, name: str) -> bool:
        with self.db_api.execute_sql_result(
            f"select key from {self.kv_store_name} where key='{name}'"
        ) as r:
            return r.scalar_one_or_none() is not None

    def remove(self, name: str) -> bool:
        self.db_api.execute_sql(f"delete from {self.kv_store_name} where key='{name}'")

    def get(self, name: str) -> Any:
        with self.db_api.execute_sql_result(
            f"select value from {self.kv_store_name} where key='{name}'"
        ) as r:
            j = r.scalar_one_or_none()
            if j is None:
                raise NameDoesNotExistError(f"{name} on {self.storage}")
            return read_json(j)

    def put(self, name: str, records_obj: Any):
        table = self.ensure_kv_store()
        self.remove(name)
        stmt = table.insert().values(key=name, value=records_obj)
        with self.db_api.connection() as conn:
            return conn.execute(stmt)
