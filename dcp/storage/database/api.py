from __future__ import annotations
from io import IOBase

import json
import os
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterator,
    Optional,
    Tuple,
    Any,
    List,
    Set,
)

import sqlalchemy
import sqlparse
from commonmodel.base import Schema
from sqlalchemy.exc import OperationalError, ProgrammingError

from dcp.data_format.formats.memory.records import Records
from dcp.storage.base import (
    StorageApi,
    Storage,
    StorageObject,
    FullPath,
    ensure_storage_object,
)
from dcp.storage.database.utils import columns_from_records
from dcp.utils.common import DcpJsonEncoder
from loguru import logger
from sqlalchemy import MetaData
from sqlalchemy.engine import Connection, Engine, Result, Inspector
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.ddl import CreateTable

from dcp.utils.data import conform_records_for_insert

if TYPE_CHECKING:
    pass

# Track what engines we've created for what urls
# so we don't have unnecessary dupes
_sa_engines: Dict[str, Engine] = {}

_sa_table_cache: Dict[Tuple[str, str], sqlalchemy.Table] = {}


def default_json_serializer(o: Any) -> Any:
    return json.dumps(o, cls=DcpJsonEncoder)


def dispose_all(keyword: Optional[str] = None):
    for k, e in _sa_engines.items():
        if keyword:
            if keyword not in str(e.url):
                continue
        e.dispose()


def get_engine_key(url: str, serializer_class_name: str) -> str:
    return f"{url}_{serializer_class_name}"


def get_engine(
    url: str, json_serializer: Callable = default_json_serializer
) -> sqlalchemy.engine.Engine:
    key = get_engine_key(url, repr(json_serializer))
    if key in _sa_engines:
        return _sa_engines[key]
    kwargs = dict(echo=False)
    if json_serializer:
        kwargs["json_serializer"] = json_serializer
    eng = sqlalchemy.create_engine(url, **kwargs)
    _sa_engines[key] = eng
    return eng


class DatabaseApi:
    def __init__(
        self,
        url: str,
        json_serializer: Callable = None,
    ):
        self.url = url
        self.storage = Storage(url)
        self.json_serializer = (
            json_serializer if json_serializer is not None else default_json_serializer
        )
        self.eng: Optional[sqlalchemy.engine.Engine] = None

    def _get_engine_key(self) -> str:
        return get_engine_key(self.url, self.json_serializer.__class__.__name__)

    def get_engine(self) -> sqlalchemy.engine.Engine:
        self.eng = get_engine(self.url, self.json_serializer)
        return self.eng

    @classmethod
    def dialect_is_supported(cls) -> bool:
        return True

    def get_quoted_identifier(self, identifier: str) -> str:
        # TODO: actually use this
        return self.get_engine().dialect.identifier_preparer.quote(identifier)

    def get_placeholder_char(self) -> str:
        return "?"

    def get_default_storage_path(self) -> list[str]:
        raise NotImplementedError

    @contextmanager
    def connection(self) -> Iterator[Connection]:
        with self.get_engine().connect() as conn:
            yield conn

    def clean_sql(self, sql: str) -> str:
        sql = sqlparse.format(sql, strip_comments=True).strip()
        return self._dbapi_escape_sql(sql)

    def _dbapi_escape_sql(self, sql: str) -> str:
        # Escape % because they are treated as string formatting chars by dbapi
        return sql.replace("%", "%%")

    def execute_sql(self, sql: str) -> Result:
        """Executes all statements in `sql` string"""
        logger.debug("Executing SQL:")
        logger.debug(sql)
        with self.connection() as conn:
            sql = self.clean_sql(sql)
            for stmt in sqlparse.split(sql):
                return conn.execute(stmt)

    @contextmanager
    def execute_sql_result(self, sql: str) -> Iterator[Result]:
        logger.debug("Executing SQL:")
        logger.debug(sql)
        with self.connection() as conn:
            sql = self.clean_sql(sql)
            res = conn.execute(sql)
            yield res

    def execute_sa_statement(self, sa_stmt) -> Result:
        sql = sa_stmt.compile(dialect=self.get_engine().dialect)
        return self.execute_sql(str(sql))

    _q = get_quoted_identifier

    # def ensure_table(self, name: str, schema: Schema) -> str:
    #     if self.exists(name):
    #         return name
    #     ddl = SchemaMapper().create_table_statement(
    #         schema=schema, dialect=self.get_engine().dialect, table_name=name,
    #     )
    #     self.execute_sql(ddl)
    #     return name

    ### StorageApi implementations ###

    def _create_alias(self, obj: StorageObject, alias_obj: StorageObject):
        self._remove_alias(alias_obj)
        self.execute_sql(
            f"create view {alias_obj.formatted_full_name} as select * from {obj.formatted_full_name}"
        )

    def _remove_alias(self, obj: StorageObject):
        self.execute_sql(f"drop view if exists {obj.formatted_full_name}")

    def _exists(self, obj: StorageObject) -> bool:
        # Hacky fallback, dialects should implement their own versions
        try:
            self.execute_sql(f"select * from {obj.formatted_full_name} limit 0")
            return True
        except (OperationalError, ProgrammingError) as x:
            s = str(x).lower()
            if "does not exist" in s or "no such" in s or "doesn't exist" in s:
                return False
            raise x

    def _record_count(self, obj: StorageObject) -> Optional[int]:
        return self.count(obj)

    def count(self, obj: StorageObject) -> int:
        with self.execute_sql_result(
            f"select count(*) from {obj.formatted_full_name}"
        ) as res:
            row = res.fetchone()
        return row[0]

    def _copy(self, obj: StorageObject, to_obj: StorageObject):
        self.execute_sql(
            f"create table {to_obj.formatted_full_name} as select * from {obj.formatted_full_name}"
        )

    def _remove(self, obj: StorageObject):
        self.execute_sql(f"drop table if exists {obj.formatted_full_name} cascade")

    def format_full_path(self, full_path: FullPath) -> str:
        return ".".join(self._q(p) for p in full_path.as_list())

    ### END StorageApi implementations ###

    def rename_table(
        self,
        table: str | FullPath | StorageObject,
        new_table: str | FullPath | StorageObject,
    ):
        obj = ensure_storage_object(table, storage=self.storage)
        to_obj = ensure_storage_object(new_table, storage=Storage(self.url))
        self.execute_sql(
            f"alter table {obj.formatted_full_name} rename to {to_obj.formatted_full_name}"
        )

    def get_schemas_and_table_names(self) -> Dict[str, Set[str]]:
        inspector: Inspector = sqlalchemy.inspect(self.get_engine())
        schemas_to_tables = {}
        for schema in inspector.get_schema_names():
            schemas_to_tables[schema] = set(inspector.get_table_names(schema))
        return schemas_to_tables

    def clean_sub_sql(self, sql: str) -> str:
        return sql.strip(" ;")

    def insert_sql(
        self,
        sess: Session,
        table: str | FullPath | StorageObject,
        sql: str,
        schema: Schema,
    ):
        obj = ensure_storage_object(table, self.storage)
        sql = self.clean_sub_sql(sql)
        columns = "\n,".join(self._q(f.name) for f in schema.fields)
        insert_sql = f"""
        insert into {obj.formatted_full_name} (
            {columns}
        )
        select
        {columns}
        from (
        {sql}
        ) as __sub
        """
        self.execute_sql(insert_sql)

    def create_table_from_sql(
        self,
        table: str | FullPath | StorageObject,
        sql: str,
    ):
        obj = ensure_storage_object(table, self.storage)
        sql = self.clean_sub_sql(sql)
        create_sql = f"""
        create table {obj.formatted_full_name} as
        select
        *
        from (
        {sql}
        ) as __sub
        """
        self.execute_sql(create_sql)

    def get_as_sqlalchemy_table(
        self,
        table: str | FullPath | StorageObject,
    ) -> sqlalchemy.Table:
        table = ensure_storage_object(table, self.storage)
        full_name = table.formatted_full_name
        # Use caching since this is a very expensive operation in some instances
        if (self.url, full_name) not in _sa_table_cache:
            sa_table = sqlalchemy.Table(
                table.full_path.name,
                MetaData(),
                # self.get_sqlalchemy_metadata(),
                autoload=True,
                autoload_with=self.get_engine(),
                schema=table.full_path.get_last_path_element(),
            )
            _sa_table_cache[(self.url, full_name)] = sa_table
        return _sa_table_cache[(self.url, full_name)]

    def create_sqlalchemy_table(self, table: sqlalchemy.Table):
        # table.metadata = self.get_sqlalchemy_metadata()
        table.metadata = MetaData()
        stmt = CreateTable(table).compile(dialect=self.get_engine().dialect)
        self.execute_sql(str(stmt))

    def bulk_insert_file(
        self,
        table: str | FullPath | StorageObject,
        f: IOBase,
        schema: Optional[Schema] = None,
    ):
        table = ensure_storage_object(table, storage=self.storage)
        self._bulk_insert_file(table, f, schema)

    def _bulk_insert_file(
        self, table: StorageObject, f: IOBase, schema: Optional[Schema] = None
    ):
        raise NotImplementedError

    def bulk_insert_records(
        self,
        table: str | FullPath | StorageObject,
        records: Records,
        schema: Optional[Schema] = None,
    ):
        table = ensure_storage_object(table, self.storage)
        # Create table whether or not there is anything to insert (side-effect consistency)
        # TODO: is it right to create the table? Seems useful to have an "empty" datablock, for instance.
        assert self._exists(table)
        # self.ensure_table(name, schema=schema)
        if not records:
            return
        self._bulk_insert(table, records, schema)

    def conform_records_for_insert(
        self,
        records: List[Dict],
        columns: List[str],
        adapt_objects_to_json: bool = True,
        conform_datetimes: bool = True,
        replace_nones: bool = False,
        as_dicts: bool = False,
    ):
        return conform_records_for_insert(
            records,
            columns,
            adapt_objects_to_json,
            conform_datetimes,
            replace_nones,
            as_dicts,
        )

    def _bulk_insert(
        self, table: StorageObject, records: Records, schema: Optional[Schema] = None
    ):
        if schema is None:
            schema = table.get_schema()
        if schema:
            columns = schema.field_names()
        else:
            columns = columns_from_records(records)
        records = self.conform_records_for_insert(records, columns)
        quoted_cols = [self.get_quoted_identifier(c) for c in columns]
        placeholders = [self.get_placeholder_char()] * len(columns)
        sql = f"""
        INSERT INTO {table.formatted_full_name} (
            {','.join(quoted_cols)}
        ) VALUES ({','.join(placeholders)})
        """
        conn = self.get_engine().raw_connection()
        curs = conn.cursor()
        try:
            curs.executemany(sql, records)
            conn.commit()
        finally:
            conn.close()

    def get_sqlalchemy_metadata(self):
        sa_engine = self.get_engine()
        meta = MetaData()
        meta.reflect(bind=sa_engine)
        return meta

    @classmethod
    @contextmanager
    def temp_local_database(cls, conn_url: str = None, **kwargs) -> Iterator[str]:
        raise NotImplementedError


class DatabaseStorageApi(DatabaseApi, StorageApi):
    def __init__(
        self,
        storage: Storage,
    ):
        super().__init__(storage.url)
        self.storage = storage


def create_db(url: str, database_name: str):
    if url.startswith("sqlite"):
        logger.info("create_db is no-op for sqlite")
        return
    sa = get_engine(url)
    conn = sa.connect()
    try:
        conn.execute(
            "commit"
        )  # Close default open transaction (can't create db inside tx)
        conn.execute(f"create database {database_name}")
    finally:
        conn.close()
        sa.dispose()


def drop_db(url: str, database_name: str, force: bool = False):
    if url.startswith("sqlite"):
        return drop_sqlite_db(url, database_name)
    if "test" not in database_name and "tmp" not in database_name and not force:
        i = input(f"Dropping db {database_name}, are you sure? (y/N)")
        if not i.lower().startswith("y"):
            return
    sa = get_engine(url)
    conn = sa.connect()
    try:
        conn.execute(
            "commit"
        )  # Close default open transaction (can't drop db inside tx)
        conn.execute(f"drop database {database_name}")
    finally:
        conn.close()
        sa.dispose()


def drop_sqlite_db(url: str, database_name: str):
    if database_name == ":memory:":
        return
    db_path = url[10:]
    if not db_path:
        # Empty sqlite url (`sqlite://`)
        return
    os.remove(db_path)
