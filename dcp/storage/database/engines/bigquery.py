from __future__ import annotations

import dataclasses
from contextlib import contextmanager
from io import IOBase
from typing import Iterator, Optional

import sqlalchemy
from commonmodel import Schema, FieldType
from sqlalchemy.ext.compiler import compiles
from sqlalchemy_bigquery.parse_url import parse_url

from dcp import Records, field_type_to_sqlalchemy_type
from dcp.storage.base import StorageObject, FullPath
from dcp.storage.database import columns_from_records
from dcp.storage.database.api import DatabaseStorageApi, DatabaseApi, get_engine
from dcp.utils.common import rand_str, to_json
from dcp.utils.data import conform_records_for_insert

BIGQUERY_SUPPORTED = False
try:
    import sqlalchemy_bigquery

    BIGQUERY_SUPPORTED = True
except ImportError:
    pass


_engine_cache = {}


class BigQueryDatabaseApi(DatabaseApi):
    def get_placeholder_char(self) -> str:
        return "%s"

    def get_engine(self) -> sqlalchemy.engine.Engine:
        if (eng := _engine_cache.get(self.url)) is not None:
            return eng

        self.eng = sqlalchemy.create_engine(self.url)
        _engine_cache[self.url] = self.eng
        return self.eng

    def get_default_storage_path(self) -> list[str]:
        path_elements = self._get_path_elements()
        return [path_elements["project_id"], path_elements["dataset_id"]]

    def _get_path_elements(self) -> dict[str, str]:
        (project_id, location, dataset_id, *rest) = parse_url(self.get_engine().url)
        return {"project_id": project_id, "dataset_id": dataset_id}

    def _ensure_fully_qualified_path(self, path: FullPath) -> FullPath:
        """Ensure path has both project_id and dataset_id in it"""
        path_elements = self._get_path_elements()
        if path.path:
            if len(path.path) == 2:
                # already fully qualified
                return path
            elif len(path.path) == 1:
                # If one element in path, then assume it is dataset_id and add the project id
                path = FullPath(
                    name=path.name, path=[path_elements["project_id"], path.path[0]]
                )
        else:
            # No path so use both project and dataset
            path = FullPath(
                name=path.name,
                path=[path_elements["project_id"], path_elements["dataset_id"]],
            )
        return path

    def format_full_path(self, full_path: FullPath) -> str:
        # BigQuery requires fully qualified names (project.dataset.table) for some operations
        path = self._ensure_fully_qualified_path(full_path)
        return super().format_full_path(path)

    ### StorageApi implementations ###
    def _exists(self, obj: StorageObject) -> bool:
        # TODO: handle referencing other datasets
        with self.execute_sql_result(
            f"select count(*) from __TABLES__ where table_id = '{obj.full_path.name}'"
        ) as res:
            row = res.fetchone()
        return row[0] == 1

    def _create_alias(self, obj: StorageObject, alias_obj: StorageObject):
        self._remove_alias(alias_obj)
        self.execute_sql(
            f"create view {alias_obj.formatted_full_name} as select * from {obj.formatted_full_name}"
        )

    def _remove(self, obj: StorageObject):
        self.execute_sql(f"drop table if exists {obj.formatted_full_name}")

    @classmethod
    def dialect_is_supported(cls) -> bool:
        return BIGQUERY_SUPPORTED

    def _bulk_insert_file(
        self, table: StorageObject, f: IOBase, schema: Optional[Schema] = None
    ):
        raise Exception("Not implemented")

    def sqlalchemy_type_str_for_field_type(self, field_type: FieldType) -> str:
        sa_type = field_type_to_sqlalchemy_type(field_type)
        sa_dialect_type = sa_type.compile(dialect=self.get_engine().dialect)
        return sa_dialect_type

    def _bulk_insert(
        self, table: StorageObject, records: Records, schema: Optional[Schema] = None
    ):

        # TODO: seems like a lot happening in the method -- column inference, type inference ... schema should be
        #  mandatory arg to force this to happen somewhere else?
        # Bigquery requires column types in the parameter placeholders in order to format value correctly
        col_types = None
        if schema:
            columns = schema.field_names()
            col_types = [
                self.sqlalchemy_type_str_for_field_type(f.field_type)
                for f in schema.fields
            ]
        else:
            columns = columns_from_records(records)
        records = self.conform_records_for_insert(
            records, columns, as_dicts=col_types is not None
        )
        quoted_cols = [self.get_quoted_identifier(c) for c in columns]
        if col_types:
            placeholders = [f"%({c}:{t})s" for c, t in zip(columns, col_types)]
        else:
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

    def conform_records_for_insert(
        self,
        records: list[dict],
        columns: list[str],
        adapt_objects_to_json: bool = True,
        conform_datetimes: bool = True,
        replace_nones: bool = False,
        as_dicts: bool = True,
    ):
        return conform_records_for_insert(
            records,
            columns,
            adapt_objects_to_json,
            conform_datetimes,
            replace_nones,
            as_dicts,
        )

    @classmethod
    @contextmanager
    def temp_local_database(cls, conn_url: str, **kwargs) -> Iterator[str]:
        temp_db = f"__tmp_dcp_{rand_str(8).lower()}"
        create_schema(conn_url, temp_db)
        test_url = f"{conn_url}/{temp_db}"
        try:
            yield test_url
        finally:
            pass
            # dispose_all(temp_db)
            # drop_schema(conn_url, temp_db)


def create_schema(conn_url: str, schema: str):
    sa = get_engine(conn_url, json_serializer=None)
    conn = sa.connect()
    try:
        conn.execute(f"create schema {schema}")
    finally:
        conn.close()
        sa.dispose()


def drop_schema(conn_url: str, schema: str):
    sa = get_engine(conn_url, json_serializer=None)
    conn = sa.connect()
    try:
        conn.execute(f"drop schema {schema}")
    finally:
        conn.close()
        sa.dispose()


class BigQueryDatabaseStorageApi(DatabaseStorageApi, BigQueryDatabaseApi):
    pass
