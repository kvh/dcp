from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Type

import pytest

from dcp import (
    Storage,
    SqliteStorageEngine,
    PostgresStorageEngine,
    MysqlStorageEngine,
    LocalFileSystemStorageEngine,
    LocalPythonStorageEngine,
    DatabaseStorageApi,
    FileSystemStorageApi,
    PythonStorageApi,
    FullPath,
    StorageObject,
)
from dcp.storage.database import (
    PostgresDatabaseStorageApi,
    MysqlDatabaseStorageApi,
    DatabaseApi,
)
from dcp.utils.common import rand_str


def test_storage():
    s = Storage("sqlite://")
    assert s.storage_engine is SqliteStorageEngine
    s = Storage("postgresql://localhost")
    assert s.storage_engine is PostgresStorageEngine
    s = Storage("mysql://localhost")
    assert s.storage_engine is MysqlStorageEngine
    s = Storage("file:///")
    assert s.storage_engine is LocalFileSystemStorageEngine
    s = Storage("python://")
    assert s.storage_engine is LocalPythonStorageEngine


def test_storage_api():
    s = Storage("sqlite://").get_api()
    assert isinstance(s, DatabaseStorageApi)
    s = Storage("postgresql://localhost").get_api()
    assert isinstance(s, PostgresDatabaseStorageApi)
    s = Storage("mysql://localhost").get_api()
    assert isinstance(s, MysqlDatabaseStorageApi)
    s = Storage("file:///").get_api()
    assert isinstance(s, FileSystemStorageApi)
    s = Storage("python://").get_api()
    assert isinstance(s, PythonStorageApi)


@pytest.mark.parametrize(
    "url",
    [
        "sqlite://",
        "postgresql://localhost",
        "mysql://",
    ],
)
def test_database_api_core_operations(url):
    s: Storage = Storage(url)
    api_cls: Type[DatabaseApi] = s.storage_engine.get_api_cls()
    if not s.get_api().dialect_is_supported():
        return
    with api_cls.temp_local_database() as db_url:
        s = Storage(db_url)
        api = s.get_database_api()
        name = "_test"
        api.execute_sql(f"create table {name} as select 1 a, 2 b")
        assert api.exists(name)
        assert not api.exists(name + "doesntexist")
        assert api.record_count(name) == 1
        api.create_alias(name, name + "alias")
        assert api.record_count(name + "alias") == 1
        api.copy(name, name + "copy")
        assert api.record_count(name + "copy") == 1
        # Test path
        if url.startswith("postgres"):
            schema = f"_test_schema_{rand_str().lower()}"
            try:
                api.execute_sql(f"create schema {schema}")
                api.execute_sql(f"create table {schema}.{name} as select 1 a, 2 b")
                pth = FullPath(name, [schema])
                assert api.exists(pth)
                assert api.exists(StorageObject(storage=s, full_path=pth))
            finally:
                api.execute_sql(f"drop schema {schema} cascade")


@pytest.mark.parametrize(
    "url",
    [
        f"file://{tempfile.mkdtemp()}",
    ],
)
def test_filesystem_api_core_operations(url):
    s = Storage(url)
    api: FileSystemStorageApi = s.get_filesystem_api()
    name = "_test"
    pth = os.path.join(url[7:], name)
    with open(pth, "w") as f:
        f.writelines(["f1,f2\n", "1,2\n"])
    assert api.exists(name)
    assert not api.exists(name + "doesntexist")
    assert api.record_count(name) == 2
    api.create_alias(name, name + "alias")
    assert api.record_count(name + "alias") == 2
    api.copy(name, name + "copy")
    assert api.record_count(name + "copy") == 2
    # Test path
    name = "_test2"
    dirs = ["dir1", "dir2"]
    dir_pth = os.path.join(url[7:], *dirs)
    os.makedirs(dir_pth)
    pth = Path(dir_pth) / name
    with open(pth, "w") as f:
        f.writelines(["f1,f2\n", "1,2\n"])
    full_pth = FullPath(name, dirs)
    assert api.exists(full_pth)
    assert api.exists(StorageObject(storage=s, full_path=full_pth))


@pytest.mark.parametrize(
    "url",
    [
        "python://",
    ],
)
def test_python_api_core_operations(url):
    api = Storage(url).get_memory_api()
    name = "_test"
    api.put(name, [{"a": 1}, {"b": 2}])
    assert api.exists(name)
    assert not api.exists(name + "doesntexist")
    assert api.record_count(name) == 2
    api.create_alias(name, name + "alias")
    assert api.record_count(name + "alias") == 2
    api.copy(name, name + "copy")
    assert api.record_count(name + "copy") == 2
