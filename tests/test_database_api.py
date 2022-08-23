from __future__ import annotations

import warnings
from typing import Type

import pytest

from dcp.storage.base import Storage
from dcp.storage.database.api import DatabaseApi, DatabaseStorageApi
from dcp.storage.database.engines.bigquery import BIGQUERY_SUPPORTED
from tests.utils import bigquery_url

urls = ["sqlite://", "postgresql://localhost", "mysql://root@localhost"]
if BIGQUERY_SUPPORTED and bigquery_url:
    urls.append(bigquery_url)


@pytest.mark.parametrize("url", urls)
def test_db_to_db(url):
    s: Storage = Storage.from_url(url)
    api_cls: Type[DatabaseApi] = s.storage_engine.get_api_cls()
    if not s.get_api().dialect_is_supported():
        warnings.warn(
            f"Skipping tests for database engine {s.storage_engine.__name__} (client library not installed)"
        )
        return
    with api_cls.temp_local_database(url) as db_url:
        name = "_test"
        storage = Storage.from_url(db_url)
        api = storage.get_database_api()

        assert not api.exists("madeuptable")
        api.execute_sql(f"create table {name} as select 'hi' a, 2 b")
        assert api.exists(name)

        assert api.record_count(name) == 1

        api.copy(name, name + "copy")
        assert api.exists(name + "copy")

        api.create_alias(name, name + "alias")
        assert api.exists(name + "alias")

        api.remove_alias(name + "alias")
        assert not api.exists(name + "alias")

        # Test % escaping
        with api.execute_sql_result(
            f"select '%%' as a from {name} where a like 'h%'"
        ) as r:
            assert list(r)[0][0] == "%%"
        api.execute_sql("select '%%' as a")

        api.remove(name)
        assert not api.exists(name)

        default = api.get_default_storage_path()
        assert default is not None
