from __future__ import annotations

from dcp.storage.database.api import DatabaseStorageApi
from dcp.storage.database.engines.postgres import PostgresDatabaseApi

BIGQUERY_SUPPORTED = False
try:
    import sqlalchemy_bigquery

    BIGQUERY_SUPPORTED = True
except ImportError:
    pass


class BigQueryDatabaseApi(PostgresDatabaseApi):
    @classmethod
    def dialect_is_supported(cls) -> bool:
        return BIGQUERY_SUPPORTED


class BigQueryDatabaseStorageApi(DatabaseStorageApi, BigQueryDatabaseApi):
    pass
