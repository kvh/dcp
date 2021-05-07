from __future__ import annotations

from dcp.storage.database.api import DatabaseStorageApi
from dcp.storage.database.engines.postgres import PostgresDatabaseApi

REDSHIFT_SUPPORTED = False
try:
    import sqlalchemy_redshift

    REDSHIFT_SUPPORTED = True
except ImportError:
    pass


class RedshiftDatabaseApi(PostgresDatabaseApi):
    @classmethod
    def dialect_is_supported(cls) -> bool:
        return REDSHIFT_SUPPORTED


class RedshiftDatabaseStorageApi(DatabaseStorageApi, RedshiftDatabaseApi):
    pass
