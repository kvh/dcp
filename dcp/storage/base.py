from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type, Union
from urllib.parse import urlparse

if TYPE_CHECKING:
    from dcp.data_format.base import DataFormat


ALL_STORAGE_CLASSES = []
ALL_STORAGE_ENGINES = []


class StorageClass:
    # natural_format: DataFormat
    # supported_formats: List[DataFormat] = []

    def __init__(self):
        raise NotImplementedError("Do not instantiate StorageClass classes")

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        ALL_STORAGE_CLASSES.append(cls)

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        raise NotImplementedError


class DatabaseStorageClass(StorageClass):
    # natural_format = DatabaseTableFormat
    # supported_formats = [DatabaseTableFormat]

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from dcp.storage.database.api import DatabaseStorageApi

        return DatabaseStorageApi


class MemoryStorageClass(StorageClass):
    # natural_format = RecordsFormat
    # supported_formats = [DataFormatBase]

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from dcp.storage.memory.engines.python import PythonStorageApi

        return PythonStorageApi


class FileSystemStorageClass(StorageClass):
    # natural_format = ArrowFileFormat
    # supported_formats = [FileDataFormatBase]

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from dcp.storage.file_system.engines.local import FileSystemStorageApi

        return FileSystemStorageApi


class StorageEngine:
    storage_class: Type[StorageClass]
    schemes: List[str] = []
    natural_format: str

    def __init__(self):
        raise NotImplementedError("Do not instantiate StorageEngine classes")

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        ALL_STORAGE_ENGINES.append(cls)

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        return cls.storage_class.get_api_cls()

    @classmethod
    def get_supported_formats(cls) -> List[DataFormat]:
        from dcp.data_format.base import ALL_DATA_FORMATS

        fmts = []
        for fmt in ALL_DATA_FORMATS:
            if fmt.natural_storage_class == cls.storage_class:
                if not fmt.natural_storage_engine or fmt.natural_storage_engine == cls:
                    fmts.append(fmt)
        return fmts

    @classmethod
    def is_supported_format(cls, fmt: DataFormat) -> bool:
        for sfmt in cls.get_supported_formats():
            if issubclass(fmt, sfmt):
                return True
        return False

    @classmethod
    def get_natural_format(cls) -> DataFormat:
        from dcp.data_format.base import get_format_for_nickname

        return get_format_for_nickname(cls.natural_format)


class SqliteStorageEngine(StorageEngine):
    storage_class = DatabaseStorageClass
    schemes = ["sqlite"]
    natural_format = "table"

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from dcp.storage.database.engines.sqlite import SqliteDatabaseStorageApi

        return SqliteDatabaseStorageApi


class PostgresStorageEngine(StorageEngine):
    storage_class = DatabaseStorageClass
    schemes = ["postgres", "postgresql"]
    natural_format = "table"

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from dcp.storage.database.engines.postgres import PostgresDatabaseStorageApi

        return PostgresDatabaseStorageApi


class MysqlStorageEngine(StorageEngine):
    storage_class = DatabaseStorageClass
    schemes = ["mysql"]
    natural_format = "table"

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from dcp.storage.database.engines.mysql import MysqlDatabaseStorageApi

        return MysqlDatabaseStorageApi


class RedshiftStorageEngine(StorageEngine):
    storage_class = DatabaseStorageClass
    schemes = ["redshift", "redshift+psycopg2"]
    natural_format = "table"

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from dcp.storage.database.engines.redshift import RedshiftDatabaseStorageApi

        return RedshiftDatabaseStorageApi


#############
# Filesystems
#############


class LocalFileSystemStorageEngine(StorageEngine):
    storage_class = FileSystemStorageClass
    schemes = ["file"]
    natural_format = "jsonl"  # TODO: arrow?

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from dcp.storage.file_system.engines.local import LocalFileSystemStorageApi

        return LocalFileSystemStorageApi


class GoogleCloudStorageEngine(StorageEngine):
    storage_class = FileSystemStorageClass
    schemes = ["gs", "gcs"]
    natural_format = "jsonl"  # TODO: arrow?

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from dcp.storage.file_system.engines.gcs import GoogleCloudStorageApi

        return GoogleCloudStorageApi


class LocalPythonStorageEngine(StorageEngine):
    storage_class = MemoryStorageClass
    schemes = ["python"]
    natural_format = "records"  # TODO: arrow?


def get_engine_for_scheme(scheme: str) -> Type[StorageEngine]:
    # Take first match IN REVERSE ORDER they were added
    # (so an Engine added later - by user perhaps - takes precedence)
    for eng in ALL_STORAGE_ENGINES[::-1]:
        if scheme in eng.schemes:
            return eng
    raise Exception(f"No matching engine for scheme {scheme}")  # TODO


@dataclass(frozen=True)
class Storage:
    url: str

    @classmethod
    def from_url(cls, url: str) -> Storage:
        return Storage(url=url)

    def get_api(self) -> StorageApi:
        return self.storage_engine.get_api_cls()(self)

    @property
    def storage_engine(self) -> Type[StorageEngine]:
        parsed = urlparse(self.url)
        return get_engine_for_scheme(parsed.scheme)


def ensure_storage(s: Union[Storage, str, None]) -> Storage:
    if s is None:
        return None
    if isinstance(s, str):
        s = Storage.from_url(s)
    return s


class NameDoesNotExistError(Exception):
    pass


class StorageApi:
    def __init__(self, storage: Storage):
        self.storage = storage

    def exists(self, name: str) -> bool:
        raise NotImplementedError

    def remove(self, name: str):
        raise NotImplementedError

    def record_count(self, name: str) -> Optional[int]:
        raise NotImplementedError

    def copy(self, name: str, to_name: str):
        raise NotImplementedError

    def create_alias(
        self, name: str, alias: str
    ):  # TODO: rename to overwrite_alias or set_alias?
        raise NotImplementedError

    def remove_alias(self, alias: str):
        raise NotImplementedError
