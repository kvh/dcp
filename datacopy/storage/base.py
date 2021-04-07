from __future__ import annotations

import enum
import os
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type
from urllib.parse import urlparse

if TYPE_CHECKING:
    from datacopy.data_format.base import DataFormat


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
        from datacopy.storage.database.api import DatabaseStorageApi

        return DatabaseStorageApi


class MemoryStorageClass(StorageClass):
    # natural_format = RecordsFormat
    # supported_formats = [DataFormatBase]

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from datacopy.storage.memory.engines.python import PythonStorageApi

        return PythonStorageApi


class FileSystemStorageClass(StorageClass):
    # natural_format = ArrowFileFormat
    # supported_formats = [FileDataFormatBase]

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from datacopy.storage.file_system.engines.local import FileSystemStorageApi

        return FileSystemStorageApi


class StorageEngine:
    storage_class: Type[StorageClass]
    schemes: List[str] = []

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
        from datacopy.data_format.base import ALL_DATA_FORMATS

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
        raise NotImplementedError
        # return cls.storage_class.natural_format


class SqliteStorageEngine(StorageEngine):
    storage_class = DatabaseStorageClass
    schemes = ["sqlite"]

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from datacopy.storage.database.engines.sqlite import SqliteDatabaseStorageApi

        return SqliteDatabaseStorageApi


class PostgresStorageEngine(StorageEngine):
    storage_class = DatabaseStorageClass
    schemes = ["postgres", "postgresql"]

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from datacopy.storage.database.engines.postgres import (
            PostgresDatabaseStorageApi,
        )

        return PostgresDatabaseStorageApi


class MysqlStorageEngine(StorageEngine):
    storage_class = DatabaseStorageClass
    schemes = ["mysql"]

    @classmethod
    def get_api_cls(cls) -> Type[StorageApi]:
        from datacopy.storage.database.engines.mysql import MysqlDatabaseStorageApi

        return MysqlDatabaseStorageApi


class LocalFileSystemStorageEngine(StorageEngine):
    storage_class = FileSystemStorageClass
    schemes = ["file"]


class LocalPythonStorageEngine(StorageEngine):
    storage_class = MemoryStorageClass
    schemes = ["python"]


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


class NameDoesNotExistError(Exception):
    pass


class StorageApi:
    def __init__(self, storage: Storage):
        self.storage = storage

    def exists(self, name: str) -> bool:
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
