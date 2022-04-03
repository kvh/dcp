from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

import networkx as nx
from commonmodel.base import Schema
from dcp.data_copy.costs import DataCopyCost
from dcp.data_format.base import DataFormat
from dcp.data_format.handler import (
    ALL_HANDLERS,
    FormatHandler,
    get_handler,
    get_handler_for_name,
    infer_format_for_name,
)
from dcp.storage.base import Storage, StorageApi, StorageClass, StorageEngine
from dcp.storage.memory.engines.python import DEFAULT_PYTHON_STORAGE


@dataclass(frozen=True)
class StorageFormat:
    storage_engine: Type[StorageEngine]
    data_format: DataFormat

    def __str__(self):
        return f"{self.storage_engine.__name__}:{self.data_format.__name__}"


@dataclass(frozen=True)
class Conversion:
    from_storage_format: StorageFormat
    to_storage_format: StorageFormat


@dataclass
class CopyRequest:
    from_name: str
    from_storage: Storage
    to_name: str
    to_storage: Storage
    to_format: Optional[DataFormat] = None
    schema: Optional[Schema] = None
    available_storages: Optional[List[Storage]] = None
    if_exists: str = "error"  # in {"error", "append", "replace"}
    delete_intermediate: bool = False
    _from_format: Optional[DataFormat] = None
    # handlers: List[Type[FormatHandler]] = ALL_HANDLERS

    @property
    def from_storage_api(self) -> StorageApi:
        return self.from_storage.get_api()

    @property
    def to_storage_api(self) -> StorageApi:
        return self.to_storage.get_api()

    @property
    def from_format_handler(self) -> FormatHandler:
        return get_handler(self.from_format, self.from_storage.storage_engine)()

    @property
    def to_format_handler(self) -> FormatHandler:
        return get_handler(self.get_to_format(), self.to_storage.storage_engine)()

    @property
    def from_format(self) -> DataFormat:
        if self._from_format:
            return self._from_format
        return infer_format_for_name(self.from_name, self.from_storage)

    @property
    def conversion(self) -> Conversion:
        return Conversion(
            from_storage_format=StorageFormat(
                storage_engine=self.from_storage.storage_engine,
                data_format=self.from_format,
            ),
            to_storage_format=StorageFormat(
                storage_engine=self.to_storage.storage_engine,
                data_format=self.get_to_format(),
            ),
        )

    def get_available_storages(self) -> List[Storage]:
        return list(
            set(
                [self.from_storage, self.to_storage]
                + (self.available_storages or [DEFAULT_PYTHON_STORAGE])
            )
        )

    def get_schema(self) -> Schema:
        if self.schema is None:
            handler = self.from_format_handler
            self.schema = handler.infer_schema(self.from_name, self.from_storage)
        return self.schema

    def get_to_format(self) -> DataFormat:
        if self.to_format is None:
            self.to_format = self.to_storage.storage_engine.get_natural_format()
        return self.to_format


CopierCallabe = Callable[[CopyRequest], None]


class NameExistsError(Exception):
    pass


class DataCopierBase:
    cost: DataCopyCost
    requires_schema_cast: bool
    from_storage_classes: Optional[List[Type[StorageClass]]] = None
    from_storage_engines: Optional[List[Type[StorageEngine]]] = None
    from_data_formats: Optional[List[DataFormat]] = None
    to_storage_classes: Optional[List[Type[StorageClass]]] = None
    to_storage_engines: Optional[List[Type[StorageEngine]]] = None
    to_data_formats: Optional[List[DataFormat]] = None
    supports_append: bool = True
    request: CopyRequest
    unregistered: bool = False

    def __init_subclass__(cls) -> None:
        if not cls.unregistered:
            ALL_DATA_COPIERS.append(cls())

    def __eq__(self, o: object) -> bool:
        return o.__class__ is self.__class__

    def create_empty(self, req: CopyRequest):
        create_empty_if_not_exists(req)
        # raise NotImplementedError

    def append(self, req: CopyRequest):
        raise NotImplementedError

    def copy(self, req: CopyRequest):
        self.check_if_exists(req)
        self.create_empty(req)
        self.append(req)
        if self.requires_schema_cast:
            self.cast_to_schema(req)

    def cast_to_schema(self, req: CopyRequest):
        req.to_format_handler.cast_to_schema(
            req.to_name, req.to_storage_api.storage, req.get_schema()
        )

    def check_if_exists(self, req: CopyRequest):
        if req.if_exists == "replace":
            return
        exists = req.to_storage.get_api().exists(req.to_name)
        if not exists:
            return
        if req.if_exists == "error":
            raise NameExistsError(
                f"{req.to_name} already exists on {req.to_storage} (if_exists=='error')"
            )
        elif req.if_exists == "append":
            if not self.supports_append:
                raise NameExistsError(
                    f"{req.to_name} already exists on {req.to_storage}, and append not supported (if_exists=='append')"
                )

    def can_handle_from(self, from_storage_format: StorageFormat) -> bool:
        if self.from_storage_classes:
            if (
                from_storage_format.storage_engine.storage_class
                not in self.from_storage_classes
            ):
                return False
        if self.from_storage_engines:
            if from_storage_format.storage_engine not in self.from_storage_engines:
                return False
        if self.from_data_formats:
            if from_storage_format.data_format not in self.from_data_formats:
                return False
        return True

    def can_handle_to(self, to_storage_format: StorageFormat) -> bool:
        if self.to_storage_classes:
            if (
                to_storage_format.storage_engine.storage_class
                not in self.to_storage_classes
            ):
                return False
        if self.to_storage_engines:
            if to_storage_format.storage_engine not in self.to_storage_engines:
                return False
        if self.to_data_formats:
            if to_storage_format.data_format not in self.to_data_formats:
                return False
        return True

    def can_handle(self, conversion: Conversion) -> bool:
        return self.can_handle_from(
            conversion.from_storage_format
        ) and self.can_handle_to(conversion.to_storage_format)


# Helper (belongs somewhere else?)
def create_empty_if_not_exists(req: CopyRequest):
    exists = req.to_storage_api.exists(req.to_name)
    if exists and req.if_exists == "replace":
        req.to_storage_api.remove(req.to_name)
        exists = False
    if not exists:
        req.to_format_handler.create_empty(
            req.to_name, req.to_storage_api.storage, req.get_schema()
        )


ALL_DATA_COPIERS = []


def copy(
    from_name: str,
    from_storage: Storage,
    to_name: str,
    to_storage: Storage,
    to_format: DataFormat = None,
    schema: Optional[Schema] = None,
    available_storages: Optional[List[Storage]] = None,
    if_exists: str = "error",
    delete_intermediate: bool = False,
    from_format: Optional[DataFormat] = None,
):
    from dcp.data_copy.graph import execute_copy_request

    return execute_copy_request(
        CopyRequest(
            from_name=from_name,
            from_storage=from_storage,
            to_name=to_name,
            to_storage=to_storage,
            to_format=to_format,
            schema=schema,
            available_storages=available_storages,
            if_exists=if_exists,
            delete_intermediate=delete_intermediate,
            _from_format=from_format,
        )
    )
