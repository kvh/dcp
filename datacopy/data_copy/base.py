from __future__ import annotations
from datacopy.data_format.handler import (
    ALL_HANDLERS,
    FormatHandler,
    get_format_for_name,
    get_handler,
    get_handler_for_name,
)
from datacopy.data_copy.costs import DataCopyCost

from openmodel.base import AnySchema, Schema
from datacopy.data_format.base import DataFormat
from datacopy.storage.base import Storage, StorageApi, StorageClass, StorageEngine

import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import (
    Any,
    TYPE_CHECKING,
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


@dataclass(frozen=True)
class CopyRequest:
    from_name: str
    from_storage: Storage
    to_name: str
    to_format: DataFormat
    to_storage: Storage
    schema: Schema = None
    # available_storages # TODO
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
        return get_handler(self.to_format, self.to_storage.storage_engine)()

    @property
    def from_format(self) -> DataFormat:
        return get_format_for_name(self.from_name, self.from_storage)

    @property
    def conversion(self) -> Conversion:
        return Conversion(
            from_storage_format=StorageFormat(
                storage_engine=self.from_storage.storage_engine,
                data_format=self.from_format,
            ),
            to_storage_format=StorageFormat(
                storage_engine=self.to_storage.storage_engine,
                data_format=self.to_format,
            ),
        )

    @property
    def available_storages(self) -> List[Storage]:
        return [self.from_storage, self.to_storage]


CopierCallabe = Callable[[CopyRequest], None]


@dataclass(frozen=True)
class DataCopier:
    cost: DataCopyCost
    copier_function: CopierCallabe
    from_storage_classes: Optional[List[Type[StorageClass]]] = None
    from_storage_engines: Optional[List[Type[StorageEngine]]] = None
    from_data_formats: Optional[List[DataFormat]] = None
    to_storage_classes: Optional[List[Type[StorageClass]]] = None
    to_storage_engines: Optional[List[Type[StorageEngine]]] = None
    to_data_formats: Optional[List[DataFormat]] = None

    def copy(self, request: CopyRequest):
        self.copier_function(request)

    __call__ = copy

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


ALL_DATA_COPIERS = []


def datacopy(
    cost: DataCopyCost,
    from_storage_classes: Optional[List[Type[StorageClass]]] = None,
    from_storage_engines: Optional[List[Type[StorageEngine]]] = None,
    from_data_formats: Optional[List[DataFormat]] = None,
    to_storage_classes: Optional[List[Type[StorageClass]]] = None,
    to_storage_engines: Optional[List[Type[StorageEngine]]] = None,
    to_data_formats: Optional[List[DataFormat]] = None,
    unregistered: bool = False,
):
    def f(copier_function: CopierCallabe) -> DataCopier:
        dc = DataCopier(
            copier_function=copier_function,
            cost=cost,
            from_storage_classes=from_storage_classes,
            from_storage_engines=from_storage_engines,
            from_data_formats=from_data_formats,
            to_storage_classes=to_storage_classes,
            to_storage_engines=to_storage_engines,
            to_data_formats=to_data_formats,
        )
        if not unregistered:
            ALL_DATA_COPIERS.append(dc)
        return dc

    return f

