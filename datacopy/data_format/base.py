from __future__ import annotations

import typing
from collections import abc
from copy import deepcopy
from itertools import tee
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Type,
    TypeVar,
)

if TYPE_CHECKING:
    from datacopy.storage.base import StorageClass, StorageEngine

T = TypeVar("T")

ALL_DATA_FORMATS = []


class DataFormatBase(Generic[T]):
    natural_storage_class: Type[StorageClass]
    natural_storage_engine: Optional[Type[StorageEngine]] = None
    storable: bool = True
    nickname: str = None

    def __init__(self):
        raise NotImplementedError("Do not instantiate DataFormat classes")

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        ALL_DATA_FORMATS.append(cls)

    @classmethod
    def get_natural_storage_class(cls) -> Type[StorageClass]:
        return cls.natural_storage_class

    @classmethod
    def get_natural_storage_engine(cls) -> Optional[Type[StorageEngine]]:
        return cls.natural_storage_engine

    @classmethod
    def is_storable(cls) -> bool:
        # Does the format store data in a stable, serializable format?
        # Examples that arent' storable: generators, file-like buffers, and
        # db cursors -- they depend on open in-memory resources that may go away
        return cls.storable


class IterableDataFormatBase(DataFormatBase[T]):
    inner_format: DataFormat


DataFormat = Type[DataFormatBase]
IterableDataFormat = Type[IterableDataFormatBase]
