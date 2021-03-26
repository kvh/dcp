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
    from dcp.storage.base import StorageClass, StorageEngine

T = TypeVar("T")

ALL_DATA_FORMATS = []


class DataFormatBase(Generic[T]):
    natural_storage_class: StorageClass
    natural_storage_engine: Optional[StorageEngine] = None
    storable: bool = True

    def __init__(self):
        raise NotImplementedError("Do not instantiate DataFormat classes")

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        ALL_DATA_FORMATS.append(cls)

    @classmethod
    def get_natural_storage_class(cls) -> StorageClass:
        return cls.natural_storage_class

    @classmethod
    def get_natural_storage_engine(cls) -> Optional[StorageEngine]:
        return cls.natural_storage_engine

    @classmethod
    def is_storable(cls) -> bool:
        # Does the format store data in a stable, serializable format?
        # Examples that arent' storable: generators, file-like buffers, and
        # db cursors -- they depend on open in-memory resources that may go away
        return cls.storable


DataFormat = Type[DataFormatBase]
