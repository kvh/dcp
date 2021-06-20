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

ALL_DATA_FORMATS: List[DataFormat] = []


class DataFormatBase(Generic[T]):
    natural_storage_class: Type[StorageClass]
    natural_storage_engine: Optional[Type[StorageEngine]] = None
    storable: bool = True
    nickname: str = None

    def __init__(self):
        # raise NotImplementedError("Do not instantiate DataFormat classes")
        pass

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Exclude intermediate base classes
        # if cls.__name__ in ["IterableDataFormatBase"]:
        #     return
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

    @classmethod
    def to_json(cls) -> str:
        return cls.nickname or cls.__name__


# class IterableDataFormatBase(DataFormatBase[T]):
#     inner_format: DataFormat


DataFormat = Type[DataFormatBase]
# IterableDataFormat = Type[IterableDataFormatBase]


def get_format_for_nickname(name: str) -> DataFormat:
    for fmt in ALL_DATA_FORMATS:
        if fmt.nickname == name:
            return fmt
    raise NameError(f"DataFormat '{name}' not found.")


class UnknownFormat(DataFormatBase):
    nickname = "unknown"
    natural_storage_class = None
    natural_storage_engine = None
