from __future__ import annotations

import enum
import os
from collections import abc
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from io import IOBase
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type
from urllib.parse import urlparse

from dcp.storage.base import (
    LocalPythonStorageEngine,
    NameDoesNotExistError,
    Storage,
    StorageApi,
)
from dcp.storage.memory.iterator import SampleableIterator
from dcp.utils.common import rand_str

LOCAL_PYTHON_STORAGE: Dict[str, Any] = {}  # TODO: global state...


def new_local_python_storage() -> Storage:
    return Storage.from_url(f"python://{rand_str(10)}/")


def clear_local_storage():
    LOCAL_PYTHON_STORAGE.clear()


def wrap_records_object(obj: Any) -> Any:
    """
    Wrap records object that are exhaustable (eg generators, file objects, db cursors)
    so that we can sample them for inspection and inference without losing records.
    """
    # TODO: handling iterators is too dangerous?
    return obj
    if isinstance(obj, SampleableIterator):
        # Already wrapped
        return obj
    # if isinstance(obj, IOBase):
    #     return SampleableIOBase(obj)
    if isinstance(obj, abc.Iterator):
        return SampleableIterator(obj)
    return obj


class PythonStorageApi(StorageApi):
    def get_path(self, name: str) -> str:
        return os.path.join(self.storage.url, name)

    def get(self, name: str) -> Any:
        pth = self.get_path(name)
        obj = LOCAL_PYTHON_STORAGE.get(pth)
        if obj is None:
            raise NameDoesNotExistError(f"{name} on {self.storage}")
        return obj

    @contextmanager
    def temp(self, name: str, records_obj: Any):
        self.put(name, records_obj)
        yield
        self.remove(name)

    def remove(self, name: str):
        pth = self.get_path(name)
        del LOCAL_PYTHON_STORAGE[pth]

    def put(self, name: str, records_obj: Any):
        # assert isinstance(
        #     mdr, MemoryRecordsObject
        # ), f"Can only store MemoryRecordsObjects, not {type(mdr)}"
        pth = self.get_path(name)
        wrapped = wrap_records_object(
            records_obj
        )  # TODO: is this the right place for this?
        LOCAL_PYTHON_STORAGE[pth] = wrapped

    def exists(self, name: str) -> bool:
        pth = self.get_path(name)
        return pth in LOCAL_PYTHON_STORAGE

    def record_count(self, name: str) -> Optional[int]:
        from dcp.data_format.handler import get_handler_for_name

        handler = get_handler_for_name(name, self.storage)
        return handler().get_record_count(name, self.storage)

    def copy(self, name: str, to_name: str):
        obj = self.get(name)
        obj_copy = deepcopy(obj)  # TODO: when does this deepcopy fail?
        self.put(to_name, obj_copy)

    def create_alias(self, name: str, alias: str):
        obj = self.get(name)
        self.put(alias, obj)

    def remove_alias(self, alias: str):
        self.remove(alias)


DEFAULT_PYTHON_STORAGE = Storage("python://_default")
