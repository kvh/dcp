from __future__ import annotations
from collections import abc
from datacopy.storage.memory.iterator import SampleableIterator

from numpy import record
from datacopy.data_format.handler import get_handler

import enum
import os
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type
from urllib.parse import urlparse

from datacopy.data_format.base import ALL_DATA_FORMATS, DataFormat, DataFormatBase
from datacopy.storage.base import (
    LocalPythonStorageEngine,
    NameDoesNotExistError,
    Storage,
    StorageApi,
)
from datacopy.utils.common import rand_str

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
    if isinstance(obj, SampleableIterator):
        # Already wrapped
        return obj
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
            raise NameDoesNotExistError(name)
        return obj

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
        obj = self.get(name)
        raise NotImplementedError
        get_record_count(
            obj
        )  # TODO: going in circles? this would be a handler thing -> infer format -> get cnt

    def copy(self, name: str, to_name: str):
        obj = self.get(name)
        obj_copy = deepcopy(obj)  # TODO: when does this deepcopy fail?
        self.put(to_name, obj_copy)

    def create_alias(self, name: str, alias: str):
        obj = self.get(name)
        self.put(alias, obj)

    def remove_alias(self, alias: str):
        self.remove(alias)

