from __future__ import annotations
from dcp.utils.common import rand_str
from dcp.storage.base import NameDoesNotExistError, Storage, StorageApi
from dcp.storage.memory.memory_records_object import MemoryRecordsObject
from dcp.data_format.formats import ArrowFileFormat, DatabaseTableFormat, RecordsFormat
from dcp.data_format.base import ALL_DATA_FORMATS, DataFormat, DataFormatBase

import enum
import os
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type
from urllib.parse import urlparse


LOCAL_PYTHON_STORAGE: Dict[str, MemoryRecordsObject] = {}  # TODO: global state...


def new_local_python_storage() -> Storage:
    return Storage.from_url(f"python://{rand_str(10)}/")


def clear_local_storage():
    LOCAL_PYTHON_STORAGE.clear()


class PythonStorageApi(StorageApi):
    def get_path(self, name: str) -> str:
        return os.path.join(self.storage.url, name)

    def get(self, name: str) -> MemoryRecordsObject:
        pth = self.get_path(name)
        mdr = LOCAL_PYTHON_STORAGE.get(pth)
        if mdr is None:
            raise NameDoesNotExistError(name)
        return mdr

    def remove(self, name: str):
        pth = self.get_path(name)
        del LOCAL_PYTHON_STORAGE[pth]

    def put(self, name: str, mdr: MemoryRecordsObject):
        pth = self.get_path(name)
        LOCAL_PYTHON_STORAGE[pth] = mdr

    def exists(self, name: str) -> bool:
        pth = self.get_path(name)
        return pth in LOCAL_PYTHON_STORAGE

    def record_count(self, name: str) -> Optional[int]:
        mdr = self.get(name)
        return mdr.record_count

    def copy(self, name: str, to_name: str):
        mdr = self.get(name)
        mdr_copy = deepcopy(mdr)
        self.put(to_name, mdr_copy)

    def create_alias(self, name: str, alias: str):
        mdr = self.get(name)
        self.put(alias, mdr)

    def remove_alias(self, alias: str):
        self.remove(alias)
