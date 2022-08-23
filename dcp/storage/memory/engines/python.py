from __future__ import annotations

import os
from contextlib import contextmanager
from copy import deepcopy
from typing import Any, Dict, Optional

from dcp.storage.base import (
    NameDoesNotExistError,
    Storage,
    StorageApi,
    StorageObject,
    FullPath,
)
from dcp.utils.common import rand_str

LOCAL_PYTHON_STORAGE: Dict[str, Any] = {}  # TODO: global state...


def new_local_python_storage() -> Storage:
    return Storage.from_url(f"python://{rand_str(10)}/")


def clear_local_storage():
    LOCAL_PYTHON_STORAGE.clear()


#
# def wrap_records_object(obj: Any) -> Any:
#     """
#     Wrap records object that are exhaustable (eg generators, file objects, db cursors)
#     so that we can sample them for inspection and inference without losing records.
#     """
#     # TODO: handling iterators is too dangerous?
#     return obj
#     if isinstance(obj, SampleableIterator):
#         # Already wrapped
#         return obj
#     # if isinstance(obj, IOBase):
#     #     return SampleableIOBase(obj)
#     if isinstance(obj, abc.Iterator):
#         return SampleableIterator(obj)
#     return obj


class PythonStorageApi(StorageApi):
    def get_path(self, name: str | FullPath | StorageObject) -> str:
        if isinstance(name, StorageObject):
            name = name.full_path
        if isinstance(name, FullPath):
            name = os.path.join(*name.as_list())
        return os.path.join(self.storage.url, name)

    def get(self, name: str | FullPath | StorageObject) -> Any:
        pth = self.get_path(name)
        obj = LOCAL_PYTHON_STORAGE.get(pth)
        if obj is None:
            raise NameDoesNotExistError(f"{name} on {self.storage}")
        return obj

    def put(self, name: str | FullPath | StorageObject, records_obj: Any):
        # assert isinstance(
        #     mdr, MemoryRecordsObject
        # ), f"Can only store MemoryRecordsObjects, not {type(mdr)}"
        # records_obj = wrap_records_object(
        #     records_obj
        # )  # TODO: is this the right place for this?
        pth = self.get_path(name)
        LOCAL_PYTHON_STORAGE[pth] = records_obj

    @contextmanager
    def temp(self, name: str, records_obj: Any):
        self.put(name, records_obj)
        yield
        self.remove(name)

    # StorageApi overrides

    def _remove(self, obj: StorageObject):
        pth = self.get_path(obj)
        del LOCAL_PYTHON_STORAGE[pth]

    def _exists(self, obj: StorageObject) -> bool:
        pth = self.get_path(obj)
        return pth in LOCAL_PYTHON_STORAGE

    def _record_count(self, obj: StorageObject) -> Optional[int]:
        from dcp.data_format.handler import get_handler_for_name

        handler = get_handler_for_name(obj)
        return handler().get_record_count(obj)

    def _copy(self, obj: StorageObject, to_obj: StorageObject):
        py_obj = self.get(obj)
        py_obj_copy = deepcopy(py_obj)  # TODO: when does this deepcopy fail?
        self.put(to_obj, py_obj_copy)

    def _create_alias(self, obj: StorageObject, alias_obj: StorageObject):
        py_obj = self.get(obj)
        self.put(alias_obj, py_obj)

    def _remove_alias(self, obj: StorageObject):
        self._remove(obj)

    def format_full_path(self, full_path: FullPath) -> str:
        return "/".join(full_path.as_list())


DEFAULT_PYTHON_STORAGE = Storage("python://_default")
