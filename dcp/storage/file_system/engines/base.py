from __future__ import annotations
from io import IOBase

import os
import shutil
import tempfile
from contextlib import contextmanager
from typing import (
    ContextManager,
    Generator,
    Iterable,
    Iterator,
    Optional,
    TextIO,
    Type,
    Union,
)

from dcp.storage.base import Storage, StorageApi, StorageObject, FullPath


def raw_line_count(f: Union[str, IOBase]) -> int:
    # Fast file cnt in python
    # From: https://stackoverflow.com/questions/845058/how-to-get-line-count-of-a-large-file-cheaply-in-python
    def _make_gen(reader):
        b = reader(1024 * 1024)
        while b:
            yield b
            b = reader(1024 * 1024)

    if isinstance(f, str):
        f = open(f, "rb")
    f_gen = _make_gen(f.raw.read)
    return sum(buf.count(b"\n") for buf in f_gen)


def get_tmp_local_file_url() -> str:
    return f"file://{tempfile.gettempdir()}"


class FileSystemStorageApi(StorageApi):
    @contextmanager
    def open(
        self, name: str | FullPath | StorageObject, *args, **kwargs
    ) -> Iterator[TextIO]:
        with open(self.get_path(name), *args, **kwargs) as f:
            yield f

    def open_name(
        self, name: str | FullPath | StorageObject, *args, **kwargs
    ) -> TextIO:
        return open(self.get_path(name), *args, **kwargs)

    def get_path(self, name: str | FullPath | StorageObject) -> str:
        if isinstance(name, StorageObject):
            name = name.full_path
        if isinstance(name, FullPath):
            name = os.path.join(*name.as_list())
        dir = self.storage.url.split("://")[1]
        return os.path.join(dir, name)

    ### StorageApi implementations ###
    def format_full_path(self, full_path: FullPath) -> str:
        return os.path.join(*full_path.as_list())

    def _exists(self, obj: StorageObject) -> bool:
        return os.path.exists(self.get_path(obj))

    def _remove(self, obj: StorageObject):
        pth = self.get_path(obj)
        try:
            os.remove(pth)
        except FileNotFoundError:
            pass

    def _create_alias(self, obj: StorageObject, alias_obj: StorageObject):
        pth = self.get_path(obj)
        alias_pth = self.get_path(alias_obj)
        self.remove(alias_pth)
        os.symlink(pth, alias_pth)

    def _remove_alias(self, obj: StorageObject):
        self.remove(obj)

    def _record_count(self, obj: StorageObject) -> Optional[int]:
        # TODO: this depends on format... hmmm, i guess let upstream handle for now
        pth = self.get_path(obj)
        return raw_line_count(pth)

    def _copy(self, obj: StorageObject, to_obj: StorageObject):
        pth = self.get_path(obj)
        to_pth = self.get_path(to_obj)
        shutil.copy(pth, to_pth)

    def write_lines_to_file(
        self,
        name: str,
        lines: Iterable[str],  # TODO: support bytes?
    ):
        with self.open(name, "w") as f:
            f.writelines(ln + "\n" for ln in lines)
