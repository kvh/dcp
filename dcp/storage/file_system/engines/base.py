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

from dcp.storage.base import Storage, StorageApi


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
    def __init__(self, storage: Storage):
        self.storage = storage

    @contextmanager
    def open(self, name: str, *args, **kwargs) -> Iterator[TextIO]:
        with open(self.get_path(name), *args, **kwargs) as f:
            yield f

    def open_name(self, name: str, *args, **kwargs) -> TextIO:
        return open(self.get_path(name), *args, **kwargs)

    def get_path(self, name: str) -> str:
        dir = self.storage.url.split("://")[1]
        return os.path.join(dir, name)

    ### StorageApi implementations ###
    def exists(self, name: str) -> bool:
        return os.path.exists(self.get_path(name))

    def remove(self, name: str):
        pth = self.get_path(name)
        try:
            os.remove(pth)
        except FileNotFoundError:
            pass

    def create_alias(self, name: str, alias: str):
        pth = self.get_path(name)
        alias_pth = self.get_path(alias)
        self.remove(alias_pth)
        os.symlink(pth, alias_pth)

    def remove_alias(self, alias: str):
        self.remove(alias)

    def record_count(self, name: str) -> Optional[int]:
        # TODO: this depends on format... hmmm, i guess let upstream handle for now
        pth = self.get_path(name)
        return raw_line_count(pth)

    def copy(self, name: str, to_name: str):
        pth = self.get_path(name)
        to_pth = self.get_path(to_name)
        shutil.copy(pth, to_pth)

    def write_lines_to_file(
        self, name: str, lines: Iterable[str],  # TODO: support bytes?
    ):
        with self.open(name, "w") as f:
            f.writelines(ln + "\n" for ln in lines)
