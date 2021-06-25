from __future__ import annotations
from dcp.storage.file_system.engines.base import FileSystemStorageApi
import io

import os
import shutil
import tempfile
from contextlib import contextmanager
from typing import ContextManager, Generator, Iterable, Iterator, Optional, TextIO, Type

from dcp.storage.base import Storage, StorageApi

try:
    from google.cloud import storage as gcs
    import gcsfs

    GOOGLE_CLOUD_STORAGE_SUPPORTED = True
except ImportError:
    gcs = None
    GOOGLE_CLOUD_STORAGE_SUPPORTED = False


class GoogleCloudStorageApi(FileSystemStorageApi):
    def __init__(self, storage: Storage):
        if gcs is None:
            raise ImportError(
                "You must install google cloud libraries (gcsfs and google-cloud-storage)"
            )
        self.storage = storage
        self.client = gcs.Client()
        self.fs = gcsfs.GCSFileSystem()
        self.bucket = self.client.bucket(self.bucket_name)

    @property
    def bucket_name(self) -> str:
        return self.storage.url.split("://")[1].split("/")[0]

    @contextmanager
    def open(self, name: str, mode: str = "r", *args, **kwargs) -> Iterator[TextIO]:
        # if "a" in mode:
        #     raise NotImplementedError
        with self.fs.open(self.get_path(name), mode, *args, **kwargs) as f:
            yield f

    # def read(self, name: str) -> TextIO:
    #     buffer = io.TextIO()
    #     blob = self.bucket.blob(self.get_path(name))
    #     blob.download_to_file(buffer)
    #     buffer.seek(0)
    #     return buffer

    def open_name(self, name: str, mode: str = "r", *args, **kwargs) -> TextIO:
        # if "a" in mode:
        #     raise NotImplementedError
        return self.fs.open(self.get_path(name), mode, *args, **kwargs)

    ### StorageApi implementations ###
    def exists(self, name: str) -> bool:
        return self.fs.exists(self.get_path(name))

    def remove(self, name: str):
        pth = self.get_path(name)
        try:
            self.fs.rm(pth)
        except FileNotFoundError:
            pass

    def create_alias(self, name: str, alias: str):
        # Just a copy? I think this is a symlink on GCS backend? Should be since immutable
        self.copy(name, alias)

    def record_count(self, name: str) -> Optional[int]:
        # Not implemented for now
        return None

    def copy(self, name: str, to_name: str):
        pth = self.get_path(name)
        to_pth = self.get_path(to_name)
        src = self.bucket.blob(pth)
        dst = self.bucket.blob(to_pth)
        self.bucket.copy_blob(src, self.bucket, dst)
