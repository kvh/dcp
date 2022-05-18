from __future__ import annotations

from typing import TypeVar

import dcp.storage.base as storage
from dcp.data_format.base import DataFormatBase

ArrowFile = TypeVar("ArrowFile")


class ArrowFileFormat(DataFormatBase[ArrowFile]):
    natural_storage_class = storage.FileSystemStorageClass
    nickname = "arrowfile"
