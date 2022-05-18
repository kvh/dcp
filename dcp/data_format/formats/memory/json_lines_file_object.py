from __future__ import annotations

from io import IOBase

import dcp.storage.base as storage
from dcp.data_format.base import DataFormatBase


class JsonLinesFileObject(IOBase):
    pass


class JsonLinesFileObjectFormat(DataFormatBase[JsonLinesFileObject]):
    natural_storage_class = storage.MemoryStorageClass
    storable = False
