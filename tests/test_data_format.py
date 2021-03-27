from __future__ import annotations

from dataclasses import asdict
from dcp.storage.base import StorageClass, StorageEngine
from dcp.data_format.base import ALL_DATA_FORMATS, DataFormatBase
from io import StringIO
from typing import Callable


def test_formats():
    for fmt in ALL_DATA_FORMATS:
        assert issubclass(fmt, DataFormatBase)
        assert issubclass(fmt.get_natural_storage_class(), StorageClass)
        eng = fmt.get_natural_storage_engine()
        if eng is not None:
            assert issubclass(eng, StorageEngine)
        assert isinstance(fmt.is_storable(), bool)
