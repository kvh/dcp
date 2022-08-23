from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    TypeVar,
    Iterator,
)

from commonmodel import (
    FieldType,
    Schema,
)

import dcp.storage.base as storage
from dcp.data_format.base import DataFormat, DataFormatBase
from dcp.data_format.handler import FormatHandler
from dcp.data_format.inference import generate_auto_schema

T = TypeVar("T")


class IteratorBase(Generic[T]):
    def __init__(self, iterator: Iterable[T], closeable: Callable = None):
        self.iterator = iterator
        self.closeable = closeable

    def __iter__(self) -> Iterator[T]:
        yield from self.iterator
        self.close()

    def chunks(self, chunksize: int) -> Iterator:
        try:
            chunk = []
            for record in self.iterator:
                chunk.append(record)
                if len(chunk) == chunksize:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk
        finally:
            self.close()

    def close(self):
        if self.closeable:
            self.closeable()

    def concat(self, append_other: IteratorBase):
        def f() -> Iterable[T]:
            yield from self.iterator
            yield from append_other.iterator

        def c():
            self.close()
            append_other.close()

        return type(self)(f(), c)


class RecordsIterator(IteratorBase[Dict[str, Any]]):
    pass


class RecordsIteratorFormat(DataFormatBase[RecordsIterator]):
    natural_storage_class = storage.MemoryStorageClass
    nickname = "records_iterator"


class PythonRecordsIteratorHandler(FormatHandler):
    """TODO: this is a write-only format at the moment (can't copy FROM this format to another),
    this is due to iterators being ephemeral objects.
    """

    for_data_formats = [RecordsIteratorFormat]
    for_storage_engines = [storage.LocalPythonStorageEngine]

    def infer_data_format(self, so: storage.StorageObject) -> Optional[DataFormat]:
        return None

    def infer_field_names(self, so: storage.StorageObject) -> List[str]:
        raise NotImplementedError

    def infer_field_type(self, so: storage.StorageObject, field: str) -> FieldType:
        raise NotImplementedError

    def cast_to_field_type(
        self, so: storage.StorageObject, field: str, field_type: FieldType
    ):
        raise NotImplementedError

    def create_empty(self, so: storage.StorageObject, schema: Schema):
        def f():
            yield from []

        so.storage.get_memory_api().put(so, RecordsIterator(f()))
