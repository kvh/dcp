from __future__ import annotations

from io import IOBase
from typing import (
    List,
    Optional,
)

from commonmodel import (
    FieldType,
    Schema,
)

import dcp.storage.base as storage
from dcp.data_format.base import DataFormat, DataFormatBase
from dcp.data_format.formats.file_system.csv_file import is_maybe_csv
from dcp.data_format.formats.memory.records import Records, select_field_type
from dcp.data_format.handler import FormatHandler
from dcp.storage.memory.iterator import SampleableIterator
from dcp.utils.data import read_csv


class CsvLinesIterator(IOBase):
    pass


class CsvLinesIteratorFormat(DataFormatBase[CsvLinesIterator]):
    nickname = "csv_lines"
    natural_storage_class = storage.MemoryStorageClass
    storable = False


SAMPLE_SIZE = 1024 * 10
SAMPLE_SIZE_LINES = 100


class PythonCsvLinesIteratorHandler(FormatHandler):
    for_data_formats = [CsvLinesIteratorFormat]
    for_storage_engines = [storage.LocalPythonStorageEngine]

    def get_sample_string(self, so: storage.StorageObject) -> str:
        obj = so.storage.get_memory_api().get(so)
        assert isinstance(obj, SampleableIterator)
        sample = obj.head(SAMPLE_SIZE_LINES)
        s = "".join(sample)
        return s

    def get_sample_records(self, so: storage.StorageObject) -> Records:
        obj = so.storage.get_memory_api().get(so)
        assert isinstance(obj, SampleableIterator)
        sample = obj.head(SAMPLE_SIZE_LINES)
        for r in read_csv(sample):
            yield r

    def infer_data_format(self, obj: storage.StorageObject) -> Optional[DataFormat]:
        py_obj = obj.storage.get_memory_api().get(obj)
        if isinstance(py_obj, SampleableIterator):
            s = self.get_sample_string(obj)
            if is_maybe_csv(s):
                return CsvLinesIteratorFormat
        return None

    # TODO: get sample
    def infer_field_names(self, so: storage.StorageObject) -> List[str]:
        names = []
        for r in self.get_sample_records(so):
            for k in r.keys():  # Ordered as of py 3.7
                if k not in names:
                    names.append(k)  # Keep order
        return names

    def infer_field_type(self, so: storage.StorageObject, field: str) -> FieldType:
        sample = []
        for r in self.get_sample_records(so):
            if field in r:
                sample.append(r[field])
            if len(sample) >= self.sample_size:
                break
        ft = select_field_type(sample)
        return ft

    def cast_to_field_type(
        self, so: storage.StorageObject, field: str, field_type: FieldType
    ):
        # TODO: no-op? not really types on strings...
        pass

    def create_empty(self, so: storage.StorageObject, schema: Schema):
        s = ",".join(schema.field_names()) + "\n"
        so.storage.get_memory_api().put(so, (ln for ln in [s]))

    def get_record_count(self, so: storage.StorageObject) -> Optional[int]:
        return None
