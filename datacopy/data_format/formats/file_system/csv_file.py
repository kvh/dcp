from __future__ import annotations

from typing import Any, Dict, List, Optional, Type, TypeVar, cast

import datacopy.storage.base as storage
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.types as satypes
from datacopy.data_format.base import DataFormat, DataFormatBase
from datacopy.data_format.formats.memory.records import (
    cast_python_object_to_field_type,
    select_field_type,
)
from datacopy.data_format.handler import FormatHandler
from dateutil import parser
from loguru import logger
from openmodel import (
    DEFAULT_FIELD_TYPE,
    Boolean,
    Date,
    DateTime,
    Field,
    FieldType,
    Float,
    Integer,
    Schema,
    Time,
)
from openmodel.field_types import Binary, Decimal, Json, LongBinary, LongText, Text
from pandas import DataFrame
from sqlalchemy.sql.ddl import CreateTable

CsvFile = TypeVar("CsvFile")


class CsvFileFormat(DataFormatBase[CsvFile]):
    natural_storage_class = storage.FileSystemStorageClass
    nickname = "csv"


class CsvFileHandler(FormatHandler):
    for_data_formats = [CsvFileFormat]
    for_storage_classes = [storage.DatabaseStorageClass]

    def infer_data_format(
        self, name: str, storage: storage.Storage
    ) -> Optional[DataFormat]:
        if name.endswith(".csv"):
            return CsvFileFormat
        # TODO: get records sample and sniff csv
        return None

    def infer_field_names(self, name, storage) -> List[str]:
        with storage.get_api().open(name) as f:
            l = f.readline()
            return [c.strip() for c in l.split(",")]

    def infer_field_type(
        self, name: str, storage: storage.Storage, field: str
    ) -> FieldType:
        # TODO: to do this, essentially need to copy into mem
        # TODO: fix once we have sample?
        return DEFAULT_FIELD_TYPE

    def cast_to_field_type(
        self, name: str, storage: storage.Storage, field: str, field_type: FieldType
    ):
        # This is a no-op, files have no inherent data types
        pass

    def create_empty(self, name, storage, schema: Schema):
        # Not sure you'd really ever want to do this?
        with storage.get_api().open(name, "w") as f:
            f.writeline(",".join(schema.field_names()))