from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Type, TypeVar, cast

import dcp.storage.base as storage
import pandas as pd
import sqlalchemy as sa
import sqlalchemy.types as satypes
from commonmodel import (
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
from commonmodel.field_types import Binary, Decimal, Json, LongBinary, LongText, Text
from dateutil import parser
from dcp.data_format.base import DataFormat, DataFormatBase
from dcp.data_format.formats.memory.records import (
    cast_python_object_to_field_type,
    select_field_type,
)
from dcp.data_format.handler import FormatHandler
from loguru import logger
from pandas import DataFrame
from sqlalchemy.sql.ddl import CreateTable

JsonLinesFile = TypeVar("JsonLinesFile")


class JsonLinesFileFormat(DataFormatBase[JsonLinesFile]):
    natural_storage_class = storage.FileSystemStorageClass
    nickname = "jsonl"


class JsonLinesFileHandler(FormatHandler):
    for_data_formats = [JsonLinesFileFormat]
    for_storage_classes = [storage.FileSystemStorageClass]

    def infer_data_format(
        self, name: str, storage: storage.Storage
    ) -> Optional[DataFormat]:
        if name.endswith(".jsonl"):
            return JsonLinesFileFormat
        # TODO: how hacky is this? very
        with storage.get_api().open(name) as f:
            ln = f.readline()
            try:
                json.loads(ln)
                return JsonLinesFileFormat
            except json.JSONDecodeError:
                pass
        return None

    def infer_field_names(self, name, storage) -> List[str]:
        with storage.get_api().open(name) as f:
            ln = f.readline()
            return [k for k in json.loads(ln).keys()]

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
        # Just "touch"
        with storage.get_api().open(name, "w"):
            pass
