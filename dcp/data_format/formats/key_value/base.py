from __future__ import annotations

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
    PythonRecordsHandler,
    cast_python_object_to_field_type,
    select_field_type,
)
from dcp.data_format.handler import FormatHandler
from loguru import logger
from pandas import DataFrame
from sqlalchemy.sql.ddl import CreateTable

Json = TypeVar("Json")


class JsonFormat(DataFormatBase[Json]):
    natural_storage_class = storage.KeyValueStorageClass
    nickname = "json"


class KeyValueDatabaseTableHandler(PythonRecordsHandler):
    for_data_formats = [JsonFormat]
    for_storage_classes = [storage.KeyValueStorageClass]

    def infer_data_format(self, name, storage) -> DataFormat:
        return JsonFormat

