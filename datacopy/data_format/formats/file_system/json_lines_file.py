from __future__ import annotations

from typing import Any, Dict, List, Type, TypeVar, cast

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

JsonLinesFile = TypeVar("JsonLinesFile")


class JsonLinesFileFormat(DataFormatBase[JsonLinesFile]):
    natural_storage_class = storage.FileSystemStorageClass
    nickname = "jsonl"
