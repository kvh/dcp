from __future__ import annotations

from sqlalchemy.sql.ddl import CreateTable
from datacopy.data_format.formats.memory.records import (
    cast_python_object_to_field_type,
    select_field_type,
)
import sqlalchemy as sa
import sqlalchemy.types as satypes
from openmodel.field_types import Binary, Decimal, Json, LongBinary, LongText, Text
from datacopy.data_format.handler import FormatHandler
from datacopy.data_format.base import DataFormat, DataFormatBase
from typing import Any, Dict, List, Type, TypeVar, cast
from loguru import logger
from dateutil import parser
import pandas as pd
from pandas import DataFrame

import dcp.storage.base as storage
from openmodel import (
    DEFAULT_FIELD_TYPE,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    Time,
    Field,
    FieldType,
    Schema,
)


ArrowFile = TypeVar("ArrowFile")


class ArrowFileFormat(DataFormatBase[ArrowFile]):
    natural_storage_class = storage.FileSystemStorageClass
    nickname = "arrowfile"

