from __future__ import annotations

import decimal
import traceback
from datetime import date, datetime, time
from io import IOBase
from typing import Any, Dict, Iterable, List, Optional, Type, Union, cast

import datacopy.storage.base as storage
import pandas as pd
from datacopy.data_format.base import DataFormat, DataFormatBase
from datacopy.data_format.handler import FormatHandler
from datacopy.utils.common import (
    ensure_bool,
    ensure_date,
    ensure_datetime,
    ensure_time,
    is_boolish,
    is_nullish,
    is_numberish,
)
from datacopy.utils.data import read_json
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
from openmodel.field_types import (
    Binary,
    Decimal,
    Json,
    LongBinary,
    LongText,
    Text,
    ensure_field_type,
)
from pandas import DataFrame


class CsvFileObject(IOBase):
    pass


class CsvFileObjectFormat(DataFormatBase[CsvFileObject]):
    natural_storage_class = storage.MemoryStorageClass
    storable = False
