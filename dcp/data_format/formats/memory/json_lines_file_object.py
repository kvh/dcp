from __future__ import annotations

import decimal
import traceback
from datetime import date, datetime, time
from io import IOBase
from typing import Any, Dict, Iterable, List, Optional, Type, Union, cast

import dcp.storage.base as storage
import pandas as pd
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
from commonmodel.field_types import (
    Binary,
    Decimal,
    Json,
    LongBinary,
    LongText,
    Text,
    ensure_field_type,
)
from dateutil import parser
from dcp.data_format.base import DataFormat, DataFormatBase
from dcp.data_format.handler import FormatHandler
from dcp.utils.common import (
    ensure_bool,
    ensure_date,
    ensure_datetime,
    ensure_time,
    is_boolish,
    is_nullish,
    is_numberish,
)
from dcp.utils.data import read_json
from loguru import logger
from pandas import DataFrame


class JsonLinesFileObject(IOBase):
    pass


class JsonLinesFileObjectFormat(DataFormatBase[JsonLinesFileObject]):
    natural_storage_class = storage.MemoryStorageClass
    storable = False
