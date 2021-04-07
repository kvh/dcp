from __future__ import annotations
from io import IOBase
import traceback
from datacopy.utils.data import read_json
from datacopy.utils.common import (
    ensure_bool,
    ensure_date,
    ensure_datetime,
    ensure_time,
    is_boolish,
    is_nullish,
    is_numberish,
)

import decimal
from openmodel.field_types import (
    Binary,
    Decimal,
    Json,
    LongBinary,
    LongText,
    Text,
    ensure_field_type,
)
from datetime import date, datetime, time

from datacopy.data_format.handler import FormatHandler
from datacopy.data_format.base import DataFormat, DataFormatBase
from typing import Any, Dict, Iterable, List, Optional, Type, Union, cast
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


class JsonLinesFileObject(IOBase):
    pass


class JsonLinesFileObjectFormat(DataFormatBase[JsonLinesFileObject]):
    natural_storage_class = storage.MemoryStorageClass
    storable = False

