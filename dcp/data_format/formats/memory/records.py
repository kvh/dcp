from __future__ import annotations

import decimal
import traceback
from datetime import date, datetime, time
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

Records = List[Dict[str, Any]]


class RecordsFormat(DataFormatBase[Records]):
    natural_storage_class = storage.MemoryStorageClass
    nickname = "records"


class PythonRecordsHandler(FormatHandler):
    for_data_formats = [RecordsFormat]
    for_storage_engines = [storage.LocalPythonStorageEngine]

    def infer_data_format(self, name, storage) -> Optional[DataFormat]:
        obj = storage.get_api().get(name)
        if isinstance(obj, list):
            if len(obj) > 0:
                if isinstance(obj[0], dict):
                    return RecordsFormat
                else:
                    return None
            # If empty list, default to records format
            return RecordsFormat
        return None

    # TODO: get sample
    def infer_field_names(self, name, storage) -> List[str]:
        records = storage.get_api().get(name)
        assert isinstance(records, list)
        if not records:
            return []
        names = []
        for r in records[:100]:
            for k in r.keys():  # Ordered as of py 3.7
                if k not in names:
                    names.append(k)  # Keep order
            # names |= set(r.keys())
        return list(names)

    def infer_field_type(
        self, name: str, storage: storage.Storage, field: str
    ) -> FieldType:
        records = storage.get_api().get(name)
        sample = []
        for r in records:
            if field in r:
                sample.append(r[field])
            if len(sample) >= self.sample_size:
                break
        ft = select_field_type(sample)
        return ft

    def cast_to_field_type(
        self, name: str, storage: storage.Storage, field: str, field_type: FieldType
    ):
        records = storage.get_api().get(name)
        for r in records:
            if field in r:
                r[field] = cast_python_object_to_field_type(r[field], field_type)
        storage.get_api().put(name, records)

    def create_empty(self, name, storage, schema: Schema):
        storage.get_api().put(name, [])


ALL_FIELD_TYPE_HELPERS: Dict[Type[FieldType], Type[FieldTypeHelper]] = {}


def get_helper(ft: Union[FieldType, Type[FieldType]]) -> FieldTypeHelper:
    if isinstance(ft, FieldType):
        ft = ft.__class__
    return ALL_FIELD_TYPE_HELPERS[ft]()


class FieldTypeHelper:
    field_type: Type[FieldType]
    python_type: type
    cardinality_rank: int

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        ALL_FIELD_TYPE_HELPERS[cls.field_type] = cls

    def is_maybe(self, obj: Any) -> bool:
        return False

    def is_definitely(self, obj: Any) -> bool:
        return isinstance(obj, self.python_type)

    def cast(self, obj: Any, strict: bool = False) -> Any:
        return obj


def _detect_field_type_fast(obj: Any) -> Optional[FieldType]:
    """
    Fast, but doesn't support adding new types via the registry.
    TODO: Fixable tho, just need to make sure added types are ranked by cardinality (separate registry?)
    """
    if is_nullish(obj):
        # TODO: this is pretty aggressive?
        return None
    for fth in ALL_FIELD_TYPE_HELPERS.values():
        fth = fth()
        if fth.is_definitely(obj):
            return ensure_field_type(fth.field_type)
    for fth in ALL_FIELD_TYPE_HELPERS.values():
        fth = fth()
        if fth.is_maybe(obj):
            return ensure_field_type(fth.field_type)
    # I don't think we should get here ever? Some random object type
    logger.error(obj)
    return DEFAULT_FIELD_TYPE


# def _detect_field_type_complete(obj: Any) -> Optional[FieldType]:
#     if is_nullish(obj):
#         # TODO: this is pretty aggressive?
#         return None
#     # If we have an
#     definitelys = []
#     for ft in global_registry.all(FieldTypeBase):
#         if isinstance(ft, type):
#             ft = ft()
#         if ft.is_definitely(obj):
#             definitelys.append(ft)
#     if definitelys:
#         # Take lowest cardinality definitely
#         return min(definitelys, key=lambda x: x.cardinality_rank)
#     maybes = []
#     for ft in global_registry.all(FieldTypeBase):
#         if isinstance(ft, type):
#             ft = ft()
#         if ft.is_maybe(obj):
#             maybes.append(ft)
#     if not maybes:
#         # I don't think we should get here ever? Some random object type
#         logger.error(obj)
#         return DEFAULT_FIELD_TYPE
#     # Take lowest cardinality maybe
#     return min(maybes, key=lambda x: x.cardinality_rank)


detect_field_type = _detect_field_type_fast


def select_field_type(objects: Iterable[Any]) -> FieldType:
    types = set()
    for o in objects:
        # Choose the minimum compatible type
        typ = detect_field_type(o)
        if typ is None:
            continue
        types.add(typ)
    if not types:
        # We detected no types, column is all null-like, or there is no data
        logger.warning("No field types detected")
        return DEFAULT_FIELD_TYPE
    # Now we must choose the HIGHEST cardinality, to accomodate ALL values
    # (the maximum minimum type)
    return max(types, key=lambda x: get_helper(x).cardinality_rank)


def cast_python_object_to_field_type(
    obj: Any, field_type: FieldType, strict: bool = False
) -> Any:
    if obj is None:
        return None
    try:
        if not isinstance(obj, Iterable) and not isinstance(obj, dict) and pd.isna(obj):
            return None
    except ValueError:
        # isna() throws ValueError
        pass
    try:
        return get_helper(field_type).cast(obj, strict=strict)
    except Exception:
        err_msg = f"Error casting python object ({obj}) to type {field_type}: {traceback.format_exc()}"
        logger.error(err_msg)
        raise NotImplementedError(err_msg)


class BooleanHelper(FieldTypeHelper):
    field_type = Boolean
    python_type = bool
    cardinality_rank = 0

    def is_maybe(self, obj: Any) -> bool:
        return is_boolish(obj)

    def cast(self, obj: Any, strict: bool = False) -> Any:
        if strict:
            return bool(obj)
        return ensure_bool(obj)


class IntegerHelper(FieldTypeHelper):
    field_type = Integer
    python_type = int
    cardinality_rank = 11

    def is_maybe(self, obj: Any) -> bool:
        try:
            int(obj)
            return True
        except (ValueError, TypeError):
            pass
        if isinstance(obj, str):
            # Handle numbers with commas? what about currencies?
            try:
                int(obj.replace(",", ""))
                return True
            except (ValueError, TypeError):
                pass
        return False

    def is_definitely(self, obj: Any) -> bool:
        if isinstance(obj, bool):
            return False
        return isinstance(obj, int)

    def cast(self, obj: Any, strict: bool = False) -> Any:
        if is_nullish(obj):
            return None
        if isinstance(obj, str):
            if not obj.strip():
                return None
            return int(obj.replace(",", ""))
        return int(obj)


class FloatHelper(FieldTypeHelper):
    field_type = Float
    python_type = float
    cardinality_rank = 13

    def is_maybe(self, obj: Any) -> bool:
        try:
            float(obj)
            return True
        except (ValueError, TypeError):
            pass
        if isinstance(obj, str):
            # Handle numbers with commas? what about currencies?
            try:
                float(obj.replace(",", ""))
                return True
            except (ValueError, TypeError):
                pass
        return False

    def cast(self, obj: Any, strict: bool = False) -> Any:
        if is_nullish(obj):
            return None
        if isinstance(obj, str):
            return float(obj.replace(",", ""))
        return float(obj)


class DecimalHelper(FieldTypeHelper):
    field_type = Decimal
    python_type = decimal.Decimal
    cardinality_rank = 12

    def is_maybe(self, obj: Any) -> bool:
        try:
            float(obj)
            return True
        except (ValueError, TypeError):
            return False

    def cast(self, obj: Any, strict: bool = False) -> Any:
        if is_nullish(obj):
            return None
        return decimal.Decimal(obj)


### TODO: binary types


LONG_TEXT = 2 ** 16


### String types
class TextHelper(FieldTypeHelper):
    field_type = Text
    python_type = str
    cardinality_rank = 20

    def is_maybe(self, obj: Any) -> bool:
        return (isinstance(obj, str) or isinstance(obj, bytes)) and (
            len(obj) < LONG_TEXT
        )

    def is_definitely(self, obj: Any) -> bool:
        # Can't ever really be sure (TODO)
        return False
        # return isinstance(obj, str) and len(obj) < LONG_TEXT

    def cast(self, obj: Any, strict: bool = False) -> Any:
        s = str(obj)
        if len(s) >= LONG_TEXT:
            raise NotImplementedError
            # TODO: cast exceptions?
            # raise CastWouldCauseDataLossException(self, obj)
        return s


class LongTextHelper(FieldTypeHelper):
    field_type = LongText
    python_type = str
    cardinality_rank = 21

    def is_maybe(self, obj: Any) -> bool:
        return isinstance(obj, str) or isinstance(obj, bytes)

    def is_definitely(self, obj: Any) -> bool:
        # Can't ever really be sure (TODO)
        return False

    def cast(self, obj: Any, strict: bool = False) -> Any:
        return str(obj)


### Datetime types
class DateHelper(FieldTypeHelper):
    field_type = Date
    python_type = date
    cardinality_rank = 10

    def is_maybe(self, obj: Any) -> bool:
        if isinstance(obj, date):
            return True
        if is_numberish(obj):
            # Numbers aren't dates!
            return False
        if not isinstance(obj, str):
            obj = str(obj)
        try:
            # We use ancient date as default to detect when no date was found
            # Will fail if trying to parse actual ancient dates!
            dt = parser.parse(obj, default=datetime(1, 1, 1))
            if dt.year < 2:
                # dateutil parser only found a time, not a date
                return False
        except Exception:
            return False
        return True

    def is_definitely(self, obj: Any) -> bool:
        if isinstance(obj, str) and 8 <= len(obj) <= 10:
            if is_numberish(obj):
                # Numbers aren't dates!
                return False
            try:
                parser.isoparse(obj)
                return True
            except (parser.ParserError, TypeError, ValueError):
                pass
            return False
        else:
            return isinstance(obj, date) and not isinstance(obj, datetime)

    def cast(self, obj: Any, strict: bool = False) -> Any:
        if is_nullish(obj):
            return None
        if strict:
            if isinstance(obj, datetime):
                obj = obj.date()
            if not isinstance(obj, date):
                raise TypeError(obj)
            return obj
        return ensure_date(obj)


class DateTimeHelper(FieldTypeHelper):
    field_type = DateTime
    python_type = datetime
    cardinality_rank = 12

    def is_maybe(self, obj: Any) -> bool:
        if isinstance(obj, datetime):
            return True
        if isinstance(obj, time):
            return False
        if is_numberish(obj):
            # Numbers aren't datetimes!
            return False
        if not isinstance(obj, str):
            obj = str(obj)
        try:
            dt = parser.parse(obj, default=datetime(1, 1, 1))
            if dt.year < 2:
                # dateutil parser only found a time, not a date
                return False
        except (parser.ParserError, TypeError, ValueError):
            return False
        return True

    def is_definitely(self, obj: Any) -> bool:
        if isinstance(obj, str) and 14 <= len(obj) <= 26:
            if is_numberish(obj):
                # Numbers aren't dates!
                return False
            try:
                parser.isoparse(obj)
                return True
            except (parser.ParserError, TypeError, ValueError):
                pass
            return False
        else:
            return isinstance(obj, datetime)

    def cast(self, obj: Any, strict: bool = False) -> Any:
        if strict:
            if isinstance(obj, date):
                obj = datetime(obj.year, obj.month, obj.day)
            if not isinstance(obj, datetime):
                raise TypeError(obj)
            return obj
        if is_nullish(obj):
            return None
        return ensure_datetime(obj)


class TimeHelper(FieldTypeHelper):
    field_type = Time
    python_type = time
    cardinality_rank = 12

    def is_maybe(self, obj: Any) -> bool:
        if isinstance(obj, time):
            return True
        if is_numberish(obj):
            # Numbers aren't times!
            return False
        if not isinstance(obj, str):
            obj = str(obj)
        try:
            # We use ancient date as default to detect when only time was found
            # Will fail if trying to parse actual ancient dates!
            dt = parser.parse(obj, default=datetime(1, 1, 1))
            if dt.year < 2:
                # dateutil parser found just a time
                return True
        except Exception:
            return False
        return False

    def cast(self, obj: Any, strict: bool = False) -> Any:
        if strict:
            if not isinstance(obj, time):
                raise TypeError(obj)
            return obj
        if is_nullish(obj):
            return None
        return ensure_time(obj)


class JsonHelper(FieldTypeHelper):
    field_type = Json
    python_type = dict
    cardinality_rank = 0  # TODO: strict json, only dicts and lists?

    def is_maybe(self, obj: Any) -> bool:
        # TODO: strings too? (Actual json string)
        return isinstance(obj, dict) or isinstance(obj, list)

    def is_definitely(self, obj: Any) -> bool:
        return isinstance(obj, dict) or isinstance(obj, list)

    def cast(self, obj: Any, strict: bool = False) -> Any:
        if strict:
            if not isinstance(obj, dict) and not isinstance(obj, list):
                raise TypeError(obj)
            return obj
        if isinstance(obj, dict) or isinstance(obj, list):
            return obj
        if isinstance(obj, str):
            return read_json(obj)
        return [obj]  # TODO: this is extra ugly, should we just fail?
