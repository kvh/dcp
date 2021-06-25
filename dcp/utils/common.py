from __future__ import annotations

import decimal
import hashlib
import json
import random
import re
import string
import uuid
from dataclasses import field
from datetime import date, datetime, time, timedelta
from enum import Enum
from typing import (
    Any,
    Dict,
    Generic,
    Iterable,
    List,
    NewType,
    Optional,
    OrderedDict,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import pytz
from dateutil import parser
from pandas.core.dtypes.missing import isnull

T = TypeVar("T")


class AttrDict(Dict):
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__


class StringEnum(Enum):
    def __str__(self):
        return self.value


def title_to_snake_case(s: str) -> str:
    s2 = ""
    for i in range(1, len(s)):
        if s[i - 1].islower() and s[i].isupper():
            # "aCamel"
            s2 += s[i - 1] + "_"
        elif (
            s[i - 1].isupper()
            and s[i].isupper()
            and i < len(s) - 1
            and s[i + 1].islower()
        ):
            # "ATitle"
            s2 += s[i - 1] + "_"
        elif s[i - 1].isnumeric() != s[i].isnumeric() and (
            s[i - 1].isalpha() or s[i].isupper()
        ):
            # "This98That"
            s2 += s[i - 1] + "_"
        else:
            s2 += s[i - 1]
    s2 += s[-1]
    return s2.lower()


def snake_to_title_case(s: str) -> str:
    s2 = ""
    title = True
    for c in s:
        if c == "_":
            title = True
            continue
        if title:
            c = c.upper()
            title = False
        s2 += c
    return s2


def as_identifier(s: str) -> str:
    # make db-compatible identifier from str
    s = re.sub(r"\W+", "_", s).lower()
    if s and not re.match(r"[a-z_]", s[0]):
        s = "_" + s  # Must start with alpha or underscore
    return s


UNAMBIGUOUS_ALPHA = "abcdefghjkmnpqrstuvwxyz"
UNAMBIGUOUS_CHARACTERS = UNAMBIGUOUS_ALPHA + UNAMBIGUOUS_ALPHA.upper() + string.digits


def rand_str(chars=20, character_set=UNAMBIGUOUS_CHARACTERS):
    rand = random.SystemRandom()
    return "".join(rand.choice(character_set) for _ in range(chars))


def utcnow():
    return pytz.utc.localize(datetime.utcnow())


def md5_hash(s: str) -> str:
    h = hashlib.md5()
    h.update(s.encode("utf8"))
    return h.hexdigest()


def dataclass_kwargs(dc: Any, kwargs: Dict) -> Dict:
    return {f.name: kwargs.get(f.name) for f in field(dc)}


def remove_dupes(a: List[T]) -> List[T]:
    seen: Set[T] = set()
    deduped: List[T] = []
    for i in a:
        if i in seen:
            continue
        seen.add(i)
        deduped.append(i)
    return deduped


def ensure_list(x: Any) -> List:
    if x is None:
        return []
    if isinstance(x, List):
        return x
    return [x]


def ensure_datetime(x: Optional[Union[str, datetime]]) -> Optional[datetime]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x
    if isinstance(x, date):
        return datetime.combine(x, datetime.min.time())
    if isinstance(x, int):
        return datetime.utcfromtimestamp(x)
    return parser.parse(x)


def ensure_date(x: Optional[Union[str, date]]) -> Optional[date]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    return parser.parse(x).date()


def ensure_time(x: Optional[Union[str, time]]) -> Optional[time]:
    if x is None:
        return None
    if isinstance(x, time):
        return x
    return parser.parse(x).time()


def ensure_utc(x: datetime) -> datetime:
    try:
        return pytz.utc.localize(x)
    except ValueError:
        pass
    return x


def ensure_bool(x: Optional[Union[str, bool]]) -> Optional[bool]:
    if x is None:
        return None
    if isinstance(x, str):
        x = x.lower()
        if x in ("t", "true"):
            return True
        if x in ("f", "false"):
            return False
        raise ValueError(x)
    elif isinstance(x, bool):
        return x
    if x == 0:
        return False
    if x == 1:
        return True
    raise TypeError(x)


def is_nullish(
    o: Any,
    null_strings=set(["None", "null", "na", "", "NULL", "NA", "N/A", "0000-00-00"]),
) -> bool:
    # TOOD: is "na" too aggressive?
    if o is None:
        return True
    if isinstance(o, str):
        if o in null_strings:
            return True
    if isinstance(o, Iterable):
        # No basic python object is "nullish", even if empty
        return False
    if isnull(o):
        return True
    return False


def is_boolish(o: Any, bool_strings=["True", "true", "False", "false"]) -> bool:
    if o is None:
        return False
    if isinstance(o, bool):
        return True
    if isinstance(o, str):
        return o in bool_strings
    return False


def is_numberish(obj: Any) -> bool:
    if (
        isinstance(obj, float)
        or isinstance(obj, int)
        or isinstance(obj, decimal.Decimal)
    ):
        return True
    if isinstance(obj, str):
        try:
            int(obj)
            return True
        except (TypeError, ValueError):
            pass
        try:
            float(obj)
            return True
        except (TypeError, ValueError):
            pass
    return False


def is_aware(d: Union[datetime, time]) -> bool:
    return d.tzinfo is not None and d.tzinfo.utcoffset(None) is not None


def date_to_str(dt: Union[str, date, datetime], date_format: str = "%F %T") -> str:
    if isinstance(dt, str):
        return dt
    return dt.strftime(date_format)


def _get_duration_components(duration: timedelta) -> Tuple[int, int, int, int, int]:
    """From Django"""
    days = duration.days
    seconds = duration.seconds
    microseconds = duration.microseconds

    minutes = seconds // 60
    seconds = seconds % 60

    hours = minutes // 60
    minutes = minutes % 60

    return days, hours, minutes, seconds, microseconds


def duration_iso_string(duration: timedelta) -> str:
    """From Django"""
    if duration < timedelta(0):
        sign = "-"
        duration *= -1
    else:
        sign = ""

    days, hours, minutes, seconds, microseconds = _get_duration_components(duration)
    ms = ".{:06d}".format(microseconds) if microseconds else ""
    return "{}P{}DT{:02d}H{:02d}M{:02d}{}S".format(
        sign, days, hours, minutes, seconds, ms
    )


def as_aware_datetime(v: Union[datetime, str, int]) -> datetime:
    if isinstance(v, datetime):
        if v.tzinfo is None:
            return v.replace(tzinfo=pytz.UTC)
        return v
    if isinstance(v, str):
        dt = parser.parse(v)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=pytz.UTC)
        return dt
    if isinstance(v, int):
        return datetime.fromtimestamp(v, pytz.utc)
    return v


def is_datetime_str(s: str) -> bool:
    """
    Relatively conservative datetime string detector. Takes preference
    for numerics over datetimes (so 20201201 is an integer, not a date)
    """
    if not isinstance(s, str):
        s = str(s)
    try:
        int(s)
        return False
    except (TypeError, ValueError):
        pass
    try:
        float(s)
        return False
    except (TypeError, ValueError):
        pass
    try:
        # We use ancient date as default to detect when no date was found
        # Will fail if trying to parse actual ancient dates!
        dt = parser.parse(s, default=datetime(1, 1, 1))
        if dt.year < 2:
            # dateutil parser only found a time, not a date
            return False
    except Exception:
        return False
    return True


class JSONEncoder(json.JSONEncoder):
    def __init__(self, *args, **kwargs):
        # kwargs["ignore_nan"] = True
        super().__init__(*args, **kwargs)

    def default(self, o: Any) -> str:
        # See "Date Time String Format" in the ECMA-262 specification.
        if isinstance(o, datetime):
            r = o.isoformat()
            if o.microsecond:
                r = r[:23] + r[26:]
            if r.endswith("+00:00"):
                r = r[:-6] + "Z"
            return r
        elif isinstance(o, date):
            return o.isoformat()
        elif isinstance(o, time):
            if is_aware(o):
                raise ValueError("JSON can't represent timezone-aware times.")
            r = o.isoformat()
            if o.microsecond:
                r = r[:12]
            return r
        elif isinstance(o, timedelta):
            return duration_iso_string(o)
        elif isinstance(o, (decimal.Decimal, uuid.UUID)):
            return str(o)
        else:
            return super().default(o)


class DcpJsonEncoder(json.JSONEncoder):
    def default(self, o: Any) -> str:
        # See "Date Time String Format" in the ECMA-262 specification.
        if isinstance(o, datetime):
            r = o.isoformat()
            if o.microsecond:
                r = r[:23] + r[26:]
            if r.endswith("+00:00"):
                r = r[:-6] + "Z"
            return r
        elif isinstance(o, date):
            return o.isoformat()
        elif isinstance(o, time):
            if is_aware(o):
                raise ValueError("JSON can't represent timezone-aware times.")
            r = o.isoformat()
            if o.microsecond:
                r = r[:12]
            return r
        elif isinstance(o, timedelta):
            return duration_iso_string(o)
        elif isinstance(o, (decimal.Decimal, uuid.UUID)):
            return str(o)
        elif hasattr(o, "to_json"):
            return o.to_json()
        elif isinstance(o, StringEnum):
            return str(o)
        else:
            return super().default(o)


def to_json(d: Any) -> str:
    return json.dumps(d, cls=DcpJsonEncoder)


def profile_stmt(stmt: str, globals: Dict, locals: Dict):
    import cProfile
    import pstats
    from pstats import SortKey

    cProfile.runctx(
        stmt,
        globals=globals,
        locals=locals,
        filename="profile.stats",
    )
    p = pstats.Stats("profile.stats")
    p.strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats(100)
