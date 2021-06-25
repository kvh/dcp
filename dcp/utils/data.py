from __future__ import annotations

import csv
import decimal
import json
import typing
from datetime import datetime
from io import IOBase
from itertools import tee
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    AnyStr,
    Dict,
    Generator,
    Generic,
    Iterable,
    Iterator,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)

from dcp.utils.common import DcpJsonEncoder, is_nullish, title_to_snake_case
from loguru import logger
from pandas import Timestamp, isnull

T = TypeVar("T")


def records_as_dict_of_lists(dl: List[Dict]) -> Dict[str, List]:
    series: Dict[str, List] = {}
    for r in dl:
        for k, v in r.items():
            if k in series:
                series[k].append(v)
            else:
                series[k] = [v]
    return series


class DcpCsvDialect(csv.Dialect):
    delimiter = ","
    quotechar = '"'
    escapechar = "\\"
    doublequote = True
    skipinitialspace = False
    quoting = csv.QUOTE_MINIMAL
    lineterminator = "\n"


def process_raw_value(v: Any) -> Any:
    if is_nullish(v):
        return None
    return v


def clean_record(
    record: Dict[str, Any], ensure_keys_snake_case: bool = True
) -> Dict[str, Any]:
    if ensure_keys_snake_case:
        return {title_to_snake_case(k): process_raw_value(v) for k, v in record.items()}
    return {k: process_raw_value(v) for k, v in record.items()}


def ensure_strings(i: Iterator[AnyStr]) -> Iterator[str]:
    for s in i:
        if isinstance(s, bytes):
            s = s.decode("utf8")
        yield s


def iterate_chunks(iterator: Iterator[T], chunk_size: int) -> Iterator[List[T]]:
    chunk = []
    for v in iterator:
        chunk.append(v)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    yield chunk


def add_header(itr: Iterable[T], header: Dict[str, Optional[T]]) -> Iterable[T]:
    first = True
    for v in itr:
        if header["header"] is None:
            header["header"] = v  # "return" this header value back to caller
        elif first:
            yield header["header"]
        yield v
        first = False


def with_header(iterator: Iterator[Iterable[T]]) -> Iterator[Iterable[T]]:
    header = {"header": None}
    for chunk in iterator:
        chunk = add_header(chunk, header)
        yield chunk


def sample_lines(lines: Iterator[str], n: int) -> List[str]:
    i = 0
    sample = []
    for ln in lines:
        sample.append(ln)
        i += 1
        if i == n:
            break
    return sample


def infer_csv_dialect(s: str) -> Type[csv.Dialect]:
    s = s.strip()
    dialect = csv.Sniffer().sniff(s, delimiters=";,|\t")
    return dialect


def is_maybe_csv(s: str) -> bool:
    if s.strip().startswith("{"):
        # Jsonl looks a lot like a csv...
        # This could generate a rare false negative if data happens to start with {
        return False
    try:
        infer_csv_dialect(s)
    except csv.Error:
        return False
    return True


def read_csv(lines: Iterable[AnyStr], dialect=None) -> Iterator[Dict]:
    if dialect is None:
        lines, lines_copy = tee(lines, 2)
        s = ""
        for i, ln in enumerate(ensure_strings(lines_copy)):
            if i >= 10:
                break
            s += ln
        dialect = infer_csv_dialect(s)
    lines = ensure_strings(lines)
    reader = csv.reader(lines, dialect=dialect)
    try:
        headers = next(reader)
    except StopIteration:
        return
    for line in reader:
        yield {h: process_raw_value(v) for h, v in zip(headers, line)}


def read_raw_string_csv(csv_str: str, **kwargs) -> Iterator[Dict]:
    lines = [ln.strip() for ln in csv_str.split("\n") if ln.strip()]
    return read_csv(lines, **kwargs)


def conform_to_csv_value(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, list) or isinstance(v, dict):
        return json.dumps(v, cls=DcpJsonEncoder)
    return v


def write_csv(
    records: List[Dict],
    file_like: IO,
    columns: List[str] = None,
    append: bool = False,
    dialect=DcpCsvDialect,
):
    writer = csv.writer(file_like, dialect=dialect)
    if not columns:
        columns = list(records[0].keys())  # Assumes all records have same keys...
    if not append:
        # Write header if not appending
        writer.writerow(columns)
    for record in records:
        row = []
        for c in columns:
            v = record.get(c)
            v = conform_to_csv_value(v)
            row.append(v)
        writer.writerow(row)


def read_json(j: str) -> Union[Dict, List]:
    return json.loads(j)  # TODO: de-serializer


def conform_records_for_insert(
    records: List[Dict],
    columns: List[str],
    adapt_objects_to_json: bool = True,
    conform_datetimes: bool = True,
):
    rows = []
    for r in records:
        row = []
        for c in columns:
            o = r.get(c)
            # TODO: this is some magic buried down here. no bueno
            if adapt_objects_to_json and (isinstance(o, list) or isinstance(o, dict)):
                o = json.dumps(o, cls=DcpJsonEncoder)
            if conform_datetimes:
                if isinstance(o, Timestamp):
                    o = o.to_pydatetime()
            row.append(o)
        rows.append(row)
    return rows


def head(file_obj: IOBase, n: int) -> Iterator:
    if not hasattr(file_obj, "seek"):
        raise TypeError("Missing seek method")
    i = 0
    for v in file_obj:
        if i >= n:
            break
        yield v
        i += 1
    file_obj.seek(0)
