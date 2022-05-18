from typing import Any

from commonmodel import Schema

from dcp import (
    Storage,
    PythonRecordsHandler,
    infer_format_for_name,
    get_handler_for_name,
)


def infer_schema_from_python_object(obj: Any) -> Schema:
    s = Storage("python://infer")
    n = "_infer"
    if isinstance(obj, dict):
        obj = [obj]
    s.get_api().put(n, obj)
    handler = get_handler_for_name(n, s)
    schema = handler().infer_schema(n, s)
    return schema
