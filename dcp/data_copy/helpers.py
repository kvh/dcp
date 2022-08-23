from typing import Any

from commonmodel import Schema

from dcp import (
    Storage,
    PythonRecordsHandler,
    infer_format,
    get_handler_for_name,
    ensure_storage_object,
)


def infer_schema_from_python_object(obj: Any) -> Schema:
    s = Storage("python://infer")
    n = "_infer"
    if isinstance(obj, dict):
        obj = [obj]
    s.get_memory_api().put(n, obj)
    so = ensure_storage_object(n, storage=s)
    handler = get_handler_for_name(so)
    schema = handler().infer_schema(so)
    return schema
