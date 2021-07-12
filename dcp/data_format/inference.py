from typing import Union
from commonmodel.base import Schema
from dcp.utils.common import rand_str

AUTO_SCHEMA_NAMESPACE = "__auto__"


def generate_auto_schema(fields, **kwargs) -> Schema:
    auto_name = f"AutoSchema({rand_str(8)})"
    args = dict(
        name=auto_name,
        namespace=AUTO_SCHEMA_NAMESPACE,
        version="0.0.1",
        description="Automatically inferred schema",
        unique_on=[],
        implementations=[],
        fields=fields,
    )
    args.update(kwargs)
    return Schema(**args)


def is_auto_schema(schema: Union[str, Schema]) -> bool:
    if isinstance(schema, Schema):
        schema = schema.key
    return schema.startswith(AUTO_SCHEMA_NAMESPACE)
