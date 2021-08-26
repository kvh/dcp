from typing import Union
from commonmodel.base import Schema
from dcp.utils.common import rand_str

GENERATED_SCHEMA_NAMESPACE = "_generated"


def generate_auto_schema(fields, **kwargs) -> Schema:
    auto_name = f"GeneratedSchema{rand_str(10)}"
    args = dict(
        name=auto_name,
        namespace=GENERATED_SCHEMA_NAMESPACE,
        version="0.0.1",
        description="Automatically inferred schema",
        unique_on=[],
        implementations=[],
        fields=fields,
    )
    args.update(kwargs)
    return Schema(**args)


def is_generated_schema(schema: Union[str, Schema]) -> bool:
    if isinstance(schema, Schema):
        schema = schema.key
    return schema.startswith(GENERATED_SCHEMA_NAMESPACE)


is_auto_schema = is_generated_schema  # Deprecated name

