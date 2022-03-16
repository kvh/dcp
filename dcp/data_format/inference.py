from typing import Union
from commonmodel.base import Schema
from dcp.utils.common import rand_str, utcnow

GENERATED_SCHEMA_PREFIX = "_GeneratedSchema_"


def generate_auto_schema(fields, **kwargs) -> Schema:
    auto_name = f"{GENERATED_SCHEMA_PREFIX}{rand_str(10)}"
    args = dict(
        name=auto_name,
        description=f"Automatically inferred from data at {utcnow()}",
        unique_on=[],
        fields=fields,
    )
    args.update(kwargs)
    return Schema(**args)


def is_generated_schema(schema: Union[str, Schema]) -> bool:
    if isinstance(schema, Schema):
        schema = schema.name
    return schema.startswith(GENERATED_SCHEMA_PREFIX)


is_auto_schema = is_generated_schema  # Deprecated name
