from dcp.utils.common import rand_str
from commonmodel.base import Schema


def generate_auto_schema(fields, **kwargs) -> Schema:
    auto_name = f"AutoSchema({rand_str(8)})"
    args = dict(
        name=auto_name,
        namespace="__auto__",
        version="0.0.1",
        description="Automatically inferred schema",
        unique_on=[],
        implementations=[],
        fields=fields,
    )
    args.update(kwargs)
    return Schema(**args)
