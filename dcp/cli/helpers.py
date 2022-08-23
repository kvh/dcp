from __future__ import annotations

import os

from dcp.data_copy.base import CopyRequest, copy
from dcp.data_format.base import get_format_for_nickname
from dcp.storage.base import Storage, ensure_storage_object


def make_copy_request(
    from_url: str, to_url: str, fmt: str = None, schema: str = None
) -> CopyRequest:
    from_split = from_url.split("/")
    to_split = to_url.split("/")
    from_name = from_split[-1]
    to_name = to_split[-1]
    from_storage_url = "/".join(from_split[:-1])
    to_storage_url = "/".join(to_split[:-1])
    to_storage = Storage(to_storage_url)
    if fmt:
        to_fmt = get_format_for_nickname(fmt)
    else:
        to_fmt = to_storage.storage_engine.get_natural_format()
    if not from_storage_url:
        # No storage url then default to local file
        pth = os.getcwd()
        from_storage_url = f"file://{pth}"
    return CopyRequest(
        from_obj=ensure_storage_object(from_name, storage=Storage(from_storage_url)),
        to_obj=ensure_storage_object(
            to_name, storage=to_storage, _data_format=to_fmt, _schema=schema
        ),
    )
