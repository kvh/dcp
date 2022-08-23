from __future__ import annotations

from cleo import Command
from dcp.cli.helpers import make_copy_request
from dcp.data_copy.base import CopyRequest
from dcp.data_copy.graph import execute_copy_request
from loguru import logger


class DcpCommand(Command):
    """
    Copy structured data between any two points

    dcp
        {from : URL or local path of source object}
        {to : URL or local path of destination object}
        {--c|cast : Cast level}
        {--f|to-format : DataFormat of destination object}
    """

    def handle(self):
        from_url = self.argument("from")
        to_url = self.argument("to")
        to_format = self.option("to-format")
        # cast = self.option("cast")
        # logger.enable("dcp")
        req: CopyRequest = make_copy_request(from_url, to_url, fmt=to_format)

        self.line(
            f"Copying `{req.from_obj.formatted_full_name}` "
            f"on {req.from_obj.storage.storage_engine.__name__} "
            f"({req.from_obj.storage.url}) "
            f"to `{req.to_obj.formatted_full_name}` "
            f"on {req.to_obj.storage.storage_engine.__name__} "
            f"({req.to_obj.storage.url})"
        )
        execute_copy_request(req)
