from __future__ import annotations
from dcp.data_format.formats.memory.records import PythonRecordsHandler
from dcp import Storage

import sys
import json
from cleo import Command
from dcp.cli.helpers import make_copy_request
from dcp.data_copy.base import CopyRequest
from dcp.data_copy.graph import execute_copy_request
from loguru import logger

from commonmodel.base import schema_to_yaml


class InferCommand(Command):
    """
    Infer schema of object

    infer
    """

    def handle(self):
        s = ""
        for line in sys.stdin:
            s += line
        d = json.loads(s)
        s = Storage("python://infer")
        n = "_infer"
        s.get_api().put(n, [d])
        schema = PythonRecordsHandler().infer_schema(n, s)
        print(schema_to_yaml(schema))
