from __future__ import annotations

import json
import sys

import yaml
from cleo import Command

from dcp import Storage
from dcp.data_format.formats.memory.records import PythonRecordsHandler


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
        s.get_memory_api().put(n, [d])
        schema = PythonRecordsHandler().infer_schema(n, s)
        print(yaml.dump(schema.dict()))
