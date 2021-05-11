import os

import networkx
import pytest
from cleo import Application, CommandTester
from dcp.cli.command import DcpCommand
from dcp.cli.helpers import make_copy_request
from dcp.data_copy.base import ALL_DATA_COPIERS, CopyRequest
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.storage.base import Storage


def test_make_copy_request():
    name = "orders.csv"
    to_name = "orders"
    to_storage = "mysql://localhost:3306/mydb"
    to_url = f"{to_storage}/{to_name}"
    req = make_copy_request(name, to_url)
    pth = os.getcwd()
    assert req == CopyRequest(
        from_name=name,
        from_storage=Storage(f"file://{pth}"),
        to_name=to_name,
        to_storage=Storage(to_storage),
        to_format=DatabaseTableFormat,
    )


def test_execute():
    application = Application()
    application.add(DcpCommand())
    command = application.find("dcp")
    command_tester = CommandTester(command)
    with pytest.raises(FileNotFoundError):
        command_tester.execute("orders.csv mysql://localhost:3306/mydb/orders")
