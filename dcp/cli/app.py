from __future__ import annotations

from cleo.application import Application
from dcp.cli.command import DcpCommand
from dcp.cli.infer import InferCommand

command = DcpCommand()

app = Application()
app.add(command.default())
app.add(InferCommand())
app.run()
