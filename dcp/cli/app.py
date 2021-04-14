from __future__ import annotations

from cleo.application import Application
from dcp.cli.command import DcpCommand

command = DcpCommand()

app = Application()
app.add(command.default())
app.run()
