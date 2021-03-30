from __future__ import annotations
from dcp.data_copy.base import CopyRequest


from cleo import Application, Command


class DcpCommand(Command):
    """
    Copy structured data between any two points

    dcp
        {from? : URL or local path of source object}
        {to? : URL or local path of destination object}
        {--c|cast : Cast level}
    """

    def handle(self):
        from_url = self.argument("from")
        to_url = self.argument("to")
        cast = self.option("cast")
        req: CopyRequest = make_copy_request(from_url, to_url)

        self.line(
            f"Copying `{req.from_name}`"
            f"on {req.from_storage.storage_engine}"
            f"({req.from_storage.url})"
            f"to `{req.to_name}`"
            f"on {req.to_storage.storage_engine}"
            f"({req.to_storage.url})"
        )

        data_copy(req)


command = DcpCommand()

app = Application()
app.add(command.default())

# this now executes the 'GreetCommand' without passing its name
app.run()
