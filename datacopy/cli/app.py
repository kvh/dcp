from __future__ import annotations
from datacopy.data_copy.graph import execute_copy_request
from datacopy.storage.base import Storage
from datacopy.data_copy.base import CopyRequest


from cleo import Application, Command


def make_copy_request(
    from_url: str, to_url: str, fmt: str = None, schema: str = None
) -> CopyRequest:
    from_split = from_url.split("/")
    to_split = from_url.split("/")
    from_name = from_split[-1]
    to_name = to_split[-1]
    from_storage_url = "/".join(from_split[:-1])
    to_storage_url = "/".join(to_split[:-1])
    to_storage = Storage(to_storage_url)
    to_fmt = to_storage.storage_engine.get_natural_format()
    return CopyRequest(
        from_name, Storage(from_storage_url), to_name, to_fmt, to_storage,
    )


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

        execute_copy_request(req)


command = DcpCommand()

app = Application()
app.add(command.default())

# this now executes the 'GreetCommand' without passing its name
app.run()
