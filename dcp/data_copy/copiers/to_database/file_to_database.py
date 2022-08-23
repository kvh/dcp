from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.storage.database.api import DatabaseStorageApi

from dcp.data_copy.base import CopyRequest, DataCopierBase
from dcp.data_copy.costs import NetworkToBufferCost
from dcp.data_format.formats.file_system.csv_file import CsvFileFormat
from dcp.storage.base import (
    DatabaseStorageClass,
    FileSystemStorageClass,
)
from dcp.storage.file_system.engines.local import FileSystemStorageApi


class FileToDatabaseMixin:
    from_storage_classes = [FileSystemStorageClass]
    to_storage_classes = [DatabaseStorageClass]
    requires_schema_cast = False
    cost = NetworkToBufferCost

    def append(self, req: CopyRequest):
        with req.from_obj.storage.get_filesystem_api().open(req.from_obj) as f:
            req.to_obj.storage.get_database_api().bulk_insert_file(
                req.to_obj, f, schema=req.get_to_schema()
            )


class CsvFileToDatabaseTable(FileToDatabaseMixin, DataCopierBase):
    from_data_formats = [CsvFileFormat]
    to_data_formats = [DatabaseTableFormat]
