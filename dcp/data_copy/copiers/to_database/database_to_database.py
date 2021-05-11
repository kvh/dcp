from dcp.data_copy.base import CopyRequest, DataCopierBase
from dcp.data_copy.costs import DiskToMemoryCost, NetworkToMemoryCost
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.storage.base import DatabaseStorageClass
from dcp.storage.database.api import DatabaseStorageApi


class DatabaseTableToDatabaseTable(DataCopierBase):
    from_storage_classes = [DatabaseStorageClass]
    from_data_formats = [DatabaseTableFormat]
    to_storage_classes = [DatabaseStorageClass]
    to_data_formats = [DatabaseTableFormat]
    cost = DiskToMemoryCost  # TODO: pretty cheap version of this though... DiskToDisk?
    requires_schema_cast = False

    def append(self, req: CopyRequest):
        assert isinstance(req.from_storage_api, DatabaseStorageApi)
        assert isinstance(req.to_storage_api, DatabaseStorageApi)
        if req.to_storage != req.from_storage:
            raise NotImplementedError(
                "Table to table copy only implemented for same database"
            )
        insert_sql = f"insert into {req.to_name} select * from {req.from_name}"
        req.from_storage_api.execute_sql(insert_sql)
