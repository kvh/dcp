from dcp.storage.database.api import DatabaseStorageApi
from dcp.storage.base import DatabaseStorageClass
from dcp.data_copy.base import CopyRequest, create_empty_if_not_exists, datacopier
from dcp.data_copy.costs import NetworkToMemoryCost, DiskToMemoryCost
from dcp.data_format.formats.database.base import DatabaseTableFormat


@datacopier(
    from_storage_classes=[DatabaseStorageClass],
    from_data_formats=[DatabaseTableFormat],
    to_storage_classes=[DatabaseStorageClass],
    to_data_formats=[DatabaseTableFormat],
    cost=DiskToMemoryCost,
)
def copy_db_to_db(req: CopyRequest):
    assert isinstance(req.from_storage_api, DatabaseStorageApi)
    assert isinstance(req.to_storage_api, DatabaseStorageApi)
    if req.to_storage != req.from_storage:
        raise NotImplementedError(
            f"Table to table copy only implemented for same database"
        )
    insert_sql = f"insert into {req.to_name} select * from {req.from_name}"
    create_empty_if_not_exists(req)
    req.from_storage_api.execute_sql(insert_sql)
