import subprocess

from dcp.data_copy.base import CopyRequest, DataCopierBase
from dcp.data_copy.costs import DiskToBufferCost, DiskToMemoryCost, NetworkToMemoryCost
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.storage.base import DatabaseStorageClass, PostgresStorageEngine
from dcp.storage.database.api import DatabaseStorageApi


class DatabaseTableToDatabaseTable(DataCopierBase):
    from_storage_classes = [DatabaseStorageClass]
    from_data_formats = [DatabaseTableFormat]
    to_storage_classes = [DatabaseStorageClass]
    to_data_formats = [DatabaseTableFormat]
    cost = DiskToMemoryCost
    requires_schema_cast = False

    def append(self, req: CopyRequest):
        if req.to_obj.storage != req.from_obj.storage:
            self.copy_between_databases(req)
        else:
            self.copy_within_database(req)

    def copy_within_database(self, req: CopyRequest):
        insert_sql = f"insert into {req.to_obj.formatted_full_name} select * from {req.from_obj.formatted_full_name}"
        req.from_obj.storage.get_database_api().execute_sql(insert_sql)

    def copy_between_databases(self, req: CopyRequest):
        batch_size = 1000
        batch = []
        with req.from_obj.storage.get_database_api().execute_sql_result(
            f"select * from {req.from_obj.formatted_full_name}"
        ) as res:
            keys = res.keys()
            for row in res:
                record = dict(zip(keys, row))
                batch.append(record)
                if len(batch) >= batch_size:
                    req.to_obj.storage.get_database_api().bulk_insert_records(
                        req.to_obj, batch, req.get_to_schema()
                    )
                    batch = []
            req.to_obj.storage.get_database_api().bulk_insert_records(
                req.to_obj, batch, req.get_to_schema()
            )


class PostgresTableToPostgresTable(DatabaseTableToDatabaseTable):
    from_storage_engines = [PostgresStorageEngine]
    to_storage_engines = [PostgresStorageEngine]
    cost = DiskToBufferCost

    def copy_between_databases(self, req: CopyRequest):
        # TODO: this writes first to the `from_name` on the to_storage, then renames to `to_name`
        table_name = req.from_obj.full_path.name
        schema = req.from_obj.full_path.get_last_path_element()
        if schema:
            table_name = schema + "." + table_name
        dump_cmd = f"pg_dump {req.from_obj.storage.url} --table {table_name}"
        restore_cmd = f"psql {req.to_obj.storage.url}"
        p1 = subprocess.Popen(dump_cmd.split(), stdout=subprocess.PIPE)
        p2 = subprocess.Popen(
            restore_cmd.split(), stdin=p1.stdout, stdout=subprocess.PIPE
        )
        p1.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
        p2.communicate()[0]

        sql = f"drop table if exists {req.to_obj.formatted_full_name}"
        req.to_obj.storage.get_database_api().execute_sql(sql)
        sql = f"alter table {req.from_obj.formatted_full_name} rename to {req.to_obj.formatted_full_name}"
        req.to_obj.storage.get_database_api().execute_sql(sql)
