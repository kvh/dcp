# from dcp.data_copy.base import datacopy
# from dcp.data_copy.conversion import Conversion
# from dcp.data_copy.costs import (
#     DiskToBufferCost,
#     FormatConversionCost,
#     MemoryToMemoryCost,
# )
# from dcp.data_format.formats.database.base import DatabaseTableFormat
# from dcp.data_format.formats.memory.dataframe import DataFrameFormat
# from dcp.data_format.formats.memory.records import Records, RecordsFormat
# from dcp.storage.base import (
#     DatabaseStorageClass,
#     FileSystemStorageClass,
#     MemoryStorageClass,
#     StorageApi,
# )
# from dcp.storage.database.api import DatabaseStorageApi
# from dcp.storage.file_system.engines.local import FileSystemStorageApi
# from dcp.storage.memory.engines.python import PythonStorageApi
# from commonmodel.base import Schema


# @datacopier(
#     from_storage_classes=[FileSystemStorageClass],
#     from_data_formats=[CsvFileFormat],
#     to_storage_classes=[DatabaseStorageClass],
#     to_data_formats=[DatabaseTableFormat],
#     cost=DiskToBufferCost + FormatConversionCost,  # TODO
# )
# def copy_csv_to_table(
#     from_name: str,
#     to_name: str,
#     conversion: Conversion,
#     from_storage_api: StorageApi,
#     to_storage_api: StorageApi,
#     schema: Schema,
# ):
#     assert isinstance(from_storage_api, FileSystemStorageApi)
#     assert isinstance(to_storage_api, DatabaseStorageApi)

#     # create empty destination table
#     # load file using db command
#     # Handle errors .... ?
