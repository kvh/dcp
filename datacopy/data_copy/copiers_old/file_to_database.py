from datacopy.storage.database.api import DatabaseStorageApi
from datacopy.storage.file_system.engines.local import FileSystemStorageApi
from datacopy.data_format.formats.database.base import DatabaseTableFormat
from datacopy.storage.base import (
    DatabaseStorageClass,
    FileSystemStorageClass,
    MemoryStorageClass,
    StorageApi,
)
from datacopy.data_copy.costs import (
    DiskToBufferCost,
    FormatConversionCost,
    MemoryToMemoryCost,
)
from datacopy.data_format.formats.memory.dataframe import DataFrameFormat
from datacopy.storage.memory.memory_records_object import as_records
from datacopy.data_format.formats.memory.records import Records, RecordsFormat
from datacopy.storage.memory.engines.python import PythonStorageApi
from openmodel.base import Schema
from datacopy.data_copy.base import datacopy


@datacopy(
    from_storage_classes=[FileSystemStorageClass],
    from_data_formats=[CsvFileFormat],
    to_storage_classes=[DatabaseStorageClass],
    to_data_formats=[DatabaseTableFormat],
    cost=DiskToBufferCost + FormatConversionCost,  # TODO
)
def copy_csv_to_table(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, FileSystemStorageApi)
    assert isinstance(to_storage_api, DatabaseStorageApi)

    # create empty destination table
    # load file using db command
    # Handle errors .... ?
