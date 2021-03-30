from dcp.storage.database.api import DatabaseStorageApi
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.storage.base import DatabaseStorageClass, StorageApi
from dcp.data_copy.costs import (
    FormatConversionCost,
    MemoryToMemoryCost,
    NetworkToMemoryCost,
)
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.storage.memory.memory_records_object import as_records
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.storage.memory.engines.python import PythonStorageApi
from schemas.base import Schema
from dcp.data_copy.conversion import Conversion
from dcp.data_copy.base import datacopy
from typing import Sequence


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[RecordsFormat],
    to_storage_classes=[DatabaseStorageClass],
    to_data_formats=[DatabaseTableFormat],
    cost=NetworkToMemoryCost,
)
def copy_records_to_db(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, DatabaseStorageApi)
    mdr = from_storage_api.get(from_name)
    conversion.to_storage_format_handler.create_empty(
        to_name, to_storage_api.storage, schema
    )
    to_storage_api.bulk_insert_records(to_name, mdr.records_object)


@datacopy(
    from_storage_classes=[PythonStorageClass],
    from_data_formats=[RecordsIteratorFormat],
    to_storage_classes=[DatabaseStorageClass],
    to_data_formats=[DatabaseTableFormat],
    cost=NetworkToBufferCost,
)
def copy_records_iterator_to_db(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, DatabaseStorageApi)
    mdr = from_storage_api.get(from_name)
    for records in mdr.records_object:
        to_storage_api.bulk_insert_records(to_name, records, schema)
