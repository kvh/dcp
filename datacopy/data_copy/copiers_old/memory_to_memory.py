from datacopy.storage.base import StorageApi
from datacopy.data_copy.costs import FormatConversionCost, MemoryToMemoryCost
from datacopy.data_format.formats.memory.dataframe import DataFrameFormat
from datacopy.storage.memory.memory_records_object import as_records
from datacopy.data_format.formats.memory.records import Records, RecordsFormat
from datacopy.storage.memory.engines.python import PythonStorageApi
from openmodel.base import Schema
from datacopy.data_copy.conversion import Conversion
from datacopy.data_copy.base import datacopy
from typing import Sequence

import pandas as pd
import pyarrow as pa


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[RecordsFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[DataFrameFormat],
    cost=MemoryToMemoryCost + FormatConversionCost,
)
def copy_records_to_df(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr: Records = from_storage_api.get(from_name)
    df = pd.DataFrame(mdr.records_object)
    to_mdr = as_records(df, data_format=DataFrameFormat, schema=schema)
    to_storage_api.put(to_name, to_mdr)
    conversion.to_storage_format_handler.cast_to_schema(
        to_name, to_storage_api.storage, schema
    )


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[DataFrameFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[RecordsFormat],
    cost=MemoryToMemoryCost + FormatConversionCost,
)
def copy_df_to_records(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    df = dataframe_to_records(mdr.records_object, schema)
    to_mdr = as_records(df, data_format=RecordsFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[DataFrameIteratorFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[RecordsIteratorFormat],
    cost=BufferToBufferCost + FormatConversionCost,
)
def copy_df_iterator_to_records_iterator(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    itr = (dataframe_to_records(df, schema) for df in mdr.records_object)
    to_mdr = as_records(itr, data_format=RecordsIteratorFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[RecordsIteratorFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[DataFrameIteratorFormat],
    cost=BufferToBufferCost + FormatConversionCost,
)
def copy_records_iterator_to_df_iterator(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    itr = (pd.DataFrame(records) for records in mdr.records_object)
    to_mdr = as_records(itr, data_format=DataFrameIteratorFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[RecordsIteratorFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[RecordsFormat],
    cost=MemoryToMemoryCost,
)
def copy_records_iterator_to_records(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    all_records = []
    for records in mdr.records_object:
        all_records.extend(records)
    to_mdr = as_records(all_records, data_format=RecordsFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[DataFrameIteratorFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[DataFrameFormat],
    cost=MemoryToMemoryCost,
)
def copy_dataframe_iterator_to_dataframe(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    all_dfs = []
    for df in mdr.records_object:
        all_dfs.append(df)
    to_mdr = as_records(pd.concat(all_dfs), data_format=DataFrameFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[DelimitedFileObjectFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[RecordsFormat],
    cost=MemoryToBufferCost + FormatConversionCost,
)
def copy_file_object_to_records(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    obj = read_csv(mdr.records_object)
    to_mdr = as_records(obj, data_format=RecordsFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[DelimitedFileObjectFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[RecordsIteratorFormat],
    cost=BufferToBufferCost + FormatConversionCost,
)
def copy_file_object_to_records_iterator(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    # Note: must keep header on each chunk when iterating delimited file object!
    # TODO: ugly hard-coded 1000 here, but how could we ever make it configurable? Not a big deal I guess
    itr = (
        read_csv(chunk)
        for chunk in with_header(iterate_chunks(mdr.records_object, 1000))
    )
    to_mdr = as_records(itr, data_format=RecordsIteratorFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[DelimitedFileObjectIteratorFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[RecordsIteratorFormat],
    cost=BufferToBufferCost + FormatConversionCost,
)
def copy_file_object_iterator_to_records_iterator(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    itr = (read_csv(chunk) for chunk in with_header(mdr.records_object))
    to_mdr = as_records(itr, data_format=RecordsIteratorFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


#########
### Arrow
#########


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[ArrowTableFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[DataFrameFormat],
    cost=MemoryToMemoryCost,  # Sometimes this is a zero-copy no-op (rarely for real world data tho due to lack of null support in numpy)
)
def copy_arrow_to_dataframe(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    df = mdr.records_object.to_pandas()
    to_mdr = as_records(df, data_format=DataFrameFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[DataFrameFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[ArrowTableFormat],
    cost=MemoryToMemoryCost,
)
def copy_dataframe_to_arrow(
    from_name: str,
    to_name: str,
    conversion: Conversion,
    from_storage_api: StorageApi,
    to_storage_api: StorageApi,
    schema: Schema,
):
    assert isinstance(from_storage_api, PythonStorageApi)
    assert isinstance(to_storage_api, PythonStorageApi)
    mdr = from_storage_api.get(from_name)
    at = pa.Table.from_pandas(mdr.records_object)
    to_mdr = as_records(at, data_format=ArrowTableFormat, schema=schema)
    to_mdr = to_mdr.conform_to_schema()
    to_storage_api.put(to_name, to_mdr)
