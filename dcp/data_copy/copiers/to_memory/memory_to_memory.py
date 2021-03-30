from dcp.utils.pandas import dataframe_to_records
from dcp.storage.base import MemoryStorageClass, StorageApi
from dcp.data_copy.costs import FormatConversionCost, MemoryToMemoryCost
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.storage.memory.memory_records_object import as_records
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.storage.memory.engines.python import PythonStorageApi
from dcp.data_copy.base import CopyRequest, datacopy
from typing import TypeVar

import pandas as pd

try:
    import pyarrow as pa
except ImportError:
    pa = TypeVar("pa")


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[RecordsFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[DataFrameFormat],
    cost=MemoryToMemoryCost + FormatConversionCost,
)
def copy_records_to_df(req: CopyRequest):
    assert isinstance(req.from_storage_api, PythonStorageApi)
    assert isinstance(req.to_storage_api, PythonStorageApi)
    mdr = req.from_storage_api.get(req.from_name)
    df = pd.DataFrame(mdr.records_object)
    to_mdr = as_records(df, data_format=DataFrameFormat, schema=req.schema)
    req.to_storage_api.put(req.to_name, to_mdr)
    # Does this belong here? Or is this a separate step?
    # The copier is responsible for preserving logical types, but not fixing mis-typed values
    # So, if the type is right in the python records, when will it NOT be right in pandas? that's
    # all we are worried about
    # req.to_format_handler.cast_to_schema(
    #     req.to_name, req.to_storage_api.storage, req.schema
    # )


@datacopy(
    from_storage_classes=[MemoryStorageClass],
    from_data_formats=[DataFrameFormat],
    to_storage_classes=[MemoryStorageClass],
    to_data_formats=[RecordsFormat],
    cost=MemoryToMemoryCost + FormatConversionCost,
)
def copy_df_to_records(req: CopyRequest):
    assert isinstance(req.from_storage_api, PythonStorageApi)
    assert isinstance(req.to_storage_api, PythonStorageApi)
    mdr = req.from_storage_api.get(req.from_name)
    df = dataframe_to_records(mdr.records_object)
    to_mdr = as_records(df, data_format=RecordsFormat, schema=req.schema)
    req.to_storage_api.put(req.to_name, to_mdr)
    # Only necessary if we think there is datatype loss when converting df->records
    # req.to_format_handler.cast_to_schema(
    #     req.to_name, req.to_storage_api.storage, req.schema
    # )


# @datacopy(
#     from_storage_classes=[MemoryStorageClass],
#     from_data_formats=[DataFrameIteratorFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[RecordsIteratorFormat],
#     cost=BufferToBufferCost + FormatConversionCost,
# )
# def copy_df_iterator_to_records_iterator(
# req: CopyRequest
# ):
#     assert isinstance(req.from_storage_api, PythonStorageApi)
#     assert isinstance(req.to_storage_api, PythonStorageApi)
#     mdr = req.from_storage_api.get(req.from_name)
#     itr = (dataframe_to_records(df, req.schema) for df in mdr.records_object)
#     to_mdr = as_records(itr, data_format=RecordsIteratorFormat, schema=req.schema)
#     to_mdr = to_mdr.conform_to_schema()
#     req.to_storage_api.put(req.to_name, to_mdr)


# @datacopy(
#     from_storage_classes=[MemoryStorageClass],
#     from_data_formats=[RecordsIteratorFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[DataFrameIteratorFormat],
#     cost=BufferToBufferCost + FormatConversionCost,
# )
# def copy_records_iterator_to_df_iterator(
# req: CopyRequest
# ):
#     assert isinstance(req.from_storage_api, PythonStorageApi)
#     assert isinstance(req.to_storage_api, PythonStorageApi)
#     mdr = req.from_storage_api.get(req.from_name)
#     itr = (pd.DataFrame(records) for records in mdr.records_object)
#     to_mdr = as_records(itr, data_format=DataFrameIteratorFormat, schema=req.schema)
#     to_mdr = to_mdr.conform_to_schema()
#     req.to_storage_api.put(req.to_name, to_mdr)


# @datacopy(
#     from_storage_classes=[MemoryStorageClass],
#     from_data_formats=[RecordsIteratorFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[RecordsFormat],
#     cost=MemoryToMemoryCost,
# )
# def copy_records_iterator_to_records(
# req: CopyRequest
# ):
#     assert isinstance(req.from_storage_api, PythonStorageApi)
#     assert isinstance(req.to_storage_api, PythonStorageApi)
#     mdr = req.from_storage_api.get(req.from_name)
#     all_records = []
#     for records in mdr.records_object:
#         all_records.extend(records)
#     to_mdr = as_records(all_records, data_format=RecordsFormat, schema=req.schema)
#     to_mdr = to_mdr.conform_to_schema()
#     req.to_storage_api.put(req.to_name, to_mdr)


# @datacopy(
#     from_storage_classes=[MemoryStorageClass],
#     from_data_formats=[DataFrameIteratorFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[DataFrameFormat],
#     cost=MemoryToMemoryCost,
# )
# def copy_dataframe_iterator_to_dataframe(
# req: CopyRequest
# ):
#     assert isinstance(req.from_storage_api, PythonStorageApi)
#     assert isinstance(req.to_storage_api, PythonStorageApi)
#     mdr = req.from_storage_api.get(req.from_name)
#     all_dfs = []
#     for df in mdr.records_object:
#         all_dfs.append(df)
#     to_mdr = as_records(pd.concat(all_dfs), data_format=DataFrameFormat, schema=req.schema)
#     to_mdr = to_mdr.conform_to_schema()
#     req.to_storage_api.put(req.to_name, to_mdr)


# @datacopy(
#     from_storage_classes=[MemoryStorageClass],
#     from_data_formats=[DelimitedFileObjectFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[RecordsFormat],
#     cost=MemoryToBufferCost + FormatConversionCost,
# )
# def copy_file_object_to_records(
# req: CopyRequest
# ):
#     assert isinstance(req.from_storage_api, PythonStorageApi)
#     assert isinstance(req.to_storage_api, PythonStorageApi)
#     mdr = req.from_storage_api.get(req.from_name)
#     obj = read_csv(mdr.records_object)
#     to_mdr = as_records(obj, data_format=RecordsFormat, schema=req.schema)
#     to_mdr = to_mdr.conform_to_schema()
#     req.to_storage_api.put(req.to_name, to_mdr)


# @datacopy(
#     from_storage_classes=[MemoryStorageClass],
#     from_data_formats=[DelimitedFileObjectFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[RecordsIteratorFormat],
#     cost=BufferToBufferCost + FormatConversionCost,
# )
# def copy_file_object_to_records_iterator(
# req: CopyRequest
# ):
#     assert isinstance(req.from_storage_api, PythonStorageApi)
#     assert isinstance(req.to_storage_api, PythonStorageApi)
#     mdr = req.from_storage_api.get(req.from_name)
#     # Note: must keep header on each chunk when iterating delimited file object!
#     # TODO: ugly hard-coded 1000 here, but how could we ever make it configurable? Not a big deal I guess
#     itr = (
#         read_csv(chunk)
#         for chunk in with_header(iterate_chunks(mdr.records_object, 1000))
#     )
#     to_mdr = as_records(itr, data_format=RecordsIteratorFormat, schema=req.schema)
#     to_mdr = to_mdr.conform_to_schema()
#     req.to_storage_api.put(req.to_name, to_mdr)


# @datacopy(
#     from_storage_classes=[MemoryStorageClass],
#     from_data_formats=[DelimitedFileObjectIteratorFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[RecordsIteratorFormat],
#     cost=BufferToBufferCost + FormatConversionCost,
# )
# def copy_file_object_iterator_to_records_iterator(
# req: CopyRequest
# ):
#     assert isinstance(req.from_storage_api, PythonStorageApi)
#     assert isinstance(req.to_storage_api, PythonStorageApi)
#     mdr = req.from_storage_api.get(req.from_name)
#     itr = (read_csv(chunk) for chunk in with_header(mdr.records_object))
#     to_mdr = as_records(itr, data_format=RecordsIteratorFormat, schema=req.schema)
#     to_mdr = to_mdr.conform_to_schema()
#     req.to_storage_api.put(req.to_name, to_mdr)


# #########
# ### Arrow
# #########


# @datacopy(
#     from_storage_classes=[MemoryStorageClass],
#     from_data_formats=[ArrowTableFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[DataFrameFormat],
#     cost=MemoryToMemoryCost,  # Sometimes this is a zero-copy no-op (rarely for real world data tho due to lack of null support in numpy)
# )
# def copy_arrow_to_dataframe(
# req: CopyRequest
# ):
#     assert isinstance(req.from_storage_api, PythonStorageApi)
#     assert isinstance(req.to_storage_api, PythonStorageApi)
#     mdr = req.from_storage_api.get(req.from_name)
#     df = mdr.records_object.to_pandas()
#     to_mdr = as_records(df, data_format=DataFrameFormat, schema=req.schema)
#     to_mdr = to_mdr.conform_to_schema()
#     req.to_storage_api.put(req.to_name, to_mdr)


# @datacopy(
#     from_storage_classes=[MemoryStorageClass],
#     from_data_formats=[DataFrameFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[ArrowTableFormat],
#     cost=MemoryToMemoryCost,
# )
# def copy_dataframe_to_arrow(
# req: CopyRequest
# ):
#     assert isinstance(req.from_storage_api, PythonStorageApi)
#     assert isinstance(req.to_storage_api, PythonStorageApi)
#     mdr = req.from_storage_api.get(req.from_name)
#     at = pa.Table.from_pandas(mdr.records_object)
#     to_mdr = as_records(at, data_format=ArrowTableFormat, schema=req.schema)
#     to_mdr = to_mdr.conform_to_schema()
#     req.to_storage_api.put(req.to_name, to_mdr)
