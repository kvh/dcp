import pandas as pd
from sqlalchemy.engine import ResultProxy

from dcp.data_copy.base import CopyRequest, DataCopierBase
from dcp.data_copy.costs import (
    FormatConversionCost,
    MemoryToMemoryCost,
)
from dcp.data_format.formats.memory.arrow_table import ArrowTable, ArrowTableFormat
from dcp.data_format.formats.memory.database_cursor import (
    DatabaseCursorFormat,
    DatabaseCursor,
)
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.data_format.formats.memory.dataframe_iterator import (
    DataFrameIterator,
    DataFrameIteratorFormat,
)
from dcp.data_format.formats.memory.records import Records, RecordsFormat
from dcp.data_format.formats.memory.records_iterator import (
    RecordsIterator,
    RecordsIteratorFormat,
)
from dcp.storage.base import MemoryStorageClass
from dcp.storage.memory.engines.python import PythonStorageApi
from dcp.utils.pandas import dataframe_to_records

try:
    import pyarrow as pa

    PYARROW_SUPPORTED = True
except ImportError:
    PYARROW_SUPPORTED = False
    pa = None


class MemoryDataCopierMixin:
    from_storage_classes = [MemoryStorageClass]
    to_storage_classes = [MemoryStorageClass]

    def append(self, req: CopyRequest):
        new = req.from_obj.storage.get_memory_api().get(req.from_obj)
        existing = req.to_obj.storage.get_memory_api().get(req.to_obj)
        final = self.concat(existing, new)
        req.to_obj.storage.get_memory_api().put(req.to_obj, final)

    def concat(self, existing, new):
        raise NotImplementedError


class RecordsToDataframe(MemoryDataCopierMixin, DataCopierBase):
    from_data_formats = [RecordsFormat]
    to_data_formats = [DataFrameFormat]
    cost = MemoryToMemoryCost + FormatConversionCost
    requires_schema_cast = True

    def concat(self, existing: pd.DataFrame, new: Records) -> pd.DataFrame:
        df = pd.DataFrame(new)
        return pd.concat([existing, df])


class DataframeToRecords(MemoryDataCopierMixin, DataCopierBase):
    from_data_formats = [DataFrameFormat]
    to_data_formats = [RecordsFormat]
    cost = MemoryToMemoryCost + FormatConversionCost
    requires_schema_cast = False  # TODO: maybe?

    def concat(self, existing: Records, new: pd.DataFrame) -> Records:
        records = dataframe_to_records(new)
        return existing + records


# Self copies


class DataframeToDataframe(MemoryDataCopierMixin, DataCopierBase):
    from_data_formats = [DataFrameFormat]
    to_data_formats = [DataFrameFormat]
    cost = MemoryToMemoryCost
    requires_schema_cast = False

    def concat(self, existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([existing, new])


class RecordsToRecords(MemoryDataCopierMixin, DataCopierBase):
    from_data_formats = [RecordsFormat]
    to_data_formats = [RecordsFormat]
    cost = MemoryToMemoryCost
    requires_schema_cast = False

    def concat(self, existing: Records, new: Records) -> Records:
        return existing + new


### Iterators


class RecordsIteratorToDataframeIterator(MemoryDataCopierMixin, DataCopierBase):
    from_data_formats = [RecordsIteratorFormat]
    to_data_formats = [DataFrameIteratorFormat]
    cost = MemoryToMemoryCost + FormatConversionCost
    requires_schema_cast = True

    def concat(
        self, existing: DataFrameIterator, new: RecordsIterator
    ) -> DataFrameIterator:
        return DataFrameIterator(existing.iterator.concat(new))


### Database


class DatabaseCursorToRecordsIterator(MemoryDataCopierMixin, DataCopierBase):
    from_data_formats = [DatabaseCursorFormat]
    to_data_formats = [RecordsIteratorFormat]
    cost = MemoryToMemoryCost + FormatConversionCost
    requires_schema_cast = False

    def concat(self, existing: RecordsIterator, new: ResultProxy) -> DataFrameIterator:
        def f():
            keys = new.keys()
            for row in new:
                yield dict(zip(keys, row))

        return existing.concat(RecordsIterator(f(), new.close))


# @datacopier(
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
#     records_object = req.from_storage_api.get(req.from_name)
#     itr = (dataframe_to_records(df, req.get_schema()) for df in records_object)
#     to_records_object = as_records(itr, data_format=RecordsIteratorFormat, schema=req.get_schema())
#     to_records_object = to_records_object.conform_to_schema()
#     req.to_obj.storage.get_memory_api().put(req.to_obj.formatted_full_name, to_records_object)


# @datacopier(
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
#     records_object = req.from_storage_api.get(req.from_name)
#     itr = (pd.DataFrame(records) for records in records_object)
#     to_records_object = as_records(itr, data_format=DataFrameIteratorFormat, schema=req.get_schema())
#     to_records_object = to_records_object.conform_to_schema()
#     req.to_obj.storage.get_memory_api().put(req.to_obj.formatted_full_name, to_records_object)


# @datacopier(
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
#     records_object = req.from_storage_api.get(req.from_name)
#     all_records = []
#     for records in records_object:
#         all_records.extend(records)
#     to_records_object = as_records(all_records, data_format=RecordsFormat, schema=req.get_schema())
#     to_records_object = to_records_object.conform_to_schema()
#     req.to_obj.storage.get_memory_api().put(req.to_obj.formatted_full_name, to_records_object)


# @datacopier(
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
#     records_object = req.from_storage_api.get(req.from_name)
#     all_dfs = []
#     for df in records_object:
#         all_dfs.append(df)
#     to_records_object = as_records(pd.concat(all_dfs), data_format=DataFrameFormat, schema=req.get_schema())
#     to_records_object = to_records_object.conform_to_schema()
#     req.to_obj.storage.get_memory_api().put(req.to_obj.formatted_full_name, to_records_object)


# @datacopier(
#     from_storage_classes=[MemoryStorageClass],
#     from_data_formats=[CsvLinesIteratorFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[RecordsFormat],
#     cost=MemoryToBufferCost + FormatConversionCost,
# )
# def copy_csv_lines_to_records(req: CopyRequest):
#     assert isinstance(req.from_storage_api, PythonStorageApi)
#     assert isinstance(req.to_storage_api, PythonStorageApi)
#     csv_lines = req.from_storage_api.get(req.from_name)
#     records = list(read_csv(csv_lines))
#     create_empty_if_not_exists(req)
#     existing_records = req.to_obj.storage.get_memory_api().get(req.to_obj.formatted_full_name)
#     req.to_obj.storage.get_memory_api().put(req.to_obj.formatted_full_name, existing_records + records)
#     # Must cast because csv does a poor job of preserving logical types
#     req.to_format_handler.cast_to_schema(
#         req.to_name, req.to_storage_api.storage, req.get_schema()
#     )


# @datacopier(
#     from_storage_classes=[MemoryStorageClass],
#     from_data_formats=[RecordsFormat],
#     to_storage_classes=[MemoryStorageClass],
#     to_data_formats=[CsvLinesIteratorFormat],
#     cost=MemoryToBufferCost + FormatConversionCost,
# )
# def copy_records_to_csv_lines(req: CopyRequest):
#     assert isinstance(req.from_storage_api, PythonStorageApi)
#     assert isinstance(req.to_storage_api, PythonStorageApi)
#     records = req.from_storage_api.get(req.from_name)
#     create_empty_if_not_exists(req)
#     csv_lines = req.to_obj.storage.get_memory_api().get(req.to_obj.formatted_full_name)
#     f = StringIO()
#     write_csv(records, f, append=True)
#     f.seek(0)
#     req.to_obj.storage.get_memory_api().put(req.to_obj.formatted_full_name, chain(csv_lines, (ln for ln in f)))
#     # Casting does no good for a csv (no concept of types)
#     # req.to_format_handler.cast_to_schema(
#     #     req.to_name, req.to_storage_api.storage, req.get_schema()
#     # )


# @datacopier(
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
#     records_object = req.from_storage_api.get(req.from_name)
#     # Note: must keep header on each chunk when iterating delimited file object!
#     # TODO: ugly hard-coded 1000 here, but how could we ever make it configurable? Not a big deal I guess
#     itr = (
#         read_csv(chunk)
#         for chunk in with_header(iterate_chunks(records_object, 1000))
#     )
#     to_records_object = as_records(itr, data_format=RecordsIteratorFormat, schema=req.get_schema())
#     to_records_object = to_records_object.conform_to_schema()
#     req.to_obj.storage.get_memory_api().put(req.to_obj.formatted_full_name, to_records_object)


# @datacopier(
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
#     records_object = req.from_storage_api.get(req.from_name)
#     itr = (read_csv(chunk) for chunk in with_header(records_object))
#     to_records_object = as_records(itr, data_format=RecordsIteratorFormat, schema=req.get_schema())
#     to_records_object = to_records_object.conform_to_schema()
#     req.to_obj.storage.get_memory_api().put(req.to_obj.formatted_full_name, to_records_object)


#########
### Arrow
#########


class ArrowTableToDataFrame(MemoryDataCopierMixin, DataCopierBase):
    from_data_formats = [ArrowTableFormat]
    to_data_formats = [DataFrameFormat]
    cost = MemoryToMemoryCost
    requires_schema_cast = False  # TODO: maybe?

    def concat(self, existing: pd.DataFrame, new: ArrowTable) -> pd.DataFrame:
        if not PYARROW_SUPPORTED:
            raise ImportError("Pyarrow is not installed")
        new_df = new.to_pandas()
        return pd.concat([existing, new_df])


class DataFrameToArrowTable(MemoryDataCopierMixin, DataCopierBase):
    from_data_formats = [DataFrameFormat]
    to_data_formats = [ArrowTableFormat]
    cost = MemoryToMemoryCost
    requires_schema_cast = False  # TODO: maybe?

    def concat(self, existing: ArrowTable, new: pd.DataFrame) -> ArrowTable:
        if not PYARROW_SUPPORTED:
            raise ImportError("Pyarrow is not installed")
        new_at = pa.Table.from_pandas(new)
        existing = pa.Table.from_batches(existing.to_batches() + new_at.to_batches())
        return existing
