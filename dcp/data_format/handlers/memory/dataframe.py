from typing import List, cast

from pandas.core.frame import DataFrame
from dcp.storage.base import LocalPythonStorageEngine
from dcp.data_format.formats import DataFrameFormat
from schemas import DateTime, Float, Integer, Boolean, DEFAULT_FIELD_TYPE, Date, Time


@format_handler(
    for_data_formats=[DataFrameFormat], for_storage_engines=[LocalPythonStorageEngine],
)
class PythonDataframeHandler:
    def infer_field_names(self, name, storage) -> List[str]:
        pass

    def infer_field_type(self, name, storage, field) -> List[Field]:
        mro = storage.get_api().get(name)
        df = mro.records_object
        cast(DataFrame, df)
        series = df[field]
        ft = pandas_series_to_field_type(series)
        return ft
        # For python storage and dataframe: map pd.dtypes -> ftypes
        # For python storage and records: infer py object type
        # For postgres storage and table: map sa types -> ftypes
        # For S3 storage and csv: infer csv types (use arrow?)

    def cast_operation_for_field_type(
        self, name, storage, field, field_type, cast_level
    ):
        pass

    def create_empty(self, name, storage, schema):
        # For python storage and dataframe: map pd.dtypes -> ftypes
        # For python storage and records: infer py object type
        # For postgres storage and table: map sa types -> ftypes
        # For S3 storage and csv: infer csv types (use arrow?)
        pass

    def supports(self, field_type) -> bool:
        # For python storage and dataframe: yes to almost all (nullable ints maybe)
        # For S3 storage and csv:
        pass


def pandas_series_to_field_type(series: Series) -> FieldType:
    """
    Cribbed from pandas.io.sql
    Changes:
        - strict datetime and JSON inference
        - No timezone handling
        - No single/32 numeric types
    """
    dtype = pd.api.types.infer_dtype(series, skipna=True).lower()
    if dtype == "datetime64" or dtype == "datetime":
        # GH 9086: TIMESTAMP is the suggested type if the column contains
        # timezone information
        # try:
        #     if col.dt.tz is not None:
        #         return TIMESTAMP(timezone=True)
        # except AttributeError:
        #     # The column is actually a DatetimeIndex
        #     if col.tz is not None:
        #         return TIMESTAMP(timezone=True)
        return DateTime()
    if dtype == "timedelta64":
        raise NotImplementedError  # TODO
    elif dtype.startswith("float"):
        return Float()
    elif dtype.startswith("int"):
        return Integer()
    elif dtype == "boolean":
        return Boolean()
    elif dtype == "date":
        return Date()
    elif dtype == "time":
        return Time()
    elif dtype == "complex":
        raise ValueError("Complex number datatype not supported")
    elif dtype == "empty":
        return DEFAULT_FIELD_TYPE
    else:
        # Handle object / string case as generic detection
        # try:
        return select_field_type(series.dropna().iloc[:100])
        # except ParserError:
        #     pass

