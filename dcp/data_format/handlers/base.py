

from dcp.storage.base import LocalPythonStorageEngine
from dcp.data_format.formats import DataFrameFormat


class ErrorBehavior(Enum):
    FAIL = 'FAIL'
    RELAX_TYPE = 'RELAX_TYPE'
    SET_NULL = 'SET_NULL'

class CastFieldOperation:
    operator: Callable
    error_behavior: ErrorBehavior

    def apply(self, name, storage, field: Field):
        raise NotImplementedError

    def on_error(self, name, storage, field: Field):
        raise NotImplementedError




class CastSchemaOperation:
    field_operations: Dict[str, CastFieldOperation]

    def apply(self, name, storage, schema: Schema):
        raise NotImplementedError

    def on_error(self, name, storage, schema: Schema):
        raise NotImplementedError


@format_handler(
    for_data_formats=[DataFrameFormat], for_storage_engines=[LocalPythonStorageEngine],
)
class PythonDataframeHandler:
    def infer_field_type(self, name, storage, field) -> List[Field]:
        # For python storage and dataframe: map pd.dtypes -> ftypes
        # For python storage and records: infer py object type
        # For postgres storage and table: map sa types -> ftypes
        # For S3 storage and csv: infer csv types (use arrow?)
        

    def cast_operation_for_field_type(self, name, storage, field, field_type, cast_level):
        pass

    def create_empty(self, name, storage, schema):
        # For python storage and dataframe: map pd.dtypes -> ftypes
        # For python storage and records: infer py object type
        # For postgres storage and table: map sa types -> ftypes
        # For S3 storage and csv: infer csv types (use arrow?)
    
    def supports(self, field_type) -> bool:
        # For python storage and dataframe: yes to almost all (nullable ints maybe)
        # For S3 storage and csv: 


@format_handler(
    for_data_formats=[], for_storage_classes=[], for_storage_engines=[],
)
class PythonDataframeHandler:
    def infer_field_type(self, name, storage, field) -> List[Field]:
        # For python storage and dataframe: map pd.dtypes -> ftypes
        # For python storage and records: infer py object type
        # For postgres storage and table: map sa types -> ftypes
        # For S3 storage and csv: infer csv types (use arrow?)
        

    def cast_operation_for_field_type(self, name, storage, field, field_type, cast_level):
        pass

    def create_empty(self, name, storage, schema):
        # For python storage and dataframe: map pd.dtypes -> ftypes
        # For python storage and records: infer py object type
        # For postgres storage and table: map sa types -> ftypes
        # For S3 storage and csv: infer csv types (use arrow?)
    
    def supports(self, field_type) -> bool:
        # For python storage and dataframe: yes to almost all (nullable ints maybe)
        # For S3 storage and csv: 