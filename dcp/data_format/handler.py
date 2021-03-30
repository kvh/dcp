from __future__ import annotations

from enum import Enum
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Type

from schemas.base import Field, Schema
from schemas.field_types import FieldType
from dcp.data_format.base import DataFormat
from dcp.storage.base import (
    LocalPythonStorageEngine,
    StorageClass,
    StorageEngine,
    Storage,
)


class ErrorBehavior(Enum):
    FAIL = "FAIL"
    RELAX_TYPE = "RELAX_TYPE"
    SET_NULL = "SET_NULL"


class CastFieldOperation:
    operator: Callable
    error_behavior: ErrorBehavior

    def apply(self, name, storage, field: Field):
        raise NotImplementedError

    def on_error(self, name, storage, field: Field):
        raise NotImplementedError


# class PythonOperator:
#     operator: Callable


# # class SqlOperator:
#     operator: Query


class CastSchemaOperation:
    field_operations: Dict[str, CastFieldOperation]

    def apply(self, name, storage, schema: Schema):
        raise NotImplementedError

    def on_error(self, name, storage, schema: Schema):
        raise NotImplementedError


ALL_HANDLERS: List[Type[FormatHandler]] = []


class FormatHandler:
    for_data_formats: List[DataFormat]
    for_storage_classes: List[StorageClass]
    for_storage_engines: List[StorageEngine]
    sample_size: int = 100

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        assert cls.for_data_formats, "Must specify data formats"
        assert (
            cls.for_storage_engines or cls.for_storage_classes
        ), "Must specify storage classes or engines"
        ALL_HANDLERS.append(cls)

    def infer_field_names(self, name, storage) -> Iterable[str]:
        raise NotImplementedError

    def infer_field_type(self, name, storage, field) -> List[Field]:
        # For python storage and dataframe: map pd.dtypes -> ftypes
        # For python storage and records: infer py object type
        # For postgres storage and table: map sa types -> ftypes
        # For S3 storage and csv: infer csv types (use arrow?)
        raise NotImplementedError

    def cast_to_field_type(
        self, name: str, storage: Storage, field: str, field_type: FieldType
    ):
        raise NotImplementedError

    def create_empty(self, name: str, storage: Storage, schema: Schema):
        # For python storage and dataframe: map pd.dtypes -> ftypes
        # For python storage and records: infer py object type
        # For postgres storage and table: map sa types -> ftypes
        # For S3 storage and csv: infer csv types (use arrow?)
        raise NotImplementedError

    def supports(self, field_type) -> bool:
        # For python storage and dataframe: yes to almost all (nullable ints maybe)
        # For S3 storage and csv:
        raise NotImplementedError


def get_handler(
    data_format: DataFormat, storage_engine: Type[StorageEngine],
) -> Type[FormatHandler]:
    # TODO: can cache this stuff
    format_handlers = [
        handler for handler in ALL_HANDLERS if data_format in handler.for_data_formats
    ]
    # reverse so later added handlers take precedence
    for handler in reversed(format_handlers):
        if (
            handler.for_storage_engines
            and storage_engine in handler.for_storage_engines
        ):
            return handler
    for handler in reversed(format_handlers):
        if (
            handler.for_storage_classes
            and storage_engine.storage_class in handler.for_storage_classes
        ):
            return handler
    raise NotImplementedError(
        f"No format handler for {data_format} on {storage_engine}"
    )


# @format_handler(
#     for_data_formats=[DataFrameFormat],
#     for_storage_engines=[storage.LocalPythonStorageEngine],
# )
# class PythonDataframeHandler:
#     def infer_field_names(self, name, storage) -> List[str]:
#         pass

#     def infer_field_type(self, name, storage, field) -> List[Field]:
#         mro = storage.get_api().get(name)
#         df = mro.records_object
#         cast(DataFrame, df)
#         series = df[field]
#         ft = pandas_series_to_field_type(series)
#         return ft
#         # For python storage and dataframe: map pd.dtypes -> ftypes
#         # For python storage and records: infer py object type
#         # For postgres storage and table: map sa types -> ftypes
#         # For S3 storage and csv: infer csv types (use arrow?)

#     def cast_operation_for_field_type(
#         self, name, storage, field, field_type, cast_level
#     ):
#         pass

#     def create_empty(self, name, storage, schema):
#         # For python storage and dataframe: map pd.dtypes -> ftypes
#         # For python storage and records: infer py object type
#         # For postgres storage and table: map sa types -> ftypes
#         # For S3 storage and csv: infer csv types (use arrow?)
#         pass

#     def supports(self, field_type) -> bool:
#         # For python storage and dataframe: yes to almost all (nullable ints maybe)
#         # For S3 storage and csv:
#         pass