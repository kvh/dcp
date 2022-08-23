from __future__ import annotations

from enum import Enum
from typing import Callable, Dict, Iterable, List, Optional, Type

from commonmodel.base import Field, Schema
from commonmodel.field_types import FieldType

from dcp.data_format.base import DataFormat
from dcp.data_format.inference import generate_auto_schema
from dcp.storage.base import (
    LocalPythonStorageEngine,
    Storage,
    StorageClass,
    StorageEngine,
    StorageObject,
)

ALL_HANDLERS: List[Type[FormatHandler]] = []


class FormatHandler:
    for_data_formats: List[DataFormat]
    for_storage_classes: List[StorageClass] = []
    for_storage_engines: List[StorageEngine] = []
    sample_size: int = 100

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # Excluded intermediate base classes
        # if cls.__name__ in ["IterableFormatHandler"]:
        #     return
        assert cls.for_data_formats, "Must specify data formats"
        assert (
            cls.for_storage_engines or cls.for_storage_classes
        ), "Must specify storage classes or engines"
        ALL_HANDLERS.append(cls)

    # def __init__(self, storage: Storage):
    #     self.storage = storage

    def infer_data_format(self, so: StorageObject) -> Optional[DataFormat]:
        raise NotImplementedError

    def infer_field_names(self, obj: StorageObject) -> Iterable[str]:
        raise NotImplementedError

    def infer_field_type(self, obj: StorageObject, field: str) -> FieldType:
        # For python storage and dataframe: map pd.dtypes -> ftypes
        # For python storage and records: infer py object type
        # For postgres storage and table: map sa types -> ftypes
        # For S3 storage and csv: infer csv types (use arrow?)
        raise NotImplementedError

    def infer_schema(self, obj: StorageObject) -> Schema:
        fields = [
            Field(name=n, field_type=self.infer_field_type(obj, n))
            for n in self.infer_field_names(obj)
        ]
        schema = generate_auto_schema(fields=fields)
        return schema

    def cast_to_field_type(self, obj: StorageObject, field: str, field_type: FieldType):
        raise NotImplementedError

    def cast_to_schema(self, obj: StorageObject, schema: Schema):
        for field in schema.fields:
            self.cast_to_field_type(obj, field.name, field.field_type)

    def create_empty(self, obj: StorageObject, schema: Schema):
        """
        Throws error if exists
        """
        # For python storage and dataframe: map pd.dtypes -> ftypes
        # For python storage and records: infer py object type
        # For postgres storage and table: map sa types -> ftypes
        # For S3 storage and csv: infer csv types (use arrow?)
        raise NotImplementedError

    def supports(self, field_type) -> bool:
        # For python storage and dataframe: yes to almost all (nullable ints maybe)
        # For S3 storage and csv:
        raise NotImplementedError

    def get_record_count(self, obj: StorageObject) -> Optional[int]:
        # Will come directly from storage engine most of time, except python memory implemented here
        if obj.storage.storage_engine == LocalPythonStorageEngine:
            obj = obj.storage.get_memory_api().get(obj.formatted_full_name)
            return len(obj)
        raise NotImplementedError

    # def apply_schema_translation(
    #     self, name: str, storage: Storage, translation: SchemaTranslation
    # ):
    #     # Will come directly from storage engine most of time, except python memory implemented here
    #     raise NotImplementedError


# Too complex, messy; let a higher level layer handle buffering and iterators
# class IterableFormatHandler(FormatHandler):
#     for_data_formats: List[IterableDataFormat]
#     for_storage_engines = [LocalPythonStorageEngine]

#     @property
#     def for_data_format(self) -> IterableDataFormat:
#         assert len(self.for_data_formats) == 1
#         return self.for_data_formats[0]

#     def get_inner_format(self) -> DataFormat:
#         return self.for_data_format.inner_format

#     def get_inner_handler(self, storage: Storage) -> Type[FormatHandler]:
#         handler = get_handler(self.get_inner_format(), storage.storage_engine)
#         return handler

#     @contextmanager
#     def get_first_inner_object(self, name: str, storage: Storage):
#         obj = so.storage.get_memory_api().get(so)
#         assert isinstance(obj, SampleableIterator)
#         inner_obj = obj.get_first()
#         inner_name = name + "__inner__"
#         with storage.get_api().temp(inner_name, inner_obj):
#             yield inner_name

#     def infer_data_format(self, name: str, storage: Storage) -> Optional[DataFormat]:
#         obj = so.storage.get_memory_api().get(so)
#         if isinstance(obj, SampleableIterator):
#             with self.get_first_inner_object(name, storage) as inner_name:
#                 handler = self.get_inner_handler(storage)
#                 fmt = handler().infer_data_format(inner_name, storage)
#                 if fmt is not None:
#                     return fmt
#         return None

#     def infer_field_names(self, name: str, storage: Storage) -> Iterable[str]:
#         with self.get_first_inner_object(name, storage) as inner_name:
#             return self.get_inner_handler(storage)().infer_field_names(inner_name, storage)

#     def infer_field_type(self, name, storage: Storage, field: str) -> List[Field]:
#         with self.get_first_inner_object(name, storage) as inner_name:
#             return self.get_inner_handler(storage)().infer_field_type(inner_name, storage, field)


#     def cast_to_field_type(
#         self, name: str, storage: Storage, field: str, field_type: FieldType
#     ):
#         obj = so.storage.get_memory_api().get(so)
#         for inner_obj for obj in
#         return (self.get_inner_handler(storage).cast_to_field_type())

#     def cast_to_schema(self, name: str, storage: Storage, schema: Schema):
#         for field in schema.fields:
#             self.cast_to_field_type(name, storage, field.name, field.field_type)

#     def create_empty(self, name: str, storage: Storage, schema: Schema):
#         # For python storage and dataframe: map pd.dtypes -> ftypes
#         # For python storage and records: infer py object type
#         # For postgres storage and table: map sa types -> ftypes
#         # For S3 storage and csv: infer csv types (use arrow?)
#         raise NotImplementedError

#     def supports(self, field_type) -> bool:
#         # For python storage and dataframe: yes to almost all (nullable ints maybe)
#         # For S3 storage and csv:
#         raise NotImplementedError


def get_handler(
    data_format: DataFormat,
    storage_engine: Type[StorageEngine],
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


def infer_format(obj: StorageObject) -> DataFormat:
    format_handlers = get_handlers_for_storage(obj.storage)
    for handler in format_handlers:
        fmt = handler().infer_data_format(obj)
        if fmt is not None:
            return fmt
    msg = f"Could not infer format of object '{obj.formatted_full_name}' on storage {obj.storage}"
    if obj.storage.storage_engine is LocalPythonStorageEngine:
        py_obj = obj.storage.get_memory_api().get(obj.formatted_full_name)
        msg = f"Could not infer format of object '{obj.formatted_full_name}' `{py_obj}`"
    raise NotImplementedError(msg)


def get_handler_for_name(obj: StorageObject) -> Type[FormatHandler]:
    return get_handler(infer_format(obj), obj.storage.storage_engine)


def get_handlers_for_storage(storage: Storage) -> List[Type[FormatHandler]]:
    format_handlers = [
        handler
        for handler in ALL_HANDLERS
        if storage.storage_engine.storage_class in handler.for_storage_classes
        or storage.storage_engine in handler.for_storage_engines
    ]
    return format_handlers


def infer_schema_for_name(name: str, storage: Storage) -> Schema:
    return get_handler_for_name(name, storage)().infer_schema(name, storage)
