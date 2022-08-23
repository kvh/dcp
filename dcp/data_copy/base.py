from __future__ import annotations

from dataclasses import dataclass
from typing import (
    Callable,
    List,
    Optional,
    Type,
    TYPE_CHECKING,
    Any,
)

from commonmodel.base import Schema

from dcp.data_copy.costs import DataCopyCost
from dcp.data_format.base import DataFormat
from dcp.storage.base import (
    Storage,
    StorageClass,
    StorageEngine,
    StorageObject,
    FullPath,
)
from dcp.storage.memory.engines.python import DEFAULT_PYTHON_STORAGE
from dcp.utils.common import rand_str

if TYPE_CHECKING:
    from dcp.data_copy.graph import CopyResult


@dataclass(frozen=True)
class StorageFormat:
    storage_engine: Type[StorageEngine]
    data_format: DataFormat

    def __str__(self):
        return f"{self.storage_engine.__name__}:{self.data_format.__name__}"


@dataclass(frozen=True)
class Conversion:
    from_storage_format: StorageFormat
    to_storage_format: StorageFormat


@dataclass
class CopyRequest:
    from_obj: StorageObject
    to_obj: StorageObject
    available_storages: Optional[List[Storage]] = None
    if_exists: str = "error"  # in {"error", "append", "replace"}
    delete_intermediate: bool = False

    @property
    def conversion(self) -> Conversion:
        return Conversion(
            from_storage_format=StorageFormat(
                storage_engine=self.from_obj.storage.storage_engine,
                data_format=self.from_obj.get_data_format(),
            ),
            to_storage_format=StorageFormat(
                storage_engine=self.to_obj.storage.storage_engine,
                data_format=self.to_obj.get_data_format(),
            ),
        )

    def get_available_storages(self) -> List[Storage]:
        return list(
            set(
                [self.from_obj.storage, self.to_obj.storage]
                + (self.available_storages or [DEFAULT_PYTHON_STORAGE])
            )
        )

    def get_to_schema(self) -> Schema:
        schema = self.to_obj._schema
        if schema is None:
            schema = self.from_obj.get_schema()
        return schema


CopierCallabe = Callable[[CopyRequest], None]


class NameExistsError(Exception):
    pass


class DataCopierBase:
    cost: DataCopyCost
    requires_schema_cast: bool
    from_storage_classes: Optional[List[Type[StorageClass]]] = None
    from_storage_engines: Optional[List[Type[StorageEngine]]] = None
    from_data_formats: Optional[List[DataFormat]] = None
    to_storage_classes: Optional[List[Type[StorageClass]]] = None
    to_storage_engines: Optional[List[Type[StorageEngine]]] = None
    to_data_formats: Optional[List[DataFormat]] = None
    supports_append: bool = True
    request: CopyRequest
    unregistered: bool = False

    def __init_subclass__(cls) -> None:
        if not cls.unregistered:
            ALL_DATA_COPIERS.append(cls())

    def __eq__(self, o: object) -> bool:
        return o.__class__ is self.__class__

    def create_empty(self, req: CopyRequest):
        create_empty_if_not_exists(req)
        # raise NotImplementedError

    def append(self, req: CopyRequest):
        raise NotImplementedError

    def copy(self, req: CopyRequest):
        self.check_if_exists(req)
        self.create_empty(req)
        self.append(req)
        if self.requires_schema_cast:
            self.cast_to_schema(req)

    def cast_to_schema(self, req: CopyRequest):
        req.to_obj.format_handler.cast_to_schema(req.to_obj, req.get_to_schema())

    def check_if_exists(self, req: CopyRequest):
        if req.if_exists == "replace":
            return
        exists = req.to_obj.storage.get_api().exists(req.to_obj)
        if not exists:
            return
        if req.if_exists == "error":
            raise NameExistsError(
                f"{req.to_obj.formatted_full_name} already exists on {req.to_obj.storage} (if_exists=='error')"
            )
        elif req.if_exists == "append":
            if not self.supports_append:
                raise NameExistsError(
                    f"{req.to_obj.formatted_full_name} already exists on {req.to_obj.storage}, and append not supported (if_exists=='append')"
                )

    def can_handle_from(self, from_storage_format: StorageFormat) -> bool:
        if self.from_storage_classes:
            if (
                from_storage_format.storage_engine.storage_class
                not in self.from_storage_classes
            ):
                return False
        if self.from_storage_engines:
            if from_storage_format.storage_engine not in self.from_storage_engines:
                return False
        if self.from_data_formats:
            if from_storage_format.data_format not in self.from_data_formats:
                return False
        return True

    def can_handle_to(self, to_storage_format: StorageFormat) -> bool:
        if self.to_storage_classes:
            if (
                to_storage_format.storage_engine.storage_class
                not in self.to_storage_classes
            ):
                return False
        if self.to_storage_engines:
            if to_storage_format.storage_engine not in self.to_storage_engines:
                return False
        if self.to_data_formats:
            if to_storage_format.data_format not in self.to_data_formats:
                return False
        return True

    def can_handle(self, conversion: Conversion) -> bool:
        return self.can_handle_from(
            conversion.from_storage_format
        ) and self.can_handle_to(conversion.to_storage_format)


# Helper (belongs somewhere else?)
def create_empty_if_not_exists(req: CopyRequest):
    exists = req.to_obj.storage.get_api().exists(req.to_obj)
    if exists and req.if_exists == "replace":
        req.to_obj.storage.get_api().remove(req.to_obj)
        exists = False
    if not exists:
        req.to_obj.format_handler.create_empty(req.to_obj, req.get_to_schema())


ALL_DATA_COPIERS = []


def copy(
    from_name: str,
    from_storage: Storage | str,
    to_name: str,
    to_storage: Storage | str,
    to_format: DataFormat = None,
    to_schema: Optional[Schema] = None,
    available_storages: Optional[List[Storage]] = None,
    if_exists: str = "error",
    delete_intermediate: bool = False,
    from_format: Optional[DataFormat] = None,
    from_schema: Optional[Schema] = None,
    from_path: list[str] = None,
    to_path: list[str] = None,
):
    from dcp.data_copy.graph import execute_copy_request

    if isinstance(from_storage, str):
        from_storage = Storage(from_storage)
    if isinstance(to_storage, str):
        to_storage = Storage(to_storage)
    return execute_copy_request(
        CopyRequest(
            from_obj=StorageObject(
                storage=from_storage,
                full_path=FullPath(name=from_name, path=from_path),
                _data_format=from_format,
                _schema=from_schema,
            ),
            to_obj=StorageObject(
                storage=to_storage,
                full_path=FullPath(name=to_name, path=to_path),
                _data_format=to_format,
                _schema=to_schema,
            ),
            available_storages=available_storages,
            if_exists=if_exists,
            delete_intermediate=delete_intermediate,
        )
    )


def copy_objects(
    from_obj: StorageObject,
    to_obj: StorageObject,
    available_storages: Optional[List[Storage]] = None,
    if_exists: str = "error",
    delete_intermediate: bool = True,
) -> CopyResult:
    from dcp.data_copy.graph import execute_copy_request

    return execute_copy_request(
        CopyRequest(
            from_obj=from_obj,
            to_obj=to_obj,
            available_storages=available_storages,
            if_exists=if_exists,
            delete_intermediate=delete_intermediate,
        )
    )


def copy_python_object(
    from_python_obj: Any,
    to_name: str,
    to_storage: Storage | str,
    to_format: DataFormat = None,
    to_schema: Optional[Schema] = None,
    available_storages: Optional[List[Storage]] = None,
    if_exists: str = "error",
    delete_intermediate: bool = False,
    from_format: Optional[DataFormat] = None,
    from_schema: Optional[Schema] = None,
    to_path: list[str] = None,
):
    mem_storage = DEFAULT_PYTHON_STORAGE
    name = rand_str()
    mem_storage.get_memory_api().put(name, from_python_obj)
    try:
        return copy(
            from_name=name,
            from_storage=mem_storage,
            to_name=to_name,
            to_storage=to_storage,
            to_format=to_format,
            to_schema=to_schema,
            available_storages=available_storages,
            if_exists=if_exists,
            delete_intermediate=delete_intermediate,
            from_format=from_format,
            from_schema=from_schema,
            to_path=to_path,
        )
    finally:
        mem_storage.get_memory_api().remove(name)
