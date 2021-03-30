from __future__ import annotations
from dcp.data_copy.costs import DataCopyCost

from schemas.base import AnySchema, Schema
from dcp.data_format.base import DataFormat
from dcp.storage.base import Storage

import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import (
    Any,
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)

import networkx as nx


@dataclass(frozen=True)
class CopyRequest:
    from_name: str
    from_storage: Storage
    to_name: str
    to_format: DataFormat
    to_storage: Storage
    schema: Schema = None

    @property
    def from_storage_api(self):
        return self.from_storage.get_api()

    @property
    def to_storage_api(self):
        return self.to_storage.get_api()


def copy(
    from_object: Any = None,
    from_name: str = None,
    from_storage: Storage = None,
    to_name: str = None,
    to_format: DataFormat = None,
    to_storage: Storage = None,
    schema: Schema = AnySchema,
):
    """
    1. To copy request
    2. Find copy path
    3. Run each copy
       1. Get copier for (SF -> SF)
       2. Copier handles copy
    """
    pass


CopierCallabe = Callable[
    [str, str, Conversion, StorageApi, StorageApi, Schema,], None,
]


@dataclass(frozen=True)
class DataCopier:
    cost: DataCopyCost
    copier_function: CopierCallabe
    from_storage_classes: Optional[List[Type[StorageClass]]] = None
    from_storage_engines: Optional[List[Type[StorageEngine]]] = None
    from_data_formats: Optional[List[DataFormat]] = None
    to_storage_classes: Optional[List[Type[StorageClass]]] = None
    to_storage_engines: Optional[List[Type[StorageEngine]]] = None
    to_data_formats: Optional[List[DataFormat]] = None

    def copy(self, request: CopyRequest):
        self.copier_function(request)

    __call__ = copy

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


all_data_copiers = []


def datacopy(
    cost: DataCopyCost,
    from_storage_classes: Optional[List[Type[StorageClass]]] = None,
    from_storage_engines: Optional[List[Type[StorageEngine]]] = None,
    from_data_formats: Optional[List[DataFormat]] = None,
    to_storage_classes: Optional[List[Type[StorageClass]]] = None,
    to_storage_engines: Optional[List[Type[StorageEngine]]] = None,
    to_data_formats: Optional[List[DataFormat]] = None,
    unregistered: bool = False,
):
    def f(copier_function: CopierCallabe) -> DataCopier:
        dc = DataCopier(
            copier_function=copier_function,
            cost=cost,
            from_storage_classes=from_storage_classes,
            from_storage_engines=from_storage_engines,
            from_data_formats=from_data_formats,
            to_storage_classes=to_storage_classes,
            to_storage_engines=to_storage_engines,
            to_data_formats=to_data_formats,
        )
        if not unregistered:
            all_data_copiers.append(dc)
        return dc

    return f


class CopyLookup:
    def __init__(
        self,
        copiers: Iterable[DataCopier],
        available_storage_engines: Set[Type[StorageEngine]] = None,
        available_data_formats: Iterable[DataFormat] = None,
        expected_record_count: int = 10000,
    ):
        self._lookup: Dict[Conversion, List[DataCopier]] = defaultdict(list)
        self._copiers: Iterable[DataCopier] = copiers
        self.available_data_formats = available_data_formats
        self.available_storage_engines = available_storage_engines
        self.available_storage_formats = self._get_all_available_formats()
        self.expected_record_count = expected_record_count  # TODO: hmmmmm
        self._graph = self._build_copy_graph(expected_record_count)

    def _get_all_available_formats(self) -> List[StorageFormat]:
        fmts = []
        for fmt in self.available_data_formats:
            for eng in self.available_storage_engines:
                for supported_fmt in eng.get_supported_formats():
                    if issubclass(fmt, supported_fmt):
                        fmts.append(StorageFormat(eng, fmt))
        return fmts

    def _build_copy_graph(self, expected_record_count: int) -> nx.MultiDiGraph:
        g = nx.MultiDiGraph()
        for c in self._copiers:
            for from_fmt in self.available_storage_formats:
                if c.can_handle_from(from_fmt):
                    for to_fmt in self.available_storage_formats:
                        if c.can_handle_to(to_fmt):
                            g.add_edge(
                                from_fmt,
                                to_fmt,
                                copier=c,
                                cost=c.cost.total_cost(expected_record_count),
                            )
                            self._lookup[Conversion(from_fmt, to_fmt)].append(c)
        return g

    def get_capable_copiers(self, conversion: Conversion) -> List[DataCopier]:
        return self._lookup.get(conversion, [])

    def get_lowest_cost_path(self, conversion: Conversion) -> Optional[ConversionPath]:
        try:
            path = nx.shortest_path(
                self._graph,
                conversion.from_storage_format,
                conversion.to_storage_format,
                weight="cost",
            )
        except nx.NetworkXNoPath:
            return None
        conversion_path = ConversionPath(
            expected_record_count=self.expected_record_count
        )
        for i in range(len(path) - 1):
            edge = Conversion(path[i], path[i + 1])
            copier = self.get_lowest_cost(edge)
            if copier:
                conversion_path.add(ConversionEdge(copier=copier, conversion=edge))
            else:
                return None
        return conversion_path

    def get_lowest_cost(self, conversion: Conversion) -> Optional[DataCopier]:
        converters = [
            (c.cost.total_cost(self.expected_record_count), random.random(), c)
            for c in self.get_capable_copiers(conversion)
        ]
        if not converters:
            return None
        return min(converters)[2]

    def display_graph(self):
        for n, adj in self._graph.adjacency():
            print(n)
            for d, attrs in adj.items():
                print("\t", d, attrs["converter"])


def get_datacopy_lookup(
    copiers: Iterable[DataCopier] = None,
    available_storage_engines: Set[Type[StorageEngine]] = None,
    available_data_formats: Iterable[DataFormat] = None,
    expected_record_count: int = 10000,
) -> CopyLookup:
    return CopyLookup(
        copiers=copiers or all_data_copiers,
        available_storage_engines=available_storage_engines
        or set(global_registry.all(StorageEngine)),
        available_data_formats=available_data_formats
        or list(global_registry.all(DataFormatBase)),
        expected_record_count=expected_record_count,
    )
