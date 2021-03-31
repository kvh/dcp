from __future__ import annotations
from dcp.data_copy.base import Conversion, DataCopier, StorageFormat, ALL_DATA_COPIERS
from dcp.data_format.base import ALL_DATA_FORMATS, DataFormat
from dcp.storage.base import ALL_STORAGE_ENGINES, StorageEngine
from dcp.data_format.handler import FormatHandler

import enum
import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import (
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
from loguru import logger
from sqlalchemy.orm.session import Session


@dataclass(frozen=True)
class CopyEdge:
    copier: DataCopier
    conversion: Conversion


@dataclass(frozen=True)
class CopyPath:
    edges: List[CopyEdge] = field(default_factory=list)
    expected_record_count: int = 10000

    def add(self, edge: CopyEdge):
        self.edges.append(edge)

    def __len__(self) -> int:
        return len(self.edges)

    @property
    def total_cost(self) -> int:
        return sum(
            c.copier.cost.total_cost(self.expected_record_count) for c in self.edges
        )


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

    def get_lowest_cost_path(self, conversion: Conversion) -> Optional[CopyPath]:
        try:
            path = nx.shortest_path(
                self._graph,
                conversion.from_storage_format,
                conversion.to_storage_format,
                weight="cost",
            )
        except nx.NetworkXNoPath:
            return None
        copy_path = CopyPath(expected_record_count=self.expected_record_count)
        for i in range(len(path) - 1):
            edge = Conversion(path[i], path[i + 1])
            copier = self.get_lowest_cost(edge)
            if copier:
                copy_path.add(CopyEdge(copier=copier, conversion=edge))
            else:
                return None
        return copy_path

    def get_lowest_cost(self, conversion: Conversion) -> Optional[DataCopier]:
        copiers = [
            (c.cost.total_cost(self.expected_record_count), random.random(), c)
            for c in self.get_capable_copiers(conversion)
        ]
        if not copiers:
            return None
        return min(copiers)[2]

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
        copiers=copiers or ALL_DATA_COPIERS,
        available_storage_engines=available_storage_engines or ALL_STORAGE_ENGINES,
        available_data_formats=available_data_formats or ALL_DATA_FORMATS,
        expected_record_count=expected_record_count,
    )