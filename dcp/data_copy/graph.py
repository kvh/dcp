from __future__ import annotations

import dataclasses
import enum
import pprint
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
from dcp.data_copy.base import (
    ALL_DATA_COPIERS,
    Conversion,
    CopyRequest,
    DataCopierBase,
    StorageFormat,
)
from dcp.data_format.base import ALL_DATA_FORMATS, DataFormat
from dcp.data_format.handler import FormatHandler
from dcp.storage.base import (
    ALL_STORAGE_ENGINES,
    Storage,
    StorageEngine,
    FullPath,
    StorageObject,
)
from dcp.utils.common import rand_str, to_json
from loguru import logger


@dataclass(frozen=True)
class CopyEdge:
    copier: DataCopierBase
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


@dataclass
class CopyResult:
    request: CopyRequest
    copy_path: CopyPath
    intermediate_created: List[StorageObject]


class CopyLookup:
    def __init__(
        self,
        copiers: Iterable[DataCopierBase],
        available_storage_engines: Set[Type[StorageEngine]] = None,
        available_data_formats: Iterable[DataFormat] = None,
        expected_record_count: int = 10000,
    ):
        self._lookup: Dict[Conversion, List[DataCopierBase]] = defaultdict(list)
        self._copiers: Iterable[DataCopierBase] = copiers
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

    def get_capable_copiers(self, conversion: Conversion) -> List[DataCopierBase]:
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

    def get_lowest_cost(self, conversion: Conversion) -> Optional[DataCopierBase]:
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
    copiers: Iterable[DataCopierBase] = None,
    available_storage_engines: Iterable[Type[StorageEngine]] = None,
    available_data_formats: Iterable[DataFormat] = None,
    expected_record_count: int = 10000,
) -> CopyLookup:
    return CopyLookup(
        copiers=copiers or ALL_DATA_COPIERS,
        available_storage_engines=available_storage_engines or ALL_STORAGE_ENGINES,
        available_data_formats=available_data_formats or ALL_DATA_FORMATS,
        expected_record_count=expected_record_count,
    )


def get_copy_path(req: CopyRequest) -> Optional[CopyPath]:
    lookup = get_datacopy_lookup(
        available_storage_engines=set(
            s.storage_engine for s in req.get_available_storages()
        ),
    )
    if req.conversion.from_storage_format == req.conversion.to_storage_format:
        # If converting self, this can mean different things based on if_exists
        # TODO: this vs create an alias?
        # TODO: self-copy is just .append(name1, name2) ??
        # if req.from_obj.storage == req.to_obj.storage:
        #     return CopyPath(edges=[])
        copiers = lookup.get_capable_copiers(req.conversion)
        if not copiers:
            # TODO: implement rest of these
            raise NotImplementedError(req.conversion)
        # assert len(copiers) == 1, copiers
        return CopyPath(edges=[CopyEdge(copiers[0], req.conversion)])
    copy_path = lookup.get_lowest_cost_path(req.conversion)
    return copy_path


def execute_copy_request(req: CopyRequest) -> CopyResult:
    copy_path = get_copy_path(req)
    if copy_path is None:
        # Nothing to do?
        raise NotImplementedError(req.conversion)
        # return CopyResult(request=req, copy_path=copy_path, intermediate_created=[])
    return execute_copy_path(req, copy_path)


def execute_copy_path(original_req: CopyRequest, pth: CopyPath):
    prev_obj = original_req.from_obj
    n = len(pth.edges)
    # if n == 0:
    #     # TODO: handle copy between identical StorageFormats
    #     # No copy required, BUT may need an alias
    #     if original_req.from_name != original_req.to_name:
    #         if original_req.from_obj.storage.url != original_req.to_obj.storage.url:
    #             raise NotImplementedError(
    #                 "Copy between same StorageFormats not supported yet"
    #             )
    #         original_req.from_storage_api.create_alias(
    #             original_req.from_name, original_req.to_name
    #         )
    created = []
    for i, conversion_edge in enumerate(pth.edges):
        conversion = conversion_edge.conversion
        target_storage_format = conversion.to_storage_format
        next_storage = select_storage(
            original_req.to_obj.storage,
            original_req.get_available_storages(),
            target_storage_format,
        )
        logger.debug(
            f"Copy: {conversion.from_storage_format} -> {conversion.to_storage_format}"
        )
        if i == n - 1:
            next_path = original_req.to_obj.full_path
        else:
            next_path = FullPath(
                f"{original_req.to_obj.full_path.name}_{rand_str(6).lower()}"
            )
        next_to_obj = dataclasses.replace(
            original_req.to_obj,
            full_path=next_path,
            storage=next_storage,
            _data_format=conversion.to_storage_format.data_format,
            _schema=original_req.get_to_schema(),
        )
        edge_req = CopyRequest(
            from_obj=prev_obj,
            to_obj=next_to_obj,
            if_exists=original_req.if_exists,
            delete_intermediate=original_req.delete_intermediate,
        )
        conversion_edge.copier.copy(edge_req)
        if i >= 2:
            if original_req.delete_intermediate:
                # If not first conversion (we don't want to delete original source!)
                edge_req.from_obj.storage_api.remove(prev_obj)
            else:
                # If not deleting previous (and not source) than add as created
                created.append((prev_obj))
        prev_obj = next_to_obj
    # Add final destination to created
    return CopyResult(request=original_req, copy_path=pth, intermediate_created=created)


def select_storage(
    target_storage: Storage,
    storages: List[Storage],
    storage_format: StorageFormat,
) -> Storage:
    eng = storage_format.storage_engine
    # By default, stay on target storage if possible (minimize transfer)
    if eng == target_storage.storage_engine:
        return target_storage
    for storage in storages:
        if eng == storage.storage_engine:
            return storage
    raise Exception(f"No matching storage {storage_format}")
