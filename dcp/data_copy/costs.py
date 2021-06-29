from __future__ import annotations

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

CostFunction = Callable[[int], int]
BUFFER_SIZE = 100
DISK_FACTOR = 1
NETWORK_FACTOR = 5


@dataclass(frozen=True)
class DataCopyCost:
    # TODO: we're not really using the cost parameter n
    # Maybe easier to just assume data n >> buffer n and nothing else matters?
    wire_cost: CostFunction = lambda n: 0
    memory_cost: CostFunction = lambda n: 0
    cpu_cost: CostFunction = lambda n: 0  # Really just for costly format conversions
    cpu_cost_weight = 0.5
    memory_cost_weight = 0.5

    def total_cost(self, n: int) -> int:
        return round(
            self.wire_cost(n)
            + self.cpu_cost(n) * self.cpu_cost_weight
            + self.memory_cost(n) * self.memory_cost_weight
        )

    def __add__(self, other: DataCopyCost) -> DataCopyCost:
        return DataCopyCost(
            wire_cost=lambda n: self.wire_cost(n) + other.wire_cost(n),
            memory_cost=lambda n: self.memory_cost(n) + other.memory_cost(n),
            cpu_cost=lambda n: self.cpu_cost(n) + other.cpu_cost(n),
        )


NoOpCost = DataCopyCost()
BufferToBufferCost = DataCopyCost(
    wire_cost=lambda n: 0, memory_cost=lambda n: BUFFER_SIZE
)
MemoryToBufferCost = DataCopyCost(wire_cost=lambda n: 0, memory_cost=lambda n: n)
MemoryToMemoryCost = DataCopyCost(wire_cost=lambda n: 0, memory_cost=lambda n: n)
DiskToBufferCost = DataCopyCost(
    wire_cost=lambda n: n, memory_cost=lambda n: BUFFER_SIZE
)
DiskToMemoryCost = DataCopyCost(wire_cost=lambda n: n, memory_cost=lambda n: n)
NetworkToMemoryCost = DataCopyCost(
    wire_cost=(
        lambda n: n * NETWORK_FACTOR
    ),  # What is this factor in practice? What's a good default (think S3 vs local SSD?)
    memory_cost=lambda n: n,
)
NetworkToBufferCost = DataCopyCost(
    wire_cost=(lambda n: n * NETWORK_FACTOR), memory_cost=lambda n: BUFFER_SIZE
)
FormatConversionCost = DataCopyCost(cpu_cost=lambda n: n)
