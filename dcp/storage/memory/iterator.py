from __future__ import annotations

import typing
from itertools import tee
from typing import Any, Generic, Iterable, Iterator, List, Optional, TypeVar, Union

T = TypeVar("T")


class SampleableIterator(Generic[T]):
    """
    Utility for sampling a small first N of an iterator, without
    bringing the whole thing into memory.
    """

    def __init__(
        self, iterator: typing.Iterator, iterated_values: Optional[List[T]] = None
    ):
        self._iterator = iterator
        self._iterated_values: List[T] = iterated_values or []
        self._is_used = False

    def __iter__(self) -> Iterator[T]:
        if self._is_used:
            raise Exception("Iterator already used")  # TODO: better exception
        for v in self._iterated_values:
            yield v
        for v in self._iterator:
            self._is_used = True
            yield v

    def get_first(self) -> Optional[T]:
        return next(self.head(1), None)

    # Unclear what you want to do here
    # def copy(self) -> SampleableIterator[T]:
    #     if self._is_used:
    #         raise Exception("Iterator already used")
    #     self._iterator, it2 = tee(self._iterator, 2)
    #     return SampleableIterator(it2, self._iterated_values)

    def head(self, n: int) -> Iterator[T]:
        if n < 1:
            return
        i = 0
        for v in self._iterated_values:
            yield v
            i += 1
            if i >= n:
                return
        if self._is_used:
            raise Exception("Iterator already used")
        for v in self._iterator:
            # Important: we are uncovering a new records object potentially
            # so we must wrap it immediately
            # wrapped_v = wrap_records_object(v)
            self._iterated_values.append(v)
            yield v
            i += 1
            if i >= n:
                return

    def __getattr__(self, name: str) -> Any:
        return getattr(self._iterator, name)
