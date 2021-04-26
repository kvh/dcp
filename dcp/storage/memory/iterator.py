from __future__ import annotations

import traceback
import typing
from io import SEEK_END, SEEK_SET, BytesIO, IOBase, StringIO, TextIOBase
from itertools import tee
from typing import Any, Generic, Iterable, Iterator, List, Optional, TypeVar, Union

T = TypeVar("T")


class SampleableIterator(Generic[T]):
    """
    Utility for sampling a small first N of an iterator, without
    bringing the whole thing into memory.
    """

    def __init__(
        self, iterator: typing.Iterator[T], iterated_values: Optional[List[T]] = None
    ):
        self._iterator = iterator
        self._iterated_values: List[T] = iterated_values or []
        self._is_used = False
        self._complete = False
        self._position = 0

    def __iter__(self) -> Iterator[T]:
        # if self._is_used:
        #     raise Exception("Iterator already used")  # TODO: better exception
        if not self._is_used or self._complete:
            i = -1
            for v in self._iterated_values:
                i += 1
                if i < self._position:
                    continue
                self._position += 1
                yield v
        for v in self._iterator:
            self._is_used = True
            yield v

    def __next__(self) -> T:
        if not self._is_used or self._complete:
            if self._position < len(self._iterated_values):
                v = self._iterated_values[self._position]
                self._position += 1
                return v
        self._is_used = True
        return next(self._iterator)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._iterator, name)

    def get_first(self) -> Optional[T]:
        return next(self.head(1), None)

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
            self._iterated_values.append(v)
            yield v
            i += 1
            if i >= n:
                return
        self._complete = True


# class SampleableIOBase:
#     def __init__(self, io_base: IOBase):
#         io_cls = BytesIO
#         if isinstance(io_base, TextIOBase):
#             io_cls = StringIO
#         self._bytes = io_cls()
#         self._iterator = io_base

#     def _load_all(self):
#         self._bytes.seek(0, SEEK_END)
#         for chunk in self._iterator:
#             self._bytes.write(chunk)

#     def _load_until(self, goal_position):
#         current_position = self._bytes.seek(0, SEEK_END)
#         while current_position < goal_position:
#             try:
#                 current_position += self._bytes.write(next(self._iterator))
#             except StopIteration:
#                 break

#     def tell(self):
#         return self._bytes.tell()

#     def read(self, size=None):
#         left_off_at = self._bytes.tell()
#         if size is None:
#             self._load_all()
#         else:
#             goal_position = left_off_at + size
#             self._load_until(goal_position)

#         self._bytes.seek(left_off_at)
#         return self._bytes.read(size)

#     def seek(self, position, whence=SEEK_SET):
#         if whence == SEEK_END:
#             self._load_all()
#         else:
#             self._bytes.seek(position, whence)

#     def __getattr__(self, name: str) -> Any:
#         return getattr(self._iterator, name)
