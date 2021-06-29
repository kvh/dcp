from __future__ import annotations

from typing import Optional, Tuple

import pytest
from dcp.data_copy.base import Conversion, DataCopierBase, StorageFormat
from dcp.data_copy.costs import NoOpCost
from dcp.data_copy.graph import get_datacopy_lookup
from dcp.data_format.formats.database.base import DatabaseTableFormat
from dcp.data_format.formats.file_system.csv_file import CsvFileFormat
from dcp.data_format.formats.memory.arrow_table import ArrowTableFormat
from dcp.data_format.formats.memory.dataframe import DataFrameFormat
from dcp.data_format.formats.memory.records import RecordsFormat
from dcp.storage.base import (
    DatabaseStorageClass,
    FileSystemStorageClass,
    LocalFileSystemStorageEngine,
    LocalPythonStorageEngine,
    MemoryStorageClass,
    MysqlStorageEngine,
    PostgresStorageEngine,
    SqliteStorageEngine,
)


def test_data_copy_lookup():
    class Db2Recs(DataCopierBase):
        from_storage_classes = [DatabaseStorageClass]
        from_data_formats = [DatabaseTableFormat]
        to_storage_classes = [MemoryStorageClass]
        to_data_formats = [RecordsFormat]
        cost = NoOpCost
        unregistered = True

    class Recs2Csv(DataCopierBase):
        from_storage_classes = [MemoryStorageClass]
        from_data_formats = [RecordsFormat]
        to_storage_classes = [FileSystemStorageClass]
        to_data_formats = [CsvFileFormat]
        cost = NoOpCost
        unregistered = True

    assert Db2Recs().can_handle_from(
        StorageFormat(MysqlStorageEngine, DatabaseTableFormat)
    )
    assert Db2Recs().can_handle_from(
        StorageFormat(PostgresStorageEngine, DatabaseTableFormat)
    )
    assert not Db2Recs().can_handle_from(
        StorageFormat(LocalFileSystemStorageEngine, CsvFileFormat)
    )
    assert not Db2Recs().can_handle_from(
        StorageFormat(LocalPythonStorageEngine, RecordsFormat)
    )

    assert not Db2Recs().can_handle_to(
        StorageFormat(MysqlStorageEngine, DatabaseTableFormat)
    )
    assert not Db2Recs().can_handle_to(
        StorageFormat(PostgresStorageEngine, DatabaseTableFormat)
    )
    assert not Db2Recs().can_handle_to(
        StorageFormat(LocalFileSystemStorageEngine, CsvFileFormat)
    )
    assert Db2Recs().can_handle_to(
        StorageFormat(LocalPythonStorageEngine, RecordsFormat)
    )

    lkup = get_datacopy_lookup(copiers=[Db2Recs(), Recs2Csv()])
    pth = lkup.get_lowest_cost_path(
        Conversion(
            StorageFormat(PostgresStorageEngine, DatabaseTableFormat),
            StorageFormat(LocalFileSystemStorageEngine, CsvFileFormat),
        )
    )
    assert [e.copier for e in pth.edges] == [Db2Recs(), Recs2Csv()]


@pytest.mark.parametrize(
    "conversion,length",
    [
        # Memory to DB
        (
            (
                StorageFormat(LocalPythonStorageEngine, RecordsFormat),
                StorageFormat(PostgresStorageEngine, DatabaseTableFormat),
            ),
            1,
        ),
        (
            (
                StorageFormat(LocalPythonStorageEngine, DataFrameFormat),
                StorageFormat(PostgresStorageEngine, DatabaseTableFormat),
            ),
            2,
        ),
        (
            (
                StorageFormat(LocalFileSystemStorageEngine, CsvFileFormat),
                StorageFormat(SqliteStorageEngine, DatabaseTableFormat),
            ),
            1,
        ),
        (
            (
                StorageFormat(SqliteStorageEngine, DatabaseTableFormat),
                StorageFormat(LocalPythonStorageEngine, ArrowTableFormat),
            ),
            3,
        ),
    ],
)
def test_conversion_costs(conversion: Tuple, length: Optional[int]):
    cp = get_datacopy_lookup().get_lowest_cost_path(Conversion(*conversion))
    if length is None:
        assert cp is None
    else:
        assert cp is not None
        # for c in cp.conversions:
        #     print(f"{c.copier.copier_function} {c.conversion}")
        assert len(cp.edges) == length
