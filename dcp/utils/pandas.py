from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd
from commonmodel import Schema
from dcp.utils.common import is_nullish
from loguru import logger
from pandas import DataFrame, Index, Series
from pandas._testing import assert_almost_equal


def sortable_columns(dtypes: Series) -> List[str]:
    sortables = []
    for i in dtypes.index:
        c = str(dtypes[i]).lower()
        if "cat" in c or "obj" in c:
            continue
        sortables.append(i)
    return sortables


def assert_dataframes_are_almost_equal(
    df1: DataFrame,
    df2: DataFrame,
    schema: Optional[Schema] = None,
    ignored_columns: List[str] = None,
    check_dtypes: bool = False,
):
    logger.debug(df1)
    logger.debug(df2)
    if ignored_columns:
        df1 = df1[[c for c in df1.columns if c not in ignored_columns]]
        df2 = df2[[c for c in df2.columns if c not in ignored_columns]]
    assert df1.shape == df2.shape, f"Different shapes: {df1.shape} {df2.shape}"
    assert set(df1.columns) == set(df2.columns)
    if schema is not None and schema.unique_on:
        df1.sort_values(schema.unique_on, inplace=True)
        df2.sort_values(schema.unique_on, inplace=True)
    for (i, r), (i2, r2) in zip(df1.iterrows(), df2.iterrows()):
        for c in r.keys():
            if is_nullish(r[c]) and is_nullish(r2[c]):
                continue
            assert_almost_equal(r[c], r2[c], check_dtype=check_dtypes)


def empty_dataframe_for_schema(schema: Schema) -> DataFrame:
    df = DataFrame()
    for field in schema.fields:
        pd_type = field.field_type.pandas_type
        df[field.name] = Series(dtype=pd_type)
    return df


# def records_to_dataframe(records: Records, schema: Schema) -> DataFrame:
#     from snapflow.core.typing.inference import conform_dataframe_to_schema

#     df = DataFrame(records)
#     return conform_dataframe_to_schema(df, schema)


def dataframe_to_records(df: DataFrame) -> List[Dict]:
    # TODO
    for c in df:
        dfc = df[c].astype(object)
        dfc.loc[pd.isna(dfc)] = None
        del df[c]
        df[c] = dfc
    return df.to_dict(orient="records")
