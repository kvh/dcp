from __future__ import annotations

import os
import tempfile
from collections.abc import Generator
from typing import Dict, Iterable, List

import jinja2
from dcp.utils.common import rand_str
from sqlalchemy.engine import Result, Row


def result_proxy_to_records(
    result_proxy: Result, rows: Iterable[Row] = None
) -> List[Dict]:
    if not result_proxy:
        return []
    if not rows:
        rows = result_proxy
    return [{k: v for k, v in zip(result_proxy.keys(), row)} for row in rows]


def db_result_batcher(result_proxy: Result, batch_size: int = 1000) -> Generator:
    while True:
        rows = result_proxy.fetchmany(batch_size)
        yield result_proxy_to_records(result_proxy, rows)
        if len(rows) < batch_size:
            return


def columns_from_records(
    records: List[Dict],
    columns: List[str] = None,
) -> List[str]:
    assert len(records) > 0, "No records to infer columns from"
    # Use first object's keys as columns. Assumes uniform dicts
    cols = set()
    for r in records[:10]:
        cols |= set(r.keys())
    columns = list(cols)
    return columns


def column_list(cols: List[str], commas_first: bool = True) -> str:
    join_str = '"\n, "' if commas_first else '",\n"'
    return '"' + join_str.join(cols) + '"'


def get_jinja_env():
    template_dir = os.path.join(os.path.dirname(__file__), "sql_templates")
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(template_dir),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    env.filters["column_list"] = column_list
    return env


def compile_jinja_sql(sql, template_ctx):
    env = get_jinja_env()
    tmpl = env.from_string(sql)
    sql = tmpl.render(**template_ctx)
    return sql


def compile_jinja_sql_template(template, template_ctx=None):
    if template_ctx is None:
        template_ctx = {}
    env = get_jinja_env()
    tmpl = env.get_template(template)
    sql = tmpl.render(**template_ctx)
    return sql


def get_tmp_sqlite_db_url(dbname=None):
    if dbname is None:
        dbname = rand_str(10)
    dir = tempfile.mkdtemp()
    return f"sqlite:///{dir}/{dbname}.db"


def column_map(table_stmt: str, from_fields: List[str], mapping: Dict[str, str]) -> str:
    # TODO: identifier quoting
    col_stmts = []
    for f in from_fields:
        col_stmts.append(f"{f} as {mapping.get(f, f)}")
    columns_stmt = ",".join(col_stmts)
    sql = f"""
    select
        {columns_stmt}
    from {table_stmt}
    """
    return sql
