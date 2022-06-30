# stdlib
import datetime
import random
from typing import Dict, List, Tuple

# third party
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.sql.expression import FromClause


def select_to_df(stmt: FromClause, engine: sa.engine.Connectable):
    python_dt_types = {datetime.date, datetime.datetime}
    parse_dates = []
    for col in stmt.c:
        try:
            if col.type.python_type in python_dt_types:
                parse_dates.append(col.name)
        except NotImplementedError:
            continue

    df = pd.read_sql(stmt, engine, parse_dates=parse_dates)
    return df


def get_ids_to_update(
    source_table: sa.Table,
    source_engine: sa.engine.Connectable,
    orm_config: Dict,
    primary_key: str,
    default_rows: Tuple[int, int] = (1, 100),
):
    select = sa.select([source_table])
    df = select_to_df(select, source_engine)
    if len(df) > 0:
        sample_df = df.sample(
            n=random.randint(*orm_config.get('update_rows', default_rows))
        )
        primary_keys = sample_df[primary_key].tolist()
        return primary_keys
    
    return []


def dataframe_to_sql(
    df: pd.DataFrame,
    engine: sa.engine.Engine,
    table: str,
    schema: str,
    if_exists: str = 'append',
    index: bool = False,
    chunksize: int = 16000,
    unique_subset: List[str] = None,
):
    df = df.drop_duplicates(subset=unique_subset)
    df.to_sql(
        table,
        engine,
        schema=schema,
        if_exists=if_exists,
        index=index,
        chunksize=chunksize
    )