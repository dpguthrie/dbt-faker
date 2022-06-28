
# stdlib
from typing import Any, Dict, Tuple, Union

# third party
import sqlalchemy as sa
from sqlalchemy.ext.declarative import DeclarativeMeta, declarative_base

from .base import get_warehouse_engine, register_session_bind


def create_base(metadata: sa.MetaData):
    return declarative_base(metadata=metadata)


WarehouseBase: Any = declarative_base()

register_session_bind(WarehouseBase, get_warehouse_engine)


class BookkeepingColumn(sa.Column):
    is_bookkeeping = True


class EtlBookkeepingMixin:
    _etl_updated_timestamp = BookkeepingColumn(sa.DateTime, nullable=False)
