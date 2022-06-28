# stdlib
import contextlib
import os

# third party
import sqlalchemy as sa
from sqlalchemy.exc import DisconnectionError, IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker


def create_engine(url: str) -> sa.engine.Engine:
    engine = sa.create_engine(
        url,
        # https://docs.sqlalchemy.org/en/13/core/pooling.html?highlight=reconnect#disconnect-handling-pessimistic
        pool_pre_ping=True,
    )

    # From: https://docs.sqlalchemy.org/en/latest/core/pooling.html#using-connection-pools-with-multiprocessing  # noqa
    @sa.event.listens_for(engine, 'connect')
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    # From: https://docs.sqlalchemy.org/en/latest/core/pooling.html#using-connection-pools-with-multiprocessing  # noqa
    @sa.event.listens_for(engine, 'checkout')
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            connection_record.connection = connection_proxy.connection = None
            raise DisconnectionError(
                f'Connection record belongs to pid {connection_record.info["pid"]} '
                f'attempting to check out in pid {pid}'
            )

    return engine


_engine = create_engine(os.environ.get('WAREHOUSE_URL'))


def get_warehouse_engine():
    """Get the snowflake engine."""
    return _engine


_session_binds = {}


def register_session_bind(key, engine_getter):
    _session_binds[key] = engine_getter


def get_session() -> Session:
    """Get an ORM session."""
    binds = {key: engine_getter() for key, engine_getter in _session_binds.items()}
    return sessionmaker(binds=binds)()


# noqa - taken from https://docs.sqlalchemy.org/en/latest/orm/session_basics.html#when-do-i-construct-a-session-when-do-i-commit-it-and-when-do-i-close-it
@contextlib.contextmanager
def session_scope(session_cls=None):
    """Provide a transactional scope around a series of operations."""
    if session_cls is not None:
        session = session_cls()
    else:
        session = get_session()
    try:
        yield session
        session.commit()
    except SQLAlchemyError:
        session.rollback()
        raise
    finally:
        session.close()


# From: http://docs.sqlalchemy.org/en/latest/core/constraints.html#configuring-constraint-naming-conventions  # noqa
convention = {
    'ix': 'ix_%(column_0_label)s',
    'uq': 'uq_%(table_name)s_%(column_0_name)s',
    'ck': 'ck_%(table_name)s_%(constraint_name)s',
    'fk': 'fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s',
    'pk': 'pk_%(table_name)s',
}

metadata = sa.MetaData(naming_convention=convention)
