# stdlib
import datetime
import random
from functools import cache

# third party
import factory
import sqlalchemy as sa

# first party
from dbt_faker.db.base import get_session, get_warehouse_engine


@cache
def _get_current_records(model, primary_key, sample):
    schema = model.__table_args__['schema']
    table = model.__tablename__
    engine = get_warehouse_engine()
    ls = []
    sql = f'''
        select {primary_key}
        from {table}
        sample ({sample})
    '''
    with engine.connect() as conn:
        conn.execute(f'USE SCHEMA {schema};')
        result = conn.execute(sql)
        for row in result:
            ls.append(getattr(row, primary_key))
    return ls


def factory_to_dict(factory_class, rows):
    return factory.build_batch(dict, rows, FACTORY_CLASS=factory_class)


class dbtFactory(factory.alchemy.SQLAlchemyModelFactory):
    _etl_updated_timestamp = factory.LazyFunction(datetime.datetime.now)
    
    @classmethod
    def _setup_next_sequence(cls):
        if cls._meta.model is dict:
            model = cls._meta.base_factory._meta.model
        else:
            model = cls._meta.model
        session = get_session()
        primary_key = [col for col in model.__table__.c if col.primary_key][0].name
        try:
            obj = session.query(model).order_by(
                getattr(model, primary_key).desc()
            ).first()
            return getattr(obj, primary_key) + 1

        except AttributeError:
            return 1


class RandomLazyFunction(factory.LazyFunction):
    def __init__(self, function):
        return super().__init__(function)
    
    def evaluate(self, instance, step, extra):
        choices = self.function()
        return random.choice(choices)
        
        
Session = sa.orm.scoped_session(sa.orm.sessionmaker())
