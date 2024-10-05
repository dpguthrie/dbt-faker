# stdlib
import random
from collections import OrderedDict
from itertools import product

# third party
import factory
import sqlalchemy as sa
from faker import Faker

# first party
from dbt_faker.orms import tpch
from dbt_faker.factories.common import (
    _get_current_records,
    dbtFactory,
    RandomLazyFunction,
    Session
)


def productize_lists(*ls, sep: str = ' '):
    list_product = list(product(*ls))
    return [sep.join(ls) for ls in list_product]


def get_nation_records():
    return _get_current_records(tpch.Nation, 'n_nationkey', 100)


def get_region_records():
    return _get_current_records(tpch.Region, 'r_regionkey', 100)


def get_supplier_records():
    return _get_current_records(tpch.Supplier, 's_suppkey', 100)


def get_customer_records():
    return _get_current_records(tpch.Customer, 'c_custkey', 25)


def get_part_records():
    return _get_current_records(tpch.Part, 'p_partkey', 10)


def get_order_records():
    return _get_current_records(tpch.Order, 'o_orderkey', 10)


# Part
_TYPE_SIZE = ['ECONOMY', 'SMALL', 'LARGE', 'MEDIUM', 'PROMO', 'SMALL', 'STANDARD']
_TYPE_LOOK = ['ANODIZED', 'BRUSHED', 'BURNISHED', 'PLATED', 'POLISHED']
_TYPE_METAL = ['BRASS', 'COPPER', 'NICKEL', 'STEEL', 'TIN']
P_TYPE_ELEMENTS = productize_lists(_TYPE_SIZE, _TYPE_LOOK, _TYPE_METAL)

_CONTAINER_SIZES = ['JUMBO', 'LG', 'MED', 'SM', 'WRAP']
_CONTAINER_TYPES = ['BAG', 'BOX', 'CAN', 'CASE', 'DRUM', 'JAR', 'PACK', 'PKG']
P_CONTAINER_ELEMENTS = productize_lists(_CONTAINER_SIZES, _CONTAINER_TYPES)

P_MFGR_ELEMENTS = [
    'Volkswagen Group',
    'Apple',
    'Toyota Group',
    'Samsung Electronics',
    'Foxconn',
    'Daimler',
    'Cardinal Health'
]

# Orders
O_ORDER_STATUS_ELEMENTS = OrderedDict([
    ('F', .48),
    ('O', .48),
    ('P', .04),
])
O_ORDERPRIORITY_ELEMENTS = [
    '1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'
]
O_CLERK_ELEMENTS = [
    'Margaret Jones',
    'Paul Brown',
    'James Stone',
    'John Richards',
    'Jenny Richardson',
    'Debbie Waters MD',
    'Austin Boyer',
    'Stephanie Hayes',
    'Barbara Sanders',
    'Andrew Gould',
    'Charles Gonzalez',
    'Joshua Hernandez',
    'Victoria Hernandez MD',
    'Sherry Simpson',
    'Erica Jimenez',
    'Mr. Dakota Lynch II',
    'Victor Nolan',
    'Amanda Hernandez',
    'Richard Kirby',
    'Michelle Roman',
    'Bradley Melton',
    'Danny Williams',
    'Ryan Rivera',
    'Charles Douglas',
    'Kathy Santana',
    'Laura Gregory',
    'Norma Mooney',
    'Susan Harris',
    'Linda Petersen',
    'Dr. James Willis',
    'Rebecca Sandoval',
    'Christopher Hunter',
    'Austin Heath',
    'Elizabeth Russell',
    'Amy Davidson',
    'Dustin Greer',
    'Andrew Butler',
    'Scott Love DDS',
    'William Jenkins',
    'Jasmine Williams',
    'Christian Johnson',
    'Lorraine Garcia',
    'Hannah Reyes',
    'Katherine Ibarra',
    'Julie Chen',
    'Caleb Fleming',
    'Rachel Martinez',
    'Rebecca Stark',
    'Melanie Patrick',
    'Rachel Lopez',
]



# LineItem
L_RETURNFLAG_ELEMENTS = OrderedDict([
    ('A', .25),
    ('N', .5),
    ('R', .25),
])
L_SHIPINSTRUCT_ELEMENTS = [
    'COLLECT COD', 'DELIVER IN PERSON', 'NONE', 'TAKE BACK RETURN'
]
L_SHIPMODE_ELEMENTS = ['AIR', 'FOB', 'MAIL', 'RAIL', 'REG AIR', 'SHIP', 'TRUCK']


class RegionFactory(dbtFactory):
    class Meta:
        model = tpch.Region
        sqlalchemy_session = Session
        
    r_regionkey = factory.Sequence(lambda n: n)
    r_name = factory.Faker('name')
    r_comment = factory.Faker('bs')
    
    
class NationFactory(dbtFactory):
    class Meta:
        model = tpch.Nation
        sqlalchemy_session = Session
        
    n_nationkey = factory.Sequence(lambda n: n)
    n_name = factory.Faker('name')
    n_comment = factory.Faker('bs')
    n_regionkey = RandomLazyFunction(get_region_records)
    
    
class SupplierFactory(dbtFactory):
    class Meta:
        model = tpch.Supplier
        sqlalchemy_session = Session
        
    s_suppkey = factory.Sequence(lambda n: n)
    s_name = factory.Faker('company')
    s_address = factory.Faker('street_address')
    s_nationkey = RandomLazyFunction(get_nation_records)
    s_phone = factory.Faker('phone_number')
    s_acctbal = factory.Faker(
        'random_int', min=1000, max=500000
    )
    s_comment = factory.Faker('bs')
    
    
class CustomerFactory(dbtFactory):
    class Meta:
        model = tpch.Customer
        sqlalchemy_session = Session
        
    c_custkey = factory.Sequence(lambda n: n)
    c_name = factory.Faker('name')
    c_address = factory.Faker('street_address')
    c_nationkey = RandomLazyFunction(get_nation_records)
    c_phone = factory.Faker('phone_number')
    c_acctbal = factory.Faker('random_int', min=-1000, max=10000)
    c_mktsegment = factory.Faker(
        'random_element',
        elements=[
            'AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY'
        ],
    )
    c_comment = factory.Faker('bs')
    user_id = factory.Faker('pyint', min_value=2, max_value=11)

    
    
class PartFactory(dbtFactory):
    class Meta:
        model = tpch.Part
        sqlalchemy_session = Session
        
    p_partkey = factory.Sequence(lambda n: n)
    p_name = factory.Faker('catch_phrase')
    p_mfgr = factory.Faker('random_element', elements=P_MFGR_ELEMENTS)
    p_brand = factory.Faker('domain_word')
    p_type = factory.Faker('random_element', elements=P_TYPE_ELEMENTS)
    p_size = factory.Faker('random_int', min=1, max=50)
    p_container = factory.Faker('random_element', elements=P_CONTAINER_ELEMENTS)
    p_retailprice = factory.Faker('random_int', min=900, max=2000)
    p_comment = factory.Faker('bs')
    
    
class PartSuppFactory(dbtFactory):
    class Meta:
        model = tpch.PartSupp
        sqlalchemy_session = Session
    
    ps_partsuppkey = factory.Sequence(lambda n: n)
    ps_partkey = RandomLazyFunction(get_part_records)
    ps_suppkey = RandomLazyFunction(get_supplier_records)
    ps_availqty = factory.Faker('random_int', min=1, max=10000)
    ps_supplycost = factory.Faker('random_int', min=1, max=1000)
    ps_comment = factory.Faker('bs')
    
    
class OrderFactory(dbtFactory):
    class Meta:
        model = tpch.Order
        sqlalchemy_session = Session
        
    o_orderkey = factory.Sequence(lambda n: n)
    o_custkey = RandomLazyFunction(get_customer_records)
    o_orderstatus = factory.Faker(
        'random_element', elements=O_ORDER_STATUS_ELEMENTS
    )
    o_totalprice = factory.Faker('random_int', min=1000, max=500000)
    o_orderdate = factory.Faker('date_between', start_date='-30d', end_date='-1d')
    o_orderpriority = factory.Faker(
        'random_element', elements=O_ORDERPRIORITY_ELEMENTS
    )
    o_clerk = factory.Faker('random_element', elements=O_CLERK_ELEMENTS)
    o_comment = factory.Faker('bs')

    
class LineItemFactory(dbtFactory):
    class Meta:
        model = tpch.LineItem
        sqlalchemy_session = Session
    
    l_linekey = factory.Sequence(lambda n: n)
    l_orderkey = RandomLazyFunction(get_order_records)
    l_partkey = RandomLazyFunction(get_part_records)
    l_suppkey = RandomLazyFunction(get_supplier_records)
    l_linenumber = factory.Sequence(lambda n: n)
    l_quantity = factory.Faker('random_int', min=1, max=50)
    l_extendedprice = factory.Faker('random_int', min=1000, max=100000)
    l_returnflag = factory.Faker('random_element', elements=L_RETURNFLAG_ELEMENTS)
    l_linestatus = factory.Faker('random_element', elements=['O', 'F'])
    l_shipdate = factory.Faker('date_between', start_date='-60d', end_date='-30d')
    l_commitdate = factory.Faker('date_between', start_date='-29d', end_date='+30d')
    l_receiptdate = factory.Faker('date_between', start_date='-90d', end_date='+90d')
    l_shipinstruct = factory.Faker(
        'random_element', elements=L_SHIPINSTRUCT_ELEMENTS
    )
    l_shipmode = factory.Faker('random_element', elements=L_SHIPMODE_ELEMENTS)
    l_comment = factory.Faker('bs')
