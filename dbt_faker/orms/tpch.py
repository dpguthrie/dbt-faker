# stdlib
from typing import Any

# third party
import sqlalchemy as sa

# first party
from dbt_faker.db.orms import EtlBookkeepingMixin, WarehouseBase


TPCH_SCHEMA = 'TPCH'


class TpchSchemaMixin(EtlBookkeepingMixin):
    __table_args__ = {'schema': TPCH_SCHEMA}



class Customer(WarehouseBase, TpchSchemaMixin):
    __tablename__ = 'customer'
    c_custkey = sa.Column(sa.Integer, primary_key=True)

    c_name = sa.Column(sa.String)
    c_address = sa.Column(sa.String)
    c_nationkey = sa.Column(
        sa.Integer,
        sa.ForeignKey('Nation.n_nationkey'), 
        nullable=False
    )
    c_phone = sa.Column(sa.String)
    c_acctbal = sa.Column(sa.Numeric(38, 0))
    c_mktsegment = sa.Column(sa.String)
    c_comment = sa.Column(sa.String)


class LineItem(WarehouseBase, TpchSchemaMixin):
    __tablename__ = 'lineitem'
    l_linekey = sa.Column(sa.Integer, primary_key=True)

    l_orderkey = sa.Column(
        sa.Integer, sa.ForeignKey('Order.o_orderkey'), nullable=False
    )
    l_partkey = sa.Column(
        sa.Integer, sa.ForeignKey('Part.p_partkey'), nullable=False)
    l_suppkey = sa.Column(
        sa.Integer, sa.ForeignKey('Supplier.s_suppkey'), nullable=False)
    l_linenumber = sa.Column(sa.Integer, nullable=False)
    l_quantity = sa.Column(sa.Integer)
    l_extendedprice = sa.Column(sa.Integer)
    l_discount = sa.Column(sa.Integer, default=0)
    l_tax = sa.Column(sa.Integer, default=0)
    l_returnflag = sa.Column(sa.String)
    l_linestatus = sa.Column(sa.String)
    l_shipdate = sa.Column(sa.Date)
    l_commitdate = sa.Column(sa.Date)
    l_receiptdate = sa.Column(sa.Date)
    l_shipinstruct = sa.Column(sa.String)
    l_shipmode = sa.Column(sa.String)
    l_comment = sa.Column(sa.String)
    
    
class Nation(WarehouseBase, TpchSchemaMixin):
    __tablename__ = 'nation'
    n_nationkey = sa.Column(sa.Integer, primary_key=True)
    
    n_name = sa.Column(sa.String)
    n_regionkey = sa.Column(sa.Integer)
    n_comment = sa.Column(sa.String)


class Order(WarehouseBase, TpchSchemaMixin):
    __tablename__ = 'orders'
    o_orderkey = sa.Column(sa.Integer, primary_key=True)
    
    o_custkey = sa.Column(
        sa.Integer, sa.ForeignKey('Customer.c_custkey'), nullable=False
    )
    o_orderstatus = sa.Column(sa.String)
    o_totalprice = sa.Column(sa.Integer)
    o_orderdate = sa.Column(sa.Date)
    o_orderpriority = sa.Column(sa.String)
    o_clerk = sa.Column(sa.String)
    o_shippriority = sa.Column(sa.String, default=0)
    o_comment = sa.Column(sa.String)
    
    
class Part(WarehouseBase, TpchSchemaMixin):
    __tablename__ = 'part'
    p_partkey = sa.Column(sa.Integer, primary_key=True)
    
    p_name = sa.Column(sa.String)
    p_mfgr = sa.Column(sa.String)
    p_brand = sa.Column(sa.String)
    p_type = sa.Column(sa.String)
    p_size = sa.Column(sa.Integer)
    p_container = sa.Column(sa.String)
    p_retailprice = sa.Column(sa.Integer)
    p_comment = sa.Column(sa.String)
    
    
class PartSupp(WarehouseBase, TpchSchemaMixin):
    __tablename__ = 'partsupp'
    ps_partkey = sa.Column(sa.Integer, primary_key=True)
    
    ps_suppkey = sa.Column(
        sa.Integer, sa.ForeignKey('Supplier.s_suppkey'), nullable=False
    )
    ps_availqty = sa.Column(sa.Integer)
    ps_supplycost = sa.Column(sa.Integer)
    ps_comment = sa.Column(sa.String)
    
    
class Region(WarehouseBase, TpchSchemaMixin):
    __tablename__ = 'region'
    r_regionkey = sa.Column(sa.Integer, primary_key=True)
    
    r_name = sa.Column(sa.String)
    r_comment = sa.Column(sa.String)
    
    
class Supplier(WarehouseBase, TpchSchemaMixin):
    __tablename__ = 'supplier'
    s_suppkey = sa.Column(sa.Integer, primary_key=True)
    
    s_name = sa.Column(sa.String)
    s_address = sa.Column(sa.String)
    s_nationkey = sa.Column(sa.Integer)
    s_phone = sa.Column(sa.String)
    s_acctbal = sa.Column(sa.Integer)
    s_comment = sa.Column(sa.String)
