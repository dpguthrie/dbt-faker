# stdlib
import random

# first party
from dbt_faker.factories import tpch as tpch_factories
from dbt_faker.orms import tpch as tpch_orms


__all__ = ['tpch_etl']


tpch_etl = {
    'config': [
        {
            'factory': tpch_factories.SupplierFactory,
            'update_rows': (1, 5),
            'perc_of_total_rows': 0,
            'orm': tpch_orms.Supplier,
        },
        {
            'factory': tpch_factories.CustomerFactory,
            'update_cadence': .25,
            'perc_of_total_rows': .03,
            'orm': tpch_orms.Customer,
        },
        {
            'factory': tpch_factories.PartFactory,
            'perc_of_total_rows': .04,
            'orm': tpch_orms.Part,
        },
        {
            'factory': tpch_factories.PartSuppFactory,
            'perc_of_total_rows': .15,
            'orm': tpch_orms.PartSupp,
        },
        {
            'factory': tpch_factories.OrderFactory,
            'perc_of_total_rows': .25,
            'orm': tpch_orms.Order,
        },
        {
            'factory': tpch_factories.LineItemFactory,
            'update_cadence': .05,
            'perc_of_total_rows': 1,
            'orm': tpch_orms.LineItem,
        },
    ],
    'rows': (500, 1000),
    'schema': 'TPCH',
}