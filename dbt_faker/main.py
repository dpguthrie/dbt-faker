# stdlib
import logging
import random

# third party
import pandas as pd
import sqlalchemy as sa

# first party
from dbt_faker.db.base import get_warehouse_engine
from dbt_faker.etl.common import dataframe_to_sql, get_ids_to_update
from dbt_faker.etl import tpch_etl
from dbt_faker.factories.common import factory_to_dict


logging.basicConfig(format='%(name)s - %(levelname)s - %(asctime)s - %(message)s', level=logging.INFO)

# Set defaults
DEFAULT_UPDATE_CADENCE = .15
DEFAULT_ROWS_TO_UPDATE = (1, 100)


ETL_SOURCES = [
    tpch_etl,
]


if __name__ == '__main__':
    source_engine = get_warehouse_engine()
    for etl_source in ETL_SOURCES:
        
        # Set source level variables
        source_schema = etl_source['schema']
        total_rows = random.randint(*etl_source['rows'])
        
        logging.info(f'{source_schema} ETL is beginning.')
        
        # Loop through each orm/factory in config
        for orm_config in etl_source['config']:
            source_table = sa.Table(
                orm_config['orm'].__tablename__,
                sa.MetaData(schema=source_schema, bind=source_engine),
                *(col._copy() for col in orm_config['orm'].__table__.c),
            )
            
            logging.info(f'Fake data generation for {source_schema}.{source_table.name} table beginning.')
            
            # Initialize factory
            factory = orm_config['factory']
            
            should_update = random.random() < orm_config.get(
                'update_cadence', DEFAULT_UPDATE_CADENCE
            )
            if should_update:

                # Table needs to have a primary key or will fail with KeyError
                primary_key = [c.name for c in source_table.c if c.primary_key][0]
                
                # Get IDs that we want to update
                ids = get_ids_to_update(
                    source_table,
                    source_engine,
                    orm_config,
                    primary_key,
                    DEFAULT_ROWS_TO_UPDATE
                )
                
                # Update existing rows with predefined set of columns
                if len(ids) > 0:
                    data = factory_to_dict(factory, len(ids))
                    update_df = pd.DataFrame(data)
                    update_df[primary_key] = ids
                    update_cols = orm_config.get('update_cols', [
                        col for col in update_df.columns if col != primary_key
                    ])
                    
                    # Ensure that the timestamp is updated as well on the record
                    update_cols.append('_etl_updated_timestamp')
                    with source_engine.begin() as conn:
                        
                        # Insert raw data into temp table
                        dataframe_to_sql(
                            update_df,
                            source_engine,
                            'temp_update_tbl',
                            source_schema,
                            if_exists='replace',
                            unique_subset=orm_config.get('unique_subset', None)
                        )
                        
                        # Create update statment
                        update_sql = f'''
                        update {source_schema}.{source_table.name} as s
                        set {', '.join([f's.{col} = t.{col}' for col in update_cols])}
                        from {source_schema}.temp_update_tbl as t
                        where s.{primary_key} = t.{primary_key};
                        '''
                        conn.execute(update_sql)
                        conn.execute(f'drop table {source_schema}.temp_update_tbl;')
                        
                    logging.info(f'{source_schema}.{source_table.name} table has been updated with {len(ids)} rows.')
                
            # Create some new data
            new_rows = int(round(total_rows * orm_config['perc_of_total_rows'], 0))
            if new_rows:
                data = factory_to_dict(factory, new_rows)
                df = pd.DataFrame(data)
                dataframe_to_sql(
                    df,
                    source_engine, 
                    source_table.name, 
                    source_schema,
                    unique_subset=orm_config.get('unique_subset', None)
                )
                logging.info(f'{source_schema}.{source_table.name} table has {len(df)} new rows.')

        if 'post_sql' in etl_source.keys():
            with source_engine.begin() as conn:
                for sql_statement in etl_source['post_sql']:
                    conn.execute(sql_statement)
            logging.info('post_sql executed')
    
    logging.info('Fake data generated!')
