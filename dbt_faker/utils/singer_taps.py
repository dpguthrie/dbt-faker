# stdlib
import json
import os
from typing import Dict

# third party
from github import Github


MAPPING = {
    'string': 'sa.String',
    'number': 'sa.Numeric',
    'object': 'SnowflakeJSON',
    'array': 'SnowflakeArray',
    'date-time': 'sa.DateTime',
    'boolean': 'sa.Boolean',
    'integer': 'sa.Integer',
    'singer.decimal': 'sa.Numeric',
    'date': 'sa.Date',
    'uri': 'sa.String',
}

DEFAULT_FAKER = {
    'sa.String': "factory.Faker('name')",
    'sa.Numeric': "factory.Faker('random_int', min=0, max=1000)",
    'sa.DateTime': "factory.Faker('date_time_this_month')",
    'sa.Boolean': "factory.Faker('pybool')",
    'sa.Date': "factory.Faker('date_this_month')",
    'SnowflakeJSON': "factory.Faker('pydict', nb_elements=1)",
    'SnowflakeArray': "factory.Faker('pylist', nb_elements=1)",
    'sa.Integer': "factory.Faker('random_int', min=0, max=1000)",
}

ROOT_DIR = os.path.realpath(
    os.path.join(
        os.path.dirname(__file__),
        '..',
    )
)


def convert(word):
    return ''.join(x.capitalize() or '_' for x in word.split('_'))


class SingerRepo:
    def __init__(
        self, 
        repo: str, 
        *, 
        schema_name: str = None, 
        pk_field: str = 'id',
        token: str = None,
        file_prefix: str = '',
    ):
        self.token = token or os.getenv('GH_TOKEN', None)
        if self.token is None:
            raise ValueError(
                'Token not provided and/or not found as environment variable "GH_TOKEN"'
            )
        self.github_client = Github(self.token)
        self.repo = repo
        self.schema_name = schema_name or self.repo.replace('tap-', '').replace('-', '_')
        self.pk_field = pk_field
        self.include_snowflake_json = False
        self.include_snowflake_array = False
        self.file_prefix = file_prefix
        
    @property
    def get_schemas(self):
        schemas = {}
        r = self.github_client.get_repo(f'singer-io/{self.repo}')
        files = r.get_dir_contents(f'{self.repo.replace("-", "_")}/schemas')
        for file in files:
            if 'json' in file.name:
                schema = file.name.replace('.json', '')
                content = file.decoded_content
                schemas[schema] = json.loads(content.decode('utf-8'))
        return schemas
    
    def _add_orm_defaults(self, table_name: str, indent: str = '    '):
        """Add default args to the model."""
        default_string = indent + f"__tablename__ = '{table_name}'\n"
        return default_string
    
    def _add_factory_defaults(self, class_name: str, indent: str = '    '):
        """Add defaults to the factory class."""
        default_string = f'{indent}class Meta:\n'
        default_string += f'{indent}{indent}model = {self.file_prefix}{self.schema_name}.{class_name}\n'
        default_string += f'{indent}{indent}sqlalchemy_session = Session\n\n'
        return default_string
    
    def schema_to_str(self, table_name: str, schema_dict: Dict):
        """Convert individual json schema into a sqlalchemy model."""
        orm_result = ''
        factory_result = ''
        if 'properties' in schema_dict.keys():
            class_name = convert(table_name)
            orm_result += f'class {class_name}(WarehouseBase, {self.schema_name.capitalize()}Mixin):\n'
            factory_result += f'class {class_name}Factory(dbtFactory):\n'
            indent = '    '
            orm_result += self._add_orm_defaults(table_name, indent)
            factory_result += self._add_factory_defaults(class_name, indent)
            if self.pk_field in schema_dict['properties']:
                col_type = self._get_col_type(schema_dict['properties']['id'])
                orm_result += f'{indent}{self.pk_field} = sa.Column({col_type}, primary_key=True)\n\n'  # noqa: E501
                factory_result += f'{indent}{self.pk_field} = {DEFAULT_FAKER[col_type]}\n\n'
            else:
                orm_result += '\n'
            for column_name, column_def in sorted(schema_dict['properties'].items()):
                if column_name != self.pk_field:
                    col_type = self._get_col_type(column_def)
                    if col_type is not None:
                        orm_result += f'{indent}{column_name} = sa.Column({col_type})\n'
                        factory_result += f'{indent}{column_name} = {DEFAULT_FAKER[col_type]}\n'
                    else:
                        print('Bad column: ', column_def)
        return f'{orm_result}\n\n', f'{factory_result}\n\n'
    
    def _add_orm_imports(self):
        """Import statements to be used at the top of the created file."""
        import_str = '# stdlib\nfrom typing import Any\n\n'
        import_str += '# third party\nimport sqlalchemy as sa\n\n'
        import_str += '# first party\nfrom dbt_faker.db.orms import '
        first_party_imports = ['EtlBookkeepingMixin', 'WarehouseBase']
        if self.include_snowflake_json:
            first_party_imports.append('SnowflakeJSON')
        if self.include_snowflake_array:
            first_party_imports.append('SnowflakeArray')
        import_str += f'{", ".join(sorted(first_party_imports))}\n\n\n'
        import_str += f'class {self.schema_name.capitalize()}Mixin(EtlBookkeepingMixin):\n'
        import_str += f"    __table_args__ = {{'schema': '{self.schema_name}'}}\n\n\n"
        return import_str
    
    def _add_factory_imports(self):
        import_str = '# third party\nimport factory\n\n'
        path = f'{self.file_prefix}{self.schema_name}'
        import_str += f'# first party\nfrom dbt_faker.orms import {path}\n'
        import_str += f'from dbt_faker.factories.common import dbtFactory, Session\n\n\n'
        return import_str
    
    def _get_col_type(self, col_def: Dict):
        try:
            if isinstance(col_def['type'], str):
                col_type = col_def['type']
            else:
                col_type = col_def['type'][1]
            
        # Most likely a $ref/shared/<file>.json
        except KeyError:
            print(col_def)
            col_type = 'object'
            
        # Some columns don't contain 'null' as the first item
        except IndexError:
            col_type = col_def['type'][0]
            
        # Ensure we can catch any data types not included in MAPPING
        try:
            sa_col_type = MAPPING[col_type]
        except KeyError:
            print(f'"{col_type}" is not implemented')
            sa_col_type = None
            
        if sa_col_type == 'SnowflakeJSON':
            self.include_snowflake_json = True
            
        if sa_col_type == 'SnowflakeArray':
            self.include_snowflake_array = True
            
        # Some 'string' data types contain additional info in a 'format' key
        if col_type == 'string' and 'format' in col_def.keys():
            try:
                sa_col_type = MAPPING[col_def['format']]
            except KeyError:
                raise NotImplementedError(
                    f'Format {col_def["format"]} is not implemented'
                )
        return sa_col_type
    
    def create_files(self):
        schemas = self.get_schemas
        orms = ''
        factories = ''
        for table_name, schema_dict in schemas.items():
            orm, factory = self.schema_to_str(table_name, schema_dict)
            orms += orm
            factories += factory
        orm_imports = self._add_orm_imports()
        factory_imports = self._add_factory_imports()
        orm_file_contents = f'{orm_imports}{orms}'
        factory_file_contents = f'{factory_imports}{factories}'
        orm_file_name = os.path.join(ROOT_DIR, 'orms', f'{self.file_prefix}{self.schema_name}.py')
        factory_file_name = os.path.join(ROOT_DIR, 'factories', f'{self.file_prefix}{self.schema_name}.py')
        with open(orm_file_name, 'w') as f:
            f.write(orm_file_contents)
        print(f'{orm_file_name} successfully created!')
        with open(factory_file_name, 'w') as f:
            f.write(factory_file_contents)
        print(f'{factory_file_name} successfully created!')
