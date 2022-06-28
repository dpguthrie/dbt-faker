# stdlib
import json

# third party
import sqlalchemy as sa



class SnowflakeJSON(sa.types.TypeDecorator):
    impl = sa.JSON

    def load_dialect_impl(self, dialect):
        if dialect.name == 'snowflake':
            return dialect.type_descriptor(sa.Text)
        else:
            return dialect.type_descriptor(self.impl)

    def process_bind_param(self, value, dialect):
        if dialect.name == 'snowflake':
            return json.dumps(value)
        else:
            return value

    def process_result_value(self, value, dialect):
        if dialect.name == 'snowflake' and value is not None:
            value = json.loads(value)
        return value


class SnowflakeArray(SnowflakeJSON):
    impl = sa.ARRAY
