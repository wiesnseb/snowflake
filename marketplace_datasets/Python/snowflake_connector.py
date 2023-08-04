import configparser
from sqlalchemy import create_engine
from api_call import *


def get_snowflake_engine(snowflake_database, snowflake_schema):
    # Read the SnowSQL config file
    config = configparser.ConfigParser()
    config.read('/Users/sebastianwiesner/.snowsql/config')

    # Retrieve the connection details
    snowflake_account = config.get('connections.NIMBUS', 'accountname')
    snowflake_user = config.get('connections.NIMBUS', 'username')
    snowflake_password = config.get('connections.NIMBUS', 'password')
    snowflake_role = config.get('connections.NIMBUS', 'rolename')
    snowflake_warehouse = config.get('connections.NIMBUS', 'warehousename')
    snowflake_database = snowflake_database
    snowflake_schema = snowflake_schema

    conn_string = f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{snowflake_database}/{snowflake_schema}?role={snowflake_role}&warehouse={snowflake_warehouse}"
    engine = create_engine(conn_string)

    return engine
