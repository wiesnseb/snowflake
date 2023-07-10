import os
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine
from api_call import *


def write_table(df, snowflake_database, snowflake_schema, snowflake_table):
    snowflake_account = os.environ.get('SNOWFLAKE_ACCOUNT')
    snowflake_user = os.environ.get('SNOWFLAKE_USER')
    snowflake_password = os.environ.get('SNOWFLAKE_PASSWORD')

    conn_string = f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{snowflake_database}/{snowflake_schema}"
    engine = create_engine(conn_string)

    #What to do if the table exists? replace, append, or fail?
    if_exists = 'replace'

    #Write the data to Snowflake, using pd_writer to speed up loading
    with engine.connect() as con:
        df.to_sql(name=snowflake_table.lower(), con=con, if_exists=if_exists, method=pd_writer, index=False)