import os
import configparser
from snowflake.connector.pandas_tools import pd_writer
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


def write_table(df, snowflake_table, snowflake_database, snowflake_schema, if_exists='replace'):
    """
    Writes a pandas DataFrame to a Snowflake table.

    Parameters:
    - df (pandas.DataFrame): The DataFrame to be written to Snowflake.
    - snowflake_database (str): The name of the Snowflake database.
    - snowflake_schema (str): The name of the Snowflake schema.
    - snowflake_table (str): The name of the Snowflake table.
    - if_exists (str, optional): Action to take if the table already exists. Possible values are 'fail', 'replace', and 'append'. Default is 'replace'.

    Returns:
    None

    Raises:
    - snowflake.connector.errors.DatabaseError: If there is an error connecting to Snowflake or executing the SQL statement.

    Example:
    >>> df = pd.DataFrame({'column1': [1, 2, 3], 'column2': [10, 20, 30]})
    >>> snowflake_database = 'my_database'
    >>> snowflake_schema = 'my_schema'
    >>> snowflake_table = 'my_table'
    >>> write_table(df, snowflake_database, snowflake_schema, snowflake_table, if_exists='append')
    """

    engine = get_snowflake_engine(snowflake_database, snowflake_schema)

    #Write the data to Snowflake, using pd_writer to speed up loading
    with engine.connect() as con:
        df.to_sql(name=snowflake_table.lower(), con=con, if_exists=if_exists, method=pd_writer, index=False)


def add_column_comments(snowflake_database, snowflake_schema, column_comments, snowflake_table):
    """
    Adds comments to specific columns in a Snowflake table.

    Parameters:
    - snowflake_database (str): The name of the Snowflake database.
    - snowflake_schema (str): The name of the Snowflake schema.
    - column_comments (dict): A dictionary where the keys are column names and the values are the corresponding comments.
    - snowflake_table (str): The name of the Snowflake table.

    Returns:
    None

    Raises:
    - snowflake.connector.errors.DatabaseError: If there is an error connecting to Snowflake or executing the SQL statement.

    Example:
    >>> snowflake_database = 'my_database'
    >>> snowflake_schema = 'my_schema'
    >>> column_comments = {'column1': 'This is the first column.', 'column2': 'This is the second column.'}
    >>> snowflake_table = 'my_table'
    >>> add_column_comments(snowflake_database, snowflake_schema, column_comments, snowflake_table)
    """
    
    engine = get_snowflake_engine(snowflake_database, snowflake_schema)

    with engine.connect() as con:
        # Retrieve the current table description from Snowflake
        query = f"DESCRIBE TABLE {snowflake_database}.{snowflake_schema}.{snowflake_table}"
        result = con.execute(query)
        table_description = result.fetchall()

        # Update column comments based on the provided dictionary
        for column_name, column_comment in column_comments.items():
            # Find the column in the table description
            matching_columns = [col for col in table_description if col[0].lower() == column_name.lower()]
            if matching_columns:
                # Modify the column description to include the provided comment
                column_description = matching_columns[0][1]
                if column_description:
                    # If the column already has a description, append the new comment
                    new_description = f"{column_description}. {column_comment}"
                else:
                    # If the column has no description, use the new comment
                    new_description = column_comment

                # Alter the table to update the column description
                alter_query = f"ALTER TABLE {snowflake_database}.{snowflake_schema}.{snowflake_table} MODIFY COLUMN {column_name} COMMENT '{new_description}'"
                con.execute(alter_query)
            else:
                print(f"Column '{column_name}' not found in table '{snowflake_table}'")


def add_table_comment(snowflake_database, snowflake_schema, snowflake_table, table_comment):
    """
    Adds a comment to a Snowflake table.

    Parameters:
    - snowflake_database (str): The name of the Snowflake database.
    - snowflake_schema (str): The name of the Snowflake schema.
    - snowflake_table (str): The name of the Snowflake table.
    - table_comment (str): The comment to be added to the table.

    Returns:
    None

    Raises:
    - snowflake.connector.errors.DatabaseError: If there is an error connecting to Snowflake or executing the SQL statement.

    Example:
    >>> snowflake_database = 'my_database'
    >>> snowflake_schema = 'my_schema'
    >>> snowflake_table = 'my_table'
    >>> table_comment = 'This is a sample table comment.'
    >>> add_table_comment(snowflake_database, snowflake_schema, snowflake_table, table_comment)
    """

    engine = get_snowflake_engine(snowflake_database, snowflake_schema)

    with engine.connect() as con:
        # Alter the table to add the table comment
        alter_query = f"COMMENT ON TABLE {snowflake_database}.{snowflake_schema}.{snowflake_table} IS '{table_comment}'"
        con.execute(alter_query)