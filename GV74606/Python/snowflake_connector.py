import os
from snowflake.connector.pandas_tools import pd_writer
from sqlalchemy import create_engine
from api_call import *


def write_table(df, snowflake_database, snowflake_schema, snowflake_table, if_exists='replace'):
    snowflake_account = os.environ.get('SNOWFLAKE_ACCOUNT')
    snowflake_user = os.environ.get('SNOWFLAKE_USER')
    snowflake_password = os.environ.get('SNOWFLAKE_PASSWORD')

    conn_string = f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{snowflake_database}/{snowflake_schema}"
    engine = create_engine(conn_string)

    #Write the data to Snowflake, using pd_writer to speed up loading
    with engine.connect() as con:
        df.to_sql(name=snowflake_table.lower(), con=con, if_exists=if_exists, method=pd_writer, index=False)


def add_column_comments(snowflake_database, snowflake_schema, column_comments, snowflake_table):
    snowflake_account = os.environ.get('SNOWFLAKE_ACCOUNT')
    snowflake_user = os.environ.get('SNOWFLAKE_USER')
    snowflake_password = os.environ.get('SNOWFLAKE_PASSWORD')

    conn_string = f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{snowflake_database}/{snowflake_schema}"
    engine = create_engine(conn_string)

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
    snowflake_account = os.environ.get('SNOWFLAKE_ACCOUNT')
    snowflake_user = os.environ.get('SNOWFLAKE_USER')
    snowflake_password = os.environ.get('SNOWFLAKE_PASSWORD')

    conn_string = f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{snowflake_database}/{snowflake_schema}"
    engine = create_engine(conn_string)

    with engine.connect() as con:
        # Alter the table to add the table comment
        alter_query = f"COMMENT ON TABLE {snowflake_database}.{snowflake_schema}.{snowflake_table} IS '{table_comment}'"
        con.execute(alter_query)