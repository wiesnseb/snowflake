import pandas as pd
from snowflake_connector import get_snowflake_engine
from snowflake.connector.pandas_tools import pd_writer


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


def transform_renewable_electricity(df):
    """
    Transforms a given DataFrame containing data into a new DataFrame with modified columns.

    Parameters:
    - df (pandas.DataFrame): The input DataFrame containing data.

    Returns:
    - new_df (pandas.DataFrame): The transformed DataFrame with modified columns.
    """
    new_df = pd.DataFrame()

    new_df['YEAR'] = df['Periods'].str[:4]
    new_df['ENERGY_SOURCES_TECHNIQUES'] = df['EnergySourcesTechniques'].str.strip().map({
        'T001028': 'Total renewable energy sources',
        'E006587': 'Hydropower',
        'E006588': 'Wind energy',
        'E006637': 'Onshore wind energy',
        'E006638': 'Offshore wind energy',
        'E006590': 'Solar photovoltaic',
        'E006566': 'Total biomass',
        'E006661': 'Municipal waste; renewable fraction',
        'E006662': 'Co-firing of biomass in electric plants',
        'E006670': 'Biomass boilers companies',
        'E006583': 'Total biogas',
        'E006673': 'Biogas from landfills',
        'E006674': 'Biogas from sewage water purification',
        'E006586': 'Other biogas',
        'E006675': 'Biogas'
    }).fillna('Unknown')
    new_df['GROSS_PRODUCTION_ELECTRICITY_NORMALIZED'] = pd.to_numeric(df['GrossProductionWithNormalisation_1'], errors='coerce')
    new_df['GROSS_PRODUCTION_OF_ELECTRICITY'] = pd.to_numeric(df['GrossProductionOfElectricity_2'], errors='coerce')
    new_df['NET_PRODUCTION_OF_ELECTRICITY'] = pd.to_numeric(df['NetProductionOfElectricity_3'], errors='coerce')
    new_df['GROSS_PRODUCTION_ELECTRICITY_NORMALIZED_IN_PER_CENT_OF_USE'] = pd.to_numeric(df['GrossProductionWithNormalisation_4'], errors='coerce')
    new_df['GROSS_PRODUCTION_OF_ELECTRICITY_IN_PER_CENT_OF_USE'] = pd.to_numeric(df['GrossProductionOfElectricity_5'], errors='coerce')
    new_df['NET_PRODUCTION_OF_ELECTRICITY_IN_PER_CENT_OF_USE'] = pd.to_numeric(df['NetProductionOfElectricity_6'], errors='coerce')
    new_df['INSTALLATION_INSTALLED_END_OF_YEAR'] = pd.to_numeric(df['InstallationsInstalledEndOfYear_7'], errors='coerce')
    new_df['ELECTRICAL_CAPACITY_END_OF_YEAR'] = pd.to_numeric(df['ElectricalCapacityEndOfYear_8'], errors='coerce')

    return new_df


def transform_milk_supply_and_dairy_production(df):
    """
    Transforms a given DataFrame containing data into a new DataFrame with modified columns.

    Parameters:
    - df (pandas.DataFrame): The input DataFrame containing data.

    Returns:
    - new_df (pandas.DataFrame): The transformed DataFrame with modified columns.
    """
    new_df = pd.DataFrame()
    
    new_df['YEAR'] = df['Periods'].str[:4]
    new_df['MONTH'] = df['Periods'].str[6:8]
    new_df['IS_YEARLY_SUMMARY'] = df['Periods'].str[4:6].map({
        'JJ': True,
        'MM': False,
    }).fillna('Unknown')
    new_df['VOLUME'] = pd.to_numeric(df['Volume_1'], errors='coerce')
    new_df['BUTTER'] = pd.to_numeric(df['Butter_4'], errors='coerce')
    new_df['FAT_CONTENT'] = pd.to_numeric(df['FatContent_2'], errors='coerce')
    new_df['PROTEIN_CONTENT'] = pd.to_numeric(df['ProteinContent_3'], errors='coerce')
    new_df['TOTAL_MILK_POWDER'] = pd.to_numeric(df['TotalMilkPowder_6'], errors='coerce')
    new_df['WHOLE_MILK_POWDER'] = pd.to_numeric(df['WholeMilkPowder_7'], errors='coerce')
    new_df['SKIMMED_MILK_POWDER'] = pd.to_numeric(df['SkimmedMilkPowder_8'], errors='coerce')
    new_df['CONCENTRATED_MILK'] = pd.to_numeric(df['ConcentratedMilk_9'], errors='coerce')
    new_df['CHEESE'] = pd.to_numeric(df['Cheese_5'], errors='coerce')
    new_df['WHEY_POWDER'] = pd.to_numeric(df['WheyPowder_10'], errors='coerce')

    return new_df