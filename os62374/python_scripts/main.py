import time
from api_call import*
from snowflake_connector import*
from transformation import*


def main():
    tic = time.perf_counter()

    snowflake_database = 'SEBAS_DB'
    snowflake_schema = 'PUBLIC'

    # Dataset renewable electricity
    df_renewable_electricity = get_data_as_dataframe('https://opendata.cbs.nl/ODataApi/odata/82610ENG/TypedDataSet')
    df_renewable_electricity = transform_renewable_electricity(df_renewable_electricity)
    write_table(df_renewable_electricity, snowflake_database, snowflake_schema, snowflake_table='renewable_electricity')

    # Dataset milk supply and dairy production
    df_milk_supply_and_dairy_production = get_data_as_dataframe('https://opendata.cbs.nl/ODataApi/odata/7425eng/TypedDataSet')
    df_milk_supply_and_dairy_production = transform_milk_supply_and_dairy_production(df_milk_supply_and_dairy_production)
    write_table(df_milk_supply_and_dairy_production, snowflake_database, snowflake_schema, snowflake_table='milk_supply_and_dairy_production')

    toc = time.perf_counter()
    print(f"Executed in {toc - tic:0.4f} seconds")

if __name__ == "__main__":
    main()