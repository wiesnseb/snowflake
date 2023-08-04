import time
from api_call import*
from snowflake_connector import*
from transformation import*

def main():
    tic = time.perf_counter()

    snowflake_database = 'MARKETPLACE'
    snowflake_schema = 'PUBLIC'

    # Dataset renewable electricity
    df_renewable_electricity = get_data_as_dataframe('https://opendata.cbs.nl/ODataApi/odata/82610ENG/TypedDataSet')
    df_renewable_electricity = transform_renewable_electricity(df_renewable_electricity)
    write_table(df=df_renewable_electricity, snowflake_database = snowflake_database, snowflake_schema=snowflake_schema, snowflake_table='renewable_electricity')
    comments_renewable_electricity = {
        'YEAR': 'Year of record.',
        'ENERGY_SOURCES_TECHNIQUES': 'Energy sources / techniques.',
        'GROSS_PRODUCTION_ELECTRICITY_NORMALIZED': 'Gross production of renewable electricity corrected for weather conditions and including the indirect production from green gas (mln kWh).',
        'GROSS_PRODUCTION_OF_ELECTRICITY': 'Gross production of electricity is the production including own use (mln kWh).',
        'NET_PRODUCTION_OF_ELECTRICITY': 'Net production of electricity is the production excluding own use (mln kWh)',
        'GROSS_PRODUCTION_ELECTRICITY_NORMALIZED_IN_PER_CENT_OF_USE': 'Gross production of renewable electricity corrected for weather conditions and including the indirect production from green gas (in % of use).',
        'GROSS_PRODUCTION_OF_ELECTRICITY_IN_PER_CENT_OF_USE': 'Gross production of electricity is the production including own use (in % of use).',
        'NET_PRODUCTION_OF_ELECTRICITY_IN_PER_CENT_OF_USE': 'Net electricity production is the production excluding own use (in % of use)',
        'INSTALLATION_INSTALLED_END_OF_YEAR': 'Number of installations installed at the end of the reporting year (number).',
        'ELECTRICAL_CAPACITY_END_OF_YEAR': 'Electrical capacity of installations installed at the end of the reporting year (megawatt).'
    }
    add_column_comments(snowflake_database=snowflake_database, snowflake_schema=snowflake_schema, snowflake_table='renewable_electricity', column_comments=comments_renewable_electricity)

    table_comments_renewable_electricity = """This table contains information about the Dutch production of renewable electricity, the number of installations used and the installed capacity of these installations.
    During production, a distinction is made between normalised gross production and non-standard gross and net production without normalisation.
    Production of electricity is shown in million kilowatt hours and as a percentage of total electricity consumption in the Netherlands.
    The production of renewable electricity is compared with total electricity consumption and not against total electricity production.
    This choice is due to European conventions. The data is broken down according to the type of energy source and the technique used to obtain the electricity.
    A distinction is made between four main categories: hydro power, wind energy, solar power and biomass.Data available from: 1990.
    This table contains definite figures until 2020, revised provisional figures of 2021 and 2022."""
    add_table_comment(snowflake_database=snowflake_database, snowflake_schema=snowflake_schema, snowflake_table='renewable_electricity', table_comment=table_comments_renewable_electricity)


    # Dataset milk supply and dairy production
    df_milk_supply_and_dairy_production = get_data_as_dataframe('https://opendata.cbs.nl/ODataApi/odata/7425eng/TypedDataSet')
    df_milk_supply_and_dairy_production = transform_milk_supply_and_dairy_production(df_milk_supply_and_dairy_production)
    write_table(df=df_milk_supply_and_dairy_production, snowflake_database=snowflake_database, snowflake_schema=snowflake_schema, snowflake_table='milk_supply_and_dairy_production')
   
    comments_milk_supply_and_dairy_production = {
        'YEAR': 'Year of record.',
        'MONTH': 'Month of record.',
        'IS_YEARLY_SUMMARY': 'Boolean if value is yearly summary (T/F).',
        'VOLUME': '1000 kilograms = 971 litre (1000 kg).',
        'BUTTER': 'All types of butter (1000 kg).',
        'FAT_CONTENT': 'Fat content of raw cows milk (in % ).',
        'PROTEIN_CONTENT': 'Protein content (mainly casein) in raw cows milk (in %).',
        'TOTAL_MILK_POWDER': 'Total milk poweder (whole and skimmed) (1000 kg).',
        'WHOLE_MILK_POWDER': 'Milk powder with 1.5 percent or more fat (1000 kg).',
        'SKIMMED_MILK_POWDER': 'Milk powder with less than 1.5 percent fat (1000 kg).',
        'CONCENTRATED_MILK': 'Thickened milk or concentrated milk is a product obtained by partially removing water (1000 kg).',
        'CHEESE': 'Cheese made of cows milk only (1000 kg).',
        'WHEY_POWDER': 'Whey (in powdered or block form) is a by-product of cheese making (1000 kg).'
    }
    add_column_comments(snowflake_database=snowflake_database, snowflake_schema=snowflake_schema, snowflake_table='milk_supply_and_dairy_production', column_comments=comments_milk_supply_and_dairy_production)
    
    table_comment_milk_supply_and_dairy_production = """Milk supply and dairy production by dairy factories. In the Netherlands about 96 percent of all raw cows milk from dairy farmers is delivered to dairy factories.
    The remaining 4 percent is kept by the dairy farmers themselves for their own use (to feed young cattle and/or manufacture dairy products).
    This table contains data about the volume of cows milk delivered by dairy farmers and the products manufactured by the dairy industry in the Netherlands.
    The table contains monthly figures as well as yearly figures. The figures on raw cows milk concern volume, protein content and fat content.
    Dairy products include butter, cheese, milk powder, concentrated milk and whey in powder or block form.The data in this table are provided by the Netherlands Enterprise Agency (RVO) gathers data for two series on dairy products, namely monthly statistics and yearly statistics.
    As the monthly figures are based on about 98 percent of all cows milk delivered to dairy factories, a provisional adjustment is done by the Agency. This adjustment only concerns collected milk and these monthly figures are revised when the yearly figures become available.
    The table does not contain figures about fresh products such as drinking milk for consumption or acidified milk products such as yoghurts.
    Figures about these products are only available for the years 1995-1997. Data available from: January 1995"""
    add_table_comment(snowflake_database=snowflake_database, snowflake_schema=snowflake_schema, snowflake_table='milk_supply_and_dairy_production', table_comment=table_comment_milk_supply_and_dairy_production)

    toc = time.perf_counter()
    print(f"Executed in {toc - tic:0.4f} seconds")

if __name__ == "__main__":
    main()