import time
from api_call import*
from sf_connector import*

snowflake_database = 'SEBAS_DB'
snowflake_schema = 'PUBLIC'
snowflake_table = 'LOL_TABLE'

def main():
    tic = time.perf_counter()

    url = "https://opendata.cbs.nl/ODataApi/odata/82610ENG/TypedDataSet"
    df = get_data_as_dataframe(url)

    write_table(df, snowflake_database, snowflake_schema, snowflake_table)

    toc = time.perf_counter()
    print(f"Finished in {toc - tic:0.4f} seconds")


if __name__ == "__main__":
    main()