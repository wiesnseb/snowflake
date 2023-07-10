import time
from api_call import*
from sf_connector import*
from transformation import*


def main():
    tic = time.perf_counter()

    snowflake_database = 'SEBAS_DB'
    snowflake_schema = 'PUBLIC'
    snowflake_table = 'renewable_electricity'

    url = "https://opendata.cbs.nl/ODataApi/odata/82610ENG/TypedDataSet"
    df = get_data_as_dataframe(url)

    df = transform_renewable_electricity(df)

    write_table(df, snowflake_database, snowflake_schema, snowflake_table)

    toc = time.perf_counter()
    print(f"Executed in {toc - tic:0.4f} seconds")

if __name__ == "__main__":
    main()