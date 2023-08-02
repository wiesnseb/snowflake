LIST @SEBAS_STAGE;

CREATE OR REPLACE TABLE RAW_MILK_SUPPLY_AND_DAIRY_PRODUCTION (
    JSON_DATA VARIANT
);

--https://opendata.cbs.nl/statline/portal.html?_la=en&_catalog=CBS&tableId=7425eng&_theme=1125
COPY INTO RAW_MILK_SUPPLY_AND_DAIRY_PRODUCTION(JSON_DATA)
FROM @SEBAS_STAGE/milk_supply_and_dairy_production.json
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);

SELECT * FROM RAW_MILK_SUPPLY_AND_DAIRY_PRODUCTION;


CREATE OR REPLACE TABLE MILK_SUPPLY_AND_DAIRY_PRODUCTION AS SELECT
    json_data.value:ID::INT AS ID
    , substring(json_data.value:Periods::VARCHAR , 0 , 4 ) AS YEAR
    , substring(json_data.value:Periods::VARCHAR , 7 , 2 ) AS MONTH
    , CASE WHEN substring(json_data.value:Periods::VARCHAR, 7, 2) = '00' THEN 'TRUE' ELSE 'FALSE' END AS IS_YEARLY_SUMMARY
    , json_data.value:Volume_1::FLOAT AS VOLUME
    , json_data.value:Butter_4::FLOAT AS BUTTER
    , json_data.value:FatContent_2::FLOAT AS FAT_CONTENT
    , json_data.value:ProteinContent_3::FLOAT AS PROTEIN_CONTENT
    , json_data.value:TotalMilkPowder_6::FLOAT AS TOTAL_MILK_POWDER
    , json_data.value:WholeMilkPowder_7::FLOAT AS WHOLE_MILK_POWDER
    , json_data.value:SkimmedMilkPowder_8::FLOAT AS SKIMMED_MILK_POWDER
    , json_data.value:ConcentratedMilk_9::FLOAT AS CONCENTRATED_MILK
    , json_data.value:Cheese_5::FLOAT AS CHEESE
    , json_data.value:WheyPowder_10::NUMBER AS WHEY_POWDER
FROM RAW_MILK_SUPPLY_AND_DAIRY_PRODUCTION,
    LATERAL FLATTEN(input => json_data:value) json_data
;


UPDATE MILK_SUPPLY_AND_DAIRY_PRODUCTION
SET WHEY_POWDER = NULL
WHERE WHEY_POWDER = 0;


SELECT * FROM MILK_SUPPLY_AND_DAIRY_PRODUCTION ORDER BY ID ASC;