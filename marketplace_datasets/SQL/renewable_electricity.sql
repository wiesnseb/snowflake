LIST @SEBAS_STAGE;

CREATE OR REPLACE TABLE RAW_RENEWABLE_ELECTRICITY(
    JSON_DATA VARIANT
);

--https://opendata.cbs.nl/statline/portal.html?_la=en&_catalog=CBS&tableId=82610ENG&_theme=1068
COPY INTO RAW_RENEWABLE_ELECTRICITY(JSON_DATA)
FROM @SEBAS_STAGE/renewable_electricity.json
FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE);

SELECT * FROM RAW_RENEWABLE_ELECTRICITY;


CREATE OR REPLACE TABLE RENEWABLE_ELECTRICITY AS SELECT
    json_data.value:ID::INT AS ID
    , substring(json_data.value:Periods::VARCHAR , 0 , 4 ) AS YEAR
    , CASE 
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'T001028' THEN 'Total renewable energy sources'
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'E006587' THEN 'Hydropower'
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'E006588' THEN 'Wind energy'
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'E006637' THEN 'Onshore wind energy'
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'E006638' THEN 'Offshore wind energy'
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'E006590' THEN 'Solar photovoltaic'
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'E006566' THEN 'Total biomass'
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'E006661' THEN 'Municipal waste; renewable fraction'
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'E006662' THEN 'Co-firing of biomass in electric plants'
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'E006670' THEN 'Biomass boilers companies'
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'E006583' THEN 'Total biogas'
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'E006673' THEN 'Biogas from landfills'
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'E006674' THEN 'Biogas from sewage water purification'
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'E006586' THEN 'Other biogas'
        WHEN RTRIM(json_data.value:EnergySourcesTechniques::VARCHAR) = 'E006675' THEN 'Biogas'
        ELSE 'Unknown'
    END AS ENERGY_SOURCES_TECHNIQUES
    , TRY_TO_DOUBLE(LTRIM(json_data.value:GrossProductionWithNormalisation_1::VARCHAR)) AS GROSS_PRODUCTION_ELECTRICITY_NORMALIZED
    , TRY_TO_DOUBLE(LTRIM(json_data.value:GrossProductionOfElectricity_2::VARCHAR)) AS GROSS_PRODUCTION_OF_ELECTRICITY
    , TRY_TO_DOUBLE(LTRIM(json_data.value:NetProductionOfElectricity_3::VARCHAR)) AS NET_PRODUCTION_OF_ELECTRICITY
    , TRY_TO_DOUBLE(LTRIM(json_data.value:GrossProductionWithNormalisation_4::VARCHAR)) AS GROSS_PRODUCTION_ELECTRICITY_NORMALIZED_IN_PER_CENT_OF_USE
    , TRY_TO_DOUBLE(LTRIM(json_data.value:GrossProductionOfElectricity_5::VARCHAR)) AS GROSS_PRODUCTION_OF_ELECTRICITY_IN_PER_CENT_OF_USE
    , TRY_TO_DOUBLE(LTRIM(json_data.value:NetProductionOfElectricity_6::VARCHAR)) AS NET_PRODUCTION_OF_ELECTRICITY_IN_PER_CENT_OF_USE
    , TRY_TO_DOUBLE(LTRIM(json_data.value:InstallationsInstalledEndOfYear_7::VARCHAR)) AS INSTALLATION_INSTALLED_END_OF_YEAR
    , TRY_TO_DOUBLE(LTRIM(json_data.value:ElectricalCapacityEndOfYear_8::VARCHAR)) AS ELECTRICAL_CAPACITY_END_OF_YEAR
FROM RAW_RENEWABLE_ELECTRICITY,
    LATERAL FLATTEN(input => json_data:value) JSON_DATA
;


SELECT * FROM RENEWABLE_ELECTRICITY ORDER BY YEAR, ID;
