CREATE DATABASE SEBAS_DB;

CREATE OR REPLACE STAGE SEBAS_STAGE;

--snowsql -a yt48846.eu-central-1.aws -u DLJuly23
--PUT ‘file:///Users/sebastianwiesner/Documents/GitHub/snowflake/os62374/milk_supply_and_dairy_production.json’ @sebas_stage;

LIST @SEBAS_STAGE;