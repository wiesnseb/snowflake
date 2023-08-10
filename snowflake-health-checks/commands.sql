-- THESE COMMANDS ARE MANUALL EXECTUED

-- Right to create application packages to accountadmin (should be default actually)
GRANT CREATE APPLICATION PACKAGE ON ACCOUNT TO ROLE accountadmin;


-- Creation of the application package (not application itself)
CREATE APPLICATION PACKAGE health_check_package;


-- Schema for the stage
CREATE SCHEMA health_check_package.stage_content;

-- Stage for the application files
CREATE OR REPLACE STAGE health_check_package.stage_content.health_check_stage
FILE_FORMAT = (TYPE = 'csv' FIELD_DELIMITER = '|' SKIP_HEADER = 1);


list @health_check_package.stage_content.health_check_stage;

GRANT REFERENCE_USAGE ON DATABASE DUMMY_DATA
TO SHARE IN APPLICATION PACKAGE HEALTH_CHECK_PACKAGE;

create or replace schema health_check_package.test_data;

CREATE OR REPLACE VIEW health_check_package.test_data.accounts_view
  AS SELECT *
FROM DUMMY_DATA.TEST_DATA.ACCOUNTS;


GRANT USAGE ON SCHEMA health_check_package.test_data
  TO SHARE IN APPLICATION PACKAGE health_check_package;
GRANT SELECT ON VIEW health_check_package.test_data.accounts_view
  TO SHARE IN APPLICATION PACKAGE health_check_package;



-- Creation of the actual application from the files loaded into the stage
-- Minimum files are: manifest.yml, setup.sql, readme.md (i think at least)
CREATE APPLICATION HEALTH_CHECK_APP
FROM APPLICATION PACKAGE health_check_package
USING '@health_check_package.stage_content.health_check_stage';

DROP APPLICATION HEALTH_CHECK_APP;



/*
--root folder
PUT file:////Users/sebastianwiesner/Documents/GitHub/snowflake/snowflake-health-checks/manifest.yml @health_check_package.stage_content.health_check_stage overwrite=true auto_compress=false;
PUT file:////Users/sebastianwiesner/Documents/GitHub/snowflake/snowflake-health-checks/readme.md @health_check_package.stage_content.health_check_stage overwrite=true auto_compress=false;

--/scripts
PUT file:////Users/sebastianwiesner/Documents/GitHub/snowflake/snowflake-health-checks/scripts/setup.sql @health_check_package.stage_content.health_check_stage/scripts overwrite=true auto_compress=false;


--/libraries/procedures
PUT file:////Users/sebastianwiesner/Documents/GitHub/snowflake/snowflake-health-checks/libraries/procedures/hello_python.py @health_check_package.stage_content.health_check_stage/libraries/procedures overwrite=true auto_compress=false;


--libraries/streamlit
PUT file:////Users/sebastianwiesner/Documents/GitHub/snowflake/snowflake-health-checks/libraries/streamlit/Home.py @health_check_package.stage_content.health_check_stage/libraries/streamlit overwrite=true auto_compress=false;


--libraries/streamlit/pages
PUT file:////Users/sebastianwiesner/Documents/GitHub/snowflake/snowflake-health-checks/libraries/streamlit/pages/Test.py @health_check_package.stage_content.health_check_stage/libraries/streamlit/pages overwrite=true auto_compress=false;

*/