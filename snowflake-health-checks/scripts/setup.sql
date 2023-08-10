-- Setup script for the health check application.


-- custom role for the application
CREATE APPLICATION ROLE app_health_check;


-- App internal (? i think)
CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE app_health_check;


CREATE VIEW IF NOT EXISTS code_schema.final_view
  AS SELECT *
FROM test_data.accounts_view;
GRANT SELECT ON VIEW code_schema.final_view TO APPLICATION ROLE app_health_check;



-- Python file in stage
CREATE or REPLACE FUNCTION code_schema.multiply(num1 float, num2 float)
  RETURNS float
  LANGUAGE PYTHON
  RUNTIME_VERSION=3.10
  IMPORTS = ('/libraries/procedures/hello_python.py')
  HANDLER='hello_python.multiply';

GRANT USAGE ON FUNCTION code_schema.multiply(FLOAT, FLOAT) TO APPLICATION ROLE app_health_check;


-- Streamlit page in stage
CREATE STREAMLIT code_schema.health_check_streamlit
  FROM '/libraries/streamlit'
  MAIN_FILE = '/Home.py'
;
GRANT USAGE ON STREAMLIT code_schema.health_check_streamlit TO APPLICATION ROLE app_health_check;
