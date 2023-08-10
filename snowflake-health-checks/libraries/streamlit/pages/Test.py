import streamlit as st
from snowflake.snowpark.context import get_active_session

st.header("Welcome to the test page")
st.write("This page needs to work")



# Get the current credentials
session = get_active_session()

#  Create an example data frame
data_frame = session.sql("SELECT * FROM code_schema.final_view;")

# Execute the query and convert it into a Pandas data frame
queried_data = data_frame.to_pandas()

# Display the Pandas data frame as a Streamlit data frame.
st.dataframe(queried_data, use_container_width=True)
