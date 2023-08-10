# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session


# Write directly to the app
st.title("If this works im happy")
st.write(
   """PLZZZZZZ.
   """
)

# Get the current credentials
session = get_active_session()
