import pandas as pd
import requests

def get_data_as_dataframe(url):
    response = requests.get(url)
    response.raise_for_status()  # Check for any errors in the HTTP request

    data = response.json()
    df = pd.DataFrame(data['value'])

    return df