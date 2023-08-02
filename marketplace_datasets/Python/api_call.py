import pandas as pd
import requests

def get_data_as_dataframe(url):
    """
    Retrieves data from a specified URL and returns it as a pandas DataFrame.

    Parameters:
    - url (str): The URL from which to fetch the data.

    Returns:
    - df (pandas.DataFrame): The data fetched from the URL, converted into a pandas DataFrame.

    Raises:
    - requests.HTTPError: If there is an error in the HTTP request, such as a 404 Not Found.

    Example:
    >>> url = "https://api.example.com/data"
    >>> df = get_data_as_dataframe(url)
    >>> print(df.head())
        column1  column2
    0       1       10
    1       2       20
    2       3       30
    3       4       40
    4       5       50
    """
    response = requests.get(url)
    response.raise_for_status()  # Check for any errors in the HTTP request

    data = response.json()
    df = pd.DataFrame(data['value'])

    return df