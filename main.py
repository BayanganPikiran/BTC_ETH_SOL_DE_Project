import requests
import logging
import time
import datetime
from dotenv import load_dotenv
import os
#TODO: pip freeze to add packages to the requirements.txt


load_dotenv()  # Loads the variables from '.env'

API_KEY = os.environ.get('CRYPTOCOMPARE_API_KEY')
if not API_KEY:
    raise ValueError("API key not found. Please set the CRYPTOCOMPARE_API_KEY in the .env file.")


# Set up basic logging
logging.basicConfig(filename='crypto_data_fetch.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

API_KEY = 'your_api_key'
BASE_URL = 'https://min-api.cryptocompare.com/data/v2/histoday'
MAX_RETRIES = 3  # Maximum number of retries for a request


def validate_data(data):
    """
    Validates the structure of the API response data.

    Parameters:
    - data (dict): The data to validate.

    Returns:
    - bool: True if the data is valid, False otherwise.
    """
    required_keys = ['time', 'high', 'low', 'open', 'close', 'volumefrom', 'volumeto']
    return all(key in data for key in required_keys)


def fetch_data(fsym, tsym, end_date=None, limit=2000):
    """
    Fetch historical OHLCV (open, high, low, close, volume) data for a specified cryptocurrency from the CryptoCompare API.

    The function fetches data daily from the specified start date to the end date. It handles pagination automatically to retrieve data beyond the API's single-call limit and includes a retry mechanism for handling request failures.

    Parameters:
    - crypto (str): Symbol of the cryptocurrency (e.g., 'BTC', 'ETH', 'SOL').
    - start_date (str): Start date for fetching data in 'YYYY-MM-DD' format.
    - end_date (str, optional): End date for fetching data in 'YYYY-MM-DD' format. Defaults to the current date.
    - limit (int, optional): The maximum number of data points to return per API call. Defaults to 2000.

    Returns:
    - DataFrame: A pandas DataFrame containing the historical data for the specified cryptocurrency.

    Raises:
    - Exception: If the maximum number of retries is exceeded during API requests.
    """
    data = []
    toTs = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d'))
    retries = 0
    max_retries = 5
    retry_delay = 10  # seconds

    while True:
        params = {
            'fsym': crypto,
            'tsym': 'USD',
            'limit': limit,
            'toTs': toTs,
            'api_key': API_KEY
        }

        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            batch = response.json()['Data']['Data']
            data.extend(batch)

            # Check if we've reached the start date
            if datetime.fromtimestamp(batch[-1]['time']) < datetime.strptime(start_date, '%Y-%m-%d'):
                break

            toTs = batch[0]['time']  # Prepare toTs for the next call
            retries = 0  # Reset retries after a successful call
        except requests.exceptions.RequestException as e:
            retries += 1
            if retries > max_retries:
                raise
            time.sleep(retry_delay)  # Wait before retrying

    return pd.DataFrame(data)


# Example usage
# end_date = datetime.datetime(2020, 1, 1)  # Replace with your desired end date
# data = fetch_data('BTC', 'USD', end_date)


btc_data = fetch_data('BTC', 'USD', limit=2000)
btc_df = pd.DataFrame(btc_data['Data']['Data'])
