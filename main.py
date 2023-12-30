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
    Fetches historical OHLCV data for a cryptocurrency pair from CryptoCompare API.

    Parameters:
    - fsym (str): The symbol of the base cryptocurrency (e.g., 'BTC').
    - tsym (str): The symbol of the target currency (e.g., 'USD').
    - end_date (datetime, optional): The end date for fetching historical data.
    - limit (int, optional): The number of data points to return per batch. Default is 2000.

    Returns:
    - list: A list of dictionaries containing the requested historical data.
    """
    aggregated_data = []
    toTs = None  # Initialize toTs for the first request
    retries = 0

    while True:
        params = {
            'fsym': fsym,
            'tsym': tsym,
            'limit': limit,
            'toTs': toTs,
            'api_key': API_KEY
        }
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)  # Set timeout for the request
            response.raise_for_status()
            data = response.json()['Data']['Data']
            if not data:
                break  # Break the loop if no data is returned

            # Data Validation
            if not all(validate_data(point) for point in data):
                logging.error(f'Data validation failed for {fsym} to {tsym}')
                raise ValueError('Invalid data structure received from API')

            aggregated_data.extend(data)

            # Update toTs to the timestamp of the earliest data point received
            toTs = data[0]['time']
            if end_date and datetime.datetime.fromtimestamp(toTs) < end_date:
                break  # Break the loop if the end date is reached

            time.sleep(1)  # Delay to avoid hitting rate limits
            retries = 0  # Reset retry count after a successful request
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as err:
            logging.error(f'Connection/Timeout error occurred for {fsym} to {tsym}: {err}')
            retries += 1
            if retries > MAX_RETRIES:
                raise
            time.sleep(5)  # Wait for 5 seconds before retrying
        except requests.exceptions.HTTPError as err:
            logging.error(f'HTTP error occurred for {fsym} to {tsym}: {err}')
            raise
        except Exception as err:
            logging.error(f'Other error occurred for {fsym} to {tsym}: {err}')
            raise

    return aggregated_data


# Example usage
# end_date = datetime.datetime(2020, 1, 1)  # Replace with your desired end date
# data = fetch_data('BTC', 'USD', end_date)


btc_data = fetch_data('BTC', 'USD', limit=2000)
btc_df = pd.DataFrame(btc_data['Data']['Data'])
