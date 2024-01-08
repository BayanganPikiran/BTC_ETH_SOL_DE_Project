from typing import Dict, Tuple
import requests
import logging
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv
import os
import traceback  # For detailed error logging
import psutil

# Test run dates (e.g., one week)
TEST_START_DATE = '2023-01-01'
TEST_END_DATE = '2023-01-08'

# Full run dates
FULL_START_DATE = '2020-04-10'
FULL_END_DATE = '2024-01-03'

# Toggle for test mode
IS_TEST_MODE = True  # Set to False for a full run

load_dotenv()  # Load the API key from the .env file

API_KEY = os.environ.get('CRYPTOCOMPARE_API_KEY')
if not API_KEY:
    raise ValueError("API key not found. Please set the CRYPTOCOMPARE_API_KEY in the .env file.")

# API endpoint for hourly data
BASE_URL = 'https://min-api.cryptocompare.com/data/v2/histohour'
MAX_RETRIES = 5  # Maximum number of retries for API requests


def validate_hourly_data(data: Dict) -> bool:
    """
    Validates whether the required keys are present in a given data record.

    This function checks if each of the required keys for a valid cryptocurrency data record
    is present in the provided data dictionary. It's used to ensure the integrity and completeness
    of the data fetched from the API.

    Parameters:
    - data (dict): The data record to validate, typically a dictionary representing a single
      cryptocurrency data point (e.g., daily historical data for BTC).

    Returns:
    - bool: Returns True if all required keys are present in the data, False otherwise.
    """
    required_keys = ['time', 'high', 'low', 'open', 'close', 'volumefrom', 'volumeto']
    return all(key in data for key in required_keys)


def fetch_hourly_data(fsym: str, tsym: str, start_date: str, end_date: str = None, limit=2000) -> pd.DataFrame:
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    all_data = []
    toTs = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d'))
    start_timestamp = datetime.timestamp(datetime.strptime(start_date, '%Y-%m-%d'))
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
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            batch = response.json()['Data']['Data']

            data_chunk = pd.DataFrame(batch)
            data_chunk['time'] = pd.to_datetime(data_chunk['time'], unit='s')  # Vectorized operation for converting time

            all_data.append(data_chunk)

            if data_chunk['time'].min() < datetime.fromtimestamp(start_timestamp):
                break

            toTs = int(data_chunk['time'].min().timestamp())
        except requests.exceptions.RequestException as e:
            retries += 1
            logging.error(f"Request exception for {fsym}: {e}")
            if retries > MAX_RETRIES:
                logging.error(f"Max retries reached for {fsym}. Last attempt failed with: {e}")
                raise
            logging.info(f"Retrying ({retries}/{MAX_RETRIES}) for {fsym} after failure.")
            time.sleep(10)

    # Concatenate all chunks
    data_df = pd.concat(all_data, ignore_index=True)

    # Data Integrity Checks
    # Timestamp Continuity Check
    expected_time_range = pd.date_range(start=start_date, end=end_date, freq='H')
    actual_time_range = pd.to_datetime(data_df['time']).dt.floor('H')
    missing_times = expected_time_range.difference(actual_time_range)
    if not missing_times.empty:
        logging.warning(f"Missing timestamps for {fsym}: {missing_times}")

    # Duplicate Record Check
    if data_df.duplicated().any():
        logging.warning(f"There are duplicate records in the data for {fsym}")

    return data_df


def save_data_to_csv(data_df: pd.DataFrame, coin_symbol: str):
    """
    Saves the given DataFrame to a CSV file with the specified naming convention.

    Parameters:
    - data_df (pd.DataFrame): The DataFrame containing cryptocurrency data.
    - coin_symbol (str): The symbol of the cryptocurrency (e.g., 'BTC').
    """
    filename = f"{coin_symbol}_hourly_data.csv"
    data_df.to_csv(filename, index=False)
    logging.info(f"Data for {coin_symbol} saved to {filename}")


# Example of how to call the function
# btc_data = fetch_hourly_data('BTC', 'USD', '2020-04-10', '2024-01-03')
# save_data_to_csv(btc_data, 'BTC')


def main(start_date, end_date):
    # Logging configuration
    logging.basicConfig(filename='crypto_data_fetch.log', level=logging.INFO,
                        format='%(asctime)s:%(levelname)s:%(message)s')

    # Start performance monitoring
    start_time = time.time()
    memory_before = psutil.Process().memory_info().rss / (1024 * 1024)  # Memory usage in MB

    try:
        # Fetch and save data for BTC
        btc_data = fetch_hourly_data('BTC', 'USD', start_date, end_date)
        save_data_to_csv(btc_data, 'BTC')

        # Fetch and save data for ETH
        eth_data = fetch_hourly_data('ETH', 'USD', start_date, end_date)
        save_data_to_csv(eth_data, 'ETH')

        # Fetch and save data for SOL
        sol_data = fetch_hourly_data('SOL', 'USD', start_date, end_date)
        save_data_to_csv(sol_data, 'SOL')

    except Exception as e:
        logging.error(f"Error occurred during data fetching or saving: {e}")
        logging.error(traceback.format_exc())
        return  # Exit the function in case of error

    # End performance monitoring
    end_time = time.time()
    memory_after = psutil.Process().memory_info().rss / (1024 * 1024)  # Memory usage in MB

    # Logging performance metrics
    logging.info(f"Execution Time: {end_time - start_time} seconds")
    logging.info(f"Memory Usage: {memory_after - memory_before} MB")


if __name__ == "__main__":
    # Choose dates based on the mode
    start_date = TEST_START_DATE if IS_TEST_MODE else FULL_START_DATE
    end_date = TEST_END_DATE if IS_TEST_MODE else FULL_END_DATE

    # Running the main function
    main(start_date, end_date)
