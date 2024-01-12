"""
Transformation Script for Cryptocurrency Data

This script performs cleaning and transformation operations on cryptocurrency data files.
It includes functionality to clean Solana data and transform data for Bitcoin, Ethereum, and Solana.

Dependencies:
- pandas: For data manipulation
- os: For file path operations
- shutil: For file copying
- logging: For logging operations
- typing: For type annotations

Usage:
Run this script directly to process predefined cryptocurrency data files ('btc_data.csv', 'eth_data.csv', 'sol_data.csv').
It first cleans the Solana data and then applies transformations to all specified files.

Author: Andre La Flamme
Date: January 2, 2024

Imported Modules:
from typing import NoReturn
"""

# The rest of your script follows from here...


import pandas as pd
import os
from shutil import copyfile
import logging
from typing import NoReturn
import datetime

# Constants
START_DATE = '2020-04-20'
END_DATE = '2024-01-03'
HIGH_COLUMN = 'high'
LOW_COLUMN = 'low'
OPEN_COLUMN = 'open'
CLOSE_COLUMN = 'close'
VOLUME_FROM_COLUMN = 'volumefrom'
VOLUME_TO_COLUMN = 'volumeto'
CONVERSION_TYPE_COLUMN = 'conversionType'
CONVERSION_SYMBOL_COLUMN = 'conversionSymbol'
BACKUP_DATE_FORMAT = "%Y%m%d_%H%M%S"
DATE_FORMAT = '%Y-%m-%d'

# Constants for logging
DEBUG_MODE = os.getenv('DEBUG_MODE', 'False').lower() == 'true'
LOG_LEVEL = logging.DEBUG if DEBUG_MODE else logging.INFO
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_FILE = 'transformation_daily.log'

logging.basicConfig(filename=LOG_FILE, level=LOG_LEVEL, format=LOG_FORMAT)


def clean_daily_sol_data(csv_path: str = 'sol_data.csv') -> NoReturn:
    """
    Cleans the Solana data CSV file by removing rows where 'high', 'low', 'open', and 'close' are all zero.
    A backup of the original file is created before performing the operation,
    ensuring that an existing backup is not overwritten.

    Parameters:
    csv_path (str): Path to the Solana data CSV file. Default is 'sol_data.csv'.
    """

    try:
        # Check if the file exists
        if not os.path.exists(csv_path):
            logging.error(f"The file {csv_path} does not exist.")
            raise FileNotFoundError(f"The file {csv_path} does not exist.")

        logging.info(f"Starting to clean data in {csv_path}")

        # Generate a timestamp for the backup file
        timestamp = datetime.datetime.now().strftime(BACKUP_DATE_FORMAT)
        backup_path = csv_path.replace('.csv', f'_backup_{timestamp}.csv')

        # Create a backup of the original file
        if not os.path.exists(backup_path):
            copyfile(csv_path, backup_path)
            logging.info(f"Backup created at {backup_path}")
        else:
            logging.info(f"Backup already exists at {backup_path}, not overwriting.")

        # Read the CSV file
        sol_data = pd.read_csv(csv_path)

        # Validate required columns
        required_columns = [HIGH_COLUMN, LOW_COLUMN, OPEN_COLUMN, CLOSE_COLUMN]
        if not all(column in sol_data.columns for column in required_columns):
            missing_columns = ', '.join([col for col in required_columns if col not in sol_data.columns])
            logging.error(f"CSV file is missing the following required columns: {missing_columns}")
            raise ValueError(f"CSV file is missing the following required columns: {missing_columns}")

        # Drop rows where 'high', 'low', 'open', and 'close' are all zero
        sol_data = sol_data[
            (sol_data[OPEN_COLUMN] != 0) & (sol_data[HIGH_COLUMN] != 0) &
            (sol_data[LOW_COLUMN] != 0) & (sol_data[CLOSE_COLUMN] != 0)]

        # Rewrite the cleaned data to csv
        sol_data.to_csv(csv_path, index=False)
        logging.info(f"Cleaned data has been written to {csv_path}")

    except FileNotFoundError as e:
        logging.error(f"File not found error in clean_sol_data: {e}")
        raise
    except ValueError as e:
        logging.error(f"Data validation error in clean_sol_data: {e}")
        raise


def transform_daily_crypto_data(csv_path: str, crypto_prefix: str) -> NoReturn:
    """
    Transforms and filters cryptocurrency data in a CSV file.

    Parameters:
    csv_path (str): Path to the cryptocurrency data CSV file.
    crypto_prefix (str): Prefix for the cryptocurrency (e.g., 'BTC', 'ETH', 'SOL').

    The function performs the following:
    - Generates a record_id column with the crypto prefix followed by a sequential integer.
    - Adds a coin_symbol column and populates it with the cryptocurrency's symbol.
    - Converts the time column from Unix timestamp to YYYY-MM-DD format and renames it to 'date'.
    - Renames 'volumefrom' to 'trade_vol_native'.
    - Renames 'volumeto' to 'trade_vol_USD' and refactors it to show the full numeric value.
    - Drops the 'conversionType' and 'conversionSymbol' columns.
    - Reorders the columns to the specified format.
    - Filters out records outside the specified date range defined by START_DATE and END_DATE.
    - Saves the transformed and filtered data to a new file with a prefix 'transformed_'.
    """

    try:
        if not os.path.exists(csv_path):
            logging.error(f"The file {csv_path} does not exist.")
            raise FileNotFoundError(f"The file {csv_path} does not exist.")

        logging.info(f"Starting transformation of data in {csv_path}")

        # Read the CSV file
        data = pd.read_csv(csv_path)

        # Validate required columns
        required_columns = ['time', VOLUME_FROM_COLUMN, VOLUME_TO_COLUMN]
        if not all(column in data.columns for column in required_columns):
            missing_columns = ', '.join([col for col in required_columns if col not in data.columns])
            logging.error(f"CSV file is missing the following required columns: {missing_columns}")
            raise ValueError(f"CSV file is missing the following required columns: {missing_columns}")

        # Generate record_id
        data['record_id'] = [f"{crypto_prefix}{str(i).zfill(2)}" for i in range(1, len(data) + 1)]
        # Add coin_symbol column
        data['coin_symbol'] = crypto_prefix
        logging.info("Record IDs and coin symbols generated.")

        # Convert 'time' column to 'date'
        data['date'] = pd.to_datetime(data['time'], unit='s').dt.strftime(DATE_FORMAT)
        data.drop('time', axis=1, inplace=True)
        logging.info("'time' column converted to 'date'.")

        # Rename columns
        data.rename(columns={VOLUME_FROM_COLUMN: 'trade_vol_native', VOLUME_TO_COLUMN: 'trade_vol_USD'}, inplace=True)
        logging.info("Columns renamed.")

        # Refactor 'trade_vol_USD' to full numeric value
        data['trade_vol_USD'] = data['trade_vol_USD'].apply(lambda x: '{:.2f}'.format(x))
        logging.info("'trade_vol_USD' column refactored to full numeric value.")

        # Drop unnecessary columns
        data.drop([CONVERSION_TYPE_COLUMN, CONVERSION_SYMBOL_COLUMN], axis=1, inplace=True)
        logging.info("'conversionType' and 'conversionSymbol' columns dropped.")

        # Reorder the columns
        desired_order = ['record_id', 'coin_symbol', 'date', OPEN_COLUMN, LOW_COLUMN, HIGH_COLUMN, CLOSE_COLUMN,
                         'trade_vol_native', 'trade_vol_USD']
        data = data[desired_order]
        logging.info("Columns reordered.")

        # Filter data by date range
        start_datetime = pd.to_datetime(START_DATE)
        end_datetime = pd.to_datetime(END_DATE)
        data['date'] = pd.to_datetime(data['date'])
        data = data[(data['date'] >= start_datetime) & (data['date'] <= end_datetime)]
        logging.info(f"Data filtered for dates between {START_DATE} and {END_DATE}.")

        # Create a new filename for the transformed data
        transformed_csv_path = f'transformed_{crypto_prefix}_daily_data.csv'

        # Save the transformed data to the new file
        data.to_csv(transformed_csv_path, index=False)
        logging.info(f"Transformed data saved to {transformed_csv_path}")

    except FileNotFoundError as e:
        logging.error(f"File not found error in transform_crypto_data: {e}")
        raise
    except ValueError as e:
        logging.error(f"Data validation error in transform_crypto_data: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error in transform_crypto_data: {e}")
        raise


if __name__ == '__main__':

    # Specify file paths and their respective prefixes
    file_paths_and_prefixes = {
        'btc_daily_data.csv': 'BTC',
        'eth_daily_data.csv': 'ETH',
        'sol_daily_data.csv': 'SOL'
    }

    # Process each file
    for file_path, prefix in file_paths_and_prefixes.items():
        try:
            # Clean data if it's the Solana dataset
            if file_path == 'sol_data.csv':
                clean_daily_sol_data(file_path)
                logging.info(f"Data cleaned for {file_path}")

            # Transform data for all datasets
            transform_daily_crypto_data(file_path, prefix)
            logging.info(f"Data transformed for {file_path}")

        except FileNotFoundError as e:
            logging.error(f"File not found error in processing {file_path}: {e}")
        except ValueError as e:
            logging.error(f"Data validation error in processing {file_path}: {e}")
        except Exception as e:  # Catching any other unexpected exceptions
            logging.error(f"Unexpected error in processing {file_path}: {e}")
