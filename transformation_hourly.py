import pandas as pd
import os
import logging

# Constants for column names
TIME_COLUMN = 'time'
HIGH_COLUMN = 'high'
LOW_COLUMN = 'low'
OPEN_COLUMN = 'open'
CLOSE_COLUMN = 'close'
VOL_NATIVE_COLUMN = 'VOL_NATIVE'  # Updated from 'volumefrom'
VOL_USD_COLUMN = 'VOL_USD'  # Updated from 'volumeto'

# Date format constants
DATE_FORMAT = '%Y-%m-%d'
HOUR_FORMAT = '%H'

# File pathS for the input CSV
INPUT_BTC_CSV_PATH = 'BTC_hourly_data.csv'
INPUT_ETH_CSV_PATH = 'ETH_hourly_data.csv'
INPUT_SOL_CSV_PATH = 'SOL_hourly_data.csv'

# Constants for logging
LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_FILE = 'hourly_crypto_data_transformation.log'


# Function to set up logging
def setup_logging() -> None:
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=LOG_LEVEL,
                            format=LOG_FORMAT,
                            handlers=[
                                logging.FileHandler(LOG_FILE),
                                logging.StreamHandler()
                            ])


def transform_hourly_data(csv_path: str, coin_symbol: str) -> None:
    # Read CSV file
    data = pd.read_csv(csv_path)

    # Transform 'time' column to 'date' and 'hour'
    data['date'] = pd.to_datetime(data[TIME_COLUMN]).dt.strftime(DATE_FORMAT)
    data['hour'] = pd.to_datetime(data[TIME_COLUMN]).dt.strftime(HOUR_FORMAT)

    # Drop the original 'time' column
    data.drop(columns=[TIME_COLUMN], inplace=True)

    # Generate record_id
    data['record_id'] = [f"{coin_symbol}_{i:05d}" for i in range(1, len(data) + 1)]

    # Rename columns for hourly trade volume
    data.rename(columns={
        VOL_NATIVE_COLUMN: 'hr_trade_vol_native',
        VOL_USD_COLUMN: 'hr_trade_vol_USD'
    }, inplace=True)

    # Reorder columns
    column_order = [
        'record_id', 'coin_symbol', 'date', 'hour',
        OPEN_COLUMN, LOW_COLUMN, HIGH_COLUMN, CLOSE_COLUMN,
        'hr_trade_vol_native', 'hr_trade_vol_USD'
    ]
    data = data[column_order]

    # Handle saving the transformed data
    output_csv_path = f"transformed_{coin_symbol}_hourly_data.csv"
    data.to_csv(output_csv_path, index=False)


# Example usage
transform_hourly_data(INPUT_BTC_CSV_PATH, 'BTC')
transform_hourly_data(INPUT_ETH_CSV_PATH, 'ETH')
transform_hourly_data(INPUT_SOL_CSV_PATH, 'SOL')

if __name__ == '__main__':
    setup_logging()
