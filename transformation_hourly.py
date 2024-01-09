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


def read_csv_file(csv_path: str) -> pd.DataFrame:
    logging.info(f"Reading CSV file: {csv_path}")
    try:
        data = pd.read_csv(csv_path)
        logging.info(f"Successfully read {len(data)} rows from {csv_path}")
        return data
    except FileNotFoundError:
        logging.error(f"File {csv_path} not found.")
    except pd.errors.ParserError:
        logging.error(f"Error parsing {csv_path}.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    return pd.DataFrame()


def validate_data(data: pd.DataFrame, required_columns: list, numeric_columns: list) -> bool:
    logging.info("Starting data validation.")

    # Check for required columns
    missing_columns = set(required_columns) - set(data.columns)
    if missing_columns:
        logging.error(f"Missing required columns: {missing_columns}")
        return False
    else:
        logging.info("All required columns are present.")

    # Check for missing values
    if data.isnull().any().any():
        missing_info = data.isnull().sum()
        logging.warning(f"Missing values found: {missing_info}")
    else:
        logging.info("No missing values found.")

    # Validate numeric columns for appropriate ranges
    if data[numeric_columns].lt(0).any().any():
        logging.error(f"Negative values found in numeric columns: {numeric_columns}")
        return False
    else:
        logging.info("Numeric columns have valid values.")

    logging.info("Data validation completed successfully.")
    return True


def transform_time_column(data: pd.DataFrame, time_column: str) -> pd.DataFrame:
    logging.info("Starting transformation of the time column.")

    try:
        data['date'] = pd.to_datetime(data[time_column]).dt.strftime(DATE_FORMAT)
        data['hour'] = pd.to_datetime(data[time_column]).dt.strftime(HOUR_FORMAT)
        data = data.drop(columns=[time_column])
        logging.info("Time column transformed into 'date' and 'hour' columns.")
    except Exception as e:
        logging.error(f"Error transforming time column: {e}")

    return data


def generate_record_id(data: pd.DataFrame, coin_symbol: str) -> pd.DataFrame:
    logging.info("Starting generation of record IDs.")

    try:
        data['record_id'] = [f"{coin_symbol}_H{i:05d}" for i in range(1, len(data) + 1)]
        logging.info("Record IDs generated successfully.")
    except Exception as e:
        logging.error(f"Error generating record IDs: {e}")

    return data


def rename_and_reorder_columns(data: pd.DataFrame, coin_symbol: str) -> pd.DataFrame:
    logging.info("Starting renaming and reordering of columns.")

    try:
        # Insert coin_symbol column
        data.insert(1, 'coin_symbol', coin_symbol)

        # Rename columns
        data.rename(columns={
            VOL_NATIVE_COLUMN: 'hr_trade_vol_native',
            VOL_USD_COLUMN: 'hr_trade_vol_USD'
        }, inplace=True)

        # Define the new column order
        column_order = [
            'record_id', 'coin_symbol', 'date', 'hour',
            OPEN_COLUMN, LOW_COLUMN, HIGH_COLUMN, CLOSE_COLUMN,
            'hr_trade_vol_native', 'hr_trade_vol_USD'
        ]

        # Reorder the columns
        data = data[column_order]
        logging.info("Columns renamed and reordered successfully.")
    except Exception as e:
        logging.error(f"Error in renaming and reordering columns: {e}")

    return data


def save_transformed_data(data: pd.DataFrame, output_csv_path: str) -> None:
    logging.info(f"Saving transformed data to {output_csv_path}")

    try:
        data.to_csv(output_csv_path, index=False)
        logging.info(f"Data successfully saved to {output_csv_path}")
    except Exception as e:
        logging.error(f"Error saving data to {output_csv_path}: {e}")


def transform_hourly_data(csv_path: str, coin_symbol: str) -> None:
    logging.info(f"Starting data transformation for {coin_symbol} using file {csv_path}")

    data = read_csv_file(csv_path)
    if data.empty:
        logging.error(f"No data found in {csv_path} or failed to read file.")
        return

    required_columns = [TIME_COLUMN, HIGH_COLUMN, LOW_COLUMN, OPEN_COLUMN, CLOSE_COLUMN, VOL_NATIVE_COLUMN, VOL_USD_COLUMN]
    numeric_columns = [HIGH_COLUMN, LOW_COLUMN, OPEN_COLUMN, CLOSE_COLUMN]

    if not validate_data(data, required_columns, numeric_columns):
        logging.error(f"Data validation failed for {csv_path}.")
        return

    data = transform_time_column(data, TIME_COLUMN)
    data = generate_record_id(data, coin_symbol)
    data = rename_and_reorder_columns(data, coin_symbol)

    output_csv_path = f"transformed_{coin_symbol}_hourly_data.csv"
    save_transformed_data(data, output_csv_path)

    logging.info(f"Data transformation completed for {coin_symbol} and saved to {output_csv_path}")


if __name__ == '__main__':
    setup_logging()
    transform_hourly_data(INPUT_BTC_CSV_PATH, 'BTC')
    transform_hourly_data(INPUT_ETH_CSV_PATH, 'ETH')
    transform_hourly_data(INPUT_SOL_CSV_PATH, 'SOL')
