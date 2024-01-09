"""
Cryptocurrency Hourly Data Transformation Script

This script is designed to process hourly trading data for cryptocurrencies such as Bitcoin (BTC),
Ethereum (ETH), and Solana (SOL). It reads hourly data from CSV files, performs various
transformations and validations, and then saves the processed data into new CSV files.

The script includes several key functionalities:
- Reading hourly trading data from specified CSV files.
- Validating the data to ensure it contains all required columns and the data is in the expected format.
- Transforming the 'time' column into separate 'date' and 'hour' columns.
- Generating unique 'record_id' for each data record, incorporating the cryptocurrency symbol.
- Renaming and reordering columns to match a predefined schema.
- Saving the transformed data into new CSV files for further use.

Dependencies:
- pandas: For data manipulation and reading/writing CSV files.
- os: For interacting with the file system.
- logging: For logging information and errors during script execution.

Usage:
The script is executed at the command line and processes files defined in the constants
(INPUT_BTC_CSV_PATH, INPUT_ETH_CSV_PATH, INPUT_SOL_CSV_PATH). The output is saved to new CSV files.

Constants:
- TIME_COLUMN, HIGH_COLUMN, etc.: Define the column names used in the data processing.
- DATE_FORMAT, HOUR_FORMAT: Define the date and time format used in transformations.
- INPUT_BTC_CSV_PATH, etc.: File paths for the input CSV files for each cryptocurrency.
- LOG_LEVEL, LOG_FORMAT, LOG_FILE: Configuration for logging.

The script's functions are modular, each handling a specific part of the data processing pipeline,
making it easy to modify and maintain. Error handling and logging are implemented throughout the script
to ensure robust execution and to provide clear insights into the process flow and any issues encountered.

Author: Andre La Flamme
Date: January 10, 2024
"""


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
    """
        Sets up the logging configuration for the script.

        Configures logging to write messages to both a file and the console.
        The log level, format, and file are defined by the constants LOG_LEVEL, LOG_FORMAT, and LOG_FILE.
        Only configures logging if no handlers have been set up previously.

        Returns:
        None
        """
    if not logging.getLogger().hasHandlers():
        logging.basicConfig(level=LOG_LEVEL,
                            format=LOG_FORMAT,
                            handlers=[
                                logging.FileHandler(LOG_FILE),
                                logging.StreamHandler()
                            ])


def read_csv_file(csv_path: str) -> pd.DataFrame:
    """
       Reads a CSV file into a pandas DataFrame.

       Parameters:
       csv_path (str): The file path to the CSV file to be read.

       Returns:
       pd.DataFrame: The DataFrame containing the data from the CSV file.
       An empty DataFrame is returned if the file cannot be read.

       This function specifically handles the following errors:
       - FileNotFoundError: If the CSV file is not found at the specified path.
       - pd.errors.EmptyDataError: If the CSV file is empty.
       - pd.errors.ParserError: If there is an error parsing the CSV file.
       - IOError: If an Input/Output error occurs while reading the file.

       Appropriate error messages are logged for each exception.
       """
    logging.info(f"Reading CSV file: {csv_path}")
    try:
        data = pd.read_csv(csv_path)
        logging.info(f"Successfully read {len(data)} rows from {csv_path}")
        return data
    except FileNotFoundError:
        logging.error(f"File {csv_path} not found.")
    except pd.errors.EmptyDataError:
        logging.error(f"No data: {csv_path} is empty.")
    except pd.errors.ParserError:
        logging.error(f"Error parsing {csv_path}: File format issue.")
    except IOError:
        logging.error(f"IOError encountered while reading {csv_path}.")
    return pd.DataFrame()


def validate_data(data: pd.DataFrame, required_columns: list, numeric_columns: list) -> bool:
    """
        Validates the data in the DataFrame.

        Checks for the presence of required columns, absence of null values,
        and non-negative values in specified numeric columns.

        Parameters:
        data (pd.DataFrame): The DataFrame to validate.
        required_columns (list): A list of columns that must be present in the data.
        numeric_columns (list): A list of columns that should have non-negative numeric values.

        Returns:
        bool: True if the data is valid, False otherwise.
        """
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
    """
        Transforms the 'time' column of the DataFrame into separate 'date' and 'hour' columns.

        Parameters:
        data (pd.DataFrame): The DataFrame containing the time data.
        time_column (str): The name of the column in the DataFrame that contains time data.

        Returns:
        pd.DataFrame: The modified DataFrame with the 'time' column split into 'date' and 'hour'.

        The function tries to parse the 'time' column as a datetime object and then formats
        it into 'date' and 'hour' based on the DATE_FORMAT and HOUR_FORMAT constants.
        It logs an error and returns the original DataFrame if any exception occurs.
        """
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
    """
    Generates a unique 'record_id' for each row in the DataFrame using vectorized operations.

    Parameters:
    data (pd.DataFrame): The DataFrame for which to generate record IDs.
    coin_symbol (str): The cryptocurrency symbol (e.g., 'BTC', 'ETH') used as a prefix in the record ID.

    Returns:
    pd.DataFrame: The DataFrame with a new 'record_id' column added.

    The 'record_id' is generated by concatenating the cryptocurrency symbol with a unique
    sequential number for each row, formatted with leading zeros. This method uses vectorized
    operations for improved performance, especially beneficial for large datasets.
    """
    logging.info("Starting generation of record IDs using vectorized operations.")

    try:
        record_count = len(data)
        data['record_id'] = coin_symbol + '_H' + pd.Series(range(1, record_count + 1)).astype(str).str.zfill(5)
        logging.info("Record IDs generated successfully.")
    except Exception as e:
        logging.error(f"Error generating record IDs: {e}")

    return data


def rename_and_reorder_columns(data: pd.DataFrame, coin_symbol: str) -> pd.DataFrame:
    """
        Renames and reorders columns in the provided DataFrame according to specified conventions.

        Parameters:
        data (pd.DataFrame): The DataFrame to be modified.
        coin_symbol (str): The cryptocurrency symbol (e.g., 'BTC', 'ETH') used in the 'coin_symbol' column.

        Returns:
        pd.DataFrame: The DataFrame with renamed and reordered columns.

        This function performs the following operations:
        - Inserts the 'coin_symbol' column as the second column.
        - Renames the 'VOL_NATIVE' and 'VOL_USD' columns to 'hr_trade_vol_native' and 'hr_trade_vol_USD'.
        - Reorders the columns to a specified format for consistency.
        An error is logged if any exception occurs during the process.
        """
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
    """
       Saves the transformed DataFrame to a CSV file.

       Parameters:
       data (pd.DataFrame): The DataFrame to be saved.
       output_csv_path (str): The file path where the CSV file will be saved.

       Returns:
       None

       This function writes the DataFrame to a CSV file at the specified path.
       It logs informational messages about the process and any errors encountered during file writing.
       """
    logging.info(f"Saving transformed data to {output_csv_path}")

    try:
        data.to_csv(output_csv_path, index=False)
        logging.info(f"Data successfully saved to {output_csv_path}")
    except Exception as e:
        logging.error(f"Error saving data to {output_csv_path}: {e}")


def transform_hourly_data(csv_path: str, coin_symbol: str) -> None:
    """
        Main function to transform hourly data for a specific cryptocurrency.

        Reads the hourly data CSV, validates and transforms the data, and then saves the transformed data.

        Parameters:
        csv_path (str): Path to the input CSV file containing hourly data.
        coin_symbol (str): The cryptocurrency symbol (e.g., 'BTC', 'ETH') representing the type of data being processed.

        Returns:
        None

        The function performs the following steps:
        - Reads the CSV file.
        - Validates the data for required columns and correct formats.
        - Transforms the 'time' column into 'date' and 'hour'.
        - Generates unique 'record_id' for each row.
        - Renames and reorders columns as per the specified schema.
        - Saves the transformed data to a new CSV file.
        It logs information about each step and any errors encountered.
        """
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
