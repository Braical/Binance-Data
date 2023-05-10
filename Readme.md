# Binance Data Downloader

Binance Data Downloader is a Python script designed to download historical price data of various cryptocurrency tickers from the Binance platform and store it in a SQLite database. The program takes into account the top 100 tickers by trading volume and fetches the data from the start of 2018 till the current date (2023-05).

## Features

- Downloads historical price data (monthly klines) for the top 100 tickers by trading volume.
- Saves the data to a SQLite database.
- Uses multiprocessing to download data for multiple tickers concurrently.
- Skips tickers for which data has already been downloaded.
- Handles exceptions gracefully and retries failed downloads up to a specified number of attempts.

## Dependencies

The script requires the following Python packages:

- `pandas`
- `requests`
- `sqlalchemy`
- `io`
- `zipfile`
- `multiprocessing`
- `random`

## How to use

1. Ensure you have the required Python packages installed.
2. Customize the variables as needed:
   - `start_date`: The date from which to start downloading data (default: '2018-01').
   - `url_base`: The base URL for downloading the data.
   - `tickers`: The list of top 100 tickers to download data for.
3. Run the script using a Python interpreter. The script will download the data and save it to a SQLite database called `base_datos_BTC.db`.

## How it works

1. The script imports the required packages and initializes the SQLite database.
2. It reads a CSV file containing the top 100 tickers by trading volume and converts it into a list.
3. It then iterates over the tickers and downloads the data for each ticker using the `download_data` function.
4. The `download_data` function checks if the data for the ticker already exists in the database. If it does, it skips the download. Otherwise, it downloads the data and saves it to the database.
5. The script uses multiprocessing to download data for multiple tickers concurrently, making the process faster.

## Notes

- The script retries failed downloads up to 5 times, with a random delay between attempts.
- Ensure that the CSV file containing the tickers and the SQLite database are in the same directory as the script.
- The script may not work for future dates beyond the knowledge cutoff (2021-09) due to potential changes in the Binance API or data format.