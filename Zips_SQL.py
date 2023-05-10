# Import required libraries
import random
import requests
import io
import zipfile
import pandas as pd
import sqlalchemy.exc
from sqlalchemy import create_engine, MetaData, inspect
import multiprocessing
from time import sleep

# Create a database engine
engine = create_engine('sqlite:///base_datos_BTC_z.db')

# Read the CSV file containing the top 100 tickers with the highest volume
df = pd.read_csv('C:/Users/Brian/OneDrive/Documentos/Python/Binance tokens/ticker_BTC.csv')
tickers = df['Tickers'].tolist()

# Set the starting date
start_date = '2023-01'

# Define the base URL for downloading data
url_base = 'https://data.binance.vision/data/spot/monthly/klines/{ticker}/1m/{ticker}-1m-{date}.zip'

# Define the column names
columns = [
    "Open time",
    "Open price",
    "High price",
    "Low price",
    "Close price",
    "Volume",
    "Close time",
    "Quote asset volume",
    "Number of trades",
    "Taker buy base asset volume",
    "Taker buy quote asset volume",
    "Unused field"
]

# Define the float columns
float_columns = [
    "Open price",
    "High price",
    "Low price",
    "Close price",
    "Volume",
    "Quote asset volume",
    "Taker buy base asset volume",
    "Taker buy quote asset volume",
]

# Define a function to save data to the database with retries in case of failure
def save_to_database(data, ticker, engine, retries=5, delay_min=0.5, delay_max=2):
    for attempt in range(retries):
        try:
            data.to_sql(ticker, con=engine, if_exists='replace')
            break
        except sqlalchemy.exc.OperationalError as e:
            if attempt < retries - 1:
                sleep_time = random.uniform(delay_min, delay_max)
                print(f"Error: {e}. Retrying in {sleep_time} seconds...")
                sleep(sleep_time)
            else:
                raise

# Define a function to download data for a specific ticker
def download_data(ticker):
    # Check if the ticker data already exists in the database
    inspector = inspect(engine)
    if inspector.has_table(ticker):
        print(f"{ticker} data already exists in the database. Skipping...")
        return
    current_date = start_date
    data = None
    print(ticker)
    # Continue until the current date is greater than the present date
    while current_date <= '2023-05':
        try:
            # Download the zip file
            url = url_base.format(ticker=ticker, date=current_date)
            print(url)
            response = requests.get(url)
            z = zipfile.ZipFile(io.BytesIO(response.content))
            file = z.namelist()[0]
            print(current_date)
            # Read the CSV file and append it to the previous data
            df = pd.read_csv(z.open(file))
            df.columns = columns
            df["Open time"] = pd.to_datetime(df["Open time"], unit='ms')
            df["Close time"] = pd.to_datetime(df["Close time"], unit='ms')

            # Convert float64 columns to float32
            for column in float_columns:
                df[column] = df[column].astype('float32')

            # Drop the "Unused field" column
            df.drop(columns=["Unused field"], inplace=True)
            if data is None:
                data = df
            else:
                data = pd.concat([data, df], ignore_index=True)

            # Move to the next date
            current_date = (pd.to_datetime(current_date) + pd.DateOffset(months=1)).strftime('%Y-%m')
            print("Moving to the next date: ", current_date)
        except Exception as e:
            current_date = (pd.to_datetime(current_date) + pd.DateOffset(months=1)).strftime('%Y-%m')
            print("Error, moving to the next date: ", current_date)
            continue

    # Save the DataFrame to the database
    if data is not None:
        save_to_database(data, ticker, engine)

#Download data for each ticker using multiprocessing
    
if __name__ == '__main__':
    with multiprocessing.Pool(processes=8) as pool:
        multiprocessing.freeze_support()
        pool.map(download_data, tickers)
        sleep(1)
