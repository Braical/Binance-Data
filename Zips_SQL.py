# Import required libraries
import requests
import io
import zipfile
import pandas as pd
from sqlalchemy import create_engine

# Create a SQLite database engine
engine = create_engine('sqlite:///base_datos.db')

# Read the list of top 100 tickers by trading volume from a CSV file
df = pd.read_csv('C:/Users/Brian/OneDrive/Documentos/Python/Binance tokens/tickers.csv')
tickers = df['Tickers'].tolist()

# Set the start date for downloading data
start_date = '2017-08'

# Define the base URL for downloading data files
url_base = 'https://data.binance.vision/data/spot/monthly/klines/{ticker}/1m/{ticker}-1m-{date}.zip'

# Iterate through each ticker
for ticker in tickers:
    current_date = start_date
    data = None
    print(ticker)
    # Continue until the current date is greater than the end date (2023-02)
    while current_date <= '2023-02':
        try:
            # Download the zip file containing the data for the current ticker and date
            url = url_base.format(ticker=ticker, date=current_date)
            response = requests.get(url)
            z = zipfile.ZipFile(io.BytesIO(response.content))
            file = z.namelist()[0]
            print(current_date)
            # Read the CSV file from the zip archive and append it to the previous data
            df = pd.read_csv(z.open(file))
            if data is None:
                data = df
            else:
                data = pd.concat([data, df], ignore_index=True)

            # Move to the next date
            current_date = (pd.to_datetime(current_date) + pd.DateOffset(months=1)).strftime('%Y-%m')
            print("Advancing to the next date: ", current_date)

        except:
            # In case of error, continue with the next date
            current_date = (pd.to_datetime(current_date) + pd.DateOffset(months=1)).strftime('%Y-%m')
            print("Error, advancing to the next date: ", current_date)

    # Save the data to the SQLite database
    if data is not None:
        data.to_sql(f'{ticker}', engine, if_exists='replace')
