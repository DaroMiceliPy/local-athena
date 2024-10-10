from sqlalchemy import text
from sqlalchemy.engine import create_engine
import os
import boto3
import botocore
import pandas as pd
import yfinance as yf
from ny_data import ny_data
from financial_data import financial_data

    
def download_yesterday_data(symbol):
    try:
        data = yf.download(symbol, period="1d")
        data["Symbol"] = symbol
    except KeyError:
        print(f"No existe el ticker {symbol}, se continua con el siguiente..")
        data = pd.DataFrame(columns=["Open", "High", "Low", "Close", "Adj Close", "Volume", "Symbol"])
        return data
         
    return data


symbols = pd.read_html("https://www.cboe.com/us/equities/market_statistics/listed_symbols/")
symbols = symbols[0]["Symbol"].to_list()
        
        
data = [download_yesterday_data(symbol) for symbol in symbols]
data = pd.concat(data)
data = data.rename(columns={"Adj Close": "Adj_Close"})

financial_data_today = financial_data()
financial_data_today.create_table()
financial_data_today.df = data
financial_data_today.save()
financial_data_today.read(limit = 10)
print(financial_data_today.df.head())


mock_hive_engine = create_engine('hive://localhost:10000/default')
mock_presto_engine = create_engine('presto://localhost:8080/hive')

data = pd.read_parquet("sample_data/ny_data.parquet")

ny_today = ny_data()
#Este metodo de create_table es solo para poder crear una tabla
ny_today.create_table()
ny_today.df = data
ny_today.save()
ny_today.read()
print(ny_today.df.head())


query = f"""
SELECT * FROM ny_data WHERE downloaded_at = '2024-10-10' limit 1
"""
ny_today.read(query = query)
print(ny_today.df.head())