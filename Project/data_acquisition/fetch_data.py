import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import yfinance as yf
import pandas as pd
from app.db.duckdb_redis import DBManager
from datetime import datetime, timedelta
from app.core.base import logger

def fetch_data():
    tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA"]  # Add more
    end_date = datetime.now()
    start_date = end_date - timedelta(days=60)

    db = DBManager().conn

    for ticker in tickers:
        try:
            df = yf.download(ticker, start=start_date, end=end_date)
            df['ticker'] = ticker
            df['market_cap'] = df['Close'] * 1e9
            df['date'] = df.index.date
            df = df[['ticker', 'date', 'market_cap', 'Close']]
            db.execute("INSERT INTO daily_data SELECT * FROM df")
            logger.info(f"Ingested {ticker}")
        except Exception as e:
            logger.error(f"Error with {ticker}: {e}")
