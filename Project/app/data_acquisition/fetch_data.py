import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))



import yfinance as yf
import pandas as pd
from app.db.duckdb_redis import DBManager
from datetime import datetime, timedelta
from app.core.base import logger
import yfinance as yf
import pandas as pd
from tqdm import tqdm

# Step 1: Get S&P 500 tickers from Wikipedia
def get_sp500_tickers():
    table = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
    tickers = table[0]['Symbol'].tolist()
    # Handle BRK.B and BF.B which need special formatting in yfinance
    tickers = [ticker.replace('.', '-') for ticker in tickers]
    return tickers

# Step 2: Fetch market cap for each ticker
def get_market_caps(tickers):
    market_caps = {}
    for ticker in tqdm(tickers):
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            market_cap = info.get("marketCap", 0)
            market_caps[ticker] = market_cap
        except Exception as e:
            print(f"Error for {ticker}: {e}")
    return market_caps

# Step 3: Sort and pick top 100
def get_top100_by_market_cap():
    tickers = get_sp500_tickers()
    market_caps = get_market_caps(tickers)
    sorted_tickers = sorted(market_caps.items(), key=lambda x: x[1], reverse=True)
    top100 = [ticker for ticker, cap in sorted_tickers[:100]]
    return top100

top_100 = get_top100_by_market_cap()



def fetch_data():
    tickers = top_100  # Add more
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
