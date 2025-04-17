import pandas as pd
import yfinance as yf
import snowflake.connector
from datetime import datetime
import time

# Step 1: Get S&P 500 tickers + sectors from Wikipedia
sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
sp500_table = pd.read_html(sp500_url)[0]
tickers_sectors = sp500_table[['Symbol', 'GICS Sector']].copy()
tickers_sectors['Symbol'] = tickers_sectors['Symbol'].str.replace('.', '-', regex=False)
tickers = tickers_sectors['Symbol'].tolist()

print(f"🚀 Starting to insert data for {len(tickers)} tickers...")

# 🔹 Connect to Snowflake
conn = snowflake.connector.connect(
    user='RITU2001',
    password='Zjt4Ye9VfBZmESK',
    account='GKEUXIR-XM23960',  # e.g., abc-xy12345
    warehouse='COMPUTE_WH',
    database='StockMarketDB',
    schema='PUBLIC'
)
cursor = conn.cursor()

for symbol in tickers:
    print(f"🔄 Fetching: {symbol}")
    try:
        df = yf.download(symbol, period="5d", interval="1d", progress=False)
        df.dropna(inplace=True)

        # 💡 Get sector from tickers_sectors DataFrame
        sector = tickers_sectors[tickers_sectors['Symbol'] == symbol]['GICS Sector'].values[0]

        for index, row in df.iterrows():
            cursor.execute(
                "INSERT INTO StockData (ticker, date, close_price, volume, sector) VALUES (%s, %s, %s, %s, %s)",
                (symbol, index.date(), float(row["Close"]), int(row["Volume"]), sector)
            )

        print(f"✅ Done: {symbol}")
        time.sleep(2)

    except Exception as e:
        print(f"⚠️ Skipped {symbol} due to error: {e}")
        continue

cursor.close()
conn.close()

print("🎉 Finished inserting all 500 tickers!")
