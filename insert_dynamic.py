import pandas as pd
import yfinance as yf
import snowflake.connector
import snowflake.connector.errors
from datetime import datetime
import time

# 🔹 Step 1: Get all S&P 500 tickers from Wikipedia
sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
sp500_table = pd.read_html(sp500_url)[0]
all_tickers = sp500_table['Symbol'].tolist()

# 🔹 Step 1.5: Fix Yahoo Finance incompatibility (dot → dash)
all_tickers = [ticker.replace('.', '-') for ticker in all_tickers]


# 🔹 Step 2: Calculate top gainers and losers
def get_gainers_losers(tickers, top_n=5):
    changes = []

    for symbol in tickers:
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="2d")  # yesterday & today
            if len(data) >= 2:
                yesterday = data.iloc[-2]['Close']
                today = data.iloc[-1]['Close']
                change_percent = ((today - yesterday) / yesterday) * 100
                changes.append((symbol, round(change_percent, 2)))
        except Exception:
            continue  # skip if any error

    changes.sort(key=lambda x: x[1], reverse=True)
    top_gainers = changes[:top_n]
    top_losers = changes[-top_n:]
    return top_gainers, top_losers

# 🔹 Step 3: Get top 5 gainers + losers
gainers, losers = get_gainers_losers(all_tickers, top_n=5)
tickers = [x[0] for x in gainers + losers]

print("📈 Top Gainers:", gainers)
print("📉 Top Losers:", losers)
print("📊 Selected Tickers:", tickers)

# 🔹 Step 4: Connect to Snowflake
conn = snowflake.connector.connect(
   user='RITU2001',
    password='Zjt4Ye9VfBZmESK',
    account='GKEUXIR-XM23960',  # e.g., abc-xy12345
    warehouse='COMPUTE_WH',
    database='StockMarketDB',
    schema='PUBLIC'
)
cursor = conn.cursor()

# 🔹 Step 5: Insert data for selected tickers
for symbol in tickers:
    try:
        stock = yf.Ticker(symbol)
        data = stock.history(period="1d", interval="1m")

        for index, row in data.iterrows():
            timestamp = index.to_pydatetime()
            price = float(row['Close'])
            volume = int(row['Volume'])

            insert_query = """
                INSERT INTO StockData (ticker, date, close_price, volume)
                VALUES (%s, %s, %s, %s)
            """
            try:
                cursor.execute(insert_query, (symbol, timestamp, price, volume))
            except snowflake.connector.errors.IntegrityError:
                pass  # skip duplicates

        print(f"✅ Inserted data for {symbol}")

    except Exception as e:
        print(f"❌ Failed for {symbol}: {e}")

# 🔹 Final steps
conn.commit()
cursor.close()
conn.close()
print("✅ All selected stock data inserted into Snowflake.")
