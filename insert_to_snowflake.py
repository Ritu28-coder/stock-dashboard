import yfinance as yf
import snowflake.connector
from datetime import datetime

# STEP 1: Fetch live stock data
ticker = yf.Ticker("AAPL")  # You can change this to any other stock
data = ticker.history(period="1d", interval="1m")
latest = data.tail(1).iloc[0]  # Get the most recent row

# Extract values
symbol = "AAPL"
price = float(latest['Close'])      # ✅ This fixes the error
volume = int(latest['Volume'])      # Still good
timestamp = latest.name.to_pydatetime()


# STEP 2: Connect to Snowflake
conn = snowflake.connector.connect(
    user='RITU2001',
    password='Zjt4Ye9VfBZmESK',
    account='GKEUXIR-XM23960',  # Something like: abc-xy12345
    warehouse='COMPUTE_WH',
    database='StockMarketDB',
    schema='PUBLIC'
)

cursor = conn.cursor()

# STEP 3: Insert data into Snowflake
insert_query = """
    INSERT INTO StockData (ticker, date, close_price, volume)
    VALUES (%s, %s, %s, %s)
"""
cursor.execute(insert_query, (symbol, timestamp, price, volume))

conn.commit()
print("✅ Stock data inserted successfully into Snowflake!")

# STEP 4: Clean up
cursor.close()
conn.close()
