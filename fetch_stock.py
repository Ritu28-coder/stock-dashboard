import yfinance as yf

# Fetch stock data for Apple (AAPL)
ticker = yf.Ticker("AAPL")

# Get today's 1-minute interval data
data = ticker.history(period="1d", interval="1m")

# Print the latest stock price
print("Latest Apple Stock Data:")
print(data.tail(1))
