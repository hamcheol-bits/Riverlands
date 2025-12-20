from pykrx import stock
from datetime import datetime

today = datetime.now().strftime("%Y%m%d")
tickers = stock.get_market_ticker_list(today, "KOSPI")
print(f"today:{today} tickers: {tickers}")

tickers = stock.get_market_ticker_list("20251219", "KOSPI")
print(f"20251219 tickers: {tickers}")

df1 = stock.get_market_fundamental("20241211", "20241219", "005930")
print(f"{df1.head()}")