import statsmodels.tsa.stattools as ts
from datetime import datetime
import pandas_datareader.data as web

# Download the Amazon OHLCV data from 1/1/2000 to 1/1/2015
amzn = web.DataReader("AMZN", "yahoo", datetime(2000,1,1), datetime(2015,1,1))

# Output the results of the Augmented Dickey-Fuller test for Amazon
# with a lag order value of 1
result = ts.adfuller(amzn['Adj Close'], 1)
print(result)
