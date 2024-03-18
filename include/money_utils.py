import requests
import pandas as pd
import ccxt
from datetime import datetime

def check_current_btc():
    end = []
    exchange = ccxt.binance() # Thay thế bằng sàn giao dịch mong muốn

    symbol = 'BTC/USDT' # Cặp giao dịch BTC/USDT

    timeframe = '1m' # Khung thời gian 1 ngày

    ohlcv = exchange.fetch_ohlcv(symbol, timeframe)
    for candle in ohlcv:
        timestamp = timestamp
        open_price = candle[1]
        high_price = candle[2]
        low_price = candle[3]
        close_price = candle[4]
        volume = candle[5]
    end.append({
        "timestamp" : datetime.fromtimestamp(candle[0]/1000.0).strftime('%Y-%m-%d %H:%M:%S'),
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": volume,
    })
    return [end]