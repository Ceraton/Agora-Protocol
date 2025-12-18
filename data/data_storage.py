import pandas as pd
import os
from typing import Optional
import asyncio
from utils.logger import setup_logger

from config import DATA_DIR

class DataStorage:
    def __init__(self):
        self.logger = setup_logger("DataStorage")
        self.storage_dir = os.path.join(os.getcwd(), DATA_DIR)
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)

    async def save_ohlcv(self, symbol, ohlcv, filename=None):
        """
        Saves OHLCV data to CSV.
        """
        try:
            if not filename:
                safe_symbol = symbol.replace('/', '_')
                filename = f"{safe_symbol}_data.csv"
                
            filepath = os.path.join(self.storage_dir, filename)
            
            # Convert list of lists to DataFrame
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            df.to_csv(filepath, index=False)
            self.logger.info(f"Data saved to {filepath}")
            return filepath
        except Exception as e:
            self.logger.error(f"Failed to save data: {e}")
            return None

    async def update_yearly_data(self, client, symbol):
        """
        Fetches 1 year of daily data (1d) and saves to specific yearly CSV.
        """
        try:
            # 365 days of 1d candles
            self.logger.info(f"Fetching yearly data for {symbol}...")
            ohlcv = await client.fetch_ohlcv(symbol, timeframe='1d', limit=365)
            if ohlcv:
                safe_symbol = symbol.replace('/', '_')
                filename = f"{safe_symbol}_yearly.csv"
                await self.save_ohlcv(symbol, ohlcv, filename=filename)
        except Exception as e:
             self.logger.error(f"Failed to update yearly data for {symbol}: {e}")

    async def update_intraday_data(self, client, symbol):
        """
        Fetches 24 hours of 1-minute data for forensic backtesting/replay.
        """
        try:
            self.logger.info(f"Fetching intraday data for {symbol}...")
            # 1440 minutes = 24 hours
            ohlcv = await client.fetch_ohlcv(symbol, timeframe='1m', limit=1440)
            if ohlcv:
                safe_symbol = symbol.replace('/', '_')
                filename = f"{safe_symbol}_intraday.csv"
                await self.save_ohlcv(symbol, ohlcv, filename=filename)
        except Exception as e:
             self.logger.error(f"Failed to update intraday data for {symbol}: {e}")

    def load_historical_data(self, filename: str) -> pd.DataFrame:
        """
        Loads historical data from CSV.
        """
        filepath = os.path.join(self.storage_dir, filename)
        if not os.path.exists(filepath):
            self.logger.error(f"File not found: {filepath}")
            return pd.DataFrame()
        
        try:
            df = pd.read_csv(filepath)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        except Exception as e:
             self.logger.error(f"Failed to load data: {e}")
             return pd.DataFrame()

# Wrapper functions for backward compatibility (used by train.py, backtest_engine.py)
_storage_instance = DataStorage()

async def fetch_and_save_historical_data(exchange_client, symbol: str, timeframe: str = '1h', limit: int = 1000, filename: Optional[str] = None):
    """
    Standalone wrapper for compatibility.
    """
    # Note: This original function logic was slightly different (did fetch + save).
    # We must reimplement it using the class methods or just logic here.
    # Reimplementing logic to match exactly what train.py expects.
    _storage_instance.logger.info(f"Fetching historical data for {symbol} ({timeframe})...")
    ohlcv = await exchange_client.fetch_ohlcv(symbol, timeframe, limit=limit)
    
    if not ohlcv:
        _storage_instance.logger.warning("No data returned.")
        return None

    return await _storage_instance.save_ohlcv(symbol, ohlcv, filename=filename)

def load_historical_data(filename: str) -> pd.DataFrame:
    """Wrapper for load_historical_data."""
    return _storage_instance.load_historical_data(filename)
