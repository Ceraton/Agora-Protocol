import ccxt.async_support as ccxt
import asyncio
import sys
from typing import Dict, List, Optional
import os
from dotenv import load_dotenv
from utils.logger import setup_logger

load_dotenv()

import time

class ExchangeClient:
    def __init__(self, exchange_id: str):
        self.logger = setup_logger(f"ExchangeClient_{exchange_id}")
        self.exchange_id = exchange_id
        self.metrics = {
             'latency_history': [], # List of ms
             'errors': 0,
             'requests': 0
        }
        self.api_key = os.getenv(f"{exchange_id.upper()}_API_KEY")
        self.secret = os.getenv(f"{exchange_id.upper()}_SECRET")
        
        if not self.api_key or not self.secret:
            self.logger.warning(f"API keys for {exchange_id} not found in environment variables. Functionality may be limited.")

        # Coinbase Cloud/Advanced Trade specific handling for PEM keys
        if exchange_id == 'coinbase' and self.secret:
            # Check if it looks like a PEM key (contains BEGIN ... KEY)
            if 'BEGIN' in self.secret and 'KEY' in self.secret:
                # Replace literal \n with actual newlines if they were escaped
                if '\\n' in self.secret:
                    self.secret = self.secret.replace('\\n', '\n')
                
                # Check if it's currently a single line but needs newlines (rare, but possible if stripped)
                # But replacing \\n covers the common .env case.
                pass

        exchange_class = getattr(ccxt, exchange_id)
        self.exchange = exchange_class({
            'apiKey': self.api_key,
            'secret': self.secret,
            'enableRateLimit': True,  # ccxt handles rate limits automatically
        })

    async def close(self):
        await self.exchange.close()

    async def fetch_ticker(self, symbol: str) -> Dict:
        """
        Fetches current ticker data for a symbol.
        """
        start = time.time()
        self.metrics['requests'] += 1
        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            lat_ms = (time.time() - start) * 1000
            self.metrics['latency_history'].append(lat_ms)
            # Keep history small
            if len(self.metrics['latency_history']) > 100:
                self.metrics['latency_history'].pop(0)
            return ticker
        except Exception as e:
            self.metrics['errors'] += 1
            self.logger.error(f"Error fetching ticker for {symbol}: {e}")
            return {}

    async def fetch_ohlcv(self, symbol: str, timeframe: str = '1m', limit: int = 100) -> List:
        """
        Fetches OHLCV (candlestick) data.
        """
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
            return ohlcv
        except Exception as e:
            self.logger.error(f"Error fetching OHLCV for {symbol}: {e}")
            return []

    async def get_balance(self) -> Dict:
        """
        Fetches account balance.
        """
        try:
            balance = await self.exchange.fetch_balance()
            return balance
        except Exception as e:
            self.logger.error(f"Error fetching balance: {e}")
            return {}

    async def fetch_order_book(self, symbol: str, limit: int = 50) -> Dict:
        """
        Fetches the L2 order book for a symbol.
        """
        try:
            order_book = await self.exchange.fetch_order_book(symbol, limit=limit)
            return order_book
        except Exception as e:
            self.logger.error(f"Error fetching order book for {symbol}: {e}")
            return {'bids': [], 'asks': []}

    async def get_price_impact(self, symbol: str, amount: float, side: str) -> float:
        """
        Estimates the price impact (slippage) of a trade by walking the book.
        Returns: Decimal fraction (e.g. 0.01 for 1% impact)
        """
        if amount <= 0: return 0.0
        
        order_book = await self.fetch_order_book(symbol, limit=100)
        levels = order_book['asks'] if side == 'buy' else order_book['bids']
        
        if not levels:
            return 1.0 # Infinite impact if no liquidity
            
        total_cost = 0.0
        remaining_amount = amount
        base_price = levels[0][0] # Best bid/ask
        
        for price, volume in levels:
            fill = min(remaining_amount, volume)
            total_cost += fill * price
            remaining_amount -= fill
            if remaining_amount <= 0:
                break
                
        if remaining_amount > 0:
            # We ran out of book depth
            avg_price = total_cost / (amount - remaining_amount)
            # Add a heavy penalty for exceeding known depth
            avg_price *= (1 + (remaining_amount / amount)) 
        else:
            avg_price = total_cost / amount
            
        impact = abs(avg_price - base_price) / base_price
        return impact

# Example usage (for testing)
async def main():
    kraken = ExchangeClient('kraken')
    ticker = await kraken.fetch_ticker('BTC/USD')
    print(f"Kraken BTC/USD: {ticker.get('last')}")
    await kraken.close()

if __name__ == "__main__":
    current_platform = sys.platform
    if current_platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
