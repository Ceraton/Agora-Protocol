import csv
import os
import json
from datetime import datetime
from utils.logger import setup_logger

class TradeRecorder:
    def __init__(self, filename='trade_history.csv', portfolio_filename='portfolio_history.csv'):
        self.filename = os.path.join(os.getcwd(), filename)
        # Ensure data_storage dir exists for portfolio file
        self.data_dir = os.path.join(os.getcwd(), 'data_storage')
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            
        self.portfolio_filename = os.path.join(self.data_dir, portfolio_filename)
        self.logger = setup_logger("TradeRecorder")
        self._ensure_file_exists()

    def _ensure_file_exists(self):
        """Creates the file with headers if it doesn't exist."""
        if not os.path.exists(self.filename):
            with open(self.filename, mode='w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Timestamp', 'Date', 'Symbol', 'Side', 'Price', 'Amount', 'Value', 'Strategy', 'Exchange'])
            self.logger.info(f"Created new trade log: {self.filename}")

    def log_trade(self, symbol, side, price, amount, strategy_name, exchange, timestamp=None):
        """Appends a trade record to the CSV."""
        if timestamp is None:
            timestamp = datetime.now()
            
        date_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        value = price * amount
        
        try:
            with open(self.filename, mode='a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp.timestamp(),
                    date_str,
                    symbol,
                    side.upper(),
                    f"{price:.8f}",
                    f"{amount:.8f}",
                    f"{value:.2f}",
                    strategy_name,
                    exchange
                ])
            self.logger.info(f"Logged trade for {symbol} to {self.filename}")
        except Exception as e:
            self.logger.error(f"Failed to log trade: {e}")

    def log_portfolio_snapshot(self, total_value_usd, asset_details):
        """
        Logs the total portfolio value for graphing.
        """
        file_exists = os.path.isfile(self.portfolio_filename)
        
        try:
            with open(self.portfolio_filename, 'a', newline='') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(['Timestamp', 'TotalValueUSD', 'Details']) # Header
                
                writer.writerow([
                    datetime.now().isoformat(),
                    total_value_usd,
                    json.dumps(asset_details) # Store breakdown as JSON string
                ])
                self.logger.info(f"Logged Portfolio Snapshot: ${total_value_usd:.2f}")
        except Exception as e:
            self.logger.error(f"Error logging portfolio snapshot: {e}")
