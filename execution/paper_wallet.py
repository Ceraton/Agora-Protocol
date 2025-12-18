import json
import os
from typing import Dict, Optional
from utils.logger import setup_logger

class PaperWallet:
    def __init__(self, initial_capital: float = 10000.0, initial_holdings: Dict[str, float] = None, filename: str = 'paper_wallet.json'):
        self.filename = os.path.join(os.getcwd(), filename)
        self.logger = setup_logger("PaperWallet")
        self.initial_capital = initial_capital
        self.initial_holdings = initial_holdings or {}
        self.balances: Dict[str, float] = {}
        
        self.load()

    def load(self):
        """Loads balances from JSON file or initializes with capital."""
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    self.balances = json.load(f)
                self.logger.info(f"Loaded paper wallet from {self.filename}")
            except Exception as e:
                self.logger.error(f"Failed to load paper wallet: {e}")
                self._init_defaults()
        else:
            self._init_defaults()

    def _init_defaults(self):
        """Initializes default balances."""
        self.balances = {'USD': self.initial_capital}
        # Merge initial crypto holdings
        for asset, amount in self.initial_holdings.items():
            self.balances[asset] = amount
            
        self.save()
        self.logger.info(f"Initialized new paper wallet with ${self.initial_capital:.2f} and {self.initial_holdings}")

    def save(self):
        """Saves current balances to JSON."""
        try:
            with open(self.filename, 'w') as f:
                json.dump(self.balances, f, indent=4)
        except Exception as e:
            self.logger.error(f"Failed to save paper wallet: {e}")

    def get_balance(self, currency: str) -> float:
        """Returns the balance of a specific currency."""
        return self.balances.get(currency, 0.0)

    def deposit(self, currency: str, amount: float):
        """Adds funds to the wallet."""
        if amount < 0:
            self.logger.error("Cannot deposit negative amount.")
            return
        
        current = self.get_balance(currency)
        self.balances[currency] = current + amount
        self.save()
        self.logger.info(f"Deposited {amount} {currency}. New Balance: {self.balances[currency]}")

    def withdraw(self, currency: str, amount: float) -> bool:
        """Subtracts funds from the wallet if sufficient balance exists."""
        if amount < 0:
            self.logger.error("Cannot withdraw negative amount.")
            return False

        current = self.get_balance(currency)
        if current >= amount:
            self.balances[currency] = current - amount
            self.save()
            self.logger.info(f"Withdrew {amount} {currency}. New Balance: {self.balances[currency]}")
            return True
        else:
            self.logger.warning(f"Insufficient funds to withdraw {amount} {currency}. Balance: {current}")
            return False
            
    def get_all_balances(self) -> Dict[str, float]:
        return self.balances
