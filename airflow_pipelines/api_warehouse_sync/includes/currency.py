"""
Currency Exchange Service
==========================
Handles currency conversion with caching for efficient lookups.
"""

from datetime import date
from functools import lru_cache
from typing import Optional
import requests
from airflow.models import Variable


class CurrencyExchange:
    """Service for converting between currencies using exchange rates."""
    
    def __init__(self):
        self.api_url = Variable.get(
            "EXCHANGE_RATE_API_URL", 
            default_var="https://api.exchangerate-api.com/v4/latest"
        )
        self._rates_cache = {}
    
    @lru_cache(maxsize=100)
    def get_rate(self, from_currency: str, to_currency: str = "USD") -> float:
        """
        Get exchange rate for currency pair.
        
        Uses caching to minimize API calls.
        Falls back to 1.0 if rate unavailable.
        """
        if from_currency.upper() == to_currency.upper():
            return 1.0
        
        try:
            response = requests.get(
                f"{self.api_url}/{from_currency.upper()}",
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            rates = data.get("rates", {})
            return rates.get(to_currency.upper(), 1.0)
        except Exception as e:
            print(f"⚠️ Failed to get exchange rate {from_currency}->{to_currency}: {e}")
            return 1.0
    
    def convert(
        self, 
        amount: float, 
        from_currency: str, 
        to_currency: str = "USD"
    ) -> float:
        """Convert amount from one currency to another."""
        rate = self.get_rate(from_currency, to_currency)
        return round(amount * rate, 2)


# Common exchange rates for offline/testing (approximate values)
FALLBACK_RATES = {
    ("EUR", "USD"): 1.10,
    ("GBP", "USD"): 1.27,
    ("CAD", "USD"): 0.74,
    ("AUD", "USD"): 0.66,
    ("JPY", "USD"): 0.0067,
}


class OfflineCurrencyExchange(CurrencyExchange):
    """Currency exchange using static rates for testing/offline mode."""
    
    def get_rate(self, from_currency: str, to_currency: str = "USD") -> float:
        if from_currency.upper() == to_currency.upper():
            return 1.0
        
        key = (from_currency.upper(), to_currency.upper())
        return FALLBACK_RATES.get(key, 1.0)
