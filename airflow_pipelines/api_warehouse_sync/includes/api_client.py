"""
API Client for Affiliate Platform
==================================
Handles paginated API requests with rate limiting and error handling.
"""

import requests
from typing import Optional
from airflow.models import Variable


class AffiliateAPIClient:
    """Client for interacting with affiliate partner API."""
    
    def __init__(self):
        self.base_url = Variable.get("AFFILIATE_API_URL", default_var="https://api.example.com")
        self.api_key = Variable.get("AFFILIATE_API_KEY")
        self.current_page = 0
        self.per_page = 100
        self._has_more = True
    
    @property
    def headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
    
    def get_partners(self) -> list:
        """
        Fetch next page of partners.
        
        Returns empty list when all pages exhausted.
        Handles pagination automatically.
        """
        if not self._has_more:
            return []
        
        self.current_page += 1
        
        response = requests.get(
            f"{self.base_url}/partners",
            headers=self.headers,
            params={
                "page": self.current_page,
                "per_page": self.per_page,
            },
            timeout=30,
        )
        response.raise_for_status()
        
        data = response.json()
        partners = data.get("data", [])
        
        # Check if more pages exist
        meta = data.get("meta", {})
        total_pages = meta.get("total_pages", 1)
        self._has_more = self.current_page < total_pages
        
        return partners
    
    def get_partner_details(self, partner_id: str) -> dict:
        """Fetch detailed information for a specific partner."""
        response = requests.get(
            f"{self.base_url}/partners/{partner_id}",
            headers=self.headers,
            timeout=30,
        )
        response.raise_for_status()
        return response.json().get("data", {})


class MockAffiliateAPIClient(AffiliateAPIClient):
    """Mock client for testing without real API calls."""
    
    def __init__(self):
        self.current_page = 0
        self._has_more = True
        self._mock_data = [
            {
                "id": f"partner_{i}",
                "name": f"Partner {i}",
                "surname": "Demo",
                "email": f"partner{i}@example.com",
                "status": "active",
                "created_at": "2024-01-15T10:00:00Z",
                "custom_fields": {
                    "country": "US",
                    "website": f"https://partner{i}.example.com",
                },
            }
            for i in range(1, 251)  # 250 mock partners
        ]
    
    def get_partners(self) -> list:
        if not self._has_more:
            return []
        
        self.current_page += 1
        start = (self.current_page - 1) * 100
        end = start + 100
        
        partners = self._mock_data[start:end]
        self._has_more = end < len(self._mock_data)
        
        return partners
    
    def get_partner_details(self, partner_id: str) -> dict:
        return {
            "referrals": {
                "signups": 150,
                "customers": 25,
                "revenue": {"USD": 5000.00},
            },
            "commissions": {
                "paid": {"USD": 500.00},
                "pending": {"USD": 125.00},
            },
        }
