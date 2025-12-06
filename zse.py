#zse data scrapper using python 

import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List, Dict, Optional 
import json 


class ZSEScraper:
    def __init__(self):
        self.base_url = "https://www.zse.co.zw"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        
    def fetch_homepage(self)-> str:
        #fetch zse homepage html content
        try:
            response = requests.get(self.base_url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            print(f"Error fetching homepage: {e}")
            return None
    
    def parse_table(self, soup: BeautifulSoup, table_identifier: str) -> List[Dict]:
        #Generic table parser - finds table by nearby heading
        results = []

        # Find the heading containing the identifier
        heading = soup.find(lambda tag: tag.name in ['h4', 'h3', 'h2'] and 
                           table_identifier.lower() in tag.get_text().lower())
        
        if not heading:
            return results
        
        # Parse table rows
        table = heading.find_next('table')
        if not table:
            return results

        rows = table.find_all('tr')
        if len(rows) < 2:  # Need header + data
            return results
        
        # Get headers from first row
        headers = [th.get_text().strip() for th in rows[0].find_all(['th', 'td'])]
        
        # Parse data rows
        for row in rows[1:]:
            cols = [td.get_text().strip() for td in row.find_all('td')]
            if cols:
                row_dict = {}
                for i, col in enumerate(cols):
                    if i < len(headers):
                        row_dict[headers[i]] = col
                results.append(row_dict)
        
        return results

    def clean_numeric(self, value: str) -> Optional[float]:
        """Clean and convert numeric strings"""
        if not value or value in ['-', 'N/A', '']:
            return None
        
        # Remove common characters
        cleaned = value.replace(',', '').replace('%', '').replace('▲', '').replace('▼', '').strip()
        
        try:
            return float(cleaned)
        except ValueError:
            return None