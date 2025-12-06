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
            response = requests.get(self.base_url, headers=self.headers)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching homepage: {e}")
            return ""

if __name__ == "__main__":
    scraper = ZSEScraper()
    html = scraper.fetch_homepage()
    if html:
        print("Successfully fetched homepage")
        print(f"Content length: {len(html)} characters")
    else:
        print("Failed to fetch homepage")