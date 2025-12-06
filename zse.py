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

    def scrape_top_gainers(self, soup: BeautifulSoup) -> List[Dict]:
        #Scrape top gainers table
        gainers = self.parse_table(soup, "TOP GAINERS")
        
        result = []
        for item in gainers:
            result.append({
                'symbol': item.get('SYMBOL', '').replace('.zw', ''),
                'price': self.clean_numeric(item.get('VALUE (ZWG cents)', '')),
                'change_pct': self.clean_numeric(item.get('CHANGE', '')),
                'currency': 'ZWG'
            })
        return result
    
    def scrape_top_losers(self, soup: BeautifulSoup) -> List[Dict]:
        #Scrape top losers table
        losers = self.parse_table(soup, "TOP LOSERS")
        
        result = []
        for item in losers:
            result.append({
                'symbol': item.get('SYMBOL', '').replace('.zw', ''),
                'price': self.clean_numeric(item.get('VALUE (ZWG Cents)', '')),
                'change_pct': self.clean_numeric(item.get('CHANGE', '')),
                'currency': 'ZWG'
            })
        return result
    

def scrape_market_indices(self, soup: BeautifulSoup) -> List[Dict]:
        #Scrape market cap indices
        indices = self.parse_table(soup, "ZSE MARKET CAP INDICES")
        
        result = []
        for item in indices:
            result.append({
                'name': item.get('INDEX', ''),
                'value': self.clean_numeric(item.get('VALUE', '')),
                'change_pct': self.clean_numeric(item.get('CHANGE', ''))
            })
        return result
    
    def scrape_sector_indices(self, soup: BeautifulSoup) -> List[Dict]:
        #Scrape sector indices
        sectors = self.parse_table(soup, "ZSE SECTOR INDICES")
        
        result = []
        for item in sectors:
            result.append({
                'name': item.get('INDEX', ''),
                'value': self.clean_numeric(item.get('VALUE', '')),
                'change_pct': self.clean_numeric(item.get('CHANGE', ''))
            })
        return result

def scrape_etfs(self, soup: BeautifulSoup) -> List[Dict]:
        #Scrape Exchange Traded Funds
        etfs = self.parse_table(soup, "EXCHANGE TRADED FUNDS")
        
        result = []
        for item in etfs:
            result.append({
                'symbol': item.get('SECURITY', '').replace('.zw', ''),
                'price': self.clean_numeric(item.get('PRICE (ZWG Cents)', '')),
                'change_pct': self.clean_numeric(item.get('CHANGE', '')),
                'market_cap': self.clean_numeric(item.get('MARKET CAP (ZWG)', '')),
                'currency': 'ZWG'
            })
        return result
    
    def scrape_reits(self, soup: BeautifulSoup) -> List[Dict]:
        #Scrape Real Estate Investment Trusts
        reits = self.parse_table(soup, "REAL ESTATE INVESTMENT TRUST")
        
        result = []
        for item in reits:
            result.append({
                'symbol': item.get('SECURITY', '').replace('.zw', ''),
                'price': self.clean_numeric(item.get('PRICE (ZWG Cents)', '')),
                'change_pct': self.clean_numeric(item.get('CHANGE', '')),
                'market_cap': self.clean_numeric(item.get('MARKET CAP (ZWG)', '')),
                'currency': 'ZWG'
            })
        return result
    
    def scrape_market_activity(self, soup: BeautifulSoup) -> Dict:
        #Scrape market activity summary
        activity_section = soup.find(lambda tag: tag.name in ['h4', 'h3'] and 
                                     'MARKET ACTIVITY' in tag.get_text().upper())
        
        if not activity_section:
            return {}
        
        # Find the date in the heading
        date_str = None
        if activity_section:
            date_text = activity_section.get_text()
            # Extract date (format: "MARKET ACTIVITY 05 DEC 2025")
            parts = date_text.split()
            if len(parts) >= 4:
                date_str = ' '.join(parts[-3:])
        
        # Find the table with market stats
        table = activity_section.find_next('table')
        if not table:
            return {}
        
        activity = {'trade_date': date_str}
        
        # Parse key-value pairs from the table
        for row in table.find_all('tr'):
            cols = row.find_all('td')
            if len(cols) == 2:
                key = cols[0].get_text().strip().rstrip(':')
                value = cols[1].get_text().strip()
                
                if 'Trades' in key:
                    activity['trades_count'] = self.clean_numeric(value)
                elif 'Turnover' in key:
                    activity['turnover'] = self.clean_numeric(value)
                elif 'Market Cap' in key:
                    activity['market_cap'] = self.clean_numeric(value)
                elif 'Foreign Purchases' in key:
                    activity['foreign_purchases'] = self.clean_numeric(value)
                elif 'Foreign Sales' in key:
                    activity['foreign_sales'] = self.clean_numeric(value)
        
        return activity
    
    def scrape_all(self) -> Dict:
        """Scrape all data from homepage"""
        html = self.fetch_homepage()
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        data = {
            'scraped_at': datetime.now().isoformat(),
            'source': self.base_url,
            'top_gainers': self.scrape_top_gainers(soup),
            'top_losers': self.scrape_top_losers(soup),
            'market_indices': self.scrape_market_indices(soup),
            'sector_indices': self.scrape_sector_indices(soup),
            'etfs': self.scrape_etfs(soup),
            'reits': self.scrape_reits(soup),
            'market_activity': self.scrape_market_activity(soup)
        }
        
        return data


# Example usage
if __name__ == "__main__":
    scraper = ZSEScraper()
    data = scraper.scrape_all()
    
    if data:
        print(json.dumps(data, indent=2))
        
        # Summary statistics
        print(f"\n=== Scrape Summary ===")
        print(f"Top Gainers: {len(data['top_gainers'])}")
        print(f"Top Losers: {len(data['top_losers'])}")
        print(f"Market Indices: {len(data['market_indices'])}")
        print(f"Sector Indices: {len(data['sector_indices'])}")
        print(f"ETFs: {len(data['etfs'])}")
        print(f"REITs: {len(data['reits'])}")
    else:
        print("Failed to scrape data")