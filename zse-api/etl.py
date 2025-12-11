import sys
import logging
from datetime import datetime, date

# Import the scraper and repositories
from scraper import ZSEScraper
from repository import (
    SecurityRepository, 
    PriceRepository, 
    MarketRepository, 
    LogRepository
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ZSEDataPipeline:
    def __init__(self):
        self.scraper = ZSEScraper()
    
    def parse_trade_date(self, date_str: str) -> date:
        """Parse date string from ZSE (e.g., '05 DEC 2025')"""
        if not date_str:
            return date.today()
        try:
            return datetime.strptime(date_str, '%d %b %Y').date()
        except Exception:
            # Fallback to today
            return date.today()
    
    def run(self) -> bool:
        """Run the full scraping pipeline"""
        start_time = datetime.now()
        records_parsed = 0
        current_data = None
        
        try:
            # Scrape data
            logger.info("Scraping ZSE homepage...")
            current_data = self.scraper.scrape_all()
            
            if not current_data:
                raise Exception("Failed to scrape data")
            
            # Determine trade date
            trade_date = date.today()
            if current_data.get('market_activity', {}).get('trade_date'):
                trade_date = self.parse_trade_date(current_data['market_activity']['trade_date'])
            
            logger.info(f"Processing data for {trade_date}...")
            
            # Helper to process securities
            def process_security_list(items, sec_type='equity'):
                count = 0
                for item in items:
                    if not item.get('symbol'):
                        continue
                        
                    # Get or create security
                    security_id = SecurityRepository.get_or_create(
                        symbol=item['symbol'],
                        security_type=sec_type
                    )
                    
                    # Store price
                    PriceRepository.save_daily_price(
                        security_id=security_id,
                        trade_date=trade_date,
                        price=item.get('price'),
                        change_pct=item.get('change_pct'),
                        market_cap=item.get('market_cap')
                    )
                    count += 1
                return count

            # Process all security lists
            records_parsed += process_security_list(current_data.get('top_gainers', []), 'equity')
            records_parsed += process_security_list(current_data.get('top_losers', []), 'equity')
            records_parsed += process_security_list(current_data.get('etfs', []), 'etf')
            records_parsed += process_security_list(current_data.get('reits', []), 'reit')
            
            # Store market indices
            for item in current_data.get('market_indices', []):
                if item.get('name'):
                    MarketRepository.save_index(
                        name=item['name'],
                        value=item.get('value'),
                        change_pct=item.get('change_pct'),
                        trade_date=trade_date,
                        index_type='market_cap'
                    )
                    records_parsed += 1
            
            # Store sector indices
            for item in current_data.get('sector_indices', []):
                if item.get('name'):
                    MarketRepository.save_index(
                        name=item['name'],
                        value=item.get('value'),
                        change_pct=item.get('change_pct'),
                        trade_date=trade_date,
                        index_type='sector'
                    )
                    records_parsed += 1
            
            # Store market activity
            if current_data.get('market_activity'):
                MarketRepository.save_snapshot(
                    trade_date=trade_date,
                    activity=current_data['market_activity']
                )
                records_parsed += 1
            
            # Log successful scrape
            LogRepository.log_scrape(
                status='success',
                records_parsed=records_parsed,
                raw_data=current_data
            )
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"""
=== Scrape Complete ===
Records parsed: {records_parsed}
Trade date: {trade_date}
Execution time: {execution_time:.2f}s
Status: SUCCESS
            """)
            
            return True
            
        except Exception as e:
            logger.error(f"ERROR: {str(e)}")
            
            try:
                LogRepository.log_scrape(
                    status='failed',
                    records_parsed=records_parsed,
                    error_message=str(e)
                )
            except Exception as log_err:
                logger.critical(f"FATAL: Could not write error log: {log_err}")
            
            return False

if __name__ == "__main__":
    pipeline = ZSEDataPipeline()
    success = pipeline.run()
    sys.exit(0 if success else 1)

# ============================================================================
# CLI Interface
# ============================================================================

if __name__ == "__main__":
    pipeline = ZSEDataPipeline(DATABASE_URL)
    success = pipeline.run()
    
    sys.exit(0 if success else 1)
