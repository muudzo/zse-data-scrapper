"""
ZSE Scraper with Database Storage
Scrapes ZSE homepage and stores data in PostgreSQL
"""

import psycopg2
from psycopg2.extras import Json
from datetime import datetime, date
import os
import sys
import json

# Import the scraper
from app.scraper import ZSEScraper

# Construct Database URL from environment variables
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "zse_db")

DATABASE_URL = os.getenv("DATABASE_URL", f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

class ZSEDataPipeline:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.scraper = ZSEScraper()
        self.conn = None
    
    def connect(self):
        """Connect to database"""
        self.conn = psycopg2.connect(self.database_url)
        return self.conn
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
    
    def get_or_create_security(self, symbol: str, security_type: str = 'equity', cursor=None) -> int:
        """Get security ID or create if doesn't exist"""
        if cursor is None:
            cursor = self.conn.cursor()
        
        # Try to find existing
        cursor.execute("SELECT id FROM securities WHERE symbol = %s", (symbol,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        
        # Create new
        cursor.execute("""
            INSERT INTO securities (symbol, security_type, currency)
            VALUES (%s, %s, 'ZWG')
            RETURNING id
        """, (symbol, security_type))
        
        return cursor.fetchone()[0]
    
    def store_daily_price(self, symbol: str, price: float, change_pct: float, 
                         trade_date: date, security_type: str = 'equity',
                         market_cap: float = None, cursor=None):
        """Store daily price data"""
        if cursor is None:
            cursor = self.conn.cursor()
        
        if price is None:
            return
        
        security_id = self.get_or_create_security(symbol, security_type, cursor)
        
        cursor.execute("""
            INSERT INTO daily_prices 
                (security_id, trade_date, price, change_pct, market_cap)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (security_id, trade_date) 
            DO UPDATE SET 
                price = EXCLUDED.price,
                change_pct = EXCLUDED.change_pct,
                market_cap = EXCLUDED.market_cap,
                data_source = 'homepage_scrape'
        """, (security_id, trade_date, price, change_pct, market_cap))
    
    def store_market_index(self, index_name: str, value: float, change_pct: float,
                          trade_date: date, index_type: str = 'market_cap', cursor=None):
        """Store market index data"""
        if cursor is None:
            cursor = self.conn.cursor()
        
        if value is None:
            return
        
        cursor.execute("""
            INSERT INTO market_indices 
                (index_name, index_type, trade_date, index_value, change_pct)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (index_name, trade_date)
            DO UPDATE SET
                index_value = EXCLUDED.index_value,
                change_pct = EXCLUDED.change_pct
        """, (index_name, index_type, trade_date, value, change_pct))
    
    def store_market_snapshot(self, activity: dict, trade_date: date, cursor=None):
        """Store market activity snapshot"""
        if cursor is None:
            cursor = self.conn.cursor()
        
        cursor.execute("""
            INSERT INTO market_snapshots 
                (trade_date, total_trades, total_turnover, market_cap, 
                 foreign_purchases, foreign_sales)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_date)
            DO UPDATE SET
                total_trades = EXCLUDED.total_trades,
                total_turnover = EXCLUDED.total_turnover,
                market_cap = EXCLUDED.market_cap,
                foreign_purchases = EXCLUDED.foreign_purchases,
                foreign_sales = EXCLUDED.foreign_sales
        """, (
            trade_date,
            activity.get('trades_count'),
            activity.get('turnover'),
            activity.get('market_cap'),
            activity.get('foreign_purchases'),
            activity.get('foreign_sales')
        ))
    
    def log_scrape(self, status: str, records_parsed: int, error_message: str = None, 
                   raw_data: dict = None, cursor=None):
        """Log scrape attempt"""
        if cursor is None:
            cursor = self.conn.cursor()
        
        cursor.execute("""
            INSERT INTO scrape_logs 
                (status, source_url, records_parsed, error_message, raw_snapshot)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            status,
            'https://www.zse.co.zw',
            records_parsed,
            error_message,
            Json(raw_data) if raw_data else None
        ))
    
    def parse_trade_date(self, date_str: str) -> date:
        """Parse date string from ZSE (e.g., '05 DEC 2025')"""
        try:
            return datetime.strptime(date_str, '%d %b %Y').date()
        except:
            # Fallback to today
            return date.today()
    
    def run(self):
        """Run the full scraping pipeline"""
        start_time = datetime.now()
        records_parsed = 0
        
        try:
            # Scrape data
            print("Scraping ZSE homepage...")
            data = self.scraper.scrape_all()
            
            if not data:
                raise Exception("Failed to scrape data")
            
            # Connect to database
            self.connect()
            cursor = self.conn.cursor()
            
            # Determine trade date
            trade_date = date.today()
            if data.get('market_activity', {}).get('trade_date'):
                trade_date = self.parse_trade_date(data['market_activity']['trade_date'])
            
            print(f"Processing data for {trade_date}...")
            
            # Store top gainers
            for item in data.get('top_gainers', []):
                if item.get('symbol'):
                    self.store_daily_price(
                        symbol=item['symbol'],
                        price=item.get('price'),
                        change_pct=item.get('change_pct'),
                        trade_date=trade_date,
                        cursor=cursor
                    )
                    records_parsed += 1
            
            # Store top losers
            for item in data.get('top_losers', []):
                if item.get('symbol'):
                    self.store_daily_price(
                        symbol=item['symbol'],
                        price=item.get('price'),
                        change_pct=item.get('change_pct'),
                        trade_date=trade_date,
                        cursor=cursor
                    )
                    records_parsed += 1
            
            # Store ETFs
            for item in data.get('etfs', []):
                if item.get('symbol'):
                    self.store_daily_price(
                        symbol=item['symbol'],
                        price=item.get('price'),
                        change_pct=item.get('change_pct'),
                        trade_date=trade_date,
                        security_type='etf',
                        market_cap=item.get('market_cap'),
                        cursor=cursor
                    )
                    records_parsed += 1
            
            # Store REITs
            for item in data.get('reits', []):
                if item.get('symbol'):
                    self.store_daily_price(
                        symbol=item['symbol'],
                        price=item.get('price'),
                        change_pct=item.get('change_pct'),
                        trade_date=trade_date,
                        security_type='reit',
                        market_cap=item.get('market_cap'),
                        cursor=cursor
                    )
                    records_parsed += 1
            
            # Store market indices
            for item in data.get('market_indices', []):
                if item.get('name'):
                    self.store_market_index(
                        index_name=item['name'],
                        value=item.get('value'),
                        change_pct=item.get('change_pct'),
                        trade_date=trade_date,
                        index_type='market_cap',
                        cursor=cursor
                    )
                    records_parsed += 1
            
            # Store sector indices
            for item in data.get('sector_indices', []):
                if item.get('name'):
                    self.store_market_index(
                        index_name=item['name'],
                        value=item.get('value'),
                        change_pct=item.get('change_pct'),
                        trade_date=trade_date,
                        index_type='sector',
                        cursor=cursor
                    )
                    records_parsed += 1
            
            # Store market activity
            if data.get('market_activity'):
                self.store_market_snapshot(
                    activity=data['market_activity'],
                    trade_date=trade_date,
                    cursor=cursor
                )
                records_parsed += 1
            
            # Log successful scrape
            self.log_scrape(
                status='success',
                records_parsed=records_parsed,
                raw_data=data,
                cursor=cursor
            )
            
            # Commit transaction
            self.conn.commit()
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            print(f"""
=== Scrape Complete ===
Records parsed: {records_parsed}
Trade date: {trade_date}
Execution time: {execution_time:.2f}s
Status: SUCCESS
            """)
            
            return True
            
        except Exception as e:
            print(f"ERROR: {str(e)}")
            
            if self.conn:
                self.conn.rollback()
                cursor = self.conn.cursor()
                try:
                    self.log_scrape(
                        status='failed',
                        records_parsed=records_parsed,
                        error_message=str(e),
                        cursor=cursor
                    )
                    self.conn.commit()
                except Exception as log_err:
                    print(f"FATAL: Could not write error log: {log_err}")
            
            return False
            
        finally:
            self.close()


# ============================================================================
# CLI Interface
# ============================================================================

if __name__ == "__main__":
    pipeline = ZSEDataPipeline(DATABASE_URL)
    success = pipeline.run()
    
    sys.exit(0 if success else 1)
