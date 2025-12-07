from app.db import get_db_cursor
from datetime import datetime
import logging
import json

logger = logging.getLogger(__name__)

def upsert_securities(data):
    """Upsert securities (Equities, ETFs, REITs)"""
    # Combine all security lists with type tagging
    securities = []
    
    # Process gainers/losers as Equities (default)
    for item in data.get('top_gainers', []) + data.get('top_losers', []):
        securities.append({
            'symbol': item['symbol'],
            'name': item.get('name', item['symbol']), # Fallback if name is missing
            'type': 'equity',
            'currency': item.get('currency', 'ZWG')
        })
        
    for item in data.get('etfs', []):
        securities.append({
            'symbol': item['symbol'],
            'name': item.get('name', item['symbol']),
            'type': 'etf',
            'currency': item.get('currency', 'ZWG')
        })
        
    for item in data.get('reits', []):
        securities.append({
            'symbol': item['symbol'],
            'name': item.get('name', item['symbol']),
            'type': 'reit',
            'currency': item.get('currency', 'ZWG')
        })

    with get_db_cursor(commit=True) as cur:
        for security in securities:
            cur.execute("""
                INSERT INTO securities (symbol, name, security_type, currency)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE 
                SET updated_at = NOW(),
                    name = EXCLUDED.name,
                    currency = EXCLUDED.currency
                RETURNING id;
            """, (security['symbol'], security['name'], security['type'], security['currency']))

def upsert_prices(data, trade_date_str=None):
    """Upsert daily prices"""
    if not trade_date_str:
        # Default to today if not provided
        trade_date = datetime.now().date()
    else:
        try:
            # Parse "05 DEC 2025" -> date object
            trade_date = datetime.strptime(trade_date_str, "%d %b %Y").date()
        except ValueError:
            logger.error(f"Could not parse date: {trade_date_str}")
            return

    # Collect all price data
    price_items = []
    # Helper to process lists
    def process_list(list_key):
        for item in data.get(list_key, []):
            price_items.append({
                'symbol': item['symbol'],
                'price': item.get('price'),
                'change_pct': item.get('change_pct'),
                'market_cap': item.get('market_cap'),
            })

    process_list('top_gainers')
    process_list('top_losers')
    process_list('etfs')
    process_list('reits')
    
    with get_db_cursor(commit=True) as cur:
        for item in price_items:
            # Get security ID
            cur.execute("SELECT id FROM securities WHERE symbol = %s", (item['symbol'],))
            res = cur.fetchone()
            if not res:
                logger.warning(f"Security not found for price upsert: {item['symbol']}")
                continue
            
            security_id = res['id']
            
            cur.execute("""
                INSERT INTO daily_prices (security_id, trade_date, price, change_pct, market_cap)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (security_id, trade_date) DO UPDATE
                SET price = EXCLUDED.price,
                    change_pct = EXCLUDED.change_pct,
                    market_cap = EXCLUDED.market_cap,
                    created_at = NOW();
            """, (security_id, trade_date, item['price'], item['change_pct'], item['market_cap']))

def upsert_market_ids(data, trade_date):
    """Upsert Market Indices"""
    indices = data.get('market_indices', []) + data.get('sector_indices', [])
    
    with get_db_cursor(commit=True) as cur:
        for idx in indices:
            cur.execute("""
                INSERT INTO market_indices (index_name, trade_date, index_value, change_pct, index_type)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (index_name, trade_date) DO UPDATE
                SET index_value = EXCLUDED.index_value,
                    change_pct = EXCLUDED.change_pct;
            """, (idx['name'], trade_date, idx['value'], idx['change_pct'], 'market_index'))

def upsert_market_activity(data):
    """Upsert market activity (now market_snapshots)"""
    activity = data.get('market_activity', {})
    if not activity:
        return

    date_str = activity.get('trade_date')
    if not date_str:
        logger.warning("No trade date found in market activity")
        return

    try:
        trade_date = datetime.strptime(date_str, "%d %b %Y").date()
    except ValueError:
        logger.error(f"Could not parse market activity date: {date_str}")
        return

    with get_db_cursor(commit=True) as cur:
        cur.execute("""
            INSERT INTO market_snapshots 
            (trade_date, total_trades, total_turnover, market_cap, foreign_purchases, foreign_sales)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_date) DO UPDATE
            SET total_trades = EXCLUDED.total_trades,
                total_turnover = EXCLUDED.total_turnover,
                market_cap = EXCLUDED.market_cap,
                foreign_purchases = EXCLUDED.foreign_purchases,
                foreign_sales = EXCLUDED.foreign_sales;
        """, (
            trade_date,
            activity.get('trades_count'),
            activity.get('turnover'),
            activity.get('market_cap'),
            activity.get('foreign_purchases'),
            activity.get('foreign_sales')
        ))

    return trade_date

def log_scrape_result(status, source_url, records=0, error_msg=None, raw_data=None):
    """Log scrape attempt to DB"""
    try:
        with get_db_cursor(commit=True) as cur:
            cur.execute("""
                INSERT INTO scrape_logs (status, source_url, records_parsed, error_message, raw_snapshot)
                VALUES (%s, %s, %s, %s, %s)
            """, (status, source_url, records, error_msg, json.dumps(raw_data) if raw_data else None))
    except Exception as e:
        logger.error(f"Failed to write scrape log: {e}")
