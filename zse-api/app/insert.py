from app.db import get_db_cursor
from datetime import datetime
import logging

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
            'type': 'equity'
        })
        
    for item in data.get('etfs', []):
        securities.append({
            'symbol': item['symbol'],
            'name': item.get('name', item['symbol']),
            'type': 'etf'
        })
        
    for item in data.get('reits', []):
        securities.append({
            'symbol': item['symbol'],
            'name': item.get('name', item['symbol']),
            'type': 'reit'
        })

    with get_db_cursor(commit=True) as cur:
        for security in securities:
            cur.execute("""
                INSERT INTO securities (symbol, name, security_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE 
                SET updated_at = NOW()
                RETURNING id;
            """, (security['symbol'], security['name'], security['type']))

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
    
    # Note: If we had a full list of all equities, we would process that too.
    # Currently we only catch what's on the homepage summaries.

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
                INSERT INTO prices (security_id, price, change_pct, market_cap, trade_date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (security_id, trade_date) DO UPDATE
                SET price = EXCLUDED.price,
                    change_pct = EXCLUDED.change_pct,
                    market_cap = EXCLUDED.market_cap,
                    captured_at = NOW();
            """, (security_id, item['price'], item['change_pct'], item['market_cap'], trade_date))

def upsert_market_activity(data):
    """Upsert market activity"""
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
            INSERT INTO market_activity 
            (trade_date, trades_count, turnover, market_cap, foreign_purchases, foreign_sales)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (trade_date) DO UPDATE
            SET trades_count = EXCLUDED.trades_count,
                turnover = EXCLUDED.turnover,
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
