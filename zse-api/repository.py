from datetime import date
from decimal import Decimal
from typing import List, Optional, Dict, Any, Union
import psycopg
from psycopg.types.json import Json
from db import get_db_cursor

class BaseRepository:
    """Base repository with common CRUD helpers (if needed)"""
    pass

class SecurityRepository(BaseRepository):
    @staticmethod
    def get_by_symbol(symbol: str) -> Optional[Dict[str, Any]]:
        with get_db_cursor() as cur:
            cur.execute("SELECT * FROM securities WHERE symbol = %s", (symbol.upper(),))
            return cur.fetchone()

    @staticmethod
    def get_or_create(symbol: str, security_type: str = 'equity', currency: str = 'ZWG') -> int:
        """Get ID or create new security"""
        with get_db_cursor(commit=True) as cur:
            cur.execute("SELECT id FROM securities WHERE symbol = %s", (symbol,))
            res = cur.fetchone()
            if res:
                return res['id']
            
            cur.execute("""
                INSERT INTO securities (symbol, security_type, currency)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (symbol, security_type, currency))
            return cur.fetchone()['id']

    @staticmethod
    def list_all(active_only: bool = True, sec_type: Optional[str] = None, sector: Optional[str] = None) -> List[Dict[str, Any]]:
        query = "SELECT * FROM securities WHERE 1=1"
        params = []
        if active_only:
            query += " AND is_active = true"
        if sec_type:
            query += " AND security_type = %s"
            params.append(sec_type)
        if sector:
            query += " AND sector = %s"
            params.append(sector)
        query += " ORDER BY symbol"
        
        with get_db_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

class PriceRepository(BaseRepository):
    @staticmethod
    def save_daily_price(security_id: int, trade_date: date, price: float, 
                        change_pct: float, market_cap: Optional[float] = None,
                        volume: Optional[int] = None, trades: Optional[int] = None) -> None:
        with get_db_cursor(commit=True) as cur:
            cur.execute("""
                INSERT INTO daily_prices 
                    (security_id, trade_date, price, change_pct, market_cap, volume, trades_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (security_id, trade_date) 
                DO UPDATE SET 
                    price = EXCLUDED.price,
                    change_pct = EXCLUDED.change_pct,
                    market_cap = EXCLUDED.market_cap,
                    volume = COALESCE(EXCLUDED.volume, daily_prices.volume),
                    trades_count = COALESCE(EXCLUDED.trades_count, daily_prices.trades_count),
                    data_source = 'scraper'
            """, (security_id, trade_date, price, change_pct, market_cap, volume, trades))

    @staticmethod
    def get_history(symbol: str, start_date: Optional[date] = None, end_date: Optional[date] = None, limit: int = 30):
        query = """
            SELECT s.symbol, s.currency, dp.*
            FROM daily_prices dp
            JOIN securities s ON s.id = dp.security_id
            WHERE s.symbol = %s
        """
        params = [symbol.upper()]
        
        if start_date:
            query += " AND dp.trade_date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND dp.trade_date <= %s"
            params.append(end_date)
            
        query += " ORDER BY dp.trade_date DESC LIMIT %s"
        params.append(limit)
        
        with get_db_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()
            
    @staticmethod
    def get_latest(symbol: str):
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT s.symbol, s.currency, dp.*
                FROM daily_prices dp
                JOIN securities s ON s.id = dp.security_id
                WHERE s.symbol = %s
                ORDER BY dp.trade_date DESC
                LIMIT 1
            """, (symbol.upper(),))
            return cur.fetchone()
            
    @staticmethod
    def get_top_movers(limit: int = 5, mode: str = 'gainers'):
        filters = "dp.change_pct > 0" if mode == 'gainers' else "dp.change_pct < 0"
        order = "DESC" if mode == 'gainers' else "ASC"
        
        with get_db_cursor() as cur:
            cur.execute(f"""
                SELECT s.symbol, dp.price, dp.change_pct, 
                       CASE WHEN dp.change_pct > 0 THEN 'gainer' ELSE 'loser' END as movement_type
                FROM daily_prices dp
                JOIN securities s ON s.id = dp.security_id
                WHERE dp.trade_date = (SELECT MAX(trade_date) FROM daily_prices)
                  AND {filters}
                ORDER BY dp.change_pct {order}
                LIMIT %s
            """, (limit,))
            return cur.fetchall()

class MarketRepository(BaseRepository):
    @staticmethod
    def save_index(name: str, value: float, change_pct: float, trade_date: date, index_type: str = 'market_cap'):
        with get_db_cursor(commit=True) as cur:
            cur.execute("""
                INSERT INTO market_indices (index_name, index_type, trade_date, index_value, change_pct)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (index_name, trade_date)
                DO UPDATE SET
                    index_value = EXCLUDED.index_value,
                    change_pct = EXCLUDED.change_pct
            """, (name, index_type, trade_date, value, change_pct))
            
    @staticmethod
    def save_snapshot(trade_date: date, activity: Dict[str, Any]):
        with get_db_cursor(commit=True) as cur:
            cur.execute("""
                INSERT INTO market_snapshots 
                    (trade_date, total_trades, total_turnover, market_cap, foreign_purchases, foreign_sales)
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

    @staticmethod
    def get_summary(trade_date: Optional[date] = None):
        with get_db_cursor() as cur:
            if trade_date:
                cur.execute("SELECT * FROM v_market_summary WHERE trade_date = %s", (trade_date,))
            else:
                cur.execute("SELECT * FROM v_market_summary ORDER BY trade_date DESC LIMIT 1")
            return cur.fetchone()

    @staticmethod
    def list_indices(trade_date: Optional[date] = None, index_type: Optional[str] = None):
        query = "SELECT * FROM market_indices WHERE 1=1"
        params = []
        if index_type:
            query += " AND index_type = %s"
            params.append(index_type)
        if trade_date:
            query += " AND trade_date = %s"
            params.append(trade_date)
        else:
            query += " AND trade_date = (SELECT MAX(trade_date) FROM market_indices)"
        query += " ORDER BY index_name"
        
        with get_db_cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

class LogRepository(BaseRepository):
    @staticmethod
    def log_scrape(status: str, records_parsed: int, error_message: Optional[str] = None, raw_data: Optional[Dict] = None):
        with get_db_cursor(commit=True) as cur:
            cur.execute("""
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

class ApiKeyRepository(BaseRepository):
    @staticmethod
    def create(key_hash: str, key_prefix: str, email: str, tier: str, limits: Dict[str, int]) -> int:
        with get_db_cursor(commit=True) as cur:
            cur.execute("""
                INSERT INTO api_keys 
                    (key_hash, key_prefix, user_email, tier, daily_limit, monthly_limit)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (key_hash, key_prefix, email, tier, limits['daily'], limits['monthly']))
            return cur.fetchone()['id']
            
    @staticmethod
    def get_by_hash(key_hash: str) -> Optional[Dict[str, Any]]:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT id, tier, requests_today, daily_limit, requests_month, monthly_limit, is_active
                FROM api_keys
                WHERE key_hash = %s
            """, (key_hash,))
            return cur.fetchone()

    @staticmethod
    def increment_usage(key_id: int):
        with get_db_cursor(commit=True) as cur:
            cur.execute("""
                UPDATE api_keys 
                SET requests_today = requests_today + 1,
                    requests_month = requests_month + 1,
                    last_used_at = NOW()
                WHERE id = %s
            """, (key_id,))

    @staticmethod
    def list_all() -> List[Dict[str, Any]]:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT * FROM api_keys ORDER BY created_at DESC
            """)
            return cur.fetchall()
            
    @staticmethod
    def set_active_status(key_id: int, is_active: bool) -> Optional[str]:
        with get_db_cursor(commit=True) as cur:
            cur.execute("""
                UPDATE api_keys SET is_active = %s WHERE id = %s RETURNING user_email
            """, (is_active, key_id))
            res = cur.fetchone()
            return res['user_email'] if res else None

    @staticmethod
    def get_stats() -> Dict[str, Any]:
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT 
                    COUNT(*) as total_keys,
                    COUNT(CASE WHEN is_active THEN 1 END) as active_keys,
                    SUM(requests_today) as total_requests_today,
                    SUM(requests_month) as total_requests_month
                FROM api_keys
            """)
            overall = cur.fetchone()
            
            cur.execute("""
                SELECT user_email, requests_today FROM api_keys 
                WHERE requests_today > 0 ORDER BY requests_today DESC LIMIT 5
            """)
            top_users = cur.fetchall()
            
            return {'overall': overall, 'top_users': top_users}

    @staticmethod
    def reset_counters(counter_type: str = 'daily'):
        field = 'requests_today' if counter_type == 'daily' else 'requests_month'
        with get_db_cursor(commit=True) as cur:
            cur.execute(f"UPDATE api_keys SET {field} = 0")
            return cur.rowcount
