from fastapi import FastAPI, HTTPException, Query
from typing import List, Optional
from pydantic import BaseModel
from datetime import date
from app.db import get_db_cursor
import logging
from app.logging_conf import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(title="ZSE Data API", description="API for Zimbabwe Stock Exchange Data")

class Security(BaseModel):
    symbol: str
    name: Optional[str] = None
    security_type: str

class Price(BaseModel):
    symbol: str
    price: float
    change_pct: Optional[float] = None
    market_cap: Optional[float] = None
    trade_date: date

class MarketActivity(BaseModel):
    trade_date: date
    trades_count: Optional[int] = None
    turnover: Optional[float] = None
    market_cap: Optional[float] = None
    foreign_purchases: Optional[float] = None
    foreign_sales: Optional[float] = None

@app.get("/")
def read_root():
    return {"status": "ok", "service": "zse-api"}

@app.get("/securities", response_model=List[Security])
def get_securities():
    """Get all securities"""
    with get_db_cursor() as cur:
        cur.execute("SELECT symbol, name, security_type FROM securities ORDER BY symbol")
        return cur.fetchall()

@app.get("/prices/latest", response_model=List[Price])
def get_latest_prices():
    """Get latest prices for all securities"""
    with get_db_cursor() as cur:
        # Get the most recent trade date
        cur.execute("SELECT MAX(trade_date) as max_date FROM prices")
        res = cur.fetchone()
        latest_date = res['max_date']
        
        if not latest_date:
            return []

        query = """
            SELECT s.symbol, p.price, p.change_pct, p.market_cap, p.trade_date
            FROM prices p
            JOIN securities s ON p.security_id = s.id
            WHERE p.trade_date = %s
            ORDER BY s.symbol
        """
        cur.execute(query, (latest_date,))
        return cur.fetchall()

@app.get("/prices/history", response_model=List[Price])
def get_price_history(
    symbol: str = Query(..., description="Security symbol"),
    start_date: Optional[date] = None, 
    end_date: Optional[date] = None
):
    """Get historical prices for a specific security"""
    query = """
        SELECT s.symbol, p.price, p.change_pct, p.market_cap, p.trade_date
        FROM prices p
        JOIN securities s ON p.security_id = s.id
        WHERE s.symbol = %s
    """
    params = [symbol]
    
    if start_date:
        query += " AND p.trade_date >= %s"
        params.append(start_date)
    
    if end_date:
        query += " AND p.trade_date <= %s"
        params.append(end_date)
        
    query += " ORDER BY p.trade_date DESC"
    
    with get_db_cursor() as cur:
        cur.execute(query, tuple(params))
        results = cur.fetchall()
        if not results:
            # Check if symbol exists
            cur.execute("SELECT 1 FROM securities WHERE symbol = %s", (symbol,))
            if not cur.fetchone():
                raise HTTPException(status_code=404, detail="Security not found")
        return results

@app.get("/market-activity", response_model=List[MarketActivity])
def get_market_activity(limit: int = 10):
    """Get recent market activity"""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT trade_date, trades_count, turnover, market_cap, foreign_purchases, foreign_sales
            FROM market_activity
            ORDER BY trade_date DESC
            LIMIT %s
        """, (limit,))
        return cur.fetchall()
