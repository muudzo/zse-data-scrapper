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
    sector: Optional[str] = None
    currency: Optional[str] = 'ZWG'

class Price(BaseModel):
    symbol: str
    price: float
    change_pct: Optional[float] = None
    market_cap: Optional[float] = None
    volume: Optional[int] = None
    trades_count: Optional[int] = None
    trade_date: date

class MarketSnapshot(BaseModel):
    trade_date: date
    total_trades: Optional[int] = None
    total_turnover: Optional[float] = None
    market_cap: Optional[float] = None
    foreign_purchases: Optional[float] = None
    foreign_sales: Optional[float] = None
    gainers_count: Optional[int] = None
    losers_count: Optional[int] = None

@app.get("/")
def read_root():
    return {"status": "ok", "service": "zse-api"}

@app.get("/securities", response_model=List[Security])
def get_securities():
    """Get all securities"""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT symbol, name, security_type, sector, currency 
            FROM securities 
            WHERE is_active = true 
            ORDER BY symbol
        """)
        return cur.fetchall()

@app.get("/prices/latest", response_model=List[Price])
def get_latest_prices():
    """Get latest prices for all securities"""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT symbol, price, change_pct, market_cap, volume, trades_count, trade_date
            FROM v_latest_prices
            ORDER BY symbol
        """)
        return cur.fetchall()

@app.get("/prices/history", response_model=List[Price])
def get_price_history(
    symbol: str = Query(..., description="Security symbol"),
    limit: int = 30
):
    """Get historical prices for a specific security"""
    with get_db_cursor() as cur:
        # Check security exists first
        cur.execute("SELECT id FROM securities WHERE symbol = %s", (symbol,))
        sec = cur.fetchone()
        if not sec:
            raise HTTPException(status_code=404, detail="Security not found")
            
        cur.execute("""
            SELECT dp.trade_date, dp.price, dp.change_pct, dp.market_cap, dp.volume, dp.trades_count
            FROM daily_prices dp
            WHERE dp.security_id = %s
            ORDER BY dp.trade_date DESC
            LIMIT %s
        """, (sec['id'], limit))
        
        results = cur.fetchall()
        # Add symbol back to response
        for r in results:
            r['symbol'] = symbol
        return results

@app.get("/market-activity", response_model=List[MarketSnapshot])
def get_market_activity():
    """Get latest market activity summary"""
    with get_db_cursor() as cur:
        cur.execute("""
            SELECT * FROM v_market_summary
        """)
        return cur.fetchall()
