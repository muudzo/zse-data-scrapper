"""
ZSE Market Data API
FastAPI application for Zimbabwe Stock Exchange data
"""

from fastapi import FastAPI, HTTPException, Depends, Security, status
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date, datetime, timedelta
from decimal import Decimal
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import hashlib

# ============================================================================
# Configuration
# ============================================================================

# Use environment variables correctly
DATABASE_URL = f"postgresql://{os.getenv('POSTGRES_USER', 'postgres')}:{os.getenv('POSTGRES_PASSWORD', 'postgres')}@{os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'zse_db')}"
API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

# ============================================================================
# Database Connection
# ============================================================================

def get_db():
    """Get database connection"""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()

# ============================================================================
# Pydantic Models
# ============================================================================

class SecurityModel(BaseModel):
    symbol: str
    name: Optional[str] = None
    security_type: str = "equity"
    sector: Optional[str] = None
    currency: str = "ZWG"
    is_active: bool = True

class DailyPrice(BaseModel):
    symbol: str
    trade_date: date
    price: Optional[Decimal] = None
    change_pct: Optional[Decimal] = None
    market_cap: Optional[Decimal] = None
    volume: Optional[int] = None
    trades_count: Optional[int] = None
    currency: str = "ZWG"

class MarketIndex(BaseModel):
    index_name: str
    trade_date: date
    index_value: Optional[Decimal] = None
    change_pct: Optional[Decimal] = None

class MarketSummary(BaseModel):
    trade_date: date
    total_trades: Optional[int] = None
    total_turnover: Optional[Decimal] = None
    market_cap: Optional[Decimal] = None
    foreign_purchases: Optional[Decimal] = None
    foreign_sales: Optional[Decimal] = None
    gainers_count: Optional[int] = None
    losers_count: Optional[int] = None

class TopMover(BaseModel):
    symbol: str
    price: Optional[Decimal] = None
    change_pct: Optional[Decimal] = None
    movement_type: str = Field(..., description="'gainer' or 'loser'")

class APIKeyInfo(BaseModel):
    tier: str
    requests_today: int
    daily_limit: int
    requests_month: int
    monthly_limit: int

# ============================================================================
# Authentication
# ============================================================================

async def verify_api_key(api_key: str = Security(API_KEY_HEADER), db = Depends(get_db)):
    """Verify API key and check rate limits"""
    # SKIP AUTH IF NO KEY PROVIDED FOR DEVELOPMENT/DEMO PURPOSES IF needed
    # BUT logic below enforces it.
    
    if not api_key:
        # Check if we want to allow public access for now? 
        # For now, let's stick to the user's logic which requires it.
        # But we might want a "public" fallback or a default key for testing.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required"
        )
    
    # Hash the API key
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    
    cursor = db.cursor()
    cursor.execute("""
        SELECT id, tier, requests_today, daily_limit, requests_month, monthly_limit, is_active
        FROM api_keys
        WHERE key_hash = %s
    """, (key_hash,))
    
    key_info = cursor.fetchone()
    
    if not key_info or not key_info['is_active']:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )
    
    # Check rate limits
    if key_info['requests_today'] >= key_info['daily_limit']:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Daily rate limit exceeded"
        )
    
    if key_info['requests_month'] >= key_info['monthly_limit']:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Monthly rate limit exceeded"
        )
    
    # Increment counters
    cursor.execute("""
        UPDATE api_keys 
        SET requests_today = requests_today + 1,
            requests_month = requests_month + 1,
            last_used_at = NOW()
        WHERE id = %s
    """, (key_info['id'],))
    db.commit()
    
    return key_info

# ============================================================================
# FastAPI App
# ============================================================================

app = FastAPI(
    title="ZSE Market Data API",
    description="Zimbabwe Stock Exchange market data API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# Health Check
# ============================================================================

@app.get("/", tags=["System"])
async def root():
    """API health check"""
    return {
        "status": "online",
        "api": "ZSE Market Data API",
        "version": "1.0.0",
        "docs": "/docs"
    }

@app.get("/health", tags=["System"])
async def health_check(db = Depends(get_db)):
    """Detailed health check"""
    try:
        cursor = db.cursor()
        cursor.execute("SELECT 1")
        db_status = "connected"
    except:
        db_status = "disconnected"
    
    return {
        "status": "healthy",
        "database": db_status,
        "timestamp": datetime.now().isoformat()
    }

# ============================================================================
# Securities Endpoints
# ============================================================================

@app.get("/api/v1/securities", response_model=List[SecurityModel], tags=["Securities"])
async def list_securities(
    security_type: Optional[str] = None,
    sector: Optional[str] = None,
    active_only: bool = True,
    api_key_info = Depends(verify_api_key),
    db = Depends(get_db)
):
    """
    Get list of all securities
    
    - **security_type**: Filter by type (equity, etf, reit)
    - **sector**: Filter by sector
    - **active_only**: Only return actively traded securities
    """
    cursor = db.cursor()
    
    query = "SELECT * FROM securities WHERE 1=1"
    params = []
    
    if active_only:
        query += " AND is_active = true"
    
    if security_type:
        query += " AND security_type = %s"
        params.append(security_type)
    
    if sector:
        query += " AND sector = %s"
        params.append(sector)
    
    query += " ORDER BY symbol"
    
    cursor.execute(query, params)
    securities = cursor.fetchall()
    
    return securities

@app.get("/api/v1/securities/{symbol}", response_model=SecurityModel, tags=["Securities"])
async def get_security(
    symbol: str,
    api_key_info = Depends(verify_api_key),
    db = Depends(get_db)
):
    """Get details for a specific security"""
    cursor = db.cursor()
    cursor.execute("SELECT * FROM securities WHERE symbol = %s", (symbol.upper(),))
    security = cursor.fetchone()
    
    if not security:
        raise HTTPException(status_code=404, detail="Security not found")
    
    return security

# ============================================================================
# Price Endpoints
# ============================================================================

@app.get("/api/v1/securities/{symbol}/prices", response_model=List[DailyPrice], tags=["Prices"])
async def get_security_prices(
    symbol: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = 30,
    api_key_info = Depends(verify_api_key),
    db = Depends(get_db)
):
    """
    Get price history for a security
    
    - **symbol**: Security symbol (e.g., DELTA, ECO)
    - **start_date**: Filter from date (YYYY-MM-DD)
    - **end_date**: Filter to date (YYYY-MM-DD)
    - **limit**: Maximum number of records (default: 30)
    """
    cursor = db.cursor()
    
    query = """
        SELECT 
            s.symbol,
            s.currency,
            dp.trade_date,
            dp.price,
            dp.change_pct,
            dp.market_cap,
            dp.volume,
            dp.trades_count
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
    
    cursor.execute(query, params)
    prices = cursor.fetchall()
    
    if not prices:
        raise HTTPException(status_code=404, detail="No price data found")
    
    return prices

@app.get("/api/v1/securities/{symbol}/latest", response_model=DailyPrice, tags=["Prices"])
async def get_latest_price(
    symbol: str,
    api_key_info = Depends(verify_api_key),
    db = Depends(get_db)
):
    """Get latest price for a security"""
    cursor = db.cursor()
    
    cursor.execute("""
        SELECT 
            s.symbol,
            s.currency,
            dp.trade_date,
            dp.price,
            dp.change_pct,
            dp.market_cap,
            dp.volume,
            dp.trades_count
        FROM daily_prices dp
        JOIN securities s ON s.id = dp.security_id
        WHERE s.symbol = %s
        ORDER BY dp.trade_date DESC
        LIMIT 1
    """, (symbol.upper(),))
    
    price = cursor.fetchone()
    
    if not price:
        raise HTTPException(status_code=404, detail="No price data found")
    
    return price

# ============================================================================
# Market Summary Endpoints
# ============================================================================

@app.get("/api/v1/market/summary", response_model=MarketSummary, tags=["Market"])
async def get_market_summary(
    trade_date: Optional[date] = None,
    api_key_info = Depends(verify_api_key),
    db = Depends(get_db)
):
    """
    Get market-wide summary statistics
    
    - **trade_date**: Specific date (defaults to latest)
    """
    cursor = db.cursor()
    
    if trade_date:
        cursor.execute("SELECT * FROM v_market_summary WHERE trade_date = %s", (trade_date,))
    else:
        cursor.execute("SELECT * FROM v_market_summary LIMIT 1")
    
    summary = cursor.fetchone()
    
    if not summary:
        raise HTTPException(status_code=404, detail="No market data found")
    
    return summary

@app.get("/api/v1/market/movers", response_model=List[TopMover], tags=["Market"])
async def get_top_movers(
    type: str = "both",  # 'gainers', 'losers', 'both'
    limit: int = 5,
    api_key_info = Depends(verify_api_key),
    db = Depends(get_db)
):
    """
    Get top gaining/losing securities
    
    - **type**: 'gainers', 'losers', or 'both'
    - **limit**: Number of securities to return
    """
    cursor = db.cursor()
    results = []
    
    if type in ['gainers', 'both']:
        cursor.execute("""
            SELECT 
                s.symbol,
                dp.price,
                dp.change_pct,
                'gainer' as movement_type
            FROM daily_prices dp
            JOIN securities s ON s.id = dp.security_id
            WHERE dp.trade_date = (SELECT MAX(trade_date) FROM daily_prices)
              AND dp.change_pct > 0
            ORDER BY dp.change_pct DESC
            LIMIT %s
        """, (limit,))
        results.extend(cursor.fetchall())
    
    if type in ['losers', 'both']:
        cursor.execute("""
            SELECT 
                s.symbol,
                dp.price,
                dp.change_pct,
                'loser' as movement_type
            FROM daily_prices dp
            JOIN securities s ON s.id = dp.security_id
            WHERE dp.trade_date = (SELECT MAX(trade_date) FROM daily_prices)
              AND dp.change_pct < 0
            ORDER BY dp.change_pct ASC
            LIMIT %s
        """, (limit,))
        results.extend(cursor.fetchall())
    
    return results

@app.get("/api/v1/market/indices", response_model=List[MarketIndex], tags=["Market"])
async def get_market_indices(
    index_type: Optional[str] = None,
    trade_date: Optional[date] = None,
    api_key_info = Depends(verify_api_key),
    db = Depends(get_db)
):
    """
    Get market indices
    
    - **index_type**: 'market_cap', 'sector', or None for all
    - **trade_date**: Specific date (defaults to latest)
    """
    cursor = db.cursor()
    
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
    
    cursor.execute(query, params)
    indices = cursor.fetchall()
    
    return indices

# ============================================================================
# API Key Management
# ============================================================================

@app.get("/api/v1/account/usage", response_model=APIKeyInfo, tags=["Account"])
async def get_api_usage(api_key_info = Depends(verify_api_key)):
    """Get current API key usage statistics"""
    return {
        "tier": api_key_info['tier'],
        "requests_today": api_key_info['requests_today'],
        "daily_limit": api_key_info['daily_limit'],
        "requests_month": api_key_info['requests_month'],
        "monthly_limit": api_key_info['monthly_limit']
    }

# ============================================================================
# Run Server
# ============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
