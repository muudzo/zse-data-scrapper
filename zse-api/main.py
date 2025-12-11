"""
ZSE Market Data API
FastAPI application for Zimbabwe Stock Exchange data
"""

from fastapi import FastAPI, HTTPException, Depends, Security, status
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import date, datetime
from decimal import Decimal
import hashlib

# Import Repositories
from repository import (
    SecurityRepository,
    PriceRepository,
    MarketRepository,
    ApiKeyRepository
)

# ============================================================================
# Configuration
# ============================================================================

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

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

async def verify_api_key(api_key: str = Security(API_KEY_HEADER)):
    """Verify API key and check rate limits"""
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key required"
        )
    
    # Hash the API key
    key_hash = hashlib.sha256(api_key.encode()).hexdigest()
    
    key_info = ApiKeyRepository.get_by_hash(key_hash)
    
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
    ApiKeyRepository.increment_usage(key_info['id'])
    
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
async def health_check():
    """Detailed health check"""
    try:
        # Simple DB check
        SecurityRepository.list_all(active_only=True)
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
    api_key_info = Depends(verify_api_key)
):
    """Get list of all securities"""
    return SecurityRepository.list_all(active_only, security_type, sector)

@app.get("/api/v1/securities/{symbol}", response_model=SecurityModel, tags=["Securities"])
async def get_security(
    symbol: str,
    api_key_info = Depends(verify_api_key)
):
    """Get details for a specific security"""
    security = SecurityRepository.get_by_symbol(symbol)
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
    api_key_info = Depends(verify_api_key)
):
    """Get price history for a security"""
    prices = PriceRepository.get_history(symbol, start_date, end_date, limit)
    if not prices:
        raise HTTPException(status_code=404, detail="No price data found")
    return prices

@app.get("/api/v1/securities/{symbol}/latest", response_model=DailyPrice, tags=["Prices"])
async def get_latest_price(
    symbol: str,
    api_key_info = Depends(verify_api_key)
):
    """Get latest price for a security"""
    price = PriceRepository.get_latest(symbol)
    if not price:
        raise HTTPException(status_code=404, detail="No price data found")
    return price

# ============================================================================
# Market Summary Endpoints
# ============================================================================

@app.get("/api/v1/market/summary", response_model=MarketSummary, tags=["Market"])
async def get_market_summary(
    trade_date: Optional[date] = None,
    api_key_info = Depends(verify_api_key)
):
    """Get market-wide summary statistics"""
    summary = MarketRepository.get_summary(trade_date)
    if not summary:
        raise HTTPException(status_code=404, detail="No market data found")
    return summary

@app.get("/api/v1/market/movers", response_model=List[TopMover], tags=["Market"])
async def get_top_movers(
    type: str = "both",  # 'gainers', 'losers', 'both'
    limit: int = 5,
    api_key_info = Depends(verify_api_key)
):
    """Get top gaining/losing securities"""
    results = []
    
    if type in ['gainers', 'both']:
        results.extend(PriceRepository.get_top_movers(limit, 'gainers'))
    
    if type in ['losers', 'both']:
        results.extend(PriceRepository.get_top_movers(limit, 'losers'))
    
    return results

@app.get("/api/v1/market/indices", response_model=List[MarketIndex], tags=["Market"])
async def get_market_indices(
    index_type: Optional[str] = None,
    trade_date: Optional[date] = None,
    api_key_info = Depends(verify_api_key)
):
    """Get market indices"""
    return MarketRepository.list_indices(trade_date, index_type)

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
