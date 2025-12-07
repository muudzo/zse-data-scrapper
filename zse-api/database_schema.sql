-- ZSE Market Data API - Database Schema
-- PostgreSQL

-- Core securities reference table
CREATE TABLE IF NOT EXISTS securities (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) UNIQUE NOT NULL,  -- e.g. "DELTA", "ECO"
    name VARCHAR(255),                    -- Full company name (to be enriched)
    security_type VARCHAR(20) DEFAULT 'equity', -- 'equity', 'etf', 'reit'
    sector VARCHAR(100),                  -- To be enriched from ZSE listings
    currency VARCHAR(3) DEFAULT 'ZWG',    -- ZWG or USD
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Daily price snapshots (captured from homepage)
CREATE TABLE IF NOT EXISTS daily_prices (
    id SERIAL PRIMARY KEY,
    security_id INTEGER REFERENCES securities(id) ON DELETE CASCADE,
    trade_date DATE NOT NULL,
    price DECIMAL(15,4),                  -- Latest price in cents
    change_pct DECIMAL(8,4),              -- Daily % change
    market_cap DECIMAL(20,2),             -- If available (ETFs/REITs)
    
    -- Additional fields for when we get OHLCV data later
    open_price DECIMAL(15,4),
    high_price DECIMAL(15,4),
    low_price DECIMAL(15,4),
    close_price DECIMAL(15,4),
    volume BIGINT,
    trades_count INTEGER,
    
    data_source VARCHAR(50) DEFAULT 'homepage_scrape',
    created_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(security_id, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON daily_prices(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_prices_security ON daily_prices(security_id, trade_date DESC);

-- Market-wide indices (All Share, Top 10, etc.)
CREATE TABLE IF NOT EXISTS market_indices (
    id SERIAL PRIMARY KEY,
    index_name VARCHAR(100) NOT NULL,
    index_type VARCHAR(50),               -- 'market_cap', 'sector', 'custom'
    trade_date DATE NOT NULL,
    index_value DECIMAL(15,4),
    change_pct DECIMAL(8,4),
    created_at TIMESTAMP DEFAULT NOW(),
    
    UNIQUE(index_name, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_market_indices_date ON market_indices(trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_market_indices_name ON market_indices(index_name, trade_date DESC);

-- Daily market summary/activity
CREATE TABLE IF NOT EXISTS market_snapshots (
    id SERIAL PRIMARY KEY,
    trade_date DATE NOT NULL UNIQUE,
    total_trades INTEGER,
    total_turnover DECIMAL(20,2),         -- ZWG
    market_cap DECIMAL(25,2),             -- Total market cap
    foreign_purchases DECIMAL(20,2),
    foreign_sales DECIMAL(20,2),
    advances INTEGER,                     -- Count of gainers
    declines INTEGER,                     -- Count of losers
    unchanged INTEGER,
    data_source VARCHAR(50) DEFAULT 'homepage_scrape',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_snapshots_date ON market_snapshots(trade_date DESC);

-- Scrape audit log
CREATE TABLE IF NOT EXISTS scrape_logs (
    id SERIAL PRIMARY KEY,
    scrape_timestamp TIMESTAMP DEFAULT NOW(),
    status VARCHAR(20),                   -- 'success', 'failed', 'partial'
    source_url TEXT,
    records_parsed INTEGER,
    error_message TEXT,
    execution_time_ms INTEGER,
    raw_snapshot JSONB                    -- Store raw JSON for debugging
);

CREATE INDEX IF NOT EXISTS idx_scrape_logs_timestamp ON scrape_logs(scrape_timestamp DESC);

-- API keys for authentication
CREATE TABLE IF NOT EXISTS api_keys (
    id SERIAL PRIMARY KEY,
    key_hash VARCHAR(128) UNIQUE NOT NULL, -- Hashed API key
    key_prefix VARCHAR(8),                 -- First 8 chars for identification
    user_email VARCHAR(255),
    tier VARCHAR(20) DEFAULT 'free',       -- 'free', 'pro', 'enterprise'
    requests_today INTEGER DEFAULT 0,
    requests_month INTEGER DEFAULT 0,
    daily_limit INTEGER DEFAULT 100,
    monthly_limit INTEGER DEFAULT 5000,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT NOW(),
    last_used_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_email ON api_keys(user_email);

-- API request logs (for analytics)
CREATE TABLE IF NOT EXISTS api_requests (
    id SERIAL PRIMARY KEY,
    api_key_id INTEGER REFERENCES api_keys(id),
    endpoint VARCHAR(255),
    method VARCHAR(10),
    status_code INTEGER,
    response_time_ms INTEGER,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_requests_timestamp ON api_requests(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_api_requests_key ON api_requests(api_key_id, created_at DESC);

-- View: Latest prices for all securities
CREATE OR REPLACE VIEW v_latest_prices AS
SELECT 
    s.symbol,
    s.name,
    s.security_type,
    s.sector,
    s.currency,
    dp.trade_date,
    dp.price,
    dp.change_pct,
    dp.market_cap,
    dp.volume,
    dp.trades_count
FROM securities s
LEFT JOIN LATERAL (
    SELECT *
    FROM daily_prices
    WHERE security_id = s.id
    ORDER BY trade_date DESC
    LIMIT 1
) dp ON true
WHERE s.is_active = true;

-- View: Market summary for today
CREATE OR REPLACE VIEW v_market_summary AS
SELECT 
    ms.*,
    (SELECT COUNT(*) FROM daily_prices dp 
     WHERE dp.trade_date = ms.trade_date AND dp.change_pct > 0) as gainers_count,
    (SELECT COUNT(*) FROM daily_prices dp 
     WHERE dp.trade_date = ms.trade_date AND dp.change_pct < 0) as losers_count
FROM market_snapshots ms
ORDER BY trade_date DESC
LIMIT 1;

-- Function: Get price history for a security
CREATE OR REPLACE FUNCTION get_price_history(
    p_symbol VARCHAR,
    p_days INTEGER DEFAULT 30
)
RETURNS TABLE (
    trade_date DATE,
    price DECIMAL,
    change_pct DECIMAL,
    volume BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        dp.trade_date,
        dp.price,
        dp.change_pct,
        dp.volume
    FROM daily_prices dp
    JOIN securities s ON s.id = dp.security_id
    WHERE s.symbol = p_symbol
    ORDER BY dp.trade_date DESC
    LIMIT p_days;
END;
$$ LANGUAGE plpgsql;
