-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Trigger to update updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Securities Table (Equities, ETFs, REITs)
CREATE TABLE IF NOT EXISTS securities (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    symbol VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255),
    sector VARCHAR(100),
    security_type VARCHAR(50), -- 'equity', 'etf', 'reit', 'index'
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TRIGGER update_securities_updated_at
    BEFORE UPDATE ON securities
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Daily Prices / Snapshots
CREATE TABLE IF NOT EXISTS prices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    security_id UUID REFERENCES securities(id) ON DELETE CASCADE,
    price DECIMAL(18, 2),
    change_pct DECIMAL(10, 2),
    market_cap DECIMAL(20, 2),
    trade_date DATE NOT NULL,
    captured_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(security_id, trade_date)
);

-- Market General Activity
CREATE TABLE IF NOT EXISTS market_activity (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    trade_date DATE UNIQUE NOT NULL,
    trades_count INTEGER,
    turnover DECIMAL(20, 2),
    market_cap DECIMAL(20, 2),
    foreign_purchases DECIMAL(20, 2),
    foreign_sales DECIMAL(20, 2),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
