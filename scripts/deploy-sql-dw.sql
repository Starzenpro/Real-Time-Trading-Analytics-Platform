-- Trading Analytics Data Warehouse Schema

-- Create schemas
CREATE SCHEMA IF NOT EXISTS trading;
CREATE SCHEMA IF NOT EXISTS risk;
CREATE SCHEMA IF NOT EXISTS compliance;

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Date Dimension
CREATE TABLE trading.dim_date (
    date_sk INT IDENTITY(1,1) PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BIT DEFAULT 0,
    is_trading_day BIT DEFAULT 1,
    fiscal_year INT,
    fiscal_quarter INT,
    created_at DATETIME2 DEFAULT GETDATE()
);

-- Symbol Dimension
CREATE TABLE trading.dim_symbol (
    symbol_sk INT IDENTITY(1,1) PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    exchange VARCHAR(50),
    currency VARCHAR(10),
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);

-- Counterparty Dimension
CREATE TABLE trading.dim_counterparty (
    counterparty_sk INT IDENTITY(1,1) PRIMARY KEY,
    counterparty_id VARCHAR(50) UNIQUE,
    counterparty_name VARCHAR(255),
    counterparty_type VARCHAR(50), -- Bank, Hedge Fund, Pension Fund, etc.
    country VARCHAR(100),
    credit_rating VARCHAR(10),
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE()
);

-- Trader Dimension
CREATE TABLE trading.dim_trader (
    trader_sk INT IDENTITY(1,1) PRIMARY KEY,
    trader_id VARCHAR(50) UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    desk VARCHAR(100), -- Equities, Fixed Income, FX, etc.
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE()
);

-- Strategy Dimension
CREATE TABLE trading.dim_strategy (
    strategy_sk INT IDENTITY(1,1) PRIMARY KEY,
    strategy_id VARCHAR(50) UNIQUE,
    strategy_name VARCHAR(255),
    strategy_type VARCHAR(100), -- Momentum, Mean Reversion, Arbitrage, etc.
    risk_profile VARCHAR(50), -- Low, Medium, High
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE()
);

-- ============================================
-- FACT TABLES
-- ============================================

-- Fact Trades
CREATE TABLE trading.fact_trades (
    trade_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    trade_id VARCHAR(50) NOT NULL UNIQUE,
    date_sk INT NOT NULL REFERENCES trading.dim_date(date_sk),
    symbol_sk INT NOT NULL REFERENCES trading.dim_symbol(symbol_sk),
    counterparty_sk INT REFERENCES trading.dim_counterparty(counterparty_sk),
    trader_sk INT REFERENCES trading.dim_trader(trader_sk),
    strategy_sk INT REFERENCES trading.dim_strategy(strategy_sk),
    
    -- Trade Details
    side VARCHAR(10) NOT NULL, -- BUY/SELL
    quantity DECIMAL(18,4) NOT NULL,
    price DECIMAL(18,4) NOT NULL,
    notional_value DECIMAL(18,2) NOT NULL,
    commission DECIMAL(18,2) DEFAULT 0,
    fees DECIMAL(18,2) DEFAULT 0,
    net_value DECIMAL(18,2) NOT NULL,
    
    -- P&L
    realized_pnl DECIMAL(18,2) DEFAULT 0,
    unrealized_pnl DECIMAL(18,2) DEFAULT 0,
    total_pnl AS (realized_pnl + unrealized_pnl) PERSISTED,
    
    -- Metadata
    trade_timestamp DATETIME2 NOT NULL,
    execution_timestamp DATETIME2 NOT NULL,
    created_at DATETIME2 DEFAULT GETDATE(),
    
    INDEX idx_trades_date (date_sk),
    INDEX idx_trades_symbol (symbol_sk),
    INDEX idx_trades_trader (trader_sk)
);

-- Fact Market Data
CREATE TABLE trading.fact_market_data (
    market_data_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_sk INT NOT NULL REFERENCES trading.dim_date(date_sk),
    symbol_sk INT NOT NULL REFERENCES trading.dim_symbol(symbol_sk),
    
    -- OHLCV
    open_price DECIMAL(18,4) NOT NULL,
    high_price DECIMAL(18,4) NOT NULL,
    low_price DECIMAL(18,4) NOT NULL,
    close_price DECIMAL(18,4) NOT NULL,
    volume BIGINT NOT NULL,
    vwap DECIMAL(18,4),
    
    -- Bid/Ask
    bid_price DECIMAL(18,4),
    ask_price DECIMAL(18,4),
    spread DECIMAL(18,4),
    
    -- Metadata
    timestamp DATETIME2 NOT NULL,
    source VARCHAR(50), -- Exchange, Broker, etc.
    created_at DATETIME2 DEFAULT GETDATE(),
    
    INDEX idx_market_date (date_sk),
    INDEX idx_market_symbol (symbol_sk)
);

-- Fact Positions
CREATE TABLE trading.fact_positions (
    position_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_sk INT NOT NULL REFERENCES trading.dim_date(date_sk),
    symbol_sk INT NOT NULL REFERENCES trading.dim_symbol(symbol_sk),
    trader_sk INT REFERENCES trading.dim_trader(trader_sk),
    
    -- Position Details
    quantity DECIMAL(18,4) NOT NULL,
    avg_entry_price DECIMAL(18,4) NOT NULL,
    current_price DECIMAL(18,4) NOT NULL,
    market_value DECIMAL(18,2) NOT NULL,
    unrealized_pnl DECIMAL(18,2) NOT NULL,
    realized_pnl DECIMAL(18,2) NOT NULL,
    
    -- Risk Metrics
    delta DECIMAL(18,4),
    gamma DECIMAL(18,4),
    theta DECIMAL(18,4),
    vega DECIMAL(18,4),
    rho DECIMAL(18,4),
    beta DECIMAL(18,4),
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    INDEX idx_positions_date (date_sk),
    INDEX idx_positions_symbol (symbol_sk)
);

-- ============================================
-- RISK TABLES
-- ============================================

-- Risk Metrics Daily
CREATE TABLE risk.fact_risk_daily (
    risk_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_sk INT NOT NULL REFERENCES trading.dim_date(date_sk),
    portfolio_id VARCHAR(100) NOT NULL,
    
    -- VaR Metrics
    var_95 DECIMAL(18,4),
    var_99 DECIMAL(18,4),
    expected_shortfall_95 DECIMAL(18,4),
    expected_shortfall_99 DECIMAL(18,4),
    
    -- Stress Testing
    stress_loss_2008 DECIMAL(18,4),
    stress_loss_covid DECIMAL(18,4),
    stress_loss_rate_hike DECIMAL(18,4),
    
    -- Portfolio Metrics
    total_exposure DECIMAL(18,2),
    net_exposure DECIMAL(18,2),
    gross_exposure DECIMAL(18,2),
    leverage_ratio DECIMAL(18,4),
    
    -- Greeks (for options)
    portfolio_delta DECIMAL(18,4),
    portfolio_gamma DECIMAL(18,4),
    portfolio_theta DECIMAL(18,4),
    portfolio_vega DECIMAL(18,4),
    portfolio_rho DECIMAL(18,4),
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    INDEX idx_risk_date (date_sk)
);

-- Scenario Analysis
CREATE TABLE risk.scenario_results (
    scenario_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    scenario_name VARCHAR(255) NOT NULL,
    run_date DATETIME2 NOT NULL,
    portfolio_id VARCHAR(100) NOT NULL,
    
    -- Scenario Parameters
    market_shock DECIMAL(18,4),
    vol_shock DECIMAL(18,4),
    rate_shock DECIMAL(18,4),
    
    -- Results
    pnl_impact DECIMAL(18,2),
    var_impact DECIMAL(18,4),
    
    created_at DATETIME2 DEFAULT GETDATE()
);

-- ============================================
-- COMPLIANCE TABLES
-- ============================================

-- Position Limits
CREATE TABLE compliance.position_limits (
    limit_sk INT IDENTITY(1,1) PRIMARY KEY,
    symbol_sk INT REFERENCES trading.dim_symbol(symbol_sk),
    trader_sk INT REFERENCES trading.dim_trader(trader_sk),
    
    limit_type VARCHAR(50), -- MAX_POSITION, MAX_NOTIONAL, etc.
    limit_value DECIMAL(18,2) NOT NULL,
    current_value DECIMAL(18,2),
    utilization_percentage AS (current_value / limit_value * 100) PERSISTED,
    
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);

-- Compliance Breaches
CREATE TABLE compliance.breaches (
    breach_sk BIGINT IDENTITY(1,1) PRIMARY KEY,
    breach_time DATETIME2 NOT NULL,
    breach_type VARCHAR(100) NOT NULL,
    
    -- Reference
    trader_sk INT REFERENCES trading.dim_trader(trader_sk),
    symbol_sk INT REFERENCES trading.dim_symbol(symbol_sk),
    
    -- Details
    limit_value DECIMAL(18,2),
    actual_value DECIMAL(18,2),
    excess_amount DECIMAL(18,2),
    
    severity VARCHAR(20), -- LOW, MEDIUM, HIGH, CRITICAL
    status VARCHAR(20), -- OPEN, INVESTIGATING, RESOLVED
    resolution_notes TEXT,
    
    created_at DATETIME2 DEFAULT GETDATE(),
    resolved_at DATETIME2
);

-- ============================================
-- AGGREGATION TABLES (for faster queries)
-- ============================================

-- Daily P&L by Trader
CREATE TABLE trading.agg_daily_pnl_trader (
    agg_date DATE NOT NULL,
    trader_sk INT NOT NULL REFERENCES trading.dim_trader(trader_sk),
    
    total_trades INT NOT NULL,
    total_volume DECIMAL(18,2) NOT NULL,
    realized_pnl DECIMAL(18,2) NOT NULL,
    unrealized_pnl DECIMAL(18,2) NOT NULL,
    total_pnl DECIMAL(18,2) NOT NULL,
    commission_total DECIMAL(18,2) NOT NULL,
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    PRIMARY KEY (agg_date, trader_sk)
);

-- Monthly Performance by Strategy
CREATE TABLE trading.agg_monthly_strategy (
    year INT NOT NULL,
    month INT NOT NULL,
    strategy_sk INT NOT NULL REFERENCES trading.dim_strategy(strategy_sk),
    
    total_trades INT NOT NULL,
    total_volume DECIMAL(18,2) NOT NULL,
    total_pnl DECIMAL(18,2) NOT NULL,
    sharpe_ratio DECIMAL(18,4),
    win_rate DECIMAL(5,2),
    avg_trade_size DECIMAL(18,2),
    
    created_at DATETIME2 DEFAULT GETDATE(),
    
    PRIMARY KEY (year, month, strategy_sk)
);

-- ============================================
-- STORED PROCEDURES
-- ============================================

-- Calculate Daily P&L
CREATE PROCEDURE trading.sp_calculate_daily_pnl
    @trade_date DATE
AS
BEGIN
    -- Aggregate daily P&L by trader
    INSERT INTO trading.agg_daily_pnl_trader (
        agg_date, trader_sk, total_trades, total_volume, 
        realized_pnl, unrealized_pnl, total_pnl, commission_total
    )
    SELECT 
        @trade_date,
        trader_sk,
        COUNT(*) as total_trades,
        SUM(notional_value) as total_volume,
        SUM(realized_pnl) as realized_pnl,
        SUM(unrealized_pnl) as unrealized_pnl,
        SUM(realized_pnl + unrealized_pnl) as total_pnl,
        SUM(commission) as commission_total
    FROM trading.fact_trades
    WHERE CAST(trade_timestamp AS DATE) = @trade_date
    GROUP BY trader_sk;
    
    -- Return summary
    SELECT 
        COUNT(DISTINCT trader_sk) as active_traders,
        SUM(total_trades) as total_trades,
        SUM(total_pnl) as total_pnl
    FROM trading.agg_daily_pnl_trader
    WHERE agg_date = @trade_date;
END;

-- Calculate VaR
CREATE PROCEDURE risk.sp_calculate_var
    @portfolio_id VARCHAR(100),
    @confidence_level DECIMAL(5,4) = 0.95
AS
BEGIN
    -- Calculate historical VaR
    WITH daily_returns AS (
        SELECT 
            date_sk,
            SUM(total_pnl) as daily_pnl
        FROM trading.fact_trades ft
        JOIN trading.dim_date dd ON ft.date_sk = dd.date_sk
        GROUP BY date_sk
    )
    SELECT 
        @portfolio_id as portfolio_id,
        @confidence_level as confidence_level,
        PERCENTILE_CONT(1 - @confidence_level) 
            WITHIN GROUP (ORDER BY daily_pnl) OVER() as var_amount
    FROM daily_returns;
END;
