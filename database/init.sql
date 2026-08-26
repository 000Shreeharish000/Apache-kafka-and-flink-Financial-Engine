-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- 1. Raw Market Ticks Table
CREATE TABLE IF NOT EXISTS market_ticks (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    volume INT NOT NULL,
    bid DOUBLE PRECISION NOT NULL,
    ask DOUBLE PRECISION NOT NULL
);

-- Convert market_ticks to Hypertable
SELECT create_hypertable('market_ticks', 'time', if_not_exists => TRUE);

-- Index for fast symbol lookups
CREATE INDEX IF NOT EXISTS idx_market_ticks_symbol_time ON market_ticks (symbol, time DESC);

-- 2. Processed Market Signals Table
CREATE TABLE IF NOT EXISTS processed_market_signals (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    return DOUBLE PRECISION NOT NULL,
    volatility DOUBLE PRECISION NOT NULL,
    volume_ratio DOUBLE PRECISION NOT NULL,
    movement VARCHAR(20) NOT NULL,
    signal VARCHAR(20) NOT NULL,
    signal_score INT NOT NULL
);

-- Convert processed_market_signals to Hypertable
SELECT create_hypertable('processed_market_signals', 'time', if_not_exists => TRUE);

-- Index for fast symbol lookups & chart rendering
CREATE INDEX IF NOT EXISTS idx_processed_signals_symbol_time ON processed_market_signals (symbol, time DESC);

-- 3. Market State Table (Aggregated overall market health)
CREATE TABLE IF NOT EXISTS market_state (
    time TIMESTAMPTZ NOT NULL,
    bullish_count INT NOT NULL,
    neutral_count INT NOT NULL,
    bearish_count INT NOT NULL,
    overall_signal VARCHAR(20) NOT NULL,
    confidence DOUBLE PRECISION NOT NULL
);

-- Convert market_state to Hypertable
SELECT create_hypertable('market_state', 'time', if_not_exists => TRUE);

-- Index for state retrieval
CREATE INDEX IF NOT EXISTS idx_market_state_time ON market_state (time DESC);
