from fastapi import APIRouter, HTTPException, Depends
from typing import List, Dict, Any
import asyncpg
from app.database import pool

router = APIRouter()

@router.get("/health")
async def health_check():
    """System health check endpoint."""
    if not pool:
        return {"status": "error", "database": "disconnected"}
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {
            "status": "healthy",
            "pipeline": {
                "kafka": "CONNECTED",
                "flink": "PROCESSING",
                "timescaledb": "CONNECTED",
                "websocket": "ONLINE"
            }
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

@router.get("/market/latest")
async def get_latest_market_signals() -> List[Dict[str, Any]]:
    """Returns the latest state for all symbols."""
    if not pool:
        raise HTTPException(status_code=503, detail="Database pool not initialized")

    query = """
        SELECT DISTINCT ON (symbol) 
            time, symbol, price, return, volatility, volume_ratio, movement, signal, signal_score
        FROM processed_market_signals
        ORDER BY symbol, time DESC;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query)
        result = []
        for r in rows:
            result.append({
                "timestamp": r["time"].isoformat() if r["time"] else None,
                "symbol": r["symbol"],
                "price": float(r["price"]),
                "return": float(r["return"]),
                "volatility": float(r["volatility"]),
                "volume_ratio": float(r["volume_ratio"]),
                "movement": r["movement"],
                "signal": r["signal"],
                "signal_score": int(r["signal_score"])
            })
        return result

@router.get("/market/state")
async def get_market_state() -> Dict[str, Any]:
    """Returns the current aggregated market state and confidence score."""
    if not pool:
        raise HTTPException(status_code=503, detail="Database pool not initialized")

    query = """
        SELECT time, bullish_count, neutral_count, bearish_count, overall_signal, confidence
        FROM market_state
        ORDER BY time DESC
        LIMIT 1;
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(query)
        if not row:
            return {
                "overall_signal": "NEUTRAL",
                "confidence": 0.0,
                "bullish_count": 0,
                "neutral_count": 0,
                "bearish_count": 0,
                "timestamp": None
            }
        return {
            "timestamp": row["time"].isoformat() if row["time"] else None,
            "overall_signal": row["overall_signal"],
            "confidence": float(row["confidence"]),
            "bullish_count": int(row["bullish_count"]),
            "neutral_count": int(row["neutral_count"]),
            "bearish_count": int(row["bearish_count"])
        }

@router.get("/market/{symbol}")
async def get_symbol_history(symbol: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Returns recent processed observations for a specific symbol."""
    if not pool:
        raise HTTPException(status_code=503, detail="Database pool not initialized")

    query = """
        SELECT time, symbol, price, return, volatility, volume_ratio, movement, signal, signal_score
        FROM processed_market_signals
        WHERE UPPER(symbol) = UPPER($1)
        ORDER BY time DESC
        LIMIT $2;
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(query, symbol, limit)
        # Reverse so records are chronological ASC for chart display
        result = []
        for r in reversed(rows):
            result.append({
                "timestamp": r["time"].isoformat() if r["time"] else None,
                "symbol": r["symbol"],
                "price": float(r["price"]),
                "return": float(r["return"]),
                "volatility": float(r["volatility"]),
                "volume_ratio": float(r["volume_ratio"]),
                "movement": r["movement"],
                "signal": r["signal"],
                "signal_score": int(r["signal_score"])
            })
        return result
