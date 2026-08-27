import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from app.api.endpoints import router as api_router
from app.database import init_db_pool, close_db_pool, get_pool
from app.websocket.manager import manager as ws_manager
from app.config import WEBSOCKET_BROADCAST_INTERVAL_MS

async def broadcast_loop():
    """Background task to fetch latest DB signals and push via WebSocket."""
    print("[WEBSOCKET BROADCASTER] Broadcaster loop started.")
    
    while True:
        try:
            await asyncio.sleep(WEBSOCKET_BROADCAST_INTERVAL_MS / 1000.0)

            pool = get_pool()
            if not ws_manager.active_connections or not pool:
                continue

            async with pool.acquire() as conn:
                # Fetch latest symbol signals
                latest_signals_rows = await conn.fetch("""
                    SELECT DISTINCT ON (symbol) 
                        time, symbol, price, return, volatility, volume_ratio, movement, signal, signal_score
                    FROM processed_market_signals
                    ORDER BY symbol, time DESC;
                """)

                latest_signals = [
                    {
                        "timestamp": r["time"].isoformat() if r["time"] else None,
                        "symbol": r["symbol"],
                        "price": float(r["price"]),
                        "return": float(r["return"]),
                        "volatility": float(r["volatility"]),
                        "volume_ratio": float(r["volume_ratio"]),
                        "movement": r["movement"],
                        "signal": r["signal"],
                        "signal_score": int(r["signal_score"])
                    }
                    for r in latest_signals_rows
                ]

                # Fetch overall market state
                state_row = await conn.fetchrow("""
                    SELECT time, bullish_count, neutral_count, bearish_count, overall_signal, confidence
                    FROM market_state
                    ORDER BY time DESC
                    LIMIT 1;
                """)

                market_state = {
                    "overall_signal": state_row["overall_signal"] if state_row else "NEUTRAL",
                    "confidence": float(state_row["confidence"]) if state_row else 0.0,
                    "bullish_count": int(state_row["bullish_count"]) if state_row else 0,
                    "neutral_count": int(state_row["neutral_count"]) if state_row else 0,
                    "bearish_count": int(state_row["bearish_count"]) if state_row else 0,
                    "timestamp": state_row["time"].isoformat() if (state_row and state_row["time"]) else None
                }

                payload = {
                    "type": "market_update",
                    "signals": latest_signals,
                    "state": market_state
                }

                await ws_manager.broadcast(payload)

        except Exception as e:
            print(f"[WEBSOCKET BROADCASTER] Broadcast loop exception: {e}")
            await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db_pool()
    task = asyncio.create_task(broadcast_loop())
    yield
    # Shutdown
    task.cancel()
    await close_db_pool()

app = FastAPI(
    title="Market Intelligence Streaming API",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api")

@app.websocket("/ws/market")
async def websocket_market_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await websocket.send_text(f'{{"type": "pong", "client_msg": "{data}"}}')
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception as e:
        print(f"[WEBSOCKET] Client error: {e}")
        ws_manager.disconnect(websocket)
