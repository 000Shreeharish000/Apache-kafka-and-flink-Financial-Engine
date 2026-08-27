import asyncpg
import asyncio
from app.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

pool: asyncpg.Pool = None

async def init_db_pool():
    global pool
    retries = 30
    for i in range(retries):
        try:
            pool = await asyncpg.create_pool(
                host=DB_HOST,
                port=int(DB_PORT),
                user=DB_USER,
                password=DB_PASS,
                database=DB_NAME,
                min_size=2,
                max_size=10
            )
            print(f"[FASTAPI DB] Connected to TimescaleDB pool at {DB_HOST}:{DB_PORT}")
            return pool
        except Exception as e:
            print(f"[FASTAPI DB] Connection failed ({i+1}/{retries}). Retrying in 2s... Error: {e}")
            await asyncio.sleep(2)
    raise Exception("Failed to connect to TimescaleDB pool")

def get_pool():
    global pool
    return pool

async def close_db_pool():
    global pool
    if pool:
        await pool.close()
        print("[FASTAPI DB] Closed TimescaleDB connection pool")
