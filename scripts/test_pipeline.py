import sys
import time
import json
import urllib.request
import websockets
import asyncio

API_BASE = "http://localhost:8001/api"
WS_URL = "ws://localhost:8001/ws/market"

def log_test(step_num, title, status, details=""):
    symbol = "PASS" if status else "FAIL"
    print(f"[{symbol}] Step {step_num}: {title}")
    if details:
        print(f"       -> {details}")

async def test_health():
    try:
        req = urllib.request.Request(f"{API_BASE}/health")
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
            if data.get("status") == "healthy":
                log_test(1, "FastAPI & TimescaleDB Health Check", True, f"Response: {data}")
                return True
            else:
                log_test(1, "FastAPI & TimescaleDB Health Check", False, f"Response: {data}")
                return False
    except Exception as e:
        log_test(1, "FastAPI & TimescaleDB Health Check", False, str(e))
        return False

async def test_latest_signals():
    try:
        req = urllib.request.Request(f"{API_BASE}/market/latest")
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
            if isinstance(data, list) and len(data) > 0:
                symbols = [item["symbol"] for item in data]
                log_test(2, "Fetch Latest Market Signals (/api/market/latest)", True, f"Tracked Symbols: {symbols}")
                return True
            else:
                log_test(2, "Fetch Latest Market Signals (/api/market/latest)", False, "Empty response array")
                return False
    except Exception as e:
        log_test(2, "Fetch Latest Market Signals (/api/market/latest)", False, str(e))
        return False

async def test_market_state():
    try:
        req = urllib.request.Request(f"{API_BASE}/market/state")
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
            if "overall_signal" in data and "confidence" in data:
                log_test(3, "Fetch Market Aggregated State (/api/market/state)", True, f"State: {data['overall_signal']} | Confidence: {data['confidence']}%")
                return True
            else:
                log_test(3, "Fetch Market Aggregated State (/api/market/state)", False, f"Missing fields: {data}")
                return False
    except Exception as e:
        log_test(3, "Fetch Market Aggregated State (/api/market/state)", False, str(e))
        return False

async def test_symbol_history():
    try:
        req = urllib.request.Request(f"{API_BASE}/market/AAPL?limit=10")
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
            if isinstance(data, list) and len(data) > 0:
                log_test(4, "Fetch Symbol Time-Series History (/api/market/AAPL)", True, f"Retrieved {len(data)} records for AAPL")
                return True
            else:
                log_test(4, "Fetch Symbol Time-Series History (/api/market/AAPL)", False, "No historical records returned")
                return False
    except Exception as e:
        log_test(4, "Fetch Symbol Time-Series History (/api/market/AAPL)", False, str(e))
        return False

async def test_websocket_stream():
    try:
        async with websockets.connect(WS_URL) as websocket:
            message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
            data = json.loads(message)
            if data.get("type") == "market_update" and "signals" in data:
                log_test(5, "WebSocket Real-Time Broadcast (/ws/market)", True, f"Received payload with {len(data['signals'])} streaming ticks")
                return True
            else:
                log_test(5, "WebSocket Real-Time Broadcast (/ws/market)", False, f"Unexpected WS frame: {message[:100]}")
                return False
    except Exception as e:
        log_test(5, "WebSocket Real-Time Broadcast (/ws/market)", False, str(e))
        return False

async def run_all_tests():
    print("============================================================")
    print("      END-TO-END PIPELINE VERIFICATION SUITE               ")
    print("============================================================")
    
    h_ok = await test_health()
    s_ok = await test_latest_signals()
    m_ok = await test_market_state()
    h_sym = await test_symbol_history()
    ws_ok = await test_websocket_stream()

    print("============================================================")
    if all([h_ok, s_ok, m_ok, h_sym, ws_ok]):
        print("RESULT: ALL PIPELINE STAGES PASSED SUCCESSFULLY! [100% OK]")
    else:
        print("RESULT: SOME PIPELINE STAGES FAILED OR ARE PENDING INITIALIZATION.")
    print("============================================================")

if __name__ == "__main__":
    asyncio.run(run_all_tests())
