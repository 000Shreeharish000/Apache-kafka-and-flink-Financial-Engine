# CEO Demonstration Guide: Real-Time Market Intelligence

Follow this step-by-step script to demonstrate the system to your CEO or technical leadership team.

---

## Step 1: Launch System with One Command
Open your terminal in the project directory and execute:

```bash
docker compose up --build
```

**What to explain:**
> *"We start the entire financial streaming infrastructure using Docker Compose. This single command provisions Kafka in KRaft mode, TimescaleDB, the market simulator producer, Flink stream engine, FastAPI backend, and the React frontend."*

---

## Step 2: Show Kafka Receiving Live Ticks
Open a secondary terminal window and run the Kafka Inspector:

```bash
python kafka/consumer/inspect.py
```

**What to explain:**
> *"Here we see raw market events being produced continuously for 5 symbols (AAPL, MSFT, NVDA, AMZN, GOOGL) into the Kafka topic `market_ticks`. Kafka acts as our durable event buffer, preventing any data loss."*

---

## Step 3: Show Flink Stream Processing Logs
Look at the `flink_stream_engine` container logs in your primary docker console:

```bash
docker logs -f flink_stream_engine
```

**What to explain:**
> *"Apache Flink consumes the raw Kafka stream and computes rolling returns, volatility, volume anomaly ratios, price movement classifications, and signal scores per event in real time."*

---

## Step 4: Show TimescaleDB Time-Series Tables
Connect to TimescaleDB to verify persistent hypertable records:

```bash
docker exec -it timescaledb_database psql -U postgres -d market_intelligence -c "SELECT time, symbol, price, return, volatility, signal, signal_score FROM processed_market_signals ORDER BY time DESC LIMIT 5;"
```

**What to explain:**
> *"TimescaleDB partitions our streaming analytics into hypertables. Flink writes the derived signals here for historical analysis and chart rendering."*

---

## Step 5: Open the React Dashboard
Navigate to [http://localhost:3000](http://localhost:3000) in your web browser.

**What to explain:**
> *"Notice that data updates continuously on screen without any manual page refreshes. The dashboard connects directly via WebSocket (`/ws/market`) to receive real-time market updates."*

---

## Step 6: Highlight Dashboard Visual Features
Point out key dashboard sections:
1. **Pipeline Status Bar**: `KAFKA -> FLINK -> TIMESCALEDB -> WEBSOCKET -> REACT DASHBOARD`.
2. **Events/sec Counter**: Displays real-time throughput.
3. **Aggregated Market State**: Shows overall state (`BULLISH`, `BEARISH`, `NEUTRAL`) with confidence score %.
4. **Interactive SVG Line Chart**: Updates price time-series dynamically when selecting a ticker.
5. **Live Stream Inspector**: Displays raw WebSocket frames arriving in real time.

---

## Step 7: Summary Explanation for Leadership
Conclude with the 30-second architecture elevator pitch:

> *"1. Market Simulator generates realistic tick events."*  
> *"2. Kafka transports and buffers events with zero data loss."*  
> *"3. Flink performs real-time windowed calculations and signal scoring."*  
> *"4. TimescaleDB persists time-series history."*  
> *"5. FastAPI pushes live signal updates to React via WebSockets with zero page refreshes."*  
