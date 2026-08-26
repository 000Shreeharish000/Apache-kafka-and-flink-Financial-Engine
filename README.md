# Real-Time Market Intelligence Streaming Dashboard

A prototype real-time financial market state streaming architecture built with **Apache Kafka**, **Apache Flink**, **TimescaleDB**, **FastAPI**, **WebSockets**, and a **React Dashboard**.

Inspired by Finflock / InstaStocks market-state engines, this system demonstrates end-to-end event-driven stream processing without page refreshes.

---

## 1. Architecture Diagram

```
                    Market Simulator (Python)
                           |
                           v
                     Kafka Producer
                           |
                           v
                    Kafka Topic
                     market_ticks
                           |
                           v
                    Apache Flink
                           |
             +-------------+-------------+
             |             |             |
             v             v             v
        Rolling Return  Volatility   Volume Anomaly
             |             |             |
             +-------------+-------------+
                           |
                           v
                    Signal Aggregator
                           |
                           v
                     TimescaleDB
                           |
                           v
                       FastAPI
                           |
                       WebSocket (/ws/market)
                           |
                           v
                      React UI
```

---

## 2. Technology Comparison Matrix

| Technology | Responsibility | Why We Use It |
| :--- | :--- | :--- |
| **Apache Kafka** | Distributed Event Streaming Buffer | Ingests continuous high-velocity market ticks with low latency, decouples producers from consumers, and guarantees partition ordering and fault tolerance. |
| **Apache Flink** | Stateful Stream Processing Engine | Performs real-time windowed calculations (Rolling Return, Volatility, Volume Anomaly) and rule-based signal classification on event streams in memory. |
| **TimescaleDB** | Time-Series Database Storage | Provides automatic hypertable chunk partitioning and fast time-series queries for historical chart lookups and persistence. |
| **FastAPI** | REST & WebSocket Backend | Serves structured REST APIs and streams live signal frames asynchronously to connected frontend clients via WebSockets. |
| **React Dashboard** | Monochromatic Full-Page Terminal UI | Displays continuous live streaming metrics, event rate, interactive price charts, and pipeline health indicators without manual page refresh. |

---

## 3. Educational Q&A: Architecture & Concepts

### 1. What problem are we solving?
Real-time financial trading systems require sub-second state detection across multiple volatile assets. Traditional request-response database polling cannot handle high-frequency market events without suffering from latency, database lock contention, and missed market signals.

### 2. Why Kafka between Producer and Flink?
Kafka acts as an enterprise-grade message broker buffer. If the stream processing engine (Flink) slows down or restarts, Kafka safely holds all incoming market ticks on disk without dropping a single event.

### 3. Why Flink instead of simple Python scripts?
Apache Flink provides out-of-the-box stateful stream processing, event-time windowing, exactly-once processing guarantees, and horizontal scalability across worker slots.

### 4. Why not Python directly to PostgreSQL?
Direct database writes per tick at thousands of ticks/sec degrade Postgres performance and risk locking write transactions. Furthermore, calculating sliding window volatility across millions of raw database rows is prohibitively expensive.

### 5. What exactly does Kafka do?
Kafka manages topics (`market_ticks`), partitions, and consumer groups (`flink-stream-processing-group`). It ensures high-throughput, ordered log append semantics for ticks across stock symbols (`AAPL`, `MSFT`, `NVDA`, `AMZN`, `GOOGL`).

### 6. What exactly does Flink do?
Flink consumes `market_ticks` and applies stream transformations:
1. **Rolling Return**: `(current_price - prev_price) / prev_price`
2. **Rolling Volatility**: Standard deviation over sliding window of recent returns.
3. **Volume Anomaly**: `current_volume / rolling_avg_volume` (`NORMAL` vs `HIGH_VOLUME`).
4. **Movement Classification**: `STRONG_UP`, `UP`, `FLAT`, `DOWN`, `STRONG_DOWN`.
5. **Rule-Based Signal Engine**: Evaluates transparent scores mapping to `BULLISH` (score >= +1), `BEARISH` (score <= -1), or `NEUTRAL`.

### 7. What exactly does TimescaleDB do?
TimescaleDB extends PostgreSQL with Hypertables—automatically partitioning time-series data into time chunks. It stores raw events (`market_ticks`), derived analytics (`processed_market_signals`), and global status (`market_state`).

### 8. Why is TimescaleDB useful if Flink is processing the stream?
Flink keeps transient window state in memory for streaming push. TimescaleDB provides persistent historical storage so new clients, compliance reporting, and REST endpoints can query historical timelines.

### 9. What happens when Flink receives 1,000 events/sec?
Flink key-partitions streams by symbol (`keyBy(symbol)`). Workers process events concurrently across TaskManager slots in memory, seamlessly handling high throughput.

### 10. What happens if the database temporarily goes down?
Kafka stores unconsumed events in topic partitions. Flink maintains internal state checkpoints and retries database sink writes until TimescaleDB recovers.

### 11. What happens if the frontend disconnects?
The FastAPI WebSocket manager detects socket teardown and removes the connection. The Kafka producer, Flink processor, and TimescaleDB continue streaming uninterrupted.

### 12. Where does state live?
- **Stream Window State**: Managed in memory by Flink.
- **Historical Persistent State**: Stored in TimescaleDB hypertables.
- **Client Render State**: Stored in React component state.

### 13. Where are calculations performed?
All windowed mathematical calculations and signal classifications take place strictly in Apache Flink.

### 14. Which parts are batch vs stream processing?
- **Stream Processing**: Event generation $\rightarrow$ Kafka $\rightarrow$ Flink $\rightarrow$ TimescaleDB insert $\rightarrow$ WebSocket push.
- **Batch Querying**: Historical REST chart queries (`/api/market/{symbol}`).

---

## 4. Quick Start: Running the System

Start the entire system locally with one Docker Compose command:

```bash
docker compose up --build
```

### Access Endpoints:
- **React Dashboard**: [http://localhost:3000](http://localhost:3000)
- **FastAPI REST Documentation**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **FastAPI Health Endpoint**: [http://localhost:8000/api/health](http://localhost:8000/api/health)
- **WebSocket Endpoint**: `ws://localhost:8000/ws/market`
- **TimescaleDB**: `localhost:5432` (`user: postgres`, `pass: postgres`, `db: market_intelligence`)

---

## 5. Manual Kafka Debugger

Inspect raw Kafka messages produced by the simulator before stream processing:

```bash
python kafka/consumer/inspect.py
```

---

## 6. End-to-End Verification Test

Run the verification test suite to confirm all 5 pipeline stages:

```bash
python scripts/test_pipeline.py
```
