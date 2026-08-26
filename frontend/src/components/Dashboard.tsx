import React, { useEffect, useState, useRef } from 'react';
import { PipelineStatus } from './PipelineStatus';
import { MarketTable, MarketSignal } from './MarketTable';
import { LiveChart } from './LiveChart';
import { EventLog, LogEntry } from './EventLog';

interface MarketState {
  overall_signal: string;
  confidence: number;
  bullish_count: number;
  neutral_count: number;
  bearish_count: number;
  timestamp: string | null;
}

export const Dashboard: React.FC = () => {
  const [wsConnected, setWsConnected] = useState<boolean>(false);
  const [signals, setSignals] = useState<MarketSignal[]>([]);
  const [marketState, setMarketState] = useState<MarketState>({
    overall_signal: 'NEUTRAL',
    confidence: 0,
    bullish_count: 0,
    neutral_count: 0,
    bearish_count: 0,
    timestamp: null
  });

  const [selectedSymbol, setSelectedSymbol] = useState<string>('AAPL');
  const [symbolHistory, setSymbolHistory] = useState<MarketSignal[]>([]);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [eventCount, setEventCount] = useState<number>(0);
  const [eventsPerSec, setEventsPerSec] = useState<number>(0);
  const [lastEventTime, setLastEventTime] = useState<string | null>(null);

  const eventCounterRef = useRef<number>(0);
  const logIdRef = useRef<number>(0);

  // Calculate Events/Sec rate
  useEffect(() => {
    const interval = setInterval(() => {
      setEventsPerSec(eventCounterRef.current);
      eventCounterRef.current = 0;
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  // Fetch symbol history when selected symbol changes
  useEffect(() => {
    const fetchHistory = async () => {
      try {
        const res = await fetch(`/api/market/${selectedSymbol}?limit=50`);
        if (res.ok) {
          const data = await res.json();
          setSymbolHistory(data);
        }
      } catch (err) {
        console.error('Error fetching symbol history:', err);
      }
    };

    fetchHistory();
  }, [selectedSymbol]);

  // Initial REST fetch for immediate render
  useEffect(() => {
    const fetchInitialData = async () => {
      try {
        const [latestRes, stateRes] = await Promise.all([
          fetch('/api/market/latest'),
          fetch('/api/market/state')
        ]);

        if (latestRes.ok) {
          const data = await latestRes.json();
          setSignals(data);
        }

        if (stateRes.ok) {
          const stateData = await stateRes.json();
          setMarketState(stateData);
        }
      } catch (err) {
        console.error('Error fetching initial data:', err);
      }
    };

    fetchInitialData();
  }, []);

  // Establish WebSocket Connection
  useEffect(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws/market`;

    let ws: WebSocket;
    let reconnectTimer: ReturnType<typeof setTimeout>;

    const connectWS = () => {
      ws = new WebSocket(wsUrl);

      ws.onopen = () => {
        setWsConnected(true);
        addLog('WEBSOCKET CONNECTED: /ws/market');
      };

      ws.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
          eventCounterRef.current += 1;
          setEventCount(prev => prev + 1);

          if (data.type === 'market_update') {
            if (data.signals) {
              setSignals(data.signals);
              
              // Update selected symbol history locally
              const target = data.signals.find((s: MarketSignal) => s.symbol === selectedSymbol);
              if (target) {
                setSymbolHistory(prev => {
                  const updated = [...prev, target];
                  return updated.slice(-50);
                });
                const timeStr = new Date(target.timestamp).toLocaleTimeString();
                setLastEventTime(timeStr);
              }
            }

            if (data.state) {
              setMarketState(data.state);
            }

            // Log update snippet
            const sampleSig = data.signals?.[0];
            if (sampleSig) {
              addLog(`STREAM EVENT -> ${sampleSig.symbol}: $${sampleSig.price} | Return: ${(sampleSig.return * 100).toFixed(2)}% | Signal: ${sampleSig.signal}`);
            }
          }
        } catch (e) {
          console.error('WS Message parsing error:', e);
        }
      };

      ws.onclose = () => {
        setWsConnected(false);
        addLog('WEBSOCKET DISCONNECTED. Retrying in 2s...');
        reconnectTimer = setTimeout(connectWS, 2000);
      };

      ws.onerror = (err) => {
        console.error('WS error:', err);
        ws.close();
      };
    };

    connectWS();

    return () => {
      if (ws) ws.close();
      if (reconnectTimer) clearTimeout(reconnectTimer);
    };
  }, [selectedSymbol]);

  const addLog = (msg: string) => {
    logIdRef.current += 1;
    const timeStr = new Date().toISOString().substring(11, 23);
    const newEntry: LogEntry = { id: logIdRef.current, timestamp: timeStr, message: msg };
    setLogs(prev => [newEntry, ...prev].slice(0, 50));
  };

  return (
    <div className="app-container">
      {/* Top Header */}
      <header className="header-panel">
        <div className="header-title">
          <h1>REAL-TIME MARKET INTELLIGENCE STREAMING DASHBOARD</h1>
          <div className="header-subtitle">
            FINFLOCK / INSTASTOCKS ARCHITECTURE PROTOTYPE (KAFKA &rarr; FLINK &rarr; TIMESCALEDB &rarr; FASTAPI &rarr; WEBSOCKET)
          </div>
        </div>

        <div className="header-stats">
          <div className="stat-box">
            <span className="stat-label">STREAM STATUS</span>
            <span className="stat-value">
              [{wsConnected ? 'CONNECTED' : 'DISCONNECTED'}]
            </span>
          </div>

          <div className="stat-box">
            <span className="stat-label">EVENTS / SEC</span>
            <span className="stat-value">{eventsPerSec} EVT/S</span>
          </div>

          <div className="stat-box">
            <span className="stat-label">TOTAL EVENTS</span>
            <span className="stat-value">{eventCount}</span>
          </div>
        </div>
      </header>

      {/* Pipeline Status Indicator Bar */}
      <PipelineStatus
        wsConnected={wsConnected}
        eventRate={eventsPerSec}
        lastEventTime={lastEventTime}
      />

      {/* Main Grid: Data Table + Live Chart */}
      <div className="main-grid">
        {/* Left Column: Aggregated Market State & Symbol Signals Table */}
        <div className="section-box">
          <div className="section-header">
            <span className="section-title">MARKET STATE & UNIVERSE SIGNALS</span>
            <span style={{ fontSize: '11px', color: '#888888', fontFamily: 'monospace' }}>
              REAL-TIME FLINK CALCULATIONS
            </span>
          </div>

          {/* Top Metric Cards */}
          <div className="market-summary-grid">
            <div className="summary-card">
              <div className="summary-card-title">MARKET STATE</div>
              <div className="summary-card-val">
                [{marketState.overall_signal}]
              </div>
            </div>

            <div className="summary-card">
              <div className="summary-card-title">CONFIDENCE</div>
              <div className="summary-card-val">
                {marketState.confidence.toFixed(0)}%
              </div>
            </div>

            <div className="summary-card">
              <div className="summary-card-title">BULL / BEAR</div>
              <div className="summary-card-val">
                {marketState.bullish_count} / {marketState.bearish_count}
              </div>
            </div>

            <div className="summary-card">
              <div className="summary-card-title">NEUTRAL</div>
              <div className="summary-card-val">
                {marketState.neutral_count}
              </div>
            </div>
          </div>

          {/* Table */}
          <MarketTable
            signals={signals}
            selectedSymbol={selectedSymbol}
            onSelectSymbol={setSelectedSymbol}
          />
        </div>

        {/* Right Column: Live Chart */}
        <div className="section-box">
          <div className="section-header">
            <span className="section-title">SELECTED TICKER ANALYTICS ({selectedSymbol})</span>
            <span style={{ fontSize: '11px', color: '#888888', fontFamily: 'monospace' }}>
              TIMESCALEDB TIME-SERIES
            </span>
          </div>

          <LiveChart
            symbol={selectedSymbol}
            history={symbolHistory}
          />

          <div style={{ marginTop: '16px', fontFamily: 'monospace', fontSize: '12px', color: '#aaaaaa' }}>
            <div style={{ fontWeight: 'bold', color: '#ffffff', marginBottom: '6px' }}>
              RULE-BASED SIGNAL EVALUATION FORMULA:
            </div>
            <div style={{ border: '1px solid #222222', padding: '10px', backgroundColor: '#050505' }}>
              <div>&bull; Return &gt; 0: +1 | Return &lt; 0: -1</div>
              <div>&bull; High Volume Anomaly (&gt;= 1.5x avg) &amp; Return &gt; 0: +1 | Return &lt; 0: -1</div>
              <div style={{ marginTop: '4px', color: '#ffffff', fontWeight: 'bold' }}>
                Score &gt;= +1 &rarr; BULLISH | Score &lt;= -1 &rarr; BEARISH | Else &rarr; NEUTRAL
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Bottom Live Terminal Event Inspector Log */}
      <EventLog logs={logs} />
    </div>
  );
};
