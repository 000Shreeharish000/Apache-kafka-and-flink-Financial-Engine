import React from 'react';

export interface LogEntry {
  id: number;
  timestamp: string;
  message: string;
}

interface Props {
  logs: LogEntry[];
}

export const EventLog: React.FC<Props> = ({ logs }) => {
  return (
    <div className="section-box" style={{ marginTop: '12px' }}>
      <div className="section-header">
        <span className="section-title">REAL-TIME STREAMING EVENT LOG (WEBSOCKET /ws/market)</span>
        <span style={{ fontSize: '11px', color: '#666666', fontFamily: 'monospace' }}>LIVE STREAM INSPECTOR</span>
      </div>

      <div className="log-panel">
        {logs.length === 0 ? (
          <div style={{ color: '#666666' }}>Initializing stream socket connection...</div>
        ) : (
          logs.map((log) => (
            <div key={log.id} className="log-line">
              <span className="log-time">[{log.timestamp}]</span>
              <span>{log.message}</span>
            </div>
          ))
        )}
      </div>
    </div>
  );
};
