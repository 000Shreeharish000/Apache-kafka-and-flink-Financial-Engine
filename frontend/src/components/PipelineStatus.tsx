import React from 'react';

interface Props {
  wsConnected: boolean;
  eventRate: number;
  lastEventTime: string | null;
}

export const PipelineStatus: React.FC<Props> = ({ wsConnected, eventRate, lastEventTime }) => {
  return (
    <div className="pipeline-panel">
      <div className="pipeline-nodes">
        <span style={{ color: '#888888', marginRight: '4px' }}>EVENT PIPELINE:</span>
        <div className="pipeline-node">KAFKA [TOPIC: market_ticks]</div>
        <span className="pipeline-arrow">&rarr;</span>
        <div className="pipeline-node">FLINK [STREAM ENGINE]</div>
        <span className="pipeline-arrow">&rarr;</span>
        <div className="pipeline-node">TIMESCALEDB [HYPERTABLE]</div>
        <span className="pipeline-arrow">&rarr;</span>
        <div className="pipeline-node">FASTAPI [/ws/market]</div>
        <span className="pipeline-arrow">&rarr;</span>
        <div className="pipeline-node" style={{ borderColor: wsConnected ? '#ffffff' : '#666666' }}>
          REACT DASHBOARD [{wsConnected ? 'CONNECTED' : 'OFFLINE'}]
        </div>
      </div>
      <div>
        <span style={{ color: '#888888' }}>LAST EVENT: </span>
        <span style={{ fontFamily: 'monospace', fontWeight: 'bold' }}>
          {lastEventTime ? lastEventTime : 'Awaiting ticks...'}
        </span>
      </div>
    </div>
  );
};
