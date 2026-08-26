import React from 'react';
import { MarketSignal } from './MarketTable';

interface Props {
  symbol: string;
  history: MarketSignal[];
}

export const LiveChart: React.FC<Props> = ({ symbol, history }) => {
  if (!history || history.length === 0) {
    return (
      <div className="chart-container" style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <span style={{ color: '#666666', fontFamily: 'monospace' }}>
          Loading real-time time-series for {symbol}...
        </span>
      </div>
    );
  }

  const prices = history.map(h => h.price);
  const minPrice = Math.min(...prices) * 0.998;
  const maxPrice = Math.max(...prices) * 1.002;
  const priceRange = (maxPrice - minPrice) || 1;

  const width = 600;
  const height = 220;
  const padding = 30;

  const points = history.map((h, i) => {
    const x = padding + (i / Math.max(1, history.length - 1)) * (width - 2 * padding);
    const y = height - padding - ((h.price - minPrice) / priceRange) * (height - 2 * padding);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');

  const latest = history[history.length - 1];

  return (
    <div className="chart-container">
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px', fontFamily: 'monospace' }}>
        <span style={{ fontWeight: 'bold' }}>SYMBOL: {symbol} TIME-SERIES STREAM</span>
        <span style={{ color: '#888888' }}>
          MIN: ${minPrice.toFixed(2)} | MAX: ${maxPrice.toFixed(2)} | LATEST: ${latest?.price.toFixed(2)}
        </span>
      </div>

      <svg width="100%" height="190" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none">
        {/* Grid lines */}
        <line x1={padding} y1={padding} x2={width - padding} y2={padding} stroke="#1a1a1a" strokeDasharray="4 4" />
        <line x1={padding} y1={height / 2} x2={width - padding} y2={height / 2} stroke="#1a1a1a" strokeDasharray="4 4" />
        <line x1={padding} y1={height - padding} x2={width - padding} y2={height - padding} stroke="#1a1a1a" strokeDasharray="4 4" />

        {/* Dynamic Price Line */}
        <polyline
          fill="none"
          stroke="#ffffff"
          strokeWidth="2"
          points={points}
        />

        {/* Data points */}
        {history.map((h, i) => {
          const x = padding + (i / Math.max(1, history.length - 1)) * (width - 2 * padding);
          const y = height - padding - ((h.price - minPrice) / priceRange) * (height - 2 * padding);
          return (
            <circle
              key={i}
              cx={x}
              cy={y}
              r={i === history.length - 1 ? 4 : 2}
              fill="#ffffff"
              stroke="#000000"
              strokeWidth="1"
            />
          );
        })}
      </svg>
    </div>
  );
};
