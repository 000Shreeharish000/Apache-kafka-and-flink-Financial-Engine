import React from 'react';

export interface MarketSignal {
  timestamp: string;
  symbol: string;
  price: number;
  return: number;
  volatility: number;
  volume_ratio: number;
  movement: string;
  signal: string;
  signal_score: number;
}

interface Props {
  signals: MarketSignal[];
  selectedSymbol: string;
  onSelectSymbol: (symbol: string) => void;
}

export const MarketTable: React.FC<Props> = ({ signals, selectedSymbol, onSelectSymbol }) => {
  // Sort symbols deterministically: AAPL, MSFT, NVDA, AMZN, GOOGL
  const order = ['AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL'];
  const sortedSignals = [...signals].sort((a, b) => {
    return order.indexOf(a.symbol) - order.indexOf(b.symbol);
  });

  return (
    <div className="data-table-wrapper">
      <table className="data-table">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Price ($)</th>
            <th>Return (%)</th>
            <th>Volatility</th>
            <th>Vol Ratio</th>
            <th>Movement</th>
            <th>Signal</th>
            <th>Score</th>
          </tr>
        </thead>
        <tbody>
          {sortedSignals.length === 0 ? (
            <tr>
              <td colSpan={8} style={{ textAlign: 'center', color: '#666666', padding: '20px' }}>
                Waiting for Flink stream events...
              </td>
            </tr>
          ) : (
            sortedSignals.map((sig) => {
              const isSelected = sig.symbol === selectedSymbol;
              const retPct = (sig.return * 100).toFixed(2);
              const isPos = sig.return > 0;
              const isNeg = sig.return < 0;

              return (
                <tr
                  key={sig.symbol}
                  className={isSelected ? 'selected' : ''}
                  onClick={() => onSelectSymbol(sig.symbol)}
                >
                  <td style={{ fontWeight: 'bold' }}>
                    {isSelected ? `> ${sig.symbol}` : sig.symbol}
                  </td>
                  <td>{sig.price.toFixed(2)}</td>
                  <td>
                    {isPos ? `+${retPct}%` : `${retPct}%`}
                  </td>
                  <td>{sig.volatility.toFixed(4)}</td>
                  <td>{sig.volume_ratio.toFixed(2)}x</td>
                  <td>
                    <span className="badge-tag">
                      {sig.movement}
                    </span>
                  </td>
                  <td>
                    <span className="badge-tag" style={{ border: '1px solid #666666' }}>
                      [{sig.signal}]
                    </span>
                  </td>
                  <td style={{ fontWeight: 'bold' }}>
                    {sig.signal_score > 0 ? `+${sig.signal_score}` : sig.signal_score}
                  </td>
                </tr>
              );
            })
          )}
        </tbody>
      </table>
    </div>
  );
};
