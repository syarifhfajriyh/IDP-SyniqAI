import React, { useState } from 'react';

/**
 * DataPreviewPanel - Shows DQ score, data preview (before/after/diff), and quality metrics
 * Matches the prototype's right panel (top section)
 */
export default function DataPreviewPanel({ 
  dqScore = 89,
  qualityMetrics = {
    completeness: 94,
    conformity: 88,
    uniqueness: 99,
    validity: 76
  },
  previewData = {
    rows_input: 2400000,
    rows_output: 2398612,
    rows_quarantined: 1247,
    duplicates_removed: 341,
    rows_rejected: 89
  },
  sampleRows = [
    { txn_id: 'txn_001', amount: '250.00', currency: 'USD', status: 'COMPLETED', _changed: ['amount', 'currency'] },
    { txn_id: 'txn_001', amount: '250.00', currency: 'usd', status: 'COMPLETED', _removed: true },
    { txn_id: 'txn_002', amount: '-84.50', currency: 'EUR', status: 'REVERSED', _changed: ['status'] },
    { txn_id: 'txn_003', amount: null, currency: 'GBP', status: 'PENDING' },
    { txn_id: 'txn_004', amount: '12,400.00', currency: 'JPY', status: 'COMPLETED' }
  ]
}) {
  const [previewMode, setPreviewMode] = useState('before'); // 'before' | 'after' | 'diff'

  const getDQColor = () => {
    if (dqScore >= 90) return 'var(--green)';
    if (dqScore >= 70) return 'var(--yellow)';
    return 'var(--red)';
  };

  const getMetricColor = (value) => {
    if (value >= 90) return 'var(--green)';
    if (value >= 70) return 'var(--yellow)';
    return 'var(--red)';
  };

  const formatNumber = (num) => {
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(0) + 'K';
    return num.toString();
  };

  return (
    <div className="data-preview-section">
      {/* DQ Score Arc */}
      <div className="dq-score-wrap">
        <div className="score-ring">
          <svg width="64" height="64" viewBox="0 0 64 64">
            <circle cx="32" cy="32" r="26" fill="none" stroke="#1a1e28" strokeWidth="8"/>
            <circle 
              cx="32" 
              cy="32" 
              r="26" 
              fill="none" 
              stroke={getDQColor()}
              strokeWidth="8"
              strokeDasharray="163"
              strokeDashoffset={163 * (1 - dqScore/100)}
              strokeLinecap="round"
            />
          </svg>
          <div className="score-text">
            <div className="score-num" style={{color: getDQColor()}}>{dqScore}</div>
            <div className="score-unit">DQ</div>
          </div>
        </div>
        
        <div className="dq-breakdown">
          {Object.entries(qualityMetrics).map(([key, value]) => (
            <div key={key} className="dq-row">
              <div className="dq-label">{key.charAt(0).toUpperCase() + key.slice(1)}</div>
              <div className="dq-bar-wrap">
                <div 
                  className="dq-bar" 
                  style={{width: `${value}%`, background: getMetricColor(value)}}
                ></div>
              </div>
              <div className="dq-pct" style={{color: getMetricColor(value)}}>{value}%</div>
            </div>
          ))}
        </div>
      </div>

      {/* Data Preview */}
      <div className="preview-header">
        <div style={{fontSize: '11px', fontWeight: 600, color: 'var(--text-bright)'}}>
          Data Preview
        </div>
        <div className="preview-toggle">
          <button 
            className={previewMode === 'before' ? 'active' : ''}
            onClick={() => setPreviewMode('before')}
          >
            Before
          </button>
          <button 
            className={previewMode === 'after' ? 'active' : ''}
            onClick={() => setPreviewMode('after')}
          >
            After
          </button>
          <button 
            className={previewMode === 'diff' ? 'active' : ''}
            onClick={() => setPreviewMode('diff')}
          >
            Diff
          </button>
        </div>
      </div>

      <div className="data-table-wrap">
        <table className="data-table">
          <thead>
            <tr>
              <th>txn_id</th>
              <th>amount</th>
              <th>currency</th>
              <th>status</th>
            </tr>
          </thead>
          <tbody>
            {sampleRows.map((row, idx) => (
              <tr key={idx} className={row._removed ? 'removed-row' : ''}>
                <td>{row.txn_id}</td>
                <td className={row._changed?.includes('amount') ? 'changed-val' : row.amount === null ? 'null-val' : ''}>
                  {row.amount === null ? 'NULL' : row.amount}
                </td>
                <td className={row._changed?.includes('currency') ? 'changed-val' : ''}>
                  {row.currency}
                </td>
                <td className={row._changed?.includes('status') ? 'changed-val' : ''}>
                  {row.status}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Quality Metrics Grid */}
      <div className="quality-section">
        <div className="quality-grid">
          <div className="quality-cell">
            <div className="q-val good">{formatNumber(previewData.rows_output)}</div>
            <div className="q-label">Rows in → Silver</div>
          </div>
          <div className="quality-cell">
            <div className="q-val warn">{formatNumber(previewData.rows_quarantined)}</div>
            <div className="q-label">Quarantined rows</div>
          </div>
          <div className="quality-cell">
            <div className="q-val good">{previewData.duplicates_removed}</div>
            <div className="q-label">Duplicates removed</div>
          </div>
          <div className="quality-cell">
            <div className="q-val bad">{previewData.rows_rejected}</div>
            <div className="q-label">Rejected (errors)</div>
          </div>
        </div>
      </div>

      <style jsx>{`
        .data-preview-section {
          display: flex;
          flex-direction: column;
          overflow-y: auto;
        }

        /* DQ Score */
        .dq-score-wrap { 
          padding: 14px; 
          border-bottom: 1px solid var(--border); 
          display: flex; 
          align-items: center; 
          gap: 14px; 
        }
        .score-ring { 
          position: relative; 
          width: 64px; 
          height: 64px; 
          flex-shrink: 0; 
        }
        .score-ring svg { transform: rotate(-90deg); }
        .score-ring .score-text { 
          position: absolute; 
          top: 50%; 
          left: 50%; 
          transform: translate(-50%,-50%); 
          text-align: center; 
        }
        .score-ring .score-num { 
          font-size: 16px; 
          font-weight: 600; 
          font-family: var(--mono); 
        }
        .score-ring .score-unit { 
          font-size: 8px; 
          color: var(--text-dim); 
        }

        .dq-breakdown { flex: 1; }
        .dq-row { 
          display: flex; 
          align-items: center; 
          gap: 6px; 
          margin-bottom: 5px; 
        }
        .dq-row:last-child { margin-bottom: 0; }
        .dq-label { 
          font-size: 10px; 
          color: var(--text-dim); 
          width: 80px; 
          flex-shrink: 0; 
        }
        .dq-bar-wrap { 
          flex: 1; 
          height: 4px; 
          background: var(--bg4); 
          border-radius: 2px; 
          overflow: hidden; 
        }
        .dq-bar { 
          height: 100%; 
          border-radius: 2px; 
          transition: width 0.6s; 
        }
        .dq-pct { 
          font-size: 9px; 
          font-family: var(--mono); 
          width: 28px; 
          text-align: right; 
        }

        /* Preview */
        .preview-header {
          padding: 10px 14px;
          border-bottom: 1px solid var(--border);
          background: var(--bg2);
          display: flex; 
          align-items: center; 
          justify-content: space-between;
          flex-shrink: 0;
        }
        .preview-toggle {
          display: flex; 
          background: var(--bg3); 
          border-radius: 4px; 
          padding: 2px;
        }
        .preview-toggle button {
          padding: 3px 10px;
          border: none; 
          background: transparent;
          color: var(--text-dim); 
          font-size: 10px;
          border-radius: 3px; 
          cursor: pointer;
          font-family: var(--sans);
          transition: all 0.15s;
        }
        .preview-toggle button.active { 
          background: var(--bg4); 
          color: var(--silver); 
        }

        .data-table-wrap { 
          overflow: auto; 
          flex: 0 0 auto; 
          max-height: 180px; 
        }
        .data-table { 
          width: 100%; 
          border-collapse: collapse; 
          font-family: var(--mono); 
          font-size: 10px; 
        }
        .data-table th { 
          padding: 5px 8px; 
          background: var(--bg3); 
          color: var(--text-dim); 
          border-bottom: 1px solid var(--border); 
          white-space: nowrap; 
          text-align: left; 
          font-weight: 500; 
          position: sticky; 
          top: 0; 
        }
        .data-table td { 
          padding: 4px 8px; 
          border-bottom: 1px solid rgba(42,48,80,0.4); 
          white-space: nowrap; 
          color: var(--text); 
        }
        .data-table tr:hover td { background: var(--bg3); }
        .data-table .null-val { 
          color: var(--text-dim); 
          font-style: italic; 
        }
        .data-table .changed-val { color: var(--green); }
        .data-table .removed-row td { 
          background: rgba(255,85,102,0.05); 
          color: var(--red); 
          text-decoration: line-through; 
        }

        /* Quality Grid */
        .quality-section { border-top: 1px solid var(--border); }
        .quality-grid { 
          display: grid; 
          grid-template-columns: 1fr 1fr; 
          gap: 1px; 
          background: var(--border); 
        }
        .quality-cell { 
          background: var(--bg2); 
          padding: 10px 12px; 
        }
        .quality-cell .q-val { 
          font-size: 20px; 
          font-weight: 600; 
          font-family: var(--mono); 
          color: var(--text-bright); 
        }
        .quality-cell .q-val.good { color: var(--green); }
        .quality-cell .q-val.warn { color: var(--yellow); }
        .quality-cell .q-val.bad { color: var(--red); }
        .quality-cell .q-label { 
          font-size: 9px; 
          color: var(--text-dim); 
          margin-top: 2px; 
        }
      `}</style>
    </div>
  );
}
