import React, { useState } from 'react';
import { EXECUTION_MODES } from '../../constants/executionModes';

/**
 * ColumnLineagePanel - Shows source → target column mappings with transformations
 * Matches the prototype's left panel
 */
export default function ColumnLineagePanel({ 
  columnMappings = [], 
  executionMode = 'incremental',
  watermarkColumn = 'created_at',
  onExecModeChange,
  onWatermarkChange
}) {
  const [activeTab, setActiveTab] = useState('mapping');

  // Mock data if no mappings provided
  const mappings = columnMappings.length > 0 ? columnMappings : [
    { source: 'transaction_id', sourceType: 'VARCHAR(36)', target: 'transaction_id', targetType: 'STRING', transform: 'TRIM', transformType: 'clean' },
    { source: 'amount', sourceType: 'DECIMAL(18,4)', target: 'amount', targetType: 'DOUBLE', transform: 'ABS → CAST', transformType: 'cast' },
    { source: 'currency_code', sourceType: 'CHAR(3)', target: 'currency_code', targetType: 'STRING', transform: 'UPPER + TRIM', transformType: 'clean' },
    { source: 'transaction_date', sourceType: 'TIMESTAMP', target: 'transaction_date', targetType: 'DATETIME64[us]' },
    { source: 'user_id', sourceType: 'INT', target: 'user_id', targetType: 'STRING', transform: 'CAST', transformType: 'cast' },
    { source: 'merchant_name', sourceType: 'TEXT', target: 'merchant_name', targetType: 'STRING', transform: 'TRIM + LOWER', transformType: 'clean' },
    { source: 'status', sourceType: 'VARCHAR(20)', target: 'status', targetType: 'STRING', transform: 'ENUM MAP', transformType: 'standard' },
    { source: 'created_at', sourceType: 'TIMESTAMP', target: 'created_at', targetType: 'DATETIME64[us]' },
    { source: 'country_code', sourceType: 'CHAR(2)', target: 'country_code', targetType: 'STRING', transform: 'UPPER + VALIDATE', transformType: 'clean' },
    { source: 'category_id', sourceType: 'INT', target: 'category_id', targetType: 'INT' },
    { source: '— (computed) —', sourceType: '', target: '_dq_score', targetType: 'DOUBLE', transform: 'COMPUTED', transformType: 'standard', isComputed: true }
  ];

  return (
    <div className="left-panel">
      {/* Panel tabs */}
      <div className="panel-tabs">
        <div 
          className={`panel-tab ${activeTab === 'mapping' ? 'active' : ''}`}
          onClick={() => setActiveTab('mapping')}
        >
          Column Mapping
        </div>
        <div 
          className={`panel-tab ${activeTab === 'diff' ? 'active' : ''}`}
          onClick={() => setActiveTab('diff')}
        >
          Schema Diff
        </div>
        <div 
          className={`panel-tab ${activeTab === 'lineage' ? 'active' : ''}`}
          onClick={() => setActiveTab('lineage')}
        >
          Lineage
        </div>
      </div>

      {/* Execution mode bar */}
      <div className="exec-mode-bar">
        <span style={{fontSize: '10px', color: 'var(--text-dim)', marginRight: '4px'}}>Mode:</span>
        {EXECUTION_MODES.map(mode => (
          <div
            key={mode.id}
            className={`exec-chip ${executionMode === mode.id ? 'active' : ''}`}
            onClick={() => onExecModeChange && onExecModeChange(mode.id)}
          >
            {mode.label}
            {mode.recommended && <span className="rec">✓ REC</span>}
          </div>
        ))}
      </div>

      {/* Watermark selector for incremental mode */}
      {executionMode === 'incremental' && (
        <div className="watermark-row">
          <span className="watermark-label">⏱ Watermark Column:</span>
          <select 
            className="watermark-select"
            value={watermarkColumn}
            onChange={(e) => onWatermarkChange && onWatermarkChange(e.target.value)}
          >
            <option value="created_at">created_at</option>
            <option value="updated_at">updated_at</option>
            <option value="transaction_date">transaction_date</option>
          </select>
        </div>
      )}

      {/* Column mappings */}
      {activeTab === 'mapping' && (
        <>
          <div className="section-header">
            <span className="title">Source → Target Mappings</span>
            <span className="count">{mappings.length} columns</span>
            <span className="action-link">+ Add computed</span>
          </div>

          <div className="panel-body">
            {mappings.map((col, idx) => (
              <div 
                key={idx} 
                className={`col-row ${col.transform ? 'has-transform' : ''}`}
              >
                <div className="col-cell source" style={col.isComputed ? {color: 'var(--text-dim)', fontStyle: 'italic'} : {}}>
                  {col.source}
                  {col.sourceType && <div className="col-type">{col.sourceType}</div>}
                </div>
                <div className="col-cell arrow">{col.isComputed ? '+' : '→'}</div>
                <div className="col-cell target">
                  {col.target}
                  <div className="col-type">{col.targetType}</div>
                  {col.transform && (
                    <span className={`transform-tag ${col.transformType || ''}`}>
                      {col.transform}
                    </span>
                  )}
                </div>
              </div>
            ))}
          </div>
        </>
      )}

      {activeTab === 'diff' && (
        <div className="panel-body" style={{padding: '20px', textAlign: 'center', color: 'var(--text-dim)'}}>
          Schema diff view coming soon...
        </div>
      )}

      {activeTab === 'lineage' && (
        <div className="panel-body" style={{padding: '20px', textAlign: 'center', color: 'var(--text-dim)'}}>
          Column-level lineage graph...
        </div>
      )}

      <style jsx>{`
        .left-panel { 
          width: 380px; 
          min-width: 380px; 
          display: flex; 
          flex-direction: column; 
          overflow: hidden; 
          border-right: 1px solid var(--border); 
        }

        .panel-tabs {
          display: flex;
          border-bottom: 1px solid var(--border);
          background: var(--bg2);
          flex-shrink: 0;
        }
        .panel-tab {
          padding: 10px 14px;
          font-size: 11px; 
          font-weight: 500;
          color: var(--text-dim);
          cursor: pointer;
          border-bottom: 2px solid transparent;
          transition: all 0.15s;
          white-space: nowrap;
        }
        .panel-tab:hover { color: var(--text); }
        .panel-tab.active { 
          color: var(--silver); 
          border-bottom-color: var(--silver); 
        }

        .exec-mode-bar {
          padding: 10px 14px;
          border-bottom: 1px solid var(--border);
          background: var(--bg2);
          flex-shrink: 0;
          display: flex; 
          gap: 6px; 
          align-items: center;
        }
        .exec-chip {
          padding: 4px 10px;
          border-radius: 4px;
          border: 1px solid var(--border);
          font-size: 10px; 
          font-weight: 500;
          cursor: pointer; 
          transition: all 0.15s;
          color: var(--text-dim);
          background: var(--bg3);
        }
        .exec-chip:hover { 
          border-color: var(--border2); 
          color: var(--text); 
        }
        .exec-chip.active { 
          border-color: var(--silver-dim); 
          color: var(--silver); 
          background: rgba(126,207,255,0.08); 
        }
        .exec-chip .rec { 
          font-size: 8px; 
          color: var(--green); 
          margin-left: 4px; 
        }

        .watermark-row { 
          padding: 8px 14px; 
          border-bottom: 1px solid var(--border); 
          display: flex; 
          align-items: center; 
          gap: 8px; 
        }
        .watermark-label { 
          font-size: 10px; 
          color: var(--text-dim); 
          white-space: nowrap; 
        }
        .watermark-select { 
          flex: 1; 
          background: var(--bg3); 
          border: 1px solid var(--border); 
          border-radius: 3px; 
          color: var(--text); 
          font-size: 10px; 
          font-family: var(--mono); 
          padding: 4px 8px; 
        }

        .panel-body { 
          flex: 1; 
          overflow-y: auto; 
          padding: 0; 
        }

        .section-header {
          padding: 10px 14px 8px;
          border-bottom: 1px solid var(--border);
          background: var(--bg2);
          position: sticky; 
          top: 0; 
          z-index: 2;
          display: flex; 
          align-items: center; 
          justify-content: space-between;
        }
        .section-header .title { 
          font-size: 11px; 
          font-weight: 600; 
          color: var(--text-bright); 
        }
        .section-header .count { 
          font-size: 10px; 
          color: var(--text-dim); 
        }
        .section-header .action-link { 
          font-size: 10px; 
          color: var(--silver); 
          cursor: pointer; 
        }

        .col-row {
          display: grid;
          grid-template-columns: 1fr 28px 1fr;
          gap: 0;
          border-bottom: 1px solid var(--border);
          align-items: stretch;
          transition: background 0.12s;
          cursor: pointer;
        }
        .col-row:hover { background: var(--bg3); }
        .col-row.has-transform { background: rgba(126,207,255,0.03); }

        .col-cell {
          padding: 7px 10px;
          font-family: var(--mono);
          font-size: 10px;
        }
        .col-cell.source { 
          color: var(--bronze); 
          border-right: 1px solid var(--border); 
        }
        .col-cell.target { color: var(--silver); }
        .col-cell.arrow { 
          display: flex; 
          align-items: center; 
          justify-content: center; 
          color: var(--text-dim); 
          font-size: 10px; 
          border-right: 1px solid var(--border); 
        }

        .col-type { 
          font-size: 9px; 
          color: var(--text-dim); 
          margin-top: 1px; 
        }
        .transform-tag {
          display: inline-block;
          margin-top: 3px;
          padding: 1px 5px;
          border-radius: 2px;
          font-size: 9px;
          font-family: var(--mono);
          background: rgba(176,136,255,0.15);
          color: var(--purple);
          border: 1px solid rgba(176,136,255,0.2);
        }
        .transform-tag.clean { 
          background: rgba(61,255,160,0.1); 
          color: var(--green); 
          border-color: rgba(61,255,160,0.2); 
        }
        .transform-tag.cast { 
          background: rgba(255,204,68,0.1); 
          color: var(--yellow); 
          border-color: rgba(255,204,68,0.2); 
        }
      `}</style>
    </div>
  );
}
