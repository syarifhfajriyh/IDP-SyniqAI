import React, { useState, useMemo } from 'react';
import { RULE_CATEGORIES, SEVERITY_COLORS } from '../../constants/rulesCatalogue';

/**
 * RuleLibrary - Browse and select data quality rules from the catalogue
 * Matches the prototype's rule selection panel
 */
export default function RuleLibrary({ activeRules, onToggleRule, searchQuery = '' }) {
  const [activeTab, setActiveTab] = useState('quality');
  const [expandedCategories, setExpandedCategories] = useState({
    finance_customer: true,
    general_universal: true,
    healthcare_patient: true
  });

  // Filter rules based on search
  const filteredRules = useMemo(() => {
    if (!searchQuery) return RULE_CATEGORIES;
    
    const query = searchQuery.toLowerCase();
    const filtered = {};
    
    Object.entries(RULE_CATEGORIES).forEach(([key, category]) => {
      const matchingRules = category.rules.filter(rule =>
        rule.label.toLowerCase().includes(query) ||
        rule.description.toLowerCase().includes(query) ||
        rule.ruleType.toLowerCase().includes(query)
      );
      
      if (matchingRules.length > 0) {
        filtered[key] = { ...category, rules: matchingRules };
      }
    });
    
    return filtered;
  }, [searchQuery]);

  const getSeverityClass = (severity) => {
    switch(severity) {
      case 'ERROR': return 'sev-error';
      case 'WARNING': return 'sev-warn';
      case 'INFO': return 'sev-info';
      default: return 'sev-info';    }
  };

  const isRuleRequired = (rule) => rule.default === true;
  const isRuleEnabled = (ruleId) => activeRules[ruleId] === true;

  return (
    <div className="rule-library">
      {/* Rule tabs */}
      <div className="rule-tabs">
        <div 
          className={`rule-tab ${activeTab === 'quality' ? 'active' : ''}`}
          onClick={() => setActiveTab('quality')}
        >
          Quality Rules
        </div>
        <div 
          className={`rule-tab ${activeTab === 'sql' ? 'active' : ''}`}
          onClick={() => setActiveTab('sql')}
        >
          Custom SQL
        </div>
        <div 
          className={`rule-tab ${activeTab === 'expectations' ? 'active' : ''}`}
          onClick={() => setActiveTab('expectations')}
        >
          Expectations
        </div>
        <div 
          className={`rule-tab ${activeTab === 'schema' ? 'active' : ''}`}
          onClick={() => setActiveTab('schema')}
        >
          Schema
        </div>
      </div>

      {/* Rules body */}
      {activeTab === 'quality' && (
        <div className="rules-body">
          {Object.entries(filteredRules).map(([categoryKey, category]) => (
            <div key={categoryKey}>
              <div className="rule-category">{category.label}</div>
              
              {category.rules.map(rule => {
                const required = isRuleRequired(rule);
                const enabled = isRuleEnabled(rule.id);
                
                return (
                  <div
                    key={rule.id}
                    className={`rule-item ${enabled ? 'enabled' : ''} ${required ? 'required' : ''}`}
                    onClick={() => !required && onToggleRule(rule.id)}
                  >
                    <div className="rule-checkbox">
                      {(enabled || required) && '✓'}
                    </div>
                    
                    <div className="rule-info">
                      <div className="rule-name">
                        {rule.label}
                        {required && <span className="req-badge">REQUIRED</span>}
                      </div>
                      <div className="rule-desc">{rule.description}</div>
                      <span className={`rule-severity ${getSeverityClass(rule.severity)}`}>
                        {rule.severity} → {rule.ruleType}
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
          ))}
        </div>
      )}

      {activeTab === 'sql' && (
        <div className="rules-body" style={{padding: '20px', textAlign: 'center', color: 'var(--text-dim)'}}>
          Custom SQL transformations will be defined in the SQL Editor panel →
        </div>
      )}

      {activeTab === 'expectations' && (
        <div className="rules-body" style={{padding: '20px', textAlign: 'center', color: 'var(--text-dim)'}}>
          Great Expectations integration coming soon...
        </div>
      )}

      {activeTab === 'schema' && (
        <div className="rules-body" style={{padding: '20px', textAlign: 'center', color: 'var(--text-dim)'}}>
          Schema validation rules...
        </div>
      )}

      <style jsx>{`
        .rule-library { 
          flex: 1; 
          display: flex; 
          flex-direction: column; 
          overflow: hidden; 
        }
        .rule-tabs { 
          display: flex; 
          border-bottom: 1px solid var(--border); 
          background: var(--bg2); 
          flex-shrink: 0; 
        }
        .rule-tab { 
          padding: 8px 14px; 
          font-size: 11px; 
          color: var(--text-dim); 
          cursor: pointer; 
          border-bottom: 2px solid transparent; 
          transition: all 0.15s; 
        }
        .rule-tab:hover { color: var(--text); }
        .rule-tab.active { 
          color: var(--silver); 
          border-bottom-color: var(--silver); 
        }

        .rules-body { flex: 1; overflow-y: auto; }

        .rule-category { 
          padding: 8px 14px 4px; 
          font-size: 9px; 
          text-transform: uppercase; 
          letter-spacing: 0.1em; 
          color: var(--text-dim); 
        }

        .rule-item {
          display: flex; 
          align-items: flex-start; 
          gap: 10px;
          padding: 8px 14px;
          border-bottom: 1px solid rgba(42,48,80,0.5);
          cursor: pointer;
          transition: background 0.12s;
          user-select: none;
        }
        .rule-item:hover { background: var(--bg3); }
        .rule-item.enabled { background: rgba(126,207,255,0.04); }
        .rule-item.required { cursor: default; }

        .rule-checkbox {
          width: 14px; 
          height: 14px;
          border-radius: 3px;
          border: 1px solid var(--border2);
          margin-top: 2px;
          flex-shrink: 0;
          display: flex; 
          align-items: center; 
          justify-content: center;
          font-size: 9px;
          transition: all 0.15s;
        }
        .rule-item.enabled .rule-checkbox {
          background: var(--silver); 
          border-color: var(--silver); 
          color: #000;
        }
        .rule-item.required .rule-checkbox {
          background: var(--green); 
          border-color: var(--green); 
          color: #000;
        }

        .rule-info { flex: 1; }
        .rule-name { 
          font-size: 11px; 
          font-weight: 500; 
          color: var(--text-bright); 
          display: flex; 
          align-items: center; 
          gap: 6px; 
        }
        .rule-name .req-badge { 
          font-size: 8px; 
          padding: 1px 5px; 
          border-radius: 2px; 
          background: rgba(61,255,160,0.15); 
          color: var(--green); 
          font-weight: 600; 
        }
        .rule-desc { 
          font-size: 10px; 
          color: var(--text-dim); 
          margin-top: 2px; 
          line-height: 1.4; 
        }

        .rule-severity {
          font-size: 8px; 
          padding: 2px 6px; 
          border-radius: 2px;
          font-weight: 600; 
          margin-top: 4px;
          display: inline-block; 
          font-family: var(--mono);
        }
        .sev-error { 
          background: rgba(255,85,102,0.15); 
          color: var(--red); 
          border: 1px solid rgba(255,85,102,0.2); 
        }
        .sev-warn { 
          background: rgba(255,204,68,0.15); 
          color: var(--yellow); 
          border: 1px solid rgba(255,204,68,0.2); 
        }
        .sev-info { 
          background: rgba(126,207,255,0.1); 
          color: var(--silver); 
          border: 1px solid rgba(126,207,255,0.2); 
        }
      `}</style>
    </div>
  );
}
