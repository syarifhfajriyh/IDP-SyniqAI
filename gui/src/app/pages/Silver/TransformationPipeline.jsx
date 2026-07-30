import React from 'react';
import { PIPELINE_STEPS } from '../../constants/executionModes';

/**
 * TransformationPipeline - Visual horizontal pipeline showing transformation stages
 * Matches the prototype's pipeline visualization
 */
export default function TransformationPipeline({ 
  activeStep = 'validate', 
  completedSteps = ['clean', 'cast', 'dedup'],
  onStepClick 
}) {
  
  const getStepClass = (stepId) => {
    const classes = ['pipe-node'];
    if (stepId === activeStep) classes.push('active');
    if (completedSteps.includes(stepId)) classes.push('done');
    if (stepId === 'quarantine' && completedSteps.includes('validate')) classes.push('warn');
    return classes.join(' ');
  };

  return (
    <div className="rule-pipeline">
      <div className="rule-pipeline-title">
        ⬡ Transformation Pipeline
        <span style={{fontSize: '9px', color: 'var(--text-dim)', fontWeight: 400}}>
          Drag to reorder
        </span>
      </div>
      
      <div className="pipeline-steps">
        {PIPELINE_STEPS.map((step, idx) => (
          <div key={step.id} className="pipe-step">
            <div 
              className={getStepClass(step.id)}
              onClick={() => onStepClick && onStepClick(step.id)}
            >
              {completedSteps.includes(step.id) && <div className="done-dot"></div>}
              <div className="pipe-node-icon">{step.icon}</div>
              <div className="pipe-node-name">{step.label}</div>
              <div className="pipe-node-count">{step.description}</div>
            </div>
            
            {idx < PIPELINE_STEPS.length - 1 && <div className="pipe-arrow"></div>}
          </div>
        ))}
      </div>

      <style jsx>{`
        .rule-pipeline {
          flex-shrink: 0;
          padding: 14px;
          border-bottom: 1px solid var(--border);
          background: var(--bg2);
        }
        .rule-pipeline-title {
          font-size: 10px; 
          font-weight: 600; 
          text-transform: uppercase; 
          letter-spacing: 0.1em;
          color: var(--text-dim); 
          margin-bottom: 10px;
          display: flex; 
          align-items: center; 
          gap: 6px;
        }
        .pipeline-steps {
          display: flex; 
          align-items: center; 
          gap: 0; 
          overflow-x: auto; 
          padding-bottom: 4px;
        }
        .pipeline-steps::-webkit-scrollbar { height: 3px; }
        .pipeline-steps::-webkit-scrollbar-track { background: var(--bg); }
        .pipeline-steps::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 2px; }

        .pipe-step {
          display: flex; 
          align-items: center; 
          gap: 0;
          flex-shrink: 0;
        }
        .pipe-node {
          display: flex; 
          flex-direction: column; 
          align-items: center;
          padding: 7px 12px;
          border-radius: 5px;
          border: 1px solid var(--border);
          background: var(--bg3);
          cursor: pointer;
          transition: all 0.15s;
          min-width: 80px;
          position: relative;
          user-select: none;
        }
        .pipe-node:hover { border-color: var(--border2); }
        .pipe-node.active { 
          border-color: var(--silver); 
          background: rgba(126,207,255,0.08); 
        }
        .pipe-node.active .pipe-node-name { color: var(--silver); }
        .pipe-node.done { border-color: var(--green-dim); }
        .pipe-node.done .pipe-node-icon { color: var(--green); }
        .pipe-node.warn { border-color: rgba(255,204,68,0.4); }
        .pipe-node.warn .pipe-node-icon { color: var(--yellow); }

        .pipe-node-icon { font-size: 14px; margin-bottom: 3px; }
        .pipe-node-name { 
          font-size: 9px; 
          font-weight: 600; 
          text-align: center; 
          color: var(--text-dim); 
          line-height: 1.2; 
        }
        .pipe-node-count { 
          font-size: 8px; 
          color: var(--text-dim); 
          margin-top: 2px; 
        }
        .pipe-node .done-dot {
          position: absolute; 
          top: -4px; 
          right: -4px;
          width: 8px; 
          height: 8px; 
          border-radius: 50%;
          background: var(--green);
          border: 1px solid var(--bg3);
        }

        .pipe-arrow {
          width: 24px; 
          height: 2px; 
          background: var(--border);
          position: relative; 
          flex-shrink: 0;
        }
        .pipe-arrow::after {
          content: '▶';
          position: absolute; 
          right: -6px; 
          top: -5px;
          font-size: 8px; 
          color: var(--border);
        }
      `}</style>
    </div>
  );
}
