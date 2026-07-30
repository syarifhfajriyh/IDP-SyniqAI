import { RefreshCw, ArrowRight, ArrowRightLeft } from 'lucide-react';

// Execution modes for Silver transformations
export const EXECUTION_MODES = [
  { 
    id: 'full_refresh', 
    label: 'Full Refresh', 
    description: 'Replace entire Silver table (destroys existing data)',
    icon: RefreshCw,
    recommended: false
  },
  { 
    id: 'incremental', 
    label: 'Incremental Append', 
    description: 'Append only new rows based on watermark column',
    icon: ArrowRight,
    recommended: true
  },
  { 
    id: 'merge', 
    label: 'Merge (Upsert)', 
    description: 'Update existing rows + insert new (requires primary key)',
    icon: ArrowRightLeft,
    recommended: false
  }
];

// Pipeline step definitions
export const PIPELINE_STEPS = [
  { id: 'clean', icon: '🧹', label: 'Data Clean', description: 'Trim, normalize, standardize' },
  { id: 'cast', icon: '⊕', label: 'Type Cast', description: 'Convert data types' },
  { id: 'dedup', icon: '◎', label: 'Dedup', description: 'Remove duplicates' },
  { id: 'validate', icon: '✓', label: 'Validate', description: 'Apply quality rules' },
  { id: 'quarantine', icon: '⚑', label: 'Quarantine', description: 'Route bad records' },
  { id: 'enrich', icon: '⊞', label: 'Enrich', description: 'Apply custom SQL' },
  { id: 'write', icon: '↗', label: 'Write', description: 'Write to Silver' }
];
