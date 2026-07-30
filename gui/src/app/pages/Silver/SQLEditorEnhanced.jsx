import React, { useState, useEffect, useRef } from 'react';
import Editor from '@monaco-editor/react';
import { 
  Play, Save, Download, Code, CheckCircle, AlertCircle, Clock, Zap, Eye, 
  History, BookOpen, Trash2, Plus, Star, StarOff, FileText, Search, X, Copy, Check
} from 'lucide-react';

/**
 * SQLEditorEnhanced - Advanced SQL editor with Monaco, templates, history, and auto-complete
 * Phase 3: Monaco Editor, Auto-complete, Query History
 * Phase 4: Saved Queries, Templates, Visual Builder (future)
 */
export default function SQLEditorEnhanced() {
  // Editor state
  const [sqlQuery, setSqlQuery] = useState(``);
  const [isRunning, setIsRunning] = useState(false);
  const [queryResult, setQueryResult] = useState(null);
  const [validationStatus, setValidationStatus] = useState('idle');
  const [activeTab, setActiveTab] = useState('query');
  
  // Monaco editor ref
  const editorRef = useRef(null);
  const monacoRef = useRef(null);
  
  // Query history state
  const [queryHistory, setQueryHistory] = useState([]);
  const [showHistory, setShowHistory] = useState(false);
  
  // Saved queries state
  const [savedQueries, setSavedQueries] = useState([]);
  const [showSaved, setShowSaved] = useState(true);
  const [saveDialogOpen, setSaveDialogOpen] = useState(false);
  const [saveQueryName, setSaveQueryName] = useState('');
  const [saveQueryDesc, setSaveQueryDesc] = useState('');
  
  // Templates state
  const [showTemplates, setShowTemplates] = useState(false);
  
  // Available tables and columns for auto-complete
  const [availableTables, setAvailableTables] = useState([]);
  const [isLoadingTables, setIsLoadingTables] = useState(true);
  
  // Selected tables for query execution (from Data Catalog)
  const [selectedTables, setSelectedTables] = useState([]);

  // Query templates
  const queryTemplates = [
    {
      id: 'basic-select',
      name: 'Basic SELECT',
      description: 'Simple SELECT query with WHERE clause',
      category: 'Basic',
      query: `-- Basic SELECT query
SELECT 
  column1,
  column2,
  column3
FROM bronze.table_name
WHERE condition = 'value'
LIMIT 100;`
    },
    {
      id: 'aggregation',
      name: 'Aggregation Query',
      description: 'GROUP BY with aggregation functions',
      category: 'Aggregation',
      query: `-- Aggregation with GROUP BY
SELECT 
  group_column,
  COUNT(*) as total_count,
  SUM(amount) as total_amount,
  AVG(amount) as avg_amount,
  MIN(amount) as min_amount,
  MAX(amount) as max_amount
FROM bronze.table_name
GROUP BY group_column
ORDER BY total_amount DESC;`
    },
    {
      id: 'inner-join',
      name: 'INNER JOIN',
      description: 'Join two tables on a common key',
      category: 'Joins',
      query: `-- INNER JOIN between two tables
SELECT 
  t1.column1,
  t1.column2,
  t2.column3,
  t2.column4
FROM bronze.table1 t1
INNER JOIN bronze.table2 t2
  ON t1.id = t2.foreign_id
WHERE t1.status = 'active';`
    },
    {
      id: 'left-join',
      name: 'LEFT JOIN',
      description: 'Left outer join with NULL handling',
      category: 'Joins',
      query: `-- LEFT JOIN with NULL handling
SELECT 
  t1.id,
  t1.name,
  COALESCE(t2.value, 0) as value,
  CASE 
    WHEN t2.id IS NULL THEN 'NO_MATCH'
    ELSE 'MATCHED'
  END as match_status
FROM bronze.table1 t1
LEFT JOIN bronze.table2 t2
  ON t1.id = t2.foreign_id;`
    },
    {
      id: 'data-cleaning',
      name: 'Data Cleaning',
      description: 'Clean and standardize data',
      category: 'Transformation',
      query: `-- Data cleaning and standardization
SELECT 
  id,
  TRIM(UPPER(name)) as name_clean,
  ABS(amount) as amount_absolute,
  COALESCE(email, 'unknown@example.com') as email_clean,
  CASE 
    WHEN status IN ('active', 'ACTIVE', 'Active') THEN 'ACTIVE'
    WHEN status IN ('inactive', 'INACTIVE', 'Inactive') THEN 'INACTIVE'
    ELSE 'UNKNOWN'
  END as status_normalized,
  created_at
FROM bronze.table_name
WHERE created_at > CURRENT_DATE - INTERVAL '30 days';`
    },
    {
      id: 'window-function',
      name: 'Window Functions',
      description: 'ROW_NUMBER, RANK, and LAG/LEAD',
      category: 'Advanced',
      query: `-- Window functions for analytics
SELECT 
  user_id,
  transaction_date,
  amount,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY transaction_date) as row_num,
  RANK() OVER (PARTITION BY user_id ORDER BY amount DESC) as amount_rank,
  LAG(amount, 1) OVER (PARTITION BY user_id ORDER BY transaction_date) as prev_amount,
  LEAD(amount, 1) OVER (PARTITION BY user_id ORDER BY transaction_date) as next_amount,
  SUM(amount) OVER (PARTITION BY user_id ORDER BY transaction_date 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
FROM bronze.transactions
ORDER BY user_id, transaction_date;`
    },
    {
      id: 'quality-check',
      name: 'Data Quality Score',
      description: 'Calculate data quality metrics',
      category: 'Quality',
      query: `-- Data quality scoring
SELECT 
  *,
  (CASE WHEN id IS NOT NULL THEN 1 ELSE 0 END +
   CASE WHEN name IS NOT NULL AND name != '' THEN 1 ELSE 0 END +
   CASE WHEN email IS NOT NULL AND email LIKE '%@%' THEN 1 ELSE 0 END +
   CASE WHEN amount IS NOT NULL AND amount > 0 THEN 1 ELSE 0 END +
   CASE WHEN created_at IS NOT NULL THEN 1 ELSE 0 END) / 5.0 as _dq_score,
  CASE 
    WHEN (CASE WHEN id IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN name IS NOT NULL AND name != '' THEN 1 ELSE 0 END +
          CASE WHEN email IS NOT NULL AND email LIKE '%@%' THEN 1 ELSE 0 END +
          CASE WHEN amount IS NOT NULL AND amount > 0 THEN 1 ELSE 0 END +
          CASE WHEN created_at IS NOT NULL THEN 1 ELSE 0 END) / 5.0 >= 0.8 THEN 'HIGH'
    WHEN (CASE WHEN id IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN name IS NOT NULL AND name != '' THEN 1 ELSE 0 END +
          CASE WHEN email IS NOT NULL AND email LIKE '%@%' THEN 1 ELSE 0 END +
          CASE WHEN amount IS NOT NULL AND amount > 0 THEN 1 ELSE 0 END +
          CASE WHEN created_at IS NOT NULL THEN 1 ELSE 0 END) / 5.0 >= 0.5 THEN 'MEDIUM'
    ELSE 'LOW'
  END as _dq_level
FROM bronze.table_name;`
    },
    {
      id: 'cte-example',
      name: 'Common Table Expression (CTE)',
      description: 'Use CTEs for complex queries',
      category: 'Advanced',
      query: `-- CTE for complex transformations
WITH 
-- Step 1: Clean data
cleaned_data AS (
  SELECT 
    id,
    TRIM(name) as name,
    amount,
    status
  FROM bronze.raw_table
  WHERE amount IS NOT NULL
),
-- Step 2: Calculate aggregates
aggregated AS (
  SELECT 
    name,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
  FROM cleaned_data
  WHERE status = 'completed'
  GROUP BY name
)
-- Step 3: Final select
SELECT 
  name,
  transaction_count,
  total_amount,
  avg_amount,
  total_amount / transaction_count as effective_avg
FROM aggregated
WHERE transaction_count >= 5
ORDER BY total_amount DESC;`
    }
  ];

  // Load from localStorage on mount
  useEffect(() => {
    const loadedHistory = JSON.parse(localStorage.getItem('sqlQueryHistory') || '[]');
    const loadedSaved = JSON.parse(localStorage.getItem('sqlSavedQueries') || '[]');
    const lastQuery = localStorage.getItem('sqlLastQuery') || queryTemplates[4].query;
    
    setQueryHistory(loadedHistory);
    setSavedQueries(loadedSaved);
    setSqlQuery(lastQuery);
    
    // Load available tables from backend
    fetchAvailableTables();
  }, []);
  
  // Fetch available tables from backend
  const fetchAvailableTables = async () => {
    try {
      setIsLoadingTables(true);
      console.log('📥 Fetching available tables from backend...');
      
      const response = await fetch('http://localhost:8000/api/sql/tables');
      const data = await response.json();
      
      if (data.success && data.tables) {
        setAvailableTables(data.tables);
        // Auto-select all tables for querying
        const selected = data.tables.map(t => ({
          name: t.name,
          path: t.path
        }));
        setSelectedTables(selected);
        console.log(`✅ Loaded ${data.tables.length} tables from Bronze layer`);
        console.log('📋 Selected tables:', selected);
      } else {
        console.error('❌ Failed to load tables:', data);
      }
    } catch (error) {
      console.error('Failed to load available tables:', error);
      // Use default tables as fallback
      setAvailableTables([
        {
          name: 'bronze.finance_transactions',
          columns: ['transaction_id', 'user_id', 'amount', 'currency_code', 'transaction_date', 
                    'merchant_name', 'status', 'created_at', 'updated_at']
        }
      ]);
    } finally {
      setIsLoadingTables(false);
      console.log('✅ Table loading complete');
    }
  };

  // Save current query to localStorage
  useEffect(() => {
    if (sqlQuery) {
      localStorage.setItem('sqlLastQuery', sqlQuery);
    }
  }, [sqlQuery]);

  // Monaco editor setup
  const handleEditorDidMount = (editor, monaco) => {
    editorRef.current = editor;
    monacoRef.current = monaco;
    
    // Configure SQL auto-completion
    monaco.languages.registerCompletionItemProvider('sql', {
      provideCompletionItems: (model, position) => {
        const suggestions = [];
        
        // Add table names
        availableTables.forEach(table => {
          suggestions.push({
            label: table.name,
            kind: monaco.languages.CompletionItemKind.Class,
            insertText: table.name,
            documentation: `Table: ${table.name}`
          });
          
          // Add columns for each table
          table.columns.forEach(col => {
            suggestions.push({
              label: `${table.name.split('.')[1]}.${col}`,
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: col,
              documentation: `Column: ${col} from ${table.name}`
            });
          });
        });
        
        // Add SQL keywords
        const keywords = [
          'SELECT', 'FROM', 'WHERE', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN',
          'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'INSERT', 'UPDATE', 'DELETE',
          'CREATE', 'DROP', 'ALTER', 'AND', 'OR', 'NOT', 'IN', 'EXISTS',
          'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'DISTINCT', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'
        ];
        
        keywords.forEach(keyword => {
          suggestions.push({
            label: keyword,
            kind: monaco.languages.CompletionItemKind.Keyword,
            insertText: keyword,
            documentation: `SQL keyword: ${keyword}`
          });
        });
        
        return { suggestions };
      }
    });
  };

  const validateQuery = async () => {
    setValidationStatus('validating');
    
    try {
      const response = await fetch('http://localhost:8000/api/sql/validate', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ query: sqlQuery })
      });
      
      const data = await response.json();
      
      if (data.valid) {
        setValidationStatus('valid');
      } else {
        setValidationStatus('invalid');
        console.error('Validation failed:', data.message);
        if (data.suggestions) {
          console.info('Suggestions:', data.suggestions);
        }
      }
    } catch (error) {
      console.error('Validation error:', error);
      setValidationStatus('invalid');
    }
  };

  const runQuery = async () => {
    setIsRunning(true);
    setActiveTab('results');
    setQueryResult(null);
    
    // Check if tables are loaded
    if (!selectedTables || selectedTables.length === 0) {
      console.warn('⚠️ No tables selected, waiting for tables to load...');
      setQueryResult({
        error: true,
        errorMessage: 'Please wait for tables to load from Bronze layer, then try again.',
        rowsAffected: 0,
        executionTime: '0s',
        columns: [],
        sampleRows: []
      });
      setIsRunning(false);
      return;
    }
    
    // Add to history
    const historyEntry = {
      id: Date.now(),
      query: sqlQuery,
      timestamp: new Date().toISOString(),
      executed: true
    };
    
    const newHistory = [historyEntry, ...queryHistory].slice(0, 50); // Keep last 50
    setQueryHistory(newHistory);
    localStorage.setItem('sqlQueryHistory', JSON.stringify(newHistory));
    
    try {
      console.log(`🚀 Executing query with ${selectedTables.length} tables:`, selectedTables);
      
      const response = await fetch('http://localhost:8000/api/sql/execute', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ 
          query: sqlQuery,
          limit: 100,  // Limit to 100 rows for display
          tables: selectedTables  // Pass selected tables with their paths
        })
      });
      
      const data = await response.json();
      
      if (data.success) {
        setQueryResult({
          rowsAffected: data.row_count,
          executionTime: `${(data.execution_time_ms / 1000).toFixed(2)}s`,
          columns: data.columns,
          sampleRows: data.rows
        });
        console.log('✓ Query executed successfully:', data.message);
      } else {
        // Show error in results
        setQueryResult({
          error: true,
          errorMessage: data.error || 'Query execution failed',
          rowsAffected: 0,
          executionTime: `${(data.execution_time_ms / 1000).toFixed(2)}s`,
          columns: [],
          sampleRows: []
        });
        console.error('Query execution failed:', data.error);
      }
    } catch (error) {
      console.error('Network error:', error);
      setQueryResult({
        error: true,
        errorMessage: 'Network error: Could not connect to backend',
        rowsAffected: 0,
        executionTime: '0s',
        columns: [],
        sampleRows: []
      });
    } finally {
      setIsRunning(false);
    }
  };

  const saveQuery = () => {
    setSaveDialogOpen(true);
  };

  const handleSaveQuery = () => {
    if (!saveQueryName.trim()) return;
    
    const savedQuery = {
      id: Date.now(),
      name: saveQueryName,
      description: saveQueryDesc,
      query: sqlQuery,
      createdAt: new Date().toISOString(),
      favorite: false
    };
    
    const newSaved = [savedQuery, ...savedQueries];
    setSavedQueries(newSaved);
    localStorage.setItem('sqlSavedQueries', JSON.stringify(newSaved));
    
    setSaveDialogOpen(false);
    setSaveQueryName('');
    setSaveQueryDesc('');
  };

  const loadSavedQuery = (query) => {
    setSqlQuery(query.query);
    setShowSaved(false);
  };

  const deleteSavedQuery = (id) => {
    const newSaved = savedQueries.filter(q => q.id !== id);
    setSavedQueries(newSaved);
    localStorage.setItem('sqlSavedQueries', JSON.stringify(newSaved));
  };

  const toggleFavorite = (id) => {
    const newSaved = savedQueries.map(q => 
      q.id === id ? { ...q, favorite: !q.favorite } : q
    );
    setSavedQueries(newSaved);
    localStorage.setItem('sqlSavedQueries', JSON.stringify(newSaved));
  };

  const loadHistoryQuery = (historyEntry) => {
    setSqlQuery(historyEntry.query);
    setShowHistory(false);
  };

  const loadTemplate = (template) => {
    setSqlQuery(template.query);
    setShowTemplates(false);
  };

  const formatSQL = () => {
    // Simple formatting - in production, use sql-formatter library
    const formatted = sqlQuery
      .replace(/SELECT/gi, 'SELECT')
      .replace(/FROM/gi, '\nFROM')
      .replace(/WHERE/gi, '\nWHERE')
      .replace(/JOIN/gi, '\nJOIN')
      .replace(/GROUP BY/gi, '\nGROUP BY')
      .replace(/ORDER BY/gi, '\nORDER BY');
    setSqlQuery(formatted);
  };

  const clearEditor = () => {
    setSqlQuery('');
  };

  const groupTemplatesByCategory = () => {
    const grouped = {};
    queryTemplates.forEach(template => {
      if (!grouped[template.category]) {
        grouped[template.category] = [];
      }
      grouped[template.category].push(template);
    });
    return grouped;
  };

  return (
    <div className="h-screen flex bg-gray-50">
      {/* Main Editor Area */}
      <div className="flex-1 flex flex-col">
        {/* Header */}
        <div className="bg-white border-b border-gray-200 p-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
                <Code className="w-6 h-6 text-blue-600" />
                SQL Editor
                <span className="text-sm font-normal text-gray-500 ml-2">Enhanced Edition</span>
              </h1>
              <p className="text-sm text-gray-500 mt-1">Write custom SQL transformations with intelligent auto-complete</p>
            </div>
            <div className="flex gap-2">
              <button
                onClick={() => setShowHistory(!showHistory)}
                className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50 flex items-center gap-2"
              >
                <History className="w-4 h-4" />
                History
              </button>
              <button
                onClick={() => setShowTemplates(!showTemplates)}
                className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50 flex items-center gap-2"
              >
                <BookOpen className="w-4 h-4" />
                Templates
              </button>
              <button
                onClick={validateQuery}
                disabled={validationStatus === 'validating'}
                className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50 flex items-center gap-2"
              >
                {validationStatus === 'validating' ? (
                  <>
                    <Clock className="w-4 h-4 animate-spin" />
                    Validating...
                  </>
                ) : validationStatus === 'valid' ? (
                  <>
                    <CheckCircle className="w-4 h-4 text-green-600" />
                    Valid
                  </>
                ) : validationStatus === 'invalid' ? (
                  <>
                    <AlertCircle className="w-4 h-4 text-red-600" />
                    Invalid
                  </>
                ) : (
                  <>
                    <CheckCircle className="w-4 h-4" />
                    Validate
                  </>
                )}
              </button>
              <button
                onClick={formatSQL}
                className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50"
              >
                Format
              </button>
              <button
                onClick={saveQuery}
                className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50 flex items-center gap-2"
              >
                <Save className="w-4 h-4" />
                Save
              </button>
              <button
                onClick={runQuery}
                disabled={isRunning || isLoadingTables || selectedTables.length === 0}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 flex items-center gap-2 disabled:bg-blue-400 disabled:cursor-not-allowed"
                title={isLoadingTables ? "Loading tables..." : selectedTables.length === 0 ? "No tables available" : "Run query"}
              >
                {isRunning ? (
                  <>
                    <Clock className="w-4 h-4 animate-spin" />
                    Running...
                  </>
                ) : isLoadingTables ? (
                  <>
                    <Clock className="w-4 h-4 animate-spin" />
                    Loading Tables...
                  </>
                ) : (
                  <>
                    <Play className="w-4 h-4" />
                    Run Query {selectedTables.length > 0 && `(${selectedTables.length} tables)`}
                  </>
                )}
              </button>
            </div>
          </div>
        </div>

        {/* Tabs */}
        <div className="bg-white border-b border-gray-200">
          <div className="flex gap-1 px-4">
            {[
              { id: 'query', label: 'Query', icon: Code },
              { id: 'results', label: 'Results', icon: Eye },
              { id: 'explain', label: 'Execution Plan', icon: Zap }
            ].map(tab => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`px-4 py-3 text-sm font-medium border-b-2 transition-colors flex items-center gap-2 ${
                  activeTab === tab.id
                    ? 'border-blue-600 text-blue-600'
                    : 'border-transparent text-gray-600 hover:text-gray-900'
                }`}
              >
                <tab.icon className="w-4 h-4" />
                {tab.label}
              </button>
            ))}
          </div>
        </div>

        {/* Tab Content */}
        <div className="flex-1 overflow-hidden">
          {activeTab === 'query' && (
            <div className="h-full flex flex-col">
              {/* Monaco Editor */}
              <div className="flex-1">
                <Editor
                  height="100%"
                  defaultLanguage="sql"
                  theme="vs-dark"
                  value={sqlQuery}
                  onChange={(value) => setSqlQuery(value || '')}
                  onMount={handleEditorDidMount}
                  options={{
                    minimap: { enabled: true },
                    fontSize: 14,
                    lineNumbers: 'on',
                    roundedSelection: true,
                    scrollBeyondLastLine: false,
                    automaticLayout: true,
                    tabSize: 2,
                    wordWrap: 'on',
                    suggest: {
                      showKeywords: true,
                      showSnippets: true
                    }
                  }}
                />
              </div>

              {/* Status Bar */}
              <div className="bg-gray-800 text-gray-300 text-xs px-4 py-2 flex items-center justify-between">
                <div className="flex items-center gap-4">
                  <span>Lines: {sqlQuery.split('\n').length}</span>
                  <span>Characters: {sqlQuery.length}</span>
                  {validationStatus === 'valid' && (
                    <span className="text-green-400 flex items-center gap-1">
                      <CheckCircle className="w-3 h-3" />
                      SQL Valid
                    </span>
                  )}
                </div>
                <div className="flex items-center gap-4">
                  <button
                    onClick={clearEditor}
                    className="hover:text-white flex items-center gap-1"
                  >
                    <X className="w-3 h-3" />
                    Clear
                  </button>
                  <span>PostgreSQL • UTF-8</span>
                </div>
              </div>
            </div>
          )}

          {activeTab === 'results' && (
            <div className="h-full bg-white p-6 overflow-auto">
              {!queryResult && !isRunning && (
                <div className="flex items-center justify-center h-full">
                  <div className="text-center">
                    <Eye className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                    <p className="text-lg font-medium text-gray-900 mb-2">No results yet</p>
                    <p className="text-sm text-gray-500">Run your query to see results</p>
                  </div>
                </div>
              )}

              {isRunning && (
                <div className="flex items-center justify-center h-full">
                  <div className="text-center">
                    <Clock className="w-12 h-12 text-blue-600 mx-auto mb-4 animate-spin" />
                    <p className="text-lg font-medium text-gray-900 mb-2">Executing query...</p>
                    <p className="text-sm text-gray-500">Please wait</p>
                  </div>
                </div>
              )}

              {queryResult && !isRunning && !queryResult.error && (
                <div>
                  {/* Query Stats */}
                  <div className="mb-6 p-4 bg-green-50 border border-green-200 rounded-lg">
                    <div className="flex items-center gap-2 mb-2">
                      <CheckCircle className="w-5 h-5 text-green-600" />
                      <span className="font-semibold text-green-900">Query executed successfully</span>
                    </div>
                    <div className="text-sm text-green-800 space-y-1">
                      <div>Rows affected: {queryResult.rowsAffected.toLocaleString()}</div>
                      <div>Execution time: {queryResult.executionTime}</div>
                    </div>
                  </div>

                  {/* Results Table */}
                  {queryResult.sampleRows && queryResult.sampleRows.length > 0 ? (
                    <>
                      <div className="border border-gray-200 rounded-lg overflow-hidden">
                        <table className="w-full text-sm">
                          <thead className="bg-gray-50">
                            <tr>
                              {queryResult.columns.map(col => (
                                <th key={col} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                  {col}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody className="bg-white divide-y divide-gray-200">
                            {queryResult.sampleRows.map((row, idx) => (
                              <tr key={idx} className="hover:bg-gray-50">
                                {queryResult.columns.map(col => (
                                  <td key={col} className="px-4 py-3 whitespace-nowrap text-gray-900">
                                    {typeof row[col] === 'object' ? JSON.stringify(row[col]) : String(row[col])}
                                  </td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>

                      <div className="mt-4 text-sm text-gray-500">
                        Showing first {queryResult.sampleRows.length} rows of {queryResult.rowsAffected.toLocaleString()} total rows
                      </div>
                    </>
                  ) : (
                    <div className="text-center py-8 text-gray-500">
                      <p>Query executed successfully but returned no rows</p>
                    </div>
                  )}
                </div>
              )}

              {queryResult && !isRunning && queryResult.error && (
                <div className="flex items-center justify-center h-full">
                  <div className="max-w-2xl">
                    <div className="p-6 bg-red-50 border border-red-200 rounded-lg">
                      <div className="flex items-center gap-2 mb-3">
                        <AlertCircle className="w-6 h-6 text-red-600" />
                        <span className="font-semibold text-red-900 text-lg">Query Execution Failed</span>
                      </div>
                      <div className="text-sm text-red-800 mb-3">
                        <div className="font-semibold mb-2">Error:</div>
                        <pre className="bg-red-100 p-3 rounded overflow-x-auto whitespace-pre-wrap font-mono text-xs">
                          {queryResult.errorMessage}
                        </pre>
                      </div>
                      <div className="text-xs text-red-700">
                        Execution time: {queryResult.executionTime}
                      </div>
                    </div>
                    <div className="mt-4 p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
                      <h4 className="font-semibold text-yellow-900 mb-2">💡 Troubleshooting Tips:</h4>
                      <ul className="text-sm text-yellow-800 space-y-1 list-disc list-inside">
                        <li>Ensure table names are prefixed with <code className="bg-yellow-100 px-1 rounded">bronze.</code></li>
                        <li>Available tables: bronze.finance_transactions, bronze.user_profiles, bronze.clickstream_events</li>
                        <li>Column names are case-sensitive</li>
                        <li>Use the Validate button to check syntax before running</li>
                      </ul>
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}

          {activeTab === 'explain' && (
            <div className="h-full bg-white p-6 overflow-auto">
              <div className="space-y-4">
                <div className="p-4 bg-blue-50 border border-blue-200 rounded-lg">
                  <h3 className="font-semibold text-blue-900 mb-2 flex items-center gap-2">
                    <Zap className="w-5 h-5" />
                    Query Execution Plan
                  </h3>
                  <div className="text-sm text-blue-800 space-y-2 font-mono">
                    <div>→ Seq Scan on finance_transactions (cost=0.00..45123.00 rows=2400000)</div>
                    <div className="ml-4">→ Filter: (amount IS NOT NULL AND user_id IS NOT NULL)</div>
                    <div className="ml-4">→ Sort: created_at DESC</div>
                    <div className="ml-4">→ Estimated rows: 2,400,000</div>
                    <div className="ml-4">→ Estimated cost: 45,123.00</div>
                  </div>
                </div>
                <div className="p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
                  <h3 className="font-semibold text-yellow-900 mb-2">Optimization Suggestions</h3>
                  <ul className="text-sm text-yellow-800 space-y-1 list-disc list-inside">
                    <li>Consider adding an index on (created_at, amount, user_id)</li>
                    <li>Using WHERE clause reduces scan by 15%</li>
                    <li>Query will benefit from partitioning by date</li>
                  </ul>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Right Sidebar - Saved Queries */}
      {showSaved && (
        <div className="w-80 border-l border-gray-200 bg-white flex flex-col">
          <div className="p-4 border-b border-gray-200">
            <div className="flex items-center justify-between mb-4">
              <h2 className="font-semibold text-gray-900 flex items-center gap-2">
                <BookOpen className="w-5 h-5" />
                Saved Queries
              </h2>
              <button
                onClick={() => setShowSaved(false)}
                className="text-gray-400 hover:text-gray-600"
              >
                <X className="w-5 h-5" />
              </button>
            </div>
            <div className="text-sm text-gray-500">
              {savedQueries.length} saved queries
            </div>
          </div>
          
          <div className="flex-1 overflow-y-auto p-4 space-y-2">
            {savedQueries.length === 0 ? (
              <div className="text-center py-8 text-gray-500">
                <FileText className="w-12 h-12 mx-auto mb-3 text-gray-400" />
                <p className="text-sm">No saved queries yet</p>
                <p className="text-xs mt-1">Save your queries for quick access</p>
              </div>
            ) : (
              savedQueries.map(query => (
                <div
                  key={query.id}
                  className="p-3 border border-gray-200 rounded-lg hover:border-blue-300 hover:bg-blue-50 transition-colors cursor-pointer group"
                >
                  <div className="flex items-start justify-between mb-2">
                    <div
                      onClick={() => loadSavedQuery(query)}
                      className="flex-1"
                    >
                      <h3 className="font-medium text-gray-900 text-sm flex items-center gap-2">
                        {query.favorite && <Star className="w-4 h-4 text-yellow-500 fill-yellow-500" />}
                        {query.name}
                      </h3>
                      {query.description && (
                        <p className="text-xs text-gray-500 mt-1">{query.description}</p>
                      )}
                      <p className="text-xs text-gray-400 mt-1">
                        {new Date(query.createdAt).toLocaleDateString()}
                      </p>
                    </div>
                    <div className="flex gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          toggleFavorite(query.id);
                        }}
                        className="p-1 hover:bg-white rounded"
                      >
                        {query.favorite ? (
                          <Star className="w-4 h-4 text-yellow-500 fill-yellow-500" />
                        ) : (
                          <StarOff className="w-4 h-4 text-gray-400" />
                        )}
                      </button>
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          deleteSavedQuery(query.id);
                        }}
                        className="p-1 hover:bg-white rounded text-red-500"
                      >
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </div>
                  </div>
                  <div className="text-xs font-mono text-gray-600 bg-gray-100 p-2 rounded max-h-16 overflow-hidden">
                    {query.query.substring(0, 100)}...
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      )}

      {/* History Modal */}
      {showHistory && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl w-[800px] max-h-[600px] flex flex-col">
            <div className="p-6 border-b border-gray-200">
              <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold text-gray-900 flex items-center gap-2">
                  <History className="w-6 h-6" />
                  Query History
                </h2>
                <button
                  onClick={() => setShowHistory(false)}
                  className="text-gray-400 hover:text-gray-600"
                >
                  <X className="w-6 h-6" />
                </button>
              </div>
              <p className="text-sm text-gray-500 mt-1">
                Last {queryHistory.length} executed queries
              </p>
            </div>
            
            <div className="flex-1 overflow-y-auto p-6 space-y-3">
              {queryHistory.length === 0 ? (
                <div className="text-center py-12 text-gray-500">
                  <History className="w-16 h-16 mx-auto mb-4 text-gray-400" />
                  <p>No query history yet</p>
                  <p className="text-sm mt-2">Run queries to see them here</p>
                </div>
              ) : (
                queryHistory.map(entry => (
                  <div
                    key={entry.id}
                    className="p-4 border border-gray-200 rounded-lg hover:border-blue-300 hover:bg-blue-50 transition-colors cursor-pointer"
                    onClick={() => loadHistoryQuery(entry)}
                  >
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-xs text-gray-500">
                        {new Date(entry.timestamp).toLocaleString()}
                      </span>
                      <span className="text-xs px-2 py-1 bg-green-100 text-green-800 rounded">
                        Executed
                      </span>
                    </div>
                    <pre className="text-sm font-mono text-gray-700 bg-gray-50 p-3 rounded overflow-x-auto max-h-32 overflow-y-auto">
                      {entry.query}
                    </pre>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      )}

      {/* Templates Modal */}
      {showTemplates && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl w-[900px] max-h-[700px] flex flex-col">
            <div className="p-6 border-b border-gray-200">
              <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold text-gray-900 flex items-center gap-2">
                  <BookOpen className="w-6 h-6" />
                  Query Templates
                </h2>
                <button
                  onClick={() => setShowTemplates(false)}
                  className="text-gray-400 hover:text-gray-600"
                >
                  <X className="w-6 h-6" />
                </button>
              </div>
              <p className="text-sm text-gray-500 mt-1">
                {queryTemplates.length} pre-built query templates
              </p>
            </div>
            
            <div className="flex-1 overflow-y-auto p-6">
              {Object.entries(groupTemplatesByCategory()).map(([category, templates]) => (
                <div key={category} className="mb-6">
                  <h3 className="text-lg font-semibold text-gray-900 mb-3">{category}</h3>
                  <div className="grid grid-cols-2 gap-3">
                    {templates.map(template => (
                      <div
                        key={template.id}
                        className="p-4 border border-gray-200 rounded-lg hover:border-blue-300 hover:bg-blue-50 transition-colors cursor-pointer"
                        onClick={() => loadTemplate(template)}
                      >
                        <h4 className="font-medium text-gray-900 mb-1">{template.name}</h4>
                        <p className="text-xs text-gray-500 mb-3">{template.description}</p>
                        <pre className="text-xs font-mono text-gray-600 bg-gray-50 p-2 rounded overflow-x-auto max-h-24 overflow-y-auto">
                          {template.query}
                        </pre>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Save Query Dialog */}
      {saveDialogOpen && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl w-[500px] p-6">
            <h2 className="text-xl font-bold text-gray-900 mb-4">Save Query</h2>
            
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Query Name *
                </label>
                <input
                  type="text"
                  value={saveQueryName}
                  onChange={(e) => setSaveQueryName(e.target.value)}
                  placeholder="e.g., Customer Aggregation"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Description (optional)
                </label>
                <textarea
                  value={saveQueryDesc}
                  onChange={(e) => setSaveQueryDesc(e.target.value)}
                  placeholder="Brief description of what this query does"
                  rows={3}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
              
              <div className="flex gap-3 justify-end">
                <button
                  onClick={() => {
                    setSaveDialogOpen(false);
                    setSaveQueryName('');
                    setSaveQueryDesc('');
                  }}
                  className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  onClick={handleSaveQuery}
                  disabled={!saveQueryName.trim()}
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed"
                >
                  Save Query
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
