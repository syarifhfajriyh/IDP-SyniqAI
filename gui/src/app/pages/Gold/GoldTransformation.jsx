import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { 
  Play, 
  Plus,
  Trash2,
  Loader2,
  AlertCircle,
  CheckCircle,
  TrendingUp,
  Users,
  Calendar,
  Table
} from 'lucide-react';

const API_BASE = 'http://localhost:8000/api';

export default function GoldTransformation() {
  const [silverTables, setSilverTables] = useState([]);
  const [transformationType, setTransformationType] = useState('aggregation');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);

  // Aggregation state
  const [selectedTable, setSelectedTable] = useState('');
  const [targetTable, setTargetTable] = useState('');
  const [groupByColumns, setGroupByColumns] = useState([]);
  const [aggregations, setAggregations] = useState([]);
  const [availableColumns, setAvailableColumns] = useState([]);

  // Join state
  const [joinTables, setJoinTables] = useState([{ table: '', alias: '' }]);
  const [joinConditions, setJoinConditions] = useState([]);
  const [selectColumns, setSelectColumns] = useState([]);

  // Rollup state
  const [dateColumn, setDateColumn] = useState('');
  const [rollupLevel, setRollupLevel] = useState('monthly');
  const [metrics, setMetrics] = useState([]);

  useEffect(() => {
    loadSilverTables();
  }, []);

  const loadSilverTables = async () => {
    try {
      console.log('Loading Silver tables from API...');
      const response = await axios.get(`${API_BASE}/gold/silver-tables`);
      console.log('API response:', response.data);
      
      if (response.data.success && response.data.tables) {
        setSilverTables(response.data.tables);
        console.log(`Loaded ${response.data.tables.length} Silver tables:`, response.data.tables);
      } else {
        console.warn('API returned no tables:', response.data);
        setError(response.data.message || 'No Silver tables found');
      }
    } catch (err) {
      console.error('Error loading Silver tables:', err);
      console.error('Error details:', err.response?.data);
      setError(err.response?.data?.detail || err.message || 'Failed to load Silver tables');
    }
  };

  const handleTableSelect = (tableName) => {
    setSelectedTable(tableName);
    const table = silverTables.find(t => t.name === tableName);
    if (table) {
      setAvailableColumns(table.columns || []);
      // Auto-suggest target table name - extract just the entity name
      const entity = table.entity || table.name;
      if (transformationType === 'aggregation') {
        setTargetTable(`syniqai_gold.finance.${entity}_performance`);
      } else if (transformationType === 'rollup') {
        setTargetTable(`syniqai_gold.finance.${entity}_${rollupLevel}`);
      } else {
        setTargetTable(`syniqai_gold.finance.${entity}_transformed`);
      }
    }
  };

  const addAggregation = () => {
    setAggregations([...aggregations, { column: '', function: 'SUM', alias: '' }]);
  };

  const removeAggregation = (index) => {
    setAggregations(aggregations.filter((_, i) => i !== index));
  };

  const updateAggregation = (index, field, value) => {
    const updated = [...aggregations];
    updated[index][field] = value;
    // Auto-generate alias if function or column changes
    if (field === 'function' || field === 'column') {
      const agg = updated[index];
      if (agg.function && agg.column) {
        updated[index].alias = `${agg.function.toLowerCase()}_${agg.column}`;
      }
    }
    setAggregations(updated);
  };

  const addJoinTable = () => {
    setJoinTables([...joinTables, { table: '', alias: '' }]);
  };

  const removeJoinTable = (index) => {
    setJoinTables(joinTables.filter((_, i) => i !== index));
  };

  const updateJoinTable = (index, field, value) => {
    const updated = [...joinTables];
    updated[index][field] = value;
    // Auto-generate alias from table name
    if (field === 'table' && value) {
      const parts = value.split('.');
      const entity = parts.length > 1 ? parts[1] : value;
      updated[index].alias = entity.charAt(0);
    }
    setJoinTables(updated);
  };

  const addJoinCondition = () => {
    setJoinConditions([...joinConditions, { left: '', right: '', type: 'INNER' }]);
  };

  const removeJoinCondition = (index) => {
    setJoinConditions(joinConditions.filter((_, i) => i !== index));
  };

  const updateJoinCondition = (index, field, value) => {
    const updated = [...joinConditions];
    updated[index][field] = value;
    setJoinConditions(updated);
  };

  const addMetric = () => {
    setMetrics([...metrics, { column: '', function: 'SUM', alias: '' }]);
  };

  const removeMetric = (index) => {
    setMetrics(metrics.filter((_, i) => i !== index));
  };

  const updateMetric = (index, field, value) => {
    const updated = [...metrics];
    updated[index][field] = value;
    if (field === 'function' || field === 'column') {
      const metric = updated[index];
      if (metric.function && metric.column) {
        updated[index].alias = `${metric.function.toLowerCase()}_${metric.column}`;
      }
    }
    setMetrics(updated);
  };

  const executeTransformation = async () => {
    try {
      setLoading(true);
      setError(null);
      setSuccess(null);

      let config = {};
      let sourceTables = [];

      // Helper function to get full table name
      const getFullTableName = (tableName) => {
        const table = silverTables.find(t => t.name === tableName);
        return table ? table.full_name : tableName;
      };

      if (transformationType === 'aggregation') {
        if (!selectedTable || groupByColumns.length === 0 || aggregations.length === 0) {
          throw new Error('Please select table, group by columns, and aggregations');
        }
        
        const fullTableName = getFullTableName(selectedTable);
        
        config = {
          group_by: groupByColumns,
          aggregations: aggregations,
          filters: []
        };
        sourceTables = [fullTableName];
        
        // Call the specific aggregation endpoint
        const response = await axios.post(`${API_BASE}/gold/transform/aggregation`, {
          silver_table: fullTableName,
          gold_table: targetTable,
          group_by: groupByColumns,
          aggregations: aggregations,
          filters: [],
          description: `Aggregation: ${groupByColumns.join(', ')} with ${aggregations.length} metrics`
        });
        
        setSuccess(`✓ Aggregation completed! Created ${targetTable} with ${response.data.result?.output_rows || 0} rows`);
        
      } else if (transformationType === 'join') {
        if (joinTables.length < 2 || joinConditions.length === 0 || selectColumns.length === 0) {
          throw new Error('Please configure at least 2 tables, join conditions, and select columns');
        }
        
        // Convert table names to full names
        const fullJoinTables = joinTables.map(jt => ({
          table: getFullTableName(jt.table),
          alias: jt.alias
        }));
        
        config = {
          tables: fullJoinTables,
          join_conditions: joinConditions,
          select_columns: selectColumns
        };
        sourceTables = fullJoinTables.map(t => t.table);
        
        // Call the specific join endpoint
        const response = await axios.post(`${API_BASE}/gold/transform/join`, {
          tables: fullJoinTables,
          join_conditions: joinConditions,
          select_columns: selectColumns,
          gold_table: targetTable,
          description: `Join: ${joinTables.length} tables`
        });
        
        setSuccess(`✓ Join completed! Created ${targetTable} with ${response.data.result?.output_rows || 0} rows`);
        
      } else if (transformationType === 'rollup') {
        if (!selectedTable || !dateColumn || metrics.length === 0) {
          throw new Error('Please select table, date column, and metrics');
        }
        
        const fullTableName = getFullTableName(selectedTable);
        
        config = {
          source_table: fullTableName,
          date_column: dateColumn,
          rollup_level: rollupLevel,
          metrics: metrics,
          group_by: groupByColumns
        };
        sourceTables = [fullTableName];
        
        // For now, use the generic transform endpoint
        const response = await axios.post(`${API_BASE}/gold/transform`, {
          source_tables: sourceTables,
          target_table: targetTable,
          transformation_type: transformationType,
          config: config,
          description: `${rollupLevel} rollup transformation`
        });
        
        setSuccess(`✓ Rollup completed! Created ${targetTable} with ${response.data.rows_generated || 0} rows`);
      }
      
      // Reset form after 3 seconds
      setTimeout(() => {
        resetForm();
      }, 3000);

    } catch (err) {
      console.error('Transformation failed:', err);
      setError(err.response?.data?.detail || err.message || 'Transformation failed');
    } finally {
      setLoading(false);
    }
  };

  const resetForm = () => {
    setSelectedTable('');
    setTargetTable('');
    setGroupByColumns([]);
    setAggregations([]);
    setJoinTables([{ table: '', alias: '' }]);
    setJoinConditions([]);
    setSelectColumns([]);
    setDateColumn('');
    setMetrics([]);
    setSuccess(null);
    setError(null);
  };

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-gray-800">Gold Transformation</h1>
        <p className="text-sm text-gray-500 mt-1">
          Create aggregations, joins, and dimensional models from Silver layer
        </p>
      </div>

      {/* Success/Error Alerts */}
      {success && (
        <div className="mb-6 bg-green-50 border border-green-200 rounded-lg p-4 flex items-start gap-3">
          <CheckCircle size={20} className="text-green-600 shrink-0 mt-0.5" />
          <div className="flex-1">
            <p className="font-medium text-green-900">{success}</p>
          </div>
        </div>
      )}

      {error && (
        <div className="mb-6 bg-red-50 border border-red-200 rounded-lg p-4 flex items-start gap-3">
          <AlertCircle size={20} className="text-red-600 shrink-0 mt-0.5" />
          <div className="flex-1">
            <p className="font-medium text-red-900">Transformation Failed</p>
            <p className="text-sm text-red-800 mt-1">{error}</p>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Configuration Panel */}
        <div className="lg:col-span-2 bg-white rounded-lg border border-gray-200 p-6">
          {/* Transformation Type Selector */}
          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Transformation Type
            </label>
            <div className="grid grid-cols-3 gap-3">
              {[
                { id: 'aggregation', label: 'Aggregation', icon: TrendingUp },
                { id: 'join', label: 'Multi-Table Join', icon: Users },
                { id: 'rollup', label: 'Time Rollup', icon: Calendar }
              ].map(type => {
                const Icon = type.icon;
                return (
                  <button
                    key={type.id}
                    onClick={() => {
                      setTransformationType(type.id);
                      resetForm();
                    }}
                    className={`px-4 py-3 rounded-lg border-2 transition-colors flex items-center justify-center gap-2 ${
                      transformationType === type.id
                        ? 'border-blue-600 bg-blue-50 text-blue-700'
                        : 'border-gray-200 bg-white text-gray-600 hover:border-gray-300'
                    }`}
                  >
                    <Icon size={18} />
                    <span className="font-medium text-sm">{type.label}</span>
                  </button>
                );
              })}
            </div>
          </div>

          {/* Target Table Name */}
          <div className="mb-6">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Target Gold Table Name
            </label>
            <input
              type="text"
              value={targetTable}
              onChange={(e) => setTargetTable(e.target.value)}
              placeholder="e.g., sales_monthly_summary"
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
          </div>

          {/* Aggregation Configuration */}
          {transformationType === 'aggregation' && (
            <>
              <div className="mb-6">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Source Silver Table
                </label>
                <select
                  value={selectedTable}
                  onChange={(e) => handleTableSelect(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                >
                  <option value="">Select a Silver table...</option>
                  {silverTables.map(table => (
                    <option key={table.name} value={table.name}>
                      {table.full_name} ({table.row_count?.toLocaleString()} rows, {table.columns?.length || 0} columns)
                    </option>
                  ))}
                </select>
                {silverTables.length === 0 && (
                  <p className="text-sm text-gray-500 mt-2">
                    <Loader2 size={14} className="inline animate-spin mr-1" />
                    Loading Silver tables from MinIO...
                  </p>
                )}
              </div>

              {/* Show selected table info */}
              {selectedTable && (
                <div className="mb-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
                  <div className="flex items-start gap-3">
                    <Table size={20} className="text-blue-600 mt-0.5 shrink-0" />
                    <div className="flex-1">
                      <p className="font-semibold text-blue-900 mb-1">Source Silver Table Selected</p>
                      <p className="text-sm text-blue-800">
                        <span className="font-mono bg-blue-100 px-2 py-0.5 rounded">{silverTables.find(t => t.name === selectedTable)?.full_name}</span>
                      </p>
                      <p className="text-xs text-blue-700 mt-2">
                        {silverTables.find(t => t.name === selectedTable)?.row_count?.toLocaleString()} rows • {availableColumns.length} columns available for aggregation
                      </p>
                    </div>
                  </div>
                </div>
              )}

              <div className="mb-6">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Group By Columns
                </label>
                <select
                  multiple
                  value={groupByColumns}
                  onChange={(e) => setGroupByColumns(Array.from(e.target.selectedOptions, option => option.value))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent h-24"
                  disabled={!selectedTable}
                >
                  {availableColumns.map(col => (
                    <option key={col} value={col}>{col}</option>
                  ))}
                </select>
                <p className="text-xs text-gray-500 mt-1">Hold Ctrl/Cmd to select multiple columns</p>
              </div>

              <div className="mb-6">
                <div className="flex items-center justify-between mb-2">
                  <label className="block text-sm font-medium text-gray-700">
                    Aggregations
                  </label>
                  <button
                    onClick={addAggregation}
                    className="flex items-center gap-1 px-3 py-1 text-xs font-medium text-blue-600 bg-blue-50 rounded-lg hover:bg-blue-100"
                  >
                    <Plus size={14} />
                    Add
                  </button>
                </div>
                <div className="space-y-3">
                  {aggregations.map((agg, idx) => (
                    <div key={idx} className="flex items-center gap-2">
                      <select
                        value={agg.column}
                        onChange={(e) => updateAggregation(idx, 'column', e.target.value)}
                        className="flex-1 px-3 py-2 border border-gray-300 rounded-lg text-sm"
                      >
                        <option value="">Select column...</option>
                        {availableColumns.map(col => (
                          <option key={col} value={col}>{col}</option>
                        ))}
                      </select>
                      <select
                        value={agg.function}
                        onChange={(e) => updateAggregation(idx, 'function', e.target.value)}
                        className="w-32 px-3 py-2 border border-gray-300 rounded-lg text-sm"
                      >
                        <option value="SUM">SUM</option>
                        <option value="AVG">AVG</option>
                        <option value="COUNT">COUNT</option>
                        <option value="MIN">MIN</option>
                        <option value="MAX">MAX</option>
                      </select>
                      <input
                        type="text"
                        value={agg.alias}
                        onChange={(e) => updateAggregation(idx, 'alias', e.target.value)}
                        placeholder="Alias"
                        className="w-40 px-3 py-2 border border-gray-300 rounded-lg text-sm"
                      />
                      <button
                        onClick={() => removeAggregation(idx)}
                        className="p-2 text-red-600 hover:bg-red-50 rounded-lg"
                      >
                        <Trash2 size={16} />
                      </button>
                    </div>
                  ))}
                  {aggregations.length === 0 && (
                    <p className="text-sm text-gray-500 italic text-center py-4">
                      No aggregations defined. Click "Add" to create one.
                    </p>
                  )}
                </div>
              </div>
            </>
          )}

          {/* Join Configuration */}
          {transformationType === 'join' && (
            <>
              {/* Show selected tables info */}
              {joinTables.some(jt => jt.table) && (
                <div className="mb-6 p-4 bg-purple-50 border border-purple-200 rounded-lg">
                  <div className="flex items-start gap-3">
                    <Table size={20} className="text-purple-600 mt-0.5 shrink-0" />
                    <div className="flex-1">
                      <p className="font-semibold text-purple-900 mb-2">Source Silver Tables Selected</p>
                      {joinTables.filter(jt => jt.table).map((jt, idx) => (
                        <div key={idx} className="mb-2">
                          <p className="text-sm text-purple-800">
                            <span className="font-bold">{jt.alias || `Table ${idx + 1}`}:</span>{' '}
                            <span className="font-mono bg-purple-100 px-2 py-0.5 rounded text-xs">
                              {silverTables.find(t => t.name === jt.table)?.full_name || jt.table}
                            </span>
                          </p>
                        </div>
                      ))}
                      <p className="text-xs text-purple-700 mt-2">
                        {joinTables.filter(jt => jt.table).length} tables ready for JOIN operation
                      </p>
                    </div>
                  </div>
                </div>
              )}
              
              <div className="mb-6">
                <div className="flex items-center justify-between mb-2">
                  <label className="block text-sm font-medium text-gray-700">
                    Tables to Join
                  </label>
                  <button
                    onClick={addJoinTable}
                    className="flex items-center gap-1 px-3 py-1 text-xs font-medium text-blue-600 bg-blue-50 rounded-lg hover:bg-blue-100"
                  >
                    <Plus size={14} />
                    Add Table
                  </button>
                </div>
                <div className="space-y-3">
                  {joinTables.map((jt, idx) => (
                    <div key={idx} className="flex items-center gap-2">
                      <select
                        value={jt.table}
                        onChange={(e) => updateJoinTable(idx, 'table', e.target.value)}
                        className="flex-1 px-3 py-2 border border-gray-300 rounded-lg text-sm"
                      >
                        <option value="">Select Silver table...</option>
                        {silverTables.map(table => (
                          <option key={table.name} value={table.name}>
                            {table.full_name} ({table.row_count?.toLocaleString()} rows)
                          </option>
                        ))}
                      </select>
                      <input
                        type="text"
                        value={jt.alias}
                        onChange={(e) => updateJoinTable(idx, 'alias', e.target.value)}
                        placeholder="Alias (e.g., t1)"
                        className="w-24 px-3 py-2 border border-gray-300 rounded-lg text-sm"
                      />
                      {joinTables.length > 1 && (
                        <button
                          onClick={() => removeJoinTable(idx)}
                          className="p-2 text-red-600 hover:bg-red-50 rounded-lg"
                        >
                          <Trash2 size={16} />
                        </button>
                      )}
                    </div>
                  ))}
                </div>
                {silverTables.length === 0 && (
                  <p className="text-sm text-gray-500 mt-2">
                    <Loader2 size={14} className="inline animate-spin mr-1" />
                    Loading Silver tables from MinIO...
                  </p>
                )}
              </div>

              <div className="mb-6">
                <div className="flex items-center justify-between mb-2">
                  <label className="block text-sm font-medium text-gray-700">
                    Join Conditions
                  </label>
                  <button
                    onClick={addJoinCondition}
                    className="flex items-center gap-1 px-3 py-1 text-xs font-medium text-blue-600 bg-blue-50 rounded-lg hover:bg-blue-100"
                  >
                    <Plus size={14} />
                    Add
                  </button>
                </div>
                <div className="space-y-3">
                  {joinConditions.map((jc, idx) => (
                    <div key={idx} className="flex items-center gap-2">
                      <input
                        type="text"
                        value={jc.left}
                        onChange={(e) => updateJoinCondition(idx, 'left', e.target.value)}
                        placeholder="e.g., a.id"
                        className="flex-1 px-3 py-2 border border-gray-300 rounded-lg text-sm"
                      />
                      <span className="text-gray-500">=</span>
                      <input
                        type="text"
                        value={jc.right}
                        onChange={(e) => updateJoinCondition(idx, 'right', e.target.value)}
                        placeholder="e.g., b.customer_id"
                        className="flex-1 px-3 py-2 border border-gray-300 rounded-lg text-sm"
                      />
                      <select
                        value={jc.type}
                        onChange={(e) => updateJoinCondition(idx, 'type', e.target.value)}
                        className="w-32 px-3 py-2 border border-gray-300 rounded-lg text-sm"
                      >
                        <option value="INNER">INNER</option>
                        <option value="LEFT">LEFT</option>
                        <option value="RIGHT">RIGHT</option>
                        <option value="FULL">FULL</option>
                      </select>
                      <button
                        onClick={() => removeJoinCondition(idx)}
                        className="p-2 text-red-600 hover:bg-red-50 rounded-lg"
                      >
                        <Trash2 size={16} />
                      </button>
                    </div>
                  ))}
                </div>
              </div>

              <div className="mb-6">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Select Columns (comma-separated)
                </label>
                <input
                  type="text"
                  value={selectColumns.join(', ')}
                  onChange={(e) => setSelectColumns(e.target.value.split(',').map(s => s.trim()).filter(Boolean))}
                  placeholder="e.g., a.customer_id, a.name, b.order_total"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>
            </>
          )}

          {/* Rollup Configuration */}
          {transformationType === 'rollup' && (
            <>
              <div className="mb-6">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Source Silver Table
                </label>
                <select
                  value={selectedTable}
                  onChange={(e) => handleTableSelect(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                >
                  <option value="">Select a Silver table...</option>
                  {silverTables.map(table => (
                    <option key={table.name} value={table.name}>
                      {table.full_name} ({table.row_count?.toLocaleString()} rows)
                    </option>
                  ))}
                </select>
                {silverTables.length === 0 && (
                  <p className="text-sm text-gray-500 mt-2">
                    <Loader2 size={14} className="inline animate-spin mr-1" />
                    Loading Silver tables from MinIO...
                  </p>
                )}
              </div>

              {/* Show selected table info */}
              {selectedTable && (
                <div className="mb-6 p-4 bg-green-50 border border-green-200 rounded-lg">
                  <div className="flex items-start gap-3">
                    <Calendar size={20} className="text-green-600 mt-0.5 shrink-0" />
                    <div className="flex-1">
                      <p className="font-semibold text-green-900 mb-1">Time-Series Rollup Source</p>
                      <p className="text-sm text-green-800">
                        <span className="font-mono bg-green-100 px-2 py-0.5 rounded">{silverTables.find(t => t.name === selectedTable)?.full_name}</span>
                      </p>
                      <p className="text-xs text-green-700 mt-2">
                        {silverTables.find(t => t.name === selectedTable)?.row_count?.toLocaleString()} rows • Rollup Level: {rollupLevel}
                      </p>
                    </div>
                  </div>
                </div>
              )}

              <div className="mb-6">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Date Column
                </label>
                <select
                  value={dateColumn}
                  onChange={(e) => setDateColumn(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                >
                  <option value="">Select date column...</option>
                  {availableColumns.map(col => (
                    <option key={col} value={col}>{col}</option>
                  ))}
                </select>
              </div>

              <div className="mb-6">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Rollup Level
                </label>
                <select
                  value={rollupLevel}
                  onChange={(e) => setRollupLevel(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                >
                  <option value="daily">Daily</option>
                  <option value="weekly">Weekly</option>
                  <option value="monthly">Monthly</option>
                  <option value="quarterly">Quarterly</option>
                  <option value="yearly">Yearly</option>
                </select>
              </div>

              <div className="mb-6">
                <div className="flex items-center justify-between mb-2">
                  <label className="block text-sm font-medium text-gray-700">
                    Metrics to Aggregate
                  </label>
                  <button
                    onClick={addMetric}
                    className="flex items-center gap-1 px-3 py-1 text-xs font-medium text-blue-600 bg-blue-50 rounded-lg hover:bg-blue-100"
                  >
                    <Plus size={14} />
                    Add Metric
                  </button>
                </div>
                <div className="space-y-3">
                  {metrics.map((metric, idx) => (
                    <div key={idx} className="flex items-center gap-2">
                      <select
                        value={metric.column}
                        onChange={(e) => updateMetric(idx, 'column', e.target.value)}
                        className="flex-1 px-3 py-2 border border-gray-300 rounded-lg text-sm"
                      >
                        <option value="">Select column...</option>
                        {availableColumns.map(col => (
                          <option key={col} value={col}>{col}</option>
                        ))}
                      </select>
                      <select
                        value={metric.function}
                        onChange={(e) => updateMetric(idx, 'function', e.target.value)}
                        className="w-32 px-3 py-2 border border-gray-300 rounded-lg text-sm"
                      >
                        <option value="SUM">SUM</option>
                        <option value="AVG">AVG</option>
                        <option value="COUNT">COUNT</option>
                        <option value="MIN">MIN</option>
                        <option value="MAX">MAX</option>
                      </select>
                      <input
                        type="text"
                        value={metric.alias}
                        onChange={(e) => updateMetric(idx, 'alias', e.target.value)}
                        placeholder="Alias"
                        className="w-40 px-3 py-2 border border-gray-300 rounded-lg text-sm"
                      />
                      <button
                        onClick={() => removeMetric(idx)}
                        className="p-2 text-red-600 hover:bg-red-50 rounded-lg"
                      >
                        <Trash2 size={16} />
                      </button>
                    </div>
                  ))}
                </div>
              </div>
            </>
          )}

          {/* Execute Button */}
          <div className="flex items-center justify-end gap-3 pt-6 border-t border-gray-200">
            <button
              onClick={resetForm}
              className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50"
            >
              Reset
            </button>
            <button
              onClick={executeTransformation}
              disabled={loading}
              className="flex items-center gap-2 px-6 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {loading ? (
                <>
                  <Loader2 size={16} className="animate-spin" />
                  Executing...
                </>
              ) : (
                <>
                  <Play size={16} />
                  Execute Transformation
                </>
              )}
            </button>
          </div>
        </div>

        {/* Info Panel */}
        <div className="space-y-6">
          {/* Transformation Type Info */}
          <div className="bg-gradient-to-br from-yellow-50 to-purple-50 border border-yellow-200 rounded-lg p-4">
            <h3 className="font-semibold text-gray-800 mb-3 flex items-center gap-2">
              <Table size={18} className="text-yellow-600" />
              {transformationType === 'aggregation' && 'Aggregation'}
              {transformationType === 'join' && 'Multi-Table Join'}
              {transformationType === 'rollup' && 'Time Rollup'}
            </h3>
            {transformationType === 'aggregation' && (
              <p className="text-sm text-gray-700 leading-relaxed">
                <strong>Aggregation</strong> creates summary tables by grouping rows and applying 
                aggregate functions (SUM, AVG, COUNT). Perfect for building business KPIs and 
                summary dashboards.
              </p>
            )}
            {transformationType === 'join' && (
              <p className="text-sm text-gray-700 leading-relaxed">
                <strong>Multi-Table Join</strong> combines multiple Silver tables into a 
                denormalized wide table. Use for creating dimensional models (fact tables) 
                and unified datasets for analytics.
              </p>
            )}
            {transformationType === 'rollup' && (
              <p className="text-sm text-gray-700 leading-relaxed">
                <strong>Time Rollup</strong> aggregates time-series data to different granularities 
                (daily→weekly→monthly). Ideal for trend analysis and performance reporting.
              </p>
            )}
          </div>

          {/* Example Use Cases */}
          <div className="bg-white border border-gray-200 rounded-lg p-4">
            <h3 className="font-semibold text-gray-800 mb-3">Example Use Cases</h3>
            <ul className="space-y-2 text-sm text-gray-700">
              {transformationType === 'aggregation' && (
                <>
                  <li className="flex items-start gap-2">
                    <span className="text-blue-600">•</span>
                    <span>Sales by region and product category</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <span className="text-blue-600">•</span>
                    <span>Customer lifetime value metrics</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <span className="text-blue-600">•</span>
                    <span>Inventory turnover by warehouse</span>
                  </li>
                </>
              )}
              {transformationType === 'join' && (
                <>
                  <li className="flex items-start gap-2">
                    <span className="text-blue-600">•</span>
                    <span>Customer 360° view (demographics + orders + support)</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <span className="text-blue-600">•</span>
                    <span>Fact table with all dimension foreign keys</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <span className="text-blue-600">•</span>
                    <span>Product catalog with supplier details</span>
                  </li>
                </>
              )}
              {transformationType === 'rollup' && (
                <>
                  <li className="flex items-start gap-2">
                    <span className="text-blue-600">•</span>
                    <span>Monthly revenue trends</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <span className="text-blue-600">•</span>
                    <span>Weekly active users (WAU)</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <span className="text-blue-600">•</span>
                    <span>Quarterly financial statements</span>
                  </li>
                </>
              )}
            </ul>
          </div>

          {/* Best Practices */}
          <div className="bg-white border border-gray-200 rounded-lg p-4">
            <h3 className="font-semibold text-gray-800 mb-3">Best Practices</h3>
            <ul className="space-y-2 text-sm text-gray-700">
              <li className="flex items-start gap-2">
                <span className="text-green-600">✓</span>
                <span>Use meaningful target table names (e.g., <code className="bg-gray-100 px-1 rounded">sales_monthly</code>)</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-600">✓</span>
                <span>Apply quality rules before transformation</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-600">✓</span>
                <span>Monitor job status for long-running operations</span>
              </li>
              <li className="flex items-start gap-2">
                <span className="text-green-600">✓</span>
                <span>Review lineage graph after transformation</span>
              </li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}
