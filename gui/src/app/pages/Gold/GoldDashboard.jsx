import React, { useState, useEffect } from 'react';
import axios from 'axios';
import { 
  Database, 
  TrendingUp, 
  Zap, 
  AlertCircle,
  Loader2,
  RefreshCw,
  Sparkles,
  Shield,
  Table as TableIcon
} from 'lucide-react';

const API_BASE = 'http://localhost:8000/api';

export default function GoldDashboard() {
  const [goldTables, setGoldTables] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [refreshing, setRefreshing] = useState(false);

  const loadGoldTables = async () => {
    try {
      setError(null);
      const response = await axios.get(`${API_BASE}/gold/tables`);
      setGoldTables(response.data.tables || []);
    } catch (err) {
      console.error('Error loading Gold tables:', err);
      setError(err.response?.data?.detail || 'Failed to load Gold tables');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    loadGoldTables();
  }, []);

  const handleRefresh = () => {
    setRefreshing(true);
    loadGoldTables();
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96 bg-gray-50">
        <div className="text-center">
          <Loader2 size={48} className="mx-auto text-blue-500 animate-spin mb-3" />
          <p className="text-gray-600">Loading Gold layer...</p>
        </div>
      </div>
    );
  }

  // Calculate statistics
  const totalTables = goldTables.length;
  const totalRows = goldTables.reduce((sum, t) => sum + (t.row_count || 0), 0);
  const avgQualityScore = goldTables.length > 0
    ? goldTables.reduce((sum, t) => sum + (t.quality_score || 0), 0) / goldTables.length
    : 0;

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-semibold text-gray-800">Gold Layer Overview</h1>
            <p className="text-sm text-gray-500 mt-1">
              Business-ready analytics and dimensional models
            </p>
          </div>
          <button
            onClick={handleRefresh}
            disabled={refreshing}
            className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-blue-600 bg-blue-50 border border-blue-200 rounded-lg hover:bg-blue-100 disabled:opacity-50 transition-colors"
          >
            <RefreshCw size={16} className={refreshing ? 'animate-spin' : ''} />
            Refresh
          </button>
        </div>
      </div>

      {/* Error Alert */}
      {error && (
        <div className="mb-6 bg-red-50 border border-red-200 rounded-lg p-4 flex items-start gap-3">
          <AlertCircle size={20} className="text-red-600 shrink-0 mt-0.5" />
          <div className="flex-1">
            <p className="font-medium text-red-900">Failed to load Gold tables</p>
            <p className="text-sm text-red-800 mt-1">{error}</p>
          </div>
        </div>
      )}

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-6">
        {/* Total Tables */}
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center gap-3">
            <div className="w-12 h-12 bg-yellow-100 rounded-lg flex items-center justify-center">
              <Database className="text-yellow-600" size={24} />
            </div>
            <div>
              <p className="text-2xl font-bold text-gray-900">{totalTables}</p>
              <p className="text-sm text-gray-600">Gold Tables</p>
            </div>
          </div>
        </div>

        {/* Total Records */}
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center gap-3">
            <div className="w-12 h-12 bg-purple-100 rounded-lg flex items-center justify-center">
              <TrendingUp className="text-purple-600" size={24} />
            </div>
            <div>
              <p className="text-2xl font-bold text-gray-900">{totalRows.toLocaleString()}</p>
              <p className="text-sm text-gray-600">Total Records</p>
            </div>
          </div>
        </div>

        {/* Average Quality */}
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center gap-3">
            <div className="w-12 h-12 bg-green-100 rounded-lg flex items-center justify-center">
              <Sparkles className="text-green-600" size={24} />
            </div>
            <div>
              <p className="text-2xl font-bold text-gray-900">{avgQualityScore.toFixed(1)}%</p>
              <p className="text-sm text-gray-600">Avg Quality Score</p>
            </div>
          </div>
        </div>
      </div>

      {/* Gold Tables List */}
      <div className="bg-white rounded-lg border border-gray-200">
        <div className="p-6 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-800">Gold Tables</h2>
          <p className="text-sm text-gray-500 mt-1">
            Business intelligence and analytics-ready datasets
          </p>
        </div>

        {goldTables.length === 0 ? (
          <div className="p-12 text-center">
            <TableIcon size={48} className="mx-auto text-gray-300 mb-4" />
            <p className="text-gray-600 mb-2">No Gold tables yet</p>
            <p className="text-sm text-gray-500">
              Create aggregations and dimensional models from Silver layer
            </p>
          </div>
        ) : (
          <div className="p-6">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {goldTables.map((table, idx) => (
                <div
                  key={idx}
                  className="bg-gray-50 border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow"
                >
                  <div className="flex items-start justify-between mb-3">
                    <div className="flex items-center gap-2">
                      <div className="w-8 h-8 bg-yellow-100 rounded flex items-center justify-center">
                        <Zap className="text-yellow-600" size={16} />
                      </div>
                      <div>
                        <h3 className="font-semibold text-gray-900 text-sm">
                          {table.table_name || table.entity}
                        </h3>
                        <p className="text-xs text-gray-500">
                          {table.source_type || table.source || 'aggregated'}
                        </p>
                      </div>
                    </div>
                  </div>

                  <div className="space-y-2">
                    <div className="flex justify-between text-xs">
                      <span className="text-gray-500">Records</span>
                      <span className="text-gray-900 font-medium">
                        {(table.row_count || 0).toLocaleString()}
                      </span>
                    </div>
                    <div className="flex justify-between text-xs">
                      <span className="text-gray-500">Columns</span>
                      <span className="text-gray-900 font-medium">
                        {table.columns || 0}
                      </span>
                    </div>
                    {table.last_updated && (
                      <div className="flex justify-between text-xs">
                        <span className="text-gray-500">Last Updated</span>
                        <span className="text-gray-900 font-medium">
                          {new Date(table.last_updated).toLocaleDateString()}
                        </span>
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Quick Actions */}
      <div className="mt-6 bg-gradient-to-r from-yellow-50 to-purple-50 border border-yellow-200 rounded-lg p-6">
        <h3 className="text-lg font-semibold text-gray-800 mb-3">Quick Start Guide</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <div className="flex items-center gap-2 mb-2">
              <Zap className="text-yellow-600" size={20} />
              <h4 className="font-semibold text-gray-800">Create Aggregations</h4>
            </div>
            <p className="text-sm text-gray-600">
              Build summary tables with GROUP BY, SUM, AVG, COUNT operations from Silver layer
            </p>
          </div>

          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <div className="flex items-center gap-2 mb-2">
              <TableIcon className="text-purple-600" size={20} />
              <h4 className="font-semibold text-gray-800">Build Star Schema</h4>
            </div>
            <p className="text-sm text-gray-600">
              Create fact and dimension tables for business intelligence and reporting
            </p>
          </div>

          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <div className="flex items-center gap-2 mb-2">
              <Shield className="text-green-600" size={20} />
              <h4 className="font-semibold text-gray-800">Monitor Quality</h4>
            </div>
            <p className="text-sm text-gray-600">
              Track data quality scores, completeness, and validation across all layers
            </p>
          </div>

          <div className="bg-white rounded-lg p-4 border border-gray-200">
            <div className="flex items-center gap-2 mb-2">
              <Sparkles className="text-blue-600" size={20} />
              <h4 className="font-semibold text-gray-800">Explore Analytics</h4>
            </div>
            <p className="text-sm text-gray-600">
              Perform exploratory analysis, detect schemas, and visualize distributions
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
