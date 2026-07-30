import React, { useState, useEffect, useCallback } from 'react';
import axios from 'axios';
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid,
} from 'recharts';
import {
  BarChart3, RefreshCw, Sparkles, AlertCircle, Database, Lightbulb,
} from 'lucide-react';

const API_BASE = 'http://localhost:8000/api';

const COLORS = { blue: '#2563eb', amber: '#b45309', green: '#16a34a' };

function correlationColor(value) {
  const v = Math.abs(value);
  if (value === 1 || (Number.isNaN(value))) return { bg: '#F3F4F6', fg: '#9CA3AF' };
  if (v >= 0.9) return { bg: '#92400E', fg: '#fff' };
  if (v >= 0.7) return { bg: '#B45309', fg: '#fff' };
  if (v >= 0.5) return { bg: '#D97706', fg: '#fff' };
  if (v >= 0.2) return { bg: value >= 0 ? '#FDE68A' : '#BFDBFE', fg: '#374151' };
  return { bg: '#F3F4F6', fg: '#9CA3AF' };
}

export default function SilverEDA() {
  const [tables, setTables] = useState([]);
  const [selected, setSelected] = useState(null);
  const [edaData, setEdaData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    (async () => {
      try {
        const res = await axios.get(`${API_BASE}/silver/tables`);
        setTables(res.data.tables || []);
      } catch (err) {
        setError('Failed to load Silver tables. Ensure Bronze→Silver transformation has run.');
      }
    })();
  }, []);

  const loadEDA = useCallback(async (source, entity) => {
    setLoading(true);
    setError(null);
    try {
      const res = await axios.get(`${API_BASE}/gold/eda/${source}/${entity}/viz`);
      setEdaData(res.data);
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to load EDA — try generating a fresh report.');
      setEdaData(null);
    } finally {
      setLoading(false);
    }
  }, []);

  const generateEDA = useCallback(async (source, entity) => {
    setGenerating(true);
    setError(null);
    try {
      await axios.post(`${API_BASE}/gold/eda/generate`, null, { params: { source, entity } });
      await loadEDA(source, entity);
    } catch (err) {
      setError(err.response?.data?.detail || 'Failed to generate EDA report');
    } finally {
      setGenerating(false);
    }
  }, [loadEDA]);

  const handleSelect = (table) => {
    const source = table.source || table.source_type;
    let entity = table.entity || table.table_name;
    if (entity && source && entity.startsWith(`${source}_`)) {
      entity = entity.substring(source.length + 1);
    }
    setSelected({ ...table, source, entity });
    loadEDA(source, entity);
  };

  const distributions = edaData?.distributions && !edaData.distributions.message ? edaData.distributions : null;
  const correlations = edaData?.correlations && !edaData.correlations.message ? edaData.correlations : null;
  const corrColumns = correlations ? Object.keys(correlations.correlation_matrix || {}) : [];

  return (
    <div className="h-full w-full flex bg-gray-50">
      {/* Table list sidebar */}
      <div className="w-72 border-r border-gray-200 bg-white overflow-y-auto flex-shrink-0">
        <div className="px-4 py-3 border-b border-gray-200">
          <h3 className="text-sm font-semibold text-gray-900 flex items-center gap-2">
            <Database className="w-4 h-4" /> Silver Tables
          </h3>
        </div>
        <div className="divide-y divide-gray-100">
          {tables.length === 0 && (
            <div className="px-4 py-6 text-xs text-gray-400">No Silver tables found yet.</div>
          )}
          {tables.map((t, i) => {
            const isActive = selected && (selected.table_name === t.table_name);
            return (
              <button
                key={i}
                onClick={() => handleSelect(t)}
                className={`w-full text-left px-4 py-3 text-sm hover:bg-gray-50 ${isActive ? 'bg-blue-50 border-l-2 border-blue-600' : ''}`}
              >
                <div className="font-medium text-gray-900 truncate">{t.table_name || t.entity}</div>
                <div className="text-xs text-gray-500">{t.source || t.source_type}</div>
              </button>
            );
          })}
        </div>
      </div>

      {/* Main panel */}
      <div className="flex-1 overflow-y-auto">
        <div className="bg-white border-b border-gray-200 px-6 py-4 flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
              <BarChart3 className="w-5 h-5 text-blue-600" /> Silver EDA
            </h2>
            <p className="text-sm text-gray-500">
              Real statistics on freshly-transformed Silver output — nulls, distributions, correlations, outliers.
            </p>
          </div>
          {selected && (
            <button
              onClick={() => generateEDA(selected.source, selected.entity)}
              disabled={generating}
              className="flex items-center gap-1.5 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white text-sm font-medium px-3 py-1.5 rounded-md"
            >
              <RefreshCw className={`w-4 h-4 ${generating ? 'animate-spin' : ''}`} />
              {generating ? 'Generating…' : 'Regenerate'}
            </button>
          )}
        </div>

        <div className="p-6 space-y-6">
          {error && (
            <div className="flex items-center gap-2 bg-red-50 border border-red-200 text-red-700 text-sm px-4 py-3 rounded-md">
              <AlertCircle className="w-4 h-4 flex-shrink-0" />
              {error}
            </div>
          )}

          {!selected && !error && (
            <div className="text-sm text-gray-400">Select a Silver table on the left to view its EDA.</div>
          )}

          {loading && <div className="text-sm text-gray-400">Loading…</div>}

          {edaData && !loading && (
            <>
              {/* Stat tiles */}
              <div className="grid grid-cols-4 gap-4">
                {[
                  { label: 'Total Rows', value: edaData.metrics?.total_rows?.toLocaleString() ?? '—' },
                  { label: 'Columns', value: edaData.metrics?.total_columns ?? '—' },
                  { label: 'Completeness', value: `${(edaData.metrics?.completeness ?? 0).toFixed(1)}%` },
                  { label: 'Quality Score', value: `${(edaData.metrics?.quality_score ?? 0).toFixed(1)}` },
                ].map((tile) => (
                  <div key={tile.label} className="bg-white rounded-xl border p-4">
                    <div className="text-xs text-gray-500">{tile.label}</div>
                    <div className="text-2xl font-semibold text-gray-900 mt-1">{tile.value}</div>
                  </div>
                ))}
              </div>

              {/* Distributions (real histogram data) */}
              <div>
                <h3 className="text-sm font-semibold text-gray-900 mb-3">Distributions</h3>
                {!distributions && (
                  <div className="text-xs text-gray-400 bg-white border rounded-xl p-4">
                    {edaData.distributions?.message || 'No numeric columns to plot.'}
                  </div>
                )}
                {distributions && (
                  <div className="grid grid-cols-2 gap-4">
                    {Object.entries(distributions).map(([col, dist]) => {
                      const chartData = (dist.histogram?.counts || []).map((count, i) => ({
                        bin: `${dist.histogram.bin_edges[i]?.toFixed(1)}`,
                        count,
                      }));
                      return (
                        <div key={col} className="bg-white rounded-xl border p-4">
                          <div className="flex items-baseline justify-between mb-2">
                            <span className="font-mono text-sm font-semibold text-gray-900">{col}</span>
                            <span className="text-xs text-gray-500">{dist.distribution_type}</span>
                          </div>
                          <ResponsiveContainer width="100%" height={140}>
                            <BarChart data={chartData}>
                              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                              <XAxis dataKey="bin" tick={{ fontSize: 9 }} />
                              <YAxis tick={{ fontSize: 9 }} />
                              <Tooltip />
                              <Bar dataKey="count" fill={COLORS.blue} radius={[2, 2, 0, 0]} />
                            </BarChart>
                          </ResponsiveContainer>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>

              {/* Correlation heatmap (real matrix) */}
              <div>
                <h3 className="text-sm font-semibold text-gray-900 mb-3">Correlation</h3>
                {!correlations && (
                  <div className="text-xs text-gray-400 bg-white border rounded-xl p-4">
                    {edaData.correlations?.message || 'Insufficient numeric columns for correlation.'}
                  </div>
                )}
                {correlations && (
                  <div className="bg-white rounded-xl border p-4 overflow-x-auto">
                    <table className="border-collapse">
                      <thead>
                        <tr>
                          <th className="p-2" />
                          {corrColumns.map((col) => (
                            <th key={col} className="p-2 text-xs font-mono text-gray-600 text-center">{col}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {corrColumns.map((rowCol) => (
                          <tr key={rowCol}>
                            <td className="p-2 text-xs font-mono text-gray-600 text-right pr-3">{rowCol}</td>
                            {corrColumns.map((colCol) => {
                              const val = correlations.correlation_matrix[rowCol]?.[colCol] ?? 0;
                              const { bg, fg } = correlationColor(rowCol === colCol ? 1 : val);
                              return (
                                <td
                                  key={colCol}
                                  className="p-0 border border-gray-200 text-center font-mono text-xs"
                                  style={{ background: bg, color: fg, width: 45, height: 32 }}
                                  title={`${rowCol} × ${colCol}: ${val.toFixed(2)}`}
                                >
                                  {rowCol === colCol ? '—' : val.toFixed(2)}
                                </td>
                              );
                            })}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {correlations.strong_correlations?.length > 0 && (
                      <div className="mt-4 space-y-2">
                        {correlations.strong_correlations.map((c, i) => (
                          <div key={i} className="text-xs text-gray-600 flex items-center gap-2">
                            <span className="font-mono font-semibold">{c.column1} × {c.column2}</span>
                            <span style={{ color: COLORS.amber }}>{c.correlation.toFixed(2)}</span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                )}
              </div>

              {/* Insights */}
              {edaData.insights?.length > 0 && (
                <div>
                  <h3 className="text-sm font-semibold text-gray-900 mb-3 flex items-center gap-2">
                    <Sparkles className="w-4 h-4 text-amber-500" /> Insights
                  </h3>
                  <div className="space-y-2">
                    {edaData.insights.map((insight, i) => (
                      <div key={i} className="flex items-start gap-2 bg-white border rounded-lg p-3 text-sm text-gray-700">
                        <Lightbulb className="w-4 h-4 text-amber-500 flex-shrink-0 mt-0.5" />
                        {typeof insight === 'string' ? insight : insight.message || JSON.stringify(insight)}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
