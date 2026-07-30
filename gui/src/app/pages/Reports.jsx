import { useState, useEffect } from 'react'
import { FileText, Download, Database, GitBranch, Activity, TrendingUp, Calendar, Filter, RefreshCw, Loader2, AlertCircle, CheckCircle, XCircle, Clock, Zap, Layers } from 'lucide-react'
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend, PieChart, Pie, Cell, CartesianGrid } from 'recharts'
import axios from 'axios'
import Alert from '../components/ui/Alert'
import DataTable from '../components/tables/DataTable'

const API_BASE = 'http://localhost:8000/api'

const COLORS = {
  bronze: '#CD7F32',
  silver: '#6B7280',
  gold: '#B8860B',
  green: '#10b981',
  blue: '#3b82f6',
  amber: '#f59e0b',
  red: '#ef4444',
}

export default function Reports() {
  const [activeTab, setActiveTab] = useState('summary')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  
  // Data states
  const [lineageSummary, setLineageSummary] = useState(null)
  const [pipelineHistory, setPipelineHistory] = useState(null)
  const [recentTransformations, setRecentTransformations] = useState([])
  const [auditEvents, setAuditEvents] = useState([])
  const [qualityTrends, setQualityTrends] = useState([])
  
  // Filters
  const [days, setDays] = useState(7)
  const [eventCategory, setEventCategory] = useState('all')

  useEffect(() => {
    loadReportData()
  }, [days, eventCategory])

  const loadReportData = async () => {
    try {
      setLoading(true)
      setError(null)

      const [summaryRes, historyRes, transformRes, auditRes, qualityRes] = await Promise.all([
        axios.get(`${API_BASE}/reports/lineage/summary`).catch(e => ({ data: { success: false, summary: { total_transformations: 0, total_tables: 0, by_layer: [] } } })),
        axios.get(`${API_BASE}/reports/pipeline/history`, { params: { days } }).catch(e => ({ data: { success: false, history: { lineage_by_day: [], audit_by_day: [] } } })),
        axios.get(`${API_BASE}/reports/transformations/recent`, { params: { limit: 50 } }).catch(e => ({ data: { success: false, transformations: [] } })),
        axios.get(`${API_BASE}/reports/audit/events`, { 
          params: { 
            event_category: eventCategory === 'all' ? null : eventCategory,
            limit: 100 
          } 
        }).catch(e => ({ data: { success: false, events: [] } })),
        axios.get(`${API_BASE}/reports/quality/validation-history`, { params: { days: 30 } }).catch(e => ({ data: { success: false, validation_history: [] } }))
      ])

      setLineageSummary(summaryRes.data.summary)
      setPipelineHistory(historyRes.data.history)
      setRecentTransformations(transformRes.data.transformations || [])
      setAuditEvents(auditRes.data.events || [])
      setQualityTrends(qualityRes.data.validation_history || [])

    } catch (err) {
      console.error('Error loading report data:', err)
      setError(err.response?.data?.detail || 'Failed to load report data from PostgreSQL')
    } finally {
      setLoading(false)
    }
  }

  const handleDownloadReport = async (reportType) => {
    try {
      const response = await axios.get(`${API_BASE}/reports/comprehensive`, {
        params: { days },
        responseType: 'json'
      })
      
      const blob = new Blob([JSON.stringify(response.data, null, 2)], { type: 'application/json' })
      const url = window.URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `syniqai_report_${reportType}_${new Date().toISOString().split('T')[0]}.json`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      window.URL.revokeObjectURL(url)
    } catch (err) {
      console.error('Error downloading report:', err)
    }
  }
  
  if (loading) {
    return (
      <div className="flex items-center justify-center h-96 bg-gray-50">
        <div className="text-center">
          <Loader2 size={48} className="mx-auto text-blue-500 animate-spin mb-3" />
          <p className="text-gray-600">Loading reports from PostgreSQL...</p>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center p-6">
        <Alert type="error">
          <div className="space-y-3">
            <p className="font-medium">Failed to load reports</p>
            <p className="text-sm">{error}</p>
            <button onClick={loadReportData} className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700">
              Retry
            </button>
          </div>
        </Alert>
      </div>
    )
  }

  const tabs = [
    { id: 'summary', label: 'Summary', icon: FileText },
    { id: 'lineage', label: 'Data Lineage', icon: GitBranch },
    { id: 'audit', label: 'Audit Trail', icon: Activity },
    { id: 'quality', label: 'Quality Trends', icon: TrendingUp },
  ]

  // Prepare transformation flow data for visualization
  const transformationFlowData = lineageSummary?.by_layer?.map(item => ({
    name: `${item.source_layer} → ${item.target_layer}`,
    transformations: item.transformation_count,
    sources: item.unique_sources,
    targets: item.unique_targets
  })) || []

  // Prepare audit events by category
  const auditByCategory = {}
  auditEvents.forEach(event => {
    const cat = event.event_category || 'unknown'
    if (!auditByCategory[cat]) {
      auditByCategory[cat] = { total: 0, success: 0, failure: 0 }
    }
    auditByCategory[cat].total++
    if (event.status === 'success') auditByCategory[cat].success++
    if (event.status === 'failure') auditByCategory[cat].failure++
  })

  const auditChartData = Object.entries(auditByCategory).map(([category, stats]) => ({
    category,
    ...stats
  }))

  const transformationColumns = [
    { 
      key: 'created_at', 
      label: 'Timestamp',
      render: (val) => new Date(val).toLocaleString()
    },
    { 
      key: 'source_table', 
      label: 'Source',
      render: (val, row) => (
        <span className="font-mono text-xs px-2 py-1 rounded" style={{ 
          backgroundColor: `${COLORS[row.source_layer]}20`,
          color: COLORS[row.source_layer]
        }}>
          {val}
        </span>
      )
    },
    { key: 'transformation_type', label: 'Transform', render: (val) => <span className="font-semibold">{val}</span> },
    { 
      key: 'target_table', 
      label: 'Target',
      render: (val, row) => (
        <span className="font-mono text-xs px-2 py-1 rounded" style={{ 
          backgroundColor: `${COLORS[row.target_layer]}20`,
          color: COLORS[row.target_layer]
        }}>
          {val}
        </span>
      )
    },
  ]

  const auditColumns = [
    { 
      key: 'event_timestamp', 
      label: 'Time',
      render: (val) => new Date(val).toLocaleString()
    },
    { 
      key: 'event_type', 
      label: 'Event',
      render: (val) => <span className="font-medium text-gray-900">{val}</span>
    },
    { 
      key: 'event_category', 
      label: 'Category',
      render: (val) => <span className="text-xs px-2 py-1 bg-blue-100 text-blue-700 rounded">{val}</span>
    },
    { 
      key: 'status', 
      label: 'Status',
      render: (val) => (
        <span className="flex items-center gap-1">
          {val === 'success' ? <CheckCircle size={14} className="text-green-600" /> : <XCircle size={14} className="text-red-600" />}
          <span className={val === 'success' ? 'text-green-700' : 'text-red-700'}>{val}</span>
        </span>
      )
    },
    { key: 'resource_name', label: 'Resource' },
  ]

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      {/* Header */}
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-semibold text-gray-800">Comprehensive Reports</h1>
          <p className="text-sm text-gray-600 mt-1">
            End-to-end visibility: Bronze → Silver → Gold with CDC and Quality tracking from PostgreSQL
          </p>
        </div>
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2 text-sm text-gray-600">
            <Calendar size={16} />
            <select 
              value={days} 
              onChange={(e) => setDays(Number(e.target.value))}
              className="border border-gray-300 rounded px-2 py-1 text-sm"
            >
              <option value={1}>Last 24h</option>
              <option value={7}>Last 7 days</option>
              <option value={30}>Last 30 days</option>
              <option value={90}>Last 90 days</option>
            </select>
          </div>
          <button 
            onClick={loadReportData}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
          >
            <RefreshCw size={16} />
            Refresh
          </button>
          <button 
            onClick={() => handleDownloadReport('comprehensive')}
            className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition"
          >
            <Download size={16} />
            Export Report
          </button>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex items-center gap-2 mb-6 border-b border-gray-200 bg-white rounded-t-lg px-4">
        {tabs.map(tab => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            className={`flex items-center gap-2 px-4 py-3 border-b-2 transition ${
              activeTab === tab.id
                ? 'border-blue-500 text-blue-600 font-medium'
                : 'border-transparent text-gray-600 hover:text-gray-900'
            }`}
          >
            <tab.icon size={18} />
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      {activeTab === 'summary' && (
        <div className="space-y-6">
          {/* Empty State Info */}
          {(!lineageSummary?.total_transformations || lineageSummary.total_transformations === 0) && (
            <Alert type="info">
              <div className="flex items-center gap-3">
                <Database size={20} />
                <div>
                  <p className="font-medium">No lineage data yet</p>
                  <p className="text-sm mt-1">
                    Run transformations to populate reports. Navigate to <strong>Silver Processing</strong> or <strong>Gold Layer Processing</strong> to execute transformations.
                  </p>
                </div>
              </div>
            </Alert>
          )}
          
          {/* KPI Cards */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="bg-white rounded-lg border border-gray-200 p-4">
              <div className="flex items-center justify-between mb-2">
                <p className="text-sm text-gray-600">Total Transformations</p>
                <Zap size={20} className="text-blue-500" />
              </div>
              <p className="text-3xl font-bold text-gray-900">{lineageSummary?.total_transformations || 0}</p>
              <p className="text-xs text-gray-500 mt-1">Across all layers</p>
            </div>

            <div className="bg-white rounded-lg border border-gray-200 p-4">
              <div className="flex items-center justify-between mb-2">
                <p className="text-sm text-gray-600">Total Tables</p>
                <Database size={20} className="text-green-500" />
              </div>
              <p className="text-3xl font-bold text-gray-900">{lineageSummary?.total_tables || 0}</p>
              <p className="text-xs text-gray-500 mt-1">In lineage graph</p>
            </div>

            <div className="bg-white rounded-lg border border-gray-200 p-4">
              <div className="flex items-center justify-between mb-2">
                <p className="text-sm text-gray-600">Audit Events</p>
                <Activity size={20} className="text-amber-500" />
              </div>
              <p className="text-3xl font-bold text-gray-900">{auditEvents.length}</p>
              <p className="text-xs text-gray-500 mt-1">Last {days} days</p>
            </div>

            <div className="bg-white rounded-lg border border-gray-200 p-4">
              <div className="flex items-center justify-between mb-2">
                <p className="text-sm text-gray-600">Quality Checks</p>
                <TrendingUp size={20} className="text-purple-500" />
              </div>
              <p className="text-3xl font-bold text-gray-900">{qualityTrends.length}</p>
              <p className="text-xs text-gray-500 mt-1">Validation runs</p>
            </div>
          </div>

          {/* Transformation Flow Chart */}
          <div className="bg-white rounded-lg border border-gray-200 p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Transformation Flow by Layer</h3>
            <ResponsiveContainer width="100%" height={300}>
              <BarChart data={transformationFlowData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" tick={{ fontSize: 12 }} />
                <YAxis tick={{ fontSize: 12 }} />
                <Tooltip />
                <Legend />
                <Bar dataKey="transformations" fill={COLORS.blue} name="Transformations" />
                <Bar dataKey="sources" fill={COLORS.green} name="Source Tables" />
                <Bar dataKey="targets" fill={COLORS.amber} name="Target Tables" />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Audit Events by Category */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-white rounded-lg border border-gray-200 p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Audit Events by Category</h3>
              <ResponsiveContainer width="100%" height={250}>
                <PieChart>
                  <Pie
                    data={auditChartData}
                    dataKey="total"
                    nameKey="category"
                    cx="50%"
                    cy="50%"
                    outerRadius={80}
                    label={(entry) => `${entry.category}: ${entry.total}`}
                  >
                    {auditChartData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={[COLORS.blue, COLORS.green, COLORS.amber, COLORS.red, COLORS.gold][index % 5]} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            </div>

            <div className="bg-white rounded-lg border border-gray-200 p-6">
              <h3 className="text-lg font-semibold text-gray-900 mb-4">Success vs Failure Rate</h3>
              <div className="space-y-3">
                {auditChartData.map((item, idx) => (
                  <div key={idx}>
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-sm font-medium text-gray-700">{item.category}</span>
                      <span className="text-xs text-gray-500">{item.total} events</span>
                    </div>
                    <div className="w-full h-4 bg-gray-100 rounded-full overflow-hidden flex">
                      <div 
                        style={{ width: `${(item.success / item.total) * 100}%` }}
                        className="bg-green-500"
                        title={`${item.success} success`}
                      />
                      <div 
                        style={{ width: `${(item.failure / item.total) * 100}%` }}
                        className="bg-red-500"
                        title={`${item.failure} failures`}
                      />
                    </div>
                    <div className="flex items-center justify-between mt-1 text-xs">
                      <span className="text-green-600">{item.success} success</span>
                      <span className="text-red-600">{item.failure} failures</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      )}

      {activeTab === 'lineage' && (
        <div className="space-y-6">
          <div className="bg-white rounded-lg border border-gray-200 p-6">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold text-gray-900">Recent Transformations</h3>
              <span className="text-sm text-gray-500">{recentTransformations.length} records</span>
            </div>
            <DataTable 
              data={recentTransformations}
              columns={transformationColumns}
            />
          </div>
        </div>
      )}

      {activeTab === 'audit' && (
        <div className="space-y-6">
          <div className="bg-white rounded-lg border border-gray-200 p-6">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold text-gray-900">Audit Trail</h3>
              <div className="flex items-center gap-2">
                <Filter size={16} className="text-gray-500" />
                <select 
                  value={eventCategory}
                  onChange={(e) => setEventCategory(e.target.value)}
                  className="border border-gray-300 rounded px-3 py-1 text-sm"
                >
                  <option value="all">All Categories</option>
                  <option value="data_processing">Data Processing</option>
                  <option value="rule_management">Rule Management</option>
                  <option value="security">Security</option>
                  <option value="compliance">Compliance</option>
                </select>
              </div>
            </div>
            <DataTable 
              data={auditEvents}
              columns={auditColumns}
            />
          </div>
        </div>
      )}

      {activeTab === 'quality' && (
        <div className="space-y-6">
          <div className="bg-white rounded-lg border border-gray-200 p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Quality Validation History</h3>
            <div className="space-y-3">
              {qualityTrends.length > 0 ? (
                qualityTrends.map((item, idx) => (
                  <div key={idx} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                    <div className="flex items-center gap-3">
                      <Clock size={16} className="text-gray-400" />
                      <div>
                        <p className="font-medium text-gray-900">{item.resource_name}</p>
                        <p className="text-xs text-gray-500">{new Date(item.event_timestamp).toLocaleString()}</p>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      {item.status === 'success' ? (
                        <span className="flex items-center gap-1 text-green-600 font-medium">
                          <CheckCircle size={16} />
                          Passed
                        </span>
                      ) : (
                        <span className="flex items-center gap-1 text-red-600 font-medium">
                          <XCircle size={16} />
                          Failed
                        </span>
                      )}
                    </div>
                  </div>
                ))
              ) : (
                <div className="text-center py-12 text-gray-500">
                  <Activity size={48} className="mx-auto mb-3 text-gray-300" />
                  <p>No quality validation history available</p>
                  <p className="text-sm mt-1">Run transformations to build quality tracking history</p>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
