import { useState, useEffect, useCallback } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { Database, CheckCircle, Activity, RefreshCw, TrendingUp, ChevronRight, BarChart3, Info, Zap, PlayCircle, StopCircle, AlertCircle, Server } from 'lucide-react'
import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, Tooltip, LineChart, Line, CartesianGrid, Cell, ReferenceLine } from 'recharts'
import axios from 'axios'
import LoadingSpinner from '../components/ui/LoadingSpinner'
import Alert from '../components/ui/Alert'

const API_BASE = 'http://localhost:8000/api'

const HEALTH_COLORS = { Good: '#10b981', Warning: '#f59e0b', Error: '#ef4444' }
const HEALTH_THRESHOLDS = { good: 90, warning: 70 }

// ── Tooltip bubble shown on hover for metric cards ──────────────────────────
function InfoTooltip({ text }) {
  const [visible, setVisible] = useState(false)
  return (
    <span className="relative inline-block ml-1 align-middle">
      <Info
        size={14}
        className="text-gray-400 hover:text-blue-500 cursor-help"
        onMouseEnter={() => setVisible(true)}
        onMouseLeave={() => setVisible(false)}
      />
      {visible && (
        <span className="absolute z-50 bottom-6 left-1/2 -translate-x-1/2 w-56 bg-gray-900 text-white text-xs rounded-lg px-3 py-2 shadow-lg leading-relaxed pointer-events-none">
          {text}
          <span className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-gray-900" />
        </span>
      )}
    </span>
  )
}

// ── Custom tooltip for Quality Trend line chart ──────────────────────────────
function QualityTrendTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  const val = payload[0].value
  const status = val >= 90 ? { label: 'Excellent', color: '#10b981' }
               : val >= 70 ? { label: 'Good',      color: '#3b82f6' }
               : val >= 60 ? { label: 'Fair',       color: '#f59e0b' }
               :              { label: 'Poor',       color: '#ef4444' }
  return (
    <div className="bg-white border border-gray-200 rounded-lg shadow-lg px-4 py-3 text-sm">
      <p className="font-semibold text-gray-700 mb-1">{label}</p>
      <p className="text-gray-600">Quality: <span className="font-bold text-gray-900">{val.toFixed(1)}%</span></p>
      <p style={{ color: status.color }} className="font-semibold mt-1">▸ {status.label}</p>
      <p className="text-gray-400 text-xs mt-1">
        {val >= 90 ? 'All checks passing — data is highly reliable'
         : val >= 70 ? 'Minor issues present — data is usable'
         : val >= 60 ? 'Moderate issues — review recommended'
         : 'Critical issues — immediate attention required'}
      </p>
    </div>
  )
}

// ── Custom tooltip for Daily Health bar chart ────────────────────────────────
function HealthBarTooltip({ active, payload }) {
  if (!active || !payload?.length) return null
  const { status, count, percentage } = payload[0].payload
  const descriptions = {
    Good:    'Tables with quality score ≥ 90% — all data quality rules are passing.',
    Warning: 'Tables with quality score between 70–89% — minor issues detected, review advised.',
    Error:   'Tables with quality score below 70% — significant data quality problems found.',
  }
  const colors = { Good: '#10b981', Warning: '#f59e0b', Error: '#ef4444' }
  return (
    <div className="bg-white border border-gray-200 rounded-lg shadow-lg px-4 py-3 text-sm max-w-[220px]">
      <div className="flex items-center gap-2 mb-2">
        <div className="w-3 h-3 rounded-full" style={{ backgroundColor: colors[status] }} />
        <span className="font-semibold text-gray-800">{status}</span>
      </div>
      <p className="text-gray-700"><span className="font-bold">{count}</span> table{count !== 1 ? 's' : ''} ({percentage}%)</p>
      <p className="text-gray-400 text-xs mt-2 leading-relaxed">{descriptions[status]}</p>
    </div>
  )
}

export default function Dashboard() {
  const { domain } = useParams()
  const navigate = useNavigate()
  const [dashboardData, setDashboardData] = useState(null)
  const [bronzeTables, setBronzeTables] = useState([])
  const [silverTables, setSilverTables] = useState([])
  const [goldTables, setGoldTables] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [selectedTables, setSelectedTables] = useState(new Set())
  const [lastRefreshed, setLastRefreshed] = useState(null)
  const [cdcStatus, setCdcStatus] = useState(null)
  const [cdcHealth, setCdcHealth] = useState(null)
  const [cdcLoading, setCdcLoading] = useState(false)

  const loadDashboardData = useCallback(async () => {
    try {
      setLoading(true)
      setError(null)

      // Fetch all data in parallel from backend (MinIO)
      const [dashResponse, bronzeResponse, silverResponse, goldResponse, qualityResponse, cdcStatusResponse, cdcHealthResponse] = await Promise.all([
        axios.get(`${API_BASE}/dashboard-summary?domain=${domain}`).catch(() => ({ data: { total_tables: 0 } })),
        axios.get(`${API_BASE}/bronze/tables/${domain}`).catch(() => ({ data: { tables: [] } })),
        axios.get(`${API_BASE}/tables/silver`).catch(() => ({ data: { tables: [] } })),
        axios.get(`${API_BASE}/gold/tables`).catch(() => ({ data: { tables: [] } })),
        axios.get(`${API_BASE}/quality`).catch(() => ({ data: { quality_data: [] } })),
        axios.get(`${API_BASE}/cdc/consumer/status`).catch(() => ({ data: { consumer: { running: false } } })),
        axios.get(`${API_BASE}/cdc/consumer/health`).catch(() => ({ data: { overall_status: 'unknown', checks: {} } })),
      ])

      const bronzeList = bronzeResponse.data.tables || []
      const silverList = (silverResponse.data.tables || []).filter(t => t.domain === domain)
      const goldList = goldResponse.data.tables || []
      const qualityItems = qualityResponse.data.quality_data || []
      
      // Set CDC status - ensure safe access
      const consumerData = cdcStatusResponse?.data?.consumer || { running: false }
      const healthData = cdcHealthResponse?.data || { overall_status: 'unknown', checks: {} }
      setCdcStatus(consumerData)
      setCdcHealth(healthData)

      const allTables = [
        ...bronzeList.map(t => ({ ...t, layer: 'bronze' })),
        ...silverList.map(t => ({ ...t, layer: 'silver' })),
        ...goldList.map(t => ({ ...t, layer: 'gold' })),
      ]

      // Build a quality lookup by table name from backend
      const qualityMap = {}
      qualityItems.forEach(q => {
        const name = q.table?.split('.').pop() || q.table
        qualityMap[name] = q.score
      })

      const totalRecords = allTables.reduce((s, t) => s + (t.row_count || 0), 0)
      
      // If no tables exist in this domain, force everything to 0 (override backend data)
      const hasTables = allTables.length > 0
      const avgQuality = (hasTables && qualityItems.length > 0)
        ? (qualityItems.reduce((s, q) => s + q.score, 0) / qualityItems.length)
        : 0
      
      // Quality trend: only show if tables exist
      const qualityTrend = (hasTables && qualityItems.length > 0)
        ? qualityItems.slice(0, 30).map((q, i) => ({
            label: q.table?.split('.').pop() || `Table ${i + 1}`,
            quality: q.score,
            completeness: q.completeness || 0,
          }))
        : []
      
      // Health distribution: only calculate if tables exist
      let goodCount = 0, warnCount = 0, errCount = 0
      if (hasTables && qualityItems.length > 0) {
        qualityItems.forEach(q => {
          if (q.score >= HEALTH_THRESHOLDS.good) goodCount++
          else if (q.score >= HEALTH_THRESHOLDS.warning) warnCount++
          else errCount++
        })
      }
      const healthTotal = goodCount + warnCount + errCount || 1
      const dailyHealth = (hasTables && (goodCount + warnCount + errCount) > 0) ? [
        { status: 'Good',    count: goodCount, percentage: Math.round(goodCount / healthTotal * 100), color: HEALTH_COLORS.Good },
        { status: 'Warning', count: warnCount,  percentage: Math.round(warnCount / healthTotal * 100),  color: HEALTH_COLORS.Warning },
        { status: 'Error',   count: errCount,   percentage: Math.round(errCount / healthTotal * 100),   color: HEALTH_COLORS.Error },
      ] : []
      
      // Data health = percentage of tables with quality >= 90% (only if tables exist)
      const goodPct = (hasTables && qualityItems.length > 0) ? Math.round(goodCount / qualityItems.length * 100) : 0

      setDashboardData({
        totalTables: allTables.length || dashResponse.data.total_tables || 0,
        avgQuality: avgQuality.toFixed(1),
        dataHealth: goodPct,
        totalRecords,
        qualityTrend,
        dailyHealth,
        allTables,
        qualityMap,
        bronzeCount: dashResponse.data.bronze_tables || bronzeList.length,
        silverCount: dashResponse.data.silver_tables || silverList.length,
        goldCount:   dashResponse.data.gold_tables   || goldList.length,
      })
      setBronzeTables(bronzeList)
      setSilverTables(silverList)
      setGoldTables(goldList)
      setLastRefreshed(new Date().toLocaleTimeString())
    } catch (err) {
      console.error('Error loading dashboard:', err)
      const isNetworkError = err.code === 'ERR_NETWORK' || err.message?.includes('Network Error')
      const errorMessage = isNetworkError 
        ? 'Cannot connect to backend (http://localhost:8000). Please start the backend: .\\start_syniqai.ps1'
        : `Failed to load data from MinIO: ${err.response?.data?.detail || err.message}`
      setError(errorMessage)
    } finally {
      setLoading(false)
    }
  }, [domain])

  useEffect(() => { loadDashboardData() }, [loadDashboardData])

  const handleStartCDC = async () => {
    setCdcLoading(true)
    try {
      const response = await axios.post(`${API_BASE}/cdc/consumer/start`)
      if (response.data.success) {
        await loadDashboardData()
      } else {
        alert(`CDC Start Failed: ${response.data.error || 'Unknown error'}\n\nMake sure Kafka is running on localhost:9092`)
      }
    } catch (err) {
      console.error('Failed to start CDC:', err)
      const msg = err.response?.data?.detail || err.message
      alert(`Failed to start CDC consumer:\n\n${msg}\n\nChecklist:\n✓ Kafka running on localhost:9092\n✓ Backend is running\n✓ CDC consumer script exists`)
    } finally {
      setCdcLoading(false)
    }
  }

  const handleStopCDC = async () => {
    setCdcLoading(true)
    try {
      await axios.post(`${API_BASE}/cdc/consumer/stop`)
      await loadDashboardData()
    } catch (err) {
      console.error('Failed to stop CDC:', err)
      alert(`Failed to stop CDC consumer: ${err.response?.data?.detail || err.message}`)
    } finally {
      setCdcLoading(false)
    }
  }

  const toggleTableSelection = (tableName) => {
    const next = new Set(selectedTables)
    next.has(tableName) ? next.delete(tableName) : next.add(tableName)
    setSelectedTables(next)
  }

  const formatNumber = (num) => {
    if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`
    if (num >= 1_000) return `${(num / 1_000).toFixed(1)}K`
    return String(num)
  }

  if (loading) return <LoadingSpinner size="lg" />
  
  if (error) return (
    <div className="min-h-screen bg-gray-50 flex items-center justify-center p-6">
      <div className="max-w-xl w-full">
        <Alert type="error">
          <div className="space-y-3">
            <p className="font-medium">Failed to load dashboard data</p>
            <p className="text-sm">{error}</p>
            <div className="flex gap-3 pt-2">
              <button 
                onClick={loadDashboardData}
                className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700">
                Retry
              </button>
              <button 
                onClick={() => window.open('http://localhost:8000/docs', '_blank')}
                className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50">
                Check Backend Status
              </button>
            </div>
          </div>
        </Alert>
      </div>
    </div>
  )

  const data = dashboardData || {
    totalTables: 0, avgQuality: 0, dataHealth: 0, totalRecords: 0,
    qualityTrend: [], dailyHealth: [], allTables: [], qualityMap: {},
    bronzeCount: 0, silverCount: 0, goldCount: 0,
  }

  const qualityNum = parseFloat(data.avgQuality)
  const qualityStatus = qualityNum >= 90 ? { label: 'Excellent', color: 'text-green-600' }
                      : qualityNum >= 70 ? { label: 'Good',      color: 'text-blue-600' }
                      : qualityNum >= 60 ? { label: 'Fair',       color: 'text-yellow-600' }
                      :                    { label: 'Poor',        color: 'text-red-600' }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 px-8 py-5">
        <div className="flex items-center justify-between max-w-[1400px] mx-auto">
          <div>
            <h1 className="text-2xl font-semibold text-gray-900 capitalize">
              {domain} — Data Quality Monitor
            </h1>
            <p className="text-sm text-gray-400 mt-0.5 flex items-center gap-2">
              <span className="flex items-center gap-1">
                <span className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></span>
                Live data from MinIO via backend
              </span>
              · Last refreshed: {lastRefreshed || '—'}
            </p>
          </div>
          <div className="flex items-center gap-3">
            <button onClick={() => navigate(`/${domain}/ingestion`)}
              className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50">
              Add Data
            </button>
            <button onClick={() => navigate(`/${domain}/quality`)}
              className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700">
              View Quality
            </button>
            <button onClick={loadDashboardData}
              className="px-4 py-2 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 flex items-center gap-2">
              <RefreshCw size={16} /> Refresh
            </button>
          </div>
        </div>
      </div>

      <div className="max-w-[1400px] mx-auto px-8 py-6 space-y-6">

        {/* ── CDC Status Widget ────────────────────────────────────────────── */}
        <div className="bg-gradient-to-r from-blue-50 to-indigo-50 rounded-xl border-2 border-blue-200 p-5">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <div className={`p-3 rounded-xl ${
                cdcStatus?.running ? 'bg-green-500 animate-pulse' : 'bg-gray-300'
              }`}>
                <Zap size={24} className="text-white" />
              </div>
              <div>
                <h3 className="text-lg font-bold text-gray-900 flex items-center gap-2">
                  CDC Streaming Pipeline
                  {cdcStatus?.running ? (
                    <span className="px-2 py-1 text-xs font-semibold text-green-700 bg-green-100 rounded-full animate-pulse">
                      LIVE
                    </span>
                  ) : (
                    <span className="px-2 py-1 text-xs font-semibold text-gray-600 bg-gray-200 rounded-full">
                      OFFLINE
                    </span>
                  )}
                </h3>
                <div className="text-sm text-gray-700 mt-1">
                  {cdcStatus?.running ? (
                    <div className="space-y-0.5">
                      <p><strong>Status:</strong> <span className="text-green-600">Streaming Active</span></p>
                      <p><strong>Uptime:</strong> {Math.floor((cdcStatus.uptime_seconds || 0) / 60)} minutes · <strong>PID:</strong> {cdcStatus.pid}</p>
                      <p><strong>Resources:</strong> {cdcStatus.memory_mb?.toFixed(1)} MB RAM · {cdcStatus.cpu_percent?.toFixed(1)}% CPU</p>
                    </div>
                  ) : (
                    <div>
                      <p><strong>Status:</strong> <span className="text-red-600">Consumer Stopped</span></p>
                      <p className="text-gray-500 text-xs mt-1">CDC consumer not active · No real-time data streaming</p>
                    </div>
                  )}
                </div>
              </div>
            </div>
            
            <div className="flex items-center gap-3">
              {/* System Health Indicators */}
              <div className="flex items-center gap-2 mr-4">
                {cdcHealth?.checks?.kafka && (
                  <div className="flex items-center gap-1.5" title="Kafka Status">
                    <Server size={16} className={cdcHealth.checks.kafka.status === 'up' ? 'text-green-500' : 'text-red-500'} />
                    <span className="text-xs font-medium text-gray-600">Kafka</span>
                  </div>
                )}
                {cdcHealth?.checks?.minio && (
                  <div className="flex items-center gap-1.5" title="MinIO Status">
                    <Database size={16} className={cdcHealth.checks.minio.status === 'up' ? 'text-green-500' : 'text-red-500'} />
                    <span className="text-xs font-medium text-gray-600">MinIO</span>
                  </div>
                )}
                {cdcHealth?.checks?.postgres && (
                  <div className="flex items-center gap-1.5" title="PostgreSQL Status">
                    <Activity size={16} className={cdcHealth.checks.postgres.status === 'up' ? 'text-green-500' : 'text-red-500'} />
                    <span className="text-xs font-medium text-gray-600">PostgreSQL</span>
                  </div>
                )}
              </div>
              
              {/* View Details Button */}
              <button 
                onClick={() => navigate(`/${domain}/cdc`)}
                className="px-5 py-2.5 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 flex items-center gap-2">
                View Details →
              </button>
            </div>
          </div>
          
          {/* Bronze Metrics - Only show when CDC is running */}
          {cdcStatus?.running && (
            <div className="grid grid-cols-4 gap-4 mt-4 pt-4 border-t border-blue-200">
              <div className="text-center">
                <p className="text-xs text-gray-500">Tables Streaming</p>
                <p className="text-2xl font-bold text-gray-900 mt-1">{data.bronzeCount || 0}</p>
              </div>
              <div className="text-center">
                <p className="text-xs text-gray-500">Total Records</p>
                <p className="text-2xl font-bold text-gray-900 mt-1">{formatNumber(data.totalRecords || 0)}</p>
              </div>
              <div className="text-center">
                <p className="text-xs text-gray-500">Avg Quality</p>
                <p className="text-2xl font-bold text-gray-900 mt-1">{data.avgQuality}%</p>
              </div>
              <div className="text-center">
                <p className="text-xs text-gray-500">Last Update</p>
                <p className="text-sm font-semibold text-gray-700 mt-2">{lastRefreshed || '—'}</p>
              </div>
            </div>
          )}
        </div>

        {/* ── KPI Cards ───────────────────────────────────────────────────── */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">

          {/* Total Tables */}
          <div className="bg-white rounded-xl border border-gray-200 p-6 hover:shadow-md transition-shadow">
            <div className="flex items-start justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500 flex items-center">
                  Total Tables
                  <InfoTooltip text="Total number of data tables currently tracked across all layers (Bronze, Silver, Gold) for this domain." />
                </p>
                <p className="text-3xl font-bold text-gray-900 mt-2">{data.totalTables}</p>
                <p className="text-xs text-gray-400 mt-2">
                  Bronze {data.bronzeCount} · Silver {data.silverCount} · Gold {data.goldCount}
                </p>
              </div>
              <div className="p-2 bg-blue-50 rounded-lg"><Database size={20} className="text-blue-600" /></div>
            </div>
          </div>

          {/* Quality Score */}
          <div className="bg-white rounded-xl border border-gray-200 p-6 hover:shadow-md transition-shadow">
            <div className="flex items-start justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500 flex items-center">
                  Quality Score
                  <InfoTooltip text="Average data quality score across all tables. Scores above 90% = Excellent, 70–89% = Good, 60–69% = Fair, below 60% = Poor." />
                </p>
                <p className="text-3xl font-bold text-gray-900 mt-2">{data.avgQuality}%</p>
                <p className={`text-xs font-semibold mt-2 ${qualityStatus.color}`}>
                  ▸ {qualityStatus.label}
                </p>
              </div>
              <div className="p-2 bg-green-50 rounded-lg"><CheckCircle size={20} className="text-green-600" /></div>
            </div>
          </div>

          {/* Data Health */}
          <div className="bg-white rounded-xl border border-gray-200 p-6 hover:shadow-md transition-shadow">
            <div className="flex items-start justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500 flex items-center">
                  Data Health
                  <InfoTooltip text="Percentage of tables classified as 'Good' (quality ≥ 90%). A high percentage means most of your data is reliable and ready to use." />
                </p>
                <p className="text-3xl font-bold text-gray-900 mt-2">{data.dataHealth}%</p>
                <p className={`text-xs font-semibold mt-2 ${data.dataHealth >= 80 ? 'text-green-600' : data.dataHealth >= 60 ? 'text-yellow-600' : 'text-red-600'}`}>
                  {data.dataHealth >= 80 ? '↑ Healthy' : data.dataHealth >= 60 ? '⚠ Moderate' : '↓ Needs attention'}
                </p>
              </div>
              <div className="p-2 bg-emerald-50 rounded-lg"><Activity size={20} className="text-emerald-600" /></div>
            </div>
          </div>

          {/* Total Records */}
          <div className="bg-white rounded-xl border border-gray-200 p-6 hover:shadow-md transition-shadow">
            <div className="flex items-start justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500 flex items-center">
                  Total Records
                  <InfoTooltip text="Sum of all rows across every tracked table in this domain. This reflects the total volume of data ingested and stored." />
                </p>
                <p className="text-3xl font-bold text-gray-900 mt-2">{formatNumber(data.totalRecords)}</p>
                <p className="text-xs text-gray-400 mt-2">Rows across all tables</p>
              </div>
              <div className="p-2 bg-purple-50 rounded-lg"><BarChart3 size={20} className="text-purple-600" /></div>
            </div>
          </div>
        </div>

        {/* ── Charts Row ──────────────────────────────────────────────────── */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

          {/* Quality Trend */}
          <div className="bg-white rounded-xl border border-gray-200 p-6">
            <div className="flex items-center justify-between mb-1">
              <h3 className="text-base font-semibold text-gray-900 flex items-center">
                Quality Trend
                <InfoTooltip text="Shows the quality score for each tracked table. Hover over any point to see the table name, exact score, and quality rating. Dashed lines mark the Good (70%) and Excellent (90%) thresholds." />
              </h3>
              <span className="text-xs text-gray-400 bg-gray-100 px-2 py-1 rounded-full">Live from backend</span>
            </div>
            <p className="text-xs text-gray-400 mb-4">Hover over a point to see details · Dashed lines = quality thresholds</p>

            {data.qualityTrend.length > 0 ? (
              <>
                <ResponsiveContainer width="100%" height={240}>
                  <LineChart data={data.qualityTrend} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                    <XAxis dataKey="label" stroke="#9ca3af" tick={{ fontSize: 11 }}
                      tickFormatter={v => v.length > 10 ? v.slice(0, 10) + '…' : v} />
                    <YAxis stroke="#9ca3af" tick={{ fontSize: 11 }} domain={[0, 100]} tickFormatter={v => `${v}%`} />
                    <ReferenceLine y={90} stroke="#10b981" strokeDasharray="4 4" label={{ value: 'Excellent', position: 'insideTopRight', fontSize: 10, fill: '#10b981' }} />
                    <ReferenceLine y={70} stroke="#f59e0b" strokeDasharray="4 4" label={{ value: 'Good', position: 'insideTopRight', fontSize: 10, fill: '#f59e0b' }} />
                    <Tooltip content={<QualityTrendTooltip />} />
                    <Line type="monotone" dataKey="quality" stroke="#3b82f6" strokeWidth={2.5} dot={{ r: 3, fill: '#3b82f6' }} activeDot={{ r: 6, fill: '#2563eb', stroke: '#fff', strokeWidth: 2 }} />
                  </LineChart>
                </ResponsiveContainer>
                <div className="flex items-center gap-6 mt-4 pt-3 border-t border-gray-100 text-xs text-gray-500">
                  <span className="flex items-center gap-1.5"><span className="w-6 border-t-2 border-blue-500 inline-block" /> Quality score</span>
                  <span className="flex items-center gap-1.5"><span className="w-6 border-t-2 border-dashed border-green-500 inline-block" /> Excellent (90%)</span>
                  <span className="flex items-center gap-1.5"><span className="w-6 border-t-2 border-dashed border-yellow-500 inline-block" /> Good (70%)</span>
                </div>
              </>
            ) : (
              <div className="flex items-center justify-center h-[240px] text-gray-400">
                <div className="text-center">
                  <TrendingUp size={44} className="mx-auto mb-2 opacity-20" />
                  <p className="font-medium">No quality data yet</p>
                  <p className="text-sm mt-1">Ingest tables to see quality trends</p>
                </div>
              </div>
            )}
          </div>

          {/* Daily Health */}
          <div className="bg-white rounded-xl border border-gray-200 p-6">
            <div className="flex items-center justify-between mb-1">
              <h3 className="text-base font-semibold text-gray-900 flex items-center">
                Daily Health
                <InfoTooltip text="Distribution of tables by health status today. Good = quality ≥ 90%, Warning = 70–89%, Error = below 70%. Hover over bars for details." />
              </h3>
              <span className="text-xs text-gray-400 bg-gray-100 px-2 py-1 rounded-full">Live from backend</span>
            </div>
            <p className="text-xs text-gray-400 mb-4">Hover over bars for status description and count</p>

            {(data.dailyHealth[0]?.count + data.dailyHealth[1]?.count + data.dailyHealth[2]?.count) > 0 ? (
              <>
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={data.dailyHealth} layout="vertical" margin={{ top: 4, right: 16, bottom: 4, left: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" horizontal={false} />
                    <XAxis type="number" stroke="#9ca3af" tick={{ fontSize: 11 }} allowDecimals={false} />
                    <YAxis type="category" dataKey="status" stroke="#9ca3af" tick={{ fontSize: 12 }} width={70} />
                    <Tooltip content={<HealthBarTooltip />} cursor={{ fill: '#f9fafb' }} />
                    <Bar dataKey="count" radius={[0, 8, 8, 0]} barSize={28}>
                      {data.dailyHealth.map((entry, i) => (
                        <Cell key={i} fill={entry.color} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>

                {/* Status legend with counts */}
                <div className="grid grid-cols-3 gap-3 mt-4 pt-4 border-t border-gray-100">
                  {data.dailyHealth.map(({ status, count, percentage, color }) => (
                    <div key={status} className="text-center">
                      <div className="flex items-center justify-center gap-1.5 mb-1">
                        <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: color }} />
                        <span className="text-xs text-gray-500">{status}</span>
                      </div>
                      <p className="text-xl font-bold text-gray-900">{count}</p>
                      <p className="text-xs text-gray-400">{percentage}% of total</p>
                    </div>
                  ))}
                </div>
              </>
            ) : (
              <div className="flex items-center justify-center h-[240px] text-gray-400">
                <div className="text-center">
                  <Activity size={44} className="mx-auto mb-2 opacity-20" />
                  <p className="font-medium">No health data yet</p>
                  <p className="text-sm mt-1">Ingest tables to see health metrics</p>
                </div>
              </div>
            )}
          </div>
        </div>

        {/* ── Today's Snapshot ────────────────────────────────────────────── */}
        <div className="bg-white rounded-xl border border-gray-200">
          <div className="px-6 py-4 border-b border-gray-200">
            <div className="flex items-start justify-between">
              <div>
                <h3 className="text-base font-semibold text-gray-900 flex items-center">
                  Today's Snapshot
                  <InfoTooltip text="A live list of all tables currently being monitored in this domain. It shows each table's data layer (Bronze = raw, Silver = cleaned, Gold = analytics-ready), record count, and quality score from the backend. Click any row to explore it in detail." />
                </h3>
                <p className="text-sm text-gray-400 mt-0.5">
                  A point-in-time view of all monitored tables · {data.allTables.length} table{data.allTables.length !== 1 ? 's' : ''} tracked · Click a row to explore
                </p>
              </div>
              <div className="flex items-center gap-3">
                <button onClick={() => navigate(`/${domain}/quality`)}
                  className="px-3 py-1.5 text-sm font-medium text-gray-700 bg-white border border-gray-300 rounded-lg hover:bg-gray-50">
                  View Quality
                </button>
                <button onClick={() => navigate(`/${domain}/ingestion`)}
                  className="px-3 py-1.5 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700">
                  Add Data
                </button>
              </div>
            </div>
          </div>

          {/* Layer tabs */}
          <div className="px-6 py-3 bg-gray-50 border-b border-gray-200">
            <div className="flex gap-2">
              {[
                { label: 'Bronze', count: bronzeTables.length, color: 'bg-orange-500', path: 'bronze', desc: 'Raw ingested data' },
                { label: 'Silver', count: silverTables.length, color: 'bg-blue-500', path: 'silver', desc: 'Cleaned & transformed' },
                { label: 'Gold',   count: goldTables.length,   color: 'bg-yellow-500', path: 'eda',   desc: 'Analytics-ready' },
              ].map(({ label, count, color, path, desc }) => (
                <button key={label} onClick={() => navigate(`/${domain}/${path}`)}
                  title={desc}
                  className="px-4 py-2 text-sm font-medium text-gray-700 hover:text-blue-600 hover:bg-blue-50 rounded-lg transition-colors flex items-center gap-2">
                  <div className={`w-2 h-2 rounded-full ${color}`} />
                  {label} ({count})
                </button>
              ))}
            </div>
          </div>

          {/* Table list */}
          <div className="max-h-[380px] overflow-y-auto divide-y divide-gray-100">
            {data.allTables.length === 0 ? (
              <div className="px-6 py-12 text-center">
                <Database size={44} className="mx-auto mb-3 text-gray-300" />
                <p className="text-gray-600 font-medium">No tables found for this domain</p>
                <p className="text-sm text-gray-400 mt-1">Ingest data first to populate the snapshot</p>
                <button onClick={() => navigate(`/${domain}/ingestion`)}
                  className="mt-4 px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700">
                  Ingest Data
                </button>
              </div>
            ) : data.allTables.map((table, idx) => {
              const name = table.table_name || table.name || '—'
              const score = data.qualityMap[name] ?? null
              const scoreColor = score == null ? 'text-gray-400'
                               : score >= 90 ? 'text-green-600'
                               : score >= 70 ? 'text-blue-600'
                               : 'text-red-600'
              const layerBadge = table.layer === 'bronze' ? 'bg-orange-100 text-orange-700'
                               : table.layer === 'silver' ? 'bg-blue-100 text-blue-700'
                               : 'bg-yellow-100 text-yellow-800'
              return (
                <div key={idx}
                  className="px-6 py-4 hover:bg-gray-50 transition-colors cursor-pointer group"
                  onClick={() => navigate(`/${domain}/eda?table=${name}`)}>
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4 flex-1 min-w-0">
                      <input type="checkbox"
                        checked={selectedTables.has(name)}
                        onChange={e => { e.stopPropagation(); toggleTableSelection(name) }}
                        className="w-4 h-4 rounded border-gray-300 text-blue-600 focus:ring-blue-500 shrink-0" />
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 flex-wrap">
                          <h4 className="font-medium text-gray-900 truncate">{name}</h4>
                          <span className={`px-2 py-0.5 text-xs font-medium rounded-full shrink-0 ${layerBadge}`}>{table.layer}</span>
                        </div>
                        <p className="text-sm text-gray-400 mt-0.5">
                          {(table.row_count || 0).toLocaleString()} records
                          {table.source_type ? ` · ${table.source_type}` : ''}
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-4 shrink-0">
                      <div className="text-right">
                        <p className="text-xs text-gray-400">Quality</p>
                        <p className={`text-sm font-bold ${scoreColor}`}>
                          {score != null ? `${score.toFixed(1)}%` : 'N/A'}
                        </p>
                      </div>
                      <ChevronRight size={18} className="text-gray-300 group-hover:text-gray-500" />
                    </div>
                  </div>
                </div>
              )
            })}
          </div>
        </div>

        {/* ── Quick Actions ────────────────────────────────────────────────── */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {[
            { label: 'Ingest New Data', desc: 'Add tables to Bronze layer', icon: Database, color: 'blue', path: 'ingestion' },
            { label: 'Quality Reports', desc: 'View detailed metrics',       icon: CheckCircle, color: 'green', path: 'quality' },
            { label: 'Process to Silver', desc: 'Clean and transform data', icon: TrendingUp, color: 'purple', path: 'silver' },
          ].map(({ label, desc, icon: Icon, color, path }) => (
            <button key={path} onClick={() => navigate(`/${domain}/${path}`)}
              className={`bg-white border-2 border-dashed border-gray-300 rounded-xl p-6 hover:border-${color}-400 hover:bg-${color}-50 transition-all group text-left`}>
              <Icon size={22} className={`mb-2 text-gray-400 group-hover:text-${color}-600`} />
              <p className="font-medium text-gray-900">{label}</p>
              <p className="text-sm text-gray-500 mt-1">{desc}</p>
            </button>
          ))}
        </div>

      </div>
    </div>
  )
}
