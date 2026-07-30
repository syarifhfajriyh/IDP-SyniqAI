import { useState, useEffect, useCallback } from 'react'
import { AlertTriangle, CheckCircle, Info, RefreshCw, TrendingUp, AlertCircle, Loader2, Download, Settings } from 'lucide-react'
import { ScatterChart, Scatter, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend, BarChart, Bar, LineChart, Line, CartesianGrid, PieChart, Pie, Cell } from 'recharts'
import { useParams } from 'react-router-dom'
import axios from 'axios'
import DataTable from '../../components/tables/DataTable'
import Alert from '../../components/ui/Alert'
import QualityBadge from '../../components/cards/QualityBadge'

const API_BASE = 'http://localhost:8000/api'

const SCORE_COLORS = {
  excellent: '#10b981',
  good: '#3b82f6',
  fair: '#f59e0b',
  poor: '#ef4444',
}

// Mock data for demonstration
const MOCK_QUALITY_DATA = [
  {
    table: 'mariadb.patient_records',
    source: 'mariadb',
    entity: 'patient_records',
    score: 94.5,
    completeness: 98.2,
    missing: 145,
    totalRows: 8500,
    columns: 24,
    processedAt: '2026-02-27T10:30:00Z',
  },
  {
    table: 'postgres.user_transactions',
    source: 'postgres',
    entity: 'user_transactions',
    score: 87.3,
    completeness: 92.5,
    missing: 892,
    totalRows: 12450,
    columns: 18,
    processedAt: '2026-02-27T10:28:00Z',
  },
  {
    table: 'mongodb.clickstream',
    source: 'mongodb',
    entity: 'clickstream',
    score: 76.8,
    completeness: 85.3,
    missing: 2340,
    totalRows: 15800,
    columns: 32,
    processedAt: '2026-02-27T10:25:00Z',
  },
  {
    table: 'mariadb.hosp_raya_patient',
    source: 'mariadb',
    entity: 'hosp_raya_patient',
    score: 91.2,
    completeness: 96.7,
    missing: 234,
    totalRows: 7200,
    columns: 28,
    processedAt: '2026-02-27T10:20:00Z',
  },
  {
    table: 's3.customer_data',
    source: 's3',
    entity: 'customer_data',
    score: 82.5,
    completeness: 89.4,
    missing: 1567,
    totalRows: 14200,
    columns: 22,
    processedAt: '2026-02-27T10:15:00Z',
  },
  {
    table: 'postgres.inventory_logs',
    source: 'postgres',
    entity: 'inventory_logs',
    score: 68.4,
    completeness: 78.9,
    missing: 3421,
    totalRows: 16200,
    columns: 15,
    processedAt: '2026-02-27T10:10:00Z',
  },
  {
    table: 'mongodb.user_profiles',
    source: 'mongodb',
    entity: 'user_profiles',
    score: 95.8,
    completeness: 99.1,
    missing: 87,
    totalRows: 9800,
    columns: 20,
    processedAt: '2026-02-27T10:05:00Z',
  },
  {
    table: 'mariadb.order_details',
    source: 'mariadb',
    entity: 'order_details',
    score: 73.2,
    completeness: 82.6,
    missing: 2145,
    totalRows: 12300,
    columns: 26,
    processedAt: '2026-02-27T10:00:00Z',
  },
]

// Helper function to get week number from date
const getWeekNumber = (date) => {
  const d = new Date(date)
  const startOfYear = new Date(d.getFullYear(), 0, 1)
  const days = Math.floor((d - startOfYear) / (24 * 60 * 60 * 1000))
  return Math.ceil((days + startOfYear.getDay() + 1) / 7)
}

// Helper function to format week label
const getWeekLabel = (date) => {
  const d = new Date(date)
  const week = getWeekNumber(d)
  return `W${week}`
}

// ── Tooltip bubble for metric cards ─────────────────────────────────────────
function InfoTooltip({ text }) {
  const [vis, setVis] = useState(false)
  return (
    <span className="relative inline-block ml-1 align-middle">
      <Info
        size={13}
        className="text-gray-400 hover:text-blue-500 cursor-help"
        onMouseEnter={() => setVis(true)}
        onMouseLeave={() => setVis(false)}
      />
      {vis && (
        <span className="absolute z-50 bottom-5 left-1/2 -translate-x-1/2 w-60 bg-gray-900 text-white text-xs rounded-lg px-3 py-2 shadow-xl leading-relaxed pointer-events-none">
          {text}
          <span className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-gray-900" />
        </span>
      )}
    </span>
  )
}

// ── Star Schema Diagram ──────────────────────────────────────────────────────
function StarSchema() {
  const [hovered, setHovered] = useState(null)

  const fact = {
    id: 'fact_sales', label: 'fact_sales',
    cx: 312, cy: 165, x: 232, y: 130, w: 160, h: 70,
    fields: ['sale_id (PK)', 'date_key (FK)', 'customer_key (FK)', 'product_key (FK)', 'location_key (FK)', 'channel_key (FK)', 'amount', 'quantity'],
    description: 'Central fact table — stores measurable business events (sales transactions) and foreign key references to all dimension tables.',
  }

  const dims = [
    { id: 'dim_date',     label: 'dim_date',    cx: 312, cy: 38,  x: 235, y: 12,  w: 154, h: 52,
      fields: ['date_key (PK)', 'full_date', 'day', 'month', 'quarter', 'year', 'fiscal_week'],
      description: 'Time dimension — supports filtering and grouping by any time granularity (day, week, month, quarter, year).' },
    { id: 'dim_customer', label: 'dim_customer', cx: 92,  cy: 108, x: 8,   y: 80,  w: 168, h: 56,
      fields: ['customer_key (PK)', 'customer_id', 'name', 'segment', 'region', 'join_date'],
      description: 'Customer dimension — stores customer demographics, segments, and onboarding dates.' },
    { id: 'dim_product',  label: 'dim_product',  cx: 534, cy: 108, x: 448, y: 80,  w: 172, h: 56,
      fields: ['product_key (PK)', 'product_id', 'name', 'category', 'brand', 'unit_price'],
      description: 'Product dimension — product hierarchy, brand, category, and pricing attributes.' },
    { id: 'dim_location', label: 'dim_location', cx: 92,  cy: 244, x: 8,   y: 216, w: 168, h: 56,
      fields: ['location_key (PK)', 'store_id', 'city', 'state', 'country', 'region'],
      description: 'Location dimension — geographic hierarchy for regional and store-level analysis.' },
    { id: 'dim_channel',  label: 'dim_channel',  cx: 534, cy: 244, x: 448, y: 216, w: 172, h: 56,
      fields: ['channel_key (PK)', 'channel_id', 'type', 'platform', 'sub_channel'],
      description: 'Sales channel dimension — how and where the sale was made (online, in-store, mobile, etc.).' },
  ]

  const info = hovered ? (hovered === 'fact_sales' ? fact : dims.find(d => d.id === hovered)) : null

  return (
    <div className="bg-white rounded border border-gray-200 p-4">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-1">
          <h3 className="text-sm font-medium text-gray-700">Relational Schema — Star Model</h3>
          <InfoTooltip text="A star schema places a central fact table (measures) surrounded by denormalised dimension tables (context). All joins are direct fact → dim, enabling fast aggregation queries with no complex joins." />
        </div>
        <span className="text-xs text-gray-400 bg-gray-50 border border-gray-200 px-2 py-0.5 rounded">Data Warehouse Model</span>
      </div>
      <div className="flex flex-col lg:flex-row gap-4">
        <svg viewBox="0 0 630 300" className="w-full lg:w-2/3" style={{ minHeight: 230 }}>
          {dims.map(d => (
            <line key={d.id}
              x1={fact.cx} y1={fact.cy} x2={d.cx} y2={d.cy}
              stroke={hovered === d.id || hovered === 'fact_sales' ? '#6366f1' : '#cbd5e1'}
              strokeWidth={hovered === d.id ? 2 : 1.5}
              strokeDasharray={hovered === d.id ? '6 3' : undefined}
            />
          ))}
          {dims.map(d => (
            <g key={d.id} style={{ cursor: 'pointer' }}
              onMouseEnter={() => setHovered(d.id)}
              onMouseLeave={() => setHovered(null)}>
              <rect x={d.x} y={d.y} width={d.w} height={d.h} rx={6}
                fill={hovered === d.id ? '#f1f5f9' : '#f8fafc'}
                stroke={hovered === d.id ? '#64748b' : '#e2e8f0'}
                strokeWidth={hovered === d.id ? 1.8 : 1}
              />
              <text x={d.cx} y={d.y + 16} textAnchor="middle" fontSize={9.5} fontWeight="600" fill="#475569" fontFamily="monospace">{d.label}</text>
              <text x={d.cx} y={d.y + 28} textAnchor="middle" fontSize={8} fill="#94a3b8" fontFamily="monospace">{d.fields[0]}</text>
              <text x={d.cx} y={d.y + 38} textAnchor="middle" fontSize={7.5} fill="#94a3b8" fontFamily="monospace">{d.fields.slice(1, 3).join(', ')}</text>
              <text x={d.cx} y={d.y + 47} textAnchor="middle" fontSize={7.5} fill="#94a3b8" fontFamily="monospace">{d.fields.slice(3, 5).join(', ')}</text>
            </g>
          ))}
          <g style={{ cursor: 'pointer' }}
            onMouseEnter={() => setHovered('fact_sales')}
            onMouseLeave={() => setHovered(null)}>
            <rect x={fact.x} y={fact.y} width={fact.w} height={fact.h} rx={8}
              fill={hovered === 'fact_sales' ? '#eff6ff' : '#dbeafe'}
              stroke={hovered === 'fact_sales' ? '#2563eb' : '#3b82f6'}
              strokeWidth={hovered === 'fact_sales' ? 2 : 1.5}
            />
            <text x={fact.cx} y={fact.y + 15} textAnchor="middle" fontSize={11} fontWeight="700" fill="#1d4ed8" fontFamily="monospace">{fact.label}</text>
            <text x={fact.cx} y={fact.y + 27} textAnchor="middle" fontSize={8} fill="#60a5fa" fontFamily="monospace">{fact.fields[0]}</text>
            <text x={fact.cx} y={fact.y + 37} textAnchor="middle" fontSize={8} fill="#60a5fa" fontFamily="monospace">{fact.fields.slice(1, 4).join(' · ')}</text>
            <text x={fact.cx} y={fact.y + 47} textAnchor="middle" fontSize={8} fill="#60a5fa" fontFamily="monospace">{fact.fields.slice(4, 6).join(' · ')}</text>
            <text x={fact.cx} y={fact.y + 57} textAnchor="middle" fontSize={8} fill="#60a5fa" fontFamily="monospace">{fact.fields.slice(6).join(' · ')}</text>
          </g>
          <g transform="translate(8,280)">
            <rect width={13} height={9} rx={2} fill="#dbeafe" stroke="#3b82f6" strokeWidth={1}/>
            <text x={17} y={8} fontSize={8} fill="#64748b" fontFamily="sans-serif">Fact Table</text>
            <rect x={78} width={13} height={9} rx={2} fill="#f8fafc" stroke="#e2e8f0" strokeWidth={1}/>
            <text x={95} y={8} fontSize={8} fill="#64748b" fontFamily="sans-serif">Dimension Table</text>
            <line x1={184} y1={4.5} x2={200} y2={4.5} stroke="#cbd5e1" strokeWidth={1.5}/>
            <text x={204} y={8} fontSize={8} fill="#64748b" fontFamily="sans-serif">FK join</text>
          </g>
        </svg>
        <div className="lg:w-1/3 bg-gray-50 rounded-lg p-3 border border-gray-100 text-xs min-h-[120px] flex flex-col gap-1.5">
          {info ? (
            <>
              <p className="font-mono font-semibold text-gray-800">{info.label}</p>
              <p className="text-gray-500 leading-relaxed">{info.description}</p>
              <p className="font-semibold text-gray-600 mt-1">Columns:</p>
              <ul className="space-y-0.5">
                {info.fields.map((f, i) => (
                  <li key={i} className={`font-mono ${f.includes('PK') ? 'text-yellow-600' : f.includes('FK') ? 'text-blue-500' : 'text-gray-500'}`}>{f}</li>
                ))}
              </ul>
            </>
          ) : (
            <p className="text-gray-400 italic text-center mt-6">Hover a table to see its columns</p>
          )}
        </div>
      </div>
      <p className="text-xs text-gray-400 mt-2">
        <span className="font-semibold text-yellow-600">PK</span> = Primary Key &nbsp;·&nbsp;
        <span className="font-semibold text-blue-500">FK</span> = Foreign Key &nbsp;·&nbsp;
        All dimension tables join directly to the fact table (no multi-hop joins).
      </p>
    </div>
  )
}

export default function QualityMonitoring() {
  const { domain } = useParams()
  const [silverTables, setSilverTables] = useState([])
  const [qualityData, setQualityData] = useState(MOCK_QUALITY_DATA)
  const [qualityHistory, setQualityHistory] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  // Save quality snapshot to history (fromBackend = true when data came from the API).
  // Persisted server-side (quality_history_snapshots table) instead of localStorage,
  // so history survives cache clears and is shared across users/sessions.
  const saveQualitySnapshot = useCallback(async (data, fromBackend = false) => {
    if (data.length === 0 || !fromBackend) return  // don't persist mock snapshots

    const avgScore = data.reduce((sum, d) => sum + d.score, 0) / data.length
    const avgCompleteness = data.reduce((sum, d) => sum + d.completeness, 0) / data.length
    const totalMissing = data.reduce((sum, d) => sum + d.missing, 0)

    try {
      await axios.post(`${API_BASE}/quality-rules/history`, {
        domain: domain || 'finance',
        week_label: getWeekLabel(new Date()),
        quality_score: avgScore,
        completeness: avgCompleteness,
        missing_values: totalMissing,
        table_count: data.length,
      })
      await loadQualityHistory()
    } catch (e) {
      console.error('Failed to save quality history snapshot:', e)
    }
  }, [domain])

  const loadQualityHistory = useCallback(async () => {
    try {
      const res = await axios.get(`${API_BASE}/quality-rules/history`, {
        params: { domain: domain || 'finance', limit: 12 },
      })
      const mapped = (res.data.history || []).map(s => ({
        timestamp: s.recorded_at,
        week: s.week_label,
        qualityScore: s.quality_score,
        completeness: s.completeness,
        missingValues: s.missing_values,
        tableCount: s.table_count,
        fromBackend: true,
      }))
      setQualityHistory(mapped)
    } catch (e) {
      console.error('Failed to load quality history:', e)
    }
  }, [domain])

  // Load real history from the backend on mount
  useEffect(() => {
    loadQualityHistory()
  }, [loadQualityHistory])

  const loadQualityData = useCallback(async () => {
    try {
      setLoading(true)
      setError(null)

      // Fetch quality data + silver table metadata from backend (MinIO)
      const [qualityResponse, silverResponse] = await Promise.all([
        axios.get(`${API_BASE}/quality`),
        axios.get(`${API_BASE}/tables/silver`),
      ])

      const qualityItems = qualityResponse.data.quality_data || []
      const tables = silverResponse.data.tables || []

      // Build a metadata lookup by "source.entity" key
      const metaMap = {}
      tables.forEach(t => { metaMap[`${t.source}.${t.entity}`] = t })

      setSilverTables(tables)

      if (qualityItems.length > 0) {
        const mapped = qualityItems.map(item => {
          const dotIdx = item.table.indexOf('.')
          const source = dotIdx > -1 ? item.table.slice(0, dotIdx) : item.table
          const entity = dotIdx > -1 ? item.table.slice(dotIdx + 1) : item.table
          const meta = metaMap[item.table] || {}
          const totalRows = meta.row_count || 0
          return {
            table: item.table,
            source,
            entity,
            score: item.score || 0,
            completeness: item.completeness || 0,
            // backend returns missing as %; convert to count when totalRows is known
            missing: totalRows > 0 ? Math.round((item.missing / 100) * totalRows) : item.missing,
            totalRows,
            columns: meta.columns || 0,
            processedAt: meta.last_updated || new Date().toISOString(),
          }
        })
        setQualityData(mapped)
        saveQualitySnapshot(mapped, true)   // ← real backend data from MinIO
      } else {
        // No Silver layer data yet - use mock data for demo but warn user
        console.warn('No quality data from backend - using mock data for demonstration')
        setQualityData(MOCK_QUALITY_DATA)
        saveQualitySnapshot(MOCK_QUALITY_DATA, false)
      }
    } catch (err) {
      console.error('Error loading quality data:', err)
      const isNetworkError = err.code === 'ERR_NETWORK' || err.message?.includes('Network Error')
      
      if (isNetworkError) {
        // Backend not reachable
        setError('Cannot connect to backend (http://localhost:8000). Please start backend: .\\start_syniqai.ps1')
        setQualityData([])
      } else {
        // Backend returned an error
        const errorMsg = err.response?.data?.detail || err.message
        setError(`Failed to load quality data from MinIO: ${errorMsg}. Using mock data for demonstration.`)
        // Show mock data but with error banner
        setQualityData(MOCK_QUALITY_DATA)
      }
    } finally {
      setLoading(false)
    }
  }, [domain, saveQualitySnapshot])

  // Load real data on mount; falls back to mock data if backend is unavailable
  useEffect(() => {
    loadQualityData()
  }, [loadQualityData])

  // Auto-refresh every 5 minutes to build history
  useEffect(() => {
    const interval = setInterval(() => {
      if (qualityData.length > 0) {
        loadQualityData()
      }
    }, 5 * 60 * 1000) // 5 minutes

    return () => clearInterval(interval)
  }, [loadQualityData, qualityData.length])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96 bg-gray-50">
        <div className="text-center">
          <Loader2 size={48} className="mx-auto text-blue-500 animate-spin mb-3" />
          <p className="text-gray-600">Loading quality metrics...</p>
        </div>
      </div>
    )
  }

  if (error && qualityData.length === 0) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center p-6">
        <div className="max-w-xl w-full">
          <Alert type="error">
            <div className="space-y-3">
              <p className="font-medium">Failed to load quality data</p>
              <p className="text-sm">{error}</p>
              <div className="flex gap-3 pt-2">
                <button 
                  onClick={loadQualityData}
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
  }

  // Calculate quality distribution
  const qualityDistribution = {
    excellent: qualityData.filter(d => d.score >= 90).length,
    good: qualityData.filter(d => d.score >= 75 && d.score < 90).length,
    fair: qualityData.filter(d => d.score >= 60 && d.score < 75).length,
    poor: qualityData.filter(d => d.score < 60).length,
    total: qualityData.length,
  }

  // Generate alerts
  const alerts = qualityData
    .filter(d => d.score < 80 || d.completeness < 90)
    .map(d => ({
      severity: d.score < 60 ? '[CRITICAL]' : d.score < 75 ? '[WARNING]' : '[INFO]',
      table: d.table,
      issue: d.score < 75 
        ? `Low quality score (${d.score.toFixed(1)})`
        : `Completeness below 90% (${d.completeness.toFixed(1)}%)`,
      action: d.score < 75 
        ? 'Review data transformation rules'
        : 'Check data source completeness',
    }))

  // Scatter data for visualization
  const scatterData = qualityData.map(d => ({
    name: d.entity,
    completeness: d.completeness,
    quality: d.score,
    rows: d.totalRows,
  }))

  const tableColumns = [
    { 
      key: 'table', 
      label: 'Table', 
      render: (val, row) => {
        const hasIssues = row.missing > 1000 || row.score < 75
        return (
          <div className="flex items-center gap-2">
            {hasIssues && <AlertTriangle size={14} className="text-orange-500" />}
            <span className="font-medium text-gray-900">{val}</span>
          </div>
        )
      }
    },
    { 
      key: 'score', 
      label: 'Quality Score',
      render: (val) => <QualityBadge score={val} />
    },
    { 
      key: 'completeness', 
      label: 'Completeness', 
      render: (val) => (
        <span className={val >= 95 ? 'text-green-600' : val >= 90 ? 'text-blue-600' : 'text-orange-600'}>
          {val.toFixed(1)}%
        </span>
      )
    },
    { key: 'totalRows', label: 'Rows', render: (val) => <span className="text-gray-700">{val.toLocaleString()}</span> },
    { key: 'columns', label: 'Columns', render: (val) => <span className="text-gray-700">{val}</span> },
    { 
      key: 'missing', 
      label: 'Missing Values',
      render: (val) => {
        const severity = val > 2000 ? 'critical' : val > 1000 ? 'warning' : 'good'
        const colors = {
          critical: 'bg-red-50 text-red-700 border-red-200',
          warning: 'bg-orange-50 text-orange-700 border-orange-200',
          good: 'bg-green-50 text-green-700 border-green-200'
        }
        const icons = {
          critical: '🔴',
          warning: '⚠️',
          good: '✓'
        }
        return (
          <div className="flex items-center gap-2">
            <span className={`px-2 py-1 rounded border text-xs font-semibold ${colors[severity]}`}>
              {icons[severity]} {val.toLocaleString()}
            </span>
          </div>
        )
      }
    },
  ]

  const alertColumns = [
    { 
      key: 'severity', 
      label: 'Severity',
      render: (val) => {
        const color = val.includes('CRITICAL') ? 'text-red-600' : val.includes('WARNING') ? 'text-orange-600' : 'text-blue-600'
        return <span className={`font-semibold ${color}`}>{val}</span>
      }
    },
    { key: 'table', label: 'Table', render: (val) => <span className="text-gray-900">{val}</span> },
    { key: 'issue', label: 'Issue', render: (val) => <span className="text-gray-700">{val}</span> },
    { key: 'action', label: 'Recommended Action', render: (val) => <span className="text-gray-700">{val}</span> },
  ]

  const avgQuality = qualityData.length > 0
    ? qualityData.reduce((sum, d) => sum + d.score, 0) / qualityData.length
    : 0

  const avgCompleteness = qualityData.length > 0
    ? qualityData.reduce((sum, d) => sum + d.completeness, 0) / qualityData.length
    : 0

  // Use historical snapshots when available; otherwise show only the current single data point
  // (never generate random/simulated data)
  const trendData = qualityHistory.length > 0
    ? qualityHistory
    : avgQuality > 0
      ? [{ week: getWeekLabel(new Date()), qualityScore: avgQuality, completeness: avgCompleteness, missingValues: 0, tableCount: qualityData.length }]
      : []

  // Score distribution data for donut chart
  const scoreDistributionData = [
    { name: 'Excellent', value: qualityDistribution.excellent, color: SCORE_COLORS.excellent },
    { name: 'Good', value: qualityDistribution.good, color: SCORE_COLORS.good },
    { name: 'Fair', value: qualityDistribution.fair, color: SCORE_COLORS.fair },
    { name: 'Poor', value: qualityDistribution.poor, color: SCORE_COLORS.poor },
  ].filter(item => item.value > 0)

  // Completeness breakdown
  const completeRecordsPercent = qualityData.length > 0 ? avgCompleteness : 0
  const missingValuesPercent = qualityData.length > 0 ? (100 - avgCompleteness) * 0.6 : 0
  const duplicatesPercent = qualityData.length > 0 ? (100 - avgCompleteness) * 0.3 : 0
  const nullValuesPercent = qualityData.length > 0 ? (100 - avgCompleteness) * 0.1 : 0

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-semibold text-gray-800">Quality Monitoring</h1>
            {qualityHistory.length > 0 && (
              <p className="text-xs text-gray-500 mt-1">
                Tracking quality across {qualityHistory.length} data points • 
                Last updated: {new Date(qualityHistory[qualityHistory.length - 1]?.timestamp).toLocaleString()}
              </p>
            )}
          </div>
          <div className="flex items-center gap-3">
            {qualityData === MOCK_QUALITY_DATA ? (
              <div className="flex items-center gap-2 px-3 py-1.5 bg-amber-50 border border-amber-300 rounded-lg text-sm font-medium text-amber-800">
                <Info size={16} />
                <span>Demo Mode - Mock Data</span>
              </div>
            ) : (
              <div className="flex items-center gap-2 px-3 py-1.5 bg-green-50 border border-green-300 rounded-lg text-sm font-medium text-green-800">
                <CheckCircle size={16} />
                <span>Live Data from MinIO</span>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Alert banner if error occurred but showing mock data */}
      {error && qualityData.length > 0 && (
        <div className="mb-6 bg-amber-50 border border-amber-200 rounded-lg p-4 flex items-start gap-3">
          <AlertTriangle size={20} className="text-amber-600 shrink-0 mt-0.5" />
          <div className="flex-1">
            <p className="font-medium text-amber-900">Using Mock Data</p>
            <p className="text-sm text-amber-800 mt-1">{error}</p>
            <p className="text-xs text-amber-700 mt-2">
              To see real data from MinIO: Start backend → Ingest tables → Process to Silver layer
            </p>
          </div>
        </div>
      )}

      {/* Main Grid Layout - Compact Cards */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
        
        {/* Total Quality Overview */}
        <div className="bg-white rounded border border-gray-200 p-4">
          <h3 className="text-sm font-medium text-gray-600 mb-3">Total Quality Overview <InfoTooltip text="Aggregated quality health across all monitored Silver layer tables. Avg Quality Score is a composite of completeness, validity, and consistency checks — 100 is perfect, below 60 needs immediate attention." /></h3>
          <div className="flex items-center justify-between">
            <div>
              <p className="text-3xl font-bold text-gray-900">{qualityData.length}</p>
              <p className="text-xs text-gray-500">Tables Monitored</p>
            </div>
            <div className="flex gap-4">
              {/* Quality Score Gauge */}
              <div className="flex flex-col items-center">
                <p className="text-xs text-gray-500 mb-2">Avg Quality Score</p>
                <div className="relative w-20 h-20">
                  <svg className="transform -rotate-90" width="80" height="80" viewBox="0 0 80 80">
                    <circle cx="40" cy="40" r="32" fill="none" stroke="#fee2e2" strokeWidth="8" />
                    <circle 
                      cx="40" cy="40" r="32" fill="none" stroke={avgQuality >= 75 ? "#22c55e" : avgQuality >= 60 ? "#f59e0b" : "#ef4444"} strokeWidth="8"
                      strokeDasharray={`${(avgQuality / 100) * 201} 201`} strokeLinecap="round"
                    />
                  </svg>
                  <div className="absolute inset-0 flex items-center justify-center">
                    <span className="text-lg font-bold text-gray-900">{avgQuality.toFixed(1)}</span>
                  </div>
                </div>
                <p className="text-xs font-semibold text-gray-600 mt-1">Score</p>
              </div>
              
              {/* Completeness Gauge */}
              <div className="flex flex-col items-center">
                <p className="text-xs text-gray-500 mb-2">Completeness</p>
                <div className="relative w-20 h-20">
                  <svg className="transform -rotate-90" width="80" height="80" viewBox="0 0 80 80">
                    <circle cx="40" cy="40" r="32" fill="none" stroke="#dcfce7" strokeWidth="8" />
                    <circle 
                      cx="40" cy="40" r="32" fill="none" stroke="#22c55e" strokeWidth="8"
                      strokeDasharray={`${(avgCompleteness / 100) * 201} 201`} strokeLinecap="round"
                    />
                  </svg>
                  <div className="absolute inset-0 flex items-center justify-center">
                    <span className="text-lg font-bold text-gray-900">{avgCompleteness.toFixed(1)}</span>
                  </div>
                </div>
                <p className="text-xs font-semibold text-gray-600 mt-1">%</p>
              </div>
            </div>
          </div>
        </div>

        {/* Quality Score Distribution */}
        <div className="bg-white rounded border border-gray-200 p-4">
          <h3 className="text-sm font-medium text-gray-600 mb-3">Quality Score Distribution <InfoTooltip text="Breakdown of tables by quality tier: Excellent (≥90%), Good (75–89%), Fair (60–74%), Poor (<60%). Aim to keep all tables in the Excellent or Good band." /></h3>
          <ResponsiveContainer width="100%" height={140}>
            <BarChart data={[
              { name: 'Excellent', count: qualityDistribution.excellent, fill: '#22c55e' },
              { name: 'Good', count: qualityDistribution.good, fill: '#3b82f6' },
              { name: 'Fair', count: qualityDistribution.fair, fill: '#f59e0b' },
              { name: 'Poor', count: qualityDistribution.poor, fill: '#ef4444' },
            ]}>
              <XAxis dataKey="name" tick={{ fontSize: 10 }} />
              <YAxis tick={{ fontSize: 10 }} />
              <Tooltip cursor={{ fill: 'rgba(0,0,0,0.04)' }} content={({ active, payload }) => {
                if (!active || !payload?.length) return null
                const { name, count } = payload[0].payload
                const tiers = {
                  Excellent: { range: 'Score ≥ 90%', desc: 'All quality checks passing — highly reliable data.', color: '#22c55e' },
                  Good:      { range: 'Score 75–89%', desc: 'Minor issues present — data is usable with caution.', color: '#3b82f6' },
                  Fair:      { range: 'Score 60–74%', desc: 'Moderate issues detected — review transformation rules.', color: '#f59e0b' },
                  Poor:      { range: 'Score < 60%', desc: 'Critical quality problems — immediate attention required.', color: '#ef4444' },
                }
                const t = tiers[name] || {}
                return (
                  <div className="bg-gray-900 text-white text-xs rounded-lg px-3 py-2 shadow-xl max-w-[200px] leading-relaxed">
                    <p className="font-semibold mb-0.5" style={{ color: t.color }}>{name} tier</p>
                    <p className="text-gray-300">{t.range}</p>
                    <p className="font-bold text-white mt-1">{count} table{count !== 1 ? 's' : ''}</p>
                    <p className="text-gray-400 mt-1">{t.desc}</p>
                  </div>
                )
              }} />
              <Bar dataKey="count" radius={[4, 4, 0, 0]}>
                {[
                  { fill: '#22c55e' },
                  { fill: '#3b82f6' },
                  { fill: '#f59e0b' },
                  { fill: '#ef4444' },
                ].map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={entry.fill} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Quality Trend - UPDATED with historical tracking */}
        <div className="bg-white rounded border border-gray-200 p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-medium text-gray-600">Quality Trend <InfoTooltip text="Weekly average quality score over time, snapshotted every 5 minutes. An upward trend means data quality is improving across all tables." /></h3>
            <div className="flex items-center gap-2">
              <TrendingUp size={14} className={
                trendData.length >= 2 && trendData[trendData.length - 1]?.qualityScore > trendData[0]?.qualityScore 
                  ? "text-green-500" 
                  : "text-gray-400"
              } />
              {qualityHistory.length > 0 ? (
                <span className={`text-xs font-medium px-1.5 py-0.5 rounded ${
                  qualityHistory.some(s => s.fromBackend)
                    ? 'bg-green-50 text-green-700 border border-green-200'
                    : 'bg-amber-50 text-amber-700 border border-amber-200'
                }`}>
                  {qualityHistory.filter(s => s.fromBackend).length > 0
                    ? `${qualityHistory.length} live snapshots`
                    : `${qualityHistory.length} snapshots (example)`}
                </span>
              ) : (
                <span className="text-xs text-gray-400">No history yet</span>
              )}
            </div>
          </div>
          <ResponsiveContainer width="100%" height={140}>
            <LineChart data={trendData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="week" tick={{ fontSize: 10 }} />
              <YAxis domain={[0, 100]} tick={{ fontSize: 10 }} />
              <Tooltip 
                contentStyle={{ backgroundColor: '#fff', border: '1px solid #e5e7eb', borderRadius: '6px', fontSize: '12px' }}
                formatter={(value) => value.toFixed(1)}
              />
              <Line 
                type="monotone" 
                dataKey="qualityScore" 
                stroke="#22c55e" 
                strokeWidth={2} 
                dot={{ fill: '#22c55e', r: 3 }}
                name="Quality Score"
              />
            </LineChart>
          </ResponsiveContainer>
          <p className="text-xs text-center mt-2">
            {qualityHistory.some(s => s.fromBackend)
              ? <span className="text-green-600 font-medium">Live backend data · Updates every 5 min</span>
              : qualityHistory.length > 0
                ? <span className="text-amber-600">Example data — backend not reached yet. Click “Load Real Data”.</span>
                : <span className="text-gray-400">Connect backend to start tracking trends</span>
            }
          </p>
        </div>

        {/* Completeness vs Quality Scatter */}
        <div className="bg-white rounded border border-gray-200 p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-medium text-gray-600">Completeness vs Quality <InfoTooltip text="Scatter plot where each dot represents a table. X-axis = data completeness %, Y-axis = overall quality score. Ideal tables cluster near the top-right (high completeness, high quality)." /></h3>
            <p className="text-xs text-gray-500">Per Table</p>
          </div>
          <ResponsiveContainer width="100%" height={140}>
            <ScatterChart>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis type="number" dataKey="completeness" domain={[0, 100]} tick={{ fontSize: 10 }} />
              <YAxis type="number" dataKey="quality" domain={[0, 100]} tick={{ fontSize: 10 }} />
              <Scatter data={scatterData} fill="#3b82f6" />
            </ScatterChart>
          </ResponsiveContainer>
        </div>

        {/* Table Quality Breakdown */}
        <div className="bg-white rounded border border-gray-200 p-4">
          <h3 className="text-sm font-medium text-gray-600 mb-3">Table Quality Breakdown <InfoTooltip text="Per-table quality scores as horizontal progress bars. Bar color encodes the tier: green = Excellent (≥90), blue = Good (75–89), amber = Fair (60–74), red = Poor (<60)." /></h3>
          <div className="space-y-2">
            {qualityData.slice(0, 5).map((table, idx) => {
              const tier = table.score >= 90 ? { label: 'Excellent', color: '#22c55e', desc: 'All quality checks passing.' }
                : table.score >= 75 ? { label: 'Good', color: '#3b82f6', desc: 'Minor issues — data is usable.' }
                : table.score >= 60 ? { label: 'Fair', color: '#f59e0b', desc: 'Moderate issues — review recommended.' }
                : { label: 'Poor', color: '#ef4444', desc: 'Critical issues — needs immediate attention.' }
              return (
                <div key={idx} className="group relative flex items-center gap-3 cursor-default">
                  <span className="text-xs text-gray-600 w-24 truncate">{table.entity}</span>
                  <div className="flex-1 h-5 bg-gray-100 rounded relative overflow-hidden">
                    <div
                      className="h-full transition-opacity group-hover:opacity-80"
                      style={{ width: `${table.score}%`, backgroundColor: tier.color }}
                    />
                  </div>
                  <span className="text-xs font-semibold text-gray-700 w-10">{table.score.toFixed(0)}</span>
                  {/* Hover tooltip */}
                  <div className="pointer-events-none absolute left-24 bottom-7 z-50 hidden group-hover:flex flex-col bg-gray-900 text-white text-xs rounded-lg px-3 py-2 shadow-xl w-52 leading-relaxed">
                    <p className="font-semibold" style={{ color: tier.color }}>{table.entity}</p>
                    <p className="text-gray-300 mt-0.5">Quality score: <span className="font-bold text-white">{table.score.toFixed(1)}%</span></p>
                    <p className="font-semibold mt-0.5" style={{ color: tier.color }}>{tier.label}</p>
                    <p className="text-gray-400 mt-0.5">{tier.desc}</p>
                    <span className="absolute top-full left-6 border-4 border-transparent border-t-gray-900" />
                  </div>
                </div>
              )
            })}
            {qualityData.length === 0 && (
              <p className="text-xs text-gray-400 text-center py-4">No data available</p>
            )}
          </div>
        </div>

        {/* Missing Values by Table */}
        <div className="bg-white rounded border border-gray-200 p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-medium text-gray-600">Missing Values by Table <InfoTooltip text="Absolute count of null or missing values per table. Green bars (<1 K) are healthy; amber (1K–2K) need review; red (>2K) require immediate attention to the data source." /></h3>
            <p className="text-xs text-gray-500">Current Count</p>
          </div>
          <ResponsiveContainer width="100%" height={140}>
            <BarChart data={qualityData.slice(0, 6)}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis dataKey="entity" tick={{ fontSize: 10 }} angle={-45} textAnchor="end" height={60} />
              <YAxis tick={{ fontSize: 10 }} />
              <Tooltip cursor={{ fill: 'rgba(0,0,0,0.04)' }} content={({ active, payload }) => {
                if (!active || !payload?.length) return null
                const row = payload[0].payload
                const sev = row.missing > 2000
                  ? { label: 'Critical', color: '#ef4444', desc: 'Requires immediate attention to the data source.' }
                  : row.missing > 1000
                  ? { label: 'Warning',  color: '#f59e0b', desc: 'Review data collection or ingestion process.' }
                  : { label: 'Good',     color: '#22c55e', desc: 'Missing value count is within acceptable range.' }
                return (
                  <div className="bg-gray-900 text-white text-xs rounded-lg px-3 py-2 shadow-xl max-w-[210px] leading-relaxed">
                    <p className="font-semibold text-white">{row.entity}</p>
                    <p className="text-gray-300 mt-0.5">{row.missing.toLocaleString()} missing values</p>
                    <p className="font-semibold mt-1" style={{ color: sev.color }}>{sev.label}</p>
                    <p className="text-gray-400 mt-0.5">{sev.desc}</p>
                  </div>
                )
              }} />
              <Bar dataKey="missing" radius={[4, 4, 0, 0]} name="Missing Values">
                {qualityData.slice(0, 6).map((entry, index) => (
                  <Cell 
                    key={`cell-${index}`} 
                    fill={entry.missing > 2000 ? '#ef4444' : entry.missing > 1000 ? '#f59e0b' : '#22c55e'}
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
          <div className="flex items-center justify-between mt-2">
            <div className="flex items-center gap-3 text-xs">
              <div className="flex items-center gap-1">
                <div className="w-3 h-3 bg-green-500 rounded"></div>
                <span className="text-gray-600">&lt;1K Good</span>
              </div>
              <div className="flex items-center gap-1">
                <div className="w-3 h-3 bg-orange-500 rounded"></div>
                <span className="text-gray-600">1K-2K Warning</span>
              </div>
              <div className="flex items-center gap-1">
                <div className="w-3 h-3 bg-red-500 rounded"></div>
                <span className="text-gray-600">&gt;2K Critical</span>
              </div>
            </div>
            <p className="text-xs font-semibold text-gray-700">
              Total: {qualityData.reduce((sum, t) => sum + t.missing, 0).toLocaleString()}
            </p>
          </div>
        </div>

        {/* Total Records & Columns */}
        <div className="bg-white rounded border border-gray-200 p-4">
          <div className="mb-2">
            <p className="text-2xl font-bold text-gray-900">
              {qualityData.reduce((sum, t) => sum + t.totalRows, 0).toLocaleString()}
            </p>
            <p className="text-xs text-gray-500">Total Records Monitored <InfoTooltip text="Sum of all rows across all monitored Silver layer tables. A larger number means more data has been ingested and is actively being quality-checked." /></p>
          </div>
          <ResponsiveContainer width="100%" height={100}>
            <BarChart data={qualityData.slice(0, 8)}>
              <Tooltip cursor={{ fill: 'rgba(0,0,0,0.04)' }} content={({ active, payload }) => {
                if (!active || !payload?.length) return null
                const row = payload[0].payload
                return (
                  <div className="bg-gray-900 text-white text-xs rounded-lg px-3 py-2 shadow-xl leading-relaxed">
                    <p className="font-semibold text-white">{row.entity}</p>
                    <p className="text-gray-300 mt-0.5">{(row.totalRows || 0).toLocaleString()} total rows</p>
                    <p className="text-gray-400 mt-0.5">Rows actively monitored in the Silver layer.</p>
                  </div>
                )
              }} />
              <Bar dataKey="totalRows" fill="#3b82f6" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
          <p className="text-xs text-gray-500 mt-2">Records per Table</p>
        </div>

        {/* Active Alerts */}
        <div className="bg-white rounded border border-gray-200 p-4">
          <div className="mb-2">
            <p className="text-2xl font-bold text-gray-900">{alerts.length}</p>
            <p className="text-xs text-gray-500">Active Quality Alerts <InfoTooltip text="Tables currently flagged for quality issues. Critical = score below 60%; Warning = score 60–79% or completeness below 90%. Alerts auto-clear once issues are resolved." /></p>
          </div>
          {alerts.length > 0 ? (
            <div className="space-y-1 max-h-[100px] overflow-y-auto">
              {alerts.slice(0, 5).map((alert, idx) => (
                <div key={idx} className="text-xs p-2 bg-red-50 rounded border border-red-200">
                  <span className={alert.severity.includes('CRITICAL') ? 'text-red-700 font-semibold' : 'text-orange-700 font-semibold'}>
                    {alert.severity}
                  </span>
                  <span className="text-gray-600 ml-2">{alert.table}</span>
                </div>
              ))}
            </div>
          ) : (
            <div className="flex items-center justify-center h-24">
              <div className="text-center">
                <CheckCircle size={32} className="mx-auto text-green-500 mb-1" />
                <p className="text-xs text-gray-500">All tables healthy</p>
              </div>
            </div>
          )}
          <p className="text-xs text-gray-500 mt-2">Quality Issues Detected</p>
        </div>

        {/* Data Completeness Overview */}
        <div className="bg-white rounded border border-gray-200 p-4">
          <div className="mb-2">
            <p className="text-2xl font-bold text-gray-900">{avgCompleteness.toFixed(1)}%</p>
            <p className="text-xs text-gray-500">Average Data Completeness <InfoTooltip text="Percentage of fields that contain non-null values across all tables. The bars below break completeness down into complete records, missing values, estimated duplicates, and nulls." /></p>
          </div>
          <div className="space-y-2">
            <div className="flex items-center justify-between text-xs">
              <span className="text-gray-600">Complete</span>
              <span className="font-semibold text-green-600">{completeRecordsPercent.toFixed(1)}%</span>
            </div>
            <div className="w-full h-2 bg-gray-100 rounded overflow-hidden">
              <div className="h-full bg-green-500" style={{ width: `${completeRecordsPercent}%` }}></div>
            </div>
            <div className="flex items-center justify-between text-xs">
              <span className="text-gray-600">Missing</span>
              <span className="font-semibold text-red-600">{missingValuesPercent.toFixed(1)}%</span>
            </div>
            <div className="w-full h-2 bg-gray-100 rounded overflow-hidden">
              <div className="h-full bg-red-500" style={{ width: `${missingValuesPercent}%` }}></div>
            </div>
            <div className="flex items-center justify-between text-xs">
              <span className="text-gray-600">Duplicates</span>
              <span className="font-semibold text-orange-600">{duplicatesPercent.toFixed(1)}%</span>
            </div>
            <div className="w-full h-2 bg-gray-100 rounded overflow-hidden">
              <div className="h-full bg-orange-500" style={{ width: `${duplicatesPercent}%` }}></div>
            </div>
          </div>
        </div>
      </div>

      {/* Star Schema Diagram */}
      <div className="mt-4">
        <StarSchema />
      </div>

      {/* Filters Section */}
      <div className="mt-6 bg-white rounded border border-gray-200 p-4">
        <div className="grid grid-cols-4 gap-6">
          <div>
            <h4 className="text-xs font-medium text-gray-600 mb-2">Quality Level</h4>
            <div className="space-y-1">
              <label className="flex items-center gap-2 text-xs text-gray-700">
                <input type="checkbox" defaultChecked className="rounded text-green-600" />
                <span className="text-green-600 font-medium">Excellent (≥90)</span>
              </label>
              <label className="flex items-center gap-2 text-xs text-gray-700">
                <input type="checkbox" defaultChecked className="rounded text-blue-600" />
                <span className="text-blue-600 font-medium">Good (75-89)</span>
              </label>
              <label className="flex items-center gap-2 text-xs text-gray-700">
                <input type="checkbox" defaultChecked className="rounded text-orange-600" />
                <span className="text-orange-600 font-medium">Fair (60-74)</span>
              </label>
              <label className="flex items-center gap-2 text-xs text-gray-700">
                <input type="checkbox" defaultChecked className="rounded text-red-600" />
                <span className="text-red-600 font-medium">Poor (&lt;60)</span>
              </label>
            </div>
          </div>
          <div>
            <h4 className="text-xs font-medium text-gray-600 mb-2">Data Sources</h4>
            <div className="space-y-1">
              {Array.from(new Set(qualityData.map(t => t.source))).slice(0, 4).map((source, idx) => (
                <label key={idx} className="flex items-center gap-2 text-xs text-gray-700">
                  <input type="checkbox" defaultChecked className="rounded" />
                  <span>{source}</span>
                </label>
              ))}
            </div>
          </div>
          <div>
            <h4 className="text-xs font-medium text-gray-600 mb-2">Alert Severity</h4>
            <div className="space-y-1">
              <label className="flex items-center gap-2 text-xs text-gray-700">
                <input type="checkbox" defaultChecked className="rounded text-red-600" />
                <span className="text-red-600 font-medium">Critical</span>
              </label>
              <label className="flex items-center gap-2 text-xs text-gray-700">
                <input type="checkbox" defaultChecked className="rounded text-orange-600" />
                <span className="text-orange-600 font-medium">Warning</span>
              </label>
              <label className="flex items-center gap-2 text-xs text-gray-700">
                <input type="checkbox" defaultChecked className="rounded text-blue-600" />
                <span className="text-blue-600 font-medium">Info</span>
              </label>
            </div>
          </div>
          <div>
            <h4 className="text-xs font-medium text-gray-600 mb-2">Actions</h4>
            <div className="space-y-2">
              <button 
                onClick={loadQualityData}
                className="w-full flex items-center justify-center gap-2 px-3 py-2 bg-blue-500 text-white text-xs rounded hover:bg-blue-600"
                title="Load real data from data lakehouse"
              >
                <RefreshCw size={14} />
                {qualityData === MOCK_QUALITY_DATA ? 'Load Real Data' : 'Refresh Data'}
              </button>
              <button className="w-full flex items-center justify-center gap-2 px-3 py-2 bg-gray-100 text-gray-700 text-xs rounded hover:bg-gray-200">
                <Download size={14} />
                Export Report
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Quality Data Tables - Expandable Section */}
      {qualityData.length > 0 && (
        <div className="mt-6 space-y-4">
          <details className="bg-white rounded border border-gray-200">
            <summary className="px-6 py-4 cursor-pointer font-medium text-gray-700 hover:bg-gray-50">
              All Quality Metrics ({qualityData.length} tables)
            </summary>
            <div className="px-6 pb-4">
              <DataTable data={qualityData} columns={tableColumns} />
            </div>
          </details>

          {alerts.length > 0 && (
            <details className="bg-white rounded border border-gray-200">
              <summary className="px-6 py-4 cursor-pointer font-medium text-gray-700 hover:bg-gray-50">
                Active Quality Alerts ({alerts.length} alerts)
              </summary>
              <div className="px-6 pb-4">
                <DataTable data={alerts} columns={alertColumns} />
              </div>
            </details>
          )}
        </div>
      )}
    </div>
  )
}
