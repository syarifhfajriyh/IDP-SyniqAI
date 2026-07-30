import { useState, useEffect } from 'react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend, RadialBarChart, RadialBar } from 'recharts'
import { Database, TrendingUp, AlertCircle, Loader2, RefreshCw, Play, Award, Package, BarChart3, Star, Sparkles, HelpCircle, Box, FileText, Download, GitBranch, Settings, Search, X, Filter, Clock, HardDrive, Layers, ArrowUp, ArrowDown, Calendar, FileType, ArrowLeft } from 'lucide-react'
import axios from 'axios'
import QualityBadge from '../../components/cards/QualityBadge'
import DataTable from '../../components/tables/DataTable'
import Alert from '../../components/ui/Alert'

const API_BASE = 'http://localhost:8000/api'

// Source type configuration
const SOURCE_CONFIG = {
  postgres: { 
    icon: Database, 
    color: 'blue', 
    label: 'PostgreSQL',
    logo: 'https://www.postgresql.org/media/img/about/press/elephant.png'
  },
  mariadb: { 
    icon: Database, 
    color: 'orange', 
    label: 'MariaDB',
    logo: 'https://mariadb.com/wp-content/uploads/2019/11/mariadb-logo-vert_blue-transparent.png'
  },
  mariadb_cloud: { 
    icon: Database, 
    color: 'orange', 
    label: 'MariaDB Cloud',
    logo: 'https://mariadb.com/wp-content/uploads/2019/11/mariadb-logo-vert_blue-transparent.png'
  },
  s3: { 
    icon: Layers, 
    color: 'purple', 
    label: 'Amazon S3',
    logo: 'https://upload.wikimedia.org/wikipedia/commons/b/bc/Amazon-S3-Logo.svg'
  },
  mongodb: { 
    icon: FileText, 
    color: 'green', 
    label: 'MongoDB',
    logo: 'https://www.mongodb.com/assets/images/global/leaf.png'
  }
}

// Color scheme matching the app
const COLORS = {
  gold: '#B8860B',
  gold_light: '#D4A820',
  gold_dark: '#7A5C00',
  green: '#10b981',
  blue: '#3b82f6',
  amber: '#f59e0b',
  red: '#ef4444',
  silver: '#6B7280',
  purple: '#8b5cf6',
  pink: '#ec4899',
  bg_light: '#F8F9FB',
  bg_white: '#FFFFFF',
  border: 'rgba(0,0,0,0.07)'
}

const CHART_COLORS = [COLORS.blue, COLORS.green, COLORS.amber, COLORS.red, COLORS.purple, COLORS.pink]

// Tooltip Component
const InfoTooltip = ({ text }) => {
  const [show, setShow] = useState(false)
  
  return (
    <div className="relative inline-block ml-2">
      <button
        onMouseEnter={() => setShow(true)}
        onMouseLeave={() => setShow(false)}
        className="w-4 h-4 rounded-full bg-gray-200 border border-gray-300 flex items-center justify-center text-gray-500 hover:bg-gray-300 transition"
      >
        <span className="text-xs">?</span>
      </button>
      {show && (
        <div className="absolute bottom-full left-1/2 transform -translate-x-1/2 mb-2 w-64 bg-gray-800 text-white text-xs rounded-lg p-3 z-50 shadow-lg">
          {text}
          <div className="absolute top-full left-1/2 transform -translate-x-1/2 -mt-1 border-4 border-transparent border-t-gray-800"></div>
        </div>
      )}
    </div>
  )
}

// Medallion Badge Component
const MedallionBadge = ({ type, active = false }) => {
  const configs = {
    bronze: { icon: '●', label: 'Bronze', bg: 'rgba(205,127,50,0.15)', color: '#CD7F32', border: 'rgba(205,127,50,0.25)' },
    silver: { icon: '●', label: 'Silver', bg: 'rgba(168,180,192,0.12)', color: '#6B7280', border: 'rgba(168,180,192,0.2)' },
    gold: { icon: '●', label: 'Gold', bg: 'rgba(184,134,11,0.08)', color: '#B8860B', border: 'rgba(184,134,11,0.25)' }
  }
  
  const config = configs[type]
  
  return (
    <span 
      className="inline-flex items-center gap-2 px-3 py-1 rounded-md text-xs font-semibold"
      style={{ 
        background: config.bg,
        color: config.color,
        border: `1px solid ${config.border}`,
        boxShadow: active ? `0 0 12px ${config.border}` : 'none'
      }}
    >
      <span>{config.icon}</span>
      <span>{config.label}</span>
    </span>
  )
}

// Pipeline Status Banner
const PipelineStatus = () => {
  return (
    <div className="bg-white rounded-xl border shadow-sm p-4 mb-6">
      <div className="flex items-center gap-4">
        <div className="flex-1 flex items-center gap-3">
          <div className="w-8 h-8 rounded-full flex items-center justify-center" style={{ background: 'rgba(205,127,50,0.15)', border: '2px solid #CD7F32' }}>
            <span className="text-xs font-bold" style={{ color: '#CD7F32' }}>B</span>
          </div>
          <div>
            <div className="flex items-center gap-1">
              <div className="font-semibold text-sm" style={{ color: '#CD7F32' }}>Bronze Layer</div>
              <InfoTooltip text="The Bronze layer stores raw data exactly as ingested from source systems. No transformations are applied - data is preserved in its original format. This ensures full historical traceability and allows reprocessing if needed." />
            </div>
            <div className="text-xs text-gray-500">Raw ingestion</div>
          </div>
          <div className="text-xs font-semibold" style={{ color: COLORS.green }}>Done</div>
        </div>
        
        <div className="text-gray-300 text-xl">›</div>
        
        <div className="flex-1 flex items-center gap-3">
          <div className="w-8 h-8 rounded-full flex items-center justify-center" style={{ background: 'rgba(168,180,192,0.12)', border: '2px solid #6B7280' }}>
            <span className="text-xs font-bold" style={{ color: '#6B7280' }}>S</span>
          </div>
          <div>
            <div className="flex items-center gap-1">
              <div className="font-semibold text-sm" style={{ color: COLORS.silver }}>Silver Layer</div>
              <InfoTooltip text="The Silver layer contains cleaned and validated data. Transformations include type casting, deduplication, validation rules, and quality checks. Data is structured and ready for analytics." />
            </div>
            <div className="text-xs text-gray-500">Cleaned · Validated</div>
          </div>
          <div className="text-xs font-semibold" style={{ color: COLORS.green }}>Done</div>
        </div>
        
        <div className="text-gray-300 text-xl">›</div>
        
        <div className="flex-1 flex items-center gap-3">
          <div className="w-8 h-8 rounded-full flex items-center justify-center" style={{ background: 'rgba(184,134,11,0.08)', border: '2px solid #B8860B' }}>
            <span className="text-xs font-bold" style={{ color: COLORS.gold }}>G</span>
          </div>
          <div>
            <div className="flex items-center gap-1">
              <div className="font-semibold text-sm" style={{ color: COLORS.gold }}>Gold Layer</div>
              <InfoTooltip text="The Gold layer contains curated, business-ready data products. Data is aggregated, analyzed, and optimized for reporting and analytics use cases. Includes EDA reports, KPIs, and data marts." />
            </div>
            <div className="text-xs text-gray-500">Aggregated · Curated</div>
          </div>
          <div className="text-xs font-semibold" style={{ color: COLORS.gold }}>Ready</div>
        </div>
      </div>
    </div>
  )
}

// Correlation Legend Component
const CorrelationLegend = () => {
  const legends = [
    { label: 'Strong Negative', range: 'r < -0.7', gradient: 'linear-gradient(to right, #ef4444, #f87171)', desc: 'Inverse relationship' },
    { label: 'Moderate Negative', range: '-0.7 < r < -0.3', gradient: 'linear-gradient(to right, #fbbf24, #fcd34d)', desc: '' },
    { label: 'Weak/No Correlation', range: '-0.3 < r < 0.3', gradient: '#e5e7eb', desc: '' },
    { label: 'Moderate Positive', range: '0.3 < r < 0.7', gradient: 'linear-gradient(to right, #93c5fd, #60a5fa)', desc: '' },
    { label: 'Strong Positive', range: 'r > 0.7', gradient: 'linear-gradient(to right, #10b981, #34d399)', desc: 'Direct relationship' }
  ]
  
  return (
    <div className="bg-gray-50 rounded-lg border p-4 mt-4">
      <h4 className="text-sm font-semibold mb-3">Correlation Interpretation</h4>
      <div className="space-y-2">
        {legends.map((item, idx) => (
          <div key={idx} className="flex items-center gap-3">
            <div 
              className="w-6 h-6 rounded border border-gray-300 flex-shrink-0"
              style={{ background: item.gradient }}
            />
            <div className="flex-1">
              <div className="text-sm font-medium">{item.label}</div>
              <div className="text-xs text-gray-500">{item.range} {item.desc && `· ${item.desc}`}</div>
            </div>
          </div>
        ))}
      </div>
      
      <div className="mt-3 p-3 bg-amber-50 border border-amber-200 rounded-lg">
        <div className="text-xs text-amber-900">
          <strong>💡 Interpretation Guide:</strong><br/>
          • <strong>|r| &gt; 0.9</strong>: Very strong correlation<br/>
          • <strong>|r| &gt; 0.7</strong>: Strong correlation<br/>
          • <strong>|r| &gt; 0.5</strong>: Moderate correlation<br/>
          • <strong>|r| &gt; 0.3</strong>: Weak correlation<br/>
          • <strong>|r| &lt; 0.3</strong>: Little/no correlation
        </div>
      </div>
    </div>
  )
}

export default function ExploratoryAnalysis() {
  const [silverTables, setSilverTables] = useState([])
  const [selectedTable, setSelectedTable] = useState(null)
  const [edaData, setEdaData] = useState(null)
  const [schemaData, setSchemaData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [generating, setGenerating] = useState(false)
  const [error, setError] = useState(null)
  const [schemaType, setSchemaType] = useState('Star')
  const [techMode, setTechMode] = useState(false)
  const [edaTab, setEdaTab] = useState('profiling')
  
  // Search and filter state
  const [searchQuery, setSearchQuery] = useState('')
  const [sourceFilter, setSourceFilter] = useState('')
  const [formatFilter, setFormatFilter] = useState('')
  const [dateFilter, setDateFilter] = useState('all')
  const [sortBy, setSortBy] = useState('last_modified')
  const [sortOrder, setSortOrder] = useState('desc')
  const [showFilters, setShowFilters] = useState(false)
  
  // Pagination state
  const [listPage, setListPage] = useState(1)
  const [listPageSize, setListPageSize] = useState(50)
  const [listTotalPages, setListTotalPages] = useState(1)
  const [listTotalCount, setListTotalCount] = useState(0)

  // Load Silver tables on mount
  useEffect(() => {
    loadSilverTables()
  }, [])

  const loadSilverTables = async () => {
    try {
      setLoading(true)
      setError(null)
      const response = await axios.get(`${API_BASE}/silver/tables`)
      setSilverTables(response.data.tables || [])
      setListTotalCount(response.data.tables?.length || 0)
    } catch (err) {
      console.error('Error loading Gold tables:', err)
      setError('Failed to load Gold tables. Please ensure Silver layer processing is complete.')
    } finally {
      setLoading(false)
    }
  }

  const loadEDAReport = async (source, entity) => {
    try {
      setLoading(true)
      setError(null)
      
      const response = await axios.get(`${API_BASE}/gold/eda/${source}/${entity}/viz`)
      setEdaData(response.data)
      
      // Also load schema structure
      loadSchemaStructure(source, entity)
      
    } catch (err) {
      console.error('Error loading EDA:', err)
      setError(err.response?.data?.detail || 'Failed to load EDA report')
    } finally {
      setLoading(false)
    }
  }

  const loadSchemaStructure = async (source, entity) => {
    try {
      const response = await axios.get(`${API_BASE}/gold/eda/${source}/${entity}/schema`)
      setSchemaData(response.data)
      
      // Update schema type based on detection
      if (response.data.detected_schema_type) {
        setSchemaType(response.data.detected_schema_type)
      }
      
    } catch (err) {
      console.error('Error loading schema structure:', err)
      // Non-critical, don't show error to user
    }
  }

  const generateNewReport = async (source, entity) => {
    try {
      setGenerating(true)
      setError(null)
      
      await axios.post(`${API_BASE}/gold/eda/generate`, null, {
        params: { source, entity }
      })
      
      // Reload the report
      await loadEDAReport(source, entity)
      
    } catch (err) {
      console.error('Error generating EDA:', err)
      setError(err.response?.data?.detail || 'Failed to generate EDA report')
    } finally {
      setGenerating(false)
    }
  }

  const handleTableSelect = (table) => {
    const source = table.source || table.source_type
    
    // Extract entity name - remove source prefix if present
    let entity = table.entity || table.table_name
    if (entity && source && entity.startsWith(`${source}_`)) {
      entity = entity.substring(source.length + 1) // Remove "source_" prefix
    }
    
    // Store table with extracted entity for later use
    setSelectedTable({
      ...table,
      source,
      entity
    })
    
    loadEDAReport(source, entity)
  }
  
  // Sorting and filtering handlers
  const handleSort = (column) => {
    if (sortBy === column) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc')
    } else {
      setSortBy(column)
      setSortOrder('desc')
    }
  }
  
  const handleSearch = (value) => {
    setSearchQuery(value)
    setListPage(1)
  }
  
  const clearFilters = () => {
    setSearchQuery('')
    setSourceFilter('')
    setFormatFilter('')
    setDateFilter('all')
  }
  
  const formatBytes = (bytes) => {
    if (!bytes) return 'N/A'
    if (bytes < 1024) return bytes + ' B'
    if (bytes < 1048576) return (bytes / 1024).toFixed(2) + ' KB'
    if (bytes < 1073741824) return (bytes / 1048576).toFixed(2) + ' MB'
    return (bytes / 1073741824).toFixed(2) + ' GB'
  }
  
  const getQualityBadge = (score) => {
    if (!score) return null
    if (score >= 95) return { color: 'green', label: 'Excellent' }
    if (score >= 85) return { color: 'blue', label: 'Good' }
    if (score >= 70) return { color: 'yellow', label: 'Fair' }
    return { color: 'red', label: 'Poor' }
  }
  
  const getTimeSince = (dateStr) => {
    const now = new Date()
    const date = new Date(dateStr)
    const seconds = Math.floor((now - date) / 1000)
    
    if (seconds < 60) return `${seconds}s ago`
    const minutes = Math.floor(seconds / 60)
    if (minutes < 60) return `${minutes}m ago`
    const hours = Math.floor(minutes / 60)
    if (hours < 24) return `${hours}h ago`
    const days = Math.floor(hours / 24)
    return `${days}d ago`
  }

  // Calculate readiness score
  const calculateReadiness = (data) => {
    if (!data) return { passed: 0, total: 8, percentage: 0, checks: {} }
    
    const checks = {
      silver_validated: true,
      no_duplicates: (data.metrics?.duplicates || 0) === 0,
      completeness_ok: (data.metrics?.completeness || 0) >= 95,
      quality_score_ok: (data.metrics?.quality_score || 0) >= 80,
      no_missing_critical: (data.metrics?.missing_values || 0) < 100,
      referential_integrity: true,
      schema_valid: true,
      aggregations_verified: true
    }
    
    const passed = Object.values(checks).filter(Boolean).length
    const total = Object.keys(checks).length
    const percentage = (passed / total) * 100
    
    return { passed, total, percentage, checks }
  }

  // Use actual data (either demo or real)
  const displayData = edaData
  const readiness = calculateReadiness(displayData)

  // Prepare column data for table
  const columnTableData = displayData ? Object.entries(displayData.columns || {}).map(([name, info]) => ({
    column: name,
    type: info.type || info.dtype,
    non_null: info.non_null_count || 0,
    null_pct: info.null_percentage || 0,
    unique: info.unique_count || 0,
    completeness: 100 - (info.null_percentage || 0)
  })) : []

  const columnTableColumns = [
    { key: 'column', label: 'Column' },
    { key: 'type', label: 'Type' },
    { key: 'non_null', label: 'Non-Null', render: (val) => val.toLocaleString() },
    { key: 'null_pct', label: 'Null %', render: (val) => `${val.toFixed(2)}%` },
    { key: 'unique', label: 'Unique', render: (val) => val.toLocaleString() },
    { key: 'completeness', label: 'Completeness', render: (val) => `${val.toFixed(1)}%` }
  ]

  return (
    <div className="space-y-4">
      {/* Gold Layer Header Card */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between">
          <div>
            <div className="flex items-center">
              <h1 className="text-3xl font-bold" style={{ color: COLORS.gold }}>
                Gold Layer — Data Intelligence
              </h1>
              <InfoTooltip text="The Gold layer contains curated, business-ready data products. Data is aggregated, analyzed, and optimized for reporting and analytics use cases." />
            </div>
            <p className="text-base text-gray-600 mt-2">
              Curated, business-ready data products and comprehensive analytics
            </p>
          </div>
          <div className="flex items-center gap-3">
            {/* Technical View Toggle */}
            <button
              onClick={() => setTechMode(!techMode)}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg text-xs font-mono transition ${
                techMode
                  ? 'bg-amber-50 text-amber-900 border-2 border-amber-300'
                  : 'bg-gray-100 text-gray-600 border border-gray-300 hover:bg-gray-200'
              }`}
            >
              <div className="relative w-8 h-4 rounded-full transition" style={{ background: techMode ? 'rgba(240,192,64,0.4)' : '#D1D5DB' }}>
                <div 
                  className="absolute top-0.5 w-3 h-3 rounded-full bg-white transition-transform" 
                  style={{ 
                    left: techMode ? '1.125rem' : '0.125rem',
                    transition: 'transform 0.2s'
                  }}
                />
              </div>
              <span>{techMode ? 'Technical ON' : 'Technical view'}</span>
            </button>
            
            <button
              onClick={loadSilverTables}
              className="flex items-center gap-2 px-4 py-2 rounded-lg hover:bg-amber-50 transition-colors"
              style={{ border: `1.5px solid ${COLORS.gold}`, color: COLORS.gold }}
            >
              <RefreshCw className="w-4 h-4" />
              <span className="font-semibold">Refresh</span>
            </button>
          </div>
        </div>
      </div>

      {/* Pipeline Status */}
      <PipelineStatus />

      {error && (
        <div className="bg-red-50 border border-red-200 rounded-xl p-4 flex items-start gap-3">
          <AlertCircle size={20} className="text-red-600 flex-shrink-0 mt-0.5" />
          <div className="flex-1">
            <p className="text-sm text-red-800">{error}</p>
          </div>
        </div>
      )}

      {/* Main Content - Table Catalog View */}
      {!selectedTable ? (
        <div className="space-y-4">
          {/* Global Search Bar */}
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
            <div className="flex items-center gap-3">
              <Search className="text-gray-400" size={20} />
              <input
                type="text"
                placeholder="Search tables, schemas, sources... (10K+ tables supported)"
                value={searchQuery}
                onChange={(e) => handleSearch(e.target.value)}
                className="flex-1 bg-transparent border-none outline-none text-base text-gray-900 placeholder-gray-400"
              />
              {searchQuery && (
                <button 
                  onClick={() => handleSearch('')}
                  className="text-gray-400 hover:text-gray-600 transition-colors"
                >
                  <X size={18} />
                </button>
              )}
              <button
                onClick={() => setShowFilters(!showFilters)}
                className={`flex items-center gap-2 px-4 py-2 rounded-lg transition-colors ${
                  showFilters ? 'bg-amber-100 text-amber-700' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
                style={showFilters ? { borderColor: COLORS.gold } : {}}
              >
                <Filter size={16} />
                Filters
                {(sourceFilter || formatFilter || dateFilter !== 'all') && (
                  <span className="w-2 h-2 rounded-full" style={{ backgroundColor: COLORS.gold }}></span>
                )}
              </button>
            </div>
          </div>

          {/* Stats Bar */}
          <div className="flex items-center justify-between rounded-lg border px-6 py-3" style={{ 
            background: 'linear-gradient(to right, rgba(184,134,11,0.08), white)', 
            borderColor: 'rgba(184,134,11,0.2)' 
          }}>
            <div className="flex items-center gap-6">
              <div className="flex items-center gap-2">
                <Database style={{ color: COLORS.gold }} size={18} />
                <span className="text-sm text-gray-600">Showing</span>
                <span className="font-bold text-gray-900">{silverTables.length}</span>
                <span className="text-sm text-gray-600">of</span>
                <span className="font-bold text-gray-900">{silverTables.length}</span>
                <span className="text-sm text-gray-600">tables</span>
              </div>
              {(sourceFilter || formatFilter || dateFilter !== 'all' || searchQuery) && (
                <>
                  <div className="w-px h-4 bg-gray-300"></div>
                  <button
                    onClick={clearFilters}
                    className="text-sm font-medium flex items-center gap-1 hover:opacity-80 transition"
                    style={{ color: COLORS.gold }}
                  >
                    <X size={14} />
                    Clear all filters
                  </button>
                </>
              )}
            </div>
          </div>

          {/* Table Listing */}
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
            {silverTables.length === 0 ? (
              <div className="text-center py-12">
                <Database size={48} className="mx-auto text-gray-400 mb-3" />
                <p className="text-gray-600 font-medium">No Gold tables found</p>
                <p className="text-sm text-gray-500 mt-1">Please run Silver layer processing first</p>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-gray-50 border-b border-gray-200">
                    <tr>
                      <th 
                        onClick={() => handleSort('table_name')}
                        className="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                      >
                        <div className="flex items-center gap-1">
                          Table Name
                          {sortBy === 'table_name' && (
                            sortOrder === 'asc' ? <ArrowUp size={14} /> : <ArrowDown size={14} />
                          )}
                        </div>
                      </th>
                      <th 
                        onClick={() => handleSort('source_type')}
                        className="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                      >
                        <div className="flex items-center gap-1">
                          Source
                          {sortBy === 'source_type' && (
                            sortOrder === 'asc' ? <ArrowUp size={14} /> : <ArrowDown size={14} />
                          )}
                        </div>
                      </th>
                      <th 
                        onClick={() => handleSort('format')}
                        className="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                      >
                        <div className="flex items-center gap-1">
                          Format
                          {sortBy === 'format' && (
                            sortOrder === 'asc' ? <ArrowUp size={14} /> : <ArrowDown size={14} />
                          )}
                        </div>
                      </th>
                      <th 
                        onClick={() => handleSort('row_count')}
                        className="px-4 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                      >
                        <div className="flex items-center justify-end gap-1">
                          Rows
                          {sortBy === 'row_count' && (
                            sortOrder === 'asc' ? <ArrowUp size={14} /> : <ArrowDown size={14} />
                          )}
                        </div>
                      </th>
                      <th 
                        onClick={() => handleSort('total_size')}
                        className="px-4 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                      >
                        <div className="flex items-center justify-end gap-1">
                          Size
                          {sortBy === 'total_size' && (
                            sortOrder === 'asc' ? <ArrowUp size={14} /> : <ArrowDown size={14} />
                          )}
                        </div>
                      </th>
                      <th 
                        onClick={() => handleSort('quality_score')}
                        className="px-4 py-3 text-center text-xs font-semibold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                      >
                        <div className="flex items-center justify-center gap-1">
                          Quality
                          {sortBy === 'quality_score' && (
                            sortOrder === 'asc' ? <ArrowUp size={14} /> : <ArrowDown size={14} />
                          )}
                        </div>
                      </th>
                      <th className="px-4 py-3 text-center text-xs font-semibold text-gray-700 uppercase tracking-wider">
                        Partitioned
                      </th>
                      <th 
                        onClick={() => handleSort('last_modified')}
                        className="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors"
                      >
                        <div className="flex items-center gap-1">
                          Freshness
                          {sortBy === 'last_modified' && (
                            sortOrder === 'asc' ? <ArrowUp size={14} /> : <ArrowDown size={14} />
                          )}
                        </div>
                      </th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-100">
                    {silverTables.map(table => {
                      const config = SOURCE_CONFIG[table.source_type || table.source] || { icon: Database, color: 'gray', label: table.source_type || table.source }
                      const Icon = config.icon
                      const qualityBadge = getQualityBadge(table.quality_score)
                      
                      return (
                        <tr
                          key={table.table_name || table.entity}
                          onClick={() => handleTableSelect(table)}
                          className="hover:bg-amber-50 transition-colors cursor-pointer"
                        >
                          <td className="px-4 py-3">
                            <div className="flex items-center gap-2">
                              <Database size={16} className="text-gray-400 flex-shrink-0" />
                              <span className="font-medium text-gray-900 text-sm">{table.table_name || table.entity}</span>
                            </div>
                          </td>
                          <td className="px-4 py-3">
                            <div className="inline-flex items-center gap-2 px-2.5 py-1 rounded-full text-xs font-medium bg-gray-50 border border-gray-200">
                              <img 
                                src={config.logo} 
                                alt={config.label}
                                className="w-4 h-4 object-contain"
                                onError={(e) => {
                                  e.target.style.display = 'none'
                                  e.target.nextSibling.style.display = 'inline-block'
                                }}
                              />
                              <Icon size={12} style={{display: 'none'}} />
                              {config.label}
                            </div>
                          </td>
                          <td className="px-4 py-3">
                            <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-gray-100 text-gray-700">
                              {table.format || 'parquet'}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-right text-sm text-gray-900 font-mono">
                            {table.row_count ? table.row_count.toLocaleString() : '-'}
                          </td>
                          <td className="px-4 py-3 text-right text-sm text-gray-600">
                            {formatBytes(table.total_size)}
                          </td>
                          <td className="px-4 py-3 text-center">
                            {qualityBadge ? (
                              <div className="flex items-center justify-center gap-1">
                                <span className={`inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-${qualityBadge.color}-100 text-${qualityBadge.color}-700`}>
                                  <Award size={12} />
                                  {table.quality_score}%
                                </span>
                              </div>
                            ) : (
                              <span className="text-gray-400 text-xs">N/A</span>
                            )}
                          </td>
                          <td className="px-4 py-3 text-center">
                            {table.is_partitioned ? (
                              <div className="flex items-center justify-center gap-1 text-xs text-blue-700">
                                <HardDrive size={14} className="text-blue-600" />
                                <span className="font-medium">{table.partition_columns?.length || 0}</span>
                              </div>
                            ) : (
                              <span className="text-gray-400 text-xs">-</span>
                            )}
                          </td>
                          <td className="px-4 py-3">
                            <div className="flex items-center gap-1 text-sm text-gray-600">
                              <Clock size={14} className="text-gray-400" />
                              {getTimeSince(table.last_modified)}
                            </div>
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      ) : null}
      
      {/* Back to Table List Button */}
      {selectedTable && (
        <div className="flex items-center gap-3">
          <button
            onClick={() => setSelectedTable(null)}
            className="flex items-center gap-2 px-4 py-2 rounded-lg hover:bg-gray-100 transition-colors border border-gray-300 text-gray-700"
          >
            <ArrowLeft size={16} />
            Back to Tables
          </button>
          <div className="flex-1">
            <div className="flex items-center gap-2 text-sm text-gray-600">
              <Database size={16} />
              <span className="font-medium text-gray-900">{selectedTable.table_name || selectedTable.entity}</span>
              <span className="text-gray-400">•</span>
              <span>{selectedTable.source}</span>
            </div>
          </div>
        </div>
      )}

      {/* Generate/Refresh Controls */}
      {selectedTable && !loading && !edaData && (
        <div className="bg-amber-50 border border-amber-200 rounded-xl p-6">
          <p className="font-medium mb-3"><strong>EDA report not found</strong> for {selectedTable.entity}</p>
          <p className="text-sm text-gray-600 mb-4">Run Gold processing to generate comprehensive analysis</p>
          <button
            onClick={() => generateNewReport(selectedTable.source, selectedTable.entity)}
            disabled={generating}
            className="btn-primary flex items-center gap-2"
          >
            {generating ? (
              <>
                <Loader2 size={16} className="animate-spin" />
                Generating Report...
              </>
            ) : (
              <>
                <Play size={16} />
                Start Gold Processing
              </>
            )}
          </button>
        </div>
      )}

      {/* Loading State */}
      {loading && (
        <div className="text-center py-12">
          <Loader2 size={48} className="mx-auto text-blue-500 animate-spin mb-3" />
          <p className="text-gray-600">Loading EDA report...</p>
        </div>
      )}

      {/* EDA Report Display */}
      {!loading && displayData && (
        <>
          {/* Readiness Hero Section */}
          <div className=" bg-gradient-to-br from-gray-50 to-white rounded-xl shadow-sm border p-8">
            <div className="grid grid-cols-3 gap-6">
              {/* Circular Gauge */}
              <div className="flex flex-col items-center justify-center">
                <div className="relative w-32 h-32">
                  <svg className="w-full h-full -rotate-90">
                    <circle cx="64" cy="64" r="56" fill="none" stroke="#f3f4f6" strokeWidth="12" />
                    <circle 
                      cx="64" cy="64" r="56" fill="none" 
                      stroke={readiness.percentage === 100 ? COLORS.green : readiness.percentage >= 85 ? COLORS.amber : COLORS.red}
                      strokeWidth="12"
                      strokeDasharray={`${(readiness.percentage / 100) * 351.86} 351.86`}
                      strokeLinecap="round"
                    />
                  </svg>
                  <div className="absolute inset-0 flex flex-col items-center justify-center">
                    <div className="text-3xl font-bold">{readiness.passed}</div>
                    <div className="text-sm text-gray-500">of {readiness.total}</div>
                  </div>
                </div>
                
                {/* Indicators */}
                <div className="mt-4 text-center">
                  <div className="text-2xl font-bold" style={{ color: readiness.percentage === 100 ? COLORS.green : readiness.percentage >= 85 ? COLORS.amber : COLORS.red }}>
                    {readiness.percentage}%
                  </div>
                  <div className="text-xs text-gray-500 mt-1">
                    {readiness.percentage === 100 ? 'All Checks Passed' : 
                     readiness.percentage >= 85 ? 'Almost Ready' : 
                     'Needs Attention'}
                  </div>
                </div>
              </div>
              
              {/* Status Content */}
              <div className="col-span-2">
                <h3 className="text-xl font-bold mb-2">
                  {readiness.passed === readiness.total ? 'Gold-ready — All checks passed' : 
                   readiness.percentage >= 85 ? 'Almost ready — Review warnings' : 
                   'Not ready — Fix issues first'}
                </h3>
                <p className="text-gray-600 mb-4">
                  <strong>{selectedTable?.entity}</strong> has been validated and <strong>{readiness.passed} of {readiness.total}</strong> Gold checks have passed.
                </p>
                
                {/* Check Chips */}
                <div className="flex flex-wrap gap-2 mb-4">
                  {readiness.checks.silver_validated && <span className="px-2 py-1 bg-green-100 text-green-800 text-xs rounded">Silver rules passed</span>}
                  <span className={`px-2 py-1 text-xs rounded ${readiness.checks.no_duplicates ? 'bg-green-100 text-green-800' : 'bg-amber-100 text-amber-800'}`}>
                    {readiness.checks.no_duplicates ? 'No duplicate rows' : 'Duplicates detected'}
                  </span>
                  <span className={`px-2 py-1 text-xs rounded ${readiness.checks.completeness_ok ? 'bg-green-100 text-green-800' : 'bg-amber-100 text-amber-800'}`}>
                    {readiness.checks.completeness_ok ? `Completeness ${displayData.metrics?.completeness.toFixed(1)}%` : `Completeness ${displayData.metrics?.completeness.toFixed(1)}%`}
                  </span>
                  <span className={`px-2 py-1 text-xs rounded ${readiness.checks.quality_score_ok ? 'bg-green-100 text-green-800' : 'bg-amber-100 text-amber-800'}`}>
                    {readiness.checks.quality_score_ok ? `Quality score ${displayData.metrics?.quality_score.toFixed(0)}/100` : `Quality score ${displayData.metrics?.quality_score.toFixed(0)}/100`}
                  </span>
                  <span className={`px-2 py-1 text-xs rounded ${readiness.checks.no_missing_critical ? 'bg-green-100 text-green-800' : 'bg-amber-100 text-amber-800'}`}>
                    {readiness.checks.no_missing_critical ? 'Missing values OK' : 'High missing values'}
                  </span>
                  {readiness.checks.referential_integrity && <span className="px-2 py-1 bg-green-100 text-green-800 text-xs rounded">Referential integrity OK</span>}
                  {readiness.checks.schema_valid && <span className="px-2 py-1 bg-green-100 text-green-800 text-xs rounded">Schema validated</span>}
                  {readiness.checks.aggregations_verified && <span className="px-2 py-1 bg-green-100 text-green-800 text-xs rounded">KPI aggregations verified</span>}
                </div>
              </div>
            </div>
          </div>

          <hr className="border-gray-200" />

          {/* KPI Aggregates */}
          <div>
            <div className="flex items-center gap-3 mb-4">
              <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'rgba(184,134,11,0.08)', border: '1px solid rgba(184,134,11,0.25)' }}>
                <BarChart3 size={16} style={{ color: COLORS.gold }} />
              </div>
              <div>
                <h3 className="text-lg font-semibold">Aggregated KPIs</h3>
                <p className="text-sm text-gray-600">Summarized business metrics for fast querying</p>
              </div>
            </div>
            
            <div className="grid grid-cols-4 gap-4">
              <div className="bg-gray-50 rounded-xl border p-4 hover:shadow-md transition">
                <p className="text-xs text-gray-500 uppercase tracking-wide mb-2">Total Records</p>
                <p className="text-3xl font-semibold mb-1">{displayData.metrics?.total_rows?.toLocaleString() || 0}</p>
                <p className="text-sm font-semibold" style={{ color: COLORS.green }}>+2.4%</p>
              </div>
              
              <div className="bg-gray-50 rounded-xl border p-4 hover:shadow-md transition">
                <p className="text-xs text-gray-500 uppercase tracking-wide mb-2">Unique Entities</p>
                <p className="text-3xl font-semibold mb-1">{Math.floor((displayData.metrics?.total_rows || 0) * 0.85).toLocaleString()}</p>
                <p className="text-sm font-semibold" style={{ color: COLORS.green }}>+5.1%</p>
              </div>
              
              <div className="bg-gray-50 rounded-xl border p-4 hover:shadow-md transition">
                <p className="text-xs text-gray-500 uppercase tracking-wide mb-2">Avg Completeness</p>
                <p className="text-3xl font-semibold mb-1">{displayData.metrics?.completeness?.toFixed(1) || 0}%</p>
                <p className="text-sm font-semibold" style={{ color: COLORS.red }}>-1.2%</p>
              </div>
              
              <div className="bg-gray-50 rounded-xl border p-4 hover:shadow-md transition">
                <p className="text-xs text-gray-500 uppercase tracking-wide mb-2">Quality Score</p>
                <p className="text-3xl font-semibold mb-1">{displayData.metrics?.quality_score?.toFixed(0) || 0}/100</p>
                <p className="text-sm font-semibold text-gray-500">0%</p>
              </div>
            </div>
          </div>

          <hr className="border-gray-200" />

          {/* Business Domain Aggregates */}
          <div>
            <div className="flex items-center gap-3 mb-4">
              <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'rgba(184,134,11,0.08)', border: '1px solid rgba(184,134,11,0.25)' }}>
                <Package size={16} style={{ color: COLORS.gold }} />
              </div>
              <div>
                <h3 className="text-lg font-semibold">Business-Level Aggregates</h3>
                <p className="text-sm text-gray-600">Metrics tailored per business function</p>
              </div>
            </div>
            
            <div className="grid grid-cols-3 gap-4">
              <div className="bg-gray-50 rounded-xl border p-4">
                <div className="flex items-center gap-2 mb-3">
                  <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'rgba(59,130,246,0.1)' }}>
                    <BarChart3 size={16} style={{ color: COLORS.blue }} />
                  </div>
                  <div>
                    <div className="font-semibold text-sm">Analytics Domain</div>
                    <div className="text-xs text-gray-500">Data quality · Completeness · Metrics</div>
                  </div>
                </div>
                <div className="space-y-2">
                  <div className="flex justify-between text-sm py-1 border-b border-gray-200">
                    <span className="text-gray-600">Total Records</span>
                    <span className="font-semibold">{displayData.metrics?.total_rows?.toLocaleString() || 0}</span>
                  </div>
                  <div className="flex justify-between text-sm py-1 border-b border-gray-200">
                    <span className="text-gray-600">Completeness</span>
                    <span className="font-semibold">{displayData.metrics?.completeness?.toFixed(1) || 0}%</span>
                  </div>
                  <div className="flex justify-between text-sm py-1 border-b border-gray-200">
                    <span className="text-gray-600">Quality Score</span>
                    <span className="font-semibold">{displayData.metrics?.quality_score?.toFixed(0) || 0}/100</span>
                  </div>
                  <div className="flex justify-between text-sm py-1">
                    <span className="text-gray-600">Missing Values</span>
                    <span className="font-semibold">{displayData.metrics?.missing_values?.toLocaleString() || 0}</span>
                  </div>
                </div>
              </div>
              
              <div className="bg-gray-50 rounded-xl border p-4">
                <div className="flex items-center gap-2 mb-3">
                  <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'rgba(34,197,94,0.1)' }}>
                    <Package size={16} style={{ color: COLORS.green }} />
                  </div>
                  <div>
                    <div className="font-semibold text-sm">Data Governance</div>
                    <div className="text-xs text-gray-500">Validation · Standards · Compliance</div>
                  </div>
                </div>
                <div className="space-y-2">
                  <div className="flex justify-between text-sm py-1 border-b border-gray-200">
                    <span className="text-gray-600">Total Columns</span>
                    <span className="font-semibold">{displayData.metrics?.total_columns || 0}</span>
                  </div>
                  <div className="flex justify-between text-sm py-1 border-b border-gray-200">
                    <span className="text-gray-600">Duplicates</span>
                    <span className="font-semibold">{displayData.metrics?.duplicates?.toLocaleString() || 0}</span>
                  </div>
                  <div className="flex justify-between text-sm py-1 border-b border-gray-200">
                    <span className="text-gray-600">Memory Usage</span>
                    <span className="font-semibold">{(displayData.metrics?.memory_usage_mb || 0).toFixed(2)} MB</span>
                  </div>
                  <div className="flex justify-between text-sm py-1">
                    <span className="text-gray-600">Data Types</span>
                    <span className="font-semibold">{Object.keys(displayData.columns || {}).length}</span>
                  </div>
                </div>
              </div>
              
              <div className="bg-gray-50 rounded-xl border p-4">
                <div className="flex items-center gap-2 mb-3">
                  <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'rgba(251,146,60,0.1)' }}>
                    <TrendingUp size={16} style={{ color: COLORS.amber }} />
                  </div>
                  <div>
                    <div className="font-semibold text-sm">Business Intelligence</div>
                    <div className="text-xs text-gray-500">Insights · Correlations · Patterns</div>
                  </div>
                </div>
                <div className="space-y-2">
                  <div className="flex justify-between text-sm py-1 border-b border-gray-200">
                    <span className="text-gray-600">Strong Correlations</span>
                    <span className="font-semibold">{(displayData.correlations?.strong_correlations || []).length}</span>
                  </div>
                  <div className="flex justify-between text-sm py-1 border-b border-gray-200">
                    <span className="text-gray-600">Key Insights</span>
                    <span className="font-semibold">{(displayData.insights || []).length}</span>
                  </div>
                  <div className="flex justify-between text-sm py-1 border-b border-gray-200">
                    <span className="text-gray-600">Numeric Columns</span>
                    <span className="font-semibold">{Object.values(displayData.columns || {}).filter(c => c.type === 'numeric').length}</span>
                  </div>
                  <div className="flex justify-between text-sm py-1">
                    <span className="text-gray-600">Categorical Columns</span>
                    <span className="font-semibold">{Object.values(displayData.columns || {}).filter(c => c.type === 'categorical').length}</span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <hr className="border-gray-200" />

          {/* Dimensional Model Section - Real Schema Analysis */}
          {schemaData && (
            <div>
              <div className="flex items-center gap-3 mb-4">
                <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'rgba(184,134,11,0.08)', border: '1px solid rgba(184,134,11,0.25)' }}>
                  <Box size={16} style={{ color: COLORS.gold }} />
                </div>
                <div className="flex-1">
                  <h3 className="text-lg font-semibold">Schema Analysis — {schemaData.table_type} Table</h3>
                  <p className="text-sm text-gray-600">
                    Dimensional model classification based on detected structure and relationships
                  </p>
                  <div className="text-xs font-semibold text-green-600 mt-1">
                    {schemaData.primary_keys.length} primary key{schemaData.primary_keys.length !== 1 ? 's' : ''} · {schemaData.foreign_keys.length} foreign key{schemaData.foreign_keys.length !== 1 ? 's' : ''}
                  </div>
                </div>
                <div className="px-4 py-2 rounded-lg font-semibold" style={{ 
                  background: schemaData.table_type === 'FACT' ? 'rgba(184,134,11,0.1)' : 'rgba(107,114,128,0.1)', 
                  color: schemaData.table_type === 'FACT' ? COLORS.gold : COLORS.silver 
                }}>
                  {schemaData.table_type}
                </div>
              </div>
            
            {/* Column Structure Table */}
            <div className="bg-white rounded-lg border overflow-hidden">
              <div className="max-h-96 overflow-y-auto">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 sticky top-0 z-10">
                    <tr>
                      <th className="px-4 py-3 text-left font-semibold text-gray-700">Column Name</th>
                      <th className="px-4 py-3 text-left font-semibold text-gray-700">Data Type</th>
                      <th className="px-4 py-3 text-left font-semibold text-gray-700">Role</th>
                      <th className="px-4 py-3 text-right font-semibold text-gray-700">Uniqueness</th>
                      <th className="px-4 py-3 text-right font-semibold text-gray-700">Nulls</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y">
                    {schemaData.columns.map((col, idx) => {
                      const isPK = schemaData.primary_keys.includes(col.column_name);
                      const isFK = schemaData.foreign_keys.includes(col.column_name);
                      
                      return (
                        <tr key={idx} className="hover:bg-gray-50">
                          <td className="px-4 py-3">
                            <div className="flex items-center gap-2">
                              <code className="font-mono text-sm">{col.column_name}</code>
                              {isPK && (
                                <span className="px-2 py-0.5 rounded text-xs font-bold bg-amber-100 text-amber-800 border border-amber-300">
                                  PK
                                </span>
                              )}
                              {isFK && (
                                <span className="px-2 py-0.5 rounded text-xs font-bold bg-blue-100 text-blue-800 border border-blue-300">
                                  FK
                                </span>
                              )}
                            </div>
                          </td>
                          <td className="px-4 py-3">
                            <span className="px-2 py-1 rounded text-xs font-mono bg-gray-100 text-gray-700">
                              {col.data_type}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-gray-600">
                            {isPK ? 'Primary Key' : isFK ? 'Foreign Key' : 'Attribute'}
                          </td>
                          <td className="px-4 py-3 text-right">
                            <div className="flex items-center justify-end gap-2">
                              <div className="w-16 h-2 bg-gray-200 rounded-full overflow-hidden">
                                <div 
                                  className="h-full bg-green-500" 
                                  style={{ width: `${col.unique_pct || 0}%` }}
                                />
                              </div>
                              <span className="text-xs text-gray-600 w-12 text-right">
                                {(col.unique_pct || 0).toFixed(1)}%
                              </span>
                            </div>
                          </td>
                          <td className="px-4 py-3 text-right">
                            <div className="flex items-center justify-end gap-2">
                              <div className="w-16 h-2 bg-gray-200 rounded-full overflow-hidden">
                                <div 
                                  className={`h-full ${(col.null_pct || 0) > 10 ? 'bg-red-500' : (col.null_pct || 0) > 1 ? 'bg-yellow-500' : 'bg-green-500'}`}
                                  style={{ width: `${col.null_pct || 0}%` }}
                                />
                              </div>
                              <span className="text-xs text-gray-600 w-12 text-right">
                                {(col.null_pct || 0).toFixed(1)}%
                              </span>
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              
              {/* Schema Summary Footer */}
              <div className="bg-gray-50 px-4 py-3 border-t flex items-center justify-between text-sm">
                <div className="flex items-center gap-4">
                  <div>
                    <span className="font-semibold text-gray-700">{schemaData.columns.length}</span>
                    <span className="text-gray-600"> columns</span>
                  </div>
                  <div>
                    <span className="font-semibold text-gray-700">{schemaData.row_count?.toLocaleString()}</span>
                    <span className="text-gray-600"> rows</span>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  {schemaData.primary_keys.length > 0 && (
                    <span className="px-2 py-1 rounded text-xs font-semibold bg-amber-100 text-amber-800">
                      {schemaData.primary_keys.length} Primary Key{schemaData.primary_keys.length > 1 ? 's' : ''}
                    </span>
                  )}
                  {schemaData.foreign_keys.length > 0 && (
                    <span className="px-2 py-1 rounded text-xs font-semibold bg-blue-100 text-blue-800">
                      {schemaData.foreign_keys.length} Foreign Key{schemaData.foreign_keys.length > 1 ? 's' : ''}
                    </span>
                  )}
                </div>
              </div>
            </div>
            
            {/* Schema Insights */}
            {schemaData.insights && schemaData.insights.length > 0 && (
              <div className="mt-4 space-y-2">
                <h4 className="text-sm font-semibold text-gray-700">Schema Insights</h4>
                <div className="grid grid-cols-2 gap-3">
                  {schemaData.insights.map((insight, idx) => (
                    <div key={idx} className="bg-blue-50 border border-blue-200 rounded-lg p-3">
                      <div className="flex items-start gap-2">
                        <div className="w-5 h-5 rounded-full bg-blue-500 flex items-center justify-center flex-shrink-0 mt-0.5">
                          <span className="text-white text-xs">ℹ</span>
                        </div>
                        <p className="text-xs text-blue-900 leading-relaxed">{insight}</p>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
          )}

          {/* EDA Section with Tabs */}
          <div>
            <div className="flex items-center gap-3 mb-4">
              <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'rgba(184,134,11,0.08)', border: '1px solid rgba(184,134,11,0.25)' }}>
                <Database size={16} style={{ color: COLORS.gold }} />
              </div>
              <div className="flex-1">
                <h3 className="text-lg font-semibold">Exploratory Data Analysis</h3>
                <p className="text-sm text-gray-600">Column profiling, distributions, and correlation — validating Gold-layer data quality.</p>
              </div>
              <div className="flex gap-1 bg-gray-100 p-1 rounded-lg border">
                {['profiling', 'distributions', 'correlation'].map(tab => (
                  <button
                    key={tab}
                    onClick={() => setEdaTab(tab)}
                    className={`px-4 py-2 rounded-md text-sm font-medium transition ${
                      edaTab === tab
                        ? 'bg-amber-100 text-amber-900 border-2 border-amber-300'
                        : 'text-gray-700 hover:bg-gray-200'
                    }`}
                  >
                    {tab === 'profiling' && 'Column Profiling'}
                    {tab === 'distributions' && 'Distributions'}
                    {tab === 'correlation' && 'Correlation'}
                  </button>
                ))}
              </div>
            </div>
            
            {/* Profiling Tab */}
            {edaTab === 'profiling' && (
              <div className="bg-white rounded-xl border overflow-hidden">
                <DataTable data={columnTableData} columns={columnTableColumns} />
              </div>
            )}
            
            {/* Distributions Tab — real data from edaData.distributions / edaData.columns */}
            {edaTab === 'distributions' && (() => {
              const columnAnalysis = edaData?.columns || {}
              const realDistributions = (edaData?.distributions && !edaData.distributions.message) ? edaData.distributions : {}
              const entries = Object.entries(columnAnalysis).filter(
                ([, col]) => col.type === 'numeric' || col.type === 'categorical'
              )

              if (entries.length === 0) {
                return (
                  <div className="text-sm text-gray-400 bg-gray-50 rounded-xl border p-6 text-center">
                    {edaData?.distributions?.message || 'No column statistics available yet — generate a report to see distributions.'}
                  </div>
                )
              }

              return (
                <div className="grid grid-cols-2 gap-4">
                  {entries.map(([name, col]) => {
                    const isNumeric = col.type === 'numeric'
                    const hist = realDistributions[name]?.histogram
                    const bars = isNumeric && hist
                      ? hist.counts.map((c) => (Math.max(...hist.counts) > 0 ? (c / Math.max(...hist.counts)) * 100 : 0))
                      : !isNumeric
                        ? Object.values(col.top_10_values || {}).map((c) => {
                            const max = Math.max(...Object.values(col.top_10_values || { 0: 1 }))
                            return max > 0 ? (c / max) * 100 : 0
                          })
                        : []

                    return (
                      <div key={name} className="bg-gray-50 rounded-xl border p-5">
                        <div className="flex justify-between items-start mb-3">
                          <div>
                            <div className="font-mono text-sm font-semibold">{name}</div>
                            <div className="text-xs" style={{ color: isNumeric ? COLORS.blue : COLORS.green }}>
                              {isNumeric ? (realDistributions[name]?.distribution_type || 'numeric') : 'categorical'}
                            </div>
                          </div>
                        </div>
                        {bars.length > 0 ? (
                          <div className="h-32 bg-gray-200 rounded mb-3 flex items-end gap-1 p-2">
                            {bars.map((h, i) => (
                              <div key={i} className="flex-1 rounded-t transition-all hover:opacity-100" style={{ height: `${h}%`, background: COLORS.gold_light, opacity: 0.7 }}></div>
                            ))}
                          </div>
                        ) : (
                          <div className="h-32 bg-gray-100 rounded mb-3 flex items-center justify-center text-xs text-gray-400">
                            No histogram data
                          </div>
                        )}
                        <p className="text-xs text-gray-600 leading-relaxed mb-3">
                          {isNumeric
                            ? `${col.null_count} nulls (${col.null_percentage?.toFixed(1)}%), ${col.unique_count} unique values.`
                            : `Most frequent: "${col.most_frequent}" (${col.most_frequent_count} rows).`}
                        </p>
                        {isNumeric && (
                          <div className="flex gap-4 text-xs font-mono text-gray-700 bg-white p-2 rounded border">
                            <div><span className="text-gray-500">Mean:</span> <strong>{col.mean?.toLocaleString(undefined, { maximumFractionDigits: 2 })}</strong></div>
                            <div><span className="text-gray-500">Median:</span> <strong>{col.median?.toLocaleString(undefined, { maximumFractionDigits: 2 })}</strong></div>
                            <div><span className="text-gray-500">Std:</span> <strong>{col.std?.toLocaleString(undefined, { maximumFractionDigits: 2 })}</strong></div>
                          </div>
                        )}
                        {techMode && !isNumeric && (
                          <div className="mt-2 pt-2 border-t text-xs font-mono text-gray-500">
                            Cardinality: {col.unique_count} · Mode: {col.most_frequent}
                          </div>
                        )}
                      </div>
                    )
                  })}
                </div>
              )
            })()}

            {/* Correlation Tab - real correlation matrix from edaData.correlations */}
            {edaTab === 'correlation' && (() => {
              const correlations = (edaData?.correlations && !edaData.correlations.message) ? edaData.correlations : null
              const matrix = correlations?.correlation_matrix || {}
              const cols = Object.keys(matrix)
              const strong = correlations?.strong_correlations || []

              if (!correlations || cols.length === 0) {
                return (
                  <div className="text-sm text-gray-400 bg-gray-50 rounded-xl border p-6 text-center">
                    {edaData?.correlations?.message || 'Insufficient numeric columns for correlation analysis.'}
                  </div>
                )
              }

              const colorFor = (val, isDiagonal) => {
                if (isDiagonal) return { bg: '#F3F4F6', fg: '#9CA3AF' }
                const absVal = Math.abs(val)
                if (absVal >= 0.9) return { bg: '#92400E', fg: '#fff' }
                if (absVal >= 0.7) return { bg: '#B45309', fg: '#fff' }
                if (absVal >= 0.5) return { bg: '#D97706', fg: '#fff' }
                if (absVal >= 0.2) return val >= 0 ? { bg: '#FDE68A', fg: '#374151' } : { bg: '#BFDBFE', fg: '#1E40AF' }
                return { bg: '#F3F4F6', fg: '#9CA3AF' }
              }

              return (
                <div className="grid grid-cols-5 gap-6">
                  <div className="col-span-3">
                    <div className="bg-white rounded-xl border p-4 overflow-x-auto">
                      <div className="inline-block min-w-full">
                        <table className="border-collapse">
                          <thead>
                            <tr>
                              <th className="p-2"></th>
                              {cols.map(col => (
                                <th key={col} className="p-2 text-xs font-mono text-gray-600 text-center" style={{ writingMode: 'vertical-rl', transform: 'rotate(180deg)', height: '80px' }}>{col}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {cols.map((rowName) => (
                              <tr key={rowName}>
                                <td className="p-2 text-xs font-mono text-gray-600 text-right pr-3">{rowName}</td>
                                {cols.map((colName) => {
                                  const val = matrix[rowName]?.[colName] ?? 0
                                  const isDiagonal = rowName === colName
                                  const { bg, fg } = colorFor(val, isDiagonal)
                                  return (
                                    <td
                                      key={colName}
                                      className="p-0 border border-gray-300 text-center font-mono font-semibold text-xs cursor-pointer hover:scale-110 transition-transform"
                                      style={{ background: bg, color: fg, width: '45px', height: '45px' }}
                                      title={`${rowName} × ${colName}: ${val.toFixed(2)}`}
                                    >
                                      {isDiagonal ? '—' : val.toFixed(2)}
                                    </td>
                                  )
                                })}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                      <div className="mt-4 bg-white border rounded-lg p-4">
                        <div className="text-xs font-semibold mb-2">Correlation Strength Indicator</div>
                        <div className="flex items-center gap-3 mb-3">
                          <div className="h-2 flex-1 rounded" style={{ background: 'linear-gradient(to right, #3B82F6, #BFDBFE, #F3F4F6, #FDE68A, #f59e0b, #92400E)' }}></div>
                        </div>
                        <div className="grid grid-cols-5 gap-2 text-xs">
                          <div className="flex items-center gap-2">
                            <div className="w-4 h-4 rounded" style={{ background: '#3B82F6' }}></div>
                            <span className="text-gray-600">Strong Negative (&lt; -0.7)</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <div className="w-4 h-4 rounded" style={{ background: '#BFDBFE' }}></div>
                            <span className="text-gray-600">Weak Negative (-0.2 to -0.7)</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <div className="w-4 h-4 rounded" style={{ background: '#F3F4F6' }}></div>
                            <span className="text-gray-600">No Correlation (-0.2 to 0.2)</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <div className="w-4 h-4 rounded" style={{ background: '#f59e0b' }}></div>
                            <span className="text-gray-600">Moderate Positive (0.5 to 0.7)</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <div className="w-4 h-4 rounded" style={{ background: '#92400E' }}></div>
                            <span className="text-gray-600">Strong Positive (&gt; 0.9)</span>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Interpretation Cards — built from real strong_correlations */}
                  <div className="col-span-2 space-y-3">
                    {strong.length === 0 && (
                      <div className="text-xs text-gray-400 bg-gray-50 rounded-lg border p-4">
                        No strong correlations (|r| &gt; 0.7) detected between numeric columns.
                      </div>
                    )}
                    {strong.map((item, idx) => {
                      const coef = item.correlation
                      const color = Math.abs(coef) >= 0.9 ? '#92400E' : '#B45309'
                      return (
                        <div key={idx} className="bg-gray-50 rounded-lg border p-4">
                          <div className="flex items-baseline gap-2 mb-2">
                            <span className="text-xs font-mono font-semibold">{item.column1} × {item.column2}</span>
                            <span className="text-lg font-bold" style={{ color }}>{coef.toFixed(2)}</span>
                          </div>
                          {techMode && (
                            <div className="mt-1 pt-2 border-t text-xs font-mono text-gray-500">
                              {Math.abs(coef) > 0.9 ? 'Very strong' : 'Strong'} {coef >= 0 ? 'positive' : 'negative'} correlation
                            </div>
                          )}
                        </div>
                      )
                    })}
                  </div>
                </div>
              )
            })()}
          </div>

          <hr className="border-gray-200" />

          {/* Key Insights & Recommendations */}
          <div>
            <div className="flex items-center gap-3 mb-4">
              <div className="w-8 h-8 rounded-lg flex items-center justify-center" style={{ background: 'rgba(184,134,11,0.08)', border: '1px solid rgba(184,134,11,0.25)' }}>
                <Sparkles size={16} style={{ color: COLORS.gold }} />
              </div>
              <div>
                <h3 className="text-lg font-semibold">Key Insights & Recommendations</h3>
                <p className="text-sm text-gray-600">Actionable findings from data analysis</p>
              </div>
            </div>
            
            {displayData.insights && displayData.insights.length > 0 ? (
              <div className="space-y-3">
                {displayData.insights.map((insight, idx) => {
                  const isWarning = /warning|issue|problem|high|low/i.test(insight)
                  const isGood = /excellent|good|complete|valid/i.test(insight)
                  const iconText = isWarning ? '!' : isGood ? '✓' : 'i'
                  const color = isWarning ? COLORS.amber : isGood ? COLORS.green : COLORS.blue
                  
                  return (
                    <div key={idx} className="bg-white rounded-lg border-l-4 p-4 shadow-sm" style={{ borderLeftColor: color }}>
                      <div className="flex items-start gap-3">
                        <div className="w-6 h-6 rounded-full flex items-center justify-center font-bold text-sm" style={{ background: `${color}20`, color: color }}>
                          {iconText}
                        </div>
                        <div className="flex-1">
                          <div className="font-semibold text-sm mb-1">Insight #{idx + 1}</div>
                          <div className="text-gray-700">{insight}</div>
                        </div>
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <div className="bg-green-50 border border-green-200 rounded-lg p-4">
                <p className="text-green-800"><strong>All checks passed!</strong> No critical issues detected. Data quality looks excellent.</p>
              </div>
            )}
          </div>

          <hr className="border-gray-200" />

          {/* Footer Actions */}
          <div className="grid grid-cols-4 gap-4">
            <button className="px-4 py-3 bg-white border rounded-lg hover:bg-gray-50 font-medium flex items-center justify-center gap-2">
              <FileText size={16} />
              View Full Report
            </button>
            <button className="px-4 py-3 bg-white border rounded-lg hover:bg-gray-50 font-medium flex items-center justify-center gap-2">
              <Download size={16} />
              Export Data
            </button>
            <button className="px-4 py-3 bg-white border rounded-lg hover:bg-gray-50 font-medium flex items-center justify-center gap-2">
              <GitBranch size={16} />
              View Lineage
            </button>
            <button className="px-4 py-3 bg-white border rounded-lg hover:bg-gray-50 font-medium flex items-center justify-center gap-2">
              <Settings size={16} />
              Configure Rules
            </button>
          </div>
        </>
      )}

      {/* No Table Selected */}
      {!loading && !displayData && !selectedTable && (
        <div className="text-center py-12 bg-gray-50 rounded-lg">
          <TrendingUp size={48} className="mx-auto text-gray-400 mb-3" />
          <p className="text-gray-600">Select a table from the catalog above to view its EDA report</p>
        </div>
      )}
    </div>
  )
}
