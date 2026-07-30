import { useState, useEffect } from 'react'
import { useParams } from 'react-router-dom'
import { Database, FileText, RefreshCw, BarChart3, Eye, Clock, Layers, CheckCircle, Search, X, ArrowLeft, ChevronDown, ChevronRight, Download, ZoomIn, ZoomOut, Image as ImageIcon, FileJson, FilePlus, Video, FileType, Filter, ArrowUp, ArrowDown, Award, HardDrive, Calendar, TrendingUp, AlertCircle, HelpCircle } from 'lucide-react'
import axios from 'axios'
import LoadingSpinner from '../components/ui/LoadingSpinner'
import Alert from '../components/ui/Alert'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, PieChart, Pie, Cell, Legend } from 'recharts'

const API_BASE = 'http://localhost:8000/api'

// Help tooltip component
const HelpTooltip = ({ title, children }) => {
  const [show, setShow] = useState(false)
  
  return (
    <div className="relative inline-block">
      <button
        onMouseEnter={() => setShow(true)}
        onMouseLeave={() => setShow(false)}
        onClick={() => setShow(!show)}
        className="ml-2 text-gray-400 hover:text-gray-600 transition-colors"
      >
        <HelpCircle size={16} />
      </button>
      {show && (
        <div className="absolute z-50 w-64 p-3 bg-gray-900 text-white text-sm rounded-lg shadow-lg -top-2 left-6">
          {title && <div className="font-semibold mb-1">{title}</div>}
          <div className="text-gray-200">{children}</div>
          <div className="absolute top-3 -left-1 w-2 h-2 bg-gray-900 transform rotate-45"></div>
        </div>
      )}
    </div>
  )
}

// Source type configuration with real logos
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

// Neutral colors for data type distribution (no alarm colors)
const COLORS = ['#3b82f6', '#10b981', '#8b5cf6', '#f59e0b', '#06b6d4', '#ec4899', '#6366f1', '#14b8a6']

// Color scheme for pipeline status
const LAYER_COLORS = {
  bronze: '#CD7F32',
  silver: '#6B7280',
  gold: '#B8860B',
  green: '#10b981'
}

// Tooltip Component for pipeline
const InfoTooltip = ({ text }) => {
  const [show, setShow] = useState(false)
  
  return (
    <div className="relative inline-block ml-1">
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

// Pipeline Status Banner
const PipelineStatus = () => {
  return (
    <div className="bg-white rounded-lg border shadow-sm p-4">
      <div className="flex items-center gap-4">
        <div className="flex-1 flex items-center gap-3">
          <div className="w-8 h-8 rounded-full flex items-center justify-center" style={{ background: 'rgba(205,127,50,0.15)', border: '2px solid #CD7F32' }}>
            <span className="text-xs font-bold" style={{ color: LAYER_COLORS.bronze }}>B</span>
          </div>
          <div>
            <div className="flex items-center gap-1">
              <div className="font-semibold text-sm" style={{ color: LAYER_COLORS.bronze }}>Bronze Layer</div>
              <InfoTooltip text="The Bronze layer stores raw data exactly as ingested from source systems. No transformations are applied - data is preserved in its original format. This ensures full historical traceability and allows reprocessing if needed." />
            </div>
            <div className="text-xs text-gray-500">Raw ingestion</div>
          </div>
          <div className="text-xs font-semibold" style={{ color: LAYER_COLORS.bronze }}>Ready</div>
        </div>
        
        <div className="text-gray-300 text-xl">›</div>
        
        <div className="flex-1 flex items-center gap-3">
          <div className="w-8 h-8 rounded-full flex items-center justify-center" style={{ background: 'rgba(168,180,192,0.12)', border: '2px solid #6B7280' }}>
            <span className="text-xs font-bold" style={{ color: LAYER_COLORS.silver }}>S</span>
          </div>
          <div>
            <div className="flex items-center gap-1">
              <div className="font-semibold text-sm" style={{ color: LAYER_COLORS.silver }}>Silver Layer</div>
              <InfoTooltip text="The Silver layer contains cleaned and validated data. Transformations include type casting, deduplication, validation rules, and quality checks. Data is structured and ready for analytics." />
            </div>
            <div className="text-xs text-gray-500">Cleaned · Validated</div>
          </div>
          <div className="text-xs font-semibold" style={{ color: '#9CA3AF' }}>Next</div>
        </div>
        
        <div className="text-gray-300 text-xl">›</div>
        
        <div className="flex-1 flex items-center gap-3">
          <div className="w-8 h-8 rounded-full flex items-center justify-center" style={{ background: 'rgba(184,134,11,0.08)', border: '2px solid #B8860B' }}>
            <span className="text-xs font-bold" style={{ color: LAYER_COLORS.gold }}>G</span>
          </div>
          <div>
            <div className="flex items-center gap-1">
              <div className="font-semibold text-sm" style={{ color: LAYER_COLORS.gold }}>Gold Layer</div>
              <InfoTooltip text="The Gold layer contains curated, business-ready data products. Data is aggregated, analyzed, and optimized for reporting and analytics use cases. Includes EDA reports, KPIs, and data marts." />
            </div>
            <div className="text-xs text-gray-500">Aggregated · Curated</div>
          </div>
          <div className="text-xs font-semibold" style={{ color: '#9CA3AF' }}>Next</div>
        </div>
      </div>
    </div>
  )
}

function Bronze() {
  const { domain } = useParams()
  const [tables, setTables] = useState([])
  const [selectedTable, setSelectedTable] = useState(null)
  const [tableDetails, setTableDetails] = useState(null)
  const [loading, setLoading] = useState(true)
  const [detailsLoading, setDetailsLoading] = useState(false)
  const [error, setError] = useState(null)
  const [schemaSearch, setSchemaSearch] = useState('')
  const [sampleSearch, setSampleSearch] = useState('')
  
  // Enhanced search and filter state
  const [searchQuery, setSearchQuery] = useState('')
  const [sourceFilter, setSourceFilter] = useState('')
  const [formatFilter, setFormatFilter] = useState('')
  const [dateFilter, setDateFilter] = useState('all')
  const [sortBy, setSortBy] = useState('last_modified')
  const [sortOrder, setSortOrder] = useState('desc')
  const [showFilters, setShowFilters] = useState(false)
  
  // Pagination state for table listing
  const [listPage, setListPage] = useState(1)
  const [listPageSize, setListPageSize] = useState(50)
  const [listTotalPages, setListTotalPages] = useState(1)
  const [listTotalCount, setListTotalCount] = useState(0)
  
  // Pagination state for detail view
  const [currentPage, setCurrentPage] = useState(1)
  const [pageSize, setPageSize] = useState(100)
  const [totalPages, setTotalPages] = useState(1)
  const [totalRows, setTotalRows] = useState(0)
  const [filteredRows, setFilteredRows] = useState(0)
  const [showingFrom, setShowingFrom] = useState(0)
  const [showingTo, setShowingTo] = useState(0)

  useEffect(() => {
    loadTables()
  }, [domain, searchQuery, sourceFilter, formatFilter, dateFilter, sortBy, sortOrder, listPage, listPageSize])

  const loadTables = async () => {
    try {
      setLoading(true)
      setError(null)
      const response = await axios.get(`${API_BASE}/bronze/tables/${domain}`, {
        params: {
          search: searchQuery,
          source_type: sourceFilter,
          format_filter: formatFilter,
          date_filter: dateFilter,
          sort_by: sortBy,
          sort_order: sortOrder,
          page: listPage,
          page_size: listPageSize
        }
      })
      
      setTables(response.data.tables || [])
      setListTotalPages(response.data.pagination?.total_pages || 1)
      setListTotalCount(response.data.pagination?.total_count || 0)
      
    } catch (err) {
      console.error('Error loading tables:', err)
      setError('Failed to load Bronze layer tables')
    } finally {
      setLoading(false)
    }
  }
  
  const handleSort = (column) => {
    if (sortBy === column) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc')
    } else {
      setSortBy(column)
      setSortOrder('desc')
    }
    setListPage(1)
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
    setListPage(1)
  }

  const loadTableDetails = async (table, page = 1, search = '', size = null) => {
    try {
      setDetailsLoading(true)
      setError(null)
      setSelectedTable(table)
      
      const response = await axios.get(`${API_BASE}/bronze/table/${domain}/${table.table_name}`, {
        params: {
          page: page,
          page_size: size || pageSize,
          search: search
        }
      })
      
      setTableDetails(response.data)
      
      // Only set pagination for structured data
      if (response.data.is_structured !== false) {
        setCurrentPage(response.data.page || 1)
        setTotalPages(response.data.total_pages || 1)
        setTotalRows(response.data.total_rows || 0)
        setFilteredRows(response.data.filtered_rows || 0)
        setShowingFrom(response.data.showing_from || 0)
        setShowingTo(response.data.showing_to || 0)
      } else {
        // Reset pagination for unstructured files
        setCurrentPage(1)
        setTotalPages(1)
        setTotalRows(0)
        setFilteredRows(0)
        setShowingFrom(0)
        setShowingTo(0)
      }
    } catch (err) {
      console.error('Error loading table details:', err)
      console.error('Error response:', err.response?.data)
      const errorMsg = err.response?.data?.detail || `Failed to load details for ${table.table_name}`
      setError(errorMsg)
      setTableDetails(null)
    } finally {
      setDetailsLoading(false)
    }
  }

  const handlePageChange = (newPage) => {
    if (!selectedTable) return
    // Allow page change as long as newPage is valid
    const maxPage = totalPages || 1
    if (newPage >= 1 && newPage <= maxPage) {
      loadTableDetails(selectedTable, newPage, sampleSearch)
    }
  }

  const handlePageSizeChange = (newSize) => {
    setPageSize(newSize)
    setCurrentPage(1)
    if (selectedTable) {
      loadTableDetails(selectedTable, 1, sampleSearch, newSize)
    }
  }

  const handleSampleSearchChange = (searchValue) => {
    setSampleSearch(searchValue)
    setCurrentPage(1)
    if (selectedTable) {
      // Debounce search to avoid too many API calls
      clearTimeout(window.searchTimeout)
      window.searchTimeout = setTimeout(() => {
        loadTableDetails(selectedTable, 1, searchValue)
      }, 300)
    }
  }

  const formatBytes = (bytes) => {
    if (!bytes || bytes === 0) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i]
  }
  
  const getQualityBadge = (score) => {
    if (score === null || score === undefined) return null
    if (score >= 90) return { label: 'Excellent', color: 'green' }
    if (score >= 75) return { label: 'Good', color: 'blue' }
    if (score >= 50) return { label: 'Fair', color: 'yellow' }
    return { label: 'Poor', color: 'red' }
  }

  const getColumnTypeDistribution = (schema) => {
    if (!schema || !schema.columns) return { data: [], total: 0 }
    const typeCounts = {}
    schema.columns.forEach(col => {
      const baseType = col.dtype?.split('(')[0] || 'unknown'
      typeCounts[baseType] = (typeCounts[baseType] || 0) + 1
    })
    const data = Object.entries(typeCounts).map(([type, count]) => ({
      name: type,
      value: count
    }))
    return {
      data,
      total: schema.columns.length
    }
  }

  const getNullPercentageData = (schema) => {
    if (!schema || !schema.columns) return []
    return schema.columns
      .filter(col => col.null_percentage > 0)
      .sort((a, b) => b.null_percentage - a.null_percentage)
      .slice(0, 10)
      .map(col => ({
        column: col.name,
        percentage: col.null_percentage
      }))
  }

  const getFilteredSchema = () => {
    if (!tableDetails?.schema?.columns) return []
    if (!schemaSearch) return tableDetails.schema.columns
    return tableDetails.schema.columns.filter(col =>
      col.name.toLowerCase().includes(schemaSearch.toLowerCase()) ||
      col.dtype.toLowerCase().includes(schemaSearch.toLowerCase())
    )
  }

  const getFilteredSampleData = () => {
    if (!tableDetails?.sample_data) return []
    if (!sampleSearch) return tableDetails.sample_data
    return tableDetails.sample_data.filter(row =>
      Object.values(row).some(value =>
        String(value).toLowerCase().includes(sampleSearch.toLowerCase())
      )
    )
  }

  const getQualityScore = (schema) => {
    if (!schema || !schema.columns) return 0
    const totalCols = schema.columns.length
    const colsWithNoNulls = schema.columns.filter(col => col.null_percentage === 0).length
    return Math.round((colsWithNoNulls / totalCols) * 100)
  }

  const getTimeSince = (dateStr) => {
    if (!dateStr) return 'Unknown'
    const date = new Date(dateStr)
    const now = new Date()
    const seconds = Math.floor((now - date) / 1000)
    
    if (seconds < 60) return `${seconds}s ago`
    const minutes = Math.floor(seconds / 60)
    if (minutes < 60) return `${minutes}m ago`
    const hours = Math.floor(minutes / 60)
    if (hours < 24) return `${hours}h ago`
    const days = Math.floor(hours / 24)
    return `${days}d ago`
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <LoadingSpinner size="lg" />
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Bronze Layer Header Card */}
      <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between">
          <div>
            <div className="flex items-center">
              <h1 className="text-3xl font-bold" style={{ color: '#CD7F32' }}>
                Bronze Layer — Data Ingestion
              </h1>
              <HelpTooltip title="What is Bronze Layer?">
                The Bronze layer stores raw data exactly as ingested from source systems. 
                No transformations are applied - data is preserved in its original format. 
                This ensures full historical traceability and allows reprocessing if needed.
              </HelpTooltip>
            </div>
            <p className="text-base text-gray-600 mt-2">
              Raw data ingested from multiple sources, preserved in original format
            </p>
          </div>
          <button
            onClick={loadTables}
            className="flex items-center gap-2 px-4 py-2 rounded-lg hover:bg-orange-50 transition-colors"
            style={{ border: '1.5px solid #CD7F32', color: '#CD7F32' }}
          >
            <RefreshCw className="w-4 h-4" />
            <span className="font-semibold">Refresh</span>
          </button>
        </div>
      </div>

      {/* Pipeline Status */}
      <PipelineStatus />

      {error && <Alert type="error">{error}</Alert>}

      {/* Main Content - State-Based View */}
      {!selectedTable ? (
        // STATE A: ENTERPRISE DATA CATALOG
        <div className="space-y-4">
          {/* SEARCH & FILTER: Global Catalog Search */}
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
              <HelpTooltip title="Search Tips">
                Search across table names, schema names, and data sources. 
                The search is real-time and can handle thousands of tables efficiently.
                Combine with filters for more precise results.
              </HelpTooltip>
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
                  showFilters ? 'bg-orange-100 text-orange-700' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                }`}
              >
                <Filter size={16} />
                Filters
                {(sourceFilter || formatFilter || dateFilter !== 'all') && (
                  <span className="w-2 h-2 bg-orange-600 rounded-full"></span>
                )}
              </button>
            </div>
          </div>

          {/* CATALOG SUMMARY: Table Counts */}
          <div className="flex items-center justify-between bg-gradient-to-r from-orange-50 to-white rounded-lg border border-orange-200 px-6 py-3">
            <div className="flex items-center gap-6">
              <div className="flex items-center gap-2">
                <Database className="text-orange-600" size={18} />
                <span className="text-sm text-gray-600">Showing</span>
                <span className="font-bold text-gray-900">{tables.length}</span>
                <span className="text-sm text-gray-600">of</span>
                <span className="font-bold text-gray-900">{listTotalCount}</span>
                <span className="text-sm text-gray-600">tables</span>
              </div>
              {(sourceFilter || formatFilter || dateFilter !== 'all' || searchQuery) && (
                <>
                  <div className="w-px h-4 bg-gray-300"></div>
                  <button
                    onClick={clearFilters}
                    className="text-sm text-orange-600 hover:text-orange-700 font-medium flex items-center gap-1"
                  >
                    <X size={14} />
                    Clear all filters
                  </button>
                </>
              )}
            </div>
          </div>

          {/* Filters Panel + Table Layout */}
          <div className="flex gap-4">
            {/* Left: Filters Panel */}
            {showFilters && (
              <div className="w-64 flex-shrink-0 space-y-4">
                {/* FILTER: Source Type */}
                <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
                  <h3 className="text-sm font-semibold text-gray-900 mb-3 flex items-center gap-2">
                    <Database size={16} />
                    Source Type
                    <HelpTooltip title="Data Sources">
                      Filter by the original data source. Each source type (PostgreSQL, MariaDB, MongoDB, S3, etc.) 
                      has different characteristics and capabilities.
                    </HelpTooltip>
                  </h3>
                  <div className="space-y-2">
                    <button
                      onClick={() => setSourceFilter('')}
                      className={`w-full text-left px-3 py-2 rounded text-sm transition-colors ${
                        sourceFilter === '' ? 'bg-orange-100 text-orange-700 font-medium' : 'hover:bg-gray-50 text-gray-700'
                      }`}
                    >
                      All Sources
                    </button>
                    {Object.keys(SOURCE_CONFIG).map(source => {
                      const config = SOURCE_CONFIG[source]
                      const Icon = config.icon
                      return (
                        <button
                          key={source}
                          onClick={() => setSourceFilter(source)}
                          className={`w-full text-left px-3 py-2 rounded text-sm transition-colors flex items-center gap-2 ${
                            sourceFilter === source ? 'bg-orange-100 text-orange-700 font-medium' : 'hover:bg-gray-50 text-gray-700'
                          }`}
                        >
                          <img 
                            src={config.logo} 
                            alt={config.label}
                            className="w-4 h-4 object-contain flex-shrink-0"
                            onError={(e) => {
                              e.target.style.display = 'none'
                              e.target.nextSibling.style.display = 'inline-block'
                            }}
                          />
                          <Icon size={14} style={{display: 'none'}} />
                          {config.label}
                        </button>
                      )
                    })}
                  </div>
                </div>

                {/* FILTER: File Format */}
                <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
                  <h3 className="text-sm font-semibold text-gray-900 mb-3 flex items-center gap-2">
                    <FileType size={16} />
                    Format
                  </h3>
                  <div className="space-y-2">
                    {['', 'parquet', 'json', 'csv', 'image', 'video', 'pdf', 'text'].map(format => (
                      <button
                        key={format}
                        onClick={() => setFormatFilter(format)}
                        className={`w-full text-left px-3 py-2 rounded text-sm transition-colors ${
                          formatFilter === format ? 'bg-orange-100 text-orange-700 font-medium' : 'hover:bg-gray-50 text-gray-700'
                        }`}
                      >
                        {format === '' ? 'All Formats' : format.toUpperCase()}
                      </button>
                    ))}
                  </div>
                </div>

                {/* FILTER: Last Modified Date */}
                <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-4">
                  <h3 className="text-sm font-semibold text-gray-900 mb-3 flex items-center gap-2">
                    <Calendar size={16} />
                    Last Modified
                  </h3>
                  <div className="space-y-2">
                    {[
                      { value: 'all', label: 'All Time' },
                      { value: '24h', label: 'Last 24 Hours' },
                      { value: '7d', label: 'Last 7 Days' },
                      { value: '30d', label: 'Last 30 Days' }
                    ].map(option => (
                      <button
                        key={option.value}
                        onClick={() => setDateFilter(option.value)}
                        className={`w-full text-left px-3 py-2 rounded text-sm transition-colors ${
                          dateFilter === option.value ? 'bg-orange-100 text-orange-700 font-medium' : 'hover:bg-gray-50 text-gray-700'
                        }`}
                      >
                        {option.label}
                      </button>
                    ))}
                  </div>
                </div>
              </div>
            )}

            {/* Right: Sortable Table */}
            <div className="flex-1 bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
              {tables.length === 0 ? (
                <div className="text-center py-16 text-gray-500 px-6">
                  <Database size={56} className="mx-auto mb-4 opacity-20" />
                  <p className="text-lg font-medium">
                    {searchQuery || sourceFilter || formatFilter || dateFilter !== 'all' 
                      ? 'No tables match your filters' 
                      : 'No tables ingested yet'}
                  </p>
                  <p className="text-sm mt-2">
                    {searchQuery || sourceFilter || formatFilter || dateFilter !== 'all'
                      ? 'Try adjusting your search or filters'
                      : 'Start by ingesting data from the Ingestion page'}
                  </p>
                </div>
              ) : (
                <>
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead className="bg-gray-50 border-b border-gray-200 sticky top-0">
                        <tr>
                          {/* Table Name */}
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
                          
                          {/* Source */}
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
                          
                          {/* Format */}
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
                          
                          {/* Rows */}
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
                          
                          {/* Size */}
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
                          
                          {/* Quality */}
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
                          
                          {/* Partitioned */}
                          <th className="px-4 py-3 text-center text-xs font-semibold text-gray-700 uppercase tracking-wider">
                            Partitioned
                          </th>
                          
                          {/* Freshness */}
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
                        {tables.map(table => {
                          const config = SOURCE_CONFIG[table.source_type] || { icon: Database, color: 'gray', label: table.source_type }
                          const Icon = config.icon
                          const qualityBadge = getQualityBadge(table.quality_score)
                          
                          return (
                            <tr
                              key={table.table_name}
                              onClick={() => loadTableDetails(table)}
                              className="hover:bg-gray-50 transition-colors cursor-pointer"
                            >
                              {/* Table Name */}
                              <td className="px-4 py-3">
                                <div className="flex items-center gap-2">
                                  <Database size={16} className="text-gray-400 flex-shrink-0" />
                                  <span className="font-medium text-gray-900 text-sm">{table.table_name}</span>
                                </div>
                              </td>
                              
                              {/* Source */}
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
                              
                              {/* Format */}
                              <td className="px-4 py-3">
                                <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-gray-100 text-gray-700">
                                  {table.format || 'N/A'}
                                </span>
                              </td>
                              
                              {/* Rows */}
                              <td className="px-4 py-3 text-right text-sm text-gray-900 font-mono">
                                {table.row_count ? table.row_count.toLocaleString() : '-'}
                              </td>
                              
                              {/* Size */}
                              <td className="px-4 py-3 text-right text-sm text-gray-600">
                                {formatBytes(table.total_size)}
                              </td>
                              
                              {/* Quality */}
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
                              
                              {/* Partitioned */}
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
                              
                              {/* Freshness */}
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

                  {/* Pagination Controls */}
                  {listTotalPages > 1 && (
                    <div className="border-t border-gray-200 px-6 py-4 bg-gray-50">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2">
                          <label className="text-sm text-gray-600">Rows per page:</label>
                          <select
                            value={listPageSize}
                            onChange={(e) => {
                              setListPageSize(Number(e.target.value))
                              setListPage(1)
                            }}
                            className="border border-gray-300 rounded px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-orange-500 bg-white"
                          >
                            <option value={25}>25</option>
                            <option value={50}>50</option>
                            <option value={100}>100</option>
                            <option value={200}>200</option>
                          </select>
                        </div>
                        
                        <div className="flex items-center gap-2">
                          <button
                            onClick={() => setListPage(1)}
                            disabled={listPage <= 1}
                            className="px-3 py-1.5 border border-gray-300 rounded text-sm font-medium disabled:opacity-40 disabled:cursor-not-allowed hover:enabled:bg-white hover:enabled:border-gray-400 transition-colors"
                          >
                            First
                          </button>
                          <button
                            onClick={() => setListPage(p => p - 1)}
                            disabled={listPage <= 1}
                            className="px-3 py-1.5 border border-gray-300 rounded text-sm font-medium disabled:opacity-40 disabled:cursor-not-allowed hover:enabled:bg-white hover:enabled:border-gray-400 transition-colors"
                          >
                            ◀ Previous
                          </button>
                          <span className="px-4 py-1.5 text-sm text-gray-700 font-medium">
                            Page {listPage} of {listTotalPages}
                          </span>
                          <button
                            onClick={() => setListPage(p => p + 1)}
                            disabled={listPage >= listTotalPages}
                            className="px-3 py-1.5 border border-gray-300 rounded text-sm font-medium disabled:opacity-40 disabled:cursor-not-allowed hover:enabled:bg-white hover:enabled:border-gray-400 transition-colors"
                          >
                            Next ▶
                          </button>
                          <button
                            onClick={() => setListPage(listTotalPages)}
                            disabled={listPage >= listTotalPages}
                            className="px-3 py-1.5 border border-gray-300 rounded text-sm font-medium disabled:opacity-40 disabled:cursor-not-allowed hover:enabled:bg-white hover:enabled:border-gray-400 transition-colors"
                          >
                            Last
                          </button>
                        </div>
                      </div>
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        </div>
      ) : (
        // STATE B: DETAIL VIEW (EDA)
        <div className="space-y-4">
          {/* Back Navigation */}
          <button
            onClick={() => {
              setSelectedTable(null)
              setTableDetails(null)
              setSchemaSearch('')
              setSampleSearch('')
              setCurrentPage(1)
              setTotalPages(1)
            }}
            className="flex items-center gap-2 px-4 py-2 text-orange-600 hover:text-orange-700 hover:bg-orange-50 rounded-lg transition-colors font-medium"
          >
            <ArrowLeft size={18} />
            Back to all tables
          </button>
            {detailsLoading ? (
              <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-12 text-center">
                <LoadingSpinner size="lg" />
                <p className="text-gray-600 mt-4">Loading table details...</p>
              </div>
            ) : !tableDetails ? (
              <Alert type="error">Failed to load table details</Alert>
            ) : (
              <div className="space-y-4">
                {/* NAVIGATION: Breadcrumb Path */}
                <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2 text-sm text-gray-500">
                    <span className="hover:text-gray-700 cursor-pointer" onClick={() => {
                      setSelectedTable(null)
                      setTableDetails(null)
                      setCurrentPage(1)
                      setTotalPages(1)
                    }}>Ingested Tables</span>
                    <span>/</span>
                    <span className="text-gray-700 font-medium">
                      {SOURCE_CONFIG[selectedTable.source_type]?.label || selectedTable.source_type}
                    </span>
                    <span>/</span>
                    <span className="text-gray-900 font-semibold">{tableDetails.table_name}</span>
                  </div>
                  <span className="text-xs text-gray-400">SECTION: Data Detail View</span>
                </div>
                </div>

                {/* Check if unstructured file - render appropriate viewer */}
                {tableDetails.is_structured === false ? (
                  // Render file viewer based on file type
                  (() => {
                    switch (tableDetails.file_type) {
                      case 'image':
                        return <ImageViewer metadata={tableDetails.metadata} domain={domain} tableName={tableDetails.table_name} />
                      case 'video':
                        return <VideoViewer metadata={tableDetails.metadata} domain={domain} tableName={tableDetails.table_name} />
                      case 'pdf':
                        return <PDFViewer metadata={tableDetails.metadata} domain={domain} tableName={tableDetails.table_name} />
                      case 'json':
                        return <JSONViewer metadata={tableDetails.metadata} domain={domain} tableName={tableDetails.table_name} />
                      case 'text':
                        return <TextViewer metadata={tableDetails.metadata} domain={domain} tableName={tableDetails.table_name} />
                      default:
                        return <GenericFileViewer metadata={tableDetails.metadata} domain={domain} tableName={tableDetails.table_name} />
                    }
                  })()
                ) : (
                  <>
                {/* TABLE OVERVIEW: Metadata & Quality Score */}
                <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
                  <div className="mb-2">
                    <span className="text-xs text-gray-500">SECTION: Table Information & Quality Metrics</span>
                  </div>
                  <div className="flex items-start justify-between mb-4">
                    <div className="flex-1">
                      <h2 className="text-2xl font-bold text-gray-900">{tableDetails.table_name}</h2>
                      <p className="text-gray-600 mt-1 capitalize flex items-center gap-2">
                        <span>{domain} • {selectedTable.source_type || 'Unknown Source'}</span>
                        <span className="text-gray-400">•</span>
                        <Clock size={14} className="inline" />
                        <span className="text-sm">Last Updated: {getTimeSince(selectedTable.last_modified)}</span>
                      </p>
                    </div>
                    <div className="flex items-center gap-3">
                      <div className="text-right">
                        <p className="text-sm text-gray-600">Quality Score</p>
                        <p className="text-2xl font-bold text-green-600">{getQualityScore(tableDetails.schema)}%</p>
                      </div>
                      <div className={`w-16 h-16 rounded-full flex items-center justify-center ${
                        getQualityScore(tableDetails.schema) >= 80 ? 'bg-green-100' :
                        getQualityScore(tableDetails.schema) >= 60 ? 'bg-yellow-100' : 'bg-red-100'
                      }`}>
                        <CheckCircle size={32} className={
                          getQualityScore(tableDetails.schema) >= 80 ? 'text-green-600' :
                          getQualityScore(tableDetails.schema) >= 60 ? 'text-yellow-600' : 'text-red-600'
                        } />
                      </div>
                    </div>
                  </div>
                  
                  <div className="grid grid-cols-3 gap-4">
                    <div className="text-center p-3 bg-gray-50 rounded-lg">
                      <p className="text-sm text-gray-600">Total Rows</p>
                      <p className="text-2xl font-bold text-gray-900 mt-1">{tableDetails.total_rows?.toLocaleString()}</p>
                    </div>
                    <div className="text-center p-3 bg-gray-50 rounded-lg">
                      <p className="text-sm text-gray-600">Columns</p>
                      <p className="text-2xl font-bold text-gray-900 mt-1">{tableDetails.schema?.columns?.length || 0}</p>
                    </div>
                    <div className="text-center p-3 bg-gray-50 rounded-lg">
                      <p className="text-sm text-gray-600">File Size</p>
                      <p className="text-2xl font-bold text-gray-900 mt-1">{formatBytes(selectedTable.total_size)}</p>
                    </div>
                  </div>
                </div>

              {/* DATA QUALITY: Pre-Silver Analysis */}
              <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
                <div className="flex items-center gap-2 mb-4">
                  <BarChart3 className="w-5 h-5 text-orange-600" />
                  <h3 className="text-lg font-semibold text-gray-900">Data Quality Analysis (Pre-Silver)</h3>
                  <span className="text-xs text-gray-500 ml-auto">SECTION: Column Analysis</span>
                </div>
                
                <div className="grid grid-cols-2 gap-6">
                  {/* Column Type Distribution */}
                  <div>
                    <h4 className="text-sm font-semibold text-gray-700 mb-3">Column Type Distribution</h4>
                    {(() => {
                      const typeDistribution = getColumnTypeDistribution(tableDetails.schema);
                      return (
                        <div className="relative">
                          <ResponsiveContainer width="100%" height={220}>
                            <PieChart>
                              <Pie
                                data={typeDistribution.data}
                                dataKey="value"
                                nameKey="name"
                                cx="50%"
                                cy="50%"
                                innerRadius={60}
                                outerRadius={90}
                                label={(entry) => `${entry.name}: ${entry.value} (${Math.round((entry.value / typeDistribution.total) * 100)}%)`}
                                labelLine={{ stroke: '#666', strokeWidth: 1 }}
                              >
                                {typeDistribution.data.map((entry, index) => (
                                  <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                                ))}
                              </Pie>
                              <Tooltip formatter={(value, name) => [`${value} columns (${Math.round((value / typeDistribution.total) * 100)}%)`, name]} />
                            </PieChart>
                          </ResponsiveContainer>
                          <div className="absolute top-1/2 left-1/2 transform -translate-x-1/2 -translate-y-1/2 text-center">
                            <p className="text-3xl font-bold text-gray-900">{typeDistribution.total}</p>
                            <p className="text-xs text-gray-600">Total Columns</p>
                          </div>
                        </div>
                      );
                    })()}
                  </div>

                  {/* Null Percentage by Column */}
                  <div>
                    <h4 className="text-sm font-semibold text-gray-700 mb-3">Top Columns with Missing Values</h4>
                    {getNullPercentageData(tableDetails.schema).length === 0 ? (
                      <div className="flex items-center justify-center h-[200px] text-gray-500">
                        <div className="text-center">
                          <CheckCircle size={32} className="mx-auto mb-2 text-green-500" />
                          <p className="text-sm">No missing values detected</p>
                        </div>
                      </div>
                    ) : (
                      <ResponsiveContainer width="100%" height={200}>
                        <BarChart data={getNullPercentageData(tableDetails.schema)} layout="horizontal">
                          <XAxis type="number" domain={[0, 100]} />
                          <YAxis type="category" dataKey="column" width={80} tick={{fontSize: 11}} />
                          <Tooltip />
                          <Bar dataKey="percentage" fill="#ef4444" />
                        </BarChart>
                      </ResponsiveContainer>
                    )}
                  </div>
                </div>
              </div>

              {/* EDA INSIGHTS: Distribution Analysis */}
              {tableDetails.eda && tableDetails.eda.numeric_distributions && Object.keys(tableDetails.eda.numeric_distributions).length > 0 && (
                <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
                  <div className="flex items-center gap-2 mb-4">
                    <TrendingUp className="w-5 h-5 text-blue-600" />
                    <h3 className="text-lg font-semibold text-gray-900">Distribution Analysis</h3>
                    <span className="text-xs text-gray-500 ml-auto">SECTION: Numeric Patterns</span>
                  </div>
                  
                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {Object.entries(tableDetails.eda.numeric_distributions).map(([col, stats]) => (
                      <div key={col} className="border border-gray-200 rounded-lg p-4 bg-gray-50">
                        <div className="flex items-center justify-between mb-3">
                          <h4 className="font-mono text-sm font-semibold text-gray-900">{col}</h4>
                          <span className={`text-xs px-2 py-1 rounded ${
                            stats.skew === 'symmetric' ? 'bg-green-100 text-green-800' :
                            stats.skew === 'left' || stats.skew === 'right' ? 'bg-yellow-100 text-yellow-800' :
                            'bg-gray-100 text-gray-800'
                          }`}>
                            {stats.skew}
                          </span>
                        </div>
                        
                        {/* Five-number summary */}
                        <div className="grid grid-cols-5 gap-1 text-xs mb-3">
                          <div className="text-center">
                            <p className="text-gray-500">Min</p>
                            <p className="font-semibold">{stats.min.toLocaleString()}</p>
                          </div>
                          <div className="text-center">
                            <p className="text-gray-500">Q1</p>
                            <p className="font-semibold">{stats.q25.toLocaleString()}</p>
                          </div>
                          <div className="text-center">
                            <p className="text-gray-500">Median</p>
                            <p className="font-semibold">{stats.median.toLocaleString()}</p>
                          </div>
                          <div className="text-center">
                            <p className="text-gray-500">Q3</p>
                            <p className="font-semibold">{stats.q75.toLocaleString()}</p>
                          </div>
                          <div className="text-center">
                            <p className="text-gray-500">Max</p>
                            <p className="font-semibold">{stats.max.toLocaleString()}</p>
                          </div>
                        </div>
                        
                        {/* Histogram */}
                        {stats.histogram && (
                          <ResponsiveContainer width="100%" height={120}>
                            <BarChart data={stats.histogram.counts.map((count, idx) => ({
                              range: `${stats.histogram.bin_edges[idx]}-${stats.histogram.bin_edges[idx+1]}`,
                              count
                            }))}>
                              <XAxis dataKey="range" tick={false} />
                              <YAxis />
                              <Tooltip />
                              <Bar dataKey="count" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                            </BarChart>
                          </ResponsiveContainer>
                        )}
                        
                        <div className="flex items-center justify-between text-xs text-gray-600 mt-2">
                          <span>Mean: {stats.mean.toLocaleString()}</span>
                          <span>σ: {stats.std.toLocaleString()}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* CATEGORICAL DATA: Frequency Breakdown */}
              {tableDetails.eda && tableDetails.eda.categorical_frequencies && Object.keys(tableDetails.eda.categorical_frequencies).length > 0 && (
                <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
                  <div className="flex items-center gap-2 mb-4">
                    <BarChart3 className="w-5 h-5 text-purple-600" />
                    <h3 className="text-lg font-semibold text-gray-900">Categorical Analysis</h3>
                    <span className="text-xs text-gray-500 ml-auto">SECTION: Value Frequencies</span>
                  </div>
                  
                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {Object.entries(tableDetails.eda.categorical_frequencies).map(([col, freq]) => (
                      <div key={col} className="border border-gray-200 rounded-lg p-4 bg-gray-50">
                        <div className="flex items-center justify-between mb-3">
                          <h4 className="font-mono text-sm font-semibold text-gray-900">{col}</h4>
                          <span className="text-xs text-gray-600">{freq.unique_count} unique values</span>
                        </div>
                        
                        {freq.distribution ? (
                          <div className="space-y-2">
                            {freq.distribution.map((item, idx) => (
                              <div key={idx}>
                                <div className="flex items-center justify-between text-xs mb-1">
                                  <span className="font-medium text-gray-700 truncate max-w-[150px]">{item.value}</span>
                                  <span className="text-gray-600">{item.count.toLocaleString()} ({item.percentage}%)</span>
                                </div>
                                <div className="w-full bg-gray-200 rounded-full h-2">
                                  <div 
                                    className="bg-purple-600 h-2 rounded-full transition-all"
                                    style={{width: `${item.percentage}%`}}
                                  />
                                </div>
                              </div>
                            ))}
                          </div>
                        ) : (
                          <p className="text-xs text-gray-500">High cardinality - {freq.cardinality}</p>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* DATA QUALITY: Semantic Warnings & Flags */}
              {tableDetails.eda && tableDetails.eda.semantic_warnings && tableDetails.eda.semantic_warnings.length > 0 && (
                <div className="bg-amber-50 border border-amber-200 rounded-xl p-6">
                  <div className="flex items-center gap-2 mb-4">
                    <AlertCircle className="w-5 h-5 text-amber-600" />
                    <h3 className="text-lg font-semibold text-gray-900">Semantic Data Quality Warnings</h3>
                    <span className="text-xs text-amber-700 ml-auto">SECTION: Action Required Before Silver</span>
                  </div>
                  
                  <div className="space-y-3">
                    {tableDetails.eda.semantic_warnings.map((warning, idx) => (
                      <div key={idx} className={`p-4 rounded-lg border ${
                        warning.severity === 'high' ? 'bg-red-50 border-red-200' :
                        warning.severity === 'medium' ? 'bg-yellow-50 border-yellow-200' :
                        'bg-blue-50 border-blue-200'
                      }`}>
                        <div className="flex items-start gap-3">
                          <div className={`mt-0.5 px-2 py-1 rounded text-xs font-semibold ${
                            warning.severity === 'high' ? 'bg-red-100 text-red-800' :
                            warning.severity === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                            'bg-blue-100 text-blue-800'
                          }`}>
                            {warning.severity.toUpperCase()}
                          </div>
                          <div className="flex-1">
                            <p className="text-sm font-medium text-gray-900 mb-1">{warning.column}</p>
                            <p className="text-sm text-gray-700">{warning.message}</p>
                            {warning.affected_rows && (
                              <p className="text-xs text-gray-500 mt-1">Affected: {warning.affected_rows.toLocaleString()} rows</p>
                            )}
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* TEMPORAL DATA: Patterns & Freshness */}
              {tableDetails.eda && tableDetails.eda.temporal_patterns && Object.keys(tableDetails.eda.temporal_patterns).length > 0 && (
                <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
                  <div className="flex items-center gap-2 mb-4">
                    <Calendar className="w-5 h-5 text-green-600" />
                    <h3 className="text-lg font-semibold text-gray-900">Temporal Pattern Analysis</h3>
                    <span className="text-xs text-gray-500 ml-auto">SECTION: Data Freshness & Patterns</span>
                  </div>
                  
                  <div className="space-y-4">
                    {Object.entries(tableDetails.eda.temporal_patterns).map(([col, pattern]) => (
                      <div key={col} className="border border-gray-200 rounded-lg p-4 bg-gray-50">
                        <h4 className="font-mono text-sm font-semibold text-gray-900 mb-3">{col}</h4>
                        
                        <div className="grid grid-cols-4 gap-4 mb-3">
                          <div>
                            <p className="text-xs text-gray-500">Earliest</p>
                            <p className="text-sm font-semibold">{pattern.min_date ? new Date(pattern.min_date).toLocaleDateString() : 'N/A'}</p>
                          </div>
                          <div>
                            <p className="text-xs text-gray-500">Latest</p>
                            <p className="text-sm font-semibold">{pattern.max_date ? new Date(pattern.max_date).toLocaleDateString() : 'N/A'}</p>
                          </div>
                          <div>
                            <p className="text-xs text-gray-500">Range</p>
                            <p className="text-sm font-semibold">{pattern.range_days} days</p>
                          </div>
                          <div>
                            <p className="text-xs text-gray-500">Unique Dates</p>
                            <p className="text-sm font-semibold">{pattern.unique_dates.toLocaleString()}</p>
                          </div>
                        </div>
                        
                        {pattern.warning && (
                          <div className="bg-yellow-50 border border-yellow-200 rounded p-3 text-sm text-yellow-800 flex items-center gap-2">
                            <AlertCircle size={16} />
                            {pattern.warning}
                          </div>
                        )}
                        
                        {pattern.monthly_distribution && (
                          <div className="mt-3">
                            <p className="text-xs text-gray-500 mb-2">Monthly Distribution</p>
                            <ResponsiveContainer width="100%" height={100}>
                              <BarChart data={pattern.monthly_distribution}>
                                <XAxis dataKey="month" tick={{fontSize: 10}} angle={-45} textAnchor="end" height={50} />
                                <YAxis />
                                <Tooltip />
                                <Bar dataKey="count" fill="#10b981" radius={[4, 4, 0, 0]} />
                              </BarChart>
                            </ResponsiveContainer>
                          </div>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* RELATIONSHIPS: Uniqueness & Correlation Signals */}
              {tableDetails.eda && (tableDetails.eda.uniqueness_signals?.length > 0 || tableDetails.eda.correlations?.strong_correlations?.length > 0) && (
                <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
                  <div className="flex items-center gap-2 mb-4">
                    <Layers className="w-5 h-5 text-indigo-600" />
                    <h3 className="text-lg font-semibold text-gray-900">Column Relationships & Patterns</h3>
                    <span className="text-xs text-gray-500 ml-auto">SECTION: Data Relationships</span>
                  </div>
                  
                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {/* Uniqueness Signals */}
                    {tableDetails.eda.uniqueness_signals && tableDetails.eda.uniqueness_signals.length > 0 && (
                      <div>
                        <h4 className="text-sm font-semibold text-gray-700 mb-3">Uniqueness Patterns</h4>
                        <div className="space-y-2">
                          {tableDetails.eda.uniqueness_signals.map((signal, idx) => (
                            <div key={idx} className="bg-indigo-50 border border-indigo-200 rounded p-3 text-sm">
                              <p className="font-mono text-xs text-indigo-900 mb-1">{signal.column}</p>
                              <p className="text-gray-700">{signal.message}</p>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                    
                    {/* Correlations */}
                    {tableDetails.eda.correlations && tableDetails.eda.correlations.strong_correlations && tableDetails.eda.correlations.strong_correlations.length > 0 && (
                      <div>
                        <h4 className="text-sm font-semibold text-gray-700 mb-3">Strong Correlations (|r| &gt; 0.7)</h4>
                        <div className="space-y-2">
                          {tableDetails.eda.correlations.strong_correlations.map((corr, idx) => (
                            <div key={idx} className="bg-blue-50 border border-blue-200 rounded p-3 text-sm">
                              <div className="flex items-center justify-between">
                                <span className="font-mono text-xs">{corr.column1} ↔ {corr.column2}</span>
                                <span className={`px-2 py-1 rounded text-xs font-semibold ${
                                  Math.abs(corr.correlation) > 0.9 ? 'bg-red-100 text-red-800' : 'bg-blue-100 text-blue-800'
                                }`}>
                                  r = {corr.correlation}
                                </span>
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* OUTLIER DETECTION: Statistical Analysis */}
              {tableDetails.eda && tableDetails.eda.outlier_summary && Object.keys(tableDetails.eda.outlier_summary).length > 0 && (
                <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
                  <div className="flex items-center gap-2 mb-4">
                    <AlertCircle className="w-5 h-5 text-red-600" />
                    <h3 className="text-lg font-semibold text-gray-900">Outlier Detection (IQR Method)</h3>
                    <span className="text-xs text-gray-500 ml-auto">SECTION: Suspicious Values Flagged</span>
                  </div>
                  
                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                    {Object.entries(tableDetails.eda.outlier_summary).map(([col, outliers]) => (
                      <div key={col} className="border border-red-200 rounded-lg p-4 bg-red-50">
                        <div className="flex items-center justify-between mb-2">
                          <h4 className="font-mono text-sm font-semibold text-gray-900">{col}</h4>
                          <span className="text-xs px-2 py-1 bg-red-100 text-red-800 rounded font-semibold">
                            {outliers.count} outliers ({outliers.percentage}%)
                          </span>
                        </div>
                        <div className="text-xs text-gray-700 mb-2">
                          <p>Valid range: {outliers.lower_bound.toLocaleString()} to {outliers.upper_bound.toLocaleString()}</p>
                        </div>
                        <div className="text-xs text-gray-600">
                          <p className="font-semibold mb-1">Sample outliers:</p>
                          <p className="font-mono">{outliers.outlier_values.join(', ')}</p>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* SCHEMA DEFINITION: Column Metadata */}
              <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
                <div className="flex items-center justify-between mb-4">
                  <div>
                    <h3 className="text-lg font-semibold text-gray-900">Schema</h3>
                    <span className="text-xs text-gray-500">SECTION: Column Definitions & Null Analysis</span>
                  </div>
                  <div className="flex items-center gap-2 bg-gray-50 rounded-lg px-3 py-2 border border-gray-200">
                    <Search size={16} className="text-gray-400" />
                    <input
                      type="text"
                      placeholder="Search columns..."
                      value={schemaSearch}
                      onChange={(e) => setSchemaSearch(e.target.value)}
                      className="bg-transparent border-none outline-none text-sm text-gray-900 placeholder-gray-400 w-48"
                    />
                    {schemaSearch && (
                      <button onClick={() => setSchemaSearch('')} className="text-gray-400 hover:text-gray-600">
                        <X size={16} />
                      </button>
                    )}
                  </div>
                </div>
                <div className="border border-gray-200 rounded-lg overflow-hidden">
                  <div className="overflow-x-auto max-h-64 overflow-y-auto">
                    <table className="min-w-full divide-y divide-gray-200 text-sm">
                      <thead className="bg-gray-50 sticky top-0">
                        <tr>
                          <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Column</th>
                          <th className="px-4 py-3 text-left text-xs font-semibold text-gray-600 uppercase">Type</th>
                          <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase">Nulls</th>
                          <th className="px-4 py-3 text-right text-xs font-semibold text-gray-600 uppercase">Null %</th>
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {getFilteredSchema(tableDetails.schema).map((col, idx) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            <td className="px-4 py-3 text-gray-900 font-mono text-xs">{col.name}</td>
                            <td className="px-4 py-3 text-gray-600 text-xs">{col.dtype}</td>
                            <td className="px-4 py-3 text-gray-600 text-xs text-right">{col.null_count}</td>
                            <td className="px-4 py-3 text-xs text-right">
                              <span className={`px-2 py-1 rounded-full ${
                                col.null_percentage > 50 ? 'bg-red-100 text-red-800' :
                                col.null_percentage > 20 ? 'bg-yellow-100 text-yellow-800' :
                                'bg-green-100 text-green-800'
                              }`}>
                                {col.null_percentage?.toFixed(1)}%
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>

              {/* RAW DATA PREVIEW: Sample Rows */}
              <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
                <div className="flex items-center justify-between mb-4">
                  <div>
                    <h3 className="text-lg font-semibold text-gray-900">Sample Data</h3>
                    <p className="text-sm text-gray-600 mt-1">
                      SECTION: Raw Data Preview | Showing rows {showingFrom}-{showingTo} of {filteredRows.toLocaleString()}
                      {filteredRows !== totalRows && (
                        <span className="text-orange-600"> (filtered from {totalRows.toLocaleString()} total)</span>
                      )}
                    </p>
                  </div>
                  <div className="flex items-center gap-2 bg-gray-50 rounded-lg px-3 py-2 border border-gray-200">
                    <Search size={16} className="text-gray-400" />
                    <input
                      type="text"
                      placeholder="Search data..."
                      value={sampleSearch}
                      onChange={(e) => handleSampleSearchChange(e.target.value)}
                      className="bg-transparent border-none outline-none text-sm text-gray-900 placeholder-gray-400 w-48"
                    />
                    {sampleSearch && (
                      <button onClick={() => handleSampleSearchChange('')} className="text-gray-400 hover:text-gray-600">
                        <X size={16} />
                      </button>
                    )}
                  </div>
                </div>
                <div className="border border-gray-200 rounded-lg overflow-hidden">
                  <div className="overflow-x-auto max-h-80 overflow-y-auto">
                    <table className="min-w-full divide-y divide-gray-200 text-xs">
                      <thead className="bg-gray-50 sticky top-0">
                        <tr>
                          {tableDetails.sample_data?.[0] && Object.keys(tableDetails.sample_data[0]).map(key => (
                            <th key={key} className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase whitespace-nowrap">
                              {key}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="bg-white divide-y divide-gray-200">
                        {tableDetails.sample_data.map((row, idx) => (
                          <tr key={idx} className="hover:bg-gray-50">
                            {Object.values(row).map((value, vidx) => (
                              <td key={vidx} className="px-3 py-2 text-gray-900 whitespace-nowrap font-mono text-xs">
                                {value === null ? (
                                  <span className="text-gray-400 italic">null</span>
                                ) : typeof value === 'object' ? (
                                  JSON.stringify(value)
                                ) : (
                                  String(value).length > 50 ? String(value).substring(0, 50) + '...' : String(value)
                                )}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
                
                {/* Pagination Controls */}
                {totalPages > 0 && (
                  <div className="mt-4 flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <label className="text-sm text-gray-600">Rows per page:</label>
                      <select
                        value={pageSize}
                        onChange={(e) => handlePageSizeChange(Number(e.target.value))}
                        className="border border-gray-300 rounded px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-orange-500"
                      >
                        <option value={50}>50</option>
                        <option value={100}>100</option>
                        <option value={200}>200</option>
                        <option value={500}>500</option>
                      </select>
                    </div>
                    
                    <div className="flex items-center gap-2">
                      <button
                        onClick={() => handlePageChange(1)}
                        disabled={currentPage <= 1}
                        className="px-3 py-1.5 border border-gray-300 rounded text-sm font-medium disabled:opacity-40 disabled:cursor-not-allowed hover:enabled:bg-gray-50 hover:enabled:border-gray-400 transition-colors"
                      >
                        First
                      </button>
                      <button
                        onClick={() => handlePageChange(currentPage - 1)}
                        disabled={currentPage <= 1}
                        className="px-3 py-1.5 border border-gray-300 rounded text-sm font-medium disabled:opacity-40 disabled:cursor-not-allowed hover:enabled:bg-gray-50 hover:enabled:border-gray-400 transition-colors"
                      >
                        ◀ Previous
                      </button>
                      <span className="px-4 py-1.5 text-sm text-gray-700 font-medium">
                        Page {currentPage} of {totalPages}
                      </span>
                      <button
                        onClick={() => handlePageChange(currentPage + 1)}
                        disabled={currentPage >= totalPages}
                        className="px-3 py-1.5 border border-gray-300 rounded text-sm font-medium disabled:opacity-40 disabled:cursor-not-allowed hover:enabled:bg-gray-50 hover:enabled:border-gray-400 transition-colors"
                      >
                        Next ▶
                      </button>
                      <button
                        onClick={() => handlePageChange(totalPages)}
                        disabled={currentPage >= totalPages}
                        className="px-3 py-1.5 border border-gray-300 rounded text-sm font-medium disabled:opacity-40 disabled:cursor-not-allowed hover:enabled:bg-gray-50 hover:enabled:border-gray-400 transition-colors"
                      >
                        Last
                      </button>
                    </div>
                  </div>
                )}
              </div>
            </>
            )}
          </div>
          )}
        </div>
      )}
    </div>
  )
}



// ============================================
// FILE VIEWER COMPONENTS
// ============================================

function ImageViewer({ metadata, domain, tableName }) {
  const [zoom, setZoom] = useState(1)
  const [loading, setLoading] = useState(false)

  const handleDownload = async () => {
    try {
      setLoading(true)
      const response = await axios.get(`${API_BASE}/bronze/download/${domain}/${tableName}`)
      window.open(response.data.download_url, '_blank')
    } catch (err) {
      console.error('Download error:', err)
    } finally {
      setLoading(false)
    }
  }

  const formatBytes = (bytes) => {
    if (!bytes) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i]
  }

  return (
    <div className="space-y-4">
      {/* FILE METADATA: Image Information */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <div className="flex items-center gap-3 mb-4">
              <div className="w-12 h-12 bg-purple-100 rounded-lg flex items-center justify-center">
                <ImageIcon className="w-6 h-6 text-purple-600" />
              </div>
              <div>
                <h3 className="text-lg font-semibold text-gray-900">Image File</h3>
                <span className="text-xs text-gray-500">SECTION: File Information</span>
                <p className="text-sm text-gray-600">{metadata.filename}</p>
              </div>
            </div>
            
            <div className="grid grid-cols-2 gap-4">
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">Format</p>
                <p className="text-sm font-semibold text-gray-900">{metadata.format || 'Unknown'}</p>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">File Size</p>
                <p className="text-sm font-semibold text-gray-900">{formatBytes(metadata.size_bytes)}</p>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">Dimensions</p>
                <p className="text-sm font-semibold text-gray-900">{metadata.width} × {metadata.height} px</p>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">Color Mode</p>
                <p className="text-sm font-semibold text-gray-900">{metadata.mode || 'RGB'}</p>
              </div>
            </div>
          </div>
          
          <button
            onClick={handleDownload}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors disabled:opacity-50"
          >
            <Download size={16} />
            Download
          </button>
        </div>
      </div>

      {/* VISUAL PREVIEW: Image Display */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h3 className="text-lg font-semibold text-gray-900">Preview</h3>
            <span className="text-xs text-gray-500">SECTION: Image Viewer</span>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setZoom(z => Math.max(0.25, z - 0.25))}
              className="p-2 border border-gray-300 rounded hover:bg-gray-50 transition-colors"
            >
              <ZoomOut size={16} />
            </button>
            <span className="text-sm text-gray-600 min-w-[60px] text-center">{Math.round(zoom * 100)}%</span>
            <button
              onClick={() => setZoom(z => Math.min(3, z + 0.25))}
              className="p-2 border border-gray-300 rounded hover:bg-gray-50 transition-colors"
            >
              <ZoomIn size={16} />
            </button>
          </div>
        </div>
        
        <div className="bg-gray-50 rounded-lg overflow-auto max-h-[600px] flex items-center justify-center p-4">
          <img
            src={`${API_BASE}/bronze/preview/${domain}/${tableName}`}
            alt={metadata.filename}
            style={{ transform: `scale(${zoom})`, transformOrigin: 'center' }}
            className="transition-transform duration-200 max-w-full"
          />
        </div>
      </div>
    </div>
  )
}

function PDFViewer({ metadata, domain, tableName }) {
  const [loading, setLoading] = useState(false)

  const handleDownload = async () => {
    try {
      setLoading(true)
      const response = await axios.get(`${API_BASE}/bronze/download/${domain}/${tableName}`)
      window.open(response.data.download_url, '_blank')
    } catch (err) {
      console.error('Download error:', err)
    } finally {
      setLoading(false)
    }
  }

  const formatBytes = (bytes) => {
    if (!bytes) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i]
  }

  return (
    <div className="space-y-4">
      {/* FILE METADATA: PDF Information */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <div className="flex items-center gap-3 mb-4">
              <div className="w-12 h-12 bg-red-100 rounded-lg flex items-center justify-center">
                <FileText className="w-6 h-6 text-red-600" />
              </div>
              <div>
                <h3 className="text-lg font-semibold text-gray-900">PDF Document</h3>
                <span className="text-xs text-gray-500">SECTION: Document Information</span>
                <p className="text-sm text-gray-600">{metadata.filename}</p>
              </div>
            </div>
            
            <div className="grid grid-cols-3 gap-4">
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">Pages</p>
                <p className="text-sm font-semibold text-gray-900">{metadata.page_count || 'Unknown'}</p>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">File Size</p>
                <p className="text-sm font-semibold text-gray-900">{formatBytes(metadata.size_bytes)}</p>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">Format</p>
                <p className="text-sm font-semibold text-gray-900">PDF</p>
              </div>
            </div>
          </div>
          
          <button
            onClick={handleDownload}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors disabled:opacity-50"
          >
            <Download size={16} />
            Download
          </button>
        </div>
      </div>

      {/* DOCUMENT PREVIEW: PDF Display */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="mb-4">
          <h3 className="text-lg font-semibold text-gray-900">Preview</h3>
          <span className="text-xs text-gray-500">SECTION: PDF Viewer</span>
        </div>
        <div className="bg-gray-50 rounded-lg overflow-hidden">
          <iframe
            src={`${API_BASE}/bronze/preview/${domain}/${tableName}`}
            className="w-full h-[600px] border-0"
            title={metadata.filename}
          />
        </div>
      </div>
    </div>
  )
}

function JSONViewer({ metadata, domain, tableName }) {
  const [loading, setLoading] = useState(false)

  const handleDownload = async () => {
    try {
      setLoading(true)
      const response = await axios.get(`${API_BASE}/bronze/download/${domain}/${tableName}`)
      window.open(response.data.download_url, '_blank')
    } catch (err) {
      console.error('Download error:', err)
    } finally {
      setLoading(false)
    }
  }

  const formatBytes = (bytes) => {
    if (!bytes) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i]
  }

  return (
    <div className="space-y-4">
      {/* FILE METADATA: JSON Information */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <div className="flex items-center gap-3 mb-4">
              <div className="w-12 h-12 bg-green-100 rounded-lg flex items-center justify-center">
                <FileJson className="w-6 h-6 text-green-600" />
              </div>
              <div>
                <h3 className="text-lg font-semibold text-gray-900">JSON Data</h3>
                <span className="text-xs text-gray-500">SECTION: File Information</span>
                <p className="text-sm text-gray-600">{metadata.filename}</p>
              </div>
            </div>
            
            <div className="grid grid-cols-3 gap-4">
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">Valid JSON</p>
                <p className="text-sm font-semibold text-gray-900">
                  {metadata.valid_json ? 'Yes' : 'No'}
                </p>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">File Size</p>
                <p className="text-sm font-semibold text-gray-900">{formatBytes(metadata.size_bytes)}</p>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">Content Type</p>
                <p className="text-sm font-semibold text-gray-900">{metadata.content_type || 'Object'}</p>
              </div>
            </div>
          </div>
          
          <button
            onClick={handleDownload}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors disabled:opacity-50"
          >
            <Download size={16} />
            Download
          </button>
        </div>
      </div>

      {/* JSON CONTENT: Data Preview */}
      {metadata.content_preview && (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
          <div className="mb-4">
            <h3 className="text-lg font-semibold text-gray-900">Preview</h3>
            <span className="text-xs text-gray-500">SECTION: JSON Viewer</span>
          </div>
          <div className="bg-gray-900 rounded-lg p-4 overflow-auto max-h-[600px]">
            <pre className="text-sm text-green-400 font-mono">
              {metadata.content_preview}
            </pre>
          </div>
        </div>
      )}
    </div>
  )
}

function GenericFileViewer({ metadata, domain, tableName }) {
  const [loading, setLoading] = useState(false)

  const handleDownload = async () => {
    try {
      setLoading(true)
      const response = await axios.get(`${API_BASE}/bronze/download/${domain}/${tableName}`)
      window.open(response.data.download_url, '_blank')
    } catch (err) {
      console.error('Download error:', err)
    } finally {
      setLoading(false)
    }
  }

  const formatBytes = (bytes) => {
    if (!bytes) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i]
  }

  return (
    <div className="space-y-4">
      {/* FILE METADATA: Generic File Information */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <div className="flex items-center gap-3 mb-4">
              <div className="w-12 h-12 bg-gray-100 rounded-lg flex items-center justify-center">
                <FilePlus className="w-6 h-6 text-gray-600" />
              </div>
              <div>
                <h3 className="text-lg font-semibold text-gray-900">File</h3>
                <span className="text-xs text-gray-500">SECTION: File Information</span>
                <p className="text-sm text-gray-600">{metadata.filename}</p>
              </div>
            </div>
            
            <div className="grid grid-cols-2 gap-4">
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">MIME Type</p>
                <p className="text-sm font-semibold text-gray-900">{metadata.mime_type}</p>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">File Size</p>
                <p className="text-sm font-semibold text-gray-900">{formatBytes(metadata.size_bytes)}</p>
              </div>
            </div>
          </div>
          
          <button
            onClick={handleDownload}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors disabled:opacity-50"
          >
            <Download size={16} />
            Download
          </button>
        </div>
      </div>

      {/* NO PREVIEW: Download Required */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-12 text-center">
        <FilePlus size={48} className="mx-auto mb-4 text-gray-400" />
        <span className="text-xs text-gray-500">SECTION: File Download</span>
        <h3 className="text-lg font-semibold text-gray-900 mb-2 mt-2">Preview Not Available</h3>
        <p className="text-gray-600 mb-4">This file type doesn't support preview. Click the download button above to save it locally.</p>
      </div>
    </div>
  )
}

function VideoViewer({ metadata, domain, tableName }) {
  const [loading, setLoading] = useState(false)

  const handleDownload = async () => {
    try {
      setLoading(true)
      const response = await axios.get(`${API_BASE}/bronze/download/${domain}/${tableName}`)
      window.open(response.data.download_url, '_blank')
    } catch (err) {
      console.error('Download error:', err)
    } finally {
      setLoading(false)
    }
  }

  const formatBytes = (bytes) => {
    if (!bytes) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i]
  }

  return (
    <div className="space-y-4">
      {/* FILE METADATA: Video Information */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <div className="flex items-center gap-3 mb-4">
              <div className="w-12 h-12 bg-blue-100 rounded-lg flex items-center justify-center">
                <Video className="w-6 h-6 text-blue-600" />
              </div>
              <div>
                <h3 className="text-lg font-semibold text-gray-900">Video File</h3>
                <span className="text-xs text-gray-500">SECTION: Video Information</span>
                <p className="text-sm text-gray-600">{metadata.filename}</p>
              </div>
            </div>
            
            <div className="grid grid-cols-2 gap-4">
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">Format</p>
                <p className="text-sm font-semibold text-gray-900">{metadata.video_format || 'MP4'}</p>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">File Size</p>
                <p className="text-sm font-semibold text-gray-900">{formatBytes(metadata.size_bytes)}</p>
              </div>
            </div>
          </div>
          
          <button
            onClick={handleDownload}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors disabled:opacity-50"
          >
            <Download size={16} />
            Download
          </button>
        </div>
      </div>

      {/* VIDEO PLAYER: Media Preview */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="mb-4">
          <h3 className="text-lg font-semibold text-gray-900">Preview</h3>
          <span className="text-xs text-gray-500">SECTION: Video Player</span>
        </div>
        <div className="bg-black rounded-lg overflow-hidden">
          <video
            controls
            className="w-full max-h-[600px]"
            src={`${API_BASE}/bronze/preview/${domain}/${tableName}`}
          >
            Your browser does not support the video tag.
          </video>
        </div>
      </div>
    </div>
  )
}

function TextViewer({ metadata, domain, tableName }) {
  const [loading, setLoading] = useState(false)

  const handleDownload = async () => {
    try {
      setLoading(true)
      const response = await axios.get(`${API_BASE}/bronze/download/${domain}/${tableName}`)
      window.open(response.data.download_url, '_blank')
    } catch (err) {
      console.error('Download error:', err)
    } finally {
      setLoading(false)
    }
  }

  const formatBytes = (bytes) => {
    if (!bytes) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i]
  }

  return (
    <div className="space-y-4">
      {/* FILE METADATA: Text File Information */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <div className="flex items-center gap-3 mb-4">
              <div className="w-12 h-12 bg-indigo-100 rounded-lg flex items-center justify-center">
                <FileType className="w-6 h-6 text-indigo-600" />
              </div>
              <div>
                <h3 className="text-lg font-semibold text-gray-900">Text File</h3>
                <span className="text-xs text-gray-500">SECTION: File Information</span>
                <p className="text-sm text-gray-600">{metadata.filename}</p>
              </div>
            </div>
            
            <div className="grid grid-cols-3 gap-4">
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">Lines</p>
                <p className="text-sm font-semibold text-gray-900">{metadata.line_count?.toLocaleString() || 'N/A'}</p>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">Characters</p>
                <p className="text-sm font-semibold text-gray-900">{metadata.char_count?.toLocaleString() || 'N/A'}</p>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs text-gray-600 mb-1">File Size</p>
                <p className="text-sm font-semibold text-gray-900">{formatBytes(metadata.size_bytes)}</p>
              </div>
            </div>
          </div>
          
          <button
            onClick={handleDownload}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors disabled:opacity-50"
          >
            <Download size={16} />
            Download
          </button>
        </div>
      </div>

      {/* TEXT CONTENT: Document Preview */}
      {metadata.content_preview && (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
          <div className="mb-4">
            <h3 className="text-lg font-semibold text-gray-900">Preview</h3>
            <span className="text-xs text-gray-500">SECTION: Text Viewer</span>
          </div>
          <div className="bg-gray-50 rounded-lg p-4 overflow-auto max-h-[600px]">
            <pre className="text-sm text-gray-800 font-mono whitespace-pre-wrap">
              {metadata.content_preview}
            </pre>
          </div>
        </div>
      )}
    </div>
  )
}

export default Bronze