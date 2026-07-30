import { useState, useEffect, useCallback } from 'react'
import { 
  Image, Video, Music, FileText, Folder, 
  TrendingUp, AlertCircle, CheckCircle, Clock,
  Database, HardDrive, Layers, RefreshCw
} from 'lucide-react'

const API_BASE = 'http://localhost:8000'

export default function MediaDashboard() {
  const [timeRange, setTimeRange] = useState('7d')
  const [recentJobs, setRecentJobs] = useState([])
  const [sourceStats, setSourceStats] = useState(null)
  const [loading, setLoading] = useState(false)
  const [lastRefresh, setLastRefresh] = useState(null)
  const [liveHealth, setLiveHealth] = useState([])
  const [hasFetched, setHasFetched] = useState(false)

  const fetchJobs = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/silver/unstructured/jobs?limit=10`)
      if (res.ok) {
        const data = await res.json()
        setRecentJobs(data.jobs || [])
      }
    } catch (err) {
      console.warn('Could not fetch unstructured jobs:', err)
    }
  }, [])

  const fetchStats = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/silver/unstructured/stats`)
      if (res.ok) {
        const data = await res.json()
        setSourceStats(data.sources || null)
      }
    } catch (err) {
      console.warn('Could not fetch source stats:', err)
    }
  }, [])

  const fetchHealth = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/silver/unstructured/health`)
      if (res.ok) {
        const data = await res.json()
        setLiveHealth(data.services || [])
      }
    } catch (err) { console.warn('Could not fetch health:', err) }
  }, [])

  const refresh = useCallback(async () => {
    setLoading(true)
    await Promise.all([fetchJobs(), fetchStats(), fetchHealth()])
    setLastRefresh(new Date().toLocaleTimeString())
    setHasFetched(true)
    setLoading(false)
  }, [fetchJobs, fetchStats, fetchHealth])

  useEffect(() => {
    refresh()
    const interval = setInterval(fetchJobs, 10000) // poll jobs every 10s
    return () => clearInterval(interval)
  }, [refresh, fetchJobs])

  // Derive media counts from sourceStats if available, else fallback to placeholders
  const mediaStats = [
    { label: 'Total Files', value: sourceStats?.s3?.total_objects?.toLocaleString() ?? '—', icon: Folder, color: 'blue', change: '' },
    { label: 'Images', value: sourceStats?.s3?.by_type?.image?.count?.toLocaleString() ?? '—', icon: Image, color: 'green', change: '' },
    { label: 'Videos', value: sourceStats?.s3?.by_type?.video?.count?.toLocaleString() ?? '—', icon: Video, color: 'purple', change: '' },
    { label: 'Audio Files', value: sourceStats?.s3?.by_type?.audio?.count?.toLocaleString() ?? '—', icon: Music, color: 'pink', change: '' },
    { label: 'Documents', value: sourceStats?.s3?.by_type?.document?.count?.toLocaleString() ?? '—', icon: FileText, color: 'orange', change: '' },
    {
      label: 'Storage Used',
      value: sourceStats?.s3?.total_size_gb != null
        ? `${sourceStats.s3.total_size_gb.toFixed(1)} GB`
        : '—',
      icon: HardDrive, color: 'red', change: ''
    }
  ]

  // Derive quality issues from failed/running jobs
  const qualityIssues = recentJobs
    .filter(j => j.status === 'failed' || j.status === 'running')
    .map(job => ({
      id: job.job_id || job.id,
      severity: job.status === 'failed' ? 'high' : 'low',
      type: `${job.cleaning_summary?.media_type || job.entity || 'media'} Processing`,
      message: job.message || (job.status === 'failed' ? 'Processing job failed' : 'Job running'),
      count: job.row_count || 0,
      dataset: job.table_name || job.entity || 'media',
    }))

  const systemHealth = liveHealth

  const getStatColor = (color) => {
    const colors = {
      blue: 'bg-blue-500',
      green: 'bg-green-500',
      purple: 'bg-purple-500',
      pink: 'bg-pink-500',
      orange: 'bg-orange-500',
      red: 'bg-red-500'
    }
    return colors[color] || 'bg-blue-500'
  }

  const getStatusColor = (status) => {
    switch(status) {
      case 'completed': return 'text-green-600 bg-green-50'
      case 'running': return 'text-blue-600 bg-blue-50'
      case 'queued': return 'text-gray-600 bg-gray-50'
      case 'failed': return 'text-red-600 bg-red-50'
      default: return 'text-gray-600 bg-gray-50'
    }
  }

  const getSeverityColor = (severity) => {
    switch(severity) {
      case 'high': return 'text-red-600 bg-red-50 border-red-200'
      case 'medium': return 'text-orange-600 bg-orange-50 border-orange-200'
      case 'low': return 'text-yellow-600 bg-yellow-50 border-yellow-200'
      default: return 'text-gray-600 bg-gray-50 border-gray-200'
    }
  }

  return (
    <div className="h-full overflow-auto bg-gray-50">
      <div className="p-6 space-y-6">
        
        {/* Header */}
        <div className="flex justify-between items-start mb-6">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Media Processing Dashboard</h1>
            <p className="text-gray-600 mt-1">Unstructured data pipeline monitoring and analytics</p>
            {lastRefresh && <p className="text-xs text-gray-400 mt-0.5">Last refreshed: {lastRefresh}</p>}
          </div>
          <div className="flex gap-2">
            <button onClick={refresh} disabled={loading} className="px-3 py-2 rounded-lg bg-white border border-gray-200 text-gray-700 hover:bg-gray-50 flex items-center gap-1">
              <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
            <button onClick={() => setTimeRange('24h')} className={`px-4 py-2 rounded-lg ${timeRange === '24h' ? 'bg-blue-600 text-white' : 'bg-white text-gray-700'}`}>24h</button>
            <button onClick={() => setTimeRange('7d')} className={`px-4 py-2 rounded-lg ${timeRange === '7d' ? 'bg-blue-600 text-white' : 'bg-white text-gray-700'}`}>7d</button>
            <button onClick={() => setTimeRange('30d')} className={`px-4 py-2 rounded-lg ${timeRange === '30d' ? 'bg-blue-600 text-white' : 'bg-white text-gray-700'}`}>30d</button>
          </div>
        </div>

        {/* Media Stats Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {mediaStats.map((stat, idx) => {
            const Icon = stat.icon
            return (
              <div key={idx} className="bg-white rounded-lg shadow p-6">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-4">
                    <div className={`${getStatColor(stat.color)} bg-opacity-10 p-3 rounded-lg`}>
                      <Icon className={`w-6 h-6 ${getStatColor(stat.color).replace('bg-', 'text-')}`} />
                    </div>
                    <div>
                      <p className="text-sm text-gray-600">{stat.label}</p>
                      <p className="text-2xl font-bold text-gray-900">{stat.value}</p>
                    </div>
                  </div>
                  <div className="text-right">
                    <span className={`text-sm font-medium ${stat.change.startsWith('+') ? 'text-green-600' : 'text-red-600'}`}>
                      {stat.change}
                    </span>
                  </div>
                </div>
              </div>
            )
          })}
        </div>

        {/* Recent Processing Jobs */}
        <div className="bg-white rounded-lg shadow">
          <div className="p-6 border-b border-gray-200">
            <div className="flex items-center justify-between">
              <h2 className="text-xl font-bold text-gray-900">Recent Processing Jobs</h2>
              <button className="text-blue-600 hover:text-blue-700 text-sm font-medium">View All</button>
            </div>
          </div>
          <div className="divide-y divide-gray-200">
            {recentJobs.length === 0 ? (
              <div className="p-6 text-center text-gray-500">
                <p className="font-medium mb-1">No unstructured processing jobs yet.</p>
                <p className="text-sm">Go to <span className="text-blue-600 font-medium">Feature Pipeline</span> tab → select a media type → click <span className="font-semibold">Run Pipeline</span> to start.</p>
              </div>
            ) : recentJobs.map(job => (
              <div key={job.job_id || job.id} className="p-6 hover:bg-gray-50">
                <div className="flex items-center justify-between mb-2">
                  <div className="flex-1">
                    <div className="flex items-center gap-3 mb-1">
                      <h3 className="font-semibold text-gray-900">{job.table_name || job.name}</h3>
                      <span className={`px-3 py-1 rounded-full text-xs font-medium ${getStatusColor(job.status)}`}>
                        {job.status}
                      </span>
                    </div>
                    <div className="flex items-center gap-4 text-sm text-gray-600">
                      <span className="flex items-center gap-1">
                        <Layers className="w-4 h-4" />
                        {job.cleaning_summary?.media_type || job.entity || 'media'}
                      </span>
                      {job.row_count != null && (
                        <span className="flex items-center gap-1">
                          <Database className="w-4 h-4" />
                          {job.row_count.toLocaleString()} files
                        </span>
                      )}
                      {job.cleaning_summary?.duration_seconds != null && (
                        <span className="flex items-center gap-1">
                          <Clock className="w-4 h-4" />
                          {Math.round(job.cleaning_summary.duration_seconds)}s
                        </span>
                      )}
                      <span className="text-gray-500">{job.started_at ? new Date(job.started_at).toLocaleString() : ''}</span>
                    </div>
                  </div>
                </div>
                {job.status === 'running' && (
                  <div className="mt-3">
                    <div className="flex justify-between text-sm text-gray-600 mb-1">
                      <span>{job.message || 'Processing…'}</span>
                      <span>{job.progress || 0}%</span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-2">
                      <div
                        className="bg-blue-600 h-2 rounded-full transition-all duration-300"
                        style={{ width: `${job.progress || 0}%` }}
                      ></div>
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Quality Issues */}
          <div className="bg-white rounded-lg shadow">
            <div className="p-6 border-b border-gray-200">
              <h2 className="text-xl font-bold text-gray-900">Media Quality Issues</h2>
            </div>
            <div className="p-6 space-y-3">
              {hasFetched && qualityIssues.length === 0 && (
                <p className="text-sm text-gray-400 italic text-center py-4">No quality issues detected from recent jobs.</p>
              )}
              {qualityIssues.map(issue => (
                <div key={issue.id} className={`p-4 rounded-lg border ${getSeverityColor(issue.severity)}`}>
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <div className="flex items-center gap-2 mb-1">
                        <AlertCircle className="w-4 h-4" />
                        <span className="font-semibold text-sm">{issue.type}</span>
                        <span className="px-2 py-0.5 rounded-full text-xs font-medium bg-white">
                          {issue.count} files
                        </span>
                      </div>
                      <p className="text-sm">{issue.message}</p>
                      <p className="text-xs mt-1 opacity-75">Dataset: {issue.dataset}</p>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* System Health */}
          <div className="bg-white rounded-lg shadow">
            <div className="p-6 border-b border-gray-200">
              <h2 className="text-xl font-bold text-gray-900">System Health</h2>
            </div>
            <div className="p-6 space-y-4">
              {hasFetched && systemHealth.length === 0 && (
                <p className="text-sm text-gray-400 italic text-center py-4">No health data available.</p>
              )}
              {systemHealth.map((system, idx) => (
                <div key={idx} className="flex items-center justify-between p-4 bg-gray-50 rounded-lg">
                  <div className="flex items-center gap-3">
                    {system.status === 'healthy' ? (
                      <CheckCircle className="w-5 h-5 text-green-600" />
                    ) : (
                      <AlertCircle className="w-5 h-5 text-orange-600" />
                    )}
                    <div>
                      <p className="font-semibold text-gray-900">{system.service}</p>
                      <p className="text-sm text-gray-600">Uptime: {system.uptime}</p>
                    </div>
                  </div>
                  <span className={`px-3 py-1 rounded-full text-xs font-medium ${
                    system.status === 'healthy' ? 'bg-green-100 text-green-700' : 'bg-orange-100 text-orange-700'
                  }`}>
                    {system.status}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>

      </div>
    </div>
  )
}
