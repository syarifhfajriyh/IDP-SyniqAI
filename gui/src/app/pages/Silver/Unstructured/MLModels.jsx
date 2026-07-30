import { useState, useEffect, useCallback } from 'react'
import { 
  Brain, Play, Pause, Upload, Download, Settings,
  Zap, Target, TrendingUp, AlertCircle, CheckCircle,
  Layers, Box, Eye, Database, Activity, RefreshCw
} from 'lucide-react'

const API_BASE = 'http://localhost:8000'

export default function MLModels() {
  const [selectedModel, setSelectedModel] = useState(null)
  const [recentJobs, setRecentJobs] = useState([])
  const [loadingJobs, setLoadingJobs] = useState(false)
  const [liveModels, setLiveModels] = useState([])
  const [loadingModels, setLoadingModels] = useState(false)
  const [hasFetchedModels, setHasFetchedModels] = useState(false)

  const fetchRecentJobs = useCallback(async () => {
    setLoadingJobs(true)
    try {
      const res = await fetch(`${API_BASE}/api/silver/unstructured/jobs?limit=5`)
      if (res.ok) { const data = await res.json(); setRecentJobs(data.jobs || []) }
    } catch (err) { console.warn('Could not fetch jobs:', err) }
    finally { setLoadingJobs(false) }
  }, [])

  const fetchModels = useCallback(async () => {
    setLoadingModels(true)
    try {
      const res = await fetch(`${API_BASE}/api/silver/unstructured/models`)
      if (res.ok) {
        const data = await res.json()
        setLiveModels(data.models || [])
      }
    } catch (err) { console.warn('Could not fetch models:', err) }
    finally { setHasFetchedModels(true); setLoadingModels(false) }
  }, [])

  useEffect(() => {
    fetchModels()
    fetchRecentJobs()
  }, [fetchModels, fetchRecentJobs])

  const models = liveModels.map((m, i) => ({ ...m, id: m.id || i }))

  // Derive performance trend from recent jobs
  const performanceData = recentJobs.slice().reverse().map(job => ({
    date: job.started_at ? new Date(job.started_at).toISOString().slice(0, 10) : '—',
    accuracy: job.cleaning_summary?.quality_score ?? 0.9,
    latency: job.cleaning_summary?.duration_seconds ? Math.round(job.cleaning_summary.duration_seconds * 1000) : 45,
  }))

  // Derive recent predictions from job history
  const recentPredictions = recentJobs.slice(0, 4).map((job, i) => ({
    id: i,
    input: job.table_name || `${job.cleaning_summary?.media_type || 'batch'}_${i + 1}`,
    output: `${job.cleaning_summary?.media_type || job.entity || 'media'} → ${job.status}`,
    confidence: job.cleaning_summary?.quality_score ?? 0.9,
    time: job.started_at ? new Date(job.started_at).toLocaleTimeString() : `${i + 1}m ago`,
  }))

  const metrics = selectedModel ? [
    { label: 'Total Predictions', value: selectedModel.predictions?.toLocaleString() || 'N/A', icon: Target, color: 'blue' },
    { label: 'Accuracy', value: selectedModel.accuracy ? `${(selectedModel.accuracy * 100).toFixed(1)}%` : 'N/A', icon: CheckCircle, color: 'green' },
    { label: 'Avg Latency', value: selectedModel.latency || 'N/A', icon: Zap, color: 'yellow' },
    { label: 'Version', value: selectedModel.version, icon: Layers, color: 'purple' }
  ] : []

  const getStatusColor = (status) => {
    switch(status) {
      case 'deployed': return 'text-green-600 bg-green-50'
      case 'training': return 'text-blue-600 bg-blue-50'
      case 'testing': return 'text-yellow-600 bg-yellow-50'
      case 'stopped': return 'text-gray-600 bg-gray-50'
      default: return 'text-gray-600 bg-gray-50'
    }
  }

  const getMetricColor = (color) => {
    const colors = {
      blue: 'bg-blue-500',
      green: 'bg-green-500',
      yellow: 'bg-yellow-500',
      purple: 'bg-purple-500'
    }
    return colors[color] || 'bg-blue-500'
  }

  return (
    <div className="h-full flex bg-gray-50">
      
      {/* Left Sidebar - Model List */}
      <div className="w-80 bg-white border-r border-gray-200 flex flex-col">
        <div className="p-4 border-b border-gray-200">
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-bold text-gray-900">ML Models</h2>
            <button className="p-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700">
              <Upload className="w-4 h-4" />
            </button>
          </div>
          
          <div className="flex gap-2 text-sm">
            <button className="px-3 py-1.5 bg-blue-50 text-blue-600 rounded-lg font-medium">All</button>
            <button className="px-3 py-1.5 text-gray-600 hover:bg-gray-50 rounded-lg">Deployed</button>
            <button className="px-3 py-1.5 text-gray-600 hover:bg-gray-50 rounded-lg">Training</button>
          </div>
        </div>

        <div className="flex-1 overflow-auto p-4 space-y-3">
          {loadingModels && !hasFetchedModels ? (
            <div className="flex items-center justify-center h-32 text-gray-400 text-sm">Loading models…</div>
          ) : models.length === 0 && hasFetchedModels ? (
            <div className="flex flex-col items-center justify-center h-32 text-gray-400">
              <Brain className="w-10 h-10 mb-2 opacity-30" />
              <p className="text-xs text-center">No models found. Deploy a model to see it here.</p>
            </div>
          ) : null}
          {models.map(model => (
            <div
              key={model.id}
              onClick={() => setSelectedModel(model)}
              className={`p-3 rounded-lg border cursor-pointer transition-all ${
                selectedModel?.id === model.id
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-200 bg-white hover:border-gray-300'
              }`}
            >
              <div className="flex items-start gap-3">
                <div className="w-12 h-12 bg-gray-100 rounded flex items-center justify-center text-2xl">
                  {model.thumbnail}
                </div>
                <div className="flex-1 min-w-0">
                  <p className="font-medium text-sm text-gray-900 truncate">{model.name}</p>
                  <p className="text-xs text-gray-600 mt-0.5">{model.type}</p>
                  <div className="flex items-center gap-2 mt-1">
                    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${getStatusColor(model.status)}`}>
                      {model.status}
                    </span>
                    <span className="text-xs text-gray-500">{model.version}</span>
                  </div>
                  {model.accuracy && (
                    <p className="text-xs text-gray-600 mt-1">
                      Accuracy: {(model.accuracy * 100).toFixed(1)}%
                    </p>
                  )}
                  {model.progress && (
                    <div className="mt-2">
                      <div className="w-full bg-gray-200 rounded-full h-1.5">
                        <div 
                          className="bg-blue-600 h-1.5 rounded-full transition-all"
                          style={{ width: `${model.progress}%` }}
                        ></div>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        
        {/* Toolbar */}
        <div className="bg-white border-b border-gray-200 p-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-xl font-bold text-gray-900">ML Model Management</h1>
              <p className="text-sm text-gray-600 mt-1">
                {selectedModel ? selectedModel.name : 'Select a model to view details'}
              </p>
            </div>
            <div className="flex items-center gap-2">
              {selectedModel && (
                <>
                  {selectedModel.status === 'deployed' && (
                    <button className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 flex items-center gap-2">
                      <Pause className="w-4 h-4" />
                      Stop
                    </button>
                  )}
                  {selectedModel.status === 'stopped' && (
                    <button className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center gap-2">
                      <Play className="w-4 h-4" />
                      Deploy
                    </button>
                  )}
                  <button className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2">
                    <Zap className="w-4 h-4" />
                    Run Test
                  </button>
                  <button className="p-2 border border-gray-300 rounded-lg hover:bg-gray-50">
                    <Settings className="w-5 h-5 text-gray-600" />
                  </button>
                </>
              )}
            </div>
          </div>
        </div>

        {/* Main Content Area */}
        <div className="flex-1 overflow-auto p-6">
          {selectedModel ? (
            <div className="space-y-6">
              
              {/* Metrics Grid */}
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                {metrics.map((metric, idx) => {
                  const Icon = metric.icon
                  return (
                    <div key={idx} className="bg-white rounded-lg shadow p-6">
                      <div className="flex items-center gap-3">
                        <div className={`${getMetricColor(metric.color)} bg-opacity-10 p-3 rounded-lg`}>
                          <Icon className={`w-6 h-6 ${getMetricColor(metric.color).replace('bg-', 'text-')}`} />
                        </div>
                        <div>
                          <p className="text-sm text-gray-600">{metric.label}</p>
                          <p className="text-2xl font-bold text-gray-900">{metric.value}</p>
                        </div>
                      </div>
                    </div>
                  )
                })}
              </div>

              {/* Model Details */}
              <div className="bg-white rounded-lg shadow">
                <div className="p-6 border-b border-gray-200">
                  <h2 className="text-lg font-bold text-gray-900">Model Details</h2>
                </div>
                <div className="p-6">
                  <div className="grid grid-cols-2 md:grid-cols-3 gap-6">
                    <div>
                      <p className="text-sm text-gray-600 mb-1">Framework</p>
                      <p className="font-semibold text-gray-900">{selectedModel.framework}</p>
                    </div>
                    <div>
                      <p className="text-sm text-gray-600 mb-1">Type</p>
                      <p className="font-semibold text-gray-900">{selectedModel.type}</p>
                    </div>
                    <div>
                      <p className="text-sm text-gray-600 mb-1">Status</p>
                      <span className={`px-3 py-1 rounded-full text-sm font-medium ${getStatusColor(selectedModel.status)}`}>
                        {selectedModel.status}
                      </span>
                    </div>
                    <div>
                      <p className="text-sm text-gray-600 mb-1">Input Format</p>
                      <p className="font-semibold text-gray-900">{selectedModel.inputs}</p>
                    </div>
                    <div>
                      <p className="text-sm text-gray-600 mb-1">Output Format</p>
                      <p className="font-semibold text-gray-900">{selectedModel.outputs}</p>
                    </div>
                    {selectedModel.deployedDate && (
                      <div>
                        <p className="text-sm text-gray-600 mb-1">Deployed</p>
                        <p className="font-semibold text-gray-900">{selectedModel.deployedDate}</p>
                      </div>
                    )}
                  </div>
                </div>
              </div>

              {/* Performance Chart */}
              <div className="bg-white rounded-lg shadow">
                <div className="p-6 border-b border-gray-200">
                  <h2 className="text-lg font-bold text-gray-900">Performance Trend</h2>
                </div>
                <div className="p-6">
                  <div className="h-64 flex items-end gap-4">
                    {performanceData.map((data, idx) => (
                      <div key={idx} className="flex-1 flex flex-col items-center gap-2">
                        <div className="w-full bg-blue-500 rounded-t hover:bg-blue-600 transition-colors cursor-pointer"
                          style={{ height: `${data.accuracy * 100}%` }}
                          title={`${(data.accuracy * 100).toFixed(1)}%`}
                        ></div>
                        <p className="text-xs text-gray-600">{data.date.slice(5)}</p>
                      </div>
                    ))}
                  </div>
                  <div className="mt-4 flex justify-center gap-6 text-sm">
                    <div className="flex items-center gap-2">
                      <div className="w-3 h-3 bg-blue-500 rounded"></div>
                      <span className="text-gray-600">Accuracy</span>
                    </div>
                  </div>
                </div>
              </div>

              {/* Recent Predictions */}
              {selectedModel.status === 'deployed' && (
                <div className="bg-white rounded-lg shadow">
                  <div className="p-6 border-b border-gray-200">
                    <h2 className="text-lg font-bold text-gray-900">Recent Predictions</h2>
                  </div>
                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead className="bg-gray-50 border-b border-gray-200">
                        <tr>
                          <th className="text-left px-6 py-3 text-xs font-semibold text-gray-700">Input</th>
                          <th className="text-left px-6 py-3 text-xs font-semibold text-gray-700">Prediction</th>
                          <th className="text-left px-6 py-3 text-xs font-semibold text-gray-700">Confidence</th>
                          <th className="text-left px-6 py-3 text-xs font-semibold text-gray-700">Time</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-200">
                        {recentPredictions.map(pred => (
                          <tr key={pred.id} className="hover:bg-gray-50">
                            <td className="px-6 py-4 text-sm text-gray-900">{pred.input}</td>
                            <td className="px-6 py-4 text-sm font-medium text-gray-900">{pred.output}</td>
                            <td className="px-6 py-4">
                              <div className="flex items-center gap-2">
                                <div className="w-24 bg-gray-200 rounded-full h-2">
                                  <div 
                                    className="bg-green-600 h-2 rounded-full"
                                    style={{ width: `${pred.confidence * 100}%` }}
                                  ></div>
                                </div>
                                <span className="text-sm text-gray-600">{(pred.confidence * 100).toFixed(0)}%</span>
                              </div>
                            </td>
                            <td className="px-6 py-4 text-sm text-gray-600">{pred.time}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {/* Training Progress */}
              {selectedModel.status === 'training' && (
                <div className="bg-white rounded-lg shadow p-6">
                  <div className="flex items-center gap-4">
                    <div className="animate-spin">
                      <Brain className="w-8 h-8 text-blue-600" />
                    </div>
                    <div className="flex-1">
                      <h3 className="font-bold text-gray-900 mb-1">Training in Progress</h3>
                      <p className="text-sm text-gray-600 mb-2">Epoch 78/100 - {selectedModel.progress}% complete</p>
                      <div className="w-full bg-gray-200 rounded-full h-3">
                        <div 
                          className="bg-blue-600 h-3 rounded-full transition-all"
                          style={{ width: `${selectedModel.progress}%` }}
                        ></div>
                      </div>
                    </div>
                  </div>
                </div>
              )}

            </div>
          ) : (
            <div className="h-full flex items-center justify-center">
              <div className="text-center text-gray-500">
                <Brain className="w-16 h-16 mx-auto mb-4 opacity-50" />
                <p className="text-lg font-medium">No model selected</p>
                <p className="text-sm mt-1">Choose a model to view details and metrics</p>
              </div>
            </div>
          )}
        </div>

      </div>

      {/* Recent Pipeline Jobs - Live from API */}
      {recentJobs.length > 0 && (
        <div className="fixed bottom-4 right-4 w-80 bg-white rounded-lg shadow-lg border border-gray-200 z-10">
          <div className="p-3 border-b border-gray-200 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Activity className="w-4 h-4 text-blue-600" />
              <span className="font-semibold text-sm text-gray-900">Live Pipeline Jobs</span>
            </div>
            <button onClick={fetchRecentJobs} disabled={loadingJobs} className="p-1 text-gray-500 hover:text-gray-700 rounded disabled:opacity-50">
              <RefreshCw className={`w-3.5 h-3.5 ${loadingJobs ? 'animate-spin' : ''}`} />
            </button>
          </div>
          <div className="divide-y divide-gray-100 max-h-48 overflow-y-auto">
            {recentJobs.map(job => (
              <div key={job.job_id} className="p-3 flex items-center justify-between text-xs">
                <div>
                  <p className="font-medium text-gray-900 truncate w-44">{job.job_id}</p>
                  <p className="text-gray-500">{job.cleaning_summary?.media_type || '—'}</p>
                </div>
                <span className={`px-2 py-0.5 rounded-full font-medium capitalize ${
                  job.status === 'completed' ? 'bg-green-100 text-green-700' :
                  job.status === 'running' ? 'bg-blue-100 text-blue-700' :
                  job.status === 'failed' ? 'bg-red-100 text-red-700' :
                  'bg-gray-100 text-gray-600'
                }`}>{job.status}</span>
              </div>
            ))}
          </div>
        </div>
      )}

    </div>
  )
}
