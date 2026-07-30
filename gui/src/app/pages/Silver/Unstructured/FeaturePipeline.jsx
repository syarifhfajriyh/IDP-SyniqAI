import { useState, useEffect, useCallback } from 'react'
import { 
  Workflow, Play, Save, Upload, Download, 
  Plus, Trash2, Settings, Zap, Database,
  Image, Video, Music, FileText, Layers, Box, RefreshCw, CheckCircle
} from 'lucide-react'

const API_BASE = 'http://localhost:8000'

export default function FeaturePipeline() {
  const [nodes, setNodes] = useState([
    { id: 1, type: 'source', label: 'Image Source', x: 50, y: 150, config: { path: '/media/images' } },
    { id: 2, type: 'embedding', label: 'Generate Embeddings', x: 300, y: 150, config: { model: 'CLIP' } },
    { id: 3, type: 'vector-db', label: 'Vector Store', x: 550, y: 150, config: { db: 'Pinecone' } }
  ])
  const [selectedNode, setSelectedNode] = useState(null)
  const [nodeConfig, setNodeConfig] = useState({})
  const [applySuccess, setApplySuccess] = useState(false)
  const [livePipelines, setLivePipelines] = useState([])
  const [running, setRunning] = useState(false)
  const [runResult, setRunResult] = useState(null)

  // Pipeline run form state
  const [runForm, setRunForm] = useState({
    media_type: 'image',
    domain: 'media',
    entity: 'assets',
    stage_to_bronze: true,
  })
  const [s3Config, setS3Config] = useState({
    s3_bucket: '',
    s3_prefix: '',
    aws_access_key: '',
    aws_secret_key: '',
    aws_region: 'ap-southeast-1',
  })
  const [showS3Config, setShowS3Config] = useState(false)
  const [mongoConfig, setMongoConfig] = useState({
    uri: '',
    host: '',
    port: '27017',
    database: '',
    collection: '',
    username: '',
    password: '',
  })
  const [showMongoConfig, setShowMongoConfig] = useState(false)

  const fetchPipelines = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/api/silver/unstructured/jobs?limit=10`)
      if (res.ok) {
        const data = await res.json()
        setLivePipelines(data.jobs || [])
      }
    } catch (err) {
      console.warn('Could not fetch pipelines:', err)
    }
  }, [])

  useEffect(() => {
    fetchPipelines()
    const interval = setInterval(fetchPipelines, 8000)
    return () => clearInterval(interval)
  }, [fetchPipelines])

  const handleRunPipeline = async () => {
    setRunning(true)
    setRunResult(null)
    try {
      const hasS3 = s3Config.s3_bucket && s3Config.aws_access_key && s3Config.aws_secret_key
      const hasMongo = mongoConfig.database && (mongoConfig.uri || mongoConfig.host)
      const body = {
        media_type: runForm.media_type,
        domain: runForm.domain,
        entity: runForm.entity,
        stage_to_bronze: runForm.stage_to_bronze,
        ...(hasS3 && { s3_config: { ...s3Config } }),
        ...(hasMongo && { mongodb_config: { ...mongoConfig } }),
      }
      const res = await fetch(`${API_BASE}/api/silver/unstructured/process`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const data = await res.json()
      setRunResult(data)
      fetchPipelines()
    } catch (err) {
      setRunResult({ error: String(err) })
    } finally {
      setRunning(false)
    }
  }

  const nodeTypes = [
    { type: 'source', label: 'Data Source', icon: Database, color: 'blue', description: 'Input data' },
    { type: 'preprocessing', label: 'Preprocessing', icon: Layers, color: 'purple', description: 'Clean & transform' },
    { type: 'embedding', label: 'Embedding Model', icon: Box, color: 'green', description: 'Generate features' },
    { type: 'vector-db', label: 'Vector Database', icon: Database, color: 'orange', description: 'Store vectors' },
    { type: 'enrichment', label: 'Enrichment', icon: Zap, color: 'yellow', description: 'Add metadata' }
  ]

  const embeddingModels = [
    { id: 'clip', name: 'CLIP (OpenAI)', type: 'Image + Text', dimensions: 512 },
    { id: 'resnet', name: 'ResNet-50', type: 'Image', dimensions: 2048 },
    { id: 'bert', name: 'BERT', type: 'Text', dimensions: 768 },
    { id: 'whisper', name: 'Whisper', type: 'Audio', dimensions: 512 },
    { id: 'videomae', name: 'VideoMAE', type: 'Video', dimensions: 768 }
  ]

  const vectorDatabases = [
    { id: 'pinecone', name: 'Pinecone', type: 'Cloud', icon: '☁️' },
    { id: 'milvus', name: 'Milvus', type: 'Self-hosted', icon: '🏠' },
    { id: 'weaviate', name: 'Weaviate', type: 'Hybrid', icon: '🔀' },
    { id: 'qdrant', name: 'Qdrant', type: 'Fast', icon: '⚡' }
  ]

  const handleSelectNode = (node) => {
    setSelectedNode(node)
    setApplySuccess(false)
    setNodeConfig({
      label: node.label,
      sourceType: node.config?.sourceType || 'Images',
      path: node.config?.path || '',
      model: node.config?.model || 'clip',
      vectorDb: node.config?.db || 'pinecone',
      indexName: node.config?.indexName || '',
    })
  }

  const handleApplyConfig = () => {
    setNodes(prev => prev.map(n => {
      if (n.id !== selectedNode.id) return n
      const updated = {
        ...n,
        label: nodeConfig.label,
        config: n.type === 'source'
          ? { sourceType: nodeConfig.sourceType, path: nodeConfig.path }
          : n.type === 'embedding'
          ? { model: nodeConfig.model }
          : { db: nodeConfig.vectorDb, indexName: nodeConfig.indexName },
      }
      setSelectedNode(updated)
      return updated
    }))
    setApplySuccess(true)
    setTimeout(() => setApplySuccess(false), 2000)
  }

  const getStatusColor = (status) => {
    switch(status) {
      case 'running': return 'text-blue-600 bg-blue-50'
      case 'completed': return 'text-green-600 bg-green-50'
      case 'pending': return 'text-gray-600 bg-gray-50'
      case 'failed': return 'text-red-600 bg-red-50'
      default: return 'text-gray-600 bg-gray-50'
    }
  }

  const getTypeColor = (type) => {
    const colors = {
      blue: 'bg-blue-500 text-white',
      purple: 'bg-purple-500 text-white',
      green: 'bg-green-500 text-white',
      orange: 'bg-orange-500 text-white',
      yellow: 'bg-yellow-500 text-white'
    }
    return colors[type] || 'bg-gray-500 text-white'
  }

  return (
    <div className="h-full flex bg-gray-50">
      
      {/* Left Sidebar - Node Palette */}
      <div className="w-64 bg-white border-r border-gray-200 flex flex-col">
        <div className="p-4 border-b border-gray-200">
          <h2 className="font-bold text-gray-900 mb-4">Pipeline Nodes</h2>
          <div className="space-y-2">
            {nodeTypes.map(nodeType => {
              const Icon = nodeType.icon
              return (
                <div
                  key={nodeType.type}
                  draggable
                  className="p-3 bg-gray-50 rounded-lg cursor-move hover:bg-gray-100 transition-colors border border-gray-200"
                >
                  <div className="flex items-center gap-2 mb-1">
                    <div className={`p-1.5 rounded ${getTypeColor(nodeType.color)}`}>
                      <Icon className="w-4 h-4" />
                    </div>
                    <span className="font-medium text-sm text-gray-900">{nodeType.label}</span>
                  </div>
                  <p className="text-xs text-gray-600">{nodeType.description}</p>
                </div>
              )
            })}
          </div>
        </div>

        <div className="p-4 flex-1 overflow-auto">
          <h3 className="font-semibold text-gray-900 mb-3 text-sm">Active Pipelines</h3>
          <div className="space-y-2">
            {livePipelines.length === 0 ? (
              <p className="text-xs text-gray-500 italic">No jobs yet</p>
            ) : livePipelines.map(pipeline => (
              <div key={pipeline.job_id} className="p-3 bg-gray-50 rounded-lg">
                <p className="font-medium text-sm text-gray-900 mb-1 truncate">{pipeline.table_name || pipeline.entity}</p>
                <div className="flex items-center gap-2 mb-2">
                  <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${getStatusColor(pipeline.status)}`}>
                    {pipeline.status}
                  </span>
                  <span className="text-xs text-gray-600">{pipeline.cleaning_summary?.media_type || ''}</span>
                </div>
                {pipeline.status === 'running' && (
                  <div>
                    <div className="fail-full bg-gray-200 rounded-full h-1.5">
                      <div
                        className="bg-blue-600 h-1.5 rounded-full transition-all"
                        style={{ width: `${pipeline.progress || 0}%` }}
                      ></div>
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        
        {/* Toolbar */}
        <div className="bg-white border-b border-gray-200 p-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-xl font-bold text-gray-900">Feature Extraction Pipeline</h1>
              <p className="text-sm text-gray-600 mt-1">Build embedding generation workflows for unstructured data</p>
            </div>
            <div className="flex items-center gap-2">
              {/* Quick run form */}
              <select
                value={runForm.media_type}
                onChange={e => setRunForm(f => ({ ...f, media_type: e.target.value }))}
                className="px-2 py-2 border border-gray-300 rounded-lg text-sm"
              >
                {['image','video','audio','document','text','pdf'].map(t => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
              <input
                type="text"
                value={runForm.entity}
                onChange={e => setRunForm(f => ({ ...f, entity: e.target.value }))}
                placeholder="entity (e.g. assets)"
                className="px-2 py-2 border border-gray-300 rounded-lg text-sm w-32"
              />
              <button className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 flex items-center gap-2">
                <Save className="w-4 h-4" />
                Save
              </button>
              <button
                onClick={handleRunPipeline}
                disabled={running}
                className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center gap-2 disabled:opacity-50"
              >
                {running ? <RefreshCw className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
                {running ? 'Starting…' : 'Run Pipeline'}
              </button>
            </div>
          </div>
          {runResult && (
            <div className={`mt-2 px-4 py-2 text-sm rounded-lg ${runResult.error ? 'bg-red-50 text-red-700' : 'bg-green-50 text-green-700'}`}>
              {runResult.error
                ? `Error: ${runResult.error}`
                : `Job queued: ${runResult.job_id} — ${runResult.message || 'Pipeline started'}`}
            </div>
          )}

          {/* S3 Source config panel */}
          <div className="mt-3">
            <button
              type="button"
              onClick={() => setShowS3Config(v => !v)}
              className="flex items-center gap-1 text-xs text-blue-600 hover:text-blue-800 font-medium"
            >
              <Settings className="w-3 h-3" />
              {showS3Config ? 'Hide' : 'Configure'} S3 Source (optional)
            </button>
            {showS3Config && (
              <div className="mt-2 p-3 bg-blue-50 border border-blue-200 rounded-lg grid grid-cols-2 gap-2 text-sm">
                <div>
                  <label className="block text-xs text-gray-600 mb-1">S3 Bucket</label>
                  <input
                    type="text"
                    value={s3Config.s3_bucket}
                    onChange={e => setS3Config(c => ({ ...c, s3_bucket: e.target.value }))}
                    placeholder="izy-raw-datalake-2026"
                    className="w-full px-2 py-1.5 border border-gray-300 rounded text-xs"
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-600 mb-1">Prefix (folder)</label>
                  <input
                    type="text"
                    value={s3Config.s3_prefix}
                    onChange={e => setS3Config(c => ({ ...c, s3_prefix: e.target.value }))}
                    placeholder="metadata/"
                    className="w-full px-2 py-1.5 border border-gray-300 rounded text-xs"
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-600 mb-1">AWS Access Key</label>
                  <input
                    type="text"
                    value={s3Config.aws_access_key}
                    onChange={e => setS3Config(c => ({ ...c, aws_access_key: e.target.value }))}
                    placeholder="AKIA…"
                    className="w-full px-2 py-1.5 border border-gray-300 rounded text-xs font-mono"
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-600 mb-1">AWS Secret Key</label>
                  <input
                    type="password"
                    value={s3Config.aws_secret_key}
                    onChange={e => setS3Config(c => ({ ...c, aws_secret_key: e.target.value }))}
                    placeholder="secret…"
                    className="w-full px-2 py-1.5 border border-gray-300 rounded text-xs font-mono"
                  />
                </div>
                <div className="col-span-2">
                  <label className="block text-xs text-gray-600 mb-1">AWS Region</label>
                  <input
                    type="text"
                    value={s3Config.aws_region}
                    onChange={e => setS3Config(c => ({ ...c, aws_region: e.target.value }))}
                    placeholder="ap-southeast-1"
                    className="w-full px-2 py-1.5 border border-gray-300 rounded text-xs"
                  />
                </div>
              </div>
            )}
          </div>

          {/* MongoDB Source config panel */}
          <div className="mt-2">
            <button
              type="button"
              onClick={() => setShowMongoConfig(v => !v)}
              className="flex items-center gap-1 text-xs text-green-700 hover:text-green-900 font-medium"
            >
              <Database className="w-3 h-3" />
              {showMongoConfig ? 'Hide' : 'Configure'} MongoDB Source (optional)
            </button>
            {showMongoConfig && (
              <div className="mt-2 p-3 bg-green-50 border border-green-200 rounded-lg grid grid-cols-2 gap-2 text-sm">
                <div className="col-span-2">
                  <label className="block text-xs text-gray-600 mb-1">Connection URI (overrides host/port)</label>
                  <input
                    type="text"
                    value={mongoConfig.uri}
                    onChange={e => setMongoConfig(c => ({ ...c, uri: e.target.value }))}
                    placeholder="mongodb+srv://user:pass@cluster.mongodb.net"
                    className="w-full px-2 py-1.5 border border-gray-300 rounded text-xs font-mono"
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-600 mb-1">Host</label>
                  <input
                    type="text"
                    value={mongoConfig.host}
                    onChange={e => setMongoConfig(c => ({ ...c, host: e.target.value }))}
                    placeholder="localhost"
                    className="w-full px-2 py-1.5 border border-gray-300 rounded text-xs"
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-600 mb-1">Port</label>
                  <input
                    type="text"
                    value={mongoConfig.port}
                    onChange={e => setMongoConfig(c => ({ ...c, port: e.target.value }))}
                    placeholder="27017"
                    className="w-full px-2 py-1.5 border border-gray-300 rounded text-xs"
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-600 mb-1">Database</label>
                  <input
                    type="text"
                    value={mongoConfig.database}
                    onChange={e => setMongoConfig(c => ({ ...c, database: e.target.value }))}
                    placeholder="media"
                    className="w-full px-2 py-1.5 border border-gray-300 rounded text-xs"
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-600 mb-1">Collection</label>
                  <input
                    type="text"
                    value={mongoConfig.collection}
                    onChange={e => setMongoConfig(c => ({ ...c, collection: e.target.value }))}
                    placeholder="assets"
                    className="w-full px-2 py-1.5 border border-gray-300 rounded text-xs"
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-600 mb-1">Username</label>
                  <input
                    type="text"
                    value={mongoConfig.username}
                    onChange={e => setMongoConfig(c => ({ ...c, username: e.target.value }))}
                    placeholder="optional"
                    className="w-full px-2 py-1.5 border border-gray-300 rounded text-xs"
                  />
                </div>
                <div>
                  <label className="block text-xs text-gray-600 mb-1">Password</label>
                  <input
                    type="password"
                    value={mongoConfig.password}
                    onChange={e => setMongoConfig(c => ({ ...c, password: e.target.value }))}
                    placeholder="optional"
                    className="w-full px-2 py-1.5 border border-gray-300 rounded text-xs"
                  />
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Pipeline Canvas */}
        <div className="flex-1 flex overflow-hidden">
          
          {/* Canvas Area */}
          <div className="flex-1 relative overflow-auto bg-gray-100">
            <svg className="absolute inset-0 w-full h-full pointer-events-none">
              {/* Grid pattern */}
              <defs>
                <pattern id="grid" width="20" height="20" patternUnits="userSpaceOnUse">
                  <path d="M 20 0 L 0 0 0 20" fill="none" stroke="#e5e7eb" strokeWidth="0.5"/>
                </pattern>
              </defs>
              <rect width="100%" height="100%" fill="url(#grid)" />
              
              {/* Connections */}
              <path d="M 150 180 C 225 180, 225 180, 300 180" stroke="#3b82f6" strokeWidth="2" fill="none" markerEnd="url(#arrowhead)" />
              <path d="M 400 180 C 475 180, 475 180, 550 180" stroke="#3b82f6" strokeWidth="2" fill="none" markerEnd="url(#arrowhead)" />
              
              <defs>
                <marker id="arrowhead" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto">
                  <polygon points="0 0, 10 3, 0 6" fill="#3b82f6" />
                </marker>
              </defs>
            </svg>

            {/* Nodes */}
            {nodes.map(node => (
              <div
                key={node.id}
                onClick={() => handleSelectNode(node)}
                className={`absolute bg-white rounded-lg shadow-lg border-2 cursor-pointer transition-all ${
                  selectedNode?.id === node.id ? 'border-blue-500' : 'border-gray-300 hover:border-gray-400'
                }`}
                style={{ left: `${node.x}px`, top: `${node.y}px`, width: '200px' }}
              >
                <div className="p-4">
                  <div className="flex items-center justify-between mb-2">
                    <div className={`p-2 rounded ${getTypeColor(nodeTypes.find(nt => nt.type === node.type)?.color || 'blue')}`}>
                      {node.type === 'source' && <Database className="w-4 h-4" />}
                      {node.type === 'embedding' && <Box className="w-4 h-4" />}
                      {node.type === 'vector-db' && <Database className="w-4 h-4" />}
                    </div>
                    <button className="p-1 hover:bg-gray-100 rounded">
                      <Trash2 className="w-4 h-4 text-gray-600" />
                    </button>
                  </div>
                  <h3 className="font-semibold text-sm text-gray-900 mb-1">{node.label}</h3>
                  {node.config && (
                    <div className="text-xs text-gray-600">
                      {Object.entries(node.config).map(([key, val]) => (
                        <div key={key}>{key}: {val}</div>
                      ))}
                    </div>
                  )}
                </div>
                {/* Connection points */}
                <div className="absolute -right-2 top-1/2 w-4 h-4 bg-blue-500 rounded-full border-2 border-white transform -translate-y-1/2"></div>
                <div className="absolute -left-2 top-1/2 w-4 h-4 bg-gray-400 rounded-full border-2 border-white transform -translate-y-1/2"></div>
              </div>
            ))}

            {/* Empty State */}
            {nodes.length === 0 && (
              <div className="absolute inset-0 flex items-center justify-center text-gray-500">
                <div className="text-center">
                  <Workflow className="w-16 h-16 mx-auto mb-4 opacity-50" />
                  <p className="text-lg font-medium">Drag nodes to build your pipeline</p>
                  <p className="text-sm mt-1">Connect data sources to embedding models and vector stores</p>
                </div>
              </div>
            )}
          </div>

          {/* Right Sidebar - Node Configuration */}
          {selectedNode && (
            <div className="w-80 bg-white border-l border-gray-200 overflow-auto">
              <div className="p-4 border-b border-gray-200">
                <h3 className="font-bold text-gray-900">Node Configuration</h3>
              </div>
              
              <div className="p-4 space-y-4">
                <div>
                  <label className="text-sm font-medium text-gray-700 mb-2 block">Node Label</label>
                  <input
                    type="text"
                    value={nodeConfig.label || ''}
                    onChange={e => setNodeConfig(c => ({ ...c, label: e.target.value }))}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                  />
                </div>

                {selectedNode.type === 'source' && (
                  <>
                    <div>
                      <label className="text-sm font-medium text-gray-700 mb-2 block">Source Type</label>
                      <select
                        value={nodeConfig.sourceType || 'Images'}
                        onChange={e => setNodeConfig(c => ({ ...c, sourceType: e.target.value }))}
                        className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                      >
                        <option>Images</option>
                        <option>Videos</option>
                        <option>Audio</option>
                        <option>Documents</option>
                      </select>
                    </div>
                    <div>
                      <label className="text-sm font-medium text-gray-700 mb-2 block">Path</label>
                      <input
                        type="text"
                        value={nodeConfig.path || ''}
                        onChange={e => setNodeConfig(c => ({ ...c, path: e.target.value }))}
                        className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                        placeholder="/path/to/data"
                      />
                    </div>
                  </>
                )}

                {selectedNode.type === 'embedding' && (
                  <>
                    <div>
                      <label className="text-sm font-medium text-gray-700 mb-2 block">Embedding Model</label>
                      <select
                        value={nodeConfig.model || 'clip'}
                        onChange={e => setNodeConfig(c => ({ ...c, model: e.target.value }))}
                        className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                      >
                        {embeddingModels.map(model => (
                          <option key={model.id} value={model.id}>
                            {model.name} ({model.type}) - {model.dimensions}D
                          </option>
                        ))}
                      </select>
                    </div>
                    {(() => {
                      const m = embeddingModels.find(m => m.id === nodeConfig.model) || embeddingModels[0]
                      return (
                        <div className="p-3 bg-blue-50 rounded-lg text-sm">
                          <p className="text-blue-900 font-medium mb-1">Model Info</p>
                          <p className="text-blue-700 text-xs">{m.name} generates {m.dimensions}-dimensional embeddings for {m.type} data.</p>
                        </div>
                      )
                    })()}
                  </>
                )}

                {selectedNode.type === 'vector-db' && (
                  <>
                    <div>
                      <label className="text-sm font-medium text-gray-700 mb-2 block">Vector Database</label>
                      <div className="space-y-2">
                        {vectorDatabases.map(db => (
                          <label key={db.id} className="flex items-center gap-3 p-3 border border-gray-300 rounded-lg cursor-pointer hover:bg-gray-50">
                            <input
                              type="radio"
                              name="vectordb"
                              value={db.id}
                              checked={nodeConfig.vectorDb === db.id}
                              onChange={() => setNodeConfig(c => ({ ...c, vectorDb: db.id }))}
                            />
                            <div className="text-2xl">{db.icon}</div>
                            <div className="flex-1">
                              <p className="font-medium text-sm text-gray-900">{db.name}</p>
                              <p className="text-xs text-gray-600">{db.type}</p>
                            </div>
                          </label>
                        ))}
                      </div>
                    </div>
                    <div>
                      <label className="text-sm font-medium text-gray-700 mb-2 block">Index Name</label>
                      <input
                        type="text"
                        value={nodeConfig.indexName || ''}
                        onChange={e => setNodeConfig(c => ({ ...c, indexName: e.target.value }))}
                        placeholder="my-embeddings-index"
                        className="w-full px-3 py-2 border border-gray-300 rounded-lg"
                      />
                    </div>
                  </>
                )}

                <div className="pt-4 border-t border-gray-200">
                  <button
                    onClick={handleApplyConfig}
                    className="w-full px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center justify-center gap-2"
                  >
                    {applySuccess
                      ? <><CheckCircle className="w-4 h-4" /> Applied!</>
                      : <><Settings className="w-4 h-4" /> Apply Configuration</>}
                  </button>
                </div>
              </div>
            </div>
          )}

        </div>
      </div>

    </div>
  )
}
