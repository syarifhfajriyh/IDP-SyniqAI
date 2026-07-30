import { useState, useEffect, useCallback } from 'react'
import { 
  Folder, File, Image, Video, Music, FileText, 
  Grid3x3, List, Search, Filter, Upload, Download,
  RefreshCw, Trash2, Eye, MoreVertical, FolderOpen,
  Calendar, HardDrive, ChevronRight
} from 'lucide-react'

const API_BASE = 'http://localhost:8000'

export default function FileBrowser() {
  const [viewMode, setViewMode] = useState('grid') // 'grid' or 'list'
  const [selectedType, setSelectedType] = useState('all')
  const [searchQuery, setSearchQuery] = useState('')
  const [currentPath, setCurrentPath] = useState(['root'])
  const [liveTables, setLiveTables] = useState([])
  const [liveFiles, setLiveFiles] = useState([])
  const [loadingFiles, setLoadingFiles] = useState(false)
  const [liveTypeCounts, setLiveTypeCounts] = useState({})
  const [storageInfo, setStorageInfo] = useState(null)
  const [hasFetched, setHasFetched] = useState(false)

  const fetchTablesAndFiles = useCallback(async () => {
    setLoadingFiles(true)
    try {
      const [tablesRes, imgRes, vidRes, audRes, docRes, statsRes] = await Promise.allSettled([
        fetch(`${API_BASE}/api/silver/unstructured/tables`),
        fetch(`${API_BASE}/api/silver/unstructured/preview/image?limit=10`),
        fetch(`${API_BASE}/api/silver/unstructured/preview/video?limit=10`),
        fetch(`${API_BASE}/api/silver/unstructured/preview/audio?limit=10`),
        fetch(`${API_BASE}/api/silver/unstructured/preview/document?limit=10`),
        fetch(`${API_BASE}/api/silver/unstructured/stats`),
      ])
      if (tablesRes.status === 'fulfilled' && tablesRes.value.ok) {
        const data = await tablesRes.value.json()
        setLiveTables(data.tables || [])
      }

      // Wire storage stats
      if (statsRes.status === 'fulfilled' && statsRes.value.ok) {
        const sdata = await statsRes.value.json()
        const s3 = sdata?.sources?.s3 || {}
        const mongo = sdata?.sources?.mongodb || {}
        const totalGB = (s3.total_size_gb || 0)
        const totalObjects = (s3.total_objects || 0) + (mongo.assets_collection || 0)
        setStorageInfo({ totalGB, totalObjects })
      }
      const allFiles = []
      const counts = {}
      const map = [
        { res: imgRes, type: 'image', emoji: '🖼️', key: 'images' },
        { res: vidRes, type: 'video', emoji: '🎬', key: 'videos' },
        { res: audRes, type: 'audio', emoji: '🎵', key: 'audio' },
        { res: docRes, type: 'document', emoji: '📄', key: 'documents' },
      ]
      for (const { res, type, emoji, key } of map) {
        if (res.status === 'fulfilled' && res.value.ok) {
          const data = await res.value.json()
          const records = data.records || []
          counts[key] = data.total || data.total_records || records.length
          records.forEach((r, i) => {
            allFiles.push({
              id: `${type}_${r.file_name || i}`,
              name: r.file_name || `${type}_${i}`,
              type,
              size: r.file_size_bytes ? `${(r.file_size_bytes / 1024 / 1024).toFixed(1)} MB` : '—',
              modified: r.last_modified || '—',
              thumbnail: emoji,
              metadata: { format: r.format || r.file_extension || type.toUpperCase() },
            })
          })
        }
      }
      // Always update state so empty results replace the placeholders
      setLiveFiles(allFiles)
      setLiveTypeCounts(counts)
    } catch (err) {
      console.warn('Could not fetch file browser data:', err)
    } finally {
      setHasFetched(true)
      setLoadingFiles(false)
    }
  }, [])

  useEffect(() => { fetchTablesAndFiles() }, [fetchTablesAndFiles])

  const fileTypes = [
    { id: 'all', label: 'All Files', icon: Folder, count: hasFetched ? liveFiles.length : '…' },
    { id: 'image', label: 'Images', icon: Image, count: hasFetched ? (liveTypeCounts.images ?? 0) : '…' },
    { id: 'video', label: 'Videos', icon: Video, count: hasFetched ? (liveTypeCounts.videos ?? 0) : '…' },
    { id: 'audio', label: 'Audio', icon: Music, count: hasFetched ? (liveTypeCounts.audio ?? 0) : '…' },
    { id: 'document', label: 'Documents', icon: FileText, count: hasFetched ? (liveTypeCounts.documents ?? 0) : '…' },
  ]

  const files = liveFiles.filter(f => {
    const typeMatch = selectedType === 'all' || f.type === selectedType
    const searchMatch = !searchQuery || f.name.toLowerCase().includes(searchQuery.toLowerCase())
    return typeMatch && searchMatch
  })

  const folders = liveTables.map((t, i) => ({
    id: i,
    name: t.table_name || t.tableName || t.prefix || `table_${i}`,
    files: t.row_count ?? '—',
    size: t.size_bytes ? `${(t.size_bytes / 1024 / 1024 / 1024).toFixed(1)} GB` : '—',
    modified: t.last_updated || '—',
  }))

  const handleFolderClick = (folderName) => {
    setCurrentPath(prev => [...prev, folderName])
    setSearchQuery('')
  }

  const handleBreadcrumbClick = (idx) => {
    setCurrentPath(prev => prev.slice(0, idx + 1))
  }

  const getFileIcon = (type) => {
    switch(type) {
      case 'image': return Image
      case 'video': return Video
      case 'audio': return Music
      case 'document': return FileText
      default: return File
    }
  }

  const getFileColor = (type) => {
    switch(type) {
      case 'image': return 'text-green-600 bg-green-50'
      case 'video': return 'text-purple-600 bg-purple-50'
      case 'audio': return 'text-pink-600 bg-pink-50'
      case 'document': return 'text-blue-600 bg-blue-50'
      default: return 'text-gray-600 bg-gray-50'
    }
  }

  return (
    <div className="h-full flex bg-gray-50">
      
      {/* Left Sidebar - File Types */}
      <div className="w-64 bg-white border-r border-gray-200 flex flex-col">
        <div className="p-4 border-b border-gray-200">
          <h2 className="font-bold text-gray-900 mb-4">File Types</h2>
          <div className="space-y-1">
            {fileTypes.map(type => {
              const Icon = type.icon
              return (
                <button
                  key={type.id}
                  onClick={() => setSelectedType(type.id)}
                  className={`w-full flex items-center justify-between p-3 rounded-lg transition-colors ${
                    selectedType === type.id
                      ? 'bg-blue-50 text-blue-600'
                      : 'text-gray-700 hover:bg-gray-50'
                  }`}
                >
                  <div className="flex items-center gap-3">
                    <Icon className="w-5 h-5" />
                    <span className="font-medium">{type.label}</span>
                  </div>
                  <span className="text-sm font-semibold">{type.count}</span>
                </button>
              )
            })}
          </div>
        </div>

        <div className="p-4 flex-1">
          <h3 className="font-semibold text-gray-900 mb-3 text-sm">Storage Info</h3>
          <div className="space-y-3">
            <div className="p-3 bg-gray-50 rounded-lg">
              <div className="flex items-center gap-2 mb-2">
                <HardDrive className="w-4 h-4 text-gray-600" />
                <span className="text-sm font-medium text-gray-700">Total Storage</span>
              </div>
              {storageInfo ? (
                <>
                  <p className="text-2xl font-bold text-gray-900">{storageInfo.totalGB.toFixed(1)} GB</p>
                  <p className="text-xs text-gray-600 mt-1">{storageInfo.totalObjects.toLocaleString()} objects indexed</p>
                </>
              ) : (
                <>
                  <p className="text-2xl font-bold text-gray-900">—</p>
                  <p className="text-xs text-gray-500 mt-1">Loading storage info…</p>
                </>
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        
        {/* Toolbar */}
        <div className="bg-white border-b border-gray-200 p-4">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center gap-2 text-sm text-gray-600">
              {currentPath.map((segment, idx) => (
                <div key={idx} className="flex items-center gap-2">
                  {idx > 0 && <ChevronRight className="w-4 h-4" />}
                  <button
                    onClick={() => handleBreadcrumbClick(idx)}
                    className={`hover:text-blue-600 font-medium ${idx === currentPath.length - 1 ? 'text-gray-900' : 'text-gray-500'}`}
                  >
                    {segment}
                  </button>
                </div>
              ))}
            </div>
            <div className="flex items-center gap-2">
              <button onClick={fetchTablesAndFiles} disabled={loadingFiles} className="p-2 hover:bg-gray-100 rounded-lg disabled:opacity-50" title="Refresh">
                <RefreshCw className={`w-5 h-5 text-gray-600 ${loadingFiles ? 'animate-spin' : ''}`} />
              </button>
              <button className="p-2 hover:bg-gray-100 rounded-lg" title="Upload">
                <Upload className="w-5 h-5 text-gray-600" />
              </button>
              <div className="border-l border-gray-300 h-6 mx-2"></div>
              <button 
                onClick={() => setViewMode('grid')}
                className={`p-2 rounded-lg ${viewMode === 'grid' ? 'bg-blue-50 text-blue-600' : 'hover:bg-gray-100 text-gray-600'}`}
              >
                <Grid3x3 className="w-5 h-5" />
              </button>
              <button 
                onClick={() => setViewMode('list')}
                className={`p-2 rounded-lg ${viewMode === 'list' ? 'bg-blue-50 text-blue-600' : 'hover:bg-gray-100 text-gray-600'}`}
              >
                <List className="w-5 h-5" />
              </button>
            </div>
          </div>

          <div className="flex gap-3">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400" />
              <input
                type="text"
                placeholder="Search files..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg"
              />
            </div>
            <button className="px-4 py-2 bg-white border border-gray-300 rounded-lg hover:bg-gray-50 flex items-center gap-2">
              <Filter className="w-5 h-5" />
              Filter
            </button>
          </div>
        </div>

        {/* File Browser Content */}
        <div className="flex-1 overflow-auto p-6">
          
          {/* Folders Section */}
          {loadingFiles && !hasFetched ? (
            <div className="flex items-center justify-center h-24 text-gray-400 text-sm">Loading folders…</div>
          ) : folders.length > 0 ? (
            <div className="mb-6">
              <h3 className="text-sm font-semibold text-gray-700 mb-3">Folders</h3>
              <div className={viewMode === 'grid' ? 'grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 gap-4' : 'space-y-2'}>
                {folders.map(folder => (
                  <div key={folder.id}
                    onClick={() => handleFolderClick(folder.name)}
                    className={viewMode === 'grid' 
                      ? 'bg-white border border-gray-200 rounded-lg p-4 hover:shadow-md cursor-pointer transition-shadow'
                      : 'bg-white border border-gray-200 rounded-lg p-4 hover:bg-gray-50 cursor-pointer flex items-center justify-between'
                    }>
                    <div className="flex items-center gap-3">
                      <FolderOpen className="w-8 h-8 text-blue-500" />
                      <div>
                        <p className="font-semibold text-gray-900">{folder.name}</p>
                        <p className="text-xs text-gray-600">{folder.files} files · {folder.size}</p>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : hasFetched ? (
            <div className="mb-6">
              <h3 className="text-sm font-semibold text-gray-700 mb-3">Folders</h3>
              <p className="text-sm text-gray-400 italic">No Silver tables found. Run a pipeline to generate data.</p>
            </div>
          ) : null}

          {/* Files Section */}
          <div>
            <h3 className="text-sm font-semibold text-gray-700 mb-3">Files</h3>
            {loadingFiles && !hasFetched ? (
              <div className="flex items-center justify-center h-32 text-gray-400 text-sm">Loading files…</div>
            ) : files.length === 0 ? (
              <div className="flex flex-col items-center justify-center h-32 text-gray-400">
                <File className="w-10 h-10 mb-2 opacity-30" />
                <p className="text-sm">{hasFetched ? 'No files found. Run a Feature Pipeline job to ingest data.' : 'Loading…'}</p>
              </div>
            ) : viewMode === 'grid' ? (
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 gap-4">
                {files.map(file => {
                  const Icon = getFileIcon(file.type)
                  return (
                    <div key={file.id} className="bg-white border border-gray-200 rounded-lg overflow-hidden hover:shadow-md cursor-pointer transition-shadow group">
                      <div className={`h-32 flex items-center justify-center text-6xl ${getFileColor(file.type)}`}>
                        {file.thumbnail}
                      </div>
                      <div className="p-3">
                        <div className="flex items-start justify-between gap-2">
                          <div className="flex-1 min-w-0">
                            <p className="font-medium text-gray-900 text-sm truncate" title={file.name}>{file.name}</p>
                            <p className="text-xs text-gray-600">{file.size}</p>
                          </div>
                          <button className="opacity-0 group-hover:opacity-100 p-1 hover:bg-gray-100 rounded">
                            <MoreVertical className="w-4 h-4 text-gray-600" />
                          </button>
                        </div>
                        {file.metadata && (
                          <div className="mt-2 text-xs text-gray-500">
                            {file.metadata.dimensions || file.metadata.duration || file.metadata.pages}
                          </div>
                        )}
                      </div>
                    </div>
                  )
                })}
              </div>
            ) : (
              <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
                <table className="w-full">
                  <thead className="bg-gray-50 border-b border-gray-200">
                    <tr>
                      <th className="text-left px-6 py-3 text-xs font-semibold text-gray-700">Name</th>
                      <th className="text-left px-6 py-3 text-xs font-semibold text-gray-700">Type</th>
                      <th className="text-left px-6 py-3 text-xs font-semibold text-gray-700">Size</th>
                      <th className="text-left px-6 py-3 text-xs font-semibold text-gray-700">Modified</th>
                      <th className="text-left px-6 py-3 text-xs font-semibold text-gray-700">Metadata</th>
                      <th className="text-right px-6 py-3 text-xs font-semibold text-gray-700">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-200">
                    {files.map(file => {
                      const Icon = getFileIcon(file.type)
                      return (
                        <tr key={file.id} className="hover:bg-gray-50 cursor-pointer">
                          <td className="px-6 py-4">
                            <div className="flex items-center gap-3">
                              <Icon className={`w-5 h-5 ${getFileColor(file.type).split(' ')[0]}`} />
                              <span className="font-medium text-gray-900">{file.name}</span>
                            </div>
                          </td>
                          <td className="px-6 py-4">
                            <span className={`px-2 py-1 rounded-full text-xs font-medium ${getFileColor(file.type)}`}>
                              {file.type}
                            </span>
                          </td>
                          <td className="px-6 py-4 text-sm text-gray-600">{file.size}</td>
                          <td className="px-6 py-4 text-sm text-gray-600">{file.modified}</td>
                          <td className="px-6 py-4 text-xs text-gray-500">
                            {file.metadata ? Object.entries(file.metadata).map(([key, val]) => `${key}: ${val}`).join(', ') : '—'}
                          </td>
                          <td className="px-6 py-4">
                            <div className="flex items-center justify-end gap-2">
                              <button className="p-1 hover:bg-gray-100 rounded" title="Preview">
                                <Eye className="w-4 h-4 text-gray-600" />
                              </button>
                              <button className="p-1 hover:bg-gray-100 rounded" title="Download">
                                <Download className="w-4 h-4 text-gray-600" />
                              </button>
                              <button className="p-1 hover:bg-gray-100 rounded" title="More">
                                <MoreVertical className="w-4 h-4 text-gray-600" />
                              </button>
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
      </div>

    </div>
  )
}
