import { useState, useEffect, useCallback } from 'react'
import { 
  Image, Video, Scan, Tag, Play, Pause, 
  ZoomIn, Download, Upload, Settings, Eye,
  Grid3x3, Box, AlertCircle, CheckCircle, Layers, RefreshCw
} from 'lucide-react'

const API_BASE = 'http://localhost:8000'

export default function ObjectDetection() {
  const [selectedMedia, setSelectedMedia] = useState(null)
  const [detectionMode, setDetectionMode] = useState('objects') // 'objects', 'faces', 'text', 'scenes'
  const [isProcessing, setIsProcessing] = useState(false)
  const [liveMediaItems, setLiveMediaItems] = useState([])
  const [loadingMedia, setLoadingMedia] = useState(false)
  const [hasFetched, setHasFetched] = useState(false)

  const fetchMediaItems = useCallback(async () => {
    setLoadingMedia(true)
    try {
      const res = await fetch(`${API_BASE}/api/silver/unstructured/preview/image?domain=media&entity=assets&limit=20`)
      if (res.ok) {
        const data = await res.json()
        const mapped = (data.records || []).map((r, i) => ({
          id: r.file_name || i,
          name: r.file_name || `image_${i}.jpg`,
          type: 'image',
          thumbnail: '🖼️',
          status: r.processing_status || 'pending',
          detections: [],
          processed: r.last_modified || null,
          width: r.width,
          height: r.height,
          format: r.format,
          blur_score: r.blur_score,
          brightness: r.brightness_avg,
        }))
        setLiveMediaItems(mapped)
      }
    } catch (err) {
      console.warn('Could not fetch image preview:', err)
    } finally {
      setHasFetched(true)
      setLoadingMedia(false)
    }
  }, [])

  useEffect(() => { fetchMediaItems() }, [fetchMediaItems])

  const mediaItems = liveMediaItems

  const models = [
    { id: 'yolov8', name: 'YOLOv8', description: 'Fast object detection', accuracy: '92%', speed: 'Fast' },
    { id: 'rcnn', name: 'Faster R-CNN', description: 'High accuracy detection', accuracy: '96%', speed: 'Medium' },
    { id: 'efficientdet', name: 'EfficientDet', description: 'Balanced performance', accuracy: '94%', speed: 'Fast' },
    { id: 'custom', name: 'Custom Model', description: 'Domain-specific trained', accuracy: '97%', speed: 'Medium' }
  ]

  const detectionTypes = [
    { id: 'objects', label: 'Objects', icon: Box, description: 'Detect common objects' },
    { id: 'faces', label: 'Faces', icon: Eye, description: 'Face detection & recognition' },
    { id: 'text', label: 'Text', icon: Tag, description: 'OCR text extraction' },
    { id: 'scenes', label: 'Scenes', icon: Layers, description: 'Scene classification' }
  ]

  const detectedObjects = selectedMedia?.detections || []

  const getStatusColor = (status) => {
    switch(status) {
      case 'processed': return 'text-green-600 bg-green-50'
      case 'processing': return 'text-blue-600 bg-blue-50'
      case 'pending': return 'text-gray-600 bg-gray-50'
      case 'failed': return 'text-red-600 bg-red-50'
      default: return 'text-gray-600 bg-gray-50'
    }
  }

  return (
    <div className="h-full flex bg-gray-50">
      
      {/* Left Sidebar - Media Gallery */}
      <div className="w-80 bg-white border-r border-gray-200 flex flex-col">
        <div className="p-4 border-b border-gray-200">
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-bold text-gray-900">Media Library {liveMediaItems.length > 0 && <span className="text-xs text-green-600 font-normal ml-1">● live</span>}</h2>
            <div className="flex items-center gap-1">
              <button onClick={fetchMediaItems} disabled={loadingMedia} className="p-1.5 text-gray-500 hover:text-gray-700 rounded-lg hover:bg-gray-100 disabled:opacity-50">
                <RefreshCw className={`w-4 h-4 ${loadingMedia ? 'animate-spin' : ''}`} />
              </button>
              <button className="p-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700">
                <Upload className="w-4 h-4" />
              </button>
            </div>
          </div>
          
          {/* Detection Mode Selector */}
          <div className="mb-4">
            <label className="text-sm font-medium text-gray-700 mb-2 block">Detection Type</label>
            <div className="grid grid-cols-2 gap-2">
              {detectionTypes.map(type => {
                const Icon = type.icon
                return (
                  <button
                    key={type.id}
                    onClick={() => setDetectionMode(type.id)}
                    className={`p-2 rounded-lg border text-left ${
                      detectionMode === type.id
                        ? 'border-blue-500 bg-blue-50 text-blue-700'
                        : 'border-gray-300 bg-white text-gray-700 hover:bg-gray-50'
                    }`}
                  >
                    <Icon className="w-4 h-4 mb-1" />
                    <div className="text-xs font-medium">{type.label}</div>
                  </button>
                )
              })}
            </div>
          </div>
        </div>

        <div className="flex-1 overflow-auto p-4 space-y-3">
          {loadingMedia && !hasFetched ? (
            <div className="flex items-center justify-center h-32 text-gray-400 text-sm">Loading media…</div>
          ) : mediaItems.length === 0 && hasFetched ? (
            <div className="flex flex-col items-center justify-center h-32 text-gray-400">
              <Image className="w-10 h-10 mb-2 opacity-30" />
              <p className="text-xs text-center">No media found. Run a Feature Pipeline job to ingest images or videos.</p>
            </div>
          ) : null}
          {mediaItems.map(item => (
            <div
              key={item.id}
              onClick={() => setSelectedMedia(item)}
              className={`p-3 rounded-lg border cursor-pointer transition-all ${
                selectedMedia?.id === item.id
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-200 bg-white hover:border-gray-300'
              }`}
            >
              <div className="flex items-start gap-3">
                <div className="w-16 h-16 bg-gray-100 rounded flex items-center justify-center text-3xl">
                  {item.thumbnail}
                </div>
                <div className="flex-1 min-w-0">
                  <p className="font-medium text-sm text-gray-900 truncate">{item.name}</p>
                  <div className="flex items-center gap-2 mt-1">
                    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${getStatusColor(item.status)}`}>
                      {item.status}
                    </span>
                    {item.type === 'video' && item.duration && (
                      <span className="text-xs text-gray-600">{item.duration}</span>
                    )}
                  </div>
                  {item.detections && item.detections.length > 0 && (
                    <p className="text-xs text-gray-600 mt-1">
                      {item.detections.length} objects detected
                    </p>
                  )}
                  {item.progress && (
                    <div className="mt-2">
                      <div className="w-full bg-gray-200 rounded-full h-1.5">
                        <div 
                          className="bg-blue-600 h-1.5 rounded-full"
                          style={{ width: `${item.progress}%` }}
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
              <h1 className="text-xl font-bold text-gray-900">Object Detection & Analysis</h1>
              <p className="text-sm text-gray-600 mt-1">
                {selectedMedia ? selectedMedia.name : 'Select media to analyze'}
              </p>
            </div>
            <div className="flex items-center gap-2">
              {selectedMedia && (
                <>
                  <button className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2">
                    <Scan className="w-4 h-4" />
                    Run Detection
                  </button>
                  <button className="p-2 border border-gray-300 rounded-lg hover:bg-gray-50">
                    <Download className="w-5 h-5 text-gray-600" />
                  </button>
                  <button className="p-2 border border-gray-300 rounded-lg hover:bg-gray-50">
                    <Settings className="w-5 h-5 text-gray-600" />
                  </button>
                </>
              )}
            </div>
          </div>
        </div>

        {/* Main Canvas Area */}
        <div className="flex-1 flex overflow-hidden">
          
          {/* Canvas/Preview Area */}
          <div className="flex-1 p-6 overflow-auto">
            {selectedMedia ? (
              <div className="bg-white rounded-lg shadow-lg p-6">
                {/* Media Display with Bounding Boxes */}
                <div className="relative bg-gray-100 rounded-lg aspect-video flex items-center justify-center">
                  <div className="text-6xl">{selectedMedia.thumbnail}</div>
                  
                  {/* Bounding Boxes Overlay */}
                  {selectedMedia.detections && selectedMedia.detections.length > 0 && (
                    <svg className="absolute inset-0 w-full h-full">
                      {selectedMedia.detections.map((detection, idx) => (
                        <g key={idx}>
                          <rect
                            x={`${(detection.bbox[0] / 800) * 100}%`}
                            y={`${(detection.bbox[1] / 600) * 100}%`}
                            width={`${((detection.bbox[2] - detection.bbox[0]) / 800) * 100}%`}
                            height={`${((detection.bbox[3] - detection.bbox[1]) / 600) * 100}%`}
                            fill="none"
                            stroke="#3b82f6"
                            strokeWidth="3"
                            rx="4"
                          />
                          <text
                            x={`${(detection.bbox[0] / 800) * 100}%`}
                            y={`${(detection.bbox[1] / 600) * 100 - 1}%`}
                            className="text-xs fill-white"
                          >
                            <tspan className="font-semibold">{detection.label}</tspan>
                            <tspan className="opacity-80"> {(detection.confidence * 100).toFixed(0)}%</tspan>
                          </text>
                        </g>
                      ))}
                    </svg>
                  )}
                </div>

                {/* Video Controls (if video) */}
                {selectedMedia.type === 'video' && (
                  <div className="mt-4 flex items-center gap-4">
                    <button className="p-2 bg-blue-600 text-white rounded-full hover:bg-blue-700">
                      <Play className="w-5 h-5" />
                    </button>
                    <div className="flex-1">
                      <input type="range" className="w-full" min="0" max="100" defaultValue="0" />
                    </div>
                    <span className="text-sm text-gray-600">0:00 / {selectedMedia.duration}</span>
                  </div>
                )}

                {/* Processing Status */}
                {selectedMedia.status === 'processing' && (
                  <div className="mt-4 p-4 bg-blue-50 border border-blue-200 rounded-lg">
                    <div className="flex items-center gap-3">
                      <div className="animate-spin">
                        <Scan className="w-5 h-5 text-blue-600" />
                      </div>
                      <div className="flex-1">
                        <p className="text-sm font-medium text-blue-900">Processing... {selectedMedia.progress}%</p>
                        <div className="w-full bg-blue-200 rounded-full h-2 mt-2">
                          <div 
                            className="bg-blue-600 h-2 rounded-full transition-all"
                            style={{ width: `${selectedMedia.progress}%` }}
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
                  <Image className="w-16 h-16 mx-auto mb-4 opacity-50" />
                  <p className="text-lg font-medium">No media selected</p>
                  <p className="text-sm mt-1">Choose an image or video to start detection</p>
                </div>
              </div>
            )}
          </div>

          {/* Right Sidebar - Detections & Models */}
          <div className="w-80 bg-white border-l border-gray-200 overflow-auto">
            
            {/* Detected Objects */}
            <div className="p-4 border-b border-gray-200">
              <h3 className="font-bold text-gray-900 mb-3">Detected Objects</h3>
              {detectedObjects.length > 0 ? (
                <div className="space-y-2">
                  {detectedObjects.map((obj, idx) => (
                    <div key={idx} className="p-3 bg-gray-50 rounded-lg">
                      <div className="flex items-center justify-between mb-1">
                        <span className="font-semibold text-gray-900">{obj.label}</span>
                        <span className="text-sm text-gray-600">{(obj.confidence * 100).toFixed(1)}%</span>
                      </div>
                      <div className="w-full bg-gray-200 rounded-full h-1.5">
                        <div 
                          className="bg-green-600 h-1.5 rounded-full"
                          style={{ width: `${obj.confidence * 100}%` }}
                        ></div>
                      </div>
                      <p className="text-xs text-gray-500 mt-1">
                        Type: {obj.type} | BBox: [{obj.bbox.join(', ')}]
                      </p>
                    </div>
                  ))}
                </div>
              ) : (
                <div className="text-center py-8 text-gray-500">
                  <AlertCircle className="w-8 h-8 mx-auto mb-2 opacity-50" />
                  <p className="text-sm">No detections yet</p>
                </div>
              )}
            </div>

            {/* ML Models */}
            <div className="p-4">
              <h3 className="font-bold text-gray-900 mb-3">Detection Models</h3>
              <div className="space-y-2">
                {models.map(model => (
                  <div key={model.id} className="p-3 border border-gray-200 rounded-lg hover:border-blue-500 cursor-pointer transition-colors">
                    <p className="font-semibold text-gray-900">{model.name}</p>
                    <p className="text-xs text-gray-600 mt-1">{model.description}</p>
                    <div className="flex items-center gap-4 mt-2 text-xs">
                      <span className="text-gray-600">Accuracy: <strong>{model.accuracy}</strong></span>
                      <span className="text-gray-600">Speed: <strong>{model.speed}</strong></span>
                    </div>
                  </div>
                ))}
              </div>
            </div>

          </div>

        </div>
      </div>

    </div>
  )
}
