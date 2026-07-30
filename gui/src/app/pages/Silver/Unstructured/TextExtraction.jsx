import { useState, useEffect, useCallback } from 'react'
import { 
  FileText, Image, File, Scan, Type, Languages,
  Download, Upload, Copy, Check, AlertCircle, Eye,
  Search, ZoomIn, Grid3x3, RefreshCw
} from 'lucide-react'

const API_BASE = 'http://localhost:8000'

export default function TextExtraction() {
  const [selectedDocument, setSelectedDocument] = useState(null)
  const [extractionMethod, setExtractionMethod] = useState('ocr') // 'ocr', 'pdf', 'handwriting'
  const [extractedText, setExtractedText] = useState('')
  const [liveDocuments, setLiveDocuments] = useState([])
  const [loadingDocs, setLoadingDocs] = useState(false)
  const [hasFetched, setHasFetched] = useState(false)

  const fetchDocuments = useCallback(async () => {
    setLoadingDocs(true)
    try {
      const res = await fetch(`${API_BASE}/api/silver/unstructured/preview/document?domain=media&entity=assets&limit=20`)
      if (res.ok) {
        const data = await res.json()
        const mapped = (data.records || []).map((r, i) => ({
          id: r.file_name || i,
          name: r.file_name || `document_${i}.pdf`,
          type: (r.file_extension || 'pdf').replace('.', ''),
          pages: r.page_count || null,
          thumbnail: (r.file_extension || '').includes('pdf') ? '📄' : '🖼️',
          status: r.processing_status || 'pending',
          extracted: !!r.extracted_text,
          textLength: r.word_count ? r.word_count * 5 : 0,
          language: r.detected_language || 'Unknown',
          confidence: r.confidence || null,
          text_preview: r.text_preview || '',
          is_corrupted: r.is_corrupted || false,
        }))
        setLiveDocuments(mapped)
      }
    } catch (err) {
      console.warn('Could not fetch document preview:', err)
    } finally {
      setHasFetched(true)
      setLoadingDocs(false)
    }
  }, [])

  useEffect(() => { fetchDocuments() }, [fetchDocuments])

  const documents = liveDocuments

  const extractionMethods = [
    { id: 'ocr', label: 'OCR', icon: Scan, description: 'Image-based text extraction' },
    { id: 'pdf', label: 'PDF Parser', icon: FileText, description: 'Native PDF text extraction' },
    { id: 'handwriting', label: 'Handwriting', icon: Type, description: 'Handwritten text recognition' }
  ]

  const extractedFields = [
    { field: 'Invoice Number', value: 'INV-2024-0312', confidence: 0.98 },
    { field: 'Date', value: '2024-03-10', confidence: 0.95 },
    { field: 'Total Amount', value: '$1,245.50', confidence: 0.97 },
    { field: 'Customer Name', value: 'Acme Corporation', confidence: 0.94 },
    { field: 'Payment Method', value: 'Credit Card', confidence: 0.91 },
    { field: 'Due Date', value: '2024-04-10', confidence: 0.96 }
  ]

  const sampleExtractedText = `INVOICE

Invoice Number: INV-2024-0312
Date: March 10, 2024
Due Date: April 10, 2024

Bill To:
Acme Corporation
123 Business Street
New York, NY 10001

Items:
1. Product A - Widget Pro ......... $500.00
2. Product B - Feature Plus ....... $745.50

Subtotal: $1,245.50
Tax (8%): $99.64
Total: $1,345.14

Payment Method: Credit Card
Status: Paid

Thank you for your business!`

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
      
      {/* Left Sidebar - Document List */}
      <div className="w-80 bg-white border-r border-gray-200 flex flex-col">
        <div className="p-4 border-b border-gray-200">
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-bold text-gray-900">Documents {liveDocuments.length > 0 && <span className="text-xs text-green-600 font-normal ml-1">● live</span>}</h2>
            <div className="flex items-center gap-1">
              <button onClick={fetchDocuments} disabled={loadingDocs} className="p-1.5 text-gray-500 hover:text-gray-700 rounded-lg hover:bg-gray-100 disabled:opacity-50">
                <RefreshCw className={`w-4 h-4 ${loadingDocs ? 'animate-spin' : ''}`} />
              </button>
              <button className="p-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700">
                <Upload className="w-4 h-4" />
              </button>
            </div>
          </div>
          
          {/* Extraction Method Selector */}
          <div className="mb-4">
            <label className="text-sm font-medium text-gray-700 mb-2 block">Extraction Method</label>
            <select 
              value={extractionMethod}
              onChange={(e) => setExtractionMethod(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg"
            >
              {extractionMethods.map(method => (
                <option key={method.id} value={method.id}>{method.label}</option>
              ))}
            </select>
          </div>

          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-gray-400" />
            <input
              type="text"
              placeholder="Search documents..."
              className="w-full pl-9 pr-3 py-2 border border-gray-300 rounded-lg text-sm"
            />
          </div>
        </div>

        <div className="flex-1 overflow-auto p-4 space-y-3">
          {loadingDocs && !hasFetched ? (
            <div className="flex items-center justify-center h-32 text-gray-400 text-sm">Loading documents…</div>
          ) : documents.length === 0 && hasFetched ? (
            <div className="flex flex-col items-center justify-center h-32 text-gray-400">
              <FileText className="w-10 h-10 mb-2 opacity-30" />
              <p className="text-xs text-center">No documents found. Run a Feature Pipeline job to ingest files.</p>
            </div>
          ) : null}
          {documents.map(doc => (
            <div
              key={doc.id}
              onClick={() => {
                setSelectedDocument(doc)
                if (doc.extracted) setExtractedText(sampleExtractedText)
              }}
              className={`p-3 rounded-lg border cursor-pointer transition-all ${
                selectedDocument?.id === doc.id
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-200 bg-white hover:border-gray-300'
              }`}
            >
              <div className="flex items-start gap-3">
                <div className="w-12 h-16 bg-gray-100 rounded flex items-center justify-center text-2xl">
                  {doc.thumbnail}
                </div>
                <div className="flex-1 min-w-0">
                  <p className="font-medium text-sm text-gray-900 truncate">{doc.name}</p>
                  <div className="flex items-center gap-2 mt-1">
                    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${getStatusColor(doc.status)}`}>
                      {doc.status}
                    </span>
                  </div>
                  {doc.pages && (
                    <p className="text-xs text-gray-600 mt-1">{doc.pages} pages</p>
                  )}
                  {doc.extracted && doc.confidence != null && (
                    <div className="mt-1 flex items-center gap-2 text-xs text-gray-600">
                      <span>{doc.textLength} chars</span>
                      <span>•</span>
                      <span>{(doc.confidence * 100).toFixed(0)}% confidence</span>
                    </div>
                  )}
                  {doc.progress && (
                    <div className="mt-2">
                      <div className="w-full bg-gray-200 rounded-full h-1.5">
                        <div 
                          className="bg-blue-600 h-1.5 rounded-full transition-all"
                          style={{ width: `${doc.progress}%` }}
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
              <h1 className="text-xl font-bold text-gray-900">Text Extraction</h1>
              <p className="text-sm text-gray-600 mt-1">
                {selectedDocument ? selectedDocument.name : 'Select a document to extract text'}
              </p>
            </div>
            <div className="flex items-center gap-2">
              {selectedDocument && (
                <>
                  <button className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2">
                    <Scan className="w-4 h-4" />
                    Extract Text
                  </button>
                  <button className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 flex items-center gap-2">
                    <Copy className="w-4 h-4" />
                    Copy
                  </button>
                  <button className="p-2 border border-gray-300 rounded-lg hover:bg-gray-50">
                    <Download className="w-5 h-5 text-gray-600" />
                  </button>
                </>
              )}
            </div>
          </div>
        </div>

        {/* Main Area */}
        <div className="flex-1 flex overflow-hidden">
          
          {/* Document Preview */}
          <div className="flex-1 p-6 overflow-auto">
            {selectedDocument ? (
              <div className="space-y-4">
                {/* Document Preview */}
                <div className="bg-white rounded-lg shadow-lg p-6">
                  <h3 className="font-semibold text-gray-900 mb-3">Document Preview</h3>
                  <div className="bg-gray-100 rounded-lg aspect-[8.5/11] flex items-center justify-center">
                    <div className="text-center">
                      <div className="text-6xl mb-2">{selectedDocument.thumbnail}</div>
                      <p className="text-sm text-gray-600">{selectedDocument.name}</p>
                      {selectedDocument.pages && (
                        <p className="text-xs text-gray-500">{selectedDocument.pages} pages</p>
                      )}
                    </div>
                  </div>
                </div>

                {/* Extracted Text */}
                {selectedDocument.extracted && (
                  <div className="bg-white rounded-lg shadow-lg p-6">
                    <div className="flex items-center justify-between mb-3">
                      <h3 className="font-semibold text-gray-900">Extracted Text</h3>
                      <div className="flex items-center gap-2 text-sm text-gray-600">
                        <Languages className="w-4 h-4" />
                        <span>{selectedDocument.language}</span>
                        <span>•</span>
                        <span className="text-green-600 font-medium">{(selectedDocument.confidence * 100).toFixed(1)}% confidence</span>
                      </div>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-4 font-mono text-sm whitespace-pre-wrap max-h-96 overflow-auto border border-gray-200">
                      {extractedText}
                    </div>
                  </div>
                )}

                {/* Processing Status */}
                {selectedDocument.status === 'processing' && (
                  <div className="bg-white rounded-lg shadow-lg p-6">
                    <div className="flex items-center gap-3">
                      <div className="animate-spin">
                        <Scan className="w-6 h-6 text-blue-600" />
                      </div>
                      <div className="flex-1">
                        <p className="font-medium text-gray-900">Extracting text... {selectedDocument.progress}%</p>
                        <div className="w-full bg-gray-200 rounded-full h-2 mt-2">
                          <div 
                            className="bg-blue-600 h-2 rounded-full transition-all"
                            style={{ width: `${selectedDocument.progress}%` }}
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
                  <FileText className="w-16 h-16 mx-auto mb-4 opacity-50" />
                  <p className="text-lg font-medium">No document selected</p>
                  <p className="text-sm mt-1">Choose a document to extract text</p>
                </div>
              </div>
            )}
          </div>

          {/* Right Sidebar - Extraction Results */}
          <div className="w-96 bg-white border-l border-gray-200 overflow-auto">
            
            {/* Structured Fields */}
            {selectedDocument?.extracted && (
              <div className="p-4 border-b border-gray-200">
                <h3 className="font-bold text-gray-900 mb-3">Extracted Fields</h3>
                <div className="space-y-2">
                  {extractedFields.map((field, idx) => (
                    <div key={idx} className="p-3 bg-gray-50 rounded-lg">
                      <div className="flex items-center justify-between mb-1">
                        <span className="text-sm font-medium text-gray-700">{field.field}</span>
                        <span className="text-xs text-gray-500">{(field.confidence * 100).toFixed(0)}%</span>
                      </div>
                      <p className="font-semibold text-gray-900">{field.value}</p>
                      <div className="w-full bg-gray-200 rounded-full h-1 mt-1">
                        <div 
                          className="bg-green-600 h-1 rounded-full"
                          style={{ width: `${field.confidence * 100}%` }}
                        ></div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Extraction Options */}
            <div className="p-4">
              <h3 className="font-bold text-gray-900 mb-3">Extraction Options</h3>
              <div className="space-y-3">
                <label className="flex items-center gap-2">
                  <input type="checkbox" defaultChecked className="rounded" />
                  <span className="text-sm text-gray-700">Auto-detect language</span>
                </label>
                <label className="flex items-center gap-2">
                  <input type="checkbox" defaultChecked className="rounded" />
                  <span className="text-sm text-gray-700">Extract tables</span>
                </label>
                <label className="flex items-center gap-2">
                  <input type="checkbox" className="rounded" />
                  <span className="text-sm text-gray-700">Preserve formatting</span>
                </label>
                <label className="flex items-center gap-2">
                  <input type="checkbox" defaultChecked className="rounded" />
                  <span className="text-sm text-gray-700">Auto-correct errors</span>
                </label>
                <label className="flex items-center gap-2">
                  <input type="checkbox" className="rounded" />
                  <span className="text-sm text-gray-700">Extract metadata</span>
                </label>
              </div>

              <div className="mt-6">
                <label className="text-sm font-medium text-gray-700 mb-2 block">Output Format</label>
                <select className="w-full px-3 py-2 border border-gray-300 rounded-lg">
                  <option>Plain Text</option>
                  <option>JSON</option>
                  <option>Markdown</option>
                  <option>CSV</option>
                </select>
              </div>

              <div className="mt-6">
                <label className="text-sm font-medium text-gray-700 mb-2 block">Language</label>
                <select className="w-full px-3 py-2 border border-gray-300 rounded-lg">
                  <option>Auto-detect</option>
                  <option>English</option>
                  <option>Spanish</option>
                  <option>French</option>
                  <option>German</option>
                  <option>Chinese</option>
                </select>
              </div>
            </div>

          </div>

        </div>
      </div>

    </div>
  )
}
