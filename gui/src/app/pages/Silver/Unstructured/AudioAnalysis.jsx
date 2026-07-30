import { useState, useEffect, useCallback } from 'react'
import { 
  Music, Play, Pause, SkipForward, Volume2, 
  Mic, MessageSquare, Download, Upload, Activity,
  Clock, Languages, User, Tag, FileAudio, RefreshCw
} from 'lucide-react'

const API_BASE = 'http://localhost:8000'

export default function AudioAnalysis() {
  const [selectedAudio, setSelectedAudio] = useState(null)
  const [isPlaying, setIsPlaying] = useState(false)
  const [analysisType, setAnalysisType] = useState('transcription') // 'transcription', 'sentiment', 'speaker'
  const [liveAudioFiles, setLiveAudioFiles] = useState([])
  const [loadingFiles, setLoadingFiles] = useState(false)
  const [hasFetched, setHasFetched] = useState(false)

  const fetchAudioFiles = useCallback(async () => {
    setLoadingFiles(true)
    try {
      const res = await fetch(`${API_BASE}/api/silver/unstructured/preview/audio?domain=media&entity=assets&limit=20`)
      if (res.ok) {
        const data = await res.json()
        const mapped = (data.records || []).map((r, i) => ({
          id: r.file_name || i,
          name: r.file_name || `audio_${i}.wav`,
          duration: r.duration_seconds ? `${Math.floor(r.duration_seconds / 60)}:${String(Math.round(r.duration_seconds % 60)).padStart(2, '0')}` : '—',
          thumbnail: '🎵',
          status: r.processing_status || 'pending',
          transcription: !!r.extracted_text,
          speakers: r.channels || 1,
          sentiment: r.sentiment_label || 'neutral',
          words: r.word_count || 0,
          confidence: r.confidence || null,
          sample_rate: r.sample_rate_hz,
          is_silent: r.is_silent,
          avg_volume: r.average_volume_db,
        }))
        setLiveAudioFiles(mapped)
      }
    } catch (err) {
      console.warn('Could not fetch audio preview:', err)
    } finally {
      setHasFetched(true)
      setLoadingFiles(false)
    }
  }, [])

  useEffect(() => { fetchAudioFiles() }, [fetchAudioFiles])

  const audioFiles = liveAudioFiles

  const sampleTranscript = [
    { id: 1, speaker: 'Agent', timestamp: '00:05', text: 'Thank you for calling customer support. How can I help you today?', sentiment: 'neutral' },
    { id: 2, speaker: 'Customer', timestamp: '00:12', text: 'Hi, I have a question about my recent order. The tracking shows it was delivered but I haven\'t received it.', sentiment: 'concerned' },
    { id: 3, speaker: 'Agent', timestamp: '00:22', text: 'I apologize for the inconvenience. Let me look into that for you right away. Can you provide your order number?', sentiment: 'helpful' },
    { id: 4, speaker: 'Customer', timestamp: '00:30', text: 'Sure, it\'s ORDER-2024-0312.', sentiment: 'neutral' },
    { id: 5, speaker: 'Agent', timestamp: '00:38', text: 'Thank you. I can see the package was marked as delivered yesterday at 2:30 PM. Sometimes carriers leave packages in safe locations. Have you checked around your property?', sentiment: 'helpful' },
    { id: 6, speaker: 'Customer', timestamp: '00:52', text: 'Oh! I just remembered - I have a secure delivery box. Let me check there. Yes, I found it! Sorry for the confusion.', sentiment: 'happy' },
    { id: 7, speaker: 'Agent', timestamp: '01:05', text: 'No problem at all! I\'m glad we found it. Is there anything else I can help you with today?', sentiment: 'positive' },
    { id: 8, speaker: 'Customer', timestamp: '01:12', text: 'No, that\'s all. Thank you so much for your help!', sentiment: 'satisfied' }
  ]

  const speakerAnalysis = [
    { speaker: 'Agent', duration: '1:45', words: 215, talkTime: '47%', sentiment: 'positive' },
    { speaker: 'Customer', duration: '2:00', words: 327, talkTime: '53%', sentiment: 'neutral' }
  ]

  const keywords = [
    { word: 'order', count: 3, relevance: 0.95 },
    { word: 'delivered', count: 2, relevance: 0.92 },
    { word: 'package', count: 2, relevance: 0.88 },
    { word: 'help', count: 2, relevance: 0.85 },
    { word: 'tracking', count: 1, relevance: 0.80 }
  ]

  const analysisTypes = [
    { id: 'transcription', label: 'Transcription', icon: MessageSquare },
    { id: 'sentiment', label: 'Sentiment', icon: Tag },
    { id: 'speaker', label: 'Speaker ID', icon: User }
  ]

  const getStatusColor = (status) => {
    switch(status) {
      case 'transcribed': return 'text-green-600 bg-green-50'
      case 'processing': return 'text-blue-600 bg-blue-50'
      case 'pending': return 'text-gray-600 bg-gray-50'
      case 'failed': return 'text-red-600 bg-red-50'
      default: return 'text-gray-600 bg-gray-50'
    }
  }

  const getSentimentColor = (sentiment) => {
    switch(sentiment) {
      case 'positive':
      case 'happy':
      case 'satisfied':
        return 'text-green-600 bg-green-50'
      case 'negative':
      case 'angry':
        return 'text-red-600 bg-red-50'
      case 'neutral':
        return 'text-gray-600 bg-gray-50'
      case 'concerned':
        return 'text-orange-600 bg-orange-50'
      default:
        return 'text-gray-600 bg-gray-50'
    }
  }

  return (
    <div className="h-full flex bg-gray-50">
      
      {/* Left Sidebar - Audio Files */}
      <div className="w-80 bg-white border-r border-gray-200 flex flex-col">
        <div className="p-4 border-b border-gray-200">
          <div className="flex items-center justify-between mb-4">
            <h2 className="font-bold text-gray-900">Audio Files {liveAudioFiles.length > 0 && <span className="text-xs text-green-600 font-normal ml-1">● live</span>}</h2>
            <div className="flex items-center gap-1">
              <button onClick={fetchAudioFiles} disabled={loadingFiles} className="p-1.5 text-gray-500 hover:text-gray-700 rounded-lg hover:bg-gray-100 disabled:opacity-50">
                <RefreshCw className={`w-4 h-4 ${loadingFiles ? 'animate-spin' : ''}`} />
              </button>
              <button className="p-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700">
                <Upload className="w-4 h-4" />
              </button>
            </div>
          </div>
          
          {/* Analysis Type Selector */}
          <div className="mb-4">
            <label className="text-sm font-medium text-gray-700 mb-2 block">Analysis Type</label>
            <select 
              value={analysisType}
              onChange={(e) => setAnalysisType(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg"
            >
              {analysisTypes.map(type => (
                <option key={type.id} value={type.id}>{type.label}</option>
              ))}
            </select>
          </div>
        </div>

        <div className="flex-1 overflow-auto p-4 space-y-3">
          {loadingFiles && !hasFetched ? (
            <div className="flex items-center justify-center h-32 text-gray-400 text-sm">Loading audio files…</div>
          ) : audioFiles.length === 0 && hasFetched ? (
            <div className="flex flex-col items-center justify-center h-32 text-gray-400">
              <Music className="w-10 h-10 mb-2 opacity-30" />
              <p className="text-xs text-center">No audio files found. Run a Feature Pipeline job to ingest audio.</p>
            </div>
          ) : null}
          {audioFiles.map(audio => (
            <div
              key={audio.id}
              onClick={() => setSelectedAudio(audio)}
              className={`p-3 rounded-lg border cursor-pointer transition-all ${
                selectedAudio?.id === audio.id
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-200 bg-white hover:border-gray-300'
              }`}
            >
              <div className="flex items-start gap-3">
                <div className="w-12 h-12 bg-gray-100 rounded flex items-center justify-center text-2xl">
                  {audio.thumbnail}
                </div>
                <div className="flex-1 min-w-0">
                  <p className="font-medium text-sm text-gray-900 truncate">{audio.name}</p>
                  <div className="flex items-center gap-2 mt-1">
                    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${getStatusColor(audio.status)}`}>
                      {audio.status}
                    </span>
                    <span className="text-xs text-gray-600">{audio.duration}</span>
                  </div>
                  {audio.transcription && (
                    <div className="mt-1 flex items-center gap-2 text-xs text-gray-600">
                      <span>{audio.speakers} speakers</span>
                      <span>•</span>
                      <span>{audio.words} words</span>
                    </div>
                  )}
                  {audio.progress && (
                    <div className="mt-2">
                      <div className="w-full bg-gray-200 rounded-full h-1.5">
                        <div 
                          className="bg-blue-600 h-1.5 rounded-full transition-all"
                          style={{ width: `${audio.progress}%` }}
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
              <h1 className="text-xl font-bold text-gray-900">Audio Analysis & Transcription</h1>
              <p className="text-sm text-gray-600 mt-1">
                {selectedAudio ? selectedAudio.name : 'Select an audio file to analyze'}
              </p>
            </div>
            <div className="flex items-center gap-2">
              {selectedAudio && (
                <>
                  <button className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2">
                    <Mic className="w-4 h-4" />
                    Run Analysis
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
          
          {/* Transcript/Analysis Area */}
          <div className="flex-1 p-6 overflow-auto">
            {selectedAudio ? (
              <div className="space-y-4">
                
                {/* Audio Player */}
                <div className="bg-white rounded-lg shadow-lg p-6">
                  <div className="flex items-center gap-4 mb-4">
                    <div className="w-20 h-20 bg-gradient-to-br from-purple-500 to-pink-500 rounded-lg flex items-center justify-center text-white text-3xl">
                      {selectedAudio.thumbnail}
                    </div>
                    <div className="flex-1">
                      <h3 className="font-bold text-gray-900">{selectedAudio.name}</h3>
                      <p className="text-sm text-gray-600">Duration: {selectedAudio.duration}</p>
                    </div>
                  </div>

                  {/* Waveform Visualization */}
                  <div className="bg-gray-100 rounded-lg h-24 mb-4 flex items-center justify-center">
                    <div className="flex items-end gap-1 h-16">
                      {[...Array(50)].map((_, idx) => (
                        <div 
                          key={idx}
                          className="w-1 bg-blue-500 rounded-t"
                          style={{ height: `${Math.random() * 100}%` }}
                        ></div>
                      ))}
                    </div>
                  </div>

                  {/* Audio Controls */}
                  <div className="flex items-center gap-4">
                    <button 
                      onClick={() => setIsPlaying(!isPlaying)}
                      className="p-3 bg-blue-600 text-white rounded-full hover:bg-blue-700"
                    >
                      {isPlaying ? <Pause className="w-5 h-5" /> : <Play className="w-5 h-5" />}
                    </button>
                    <button className="p-2 text-gray-600 hover:text-gray-900">
                      <SkipForward className="w-5 h-5" />
                    </button>
                    <div className="flex-1">
                      <input type="range" className="w-full" min="0" max="100" defaultValue="0" />
                    </div>
                    <span className="text-sm text-gray-600">0:00 / {selectedAudio.duration}</span>
                    <button className="p-2 text-gray-600 hover:text-gray-900">
                      <Volume2 className="w-5 h-5" />
                    </button>
                  </div>
                </div>

                {/* Transcription */}
                {selectedAudio.transcription && (
                  <div className="bg-white rounded-lg shadow-lg p-6">
                    <div className="flex items-center justify-between mb-4">
                      <h3 className="font-semibold text-gray-900">Transcription</h3>
                      <div className="flex items-center gap-2 text-sm text-gray-600">
                        {selectedAudio.confidence != null && (
                          <span className="text-green-600 font-medium">{(selectedAudio.confidence * 100).toFixed(1)}% confidence</span>
                        )}
                      </div>
                    </div>
                    <div className="space-y-3 max-h-96 overflow-auto">
                      {sampleTranscript.map(line => (
                        <div key={line.id} className="flex gap-3">
                          <div className="flex-shrink-0 w-24 text-right">
                            <span className="text-xs text-gray-500">{line.timestamp}</span>
                          </div>
                          <div className="flex-1">
                            <div className="flex items-center gap-2 mb-1">
                              <span className="font-semibold text-sm text-gray-900">{line.speaker}</span>
                              <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${getSentimentColor(line.sentiment)}`}>
                                {line.sentiment}
                              </span>
                            </div>
                            <p className="text-gray-700">{line.text}</p>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* Processing Status */}
                {selectedAudio.status === 'processing' && (
                  <div className="bg-white rounded-lg shadow-lg p-6">
                    <div className="flex items-center gap-3">
                      <div className="animate-spin">
                        <Mic className="w-6 h-6 text-blue-600" />
                      </div>
                      <div className="flex-1">
                        <p className="font-medium text-gray-900">Analyzing audio... {selectedAudio.progress}%</p>
                        <div className="w-full bg-gray-200 rounded-full h-2 mt-2">
                          <div 
                            className="bg-blue-600 h-2 rounded-full transition-all"
                            style={{ width: `${selectedAudio.progress}%` }}
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
                  <Music className="w-16 h-16 mx-auto mb-4 opacity-50" />
                  <p className="text-lg font-medium">No audio selected</p>
                  <p className="text-sm mt-1">Choose an audio file to analyze</p>
                </div>
              </div>
            )}
          </div>

          {/* Right Sidebar - Analysis Results */}
          <div className="w-80 bg-white border-l border-gray-200 overflow-auto">
            
            {/* Speaker Analysis */}
            {selectedAudio?.transcription && (
              <>
                <div className="p-4 border-b border-gray-200">
                  <h3 className="font-bold text-gray-900 mb-3">Speaker Analysis</h3>
                  <div className="space-y-3">
                    {speakerAnalysis.map((speaker, idx) => (
                      <div key={idx} className="p-3 bg-gray-50 rounded-lg">
                        <div className="flex items-center justify-between mb-2">
                          <span className="font-semibold text-gray-900">{speaker.speaker}</span>
                          <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${getSentimentColor(speaker.sentiment)}`}>
                            {speaker.sentiment}
                          </span>
                        </div>
                        <div className="grid grid-cols-2 gap-2 text-xs text-gray-600">
                          <div>
                            <p className="text-gray-500">Talk Time</p>
                            <p className="font-semibold text-gray-900">{speaker.talkTime}</p>
                          </div>
                          <div>
                            <p className="text-gray-500">Words</p>
                            <p className="font-semibold text-gray-900">{speaker.words}</p>
                          </div>
                        </div>
                        <div className="w-full bg-gray-200 rounded-full h-2 mt-2">
                          <div 
                            className="bg-blue-600 h-2 rounded-full"
                            style={{ width: speaker.talkTime }}
                          ></div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>

                {/* Keywords */}
                <div className="p-4 border-b border-gray-200">
                  <h3 className="font-bold text-gray-900 mb-3">Keywords</h3>
                  <div className="space-y-2">
                    {keywords.map((keyword, idx) => (
                      <div key={idx} className="flex items-center justify-between p-2 bg-gray-50 rounded">
                        <span className="font-medium text-gray-900">{keyword.word}</span>
                        <div className="flex items-center gap-2">
                          <span className="text-sm text-gray-600">{keyword.count}x</span>
                          <span className="text-xs text-gray-500">{(keyword.relevance * 100).toFixed(0)}%</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </>
            )}

            {/* Analysis Options */}
            <div className="p-4">
              <h3 className="font-bold text-gray-900 mb-3">Analysis Options</h3>
              <div className="space-y-3">
                <label className="flex items-center gap-2">
                  <input type="checkbox" defaultChecked className="rounded" />
                  <span className="text-sm text-gray-700">Speaker diarization</span>
                </label>
                <label className="flex items-center gap-2">
                  <input type="checkbox" defaultChecked className="rounded" />
                  <span className="text-sm text-gray-700">Sentiment analysis</span>
                </label>
                <label className="flex items-center gap-2">
                  <input type="checkbox" className="rounded" />
                  <span className="text-sm text-gray-700">Keyword extraction</span>
                </label>
                <label className="flex items-center gap-2">
                  <input type="checkbox" defaultChecked className="rounded" />
                  <span className="text-sm text-gray-700">Punctuation</span>
                </label>
              </div>

              <div className="mt-6">
                <label className="text-sm font-medium text-gray-700 mb-2 block">Language</label>
                <select className="w-full px-3 py-2 border border-gray-300 rounded-lg">
                  <option>Auto-detect</option>
                  <option>English</option>
                  <option>Spanish</option>
                  <option>French</option>
                  <option>Mandarin</option>
                </select>
              </div>

              <div className="mt-6">
                <label className="text-sm font-medium text-gray-700 mb-2 block">Model</label>
                <select className="w-full px-3 py-2 border border-gray-300 rounded-lg">
                  <option>Whisper Large v3</option>
                  <option>Whisper Medium</option>
                  <option>Azure Speech</option>
                  <option>Google Speech-to-Text</option>
                </select>
              </div>
            </div>

          </div>

        </div>
      </div>

    </div>
  )
}
