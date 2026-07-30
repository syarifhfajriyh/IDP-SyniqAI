import { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Database, TestTube, Play, CheckCircle, AlertCircle, Loader2, ArrowRight, ArrowLeft, Terminal, ChevronDown, ChevronUp, Upload, Cloud, FileJson, HelpCircle } from 'lucide-react';
import axios from 'axios';

const API_BASE = 'http://localhost:8000/api';

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

// Data source options with real logos
const DATA_SOURCES = [
  {
    id: 'postgres',
    name: 'PostgreSQL',
    icon: Database,
    description: 'Relational database with SQL support',
    color: 'blue',
    logo: 'https://www.postgresql.org/media/img/about/press/elephant.png'
  },
  {
    id: 'mariadb',
    name: 'MariaDB',
    icon: Database,
    description: 'MySQL-compatible relational database',
    color: 'orange',
    logo: 'https://mariadb.com/wp-content/uploads/2019/11/mariadb-logo-vert_blue-transparent.png',
    hasCloudOption: true
  },
  {
    id: 's3',
    name: 'Amazon S3',
    icon: Cloud,
    description: 'Object storage service',
    color: 'purple',
    logo: 'https://upload.wikimedia.org/wikipedia/commons/b/bc/Amazon-S3-Logo.svg'
  },
  {
    id: 'mongodb',
    name: 'MongoDB',
    icon: FileJson,
    description: 'NoSQL document database',
    color: 'green',
    logo: 'https://www.mongodb.com/assets/images/global/leaf.png'
  }
];

export default function Ingestion() {
  const { domain } = useParams();
  const navigate = useNavigate();
  const [step, setStep] = useState(1);
  const [selectedSource, setSelectedSource] = useState(null);
  const [isMariaDBCloud, setIsMariaDBCloud] = useState(false);
  const [connectionConfig, setConnectionConfig] = useState({
    // Database fields
    host: '',
    port: '',
    database: '',
    user: '',
    password: '',
    ssl_ca: '',
    // S3 fields
    s3_bucket: '',
    s3_prefix: '',
    aws_access_key: '',
    aws_secret_key: '',
    aws_region: '',
    // MongoDB fields
    mongo_uri: '',
    collection: '',
    query: '{}',
    flatten_documents: true
  });
  const [testResult, setTestResult] = useState(null);
  const [availableTables, setAvailableTables] = useState([]);
  const [selectedEntity, setSelectedEntity] = useState('');
  const [chunkSize, setChunkSize] = useState(10000);
  const [enableCDC, setEnableCDC] = useState(false);  // NEW: CDC option
  const [jobId, setJobId] = useState(null);
  const [jobStatus, setJobStatus] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  
  // Backend logs viewer
  const [logs, setLogs] = useState([]);
  const [showLogs, setShowLogs] = useState(true);
  
  // Helper to add log entries
  const addLog = (type, message, data = null) => {
    const timestamp = new Date().toLocaleTimeString();
    setLogs(prev => [...prev, { timestamp, type, message, data }]);
  };

  // Test connection
  const handleTestConnection = async () => {
    if (!selectedSource) return;
    
    setLoading(true);
    setError(null);
    setTestResult(null);
    
    // Determine actual source type (handle mariadb cloud)
    let actualSourceType = selectedSource.id;
    if (selectedSource.id === 'mariadb' && isMariaDBCloud) {
      actualSourceType = 'mariadb_cloud';
    }
    
    // Build request data based on source type
    let requestData = {
      source_type: actualSourceType
    };
    
    // Add source-specific fields only
    if (selectedSource.id === 'mongodb') {
      requestData = {
        ...requestData,
        mongo_uri: connectionConfig.mongo_uri,
        database: connectionConfig.database,
        collection: connectionConfig.collection,
        query: connectionConfig.query || '{}',
        chunk_size: chunkSize || 10000,
        flatten_documents: connectionConfig.flatten_documents
      };
    } else if (selectedSource.id === 's3') {
      requestData = {
        ...requestData,
        s3_bucket: connectionConfig.s3_bucket,
        s3_prefix: connectionConfig.s3_prefix,
        aws_access_key: connectionConfig.aws_access_key,
        aws_secret_key: connectionConfig.aws_secret_key,
        aws_region: connectionConfig.aws_region
      };
    } else {
      // Database sources (postgres, mariadb, mariadb_cloud)
      requestData = {
        ...requestData,
        host: connectionConfig.host,
        port: connectionConfig.port,
        database: connectionConfig.database,
        user: connectionConfig.user,
        password: connectionConfig.password,
        ssl_ca: connectionConfig.ssl_ca
      };
    }
    
    console.log('[Ingestion] Testing connection with data:', requestData);
    addLog('info', `Testing ${selectedSource.name} connection...`, requestData);
    
    try {
      console.log('[Ingestion] Sending POST request to:', `${API_BASE}/connection/test`);
      const response = await axios.post(`${API_BASE}/connection/test`, requestData);
      
      console.log('[Ingestion] Response received:', response.data);
      addLog('success', 'Connection test response received', response.data);
      
      setTestResult(response.data);
      if (response.data.success) {
        setAvailableTables(response.data.available_tables || []);
        addLog('success', `✓ Found ${response.data.available_tables?.length || 0} ${selectedSource.id === 'mongodb' ? 'collections' : 'tables'}`);
        // Automatically advance to step 4 (table selection) on successful connection
        setTimeout(() => {
          setStep(4);
        }, 1000); // Brief delay to show success message
      } else {
        // Show detailed error from backend
        const errorDetails = [];
        if (response.data.message) errorDetails.push(response.data.message);
        if (response.data.error) errorDetails.push(`Details: ${response.data.error}`);
        if (response.data.error_type) errorDetails.push(`Type: ${response.data.error_type}`);
        const errorMsg = errorDetails.join(' | ');
        setError(errorMsg);
        addLog('error', '✗ Connection failed', response.data);
      }
    } catch (err) {
      console.error('[Ingestion] Connection test error:', err);
      console.error('[Ingestion] Error response:', err.response);
      const errorMsg = err.response?.data?.error || err.response?.data?.detail || err.message || 'Connection test failed';
      setError(errorMsg);
      addLog('error', `✗ Exception: ${errorMsg}`, {
        status: err.response?.status,
        data: err.response?.data,
        message: err.message
      });
    } finally {
      setLoading(false);
    }
  };

  // Start ingestion
  const handleStartIngestion = async () => {
    if (!selectedEntity) {
      setError('Please select a table/entity to ingest');
      return;
    }
    
    setLoading(true);
    setError(null);
    
    const requestData = {
      source_type: selectedSource.id,
      connection_config: connectionConfig,
      extraction_request: {
        entity: selectedEntity,
        chunk_size: chunkSize
      },
      domain: domain || 'general',  // Include domain from URL params
      enable_cdc: enableCDC  // NEW: Include CDC flag
    };
    
    addLog('info', `Starting ingestion for ${selectedEntity}${enableCDC ? ' (CDC enabled)' : ''}...`, requestData);
    
    try {
      const response = await axios.post(`${API_BASE}/ingestion/start`, requestData);
      
      addLog('success', `✓ Ingestion started (Job ID: ${response.data.job_id})`);
      
      setJobId(response.data.job_id);
      setStep(6); // Move to monitoring step
      startStatusPolling(response.data.job_id);
    } catch (err) {
      const errorMsg = err.response?.data?.detail || 'Failed to start ingestion';
      setError(errorMsg);
      addLog('error', `✗ Failed to start ingestion: ${errorMsg}`, err.response?.data);
      setLoading(false);
    }
  };

  // Poll job status
  const startStatusPolling = (id) => {
    addLog('info', 'Starting status polling...');
    const interval = setInterval(async () => {
      try {
        const response = await axios.get(`${API_BASE}/ingestion/status/${id}`);
        setJobStatus(response.data);
        
        if (response.data.status === 'completed' || response.data.status === 'failed') {
          clearInterval(interval);
          setLoading(false);
          addLog(response.data.status === 'completed' ? 'success' : 'error', 
            `Ingestion ${response.data.status}`, response.data);
        }
      } catch (err) {
        console.error('Error polling status:', err);
        clearInterval(interval);
        setLoading(false);
        addLog('error', 'Status polling failed', err.message);
      }
    }, 2000);
  };

  // Reset wizard
  const handleReset = () => {
    addLog('info', 'Wizard reset - ready for new ingestion');
    setStep(1);
    setSelectedSource(null);
    setConnectionConfig({
      // Database fields
      host: '',
      port: '',
      database: '',
      user: '',
      password: '',
      ssl_ca: '',
      // S3 fields
      s3_bucket: '',
      s3_prefix: '',
      aws_access_key: '',
      aws_secret_key: '',
      aws_region: ''
    });
    setTestResult(null);
    setAvailableTables([]);
    setSelectedEntity('');
    setJobId(null);
    setJobStatus(null);
    setError(null);
  };

  return (
    <div className="p-6">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-800">Data Ingestion</h1>
        <p className="text-gray-600">Import data from external sources into the Bronze layer</p>
      </div>

      {/* Progress Steps */}
      <div className="mb-8 flex items-center justify-center">
        {[
          { num: 1, label: 'Source' },
          { num: 2, label: 'Configure' },
          { num: 3, label: 'Test' },
          { num: 4, label: 'Select' },
          { num: 5, label: 'Ingest' },
          { num: 6, label: 'Monitor' }
        ].map((s, idx) => (
          <div key={s.num} className="flex items-center">
            <div className={`flex items-center justify-center w-10 h-10 rounded-full border-2 
              ${step >= s.num ? 'bg-blue-600 border-blue-600 text-white' : 'bg-white border-gray-300 text-gray-400'}`}>
              {step > s.num ? <CheckCircle className="w-5 h-5" /> : <span>{s.num}</span>}
            </div>
            <div className={`ml-2 text-sm ${step >= s.num ? 'text-blue-600 font-medium' : 'text-gray-400'}`}>
              {s.label}
            </div>
            {idx < 5 && <ArrowRight className="w-5 h-5 mx-4 text-gray-300" />}
          </div>
        ))}
      </div>

      {/* Error Display */}
      {error && (
        <div className="mb-6 bg-red-50 border border-red-200 rounded-lg p-4 flex items-start">
          <AlertCircle className="w-5 h-5 text-red-500 mr-3 flex-shrink-0 mt-0.5" />
          <div className="flex-1">
            <p className="text-red-800 font-medium">Error</p>
            <p className="text-red-700 text-sm">{error}</p>
          </div>
        </div>
      )}

      {/* Step 1: Source Selection */}
      {step === 1 && (
        <div>
          <div className="flex items-center mb-4">
            <h2 className="text-xl font-semibold">Select Data Source</h2>
            <HelpTooltip title="Choose Your Data Source">
              Select the type of data source you want to connect to. 
              Each source has specific connection requirements and capabilities.
            </HelpTooltip>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {DATA_SOURCES.map(source => {
              const Icon = source.icon;
              return (
                <button
                  key={source.id}
                  onClick={() => {
                    setSelectedSource(source);
                    setIsMariaDBCloud(false);  // Reset MariaDB cloud selection
                    setStep(2);
                  }}
                  className={`p-6 rounded-lg border-2 transition-all hover:shadow-lg
                    ${selectedSource?.id === source.id 
                      ? 'border-blue-600 bg-blue-50' 
                      : 'border-gray-200 bg-white hover:border-blue-300'}`}
                >
                  <div className="relative w-12 h-12 mb-3">
                    <img 
                      src={source.logo} 
                      alt={source.name}
                      className="w-full h-full object-contain"
                      onError={(e) => {
                        e.target.style.display = 'none'
                        e.target.nextSibling.style.display = 'block'
                      }}
                    />
                    <Icon className={`w-12 h-12 text-${source.color}-600`} style={{display: 'none'}} />
                  </div>
                  <h3 className="font-semibold text-gray-800 mb-2">{source.name}</h3>
                  <p className="text-sm text-gray-600">{source.description}</p>
                </button>
              );
            })}
          </div>
        </div>
      )}

      {/* Step 2: Connection Configuration */}
      {step === 2 && selectedSource && (
        <div>
          <div className="flex items-center mb-4">
            <h2 className="text-xl font-semibold">Configure {selectedSource.name} Connection</h2>
            <HelpTooltip title="Connection Details">
              Enter your connection credentials here. The system will test the connection 
              automatically when you click "Test Connection". Make sure all required fields 
              are filled correctly.
            </HelpTooltip>
          </div>
          <div className="bg-white rounded-lg border border-gray-200 p-6 max-w-2xl">
            
            {/* MariaDB Deployment Type Selector */}
            {selectedSource.hasCloudOption && (
              <div className="mb-6 pb-6 border-b border-gray-200">
                <label className="block text-sm font-medium text-gray-700 mb-3">
                  Deployment Type
                  <HelpTooltip title="Choose Deployment">
                    Select whether you're connecting to a self-hosted MariaDB instance or MariaDB SkySQL (cloud).
                    Cloud instances require an SSL certificate.
                  </HelpTooltip>
                </label>
                <div className="grid grid-cols-2 gap-3">
                  <button
                    type="button"
                    onClick={() => {
                      setIsMariaDBCloud(false);
                      setConnectionConfig({ ...connectionConfig, ssl_ca: '' });  // Clear SSL cert when switching to local
                    }}
                    className={`p-4 rounded-lg border-2 transition-all text-left ${
                      !isMariaDBCloud
                        ? 'border-blue-600 bg-blue-50 shadow-sm'
                        : 'border-gray-200 bg-white hover:border-gray-300'
                    }`}
                  >
                    <div className="flex items-center gap-2 mb-2">
                      <Database size={18} className={!isMariaDBCloud ? 'text-blue-600' : 'text-gray-600'} />
                      <span className={`font-semibold ${
                        !isMariaDBCloud ? 'text-blue-900' : 'text-gray-900'
                      }`}>
                        Local / Self-Hosted
                      </span>
                    </div>
                    <p className="text-xs text-gray-600">Standard MariaDB server on-premises or VM</p>
                  </button>
                  
                  <button
                    type="button"
                    onClick={() => setIsMariaDBCloud(true)}
                    className={`p-4 rounded-lg border-2 transition-all text-left ${
                      isMariaDBCloud
                        ? 'border-blue-600 bg-blue-50 shadow-sm'
                        : 'border-gray-200 bg-white hover:border-gray-300'
                    }`}
                  >
                    <div className="flex items-center gap-2 mb-2">
                      <Cloud size={18} className={isMariaDBCloud ? 'text-blue-600' : 'text-gray-600'} />
                      <span className={`font-semibold ${
                        isMariaDBCloud ? 'text-blue-900' : 'text-gray-900'
                      }`}>
                        Cloud (SkySQL)
                      </span>
                    </div>
                    <p className="text-xs text-gray-600">MariaDB SkySQL managed service</p>
                  </button>
                </div>
              </div>
            )}
            
            {/* Connection Test Result Display */}
            {testResult && (
              <div className={`mb-4 p-4 rounded-lg border-2 ${
                testResult.success 
                  ? 'bg-green-50 border-green-200' 
                  : 'bg-red-50 border-red-200'
              }`}>
                <div className="flex items-start">
                  {testResult.success ? (
                    <CheckCircle className="w-5 h-5 text-green-600 mr-3 flex-shrink-0 mt-0.5" />
                  ) : (
                    <AlertCircle className="w-5 h-5 text-red-600 mr-3 flex-shrink-0 mt-0.5" />
                  )}
                  <div className="flex-1">
                    <p className={`font-medium ${testResult.success ? 'text-green-800' : 'text-red-800'}`}>
                      {testResult.message}
                    </p>
                    {testResult.server_version && (
                      <p className="text-sm text-green-700 mt-1">Server Version: {testResult.server_version}</p>
                    )}
                    {availableTables.length > 0 && (
                      <p className="text-sm text-green-700 mt-1">
                        Found {availableTables.length} {selectedSource.id === 'mongodb' ? 'collections' : 'tables'} - Redirecting to table selection...
                      </p>
                    )}
                  </div>
                </div>
              </div>
            )}
            
            {error && (
              <div className="mb-4 p-4 rounded-lg border-2 bg-red-50 border-red-200">
                <div className="flex items-start">
                  <AlertCircle className="w-5 h-5 text-red-600 mr-3 flex-shrink-0 mt-0.5" />
                  <div className="flex-1">
                    <p className="font-medium text-red-800">Connection Failed</p>
                    <p className="text-sm text-red-700 mt-1">{error}</p>
                  </div>
                </div>
              </div>
            )}
            
            {/* MongoDB Configuration Fields */}
            {selectedSource.id === 'mongodb' ? (
              <div className="grid grid-cols-2 gap-4">
                <div className="col-span-2">
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    MongoDB Connection URI
                    <span className="text-red-500 ml-1">*</span>
                  </label>
                  <input
                    type="text"
                    value={connectionConfig.mongo_uri}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, mongo_uri: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent font-mono text-sm"
                    placeholder="mongodb://username:password@localhost:27017/"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Examples: <br/>
                    • Local: mongodb://localhost:27017/<br/>
                    • Atlas: mongodb+srv://user:pass@cluster.mongodb.net/<br/>
                    • On-Prem: mongodb://user:pass@192.168.1.100:27017/
                  </p>
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Database Name
                    <span className="text-red-500 ml-1">*</span>
                  </label>
                  <input
                    type="text"
                    value={connectionConfig.database}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, database: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="media_db"
                  />
                  <p className="text-xs text-gray-500 mt-1">The database to query</p>
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Collection Name
                    <span className="text-red-500 ml-1">*</span>
                  </label>
                  <input
                    type="text"
                    value={connectionConfig.collection}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, collection: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="images_metadata"
                  />
                  <p className="text-xs text-gray-500 mt-1">The collection to ingest</p>
                </div>
                
                <div className="col-span-2">
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Query Filter (JSON)
                  </label>
                  <textarea
                    value={connectionConfig.query}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, query: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent font-mono text-sm"
                    rows={3}
                    placeholder='{"status": "active"}'
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    MongoDB query filter in JSON format. Examples:<br/>
                    • All documents: {'{}'}<br/>
                    • Filter: {'{{"status": "active"}}'}<br/>
                    • Date range: {'{{"created_at": {{"$gte": "2024-01-01"}}}}'}
                  </p>
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Chunk Size
                  </label>
                  <input
                    type="number"
                    value={chunkSize}
                    onChange={(e) => setChunkSize(parseInt(e.target.value))}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="10000"
                  />
                  <p className="text-xs text-gray-500 mt-1">Documents per chunk (default: 10000)</p>
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    Flatten Documents
                  </label>
                  <select
                    value={connectionConfig.flatten_documents}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, flatten_documents: e.target.value === 'true' })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  >
                    <option value="true">Yes - Flatten nested documents</option>
                    <option value="false">No - Keep nested structure</option>
                  </select>
                  <p className="text-xs text-gray-500 mt-1">Convert nested JSON to flat columns</p>
                </div>
              </div>
            ) : selectedSource.id === 's3' ? (
              <div className="grid grid-cols-2 gap-4">
                <div className="col-span-2">
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    S3 Bucket Name
                    <span className="text-red-500 ml-1">*</span>
                  </label>
                  <input
                    type="text"
                    value={connectionConfig.s3_bucket}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, s3_bucket: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="my-bucket-name"
                  />
                  <p className="text-xs text-gray-500 mt-1">The name of your S3 bucket</p>
                </div>
                
                <div className="col-span-2">
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    S3 Prefix (Folder Path)
                  </label>
                  <input
                    type="text"
                    value={connectionConfig.s3_prefix}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, s3_prefix: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="metadata/" 
                  />
                  <p className="text-xs text-gray-500 mt-1">Optional: Filter files by prefix/folder (e.g., data/, or leave empty for root)</p>
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    AWS Access Key ID
                    <span className="text-red-500 ml-1">*</span>
                  </label>
                  <input
                    type="text"
                    value={connectionConfig.aws_access_key}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, aws_access_key: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent font-mono text-sm"
                    placeholder="AKIAIOSFODNN7EXAMPLE"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    AWS Region
                    <span className="text-red-500 ml-1">*</span>
                  </label>
                  <input
                    type="text"
                    value={connectionConfig.aws_region}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, aws_region: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="ap-southeast-1"
                  />
                  <p className="text-xs text-gray-500 mt-1">e.g., us-east-1, ap-southeast-1</p>
                </div>
                
                <div className="col-span-2">
                  <label className="block text-sm font-medium text-gray-700 mb-2">
                    AWS Secret Access Key
                    <span className="text-red-500 ml-1">*</span>
                  </label>
                  <input
                    type="password"
                    value={connectionConfig.aws_secret_key}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, aws_secret_key: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent font-mono text-sm"
                    placeholder="••••••••••••••••••••••••••••••••••••••••"
                  />
                </div>
              </div>
            ) : (
              /* Database Configuration Fields */
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">Host</label>
                  <input
                    type="text"
                    value={connectionConfig.host}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, host: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="localhost"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">Port</label>
                  <input
                    type="number"
                    value={connectionConfig.port}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, port: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="5432"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">Database</label>
                  <input
                    type="text"
                    value={connectionConfig.database}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, database: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="mydb"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">User</label>
                  <input
                    type="text"
                    value={connectionConfig.user}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, user: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="username"
                  />
                </div>
                <div className="col-span-2">
                  <label className="block text-sm font-medium text-gray-700 mb-2">Password</label>
                  <input
                    type="password"
                    value={connectionConfig.password}
                    onChange={(e) => setConnectionConfig({ ...connectionConfig, password: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                    placeholder="••••••••"
                  />
                </div>
                
                {/* SSL Certificate Field (MariaDB Cloud only) */}
                {selectedSource.id === 'mariadb' && isMariaDBCloud && (
                  <div className="col-span-2">
                    <label className="block text-sm font-medium text-gray-700 mb-2">
                      SSL Certificate Path
                      <span className="text-red-500 ml-1">*</span>
                      <span className="text-xs font-normal text-gray-500 ml-2">(Full file path required)</span>
                    </label>
                    <input
                      type="text"
                      value={connectionConfig.ssl_ca || ''}
                      onChange={(e) => setConnectionConfig({ ...connectionConfig, ssl_ca: e.target.value.trim() })}
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent font-mono text-sm"
                      placeholder="C:\Users\YourName\Documents\globalsignrootca.pem"
                    />
                    <p className="text-xs text-gray-500 mt-1">
                      📁 Paste the complete file path to your SSL certificate (.pem file)
                    </p>
                  </div>
                )}
              </div>
            )}
            
            <div className="mt-6 flex justify-between">
              <button
                onClick={() => setStep(1)}
                className="px-4 py-2 text-gray-600 hover:text-gray-800 flex items-center"
              >
                <ArrowLeft className="w-4 h-4 mr-2" />
                Back
              </button>
              <button
                onClick={handleTestConnection}
                disabled={
                  loading || (
                    selectedSource.id === 'mongodb' 
                      ? (!connectionConfig.mongo_uri || !connectionConfig.database || !connectionConfig.collection)
                      : selectedSource.id === 's3'
                      ? (!connectionConfig.s3_bucket || !connectionConfig.aws_access_key || !connectionConfig.aws_secret_key || !connectionConfig.aws_region)
                      : selectedSource.id === 'mariadb' && isMariaDBCloud
                      ? (!connectionConfig.host || !connectionConfig.database || !connectionConfig.user || !connectionConfig.ssl_ca)
                      : (!connectionConfig.host || !connectionConfig.database || !connectionConfig.user)
                  )
                }
                className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed flex items-center"
              >
                {loading ? (
                  <>
                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                    Testing Connection...
                  </>
                ) : testResult?.success ? (
                  <>
                    <CheckCircle className="w-4 h-4 mr-2" />
                    Connected! Proceeding...
                  </>
                ) : (
                  <>
                    <TestTube className="w-4 h-4 mr-2" />
                    Test Connection
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Step 4: Table Selection */}
      {step === 4 && (
        <div>
          <div className="flex items-center mb-4">
            <h2 className="text-xl font-semibold">Select Table/Entity</h2>
            <HelpTooltip title="Choose Data to Ingest">
              Select which table or collection you want to ingest into the Bronze layer. 
              You can ingest multiple tables by running the ingestion process separately for each.
            </HelpTooltip>
          </div>
          <div className="bg-white rounded-lg border border-gray-200 p-6 max-w-2xl">
            <div className="mb-4">
              <label className="block text-sm font-medium text-gray-700 mb-2">Table/Entity</label>
              <select
                value={selectedEntity}
                onChange={(e) => setSelectedEntity(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              >
                <option value="">Select a table...</option>
                {availableTables.map(table => (
                  <option key={table} value={table}>{table}</option>
                ))}
              </select>
            </div>
            
            <div className="mb-4">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Chunk Size (rows per batch)
              </label>
              <input
                type="number"
                value={chunkSize}
                onChange={(e) => setChunkSize(parseInt(e.target.value))}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                min="1000"
                max="100000"
                step="1000"
              />
              <p className="text-sm text-gray-500 mt-1">Recommended: 10,000 for optimal performance</p>
            </div>
            
            <div className="mt-6 flex justify-between">
              <button
                onClick={() => setStep(2)}
                className="px-4 py-2 text-gray-600 hover:text-gray-800 flex items-center"
              >
                <ArrowLeft className="w-4 h-4 mr-2" />
                Back
              </button>
              <button
                onClick={() => setStep(5)}
                disabled={!selectedEntity}
                className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed flex items-center"
              >
                Next
                <ArrowRight className="w-4 h-4 ml-2" />
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Step 5: Start Ingestion */}
      {step === 5 && (
        <div>
          <div className="flex items-center mb-4">
            <h2 className="text-xl font-semibold">Start Ingestion</h2>
            <HelpTooltip title="Begin Data Ingestion">
              Review your settings and start the data ingestion process. 
              The system will fetch data from your source and store it in the Bronze layer. 
              You can monitor progress in real-time.
            </HelpTooltip>
          </div>
          <div className="bg-white rounded-lg border border-gray-200 p-6 max-w-2xl">
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 mb-6">
              <h3 className="font-medium text-blue-900 mb-2">Ingestion Summary</h3>
              <div className="text-sm text-blue-800 space-y-1">
                <p><span className="font-medium">Source:</span> {selectedSource.name}</p>
                <p><span className="font-medium">Database:</span> {connectionConfig.database}</p>
                <p><span className="font-medium">Table:</span> {selectedEntity}</p>
                <p><span className="font-medium">Chunk Size:</span> {chunkSize.toLocaleString()} rows</p>
              </div>
            </div>
            
            {/* CDC Option - Only show for database sources */}
            {(selectedSource.id === 'postgres' || selectedSource.id === 'mariadb' || selectedSource.id === 'mariadb_cloud') && (
              <div className="bg-green-50 border border-green-200 rounded-lg p-4 mb-4">
                <label className="flex items-start cursor-pointer">
                  <input
                    type="checkbox"
                    checked={enableCDC}
                    onChange={(e) => setEnableCDC(e.target.checked)}
                    className="mt-1 mr-3 h-4 w-4 text-green-600 focus:ring-green-500 border-gray-300 rounded"
                  />
                  <div>
                    <div className="font-medium text-green-900 mb-1">
                      Enable Change Data Capture (CDC)
                    </div>
                    <div className="text-sm text-green-700">
                      Automatically monitor this table for real-time changes (INSERT/UPDATE/DELETE). 
                      A CDC connector will be created after ingestion completes.
                    </div>
                  </div>
                </label>
              </div>
            )}
            
            <p className="text-gray-600 mb-4">
              Ready to start ingesting data into the Bronze layer. This process will run in the background 
              and you can monitor its progress in real-time.
            </p>
            
            <div className="mt-6 flex justify-between">
              <button
                onClick={() => setStep(4)}
                className="px-4 py-2 text-gray-600 hover:text-gray-800 flex items-center"
              >
                <ArrowLeft className="w-4 h-4 mr-2" />
                Back
              </button>
              <button
                onClick={handleStartIngestion}
                disabled={loading}
                className="px-6 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:bg-gray-300 flex items-center"
              >
                {loading ? (
                  <>
                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                    Starting...
                  </>
                ) : (
                  <>
                    <Play className="w-4 h-4 mr-2" />
                    Start Ingestion
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Step 6: Monitor Progress */}
      {step === 6 && jobStatus && (
        <div>
          <h2 className="text-xl font-semibold mb-4">Ingestion Progress</h2>
          <div className="bg-white rounded-lg border border-gray-200 p-6 max-w-2xl">
            <div className="mb-6">
              <div className="flex justify-between items-center mb-2">
                <span className="text-sm font-medium text-gray-700">Status</span>
                <span className={`px-3 py-1 rounded-full text-sm font-medium ${
                  jobStatus.status === 'completed' ? 'bg-green-100 text-green-800' :
                  jobStatus.status === 'failed' ? 'bg-red-100 text-red-800' :
                  jobStatus.status === 'running' ? 'bg-blue-100 text-blue-800' :
                  'bg-gray-100 text-gray-800'
                }`}>
                  {jobStatus.status.toUpperCase()}
                </span>
              </div>
              
              {jobStatus.progress && (
                <>
                  <div className="w-full bg-gray-200 rounded-full h-4 mb-2">
                    <div 
                      className="bg-blue-600 h-4 rounded-full transition-all duration-500"
                      style={{ width: `${jobStatus.progress.progress_percent || 0}%` }}
                    ></div>
                  </div>
                  
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <p className="text-gray-600">Rows Processed</p>
                      <p className="font-semibold text-gray-800">
                        {(jobStatus.progress.rows_processed || 0).toLocaleString()}
                        {jobStatus.progress.total_rows && ` / ${jobStatus.progress.total_rows.toLocaleString()}`}
                      </p>
                    </div>
                    <div>
                      <p className="text-gray-600">Chunks Written</p>
                      <p className="font-semibold text-gray-800">
                        {jobStatus.progress.chunks_written || 0}
                      </p>
                    </div>
                    <div>
                      <p className="text-gray-600">Progress</p>
                      <p className="font-semibold text-gray-800">
                        {(jobStatus.progress.progress_percent || 0).toFixed(1)}%
                      </p>
                    </div>
                    <div>
                      <p className="text-gray-600">Duration</p>
                      <p className="font-semibold text-gray-800">
                        {jobStatus.progress.duration_seconds 
                          ? `${Math.floor(jobStatus.progress.duration_seconds)}s` 
                          : '-'}
                      </p>
                    </div>
                  </div>
                </>
              )}
              
              {jobStatus.minio_location && (
                <div className="mt-4 p-3 bg-gray-50 rounded border border-gray-200">
                  <p className="text-sm text-gray-600">MinIO Location</p>
                  <p className="text-sm font-mono text-gray-800">{jobStatus.minio_location}</p>
                </div>
              )}
              
              {jobStatus.error_message && (
                <div className="mt-4 p-3 bg-red-50 rounded border border-red-200">
                  <p className="text-sm font-medium text-red-800">Error</p>
                  <p className="text-sm text-red-700">{jobStatus.error_message}</p>
                </div>
              )}
            </div>
            
            {jobStatus.status === 'completed' && (
              <div className="bg-green-50 border border-green-200 rounded-lg p-4 mb-4">
                <div className="flex items-start">
                  <CheckCircle className="w-5 h-5 text-green-600 mr-3 flex-shrink-0 mt-0.5" />
                  <div>
                    <p className="font-medium text-green-800">Ingestion Complete!</p>
                    <p className="text-sm text-green-700 mt-1">
                      Data has been successfully ingested into the Bronze layer. 
                      You can now process it to Silver or view the raw data.
                    </p>
                  </div>
                </div>
              </div>
            )}
            
            <div className="flex justify-between">
              <button
                onClick={handleReset}
                className="px-4 py-2 text-gray-600 hover:text-gray-800"
              >
                Ingest Another Table
              </button>
              {jobStatus.status === 'completed' && (
                <div className="space-x-2">
                  <button 
                    onClick={() => navigate(`/${domain}/silver`)}
                    className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                  >
                    Process to Silver
                  </button>
                  <button 
                    onClick={() => navigate(`/${domain}/bronze`)}
                    className="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors"
                  >
                    View Data
                  </button>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
      
      {/* Backend Logs Terminal */}
      <div className="mt-8 border-t-2 border-gray-200 pt-4">
        <button
          onClick={() => setShowLogs(!showLogs)}
          className="w-full flex items-center justify-between bg-gray-900 text-white px-4 py-3 rounded-t-lg hover:bg-gray-800 transition-colors"
        >
          <div className="flex items-center">
            <Terminal className="w-5 h-5 mr-2" />
            <span className="font-semibold">Backend Logs & Debugging Terminal</span>
            <span className="ml-3 px-2 py-0.5 bg-green-500 text-xs rounded-full">
              {logs.length} events
            </span>
          </div>
          {showLogs ? <ChevronUp className="w-5 h-5" /> : <ChevronDown className="w-5 h-5" />}
        </button>
        
        {showLogs && (
          <div className="bg-gray-950 rounded-b-lg p-4 max-h-96 overflow-y-auto font-mono text-sm">
            {logs.length === 0 ? (
              <div className="text-gray-500 text-center py-8">
                No logs yet. Test a connection to see backend activity...
              </div>
            ) : (
              <div className="space-y-2">
                {logs.map((log, idx) => (
                  <div key={idx} className="border-l-2 pl-3 py-1" style={{
                    borderColor: log.type === 'error' ? '#ef4444' : log.type === 'success' ? '#10b981' : '#3b82f6'
                  }}>
                    <div className="flex items-start">
                      <span className="text-gray-500 mr-3 text-xs">[{log.timestamp}]</span>
                      <span className={`font-semibold mr-2 text-xs uppercase ${
                        log.type === 'error' ? 'text-red-400' : 
                        log.type === 'success' ? 'text-green-400' : 
                        'text-blue-400'
                      }`}>
                        {log.type}
                      </span>
                      <span className="text-gray-300 flex-1">{log.message}</span>
                    </div>
                    {log.data && (
                      <div className="mt-1 ml-24 text-xs">
                        <details className="cursor-pointer">
                          <summary className="text-gray-500 hover:text-gray-400">
                            View details...
                          </summary>
                          <pre className="mt-2 p-2 bg-gray-900 rounded text-gray-400 overflow-x-auto">
                            {JSON.stringify(log.data, null, 2)}
                          </pre>
                        </details>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
            
            {logs.length > 0 && (
              <div className="mt-4 pt-4 border-t border-gray-800 flex justify-between items-center">
                <button
                  onClick={() => setLogs([])}
                  className="px-3 py-1 bg-red-600 text-white text-xs rounded hover:bg-red-700"
                >
                  Clear Logs
                </button>
                <div className="text-gray-500 text-xs">
                  {logs.filter(l => l.type === 'error').length} errors • {' '}
                  {logs.filter(l => l.type === 'success').length} success • {' '}
                  {logs.filter(l => l.type === 'info').length} info
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
