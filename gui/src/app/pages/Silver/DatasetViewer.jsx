import React, { useState, useEffect } from 'react';
import { Table, Database, Download, RefreshCw, Filter, Eye, Code, BarChart3, History, ChevronDown, Play, Copy, CheckCircle, AlertCircle, Info, Shield, ArrowLeft, Edit2, Trash2, List } from 'lucide-react';
import { useNavigate, useParams, useSearchParams } from 'react-router-dom';
import axios from 'axios';

/**
 * DatasetViewer - Detailed view of a specific dataset
 * Shows schema, sample data, statistics, and history
 */
export default function DatasetViewer({ dataset }) {
  const navigate = useNavigate();
  const { domain } = useParams();
  const [searchParams] = useSearchParams();
  
  const [activeTab, setActiveTab] = useState('preview'); // 'preview', 'schema', 'statistics', 'history', 'lineage'
  const [previewLimit, setPreviewLimit] = useState(100);
  const [showRawSQL, setShowRawSQL] = useState(false);
  const [schemaHistory, setSchemaHistory] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState(null);
  
  // CDC Messages state (for History tab)
  const [cdcMessages, setCdcMessages] = useState([]);
  const [cdcTopics, setCdcTopics] = useState([]);
  const [selectedCdcTopic, setSelectedCdcTopic] = useState('');
  
  // Query validation state
  const [querySQL, setQuerySQL] = useState('');
  const [queryResults, setQueryResults] = useState(null);
  const [queryLoading, setQueryLoading] = useState(false);
  const [queryError, setQueryError] = useState(null);
  const [copied, setCopied] = useState(false);

  // Real data state
  const [previewData, setPreviewData] = useState([]);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [schemaData, setSchemaData] = useState([]);
  const [schemaLoading, setSchemaLoading] = useState(false);
  const [statisticsData, setStatisticsData] = useState([]);
  const [statisticsLoading, setStatisticsLoading] = useState(false);

  // Mock data - replace with actual API calls
  const defaultDataset = dataset || {
    name: 'finance_transactions',
    domain: 'finance',
    source: 'MariaDB',
    layer: 'bronze',
    rowCount: '2.4M',
    size: '1.2 GB',
    columns: 15
  };

  // Log the dataset to verify it's being passed correctly
  useEffect(() => {
    if (dataset) {
      console.log('DatasetViewer received dataset:', dataset);
      console.log('Using dataset for Check Quality:', {
        name: defaultDataset.name,
        domain: defaultDataset.domain,
        source: defaultDataset.source
      });
    }
  }, [dataset]);

  // Don't use hardcoded mock data anymore - these will be empty until fetched
  const schema = schemaData;
  const sampleData = previewData;
  const statistics = statisticsData;

  // Fetch CDC messages from Kafka (like KafkaBacklog component)
  useEffect(() => {
    const fetchCdcTopics = async () => {
      try {
        const response = await axios.get(`http://localhost:8000/api/kafka/cdc/topics`);
        if (response.data.success && response.data.topics.length > 0) {
          setCdcTopics(response.data.topics);
          
          // Auto-select topic matching the table name
          if (defaultDataset.name) {
            let cleanTableName = defaultDataset.name;
            if (cleanTableName.includes('.')) {
              cleanTableName = cleanTableName.split('.').pop();
            }
            cleanTableName = cleanTableName.replace(/_cleaned$|_validated$|_transformed$/, '');
            
            // Find matching topic
            const matchingTopic = response.data.topics.find(topic => 
              topic.toLowerCase().includes(cleanTableName.toLowerCase()) ||
              topic.toLowerCase().includes(defaultDataset.domain?.toLowerCase() || '')
            );
            
            if (matchingTopic) {
              setSelectedCdcTopic(matchingTopic);
            } else if (response.data.topics.length > 0) {
              setSelectedCdcTopic(response.data.topics[0]);
            }
          }
        }
      } catch (error) {
        console.error('Error fetching CDC topics:', error);
      }
    };

    if (activeTab === 'history') {
      fetchCdcTopics();
    }
  }, [activeTab, defaultDataset.name]);

  // Fetch CDC messages when topic is selected
  useEffect(() => {
    const fetchCdcMessages = async () => {
      if (activeTab === 'history' && selectedCdcTopic) {
        setHistoryLoading(true);
        setHistoryError(null);
        
        try {
          console.log(`Fetching CDC messages from topic: ${selectedCdcTopic}`);
          
          const response = await axios.get(`http://localhost:8000/api/kafka/cdc/messages`, {
            params: {
              topic: selectedCdcTopic,
              limit: 50,
              offset: 'earliest'
            }
          });
          
          if (response.data.success) {
            const messages = response.data.messages || [];
            setCdcMessages(messages);
            console.log(`✓ Fetched ${messages.length} CDC messages from Kafka`);
            if (messages.length === 0) {
              setHistoryError('No CDC messages found in this topic yet. Perform INSERT/UPDATE/DELETE operations on the source table to see them here.');
            }
          } else {
            setHistoryError(response.data.error || 'Failed to load CDC messages');
          }
        } catch (error) {
          console.error('Error fetching CDC messages:', error);
          const errorMsg = error.response?.data?.detail || error.response?.data?.error || 'Failed to connect to Kafka backend';
          setHistoryError(errorMsg);
          setCdcMessages([]);
          console.warn(`⚠️ Kafka CDC message fetch failed: ${errorMsg}`);
        } finally {
          setHistoryLoading(false);
        }
      }
    };
    
    fetchCdcMessages();
    
    // Auto-refresh every 10 seconds
    if (activeTab === 'history' && selectedCdcTopic) {
      const interval = setInterval(fetchCdcMessages, 10000);
      return () => clearInterval(interval);
    }
  }, [activeTab, selectedCdcTopic]);

  // Initialize query SQL when tab is opened
  useEffect(() => {
    if (activeTab === 'query' && !querySQL) {
      // Use a simple placeholder - will be replaced with S3 path during execution
      const normalizedSource = (defaultDataset.source || 'postgres').toLowerCase().replace('sql', '');
      const normalizedDomain = (defaultDataset.domain || 'finance').toLowerCase();
      const tableAlias = `syniqai_bronze_${normalizedDomain}_${normalizedSource}_${defaultDataset.name}`;
      setQuerySQL(`SELECT * FROM ${tableAlias} LIMIT 100;`);
    }
  }, [activeTab, defaultDataset.name, defaultDataset.source, defaultDataset.domain]);

  // Fetch preview data when Preview tab is opened
  useEffect(() => {
    const fetchPreviewData = async () => {
      if (activeTab === 'preview' && defaultDataset.name) {
        setPreviewLoading(true);
        try {
          const response = await axios.get(`http://localhost:8000/api/bronze-data/preview-data/${defaultDataset.name}`, {
            params: {
              domain: defaultDataset.domain || 'finance',
              source: defaultDataset.source?.toLowerCase() || 'postgres',
              limit: previewLimit
            }
          });
          if (response.data.success) {
            setPreviewData(response.data.rows || []);
            console.log(`Fetched ${response.data.rows?.length || 0} preview rows from MinIO`);
          }
        } catch (error) {
          console.error('Error fetching preview data:', error);
          setPreviewData([]);
        } finally {
          setPreviewLoading(false);
        }
      }
    };
    fetchPreviewData();
  }, [activeTab, defaultDataset.name, defaultDataset.domain, defaultDataset.source, previewLimit]);

  // Fetch schema when Schema tab is opened
  useEffect(() => {
    const fetchSchema = async () => {
      if (activeTab === 'schema' && defaultDataset.name) {
        setSchemaLoading(true);
        try {
          const response = await axios.get(`http://localhost:8000/api/bronze-data/schema/${defaultDataset.name}`, {
            params: {
              domain: defaultDataset.domain || 'finance',
              source: defaultDataset.source?.toLowerCase() || 'postgres'
            }
          });
          if (response.data.success) {
            setSchemaData(response.data.schema || []);
            console.log(`Fetched schema with ${response.data.schema?.length || 0} columns from MinIO`);
          }
        } catch (error) {
          console.error('Error fetching schema:', error);
          setSchemaData([]);
        } finally {
          setSchemaLoading(false);
        }
      }
    };
    fetchSchema();
  }, [activeTab, defaultDataset.name, defaultDataset.domain, defaultDataset.source]);

  // Fetch statistics when Statistics tab is opened
  useEffect(() => {
    const fetchStatistics = async () => {
      if (activeTab === 'statistics' && defaultDataset.name) {
        setStatisticsLoading(true);
        try {
          const response = await axios.get(`http://localhost:8000/api/bronze-data/statistics/${defaultDataset.name}`, {
            params: {
              domain: defaultDataset.domain || 'finance',
              source: defaultDataset.source?.toLowerCase() || 'postgres'
            }
          });
          if (response.data.success) {
            setStatisticsData(response.data.statistics || []);
            console.log(`Fetched statistics for ${response.data.statistics?.length || 0} columns from MinIO`);
          }
        } catch (error) {
          console.error('Error fetching statistics:', error);
          setStatisticsData([]);
        } finally {
          setStatisticsLoading(false);
        }
      }
    };
    fetchStatistics();
  }, [activeTab, defaultDataset.name, defaultDataset.domain, defaultDataset.source]);

  // Handle Run Query
  const handleRunQuery = async () => {
    setQueryLoading(true);
    setQueryError(null);
    
    try {
      // Normalize source name (PostgreSQL -> postgres)
      const normalizedSource = (defaultDataset.source || 'postgres').toLowerCase().replace('sql', '');
      const normalizedDomain = (defaultDataset.domain || 'finance').toLowerCase();
      const tableName = defaultDataset.name;
      
      // Construct S3 path (same as Preview tab)
      const s3Path = `s3://syniqai-bronze/${normalizedDomain}/${normalizedSource}/${tableName}/*.parquet`;
      
      console.log('🔍 Query Execution Details:', {
        tableName,
        s3Path,
        originalQuery: querySQL,
        dataset: defaultDataset
      });
      
      // Replace the table name in the query with the actual S3 path for direct DuckDB querying
      const modifiedQuery = querySQL.replace(
        /FROM\s+[\w_]+/i,
        `FROM '${s3Path}'`
      );
      
      console.log('📝 Modified Query (DuckDB style):', modifiedQuery);
      
      // Try to execute using DuckDB-compatible endpoint
      // If this fails, we'll create a simple execute endpoint
      const response = await axios.post('http://localhost:8000/api/bronze-data/execute-query', {
        query: modifiedQuery,
        table_name: tableName,
        domain: normalizedDomain,
        source: normalizedSource,
        limit: 1000
      });
      
      console.log('✅ Query Response:', response.data);
      
      if (response.data.success) {
        setQueryResults({
          success: true,
          columns: response.data.columns || [],
          rows: response.data.rows || [],
          row_count: response.data.row_count || response.data.rows?.length || 0,
          execution_time_ms: response.data.execution_time_ms || 0
        });
        console.log(`✅ Query executed successfully: ${response.data.rows?.length || 0} rows returned`);
      } else {
        const errorMsg = response.data.error || response.data.message || 'Query execution failed';
        console.error('❌ Query failed:', errorMsg);
        setQueryError(String(errorMsg)); // Ensure it's a string
      }
    } catch (error) {
      console.error('❌ Query execution error:', error);
      console.error('📋 Error response:', error.response?.data);
      
      // Extract error message and ensure it's a string
      let errorMsg = 'Failed to execute query. Check console for details.';
      
      if (error.response?.data) {
        const data = error.response.data;
        if (typeof data === 'string') {
          errorMsg = data;
        } else if (data.detail) {
          if (Array.isArray(data.detail)) {
            // FastAPI validation errors
            errorMsg = data.detail.map(err => `${err.loc?.join('.')}: ${err.msg}`).join(', ');
          } else {
            errorMsg = String(data.detail);
          }
        } else if (data.error) {
          errorMsg = String(data.error);
        } else if (data.message) {
          errorMsg = String(data.message);
        }
      } else if (error.message) {
        errorMsg = String(error.message);
      }
      
      setQueryError(errorMsg);
    } finally {
      setQueryLoading(false);
    }
  };

  // Handle Copy SQL
  const handleCopySQL = async () => {
    try {
      await navigator.clipboard.writeText(querySQL);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch (error) {
      console.error('Failed to copy:', error);
    }
  };

  const tabs = [
    { id: 'preview', label: 'Data Preview', icon: Eye },
    { id: 'schema', label: 'Schema', icon: Database },
    { id: 'statistics', label: 'Statistics', icon: BarChart3 },
    { id: 'history', label: 'History', icon: History },
    { id: 'query', label: 'Query', icon: Code }
  ];

  return (
    <div className="h-screen flex flex-col bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 p-6">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <button
              onClick={(e) => {
                e.preventDefault();
                navigate(`/${domain || defaultDataset.domain || 'finance'}/silver?tab=catalog`);
              }}
              className="p-2 text-gray-600 hover:bg-gray-100 rounded-lg transition-colors"
              title="Back to Catalog"
            >
              <ArrowLeft className="w-5 h-5" />
            </button>
            <div>
              <div className="flex items-center gap-3">
                <Table className="w-6 h-6 text-gray-600" />
                <h1 className="text-2xl font-bold text-gray-900">{defaultDataset.name}</h1>
                <span className="px-2 py-1 bg-gray-100 text-gray-800 rounded text-xs font-medium">
                  {defaultDataset.layer}
                </span>
              </div>
              <p className="text-sm text-gray-500 mt-2">
                {defaultDataset.domain} • {defaultDataset.source} • {defaultDataset.rowCount} rows • {defaultDataset.size}
              </p>
            </div>
          </div>
          <div className="flex gap-2">
            <button className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50 flex items-center gap-2">
              <RefreshCw className="w-4 h-4" />
              Refresh
            </button>
            <button 
              onClick={(e) => {
                e.preventDefault();
                console.log('Navigating to Check Quality with:', {
                  table: defaultDataset.name,
                  domain: defaultDataset.domain,
                  source: defaultDataset.source
                });
                // Navigate to the quality tab with parameters
                navigate(
                  `/${domain || defaultDataset.domain || 'finance'}/silver?tab=quality&table=${encodeURIComponent(defaultDataset.name)}&domain=${encodeURIComponent(defaultDataset.domain || 'finance')}&source=${encodeURIComponent(defaultDataset.source || 'postgres')}`
                );
              }}
              className="px-4 py-2 bg-gradient-to-r from-blue-600 to-indigo-600 text-white rounded-lg text-sm font-medium hover:from-blue-700 hover:to-indigo-700 flex items-center gap-2 shadow-md"
              title={`Check Quality for ${defaultDataset.name}`}
            >
              <Shield className="w-4 h-4" />
              Check Quality
            </button>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="bg-white border-b border-gray-200">
        <div className="flex gap-1 px-6">
          {tabs.map(tab => {
            const Icon = tab.icon;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`px-4 py-3 text-sm font-medium border-b-2 transition-colors flex items-center gap-2 ${
                  activeTab === tab.id
                  ? 'border-blue-600 text-blue-600'
                  : 'border-transparent text-gray-600 hover:text-gray-900'
              }`}
            >
              <Icon className="w-4 h-4" />
              {tab.label}
            </button>
          );
        })}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-6">
        {activeTab === 'preview' && (
          <div>
            <div className="bg-white rounded-lg shadow border border-gray-200">
              <div className="p-4 border-b border-gray-200 flex items-center justify-between">
                <h2 className="text-lg font-semibold text-gray-900">Table Data Preview</h2>
                <div className="flex items-center gap-3">
                  {previewLoading && (
                    <div className="flex items-center gap-2 text-sm text-gray-500">
                      <RefreshCw className="w-4 h-4 animate-spin" />
                      Loading...
                    </div>
                  )}
                  <label className="text-sm text-gray-600">Rows:</label>
                  <select
                    value={previewLimit}
                    onChange={(e) => setPreviewLimit(Number(e.target.value))}
                    className="border border-gray-300 rounded px-2 py-1 text-sm"
                  >
                    <option value={10}>10</option>
                    <option value={50}>50</option>
                    <option value={100}>100</option>
                    <option value={500}>500</option>
                  </select>
                </div>
              </div>
              <div className="overflow-x-auto">
                {previewLoading ? (
                  <div className="text-center py-12 text-gray-500">
                    <RefreshCw className="w-8 h-8 mx-auto mb-3 text-gray-400 animate-spin" />
                    <p>Loading data from MinIO...</p>
                  </div>
                ) : sampleData.length === 0 ? (
                  <div className="text-center py-12 text-gray-500">
                    <Database className="w-12 h-12 mx-auto mb-3 text-gray-400" />
                    <p>No data found</p>
                    <p className="text-sm mt-1">Table might be empty or not accessible</p>
                  </div>
                ) : (
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50">
                      <tr>
                        {Object.keys(sampleData[0] || {}).map(key => (
                          <th key={key} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">
                            {key}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {sampleData.map((row, idx) => (
                        <tr key={idx} className="hover:bg-gray-50">
                          {Object.values(row).map((value, cellIdx) => (
                            <td key={cellIdx} className="px-4 py-3 whitespace-nowrap text-gray-900">
                              {value === null ? <span className="text-gray-400 italic">null</span> : String(value)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            </div>
          </div>
        )}

        {activeTab === 'schema' && (
          <div className="bg-white rounded-lg shadow border border-gray-200">
            <div className="p-4 border-b border-gray-200 flex items-center justify-between">
              <h2 className="text-lg font-semibold text-gray-900">Schema Definition from MinIO</h2>
              {schemaLoading && (
                <div className="flex items-center gap-2 text-sm text-gray-500">
                  <RefreshCw className="w-4 h-4 animate-spin" />
                  Loading...
                </div>
              )}
            </div>
            <div className="overflow-x-auto">
              {schemaLoading ? (
                <div className="text-center py-12 text-gray-500">
                  <RefreshCw className="w-8 h-8 mx-auto mb-3 text-gray-400 animate-spin" />
                  <p>Loading schema from MinIO...</p>
                </div>
              ) : schema.length === 0 ? (
                <div className="text-center py-12 text-gray-500">
                  <Database className="w-12 h-12 mx-auto mb-3 text-gray-400" />
                  <p>No schema found</p>
                </div>
              ) : (
                <table className="w-full text-sm">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Column Name</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Data Type</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Nullable</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Key</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Description</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {schema.map((col, idx) => (
                      <tr key={idx} className="hover:bg-gray-50">
                        <td className="px-6 py-4 whitespace-nowrap font-medium text-gray-900">{col.name}</td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className="px-2 py-1 bg-blue-50 text-blue-700 rounded text-xs font-mono">
                            {col.type}
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          {col.nullable ? (
                            <span className="text-gray-600">Yes</span>
                          ) : (
                            <span className="text-red-600 font-medium">No</span>
                          )}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          {col.primaryKey && (
                            <span className="px-2 py-1 bg-purple-50 text-purple-700 rounded text-xs font-medium">
                              PRIMARY
                            </span>
                          )}
                        </td>
                        <td className="px-6 py-4 text-gray-600">{col.description}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        )}

        {activeTab === 'statistics' && (
          <div className="bg-white rounded-lg shadow border border-gray-200">
            <div className="p-4 border-b border-gray-200 flex items-center justify-between">
              <h2 className="text-lg font-semibold text-gray-900">Column Statistics from MinIO</h2>
              {statisticsLoading && (
                <div className="flex items-center gap-2 text-sm text-gray-500">
                  <RefreshCw className="w-4 h-4 animate-spin" />
                  Calculating...
                </div>
              )}
            </div>
            <div className="overflow-x-auto">
              {statisticsLoading ? (
                <div className="text-center py-12 text-gray-500">
                  <RefreshCw className="w-8 h-8 mx-auto mb-3 text-gray-400 animate-spin" />
                  <p>Calculating statistics from MinIO data...</p>
                  <p className="text-sm mt-1">This may take a moment...</p>
                </div>
              ) : statistics.length === 0 ? (
                <div className="text-center py-12 text-gray-500">
                  <BarChart3 className="w-12 h-12 mx-auto mb-3 text-gray-400" />
                  <p>No statistics available</p>
                </div>
              ) : (
                <table className="w-full text-sm">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Column</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Type</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Distinct</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Nulls</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Null %</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Min</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Max</th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Avg</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {statistics.map((stat, idx) => (
                      <tr key={idx} className="hover:bg-gray-50">
                        <td className="px-6 py-4 whitespace-nowrap font-medium text-gray-900">{stat.column}</td>
                        <td className="px-6 py-4 whitespace-nowrap text-gray-600">{stat.type}</td>
                        <td className="px-6 py-4 whitespace-nowrap text-gray-900">{stat.distinct}</td>
                        <td className="px-6 py-4 whitespace-nowrap text-gray-900">{stat.nulls}</td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className={stat.nullPercent > 5 ? 'text-red-600 font-medium' : 'text-gray-600'}>
                            {stat.nullPercent}%
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-gray-900">{stat.min}</td>
                        <td className="px-6 py-4 whitespace-nowrap text-gray-900">{stat.max}</td>
                        <td className="px-6 py-4 whitespace-nowrap text-gray-900">{stat.avg}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          </div>
        )}

        {activeTab === 'history' && (
          <div className="bg-white rounded-lg shadow border border-gray-200">
            <div className="p-4 border-b border-gray-200 flex items-center justify-between">
              <div>
                <h2 className="text-lg font-semibold text-gray-900 flex items-center">
                  <Database className="w-5 h-5 mr-2 text-purple-600" />
                  CDC Message History from Kafka
                </h2>
                <p className="text-sm text-gray-500 mt-0.5">
                  Real-time CDC operations (INSERT/UPDATE/DELETE) captured from source database
                </p>
              </div>
              <div className="flex items-center gap-3">
                {selectedCdcTopic && (
                  <span className="px-3 py-1 bg-purple-100 text-purple-700 rounded-full text-sm font-semibold">
                    {cdcMessages.length} message{cdcMessages.length !== 1 ? 's' : ''}
                  </span>
                )}
                {historyLoading && (
                  <div className="flex items-center gap-2 text-sm text-gray-500">
                    <RefreshCw className="w-4 h-4 animate-spin" />
                    Loading from Kafka...
                  </div>
                )}
              </div>
            </div>
            
            {/* Topic Selector */}
            {cdcTopics.length > 0 && (
              <div className="px-6 py-3 bg-gray-50 border-b border-gray-200">
                <div className="flex items-center gap-3">
                  <List className="w-4 h-4 text-gray-500" />
                  <label className="text-sm font-medium text-gray-700">Kafka Topic:</label>
                  <select
                    value={selectedCdcTopic}
                    onChange={(e) => setSelectedCdcTopic(e.target.value)}
                    className="flex-1 px-3 py-2 border-2 border-gray-300 rounded-lg text-sm font-mono bg-white hover:border-purple-400 focus:border-purple-500 focus:ring-2 focus:ring-purple-200 outline-none transition-colors"
                  >
                    {cdcTopics.map(topic => (
                      <option key={topic} value={topic}>
                        {topic}
                      </option>
                    ))}
                  </select>
                </div>
              </div>
            )}
            
            {historyError && !historyLoading && (
              <div className="p-4 bg-yellow-50 border-b border-yellow-100 flex items-start gap-3">
                <Info className="w-5 h-5 text-yellow-600 flex-shrink-0 mt-0.5" />
                <div className="flex-1">
                  <p className="text-sm text-yellow-800 font-medium mb-1">
                    {historyError}
                  </p>
                  <p className="text-xs text-yellow-700">
                    💡 Make sure CDC is enabled and running. Perform database operations to generate CDC messages.
                  </p>
                </div>
              </div>
            )}
            
            <div className="p-6">
              {cdcMessages.length === 0 && !historyLoading ? (
                <div className="text-center py-12 text-gray-500">
                  <Database className="w-16 h-16 mx-auto mb-4 text-gray-400" />
                  <h3 className="text-lg font-semibold text-gray-900 mb-2">
                    {!selectedCdcTopic ? 'No CDC topics available' : 'No CDC messages yet'}
                  </h3>
                  <p className="text-sm text-gray-600 mb-4">
                    {!selectedCdcTopic 
                      ? 'CDC topics will appear here once Change Data Capture is enabled' 
                      : 'CDC operations (INSERT/UPDATE/DELETE) will appear here when you modify data in the source table'}
                  </p>
                  <div className="text-left max-w-md mx-auto bg-gray-50 rounded-lg p-4 border border-gray-200">
                    <p className="text-sm font-semibold text-gray-700 mb-2">To see CDC messages:</p>
                    <ol className="text-sm text-gray-700 space-y-2 list-decimal list-inside">
                      <li>Ensure CDC consumer is running (check CDC page)</li>
                      <li>Make sure Kafka is running on localhost:9092</li>
                      <li>Perform INSERT/UPDATE/DELETE on the source database</li>
                      <li>Messages will appear here in real-time</li>
                    </ol>
                  </div>
                </div>
              ) : (
                <div className="space-y-3">
                  {cdcMessages.map((msg, idx) => {
                    // Helper functions (same as KafkaBacklog)
                    const getOperationIcon = (operation) => {
                      switch (operation) {
                        case 'INSERT': return <CheckCircle className="w-4 h-4" />;
                        case 'UPDATE': return <Edit2 className="w-4 h-4" />;
                        case 'DELETE': return <Trash2 className="w-4 h-4" />;
                        default: return <History className="w-4 h-4" />;
                      }
                    };
                    
                    const getOperationColor = (operation) => {
                      switch (operation) {
                        case 'INSERT': return 'bg-green-100 text-green-700 border-green-300';
                        case 'UPDATE': return 'bg-blue-100 text-blue-700 border-blue-300';
                        case 'DELETE': return 'bg-red-100 text-red-700 border-red-300';
                        default: return 'bg-gray-100 text-gray-700 border-gray-300';
                      }
                    };
                    
                    const formatTimestamp = (timestamp) => {
                      if (!timestamp) return 'N/A';
                      const date = new Date(timestamp);
                      return date.toLocaleString('en-US', {
                        month: 'short',
                        day: 'numeric',
                        year: 'numeric',
                        hour: '2-digit',
                        minute: '2-digit',
                        second: '2-digit'
                      });
                    };
                    
                    const getChangedFields = (before, after) => {
                      if (!before || !after) return [];
                      const changes = [];
                      Object.keys(after).forEach(key => {
                        if (before[key] !== after[key]) {
                          changes.push({
                            field: key,
                            before: before[key],
                            after: after[key]
                          });
                        }
                      });
                      return changes;
                    };
                    
                    const changes = getChangedFields(msg.before, msg.after);
                    const displayData = msg.after || msg.before || {};
                    
                    return (
                      <div
                        key={`${msg.offset}-${idx}`}
                        className="border-2 border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow"
                      >
                        <div className="flex items-start justify-between mb-3">
                          <div className="flex items-center gap-3">
                            <div className={`px-3 py-1.5 rounded-lg border-2 font-semibold text-sm flex items-center gap-1.5 ${getOperationColor(msg.operation_name)}`}>
                              {getOperationIcon(msg.operation_name)}
                              {msg.operation_name}
                            </div>
                            <div className="flex items-center gap-2">
                              <span className="text-sm text-gray-500">DB:</span>
                              <span className="px-2 py-1 bg-purple-100 text-purple-700 rounded font-mono text-sm font-semibold">
                                {msg.database || 'unknown'}
                              </span>
                              <span className="text-gray-400">|</span>
                              <span className="text-sm text-gray-500">Table:</span>
                              <span className="px-2 py-1 bg-gray-100 text-gray-700 rounded font-mono text-sm">
                                {msg.table}
                              </span>
                            </div>
                          </div>
                          <div className="text-right">
                            <p className="text-xs text-gray-500">Offset {msg.offset}</p>
                            <p className="text-xs text-gray-600 mt-0.5">{formatTimestamp(msg.timestamp)}</p>
                          </div>
                        </div>

                        {/* Display data fields - Generic for any table */}
                        {Object.keys(displayData).length > 0 && (
                          <div className="bg-gray-50 rounded-lg p-3 border border-gray-200">
                            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3 text-sm">
                              {Object.entries(displayData).slice(0, 8).map(([key, value]) => (
                                <div key={key}>
                                  <p className="text-gray-500 font-medium mb-1 text-xs uppercase tracking-wide">
                                    {key.replace(/_/g, ' ')}
                                  </p>
                                  <p className="font-semibold text-gray-900 truncate" title={String(value)}>
                                    {value !== null && value !== undefined ? String(value) : 'null'}
                                  </p>
                                </div>
                              ))}
                            </div>
                            {Object.keys(displayData).length > 8 && (
                              <p className="text-xs text-gray-500 mt-2 text-center">
                                ... and {Object.keys(displayData).length - 8} more field{Object.keys(displayData).length - 8 > 1 ? 's' : ''}
                              </p>
                            )}
                          </div>
                        )}

                        {/* Show changes for UPDATE operations */}
                        {msg.operation_name === 'UPDATE' && changes.length > 0 && (
                          <div className="mt-3 pt-3 border-t border-gray-200">
                            <p className="text-xs font-semibold text-gray-600 mb-2">CHANGES:</p>
                            <div className="flex flex-wrap gap-2">
                              {changes.map((change, changeIdx) => (
                                <div key={changeIdx} className="px-2 py-1 bg-blue-50 border border-blue-200 rounded text-xs">
                                  <span className="font-medium text-gray-700">{change.field}:</span>
                                  <span className="text-red-600 line-through ml-1">{String(change.before)}</span>
                                  <span className="mx-1">→</span>
                                  <span className="text-green-600 font-semibold">{String(change.after)}</span>
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </div>
        )}

        {activeTab === 'query' && (
          <div className="space-y-4">
            {/* Header with explanation */}
            <div className="bg-blue-50 border border-blue-200 rounded-lg p-4">
              <div className="flex items-start gap-3">
                <Info className="w-5 h-5 text-blue-600 flex-shrink-0 mt-0.5" />
                <div>
                  <h3 className="font-semibold text-blue-900 mb-1">Quick Validation & Testing</h3>
                  <p className="text-sm text-blue-800">
                    This interface is for <strong>quick data inspection and validation</strong> only. 
                    Use SELECT queries to check data quality, test assumptions, and verify transformations.
                  </p>
                  <p className="text-sm text-blue-700 mt-2">
                    💡 <strong>For data transformations</strong>, use the <strong>Transform</strong> button above with the full SQL editor (Monaco, templates, auto-complete).
                  </p>
                </div>
              </div>
            </div>

            {/* Step-by-step workflow */}
            <div className="bg-white rounded-lg shadow border border-gray-200">
              <div className="p-4 border-b border-gray-200">
                <h2 className="text-lg font-semibold text-gray-900">Validation Query Workflow</h2>
              </div>
              
              <div className="p-6 space-y-6">
                {/* Step 1: Edit SQL */}
                <div className="flex gap-4">
                  <div className="flex-shrink-0">
                    <div className="w-8 h-8 rounded-full bg-blue-100 text-blue-600 flex items-center justify-center font-bold">
                      1
                    </div>
                  </div>
                  <div className="flex-1">
                    <h3 className="font-semibold text-gray-900 mb-2">Edit Your Validation Query</h3>
                    <p className="text-sm text-gray-600 mb-3">
                      Write a SELECT query to inspect your data. Examples: check nulls, count rows, find duplicates.
                    </p>
                    <div className="bg-gray-900 rounded-lg p-4">
                      <textarea
                        value={querySQL}
                        onChange={(e) => setQuerySQL(e.target.value)}
                        className="w-full bg-transparent text-white font-mono text-sm resize-none focus:outline-none"
                        rows={6}
                        placeholder="SELECT * FROM bronze.finance_transactions LIMIT 100;"
                      />
                    </div>
                    <p className="text-xs text-gray-500 mt-2">
                      ⚠️ Only SELECT queries allowed. No INSERT/UPDATE/DELETE/DROP.
                    </p>
                  </div>
                </div>

                {/* Step 2: Run Query */}
                <div className="flex gap-4">
                  <div className="flex-shrink-0">
                    <div className="w-8 h-8 rounded-full bg-blue-100 text-blue-600 flex items-center justify-center font-bold">
                      2
                    </div>
                  </div>
                  <div className="flex-1">
                    <h3 className="font-semibold text-gray-900 mb-2">Execute & Review Results</h3>
                    <p className="text-sm text-gray-600 mb-3">
                      Run your query to see results. Verify data quality and test your assumptions.
                    </p>
                    <div className="flex gap-2">
                      <button 
                        onClick={handleRunQuery}
                        disabled={queryLoading || !querySQL.trim()}
                        className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed flex items-center gap-2"
                      >
                        {queryLoading ? (
                          <>
                            <RefreshCw className="w-4 h-4 animate-spin" />
                            Running...
                          </>
                        ) : (
                          <>
                            <Play className="w-4 h-4" />
                            Run Query
                          </>
                        )}
                      </button>
                      <button 
                        onClick={handleCopySQL}
                        className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50 flex items-center gap-2"
                      >
                        {copied ? (
                          <>
                            <CheckCircle className="w-4 h-4 text-green-600" />
                            Copied!
                          </>
                        ) : (
                          <>
                            <Copy className="w-4 h-4" />
                            Copy SQL
                          </>
                        )}
                      </button>
                    </div>

                    {/* Query Error */}
                    {queryError && (
                      <div className="mt-3 p-3 bg-red-50 border border-red-200 rounded-lg flex items-start gap-2">
                        <AlertCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
                        <div>
                          <p className="text-sm font-medium text-red-900">Query Failed</p>
                          <p className="text-sm text-red-700">{queryError}</p>
                        </div>
                      </div>
                    )}
                  </div>
                </div>

                {/* Step 3: Analyze Results */}
                {queryResults && (
                  <div className="flex gap-4">
                    <div className="flex-shrink-0">
                      <div className="w-8 h-8 rounded-full bg-green-100 text-green-600 flex items-center justify-center font-bold">
                        3
                      </div>
                    </div>
                    <div className="flex-1">
                      <h3 className="font-semibold text-gray-900 mb-2">Analyze Results</h3>
                      
                      {/* Query Metadata */}
                      <div className="bg-green-50 border border-green-200 rounded-lg p-3 mb-3">
                        <div className="flex items-center gap-4 text-sm">
                          <span className="text-green-800">
                            <strong>✓ Success:</strong> {queryResults.message}
                          </span>
                          <span className="text-gray-600">
                            {queryResults.row_count} rows
                          </span>
                          <span className="text-gray-600">
                            {queryResults.execution_time_ms.toFixed(2)}ms
                          </span>
                          <span className="px-2 py-0.5 bg-green-100 text-green-700 rounded text-xs font-medium">
                            {queryResults.query_type}
                          </span>
                        </div>
                      </div>

                      {/* Results Table */}
                      {queryResults.rows && queryResults.rows.length > 0 ? (
                        <div className="border border-gray-200 rounded-lg overflow-hidden">
                          <div className="overflow-x-auto max-h-96">
                            <table className="w-full text-sm">
                              <thead className="bg-gray-50 sticky top-0">
                                <tr>
                                  {queryResults.columns.map((col, idx) => (
                                    <th key={idx} className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap border-b border-gray-200">
                                      {col}
                                    </th>
                                  ))}
                                </tr>
                              </thead>
                              <tbody className="bg-white divide-y divide-gray-200">
                                {queryResults.rows.map((row, rowIdx) => (
                                  <tr key={rowIdx} className="hover:bg-gray-50">
                                    {queryResults.columns.map((col, colIdx) => (
                                      <td key={colIdx} className="px-4 py-3 whitespace-nowrap text-gray-900">
                                        {row[col] === null ? (
                                          <span className="text-gray-400 italic">null</span>
                                        ) : typeof row[col] === 'object' ? (
                                          JSON.stringify(row[col])
                                        ) : (
                                          String(row[col])
                                        )}
                                      </td>
                                    ))}
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      ) : (
                        <div className="text-center py-8 text-gray-500 border border-gray-200 rounded-lg">
                          <Database className="w-12 h-12 mx-auto mb-3 text-gray-400" />
                          <p>No results returned</p>
                          <p className="text-sm mt-1">Query executed successfully but returned 0 rows</p>
                        </div>
                      )}
                    </div>
                  </div>
                )}

                {/* Help text when no results yet */}
                {!queryResults && !queryLoading && (
                  <div className="flex gap-4">
                    <div className="flex-shrink-0">
                      <div className="w-8 h-8 rounded-full bg-gray-100 text-gray-400 flex items-center justify-center font-bold">
                        3
                      </div>
                    </div>
                    <div className="flex-1">
                      <h3 className="font-semibold text-gray-500 mb-2">Analyze Results</h3>
                      <p className="text-sm text-gray-500">
                        Results will appear here after running your query.
                      </p>
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* Quick tips */}
            <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
              <h4 className="font-semibold text-gray-900 mb-2 text-sm">💡 Common Validation Queries</h4>
              <div className="grid grid-cols-2 gap-2 text-xs">
                <button 
                  onClick={() => setQuerySQL(`SELECT COUNT(*) as total_rows FROM ${defaultDataset.layer}.${defaultDataset.name};`)}
                  className="text-left px-3 py-2 bg-white border border-gray-200 rounded hover:bg-gray-50"
                >
                  Count total rows
                </button>
                <button 
                  onClick={() => setQuerySQL(`SELECT * FROM ${defaultDataset.layer}.${defaultDataset.name} WHERE amount IS NULL;`)}
                  className="text-left px-3 py-2 bg-white border border-gray-200 rounded hover:bg-gray-50"
                >
                  Check for nulls
                </button>
                <button 
                  onClick={() => setQuerySQL(`SELECT status, COUNT(*) as count FROM ${defaultDataset.layer}.${defaultDataset.name} GROUP BY status;`)}
                  className="text-left px-3 py-2 bg-white border border-gray-200 rounded hover:bg-gray-50"
                >
                  Group by status
                </button>
                <button 
                  onClick={() => setQuerySQL(`SELECT MIN(amount) as min, MAX(amount) as max, AVG(amount) as avg FROM ${defaultDataset.layer}.${defaultDataset.name};`)}
                  className="text-left px-3 py-2 bg-white border border-gray-200 rounded hover:bg-gray-50"
                >
                  Statistical summary
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
