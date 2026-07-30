import React, { useState, useEffect } from 'react';
import { Database, Search, Filter, ChevronRight, ChevronDown, Table, FileText, Tag, Clock, Users, BarChart3, Eye, RefreshCw } from 'lucide-react';
import { useNavigate, useParams } from 'react-router-dom';
import axios from 'axios';

const API_BASE = 'http://localhost:8000/api';

/**
 * DataCatalog - Browse and discover datasets by domain/source
 * Shows table metadata, schemas, and data lineage
 */
export default function DataCatalog({ onDatasetSelect }) {
  const navigate = useNavigate();
  const { domain } = useParams();
  
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedDomain, setSelectedDomain] = useState('all');
  const [selectedSource, setSelectedSource] = useState('all');
  const [expandedDomains, setExpandedDomains] = useState({ finance: true });
  const [viewMode, setViewMode] = useState('list'); // 'list' or 'grid'
  
  // State for backend data
  const [domains, setDomains] = useState([]);
  const [datasets, setDatasets] = useState([]);
  const [filteredDatasets, setFilteredDatasets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState(new Date());

  const sources = ['all', 'MariaDB', 'PostgreSQL', 'MongoDB', 'S3'];

  // Fetch initial data
  useEffect(() => {
    fetchCatalogData();
  }, []);

  // Apply filters when search, domain, or source changes
  useEffect(() => {
    filterDatasets();
  }, [searchQuery, selectedDomain, selectedSource, datasets]);

  const fetchCatalogData = async () => {
    setLoading(true);
    
    try {
      // Fetch domains
      const domainsResponse = await axios.get(`${API_BASE}/silver/catalog/domains`);
      if (domainsResponse.data.success) {
        setDomains(domainsResponse.data.domains);
      }
    } catch (err) {
      console.error('Failed to fetch domains:', err);
    }
    
    try {
      // Fetch datasets
      const datasetsResponse = await axios.get(`${API_BASE}/silver/catalog/datasets`);
      if (datasetsResponse.data.success) {
        setDatasets(datasetsResponse.data.datasets);
      }
    } catch (err) {
      console.error('Failed to fetch datasets:', err);
    }
    
    setLoading(false);
    setLastRefresh(new Date());
  };

  const filterDatasets = () => {
    let filtered = [...datasets];
    
    // Apply search filter
    if (searchQuery) {
      const query = searchQuery.toLowerCase();
      filtered = filtered.filter(ds => 
        ds.name.toLowerCase().includes(query) ||
        ds.description.toLowerCase().includes(query) ||
        ds.tags.some(tag => tag.toLowerCase().includes(query))
      );
    }
    
    // Apply domain filter
    if (selectedDomain !== 'all') {
      filtered = filtered.filter(ds => ds.domain === selectedDomain);
    }
    
    // Apply source filter
    if (selectedSource !== 'all') {
      filtered = filtered.filter(ds => ds.source === selectedSource);
    }
    
    setFilteredDatasets(filtered);
  };

  const handleRefresh = () => {
    fetchCatalogData();
  };

  const toggleDomain = (domainId) => {
    setExpandedDomains(prev => ({
      ...prev,
      [domainId]: !prev[domainId]
    }));
  };

  const getLayerColor = (layer) => {
    switch(layer) {
      case 'bronze': return 'bg-orange-100 text-orange-800';
      case 'silver': return 'bg-gray-100 text-gray-800';
      case 'gold': return 'bg-yellow-100 text-yellow-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const getQualityColor = (score) => {
    if (score >= 95) return 'text-green-600';
    if (score >= 85) return 'text-yellow-600';
    return 'text-red-600';
  };

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Left Sidebar - Domain Browser */}
      <div className="w-64 bg-white border-r border-gray-200 overflow-y-auto">
        <div className="p-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
            <Database className="w-5 h-5" />
            Domains
          </h2>
        </div>
        <div className="p-2">
          <button
            onClick={() => setSelectedDomain('all')}
            className={`w-full text-left px-3 py-2 rounded-lg text-sm font-medium transition-colors ${
              selectedDomain === 'all' ? 'bg-blue-50 text-blue-700' : 'text-gray-700 hover:bg-gray-50'
            }`}
          >
            All Domains ({datasets.length})
          </button>
          {domains.map(domain => (
            <div key={domain.id} className="mt-1">
              <button
                onClick={() => {
                  setSelectedDomain(domain.id);
                  toggleDomain(domain.id);
                }}
                className={`w-full text-left px-3 py-2 rounded-lg text-sm font-medium transition-colors flex items-center justify-between ${
                  selectedDomain === domain.id ? 'bg-blue-50 text-blue-700' : 'text-gray-700 hover:bg-gray-50'
                }`}
              >
                <span className="flex items-center gap-2">
                  <div className="w-2 h-2 rounded-full" style={{ backgroundColor: domain.color }} />
                  {domain.name}
                </span>
                <span className="text-xs text-gray-500">{domain.count}</span>
              </button>
            </div>
          ))}
        </div>

        <div className="p-4 border-t border-gray-200 mt-4">
          <h3 className="text-sm font-semibold text-gray-900 mb-2">Source Systems</h3>
          <div className="space-y-1">
            {['all', 'MariaDB', 'PostgreSQL', 'MongoDB', 'S3'].map(source => (
              <button
                key={source}
                onClick={() => setSelectedSource(source)}
                className={`w-full text-left px-3 py-1.5 rounded text-xs transition-colors ${
                  selectedSource === source ? 'bg-blue-50 text-blue-700' : 'text-gray-600 hover:bg-gray-50'
                }`}
              >
                {source === 'all' ? 'All Sources' : source}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 overflow-y-auto">
        <div className="p-6">
          {/* Header */}
          <div className="mb-6 flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Data Catalog</h1>
              <p className="text-sm text-gray-500 mt-1">
                Discover and explore datasets across domains
                {lastRefresh && <span className="ml-2">• Last updated: {lastRefresh.toLocaleTimeString()}</span>}
              </p>
            </div>
            <button
              onClick={handleRefresh}
              disabled={loading}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400"
            >
              <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
          </div>

          {/* Search and Filters */}
          <div className="flex gap-4 mb-6">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400" />
              <input
                type="text"
                placeholder="Search datasets, descriptions, tags..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <button className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50 flex items-center gap-2">
              <Filter className="w-4 h-4" />
              Filters
            </button>
          </div>

          {/* Results Count */}
          <div className="mb-4 flex items-center justify-between">
            <div className="text-sm text-gray-600">
              {loading ? (
                <span className="flex items-center gap-2">
                  <RefreshCw className="w-4 h-4 animate-spin" />
                  Loading datasets...
                </span>
              ) : (
                <span>{filteredDatasets.length} {filteredDatasets.length === 1 ? 'dataset' : 'datasets'} found</span>
              )}
            </div>
          </div>

          {/* Dataset List */}
          <div className="space-y-4">
            {loading ? (
              <div className="flex items-center justify-center py-12">
                <RefreshCw className="w-8 h-8 text-blue-600 animate-spin" />
              </div>
            ) : filteredDatasets.length === 0 ? (
              <div className="text-center py-12 bg-white rounded-lg border-2 border-dashed border-gray-300">
                <Database className="w-16 h-16 text-gray-400 mx-auto mb-4" />
                <h3 className="text-lg font-semibold text-gray-900 mb-2">No datasets found</h3>
                
                {datasets.length === 0 ? (
                  // No Bronze tables at all
                  <div className="max-w-2xl mx-auto">
                    <p className="text-gray-600 mb-4">
                      No Bronze tables available yet. You need to ingest data first.
                    </p>
                    
                    <div className="bg-blue-50 border border-blue-200 rounded-lg p-6 text-left mb-4">
                      <h4 className="font-semibold text-blue-900 mb-3 flex items-center gap-2">
                        <span className="bg-blue-600 text-white rounded-full w-6 h-6 flex items-center justify-center text-sm">1</span>
                        Quick Start Guide
                      </h4>
                      <ol className="space-y-3 text-sm text-gray-700">
                        <li className="flex items-start gap-2">
                          <span className="font-mono bg-white px-2 py-1 rounded border border-gray-300 text-xs">Step 1:</span>
                          <span>Go to <strong>Data Ingestion</strong> page (sidebar menu)</span>
                        </li>
                        <li className="flex items-start gap-2">
                          <span className="font-mono bg-white px-2 py-1 rounded border border-gray-300 text-xs">Step 2:</span>
                          <span>Select a data source (MariaDB, PostgreSQL, MongoDB, or CSV file)</span>
                        </li>
                        <li className="flex items-start gap-2">
                          <span className="font-mono bg-white px-2 py-1 rounded border border-gray-300 text-xs">Step 3:</span>
                          <span>Configure connection and run ingestion to create <strong>Bronze tables</strong></span>
                        </li>
                        <li className="flex items-start gap-2">
                          <span className="font-mono bg-white px-2 py-1 rounded border border-gray-300 text-xs">Step 4:</span>
                          <span>Come back here to see your Bronze tables in the catalog</span>
                        </li>
                        <li className="flex items-start gap-2">
                          <span className="font-mono bg-white px-2 py-1 rounded border border-gray-300 text-xs">Step 5:</span>
                          <span>Click on a table and use <strong>Check Quality</strong> button to validate data before transformation</span>
                        </li>
                      </ol>
                    </div>
                    
                    <button
                      onClick={() => window.location.href = window.location.pathname.replace('/silver', '/ingestion')}
                      className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-medium inline-flex items-center gap-2"
                    >
                      <Database className="w-5 h-5" />
                      Go to Data Ingestion
                    </button>
                  </div>
                ) : (
                  // Has Bronze tables but filtered out
                  <div>
                    <p className="text-gray-600 mb-2">No datasets match your current filters</p>
                    <p className="text-sm text-gray-500">Try adjusting your domain, source, or search query</p>
                    <button
                      onClick={() => {
                        setSearchQuery('');
                        setSelectedDomain('all');
                        setSelectedSource('all');
                      }}
                      className="mt-4 px-4 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200"
                    >
                      Clear All Filters
                    </button>
                  </div>
                )}
              </div>
            ) : 
            filteredDatasets.map(dataset => (
              <div
                key={dataset.id}
                className="bg-white rounded-lg shadow border border-gray-200 p-6 hover:shadow-md transition-shadow cursor-pointer"
                onClick={() => onDatasetSelect && onDatasetSelect(dataset)}
              >
                <div className="flex items-start justify-between">
                  <div className="flex-1">
                    <div className="flex items-center gap-3 mb-2">
                      <Table className="w-5 h-5 text-gray-600" />
                      <h3 className="text-lg font-semibold text-gray-900">{dataset.name}</h3>
                      <span className={`px-2 py-1 rounded text-xs font-medium ${getLayerColor(dataset.layer)}`}>
                        {dataset.layer}
                      </span>
                    </div>
                    <p className="text-sm text-gray-600 mb-3">{dataset.description}</p>
                    
                    {/* Tags */}
                    <div className="flex flex-wrap gap-2 mb-3">
                      {dataset.tags.map((tag, idx) => (
                        <span key={idx} className="px-2 py-1 bg-blue-50 text-blue-700 rounded text-xs font-medium flex items-center gap-1">
                          <Tag className="w-3 h-3" />
                          {tag}
                        </span>
                      ))}
                    </div>

                    {/* Metadata Grid */}
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 pt-3 border-t border-gray-200">
                      <div>
                        <p className="text-xs text-gray-500">Source</p>
                        <p className="text-sm font-medium text-gray-900">{dataset.source}</p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-500">Rows</p>
                        <p className="text-sm font-medium text-gray-900">{dataset.rowCount}</p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-500">Size</p>
                        <p className="text-sm font-medium text-gray-900">{dataset.size}</p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-500">Columns</p>
                        <p className="text-sm font-medium text-gray-900">{dataset.columns}</p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-500">Owner</p>
                        <p className="text-sm font-medium text-gray-900 flex items-center gap-1">
                          <Users className="w-3 h-3" />
                          {dataset.owner}
                        </p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-500">Last Updated</p>
                        <p className="text-sm font-medium text-gray-900 flex items-center gap-1">
                          <Clock className="w-3 h-3" />
                          {dataset.lastUpdated}
                        </p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-500">Update Frequency</p>
                        <p className="text-sm font-medium text-gray-900">{dataset.updateFrequency}</p>
                      </div>
                      <div>
                        <p className="text-xs text-gray-500">Quality Score</p>
                        <p className={`text-sm font-bold flex items-center gap-1 ${getQualityColor(dataset.quality)}`}>
                          <BarChart3 className="w-3 h-3" />
                          {dataset.quality}%
                        </p>
                      </div>
                    </div>
                  </div>

                  {/* Actions */}
                  <div className="ml-4 flex gap-2">
                    <button 
                      onClick={(e) => {
                        e.stopPropagation();
                        console.log('Preview clicked for table:', dataset.name, 'Dataset:', dataset);
                        onDatasetSelect && onDatasetSelect(dataset);
                      }}
                      className="p-2 text-blue-600 hover:bg-blue-50 rounded-lg transition-colors"
                      title="Preview Data"
                    >
                      <Eye className="w-5 h-5" />
                    </button>
                  </div>
                </div>
              </div>
            ))
            }
          </div>
        </div>
      </div>
    </div>
  );
}
