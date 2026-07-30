import React, { useState, useEffect } from 'react';
import { GitBranch, Database, Table, ArrowRight, Search, ZoomIn, ZoomOut, Maximize2, RefreshCw, Loader } from 'lucide-react';
import axios from 'axios';

const API_BASE = 'http://localhost:8000/api';

/**
 * DataLineage - Visualize data lineage from source to destination
 * Shows table-level and column-level lineage with transformations
 */
export default function DataLineage() {
  const [viewMode, setViewMode] = useState('table'); // 'table' or 'column'
  const [selectedTable, setSelectedTable] = useState(null);
  const [zoomLevel, setZoomLevel] = useState(100);
  const [lineageGraph, setLineageGraph] = useState({ nodes: [], edges: [] });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [impactAnalysis, setImpactAnalysis] = useState(null);

  // Fetch lineage graph from backend  
  const fetchLineageGraph = async () => {
    try {
      setLoading(true);
      const response = await axios.get(`${API_BASE}/lineage/graph`);
      
      if (response.data.success) {
        const graph = response.data.graph;
        
        // Auto-layout nodes by layer
        const nodesByLayer = {};
        graph.nodes.forEach(node => {
          if (!nodesByLayer[node.layer]) {
            nodesByLayer[node.layer] = [];
          }
          nodesByLayer[node.layer].push(node);
        });
        
        // Position nodes
        const layers = ['source', 'bronze', 'silver', 'gold'];
        let positionedNodes = [];
        
        layers.forEach((layer, layerIdx) => {
          const nodesInLayer = nodesByLayer[layer] || [];
          nodesInLayer.forEach((node, nodeIdx) => {
            positionedNodes.push({
              ...node,
              x: 100 + (layerIdx * 300),
              y: 100 + (nodeIdx * 150)
            });
          });
        });
        
        setLineageGraph({
          nodes: positionedNodes,
          edges: graph.edges
        });
        
        // Select first table if none selected
        if (!selectedTable && positionedNodes.length > 0) {
          setSelectedTable(positionedNodes[0].id);
        }
      }
      
      setError(null);
    } catch (err) {
      console.error('Failed to fetch lineage:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };
  
  // Fetch impact analysis for selected table
  const fetchImpactAnalysis = async (table) => {
    if (!table) return;
    
    try {
      const response = await axios.get(`${API_BASE}/lineage/impact/${encodeURIComponent(table)}`);
      
      if (response.data.success) {
        setImpactAnalysis(response.data.impact);
      }
    } catch (err) {
      console.error('Failed to fetch impact analysis:', err);
    }
  };
  
  // Initial load
  useEffect(() => {
    fetchLineageGraph();
  }, []);
  
  // Fetch impact analysis when table selected
  useEffect(() => {
    if (selectedTable) {
      fetchImpactAnalysis(selectedTable);
    }
  }, [selectedTable]);
  
  // Filter nodes by search query
  const filteredNodes = lineageGraph.nodes.filter(node => 
    node.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
    node.full_name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const getLayerColor = (layer) => {
    switch(layer) {
      case 'bronze': return { bg: '#FEF3C7', border: '#F59E0B', text: '#92400E' };
      case 'silver': return { bg: '#E5E7EB', border: '#6B7280', text: '#1F2937' };
      case 'gold': return { bg: '#FEF9C3', border: '#EAB308', text: '#854D0E' };
      default: return { bg: '#F3F4F6', border: '#9CA3AF', text: '#374151' };
    }
  };

  return (
    <div className="h-screen flex flex-col bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 p-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
              <GitBranch className="w-6 h-6" />
              Data Lineage
            </h1>
            <p className="text-sm text-gray-500 mt-1">Track data flow from source to destination</p>
          </div>
          <div className="flex gap-2">
            <button
              onClick={fetchLineageGraph}
              disabled={loading}
              className="p-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 disabled:opacity-50"
            >
              <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
            </button>
            <button
              onClick={() => setZoomLevel(Math.max(50, zoomLevel - 10))}
              className="p-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50"
            >
              <ZoomOut className="w-4 h-4" />
            </button>
            <button
              onClick={() => setZoomLevel(100)}
              className="px-3 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50"
            >
              {zoomLevel}%
            </button>
            <button
              onClick={() => setZoomLevel(Math.min(200, zoomLevel + 10))}
              className="p-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50"
            >
              <ZoomIn className="w-4 h-4" />
            </button>
          </div>
        </div>

        {/* View Mode Toggle */}
        <div className="mt-4 flex gap-2">
          <button
            onClick={() => setViewMode('table')}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              viewMode === 'table'
                ? 'bg-blue-600 text-white'
                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
            }`}
          >
            Table-Level Lineage
          </button>
          <button
            onClick={() => setViewMode('column')}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              viewMode === 'column'
                ? 'bg-blue-600 text-white'
                : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
            }`}
          >
            Column-Level Lineage
          </button>
        </div>

        {/* Search */}
        <div className="mt-4">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400" />
            <input
              type="text"
              placeholder="Search tables, columns, or pipelines..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Lineage Visualization */}
        <div className="flex-1 overflow-hidden relative bg-white">
          {loading ? (
            <div className="h-full flex items-center justify-center">
              <div className="text-center">
                <Loader className="w-12 h-12 text-blue-500 mx-auto mb-4 animate-spin" />
                <p className="text-lg font-medium text-gray-900">Loading lineage graph...</p>
              </div>
            </div>
          ) : error ? (
            <div className="h-full flex items-center justify-center">
              <div className="text-center">
                <GitBranch className="w-12 h-12 text-red-500 mx-auto mb-4" />
                <p className="text-lg font-medium text-gray-900 mb-2">Failed to load lineage</p>
                <p className="text-sm text-gray-500 mb-4">{error}</p>
                <button
                  onClick={fetchLineageGraph}
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700"
                >
                  Retry
                </button>
              </div>
            </div>
          ) : lineageGraph.nodes.length === 0 ? (
            <div className="h-full flex items-center justify-center">
              <div className="text-center">
                <GitBranch className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                <p className="text-lg font-medium text-gray-900 mb-2">No lineage data  yet</p>
                <p className="text-sm text-gray-500">
                  Run some transformations to build the lineage graph
                </p>
              </div>
            </div>
          ) : viewMode === 'table' ? (
            <div className="h-full overflow-auto p-8">
              {/* Grid Background */}
              <div
                className="absolute inset-0"
                style={{
                  backgroundImage: `
                    linear-gradient(to right, #e5e7eb 1px, transparent 1px),
                    linear-gradient(to bottom, #e5e7eb 1px, transparent 1px)
                  `,
                  backgroundSize: '20px 20px'
                }}
              />

              {/* SVG for Connections */}
              <svg
                className="absolute inset-0 w-full h-full pointer-events-none"
                style={{ zIndex: 1, transform: `scale(${zoomLevel / 100})`, transformOrigin: '0 0' }}
              >
                {lineageGraph.edges.map((edge, idx) => {
                  const fromNode = lineageGraph.nodes.find(n => n.id === edge.from);
                  const toNode = lineageGraph.nodes.find(n => n.id === edge.to);
                  if (!fromNode || !toNode) return null;

                  const startX = fromNode.x + 200;
                  const startY = fromNode.y + 50;
                  const endX = toNode.x;
                  const endY = toNode.y + 50;
                  const midX = (startX + endX) / 2;

                  return (
                    <g key={idx}>
                      <path
                        d={`M ${startX} ${startY} C ${midX} ${startY}, ${midX} ${endY}, ${endX} ${endY}`}
                        stroke="#6B7280"
                        strokeWidth="2"
                        fill="none"
                        markerEnd="url(#arrowhead)"
                      />
                      {/* Transform labels */}
                      <foreignObject
                        x={midX - 50}
                        y={(startY + endY) / 2 - 15}
                        width="100"
                        height="30"
                      >
                        <div className="text-xs bg-yellow-100 text-yellow-800 px-2 py-1 rounded border border-yellow-300 text-center">
                          {edge.transforms && Array.isArray(edge.transforms) ? edge.transforms.join(' → ') : edge.transformation_type || 'transformation'}
                        </div>
                      </foreignObject>
                    </g>
                  );
                })}
                <defs>
                  <marker
                    id="arrowhead"
                    markerWidth="10"
                    markerHeight="10"
                    refX="8"
                    refY="3"
                    orient="auto"
                  >
                    <polygon points="0 0, 10 3, 0 6" fill="#6B7280" />
                  </marker>
                </defs>
              </svg>

              {/* Nodes */}
              <div
                className="relative"
                style={{ zIndex: 2, transform: `scale(${zoomLevel / 100})`, transformOrigin: '0 0' }}
              >
                {filteredNodes.map(node => {
                  const colors = getLayerColor(node.layer);
                  const isSelected = selectedTable === node.id;
                  return (
                    <div
                      key={node.id}
                      onClick={() => setSelectedTable(node.id)}
                      className={`absolute bg-white rounded-lg shadow-lg border-2 cursor-pointer hover:shadow-xl transition-all ${
                        isSelected ? 'ring-2 ring-blue-500' : ''
                      }`}
                      style={{
                        left: node.x,
                        top: node.y,
                        width: '200px',
                        borderColor: isSelected ? '#3B82F6' : colors.border
                      }}
                    >
                      {/* Node Header */}
                      <div
                        className="p-3 rounded-t-lg"
                        style={{ backgroundColor: colors.bg }}
                      >
                        <div className="flex items-center gap-2">
                          <Database className="w-4 h-4" style={{ color: colors.border }} />
                          <span
                            className="text-xs font-bold uppercase"
                            style={{ color: colors.text }}
                          >
                            {node.layer}
                          </span>
                        </div>
                      </div>

                      {/* Node Content */}
                      <div className="p-3">
                        <div className="font-semibold text-sm text-gray-900 mb-1">{node.name}</div>
                        <div className="text-xs text-gray-500">
                          {node.transformation_count} transformation{node.transformation_count !== 1 ? 's' : ''}
                        </div>
                      </div>

                      {/* Connection Points */}
                      <div
                        className="absolute -left-2 top-1/2 transform -translate-y-1/2 w-4 h-4 bg-white border-2 rounded-full"
                        style={{ borderColor: colors.border }}
                      />
                      <div
                        className="absolute -right-2 top-1/2 transform -translate-y-1/2 w-4 h-4 bg-white border-2 rounded-full"
                        style={{ borderColor: colors.border }}
                      />
                    </div>
                  );
                })}
              </div>
            </div>
          ) : (
            // Column-Level Lineage - Coming Soon
            <div className="h-full flex items-center justify-center">
              <div className="text-center max-w-md">
                <Table className="w-16 h-16 text-gray-400 mx-auto mb-4" />
                <h3 className="text-xl font-semibold text-gray-900 mb-2">
                  Column-Level Lineage
                </h3>
                <p className="text-gray-600 mb-4">
                  Track transformations at the column level - see how individual fields are transformed from source to destination.
                </p>
                <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-left">
                  <h4 className="font-semibold text-blue-900 mb-2">Coming Soon:</h4>
                  <ul className="text-sm text-blue-800 space-y-1">
                    <li>• Field-level transformation tracking</li>
                    <li>• Type conversion history</li>
                    <li>• Computed column origins</li>
                    <li>• Column dependency graph</li>
                  </ul>
                </div>
                <button
                  onClick={() => setViewMode('table')}
                  className="mt-6 px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm font-medium"
                >
                  View Table-Level Lineage
                </button>
              </div>
            </div>
          )}
        </div>

        {/* Right Sidebar - Impact Analysis */}
        <div className="w-80 bg-white border-l border-gray-200 overflow-y-auto">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">Impact Analysis</h2>
            <p className="text-xs text-gray-500 mt-1">Upstream & downstream dependencies</p>
          </div>

          <div className="p-4">
            {!selectedTable ? (
              <div className="text-center py-8">
                <GitBranch className="w-12 h-12 text-gray-400 mx-auto mb-3" />
                <p className="text-sm text-gray-600">Select a table to view impact analysis</p>
              </div>
            ) : !impactAnalysis ? (
              <div className="text-center py-8">
                <Loader className="w-8 h-8 text-blue-500 mx-auto mb-3 animate-spin" />
                <p className="text-sm text-gray-600">Loading impact analysis...</p>
              </div>
            ) : (
              <>
                {/* Upstream */}
                <div className="mb-6">
                  <h3 className="text-sm font-semibold text-gray-900 mb-3">
                    Upstream Sources ({impactAnalysis.upstream_sources?.length || 0})
                  </h3>
                  <div className="space-y-2">
                    {impactAnalysis.upstream_sources && impactAnalysis.upstream_sources.length > 0 ? (
                      impactAnalysis.upstream_sources.map((source, idx) => (
                        <div key={idx} className="p-3 bg-orange-50 rounded-lg border border-orange-200">
                          <div className="flex items-center gap-2 mb-1">
                            <Table className="w-4 h-4 text-orange-600" />
                            <span className="text-sm font-medium text-gray-900">{source}</span>
                          </div>
                        </div>
                      ))
                    ) : (
                      <p className="text-xs text-gray-500 italic">No upstream sources</p>
                    )}
                  </div>
                </div>

                {/* Downstream */}
                <div>
                  <h3 className="text-sm font-semibold text-gray-900 mb-3">
                    Downstream Dependencies ({impactAnalysis.downstream_dependencies?.length || 0})
                  </h3>
                  <div className="space-y-2">
                    {impactAnalysis.downstream_dependencies && impactAnalysis.downstream_dependencies.length > 0 ? (
                      impactAnalysis.downstream_dependencies.map((dest, idx) => (
                        <div key={idx} className="p-3 bg-blue-50 rounded-lg border border-blue-200">
                          <div className="flex items-center gap-2 mb-1">
                            <Table className="w-4 h-4 text-blue-600" />
                            <span className="text-sm font-medium text-gray-900">{dest}</span>
                          </div>
                        </div>
                      ))
                    ) : (
                      <p className="text-xs text-gray-500 italic">No downstream dependencies</p>
                    )}
                  </div>
                </div>

                {/* Impact Warning */}
                {impactAnalysis.downstream_dependencies && impactAnalysis.downstream_dependencies.length > 0 && (
                  <div className="mt-6 p-4 bg-yellow-50 rounded-lg border border-yellow-200">
                    <h4 className="text-sm font-semibold text-yellow-900 mb-2">⚠️ Impact Warning</h4>
                    <p className="text-xs text-yellow-800">
                      Changes to this table will affect {impactAnalysis.total_downstream} downstream table(s).
                      Risk Level: <span className="font-bold uppercase">{impactAnalysis.risk_level}</span>
                    </p>
                  </div>
                )}

                {/* Summary Stats */}
                <div className="mt-6 p-4 bg-gray-50 rounded-lg">
                  <h4 className="text-sm font-semibold text-gray-900 mb-3">Summary</h4>
                  <div className="space-y-2 text-xs">
                    <div className="flex justify-between">
                      <span className="text-gray-600">Total Upstream:</span>
                      <span className="font-medium text-gray-900">{impactAnalysis.total_upstream || 0}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-600">Total Downstream:</span>
                      <span className="font-medium text-gray-900">{impactAnalysis.total_downstream || 0}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-gray-600">Risk Level:</span>
                      <span className={`font-bold uppercase ${
                        impactAnalysis.risk_level === 'high' ? 'text-red-600' :
                        impactAnalysis.risk_level === 'medium' ? 'text-yellow-600' : 'text-green-600'
                      }`}>
                        {impactAnalysis.risk_level}
                      </span>
                    </div>
                  </div>
                </div>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
