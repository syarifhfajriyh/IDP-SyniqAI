import React, { useState, useEffect } from 'react';
import { Plus, Play, Save, Settings, Trash2, Copy, GitBranch, Database, Filter, Shuffle, Calculator, Table as TableIcon, Code, HelpCircle, AlertCircle, CheckCircle, Loader } from 'lucide-react';

/**
 * HelpTooltip Component - Displays help information on hover or click
 */
const HelpTooltip = ({ children }) => {
  const [showTooltip, setShowTooltip] = useState(false);
  
  return (
    <div className="relative inline-block ml-2">
      <button
        type="button"
        onMouseEnter={() => setShowTooltip(true)}
        onMouseLeave={() => setShowTooltip(false)}
        onClick={() => setShowTooltip(!showTooltip)}
        className="text-blue-500 hover:text-blue-700 focus:outline-none"
      >
        <HelpCircle className="w-4 h-4" />
      </button>
      {showTooltip && (
        <div className="absolute left-6 top-0 z-50 w-64 p-3 bg-gray-900 text-white text-xs rounded-lg shadow-lg">
          <div className="space-y-1">{children}</div>
        </div>
      )}
    </div>
  );
};

/**
 * PipelineBuilder - Visual DAG pipeline builder for data transformations
 * Drag-and-drop interface for building transformation pipelines
 */
export default function PipelineBuilder() {
  const [pipelineName, setPipelineName] = useState('New Pipeline');
  const [nodes, setNodes] = useState([
    { id: 1, type: 'source', name: 'finance_transactions', x: 100, y: 150, config: { table: 'finance_transactions' } },
    { id: 2, type: 'join', name: 'Join Tables', x: 300, y: 150, config: { join_type: 'inner', right_table: 'finance_transactions', on: 'user_id' } },
    { id: 3, type: 'filter', name: 'Filter Rows', x: 500, y: 150, config: { condition: 'amount > 0' } },
    { id: 4, type: 'destination', name: 'pipeline_output', x: 700, y: 150, config: { table: 'pipeline_output', format: 'parquet' } }
  ]);
  const [connections, setConnections] = useState([
    { from: 1, to: 2 },
    { from: 2, to: 3 },
    { from: 3, to: 4 }
  ]);
  const [selectedNode, setSelectedNode] = useState(null);
  const [isDragging, setIsDragging] = useState(false);
  const [draggedNode, setDraggedNode] = useState(null);
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
  const [isConnecting, setIsConnecting] = useState(false);
  const [connectionStart, setConnectionStart] = useState(null);
  const [mousePosition, setMousePosition] = useState({ x: 0, y: 0 });
  const [validationErrors, setValidationErrors] = useState([]);
  const [isExecuting, setIsExecuting] = useState(false);
  const [executionResult, setExecutionResult] = useState(null);
  const [savedPipelines, setSavedPipelines] = useState([]);
  const [lastSaved, setLastSaved] = useState(null);

  // Load saved pipelines from localStorage on mount
  useEffect(() => {
    const saved = localStorage.getItem('syniqai_pipelines');
    if (saved) {
      try {
        setSavedPipelines(JSON.parse(saved));
      } catch (e) {
        console.error('Failed to load saved pipelines:', e);
      }
    }
  }, []);

  // DAG Validation - Check if pipeline is valid
  const validatePipeline = () => {
    const errors = [];

    // Check if there's at least one source node
    const sourceNodes = nodes.filter(n => n.type === 'source');
    if (sourceNodes.length === 0) {
      errors.push('Pipeline must have at least one Source node');
    }

    // Check if there's at least one destination node
    const destNodes = nodes.filter(n => n.type === 'destination');
    if (destNodes.length === 0) {
      errors.push('Pipeline must have at least one Destination node');
    }

    // Check if all nodes have required configuration
    nodes.forEach(node => {
      if (node.type === 'source' && !node.config.table) {
        errors.push(`Source node "${node.name}" is missing table configuration`);
      }
      if (node.type === 'destination' && !node.config.table) {
        errors.push(`Destination node "${node.name}" is missing table configuration`);
      }
      if (node.type === 'join') {
        if (!node.config.right_table) {
          errors.push(`Join node "${node.name}" is missing right_table configuration`);
        }
        if (!node.config.on) {
          errors.push(`Join node "${node.name}" is missing join key (on) configuration`);
        }
      }
      if (node.type === 'filter' && !node.config.condition) {
        errors.push(`Filter node "${node.name}" is missing condition configuration`);
      }
    });

    // Check if all transformation nodes are connected
    nodes.forEach(node => {
      if (node.type !== 'source') {
        const hasInput = connections.some(c => c.to === node.id);
        if (!hasInput) {
          errors.push(`Node "${node.name}" has no input connection`);
        }
      }
      if (node.type !== 'destination') {
        const hasOutput = connections.some(c => c.from === node.id);
        if (!hasOutput) {
          errors.push(`Node "${node.name}" has no output connection`);
        }
      }
    });

    setValidationErrors(errors);
    return errors.length === 0;
  };

  // Convert DAG to transformation pipeline format
  const convertDAGToPipeline = () => {
    // Find source node
    const sourceNode = nodes.find(n => n.type === 'source');
    if (!sourceNode) {
      throw new Error('No source node found');
    }

    // Find destination node
    const destNode = nodes.find(n => n.type === 'destination');
    if (!destNode) {
      throw new Error('No destination node found');
    }

    // Build execution order by following connections
    const executionOrder = [];
    const visited = new Set();
    
    const traverse = (nodeId) => {
      if (visited.has(nodeId)) return;
      visited.add(nodeId);
      
      const node = nodes.find(n => n.id === nodeId);
      if (!node) return;
      
      // Add transformation nodes (not source/destination)
      if (node.type !== 'source' && node.type !== 'destination') {
        executionOrder.push(node);
      }
      
      // Find next connected nodes
      const nextConnections = connections.filter(c => c.from === nodeId);
      nextConnections.forEach(conn => traverse(conn.to));
    };
    
    traverse(sourceNode.id);

    // Convert nodes to transformation steps
    const transformations = executionOrder.map(node => {
      switch (node.type) {
        case 'join':
          const joinType = node.config.join_type || 'inner';
          return {
            operation: `join_${joinType}`,
            params: {
              right_table: node.config.right_table,
              on: node.config.on
            }
          };
        
        case 'filter':
          return {
            operation: 'filter',
            params: {
              condition: node.config.condition
            }
          };
        
        case 'select':
          return {
            operation: 'select_columns',
            params: {
              columns: Array.isArray(node.config.columns) 
                ? node.config.columns 
                : node.config.columns?.split('\n').filter(c => c.trim())
            }
          };
        
        case 'aggregate':
          const aggType = node.config.agg || 'sum';
          return {
            operation: `group_${aggType}`,
            params: {
              group_by: node.config.groupBy,
              agg_columns: node.config.aggColumns
            }
          };
        
        case 'transform':
          const operation = node.config.operation;
          const params = {};
          
          if (operation === 'extract_datetime') {
            params.column = node.config.column;
            params.parts = node.config.parts;
          } else if (operation === 'log_transform' || operation === 'normalize' || operation === 'standardize') {
            params.columns = node.config.columns;
          } else if (operation === 'drop_duplicates') {
            params.columns = node.config.columns;
          } else if (operation === 'fill_null') {
            params.columns = node.config.columns;
            params.fill_value = node.config.fillValue;
          }
          
          return {
            operation: operation,
            params: params,
            enabled: true
          };
        
        case 'sql':
          return {
            operation: 'custom_sql',
            params: {
              query: node.config.query
            }
          };
        
        default:
          return null;
      }
    }).filter(t => t !== null);

    // Build final pipeline configuration
    return {
      source_table: sourceNode.config.table,
      target_table: destNode.config.table,
      transformations,
      output_config: {
        domain: 'finance', // Default domain
        format: destNode.config.format || 'parquet'
      }
    };
  };

  // Execute pipeline via API
  const executePipeline = async () => {
    // Validate first
    if (!validatePipeline()) {
      return;
    }

    setIsExecuting(true);
    setExecutionResult(null);

    try {
      // Convert DAG to pipeline config
      const pipelineConfig = convertDAGToPipeline();
      
      console.log('🚀 Executing pipeline:', pipelineConfig);
      console.log('📊 Pipeline details:', {
        source_table: pipelineConfig.source_table,
        target_table: pipelineConfig.target_table,
        transformations: pipelineConfig.transformations.length,
        format: pipelineConfig.output_config.format
      });

      // Call backend API
      const response = await fetch('http://localhost:8000/api/silver/execute-transformation', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(pipelineConfig)
      });

      if (!response.ok) {
        const errorData = await response.json();
        console.error('❌ Backend error response:', errorData);
        
        // Handle different error formats
        let errorMessage = 'Unknown error occurred';
        
        if (typeof errorData.detail === 'string') {
          errorMessage = errorData.detail;
        } else if (Array.isArray(errorData.detail)) {
          errorMessage = errorData.detail.map(err => {
            if (typeof err === 'string') return err;
            if (err.msg) return `${err.loc?.join('.') || 'Error'}: ${err.msg}`;
            return JSON.stringify(err);
          }).join('; ');
        } else if (typeof errorData.detail === 'object') {
          errorMessage = JSON.stringify(errorData.detail, null, 2);
        } else if (errorData.message) {
          errorMessage = errorData.message;
        } else {
          errorMessage = `HTTP ${response.status}: ${response.statusText}`;
        }
        
        throw new Error(errorMessage);
      }

      const result = await response.json();
      
      setExecutionResult({
        success: true,
        ...result.result,
        message: 'Pipeline executed successfully!'
      });

      console.log('✅ Pipeline execution complete:', result);

    } catch (error) {
      console.error('❌ Pipeline execution failed:', error);
      console.error('Error details:', {
        message: error.message,
        stack: error.stack,
        name: error.name
      });
      
      // Format error message for display
      let displayError = error.message || 'Unknown error occurred';
      
      // If it's a network error
      if (error.message.includes('Failed to fetch') || error.message.includes('NetworkError')) {
        displayError = 'Cannot connect to backend server. Please ensure the backend is running on http://localhost:8000';
      }
      
      setExecutionResult({
        success: false,
        error: displayError,
        fullError: error.stack || error.toString()
      });
    } finally {
      setIsExecuting(false);
    }
  };

  // Save pipeline to localStorage
  const savePipelineToStorage = () => {
    const pipeline = {
      id: Date.now(),
      name: pipelineName,
      nodes,
      connections,
      createdAt: new Date().toISOString()
    };

    const updated = [...savedPipelines, pipeline];
    setSavedPipelines(updated);
    localStorage.setItem('syniqai_pipelines', JSON.stringify(updated));
    setLastSaved(new Date());
    
    alert(`Pipeline "${pipelineName}" saved successfully!`);
  };

  // Load pipeline from saved list
  const loadPipeline = (pipeline) => {
    setPipelineName(pipeline.name);
    setNodes(pipeline.nodes);
    setConnections(pipeline.connections);
    setSelectedNode(null);
    setExecutionResult(null);
    setValidationErrors([]);
  };

  // Apply configuration changes to selected node
  const applyConfiguration = () => {
    if (!selectedNode) return;
    
    // Configuration is already updated via onChange handlers
    // This button provides explicit save confirmation
    alert(`Configuration applied to "${selectedNode.name}"`);
    
    // Revalidate pipeline
    validatePipeline();
  };

  // Handle node dragging
  const handleNodeMouseDown = (e, node) => {
    if (e.target.closest('button')) return; // Don't drag when clicking buttons
    
    e.stopPropagation();
    setIsDragging(true);
    setDraggedNode(node);
    
    const rect = e.currentTarget.getBoundingClientRect();
    setDragOffset({
      x: e.clientX - rect.left,
      y: e.clientY - rect.top
    });
  };

  const handleCanvasMouseMove = (e) => {
    if (isDragging && draggedNode) {
      // Update node position
      const canvas = e.currentTarget;
      const rect = canvas.getBoundingClientRect();
      const x = e.clientX - rect.left - dragOffset.x;
      const y = e.clientY - rect.top - dragOffset.y;
      
      setNodes(nodes.map(n => 
        n.id === draggedNode.id 
          ? { ...n, x: Math.max(0, Math.min(x, rect.width - 120)), y: Math.max(0, Math.min(y, rect.height - 100)) }
          : n
      ));
    } else if (isConnecting) {
      // Update mouse position for connection preview
      const canvas = e.currentTarget;
      const rect = canvas.getBoundingClientRect();
      setMousePosition({
        x: e.clientX - rect.left,
        y: e.clientY - rect.top
      });
    }
  };

  const handleCanvasMouseUp = () => {
    if (isDragging) {
      setIsDragging(false);
      setDraggedNode(null);
      
      // Update selected node if it was dragged
      if (draggedNode && selectedNode?.id === draggedNode.id) {
        const updatedNode = nodes.find(n => n.id === draggedNode.id);
        setSelectedNode(updatedNode);
      }
    } else if (isConnecting) {
      setIsConnecting(false);
      setConnectionStart(null);
    }
  };

  // Handle connection creation
  const handleConnectionStart = (e, nodeId) => {
    e.stopPropagation();
    setIsConnecting(true);
    setConnectionStart(nodeId);
    
    const canvas = e.currentTarget.closest('.canvas-area');
    const rect = canvas.getBoundingClientRect();
    setMousePosition({
      x: e.clientX - rect.left,
      y: e.clientY - rect.top
    });
  };

  const handleConnectionEnd = (e, nodeId) => {
    e.stopPropagation();
    
    if (isConnecting && connectionStart && connectionStart !== nodeId) {
      // Check if connection already exists
      const exists = connections.some(
        c => (c.from === connectionStart && c.to === nodeId) || 
             (c.from === nodeId && c.to === connectionStart)
      );
      
      if (!exists) {
        // Add new connection
        setConnections([...connections, { from: connectionStart, to: nodeId }]);
        
        // Revalidate
        setTimeout(() => validatePipeline(), 0);
      }
    }
    
    setIsConnecting(false);
    setConnectionStart(null);
  };

  // Delete connection
  const deleteConnection = (from, to) => {
    setConnections(connections.filter(c => !(c.from === from && c.to === to)));
    setTimeout(() => validatePipeline(), 0);
  };

  const nodeTypes = [
    { type: 'source', name: 'Source Table', icon: Database, color: '#10b981' },
    { type: 'select', name: 'Select Columns', icon: TableIcon, color: '#3b82f6' },
    { type: 'filter', name: 'Filter Rows', icon: Filter, color: '#f59e0b' },
    { type: 'join', name: 'Join Tables', icon: GitBranch, color: '#8b5cf6' },
    { type: 'aggregate', name: 'Aggregate', icon: Calculator, color: '#ec4899' },
    { type: 'transform', name: 'Transform', icon: Shuffle, color: '#06b6d4' },
    { type: 'sql', name: 'Custom SQL', icon: Code, color: '#64748b' },
    { type: 'destination', name: 'Destination', icon: Database, color: '#ef4444' }
  ];

  const getNodeIcon = (type) => {
    const nodeType = nodeTypes.find(nt => nt.type === type);
    return nodeType ? nodeType.icon : Database;
  };

  const getNodeColor = (type) => {
    const nodeType = nodeTypes.find(nt => nt.type === type);
    return nodeType ? nodeType.color : '#6B7280';
  };

  const addNode = (type) => {
    const newNode = {
      id: nodes.length + 1,
      type,
      name: nodeTypes.find(nt => nt.type === type)?.name || 'New Node',
      x: 300 + Math.random() * 200,
      y: 100 + Math.random() * 200,
      config: {}
    };
    setNodes([...nodes, newNode]);
  };

  const deleteNode = (nodeId) => {
    setNodes(nodes.filter(n => n.id !== nodeId));
    setConnections(connections.filter(c => c.from !== nodeId && c.to !== nodeId));
    if (selectedNode?.id === nodeId) {
      setSelectedNode(null);
    }
    // Revalidate after deletion
    setTimeout(() => validatePipeline(), 0);
  };

  return (
    <div className="h-screen flex bg-gray-50">
      {/* Left Sidebar - Node Palette */}
      <div className="w-64 bg-white border-r border-gray-200 overflow-y-auto">
        <div className="p-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900">Transformation Nodes</h2>
          <p className="text-xs text-gray-500 mt-1">Drag to canvas to add</p>
        </div>
        <div className="p-4 space-y-2">
          {nodeTypes.map(nodeType => {
            const Icon = nodeType.icon;
            return (
              <button
                key={nodeType.type}
                onClick={() => addNode(nodeType.type)}
                className="w-full flex items-center gap-3 p-3 border border-gray-200 rounded-lg hover:bg-gray-50 hover:border-gray-300 transition-all text-left"
              >
                <div
                  className="w-8 h-8 rounded-lg flex items-center justify-center"
                  style={{ backgroundColor: `${nodeType.color}20` }}
                >
                  <Icon className="w-4 h-4" style={{ color: nodeType.color }} />
                </div>
                <div className="flex-1">
                  <div className="text-sm font-medium text-gray-900">{nodeType.name}</div>
                </div>
              </button>
            );
          })}
        </div>
      </div>

      {/* Main Canvas */}
      <div className="flex-1 flex flex-col">
        {/* Top Toolbar */}
        <div className="bg-white border-b border-gray-200 p-4">
          <div className="flex items-center justify-between">
            <div className="flex-1 max-w-md">
              <input
                type="text"
                value={pipelineName}
                onChange={(e) => setPipelineName(e.target.value)}
                className="text-xl font-bold text-gray-900 bg-transparent border-none outline-none focus:ring-0 w-full"
              />
              <p className="text-sm text-gray-500">
                {lastSaved ? `Last saved: ${lastSaved.toLocaleTimeString()}` : 'Not saved yet'}
              </p>
            </div>
            <div className="flex gap-2">
              <button
                onClick={validatePipeline}
                className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50 flex items-center gap-2"
              >
                <AlertCircle className="w-4 h-4" />
                Validate
              </button>
              <button
                onClick={savePipelineToStorage}
                className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50 flex items-center gap-2"
              >
                <Save className="w-4 h-4" />
                Save
              </button>
              <button
                onClick={executePipeline}
                disabled={isExecuting}
                className={`px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 flex items-center gap-2 ${
                  isExecuting ? 'opacity-50 cursor-not-allowed' : ''
                }`}
              >
                {isExecuting ? (
                  <>
                    <Loader className="w-4 h-4 animate-spin" />
                    Executing...
                  </>
                ) : (
                  <>
                    <Play className="w-4 h-4" />
                    Run Pipeline
                  </>
                )}
              </button>
            </div>
          </div>
        </div>

        {/* Canvas Area */}
        <div 
          className="flex-1 overflow-hidden relative bg-gray-50 canvas-area"
          onMouseMove={handleCanvasMouseMove}
          onMouseUp={handleCanvasMouseUp}
        >
          {/* Validation Errors Banner */}
          {validationErrors.length > 0 && (
            <div className="absolute top-4 left-1/2 transform -translate-x-1/2 z-50 max-w-2xl w-full mx-4">
              <div className="bg-red-50 border border-red-200 rounded-lg p-4 shadow-lg">
                <div className="flex items-start gap-3">
                  <AlertCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
                  <div className="flex-1">
                    <h3 className="text-sm font-semibold text-red-900 mb-2">
                      Pipeline Validation Errors ({validationErrors.length})
                    </h3>
                    <ul className="space-y-1">
                      {validationErrors.map((error, idx) => (
                        <li key={idx} className="text-xs text-red-700">• {error}</li>
                      ))}
                    </ul>
                  </div>
                  <button
                    onClick={() => setValidationErrors([])}
                    className="text-red-400 hover:text-red-600"
                  >
                    ×
                  </button>
                </div>
              </div>
            </div>
          )}

          {/* Execution Results Banner */}
          {executionResult && (
            <div className="absolute top-4 left-1/2 transform -translate-x-1/2 z-50 max-w-2xl w-full mx-4">
              <div className={`border rounded-lg p-4 shadow-lg ${
                executionResult.success 
                  ? 'bg-green-50 border-green-200' 
                  : 'bg-red-50 border-red-200'
              }`}>
                <div className="flex items-start gap-3">
                  {executionResult.success ? (
                    <CheckCircle className="w-5 h-5 text-green-600 flex-shrink-0 mt-0.5" />
                  ) : (
                    <AlertCircle className="w-5 h-5 text-red-600 flex-shrink-0 mt-0.5" />
                  )}
                  <div className="flex-1">
                    <h3 className={`text-sm font-semibold mb-2 ${
                      executionResult.success ? 'text-green-900' : 'text-red-900'
                    }`}>
                      {executionResult.success ? '✅ Pipeline Executed Successfully!' : '❌ Execution Failed'}
                    </h3>
                    {executionResult.success ? (
                      <div className="text-xs space-y-1">
                        <div className="text-green-700">
                          <strong>Input Rows:</strong> {executionResult.input_rows?.toLocaleString() || 'N/A'}
                        </div>
                        <div className="text-green-700">
                          <strong>Output Rows:</strong> {executionResult.output_rows?.toLocaleString() || 'N/A'}
                        </div>
                        <div className="text-green-700">
                          <strong>Duration:</strong> {executionResult.duration || 'N/A'}
                        </div>
                        <div className="text-green-700">
                          <strong>Output:</strong> {executionResult.output_path || 'N/A'}
                        </div>
                      </div>
                    ) : (
                      <div className="space-y-2">
                        <div className="text-xs text-red-700 font-medium">
                          {executionResult.error}
                        </div>
                        {executionResult.fullError && (
                          <details className="text-xs">
                            <summary className="text-red-600 cursor-pointer hover:text-red-800">
                              Show full error details
                            </summary>
                            <pre className="mt-2 p-2 bg-red-100 rounded text-red-800 overflow-x-auto max-h-32 text-xs">
                              {executionResult.fullError}
                            </pre>
                          </details>
                        )}
                      </div>
                    )}
                  </div>
                  <button
                    onClick={() => setExecutionResult(null)}
                    className={executionResult.success ? 'text-green-400 hover:text-green-600' : 'text-red-400 hover:text-red-600'}
                  >
                    ×
                  </button>
                </div>
              </div>
            </div>
          )}

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
          <svg className="absolute inset-0 w-full h-full pointer-events-none" style={{ zIndex: 1 }}>
            {connections.map((conn, idx) => {
              const fromNode = nodes.find(n => n.id === conn.from);
              const toNode = nodes.find(n => n.id === conn.to);
              if (!fromNode || !toNode) return null;

              const startX = fromNode.x + 120;
              const startY = fromNode.y + 40;
              const endX = toNode.x;
              const endY = toNode.y + 40;
              const midX = (startX + endX) / 2;

              return (
                <g key={idx}>
                  <path
                    d={`M ${startX} ${startY} C ${midX} ${startY}, ${midX} ${endY}, ${endX} ${endY}`}
                    stroke="#6B7280"
                    strokeWidth="2"
                    fill="none"
                    markerEnd="url(#arrowhead)"
                    className="pointer-events-auto cursor-pointer hover:stroke-red-500 transition-colors"
                    onClick={(e) => {
                      e.stopPropagation();
                      if (window.confirm('Delete this connection?')) {
                        deleteConnection(conn.from, conn.to);
                      }
                    }}
                  />
                </g>
              );
            })}
            
            {/* Connection preview while dragging */}
            {isConnecting && connectionStart && (
              <g>
                <path
                  d={`M ${nodes.find(n => n.id === connectionStart).x + 120} ${nodes.find(n => n.id === connectionStart).y + 40} L ${mousePosition.x} ${mousePosition.y}`}
                  stroke="#3B82F6"
                  strokeWidth="2"
                  strokeDasharray="5,5"
                  fill="none"
                />
              </g>
            )}
            
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
          <div className="absolute inset-0 p-8" style={{ zIndex: 2 }}>
            {nodes.map(node => {
              const Icon = getNodeIcon(node.type);
              const color = getNodeColor(node.type);
              const isSelected = selectedNode?.id === node.id;

              return (
                <div
                  key={node.id}
                  className={`absolute bg-white rounded-lg shadow-md border-2 transition-all ${
                    isSelected ? 'ring-2 ring-blue-500 border-blue-500' : 'border-gray-200 hover:border-gray-400'
                  } ${isDragging && draggedNode?.id === node.id ? 'cursor-grabbing opacity-75' : 'cursor-grab'}`}
                  style={{
                    left: node.x,
                    top: node.y,
                    width: '120px',
                    userSelect: 'none'
                  }}
                  onMouseDown={(e) => handleNodeMouseDown(e, node)}
                  onClick={(e) => {
                    if (!isDragging) {
                      e.stopPropagation();
                      setSelectedNode(node);
                    }
                  }}
                >
                  {/* Node Header */}
                  <div
                    className="p-2 rounded-t-lg flex items-center justify-center"
                    style={{ backgroundColor: `${color}20` }}
                  >
                    <Icon className="w-5 h-5" style={{ color }} />
                  </div>

                  {/* Node Content */}
                  <div className="p-2 text-center">
                    <div className="text-xs font-semibold text-gray-900 truncate">
                      {node.name}
                    </div>
                    {node.type === 'join' && (
                      <div className="text-xs text-blue-600 font-medium mt-1">
                        ⭐ Spark
                      </div>
                    )}
                    {node.type === 'source' && (
                      <div className="text-xs text-gray-500 mt-1 truncate">
                        {node.config.table || 'No table'}
                      </div>
                    )}
                    {node.type === 'destination' && (
                      <div className="text-xs text-gray-500 mt-1 truncate">
                        {node.config.table || 'No table'}
                      </div>
                    )}
                  </div>

                  {/* Node Actions */}
                  {isSelected && (
                    <div className="absolute -top-8 left-0 right-0 flex justify-center gap-1">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          deleteNode(node.id);
                        }}
                        className="p-1 bg-red-500 text-white rounded hover:bg-red-600"
                      >
                        <Trash2 className="w-3 h-3" />
                      </button>
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          alert('Configure node');
                        }}
                        className="p-1 bg-gray-700 text-white rounded hover:bg-gray-800"
                      >
                        <Settings className="w-3 h-3" />
                      </button>
                    </div>
                  )}

                  {/* Connection Points */}
                  <div
                    className="absolute -left-2 top-1/2 transform -translate-y-1/2 w-4 h-4 bg-white border-2 border-green-500 rounded-full cursor-crosshair hover:bg-green-50 hover:scale-110 transition-all z-10"
                    title="Input - Click to connect"
                    onMouseDown={(e) => {
                      e.stopPropagation();
                    }}
                    onMouseUp={(e) => handleConnectionEnd(e, node.id)}
                  />
                  <div
                    className="absolute -right-2 top-1/2 transform -translate-y-1/2 w-4 h-4 bg-white border-2 border-blue-500 rounded-full cursor-crosshair hover:bg-blue-50 hover:scale-110 transition-all z-10"
                    title="Output - Drag to connect"
                    onMouseDown={(e) => handleConnectionStart(e, node.id)}
                  />
                </div>
              );
            })}
          </div>

          {/* Empty State */}
          {nodes.length === 0 && (
            <div className="absolute inset-0 flex items-center justify-center">
              <div className="text-center">
                <Plus className="w-12 h-12 text-gray-400 mx-auto mb-4" />
                <p className="text-lg font-medium text-gray-900 mb-2">No nodes yet</p>
                <p className="text-sm text-gray-500">Add nodes from the left panel to start building your pipeline</p>
              </div>
            </div>
          )}
        </div>

        {/* Pipeline Stats Footer */}
        <div className="bg-white border-t border-gray-200 p-4">
          <div className="flex items-center justify-between text-sm">
            <div className="flex gap-6 text-gray-600">
              <span>{nodes.length} Nodes</span>
              <span>{connections.length} Connections</span>
              {validationErrors.length === 0 ? (
                <span className="text-green-600 font-medium flex items-center gap-1">
                  <CheckCircle className="w-4 h-4" />
                  Valid Pipeline
                </span>
              ) : (
                <span className="text-red-600 font-medium flex items-center gap-1">
                  <AlertCircle className="w-4 h-4" />
                  {validationErrors.length} Error{validationErrors.length !== 1 ? 's' : ''}
                </span>
              )}
            </div>
            <div className="text-gray-500 text-xs flex items-center gap-4">
              <span>💡 <strong>Tip:</strong> Drag nodes to move • Drag from blue port to green port to connect • Click connections to delete</span>
              <span>{savedPipelines.length} saved</span>
            </div>
          </div>
        </div>
      </div>

      {/* Right Sidebar - Node Configuration */}
      {selectedNode && (
        <div className="w-80 bg-white border-l border-gray-200 overflow-y-auto">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">Node Configuration</h2>
            <p className="text-xs text-gray-500 mt-1">{selectedNode.name}</p>
          </div>
          <div className="p-4 space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Node Name</label>
              <input
                type="text"
                value={selectedNode.name}
                onChange={(e) => {
                  setNodes(nodes.map(n => n.id === selectedNode.id ? { ...n, name: e.target.value } : n));
                  setSelectedNode({ ...selectedNode, name: e.target.value });
                }}
                className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>

            {selectedNode.type === 'source' && (
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Source Table</label>
                <input
                  type="text"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  placeholder="finance_transactions"
                  value={selectedNode.config.table || ''}
                  onChange={(e) => {
                    setNodes(nodes.map(n => 
                      n.id === selectedNode.id 
                        ? { ...n, config: { ...n.config, table: e.target.value } }
                        : n
                    ));
                    setSelectedNode({ 
                      ...selectedNode, 
                      config: { ...selectedNode.config, table: e.target.value } 
                    });
                  }}
                />
                <p className="text-xs text-gray-500 mt-1">Enter table name from Bronze layer (e.g., finance_transactions)</p>
              </div>
            )}

            {selectedNode.type === 'filter' && (
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Filter Condition</label>
                <textarea
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm"
                  rows={4}
                  placeholder="amount > 0 AND status = 'completed'"
                  value={selectedNode.config.condition || ''}
                  onChange={(e) => {
                    setNodes(nodes.map(n => 
                      n.id === selectedNode.id 
                        ? { ...n, config: { ...n.config, condition: e.target.value } }
                        : n
                    ));
                    setSelectedNode({ 
                      ...selectedNode, 
                      config: { ...selectedNode.config, condition: e.target.value } 
                    });
                  }}
                />
              </div>
            )}

            {selectedNode.type === 'select' && (
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Select Columns</label>
                <textarea
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm"
                  rows={6}
                  placeholder="transaction_id&#10;amount&#10;user_id"
                  value={selectedNode.config.columns?.join('\n') || ''}
                  onChange={(e) => {
                    const columns = e.target.value.split('\n').filter(c => c.trim());
                    setNodes(nodes.map(n => 
                      n.id === selectedNode.id 
                        ? { ...n, config: { ...n.config, columns } }
                        : n
                    ));
                    setSelectedNode({ 
                      ...selectedNode, 
                      config: { ...selectedNode.config, columns } 
                    });
                  }}
                />
              </div>
            )}

            {selectedNode.type === 'destination' && (
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">Destination Table</label>
                  <input
                    type="text"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                    placeholder="pipeline_output"
                    value={selectedNode.config.table || ''}
                    onChange={(e) => {
                      setNodes(nodes.map(n => 
                        n.id === selectedNode.id 
                          ? { ...n, config: { ...n.config, table: e.target.value } }
                          : n
                      ));
                      setSelectedNode({ 
                        ...selectedNode, 
                        config: { ...selectedNode.config, table: e.target.value } 
                      });
                    }}
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">Output Format</label>
                  <select
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                    value={selectedNode.config.format || 'parquet'}
                    onChange={(e) => {
                      setNodes(nodes.map(n => 
                        n.id === selectedNode.id 
                          ? { ...n, config: { ...n.config, format: e.target.value } }
                          : n
                      ));
                      setSelectedNode({ 
                        ...selectedNode, 
                        config: { ...selectedNode.config, format: e.target.value } 
                      });
                    }}
                  >
                    <option value="parquet">Parquet</option>
                    <option value="iceberg">Apache Iceberg</option>
                  </select>
                </div>
              </div>
            )}

            {selectedNode.type === 'join' && (
              <div className="space-y-4">
                <div className="bg-blue-50 border border-blue-200 rounded-lg p-3">
                  <div className="flex items-start">
                    <span className="text-blue-700 text-xs">
                      ⭐ <strong>Spark-Powered JOINs</strong> - Distributed processing for high-performance table joins
                    </span>
                  </div>
                </div>

                <div>
                  <label className="flex items-center text-sm font-medium text-gray-700 mb-2">
                    Join Type
                    <HelpTooltip>
                      <div><strong>⭐ Spark JOIN Types:</strong></div>
                      <ul className="mt-1 space-y-1 text-xs">
                        <li>• <strong>Inner:</strong> Only matching rows from both tables</li>
                        <li>• <strong>Left:</strong> All left rows + matched right rows (nulls for non-matches)</li>
                        <li>• <strong>Right:</strong> All right rows + matched left rows (nulls for non-matches)</li>
                        <li>• <strong>Outer:</strong> All rows from both tables (nulls where no match)</li>
                      </ul>
                    </HelpTooltip>
                  </label>
                  <select
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                    value={selectedNode.config.join_type || 'inner'}
                    onChange={(e) => {
                      setNodes(nodes.map(n => 
                        n.id === selectedNode.id 
                          ? { ...n, config: { ...n.config, join_type: e.target.value } }
                          : n
                      ));
                      setSelectedNode({ 
                        ...selectedNode, 
                        config: { ...selectedNode.config, join_type: e.target.value } 
                      });
                    }}
                  >
                    <option value="inner">Inner Join</option>
                    <option value="left">Left Join</option>
                    <option value="right">Right Join</option>
                    <option value="outer">Outer Join (Full)</option>
                  </select>
                </div>

                <div>
                  <label className="flex items-center text-sm font-medium text-gray-700 mb-2">
                    Right Table
                    <HelpTooltip>
                      <div>Table to join with the current dataset</div>
                      <div className="mt-1 text-xs">Example: finance_transactions, customer_profiles</div>
                    </HelpTooltip>
                  </label>
                  <input
                    type="text"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                    placeholder="finance_transactions"
                    value={selectedNode.config.right_table || ''}
                    onChange={(e) => {
                      setNodes(nodes.map(n => 
                        n.id === selectedNode.id 
                          ? { ...n, config: { ...n.config, right_table: e.target.value } }
                          : n
                      ));
                      setSelectedNode({ 
                        ...selectedNode, 
                        config: { ...selectedNode.config, right_table: e.target.value } 
                      });
                    }}
                  />
                </div>

                <div>
                  <label className="flex items-center text-sm font-medium text-gray-700 mb-2">
                    Join Key(s)
                    <HelpTooltip>
                      <div>Column name(s) to join on</div>
                      <ul className="mt-1 space-y-1 text-xs">
                        <li>• Single key: user_id</li>
                        <li>• Multiple keys: user_id,account_id</li>
                      </ul>
                    </HelpTooltip>
                  </label>
                  <input
                    type="text"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                    placeholder="user_id"
                    value={selectedNode.config.on || ''}
                    onChange={(e) => {
                      setNodes(nodes.map(n => 
                        n.id === selectedNode.id 
                          ? { ...n, config: { ...n.config, on: e.target.value } }
                          : n
                      ));
                      setSelectedNode({ 
                        ...selectedNode, 
                        config: { ...selectedNode.config, on: e.target.value } 
                      });
                    }}
                  />
                </div>
              </div>
            )}

            {selectedNode.type === 'aggregate' && (
              <div className="space-y-4">
                <div>
                  <label className="flex items-center text-sm font-medium text-gray-700 mb-2">
                    Group By Column(s)
                    <HelpTooltip>
                      <div>Columns to group by (comma-separated)</div>
                      <div className="mt-1 text-xs">Example: transaction_type, status</div>
                    </HelpTooltip>
                  </label>
                  <input
                    type="text"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                    placeholder="transaction_type"
                    value={selectedNode.config.groupBy || ''}
                    onChange={(e) => {
                      setNodes(nodes.map(n => 
                        n.id === selectedNode.id 
                          ? { ...n, config: { ...n.config, groupBy: e.target.value } }
                          : n
                      ));
                      setSelectedNode({ 
                        ...selectedNode, 
                        config: { ...selectedNode.config, groupBy: e.target.value } 
                      });
                    }}
                  />
                </div>

                <div>
                  <label className="flex items-center text-sm font-medium text-gray-700 mb-2">
                    Aggregation Function
                    <HelpTooltip>
                      <div><strong>Aggregation Functions:</strong></div>
                      <ul className="mt-1 space-y-1 text-xs">
                        <li>• <strong>sum:</strong> Total of all values</li>
                        <li>• <strong>avg:</strong> Average value</li>
                        <li>• <strong>count:</strong> Count of rows</li>
                        <li>• <strong>min:</strong> Minimum value</li>
                        <li>• <strong>max:</strong> Maximum value</li>
                      </ul>
                    </HelpTooltip>
                  </label>
                  <select
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                    value={selectedNode.config.agg || 'sum'}
                    onChange={(e) => {
                      setNodes(nodes.map(n => 
                        n.id === selectedNode.id 
                          ? { ...n, config: { ...n.config, agg: e.target.value } }
                          : n
                      ));
                      setSelectedNode({ 
                        ...selectedNode, 
                        config: { ...selectedNode.config, agg: e.target.value } 
                      });
                    }}
                  >
                    <option value="sum">Sum</option>
                    <option value="avg">Average</option>
                    <option value="count">Count</option>
                    <option value="min">Minimum</option>
                    <option value="max">Maximum</option>
                  </select>
                </div>

                <div>
                  <label className="flex items-center text-sm font-medium text-gray-700 mb-2">
                    Column(s) to Aggregate
                    <HelpTooltip>
                      <div>Columns to apply aggregation on (comma-separated)</div>
                      <div className="mt-1 text-xs">Example: amount, quantity</div>
                    </HelpTooltip>
                  </label>
                  <input
                    type="text"
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                    placeholder="amount"
                    value={selectedNode.config.aggColumns || ''}
                    onChange={(e) => {
                      setNodes(nodes.map(n => 
                        n.id === selectedNode.id 
                          ? { ...n, config: { ...n.config, aggColumns: e.target.value } }
                          : n
                      ));
                      setSelectedNode({ 
                        ...selectedNode, 
                        config: { ...selectedNode.config, aggColumns: e.target.value } 
                      });
                    }}
                  />
                </div>
              </div>
            )}

            {selectedNode.type === 'transform' && (
              <div className="space-y-4">
                <div>
                  <label className="flex items-center text-sm font-medium text-gray-700 mb-2">
                    Transformation Type
                    <HelpTooltip>
                      <div><strong>Common Transformations:</strong></div>
                      <ul className="mt-1 space-y-1 text-xs">
                        <li>• <strong>extract_datetime:</strong> Extract year, month, day from timestamp</li>
                        <li>• <strong>log_transform:</strong> Apply logarithm to numeric columns</li>
                        <li>• <strong>normalize:</strong> Scale values to 0-1 range</li>
                        <li>• <strong>drop_duplicates:</strong> Remove duplicate rows</li>
                      </ul>
                    </HelpTooltip>
                  </label>
                  <select
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                    value={selectedNode.config.operation || ''}
                    onChange={(e) => {
                      setNodes(nodes.map(n => 
                        n.id === selectedNode.id 
                          ? { ...n, config: { ...n.config, operation: e.target.value } }
                          : n
                      ));
                      setSelectedNode({ 
                        ...selectedNode, 
                        config: { ...selectedNode.config, operation: e.target.value } 
                      });
                    }}
                  >
                    <option value="">Select transformation...</option>
                    <option value="extract_datetime">Extract Date Parts</option>
                    <option value="log_transform">Log Transform</option>
                    <option value="normalize">Normalize (Min-Max)</option>
                    <option value="standardize">Standardize (Z-score)</option>
                    <option value="drop_duplicates">Drop Duplicates</option>
                    <option value="fill_null">Fill Null Values</option>
                  </select>
                </div>

                {selectedNode.config.operation === 'extract_datetime' && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">Column</label>
                    <input
                      type="text"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                      placeholder="transaction_date"
                      value={selectedNode.config.column || ''}
                      onChange={(e) => {
                        setNodes(nodes.map(n => 
                          n.id === selectedNode.id 
                            ? { ...n, config: { ...n.config, column: e.target.value } }
                            : n
                        ));
                        setSelectedNode({ 
                          ...selectedNode, 
                          config: { ...selectedNode.config, column: e.target.value } 
                        });
                      }}
                    />
                    <label className="block text-sm font-medium text-gray-700 mb-2 mt-3">Parts (comma-separated)</label>
                    <input
                      type="text"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                      placeholder="year,month,quarter,dayofweek"
                      value={selectedNode.config.parts || ''}
                      onChange={(e) => {
                        setNodes(nodes.map(n => 
                          n.id === selectedNode.id 
                            ? { ...n, config: { ...n.config, parts: e.target.value } }
                            : n
                        ));
                        setSelectedNode({ 
                          ...selectedNode, 
                          config: { ...selectedNode.config, parts: e.target.value } 
                        });
                      }}
                    />
                  </div>
                )}

                {(selectedNode.config.operation === 'log_transform' || 
                  selectedNode.config.operation === 'normalize' || 
                  selectedNode.config.operation === 'standardize') && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">Column(s) (comma-separated)</label>
                    <input
                      type="text"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                      placeholder="amount,quantity"
                      value={selectedNode.config.columns || ''}
                      onChange={(e) => {
                        setNodes(nodes.map(n => 
                          n.id === selectedNode.id 
                            ? { ...n, config: { ...n.config, columns: e.target.value } }
                            : n
                        ));
                        setSelectedNode({ 
                          ...selectedNode, 
                          config: { ...selectedNode.config, columns: e.target.value } 
                        });
                      }}
                    />
                  </div>
                )}

                {selectedNode.config.operation === 'drop_duplicates' && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">Columns to Check (comma-separated)</label>
                    <input
                      type="text"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                      placeholder="user_id,transaction_date"
                      value={selectedNode.config.columns || ''}
                      onChange={(e) => {
                        setNodes(nodes.map(n => 
                          n.id === selectedNode.id 
                            ? { ...n, config: { ...n.config, columns: e.target.value } }
                            : n
                        ));
                        setSelectedNode({ 
                          ...selectedNode, 
                          config: { ...selectedNode.config, columns: e.target.value } 
                        });
                      }}
                    />
                  </div>
                )}

                {selectedNode.config.operation === 'fill_null' && (
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-2">Column(s) (comma-separated)</label>
                    <input
                      type="text"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 mb-3"
                      placeholder="amount,status"
                      value={selectedNode.config.columns || ''}
                      onChange={(e) => {
                        setNodes(nodes.map(n => 
                          n.id === selectedNode.id 
                            ? { ...n, config: { ...n.config, columns: e.target.value } }
                            : n
                        ));
                        setSelectedNode({ 
                          ...selectedNode, 
                          config: { ...selectedNode.config, columns: e.target.value } 
                        });
                      }}
                    />
                    <label className="block text-sm font-medium text-gray-700 mb-2">Fill Value</label>
                    <input
                      type="text"
                      className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                      placeholder="0 or 'Unknown'"
                      value={selectedNode.config.fillValue || ''}
                      onChange={(e) => {
                        setNodes(nodes.map(n => 
                          n.id === selectedNode.id 
                            ? { ...n, config: { ...n.config, fillValue: e.target.value } }
                            : n
                        ));
                        setSelectedNode({ 
                          ...selectedNode, 
                          config: { ...selectedNode.config, fillValue: e.target.value } 
                        });
                      }}
                    />
                  </div>
                )}
              </div>
            )}

            {selectedNode.type === 'sql' && (
              <div>
                <label className="flex items-center text-sm font-medium text-gray-700 mb-2">
                  Custom SQL Query
                  <HelpTooltip>
                    <div>Write custom SQL to transform the data</div>
                    <div className="mt-1 text-xs">Use "df" as the table name for the input data</div>
                    <div className="mt-1 text-xs">Example: SELECT * FROM df WHERE amount &gt; 1000</div>
                  </HelpTooltip>
                </label>
                <textarea
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm"
                  rows={8}
                  placeholder="SELECT&#10;  transaction_type,&#10;  SUM(amount) as total_amount,&#10;  COUNT(*) as count&#10;FROM df&#10;GROUP BY transaction_type"
                  value={selectedNode.config.query || ''}
                  onChange={(e) => {
                    setNodes(nodes.map(n => 
                      n.id === selectedNode.id 
                        ? { ...n, config: { ...n.config, query: e.target.value } }
                        : n
                    ));
                    setSelectedNode({ 
                      ...selectedNode, 
                      config: { ...selectedNode.config, query: e.target.value } 
                    });
                  }}
                />
              </div>
            )}

            <button 
              onClick={applyConfiguration}
              className="w-full px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700"
            >
              Apply Configuration
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
