import React, { useState, useEffect, useCallback, useMemo } from 'react';
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  Handle,
  Position,
  useNodesState,
  useEdgesState,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { Key, Link2, RefreshCw, AlertCircle, Database } from 'lucide-react';
import axios from 'axios';

const API_BASE = 'http://localhost:8000/api';

const NODE_WIDTH = 260;
const NODE_HEIGHT_PER_COLUMN = 24;
const GRID_COLS = 4;
const GRID_GAP_X = 320;
const GRID_GAP_Y = 60;

function TableNode({ data }) {
  return (
    <div className="bg-white border border-gray-300 rounded-lg shadow-md overflow-hidden" style={{ width: NODE_WIDTH }}>
      <Handle type="target" position={Position.Left} style={{ opacity: 0 }} />
      <Handle type="source" position={Position.Right} style={{ opacity: 0 }} />
      <div className="bg-blue-600 text-white px-3 py-2 flex items-center gap-2">
        <Database className="w-4 h-4 flex-shrink-0" />
        <span className="font-semibold text-sm truncate">{data.table}</span>
      </div>
      <div className="divide-y divide-gray-100">
        {data.columns.map((col) => (
          <div
            key={col.name}
            className={`flex items-center justify-between px-3 py-1 text-xs ${
              col.is_primary_key ? 'bg-amber-50' : ''
            }`}
          >
            <span className="flex items-center gap-1.5 truncate">
              {col.is_primary_key && <Key className="w-3 h-3 text-amber-600 flex-shrink-0" />}
              <span className={col.is_primary_key ? 'font-semibold text-gray-900' : 'text-gray-700'}>
                {col.name}
              </span>
            </span>
            <span className="text-gray-400 flex-shrink-0 ml-2">{col.type}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

const nodeTypes = { tableNode: TableNode };

function layoutNodes(nodes) {
  return nodes.map((n, i) => ({
    ...n,
    position: {
      x: (i % GRID_COLS) * GRID_GAP_X,
      y: Math.floor(i / GRID_COLS) * (n.data.columns.length * NODE_HEIGHT_PER_COLUMN + 40 + GRID_GAP_Y),
    },
  }));
}

export default function ERDDiagram() {
  const [schema, setSchema] = useState('public');
  const [database, setDatabase] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [erdData, setErdData] = useState(null);
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  const loadERD = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = { schema };
      if (database.trim()) params.database = database.trim();
      const res = await axios.get(`${API_BASE}/erd/schema`, { params });
      if (!res.data.success) throw new Error(res.data.detail || 'Failed to load ERD');
      setErdData(res.data);

      const rfNodes = layoutNodes(
        res.data.nodes.map((n) => ({
          id: n.table,
          type: 'tableNode',
          data: n,
          position: { x: 0, y: 0 },
        }))
      );
      const rfEdges = res.data.edges.map((e) => ({
        id: e.id,
        source: e.source_table,
        target: e.target_table,
        label: `${e.source_column} → ${e.target_column}`,
        animated: false,
        style: { stroke: '#2563eb' },
        labelStyle: { fontSize: 10, fill: '#4b5563' },
        markerEnd: { type: 'arrowclosed', color: '#2563eb' },
      }));

      setNodes(rfNodes);
      setEdges(rfEdges);
    } catch (err) {
      setError(err.response?.data?.detail || err.message || 'Failed to load schema relationships');
      setNodes([]);
      setEdges([]);
    } finally {
      setLoading(false);
    }
  }, [schema, database, setNodes, setEdges]);

  useEffect(() => {
    loadERD();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const summary = useMemo(() => {
    if (!erdData) return null;
    return `${erdData.table_count} tables · ${erdData.relationship_count} relationships`;
  }, [erdData]);

  return (
    <div className="h-full w-full flex flex-col bg-gray-50">
      <div className="bg-white border-b border-gray-200 px-6 py-4 flex items-center gap-4 flex-wrap">
        <div>
          <h2 className="text-lg font-semibold text-gray-900">Entity Relationship Diagram</h2>
          <p className="text-sm text-gray-500">
            Live primary/foreign key structure introspected from the source database
            {summary ? ` — ${summary}` : ''}
          </p>
        </div>
        <div className="flex items-center gap-2 ml-auto">
          <input
            type="text"
            placeholder="database (optional)"
            value={database}
            onChange={(e) => setDatabase(e.target.value)}
            className="border border-gray-300 rounded-md px-3 py-1.5 text-sm w-48"
          />
          <input
            type="text"
            placeholder="schema"
            value={schema}
            onChange={(e) => setSchema(e.target.value)}
            className="border border-gray-300 rounded-md px-3 py-1.5 text-sm w-28"
          />
          <button
            onClick={loadERD}
            disabled={loading}
            className="flex items-center gap-1.5 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white text-sm font-medium px-3 py-1.5 rounded-md"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
            {loading ? 'Loading…' : 'Refresh'}
          </button>
        </div>
      </div>

      {error && (
        <div className="mx-6 mt-4 flex items-center gap-2 bg-red-50 border border-red-200 text-red-700 text-sm px-4 py-3 rounded-md">
          <AlertCircle className="w-4 h-4 flex-shrink-0" />
          {error}
        </div>
      )}

      {!error && erdData && erdData.relationship_count === 0 && (
        <div className="mx-6 mt-4 flex items-center gap-2 bg-amber-50 border border-amber-200 text-amber-700 text-sm px-4 py-3 rounded-md">
          <Link2 className="w-4 h-4 flex-shrink-0" />
          No foreign key relationships found in this schema — tables are shown with no connecting edges.
        </div>
      )}

      <div className="flex-1 min-h-0">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          nodeTypes={nodeTypes}
          fitView
          minZoom={0.1}
        >
          <Background />
          <Controls />
          <MiniMap
            nodeColor="#2563eb"
            maskColor="rgba(240, 240, 240, 0.6)"
            style={{ background: '#fff' }}
          />
        </ReactFlow>
      </div>
    </div>
  );
}
