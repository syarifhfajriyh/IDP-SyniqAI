import React, { useState, useEffect, useMemo } from 'react';
import { useNavigate, useParams } from 'react-router-dom';
import axios from 'axios';
import {
  Search, RefreshCw, Database, Layers, Sparkles, Network, BarChart3,
  Table2, Clock, HardDrive, ExternalLink,
} from 'lucide-react';

const API_BASE = 'http://localhost:8000/api';

const LAYER_STYLE = {
  bronze: { badge: 'bg-orange-100 text-orange-800 border-orange-200', icon: Database, dot: 'bg-orange-500' },
  silver: { badge: 'bg-slate-100 text-slate-800 border-slate-200', icon: Layers, dot: 'bg-slate-400' },
  gold: { badge: 'bg-amber-100 text-amber-800 border-amber-200', icon: Sparkles, dot: 'bg-amber-500' },
};

function qualityColor(score) {
  if (score >= 95) return 'text-green-600';
  if (score >= 85) return 'text-amber-600';
  return 'text-red-600';
}

export default function DataMart() {
  const navigate = useNavigate();
  const { domain } = useParams();

  const [datasets, setDatasets] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [search, setSearch] = useState('');
  const [layerFilter, setLayerFilter] = useState('all');
  const [sourceFilter, setSourceFilter] = useState('all');

  const load = async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await axios.get(`${API_BASE}/silver/catalog/datasets`);
      setDatasets(res.data.datasets || []);
    } catch (err) {
      setError('Failed to load the data mart catalog.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const sources = useMemo(
    () => ['all', ...new Set(datasets.map((d) => d.source).filter(Boolean))],
    [datasets]
  );

  const filtered = useMemo(() => {
    return datasets.filter((d) => {
      if (layerFilter !== 'all' && d.layer !== layerFilter) return false;
      if (sourceFilter !== 'all' && d.source !== sourceFilter) return false;
      if (search) {
        const q = search.toLowerCase();
        if (!d.name?.toLowerCase().includes(q) && !d.description?.toLowerCase().includes(q)) return false;
      }
      return true;
    });
  }, [datasets, layerFilter, sourceFilter, search]);

  const counts = useMemo(() => {
    const c = { all: datasets.length, bronze: 0, silver: 0, gold: 0 };
    datasets.forEach((d) => { c[d.layer] = (c[d.layer] || 0) + 1; });
    return c;
  }, [datasets]);

  const goToLayer = (dataset) => {
    if (dataset.layer === 'bronze') navigate(`/${domain}/bronze`);
    else if (dataset.layer === 'silver') navigate(`/${domain}/silver?tab=catalog`);
    else navigate(`/${domain}/gold`);
  };

  return (
    <div className="h-full w-full flex flex-col bg-gray-50 overflow-hidden">
      <div className="bg-white border-b border-gray-200 px-6 py-5">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h1 className="text-xl font-semibold text-gray-900 flex items-center gap-2">
              <Table2 className="w-5 h-5 text-blue-600" /> Data Mart
            </h1>
            <p className="text-sm text-gray-500 mt-0.5">
              Every table across Bronze, Silver, and Gold — in one place.
            </p>
          </div>
          <button
            onClick={load}
            disabled={loading}
            className="flex items-center gap-1.5 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 text-white text-sm font-medium px-3 py-1.5 rounded-md"
          >
            <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
            {loading ? 'Loading…' : 'Refresh'}
          </button>
        </div>

        <div className="flex items-center gap-3 flex-wrap">
          <div className="relative flex-1 min-w-[240px]">
            <Search className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2" />
            <input
              type="text"
              placeholder="Search tables…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="w-full pl-9 pr-3 py-2 border border-gray-300 rounded-md text-sm"
            />
          </div>

          <div className="flex gap-1 bg-gray-100 p-1 rounded-lg">
            {['all', 'bronze', 'silver', 'gold'].map((layer) => (
              <button
                key={layer}
                onClick={() => setLayerFilter(layer)}
                className={`px-3 py-1.5 rounded-md text-sm font-medium capitalize transition ${
                  layerFilter === layer ? 'bg-white shadow text-gray-900' : 'text-gray-500 hover:text-gray-800'
                }`}
              >
                {layer} <span className="text-xs text-gray-400">({counts[layer] || 0})</span>
              </button>
            ))}
          </div>

          <select
            value={sourceFilter}
            onChange={(e) => setSourceFilter(e.target.value)}
            className="border border-gray-300 rounded-md px-3 py-2 text-sm"
          >
            {sources.map((s) => (
              <option key={s} value={s}>{s === 'all' ? 'All sources' : s}</option>
            ))}
          </select>

          <div className="flex gap-2 ml-auto">
            <button
              onClick={() => navigate(`/${domain}/silver?tab=erd`)}
              className="flex items-center gap-1.5 text-sm font-medium text-blue-700 border border-blue-200 bg-blue-50 hover:bg-blue-100 px-3 py-1.5 rounded-md"
            >
              <Network className="w-4 h-4" /> View ERD
            </button>
            <button
              onClick={() => navigate(`/${domain}/silver?tab=eda`)}
              className="flex items-center gap-1.5 text-sm font-medium text-blue-700 border border-blue-200 bg-blue-50 hover:bg-blue-100 px-3 py-1.5 rounded-md"
            >
              <BarChart3 className="w-4 h-4" /> Silver EDA
            </button>
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-6">
        {error && (
          <div className="bg-red-50 border border-red-200 text-red-700 text-sm px-4 py-3 rounded-md mb-4">
            {error}
          </div>
        )}

        {!loading && filtered.length === 0 && !error && (
          <div className="text-center text-gray-400 text-sm py-16">
            No tables match the current filters.
          </div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
          {filtered.map((d) => {
            const style = LAYER_STYLE[d.layer] || LAYER_STYLE.bronze;
            const LayerIcon = style.icon;
            return (
              <div
                key={d.id}
                className="bg-white border border-gray-200 rounded-xl p-4 hover:shadow-md transition-shadow flex flex-col"
              >
                <div className="flex items-start justify-between mb-2">
                  <div className={`flex items-center gap-1.5 text-xs font-semibold px-2 py-1 rounded-full border ${style.badge}`}>
                    <LayerIcon className="w-3.5 h-3.5" />
                    {d.layer}
                  </div>
                  {typeof d.quality === 'number' && (
                    <span className={`text-xs font-semibold ${qualityColor(d.quality)}`}>
                      {d.quality}% quality
                    </span>
                  )}
                </div>

                <h3 className="font-mono text-sm font-semibold text-gray-900 truncate mb-1" title={d.name}>
                  {d.name}
                </h3>
                <p className="text-xs text-gray-500 mb-3 line-clamp-2">{d.description}</p>

                <div className="flex items-center gap-3 text-xs text-gray-400 mb-3">
                  <span className="flex items-center gap-1"><HardDrive className="w-3 h-3" /> {d.size}</span>
                  <span className="flex items-center gap-1"><Clock className="w-3 h-3" /> {d.updateFrequency}</span>
                </div>

                <div className="flex flex-wrap gap-1 mb-3">
                  {(d.tags || []).map((tag) => (
                    <span key={tag} className="text-[10px] bg-gray-100 text-gray-600 px-2 py-0.5 rounded-full">
                      {tag}
                    </span>
                  ))}
                </div>

                <button
                  onClick={() => goToLayer(d)}
                  className="mt-auto flex items-center justify-center gap-1.5 text-xs font-medium text-gray-700 border border-gray-200 hover:bg-gray-50 py-1.5 rounded-md"
                >
                  <ExternalLink className="w-3.5 h-3.5" /> View in {d.layer}
                </button>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
