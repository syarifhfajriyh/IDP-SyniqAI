import React, { useState, useEffect } from 'react';
import { Activity, Clock, CheckCircle, XCircle, AlertTriangle, Play, Pause, RotateCcw, Eye, Filter, Download, RefreshCw } from 'lucide-react';
import axios from 'axios';

const API_BASE = 'http://localhost:8000/api';

/**
 * JobMonitoring - Monitor pipeline execution and job history
 * Shows real-time status, logs, and performance metrics
 */
export default function JobMonitoring() {
  const [activeTab, setActiveTab] = useState('all'); // 'all', 'running', 'completed', 'failed'
  const [selectedJob, setSelectedJob] = useState(null);
  const [jobs, setJobs] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [autoRefresh, setAutoRefresh] = useState(true);

  // Fetch jobs from backend
  const fetchJobs = async (status = null) => {
    try {
      setLoading(true);
      const params = status ? { status, limit: 100 } : { limit: 100 };
      const response = await axios.get(`${API_BASE}/silver/jobs`, { params });
      
      setJobs(response.data.jobs || []);
      setError(null);
    } catch (err) {
      console.error('Failed to fetch jobs:', err);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  // Auto-refresh every 5 seconds for running jobs
  useEffect(() => {
    fetchJobs();
    
    if (autoRefresh) {
      const interval = setInterval(() => {
        fetchJobs();
      }, 5000);
      
      return () => clearInterval(interval);
    }
  }, [autoRefresh]);

  // Filter jobs by active tab
  const filteredJobs = jobs.filter(job => {
    if (activeTab === 'all') return true;
    if (activeTab === 'running') return job.status === 'running' || job.status === 'pending';
    if (activeTab === 'completed') return job.status === 'completed';
    if (activeTab === 'failed') return job.status === 'failed';
    return true;
  });

  // Calculate statistics
  const stats = {
    total: jobs.length,
    running: jobs.filter(j => j.status === 'running' || j.status === 'pending').length,
    completed: jobs.filter(j => j.status === 'completed').length,
    failed: jobs.filter(j => j.status === 'failed').length
  };

  const formatDuration = (startTime, endTime) => {
    if (!startTime) return 'N/A';
    
    const start = new Date(startTime);
    const end = endTime ? new Date(endTime) : new Date();
    const diff = Math.floor((end - start) / 1000); // seconds
    
    if (diff < 60) return `${diff}s`;
    if (diff < 3600) return `${Math.floor(diff / 60)}m ${diff % 60}s`;
    return `${Math.floor(diff / 3600)}h ${Math.floor((diff % 3600) / 60)}m`;
  };

  const getStatusIcon = (status) => {
    switch(status) {
      case 'completed': return <CheckCircle className="w-5 h-5 text-green-500" />;
      case 'failed': return <XCircle className="w-5 h-5 text-red-500" />;
      case 'running': return <Activity className="w-5 h-5 text-blue-500 animate-pulse" />;
      case 'pending': return <Clock className="w-5 h-5 text-yellow-500" />;
      default: return <AlertTriangle className="w-5 h-5 text-gray-500" />;
    }
  };

  const getStatusColor = (status) => {
    switch(status) {
      case 'completed': return 'bg-green-100 text-green-800 border-green-300';
      case 'failed': return 'bg-red-100 text-red-800 border-red-300';
      case 'running': return 'bg-blue-100 text-blue-800 border-blue-300';
      case 'pending': return 'bg-yellow-100 text-yellow-800 border-yellow-300';
      default: return 'bg-gray-100 text-gray-800 border-gray-300';
    }
  };

  return (
    <div className="h-screen flex flex-col bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 p-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
              <Activity className="w-6 h-6" />
              Job Monitoring
            </h1>
            <p className="text-sm text-gray-500 mt-1">Real-time Silver transformation job tracking</p>
          </div>
          <div className="flex gap-2">
            <button
              onClick={() => setAutoRefresh(!autoRefresh)}
              className={`px-4 py-2 border rounded-lg text-sm font-medium flex items-center gap-2 ${
                autoRefresh 
                  ? 'border-blue-300 bg-blue-50 text-blue-700' 
                  : 'border-gray-300 text-gray-700 hover:bg-gray-50'
              }`}
            >
              <RefreshCw className={`w-4 h-4 ${autoRefresh ? 'animate-spin' : ''}`} />
              {autoRefresh ? 'Auto-refresh ON' : 'Auto-refresh OFF'}
            </button>
            <button 
              onClick={() => fetchJobs()}
              disabled={loading}
              className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-700 hover:bg-gray-50 flex items-center gap-2 disabled:opacity-50"
            >
              <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} />
              Refresh
            </button>
          </div>
        </div>
      </div>

      {/* Stats Bar */}
      <div className="bg-white border-b border-gray-200 p-4">
        <div className="grid grid-cols-4 gap-4">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-blue-50 flex items-center justify-center">
              <Clock className="w-5 h-5 text-blue-600" />
            </div>
            <div>
              <p className="text-sm text-gray-600">Running</p>
              <p className="text-xl font-bold text-gray-900">{stats.running}</p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-green-50 flex items-center justify-center">
              <CheckCircle className="w-5 h-5 text-green-600" />
            </div>
            <div>
              <p className="text-sm text-gray-600">Completed</p>
              <p className="text-xl font-bold text-gray-900">{stats.completed}</p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-red-50 flex items-center justify-center">
              <XCircle className="w-5 h-5 text-red-600" />
            </div>
            <div>
              <p className="text-sm text-gray-600">Failed</p>
              <p className="text-xl font-bold text-gray-900">{stats.failed}</p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-purple-50 flex items-center justify-center">
              <Activity className="w-5 h-5 text-purple-600" />
            </div>
            <div>
              <p className="text-sm text-gray-600">Total Jobs</p>
              <p className="text-xl font-bold text-gray-900">{stats.total}</p>
            </div>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="bg-white border-b border-gray-200">
        <div className="flex gap-1 px-6">
          {[
            { id: 'all', label: 'All Jobs', count: stats.total },
            { id: 'running', label: 'Running', count: stats.running },
            { id: 'completed', label: 'Completed', count: stats.completed },
            { id: 'failed', label: 'Failed', count: stats.failed }
          ].map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`px-4 py-3 text-sm font-medium border-b-2 transition-colors ${
                activeTab === tab.id
                  ? 'border-blue-600 text-blue-600'
                  : 'border-transparent text-gray-600 hover:text-gray-900'
              }`}
            >
              {tab.label}
              <span className="ml-2 px-2 py-0.5 bg-gray-100 text-gray-600 rounded-full text-xs">
                {tab.count}
              </span>
            </button>
          ))}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-6">
        {loading && !jobs.length ? (
          <div className="bg-white rounded-lg border border-gray-200 p-12 text-center">
            <RefreshCw className="w-12 h-12 text-blue-500 mx-auto mb-4 animate-spin" />
            <p className="text-lg font-medium text-gray-900">Loading jobs...</p>
          </div>
        ) : error ? (
          <div className="bg-white rounded-lg border border-red-200 p-12 text-center">
            <XCircle className="w-12 h-12 text-red-500 mx-auto mb-4" />
            <p className="text-lg font-medium text-gray-900 mb-2">Failed to load jobs</p>
            <p className="text-sm text-gray-500 mb-4">{error}</p>
            <button
              onClick={() => fetchJobs()}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700"
            >
              Retry
            </button>
          </div>
        ) : filteredJobs.length === 0 ? (
          <div className="bg-white rounded-lg border border-gray-200 p-12 text-center">
            <Activity className="w-12 h-12 text-gray-400 mx-auto mb-4" />
            <p className="text-lg font-medium text-gray-900 mb-2">No jobs found</p>
            <p className="text-sm text-gray-500">
              {activeTab === 'all' && 'No transformation jobs have been executed yet'}
              {activeTab === 'running' && 'No jobs are currently running'}
              {activeTab === 'completed' && 'No completed jobs found'}
              {activeTab === 'failed' && 'No failed jobs found'}
            </p>
          </div>
        ) : (
          <div className="space-y-4">
            {filteredJobs.map(job => (
              <div 
                key={job.job_id} 
                className="bg-white rounded-lg shadow border border-gray-200 p-6 hover:shadow-md transition-shadow cursor-pointer"
                onClick={() => setSelectedJob(job)}
              >
                <div className="flex items-start justify-between mb-4">
                  <div className="flex items-center gap-3">
                    {getStatusIcon(job.status)}
                    <div>
                      <h3 className="text-lg font-semibold text-gray-900">
                        {job.table_name || 'Transformation Job'}
                      </h3>
                      <p className="text-sm text-gray-500">
                        {job.source}/{job.entity} • Started {new Date(job.started_at).toLocaleString()}
                      </p>
                    </div>
                  </div>
                  <span className={`px-3 py-1 rounded-full text-xs font-medium border ${getStatusColor(job.status)}`}>
                    {job.status.toUpperCase()}
                  </span>
                </div>

                {/* Progress Bar for Running Jobs */}
                {(job.status === 'running' || job.status === 'pending') && (
                  <div className="mb-4">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-sm font-medium text-gray-700">
                        Progress: {job.progress || 0}%
                      </span>
                      <span className="text-sm text-gray-500">
                        {formatDuration(job.started_at, null)} elapsed
                      </span>
                    </div>
                    <div className="w-full bg-gray-200 rounded-full h-3">
                      <div
                        className="bg-blue-500 h-3 rounded-full transition-all"
                        style={{ width: `${job.progress || 0}%` }}
                      />
                    </div>
                  </div>
                )}

                {/* Job Details Grid */}
                <div className="grid grid-cols-4 gap-4 mb-4">
                  <div>
                    <p className="text-xs text-gray-500 mb-1">Duration</p>
                    <p className="text-sm font-medium text-gray-900">
                      {formatDuration(job.started_at, job.completed_at)}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-gray-500 mb-1">Records</p>
                    <p className="text-sm font-medium text-gray-900">
                      {job.row_count ? job.row_count.toLocaleString() : 'N/A'}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-gray-500 mb-1">Quality Score</p>
                    <p className="text-sm font-medium text-gray-900">
                      {job.quality_score ? `${(job.quality_score * 100).toFixed(1)}%` : 'N/A'}
                    </p>
                  </div>
                  <div>
                    <p className="text-xs text-gray-500 mb-1">Job ID</p>
                    <p className="text-xs font-mono text-gray-600 truncate">
                      {job.job_id}
                    </p>
                  </div>
                </div>

                {/* Message/Error */}
                {job.message && (
                  <div className="mt-3 p-3 bg-blue-50 rounded-lg">
                    <p className="text-sm text-blue-900">{job.message}</p>
                  </div>
                )}
                {job.error_message && (
                  <div className="mt-3 p-3 bg-red-50 rounded-lg">
                    <p className="text-sm text-red-900 font-medium">Error:</p>
                    <p className="text-sm text-red-800 mt-1">{job.error_message}</p>
                  </div>
                )}

                {/* Cleaning Summary */}
                {job.cleaning_summary && (
                  <div className="mt-3 p-3 bg-gray-50 rounded-lg">
                    <p className="text-xs font-medium text-gray-700 mb-2">Cleaning Summary:</p>
                    <div className="grid grid-cols-3 gap-2 text-xs">
                      <div>
                        <span className="text-gray-600">Quarantined:</span>
                        <span className="ml-1 font-medium text-gray-900">
                          {job.cleaning_summary.rows_quarantined || 0}
                        </span>
                      </div>
                      <div>
                        <span className="text-gray-600">Cleaned:</span>
                        <span className="ml-1 font-medium text-gray-900">
                          {job.cleaning_summary.rows_cleaned || 0}
                        </span>
                      </div>
                      <div>
                        <span className="text-gray-600">Validated:</span>
                        <span className="ml-1 font-medium text-gray-900">
                          {job.cleaning_summary.rows_validated || 0}
                        </span>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
