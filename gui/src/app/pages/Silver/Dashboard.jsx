import React, { useState, useEffect } from 'react';
import { Activity, Database, CheckCircle, AlertTriangle, Clock, TrendingUp, Zap, Users, RefreshCw } from 'lucide-react';
import axios from 'axios';

const API_BASE = 'http://localhost:8000/api';

/**
 * Dashboard - Main overview page for the Data Transformation System
 * Shows key metrics, recent activity, pipeline status, and system health
 */
export default function Dashboard() {
  const [stats, setStats] = useState({
    totalPipelines: 0,
    activePipelines: 0,
    totalDatasets: 0,
    dataProcessedToday: '0 GB',
    successRate: 100,
    avgExecutionTime: '0 min',
    failedJobsToday: 0,
    dataQualityScore: 95.0
  });
  const [recentPipelines, setRecentPipelines] = useState([]);
  const [dataQualityIssues, setDataQualityIssues] = useState([]);
  const [systemHealth, setSystemHealth] = useState([]);
  const [loading, setLoading] = useState(true);
  const [lastRefresh, setLastRefresh] = useState(new Date());

  // Fetch all dashboard data
  useEffect(() => {
    fetchDashboardData();
    
    // Auto-refresh every 30 seconds
    const interval = setInterval(fetchDashboardData, 30000);
    return () => clearInterval(interval);
  }, []);

  const fetchDashboardData = async () => {
    setLoading(true);
    
    try {
      // Fetch metrics
      const metricsResponse = await axios.get(`${API_BASE}/silver/dashboard/metrics`);
      if (metricsResponse.data.success) {
        setStats(metricsResponse.data.metrics);
      }
    } catch (err) {
      console.error('Failed to fetch metrics:', err);
    }
    
    try {
      // Fetch recent pipelines
      const pipelinesResponse = await axios.get(`${API_BASE}/silver/dashboard/recent-pipelines?limit=10`);
      if (pipelinesResponse.data.success) {
        setRecentPipelines(pipelinesResponse.data.pipelines);
      }
    } catch (err) {
      console.error('Failed to fetch pipelines:', err);
    }
    
    try {
      // Fetch system health
      const healthResponse = await axios.get(`${API_BASE}/silver/dashboard/system-health`);
      if (healthResponse.data.success) {
        setSystemHealth(healthResponse.data.components);
      }
    } catch (err) {
      console.error('Failed to fetch system health:', err);
    }
    
    try {
      // Fetch quality issues
      const qualityResponse = await axios.get(`${API_BASE}/silver/dashboard/quality-issues?limit=10`);
      if (qualityResponse.data.success) {
        setDataQualityIssues(qualityResponse.data.issues);
      }
    } catch (err) {
      console.error('Failed to fetch quality issues:', err);
    }
    
    setLoading(false);
    setLastRefresh(new Date());
  };

  const handleRefresh = () => {
    fetchDashboardData();
  };

  const getStatusIcon = (status) => {
    switch(status) {
      case 'running': return <Clock className="w-4 h-4 text-blue-500 animate-pulse" />;
      case 'success': return <CheckCircle className="w-4 h-4 text-green-500" />;
      case 'failed': return <AlertTriangle className="w-4 h-4 text-red-500" />;
      default: return <Activity className="w-4 h-4 text-gray-500" />;
    }
  };

  const getStatusColor = (status) => {
    switch(status) {
      case 'running': return 'bg-blue-50 text-blue-700 border-blue-200';
      case 'success': return 'bg-green-50 text-green-700 border-green-200';
      case 'failed': return 'bg-red-50 text-red-700 border-red-200';
      default: return 'bg-gray-50 text-gray-700 border-gray-200';
    }
  };

  const getSeverityColor = (severity) => {
    switch(severity) {
      case 'high': return 'bg-red-100 text-red-800';
      case 'medium': return 'bg-yellow-100 text-yellow-800';
      case 'low': return 'bg-blue-100 text-blue-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  return (
    <div className="p-6 space-y-6 bg-gray-50 min-h-screen">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Data Transformation Dashboard</h1>
          <p className="text-sm text-gray-500 mt-1">
            Overview of your data pipelines and system health
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

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Total Pipelines</p>
              <p className="text-2xl font-bold text-gray-900 mt-2">{stats.totalPipelines}</p>
              <p className="text-xs text-green-600 mt-1">{stats.activePipelines} active</p>
            </div>
            <div className="p-3 bg-blue-50 rounded-lg">
              <Zap className="w-6 h-6 text-blue-600" />
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Total Datasets</p>
              <p className="text-2xl font-bold text-gray-900 mt-2">{stats.totalDatasets}</p>
              <p className="text-xs text-gray-500 mt-1">{stats.dataProcessedToday} today</p>
            </div>
            <div className="p-3 bg-green-50 rounded-lg">
              <Database className="w-6 h-6 text-green-600" />
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Success Rate</p>
              <p className="text-2xl font-bold text-gray-900 mt-2">{stats.successRate}%</p>
              <p className="text-xs text-gray-500 mt-1">{stats.failedJobsToday} failed today</p>
            </div>
            <div className="p-3 bg-purple-50 rounded-lg">
              <TrendingUp className="w-6 h-6 text-purple-600" />
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6 border border-gray-200">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Data Quality Score</p>
              <p className="text-2xl font-bold text-gray-900 mt-2">{stats.dataQualityScore}%</p>
              <p className="text-xs text-gray-500 mt-1">Avg execution: {stats.avgExecutionTime}</p>
            </div>
            <div className="p-3 bg-orange-50 rounded-lg">
              <Activity className="w-6 h-6 text-orange-600" />
            </div>
          </div>
        </div>
      </div>

      {/* Two Column Layout */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Recent Pipelines */}
        <div className="lg:col-span-2 bg-white rounded-lg shadow border border-gray-200">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">Recent Pipeline Runs</h2>
          </div>
          <div className="p-4 space-y-3">
            {recentPipelines.map((pipeline, idx) => (
              <div key={idx} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors">
                <div className="flex items-center gap-3 flex-1">
                  {getStatusIcon(pipeline.status)}
                  <div className="flex-1">
                    <p className="text-sm font-medium text-gray-900">{pipeline.name}</p>
                    <p className="text-xs text-gray-500">{pipeline.lastRun} • {pipeline.duration}</p>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  {pipeline.status === 'running' && (
                    <div className="w-32 bg-gray-200 rounded-full h-2">
                      <div 
                        className="bg-blue-500 h-2 rounded-full transition-all"
                        style={{ width: `${pipeline.progress}%` }}
                      />
                    </div>
                  )}
                  <span className={`px-2 py-1 rounded text-xs font-medium border ${getStatusColor(pipeline.status)}`}>
                    {pipeline.status}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* System Health */}
        <div className="bg-white rounded-lg shadow border border-gray-200">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900">System Health</h2>
          </div>
          <div className="p-4 space-y-3">
            {systemHealth.map((component, idx) => (
              <div key={idx} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                <div>
                  <p className="text-sm font-medium text-gray-900">{component.component}</p>
                  <p className="text-xs text-gray-500">Uptime: {component.uptime}</p>
                </div>
                <div className={`px-2 py-1 rounded text-xs font-medium ${
                  component.status === 'healthy' ? 'bg-green-100 text-green-800' : 'bg-yellow-100 text-yellow-800'
                }`}>
                  {component.status}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Data Quality Issues */}
      <div className="bg-white rounded-lg shadow border border-gray-200">
        <div className="p-4 border-b border-gray-200">
          <h2 className="text-lg font-semibold text-gray-900">Data Quality Issues</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Dataset</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Rule</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Severity</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Failed Records</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {dataQualityIssues.map((issue, idx) => (
                <tr key={idx} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                    {issue.dataset}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {issue.rule}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap">
                    <span className={`px-2 py-1 rounded text-xs font-medium ${getSeverityColor(issue.severity)}`}>
                      {issue.severity}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {issue.count.toLocaleString()}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm">
                    <button className="text-blue-600 hover:text-blue-800 font-medium">
                      View Details
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
