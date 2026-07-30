import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { 
  Activity, CheckCircle, AlertCircle, PlayCircle, StopCircle, 
  RefreshCw, Zap, Server, Layers, AlertTriangle, ArrowRight
} from 'lucide-react';
import axios from 'axios';
import KafkaBacklog from '../../components/KafkaBacklog';

const API_BASE = 'http://localhost:8000/api';

export default function RealtimeCDCTab({ domain, autoRefresh, onRefreshed }) {
  const navigate = useNavigate();
  
  const [cdcStatus, setCdcStatus] = useState(null);
  const [cdcHealth, setCdcHealth] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  
  const fetchCDCStatus = async () => {
    try {
      const response = await axios.get(`${API_BASE}/cdc/consumer/status`);
      setCdcStatus(response.data.consumer);
      setError(null);
    } catch (err) {
      console.error('Failed to fetch CDC status:', err);
      setError('Unable to connect to backend. Is the server running?');
    }
  };
  
  const fetchCDCHealth = async () => {
    try {
      const response = await axios.get(`${API_BASE}/cdc/consumer/health`);
      setCdcHealth(response.data);
    } catch (err) {
      console.error('Failed to fetch CDC health:', err);
    }
  };
  
  const refreshAll = async () => {
    setLoading(true);
    try {
      await Promise.all([fetchCDCStatus(), fetchCDCHealth()]);
      if (onRefreshed) onRefreshed();
    } finally {
      setLoading(false);
    }
  };
  
  const handleStartCDC = async () => {
    setLoading(true);
    try {
      const response = await axios.post(`${API_BASE}/cdc/consumer/start`);
      if (response.data.success) {
        await refreshAll();
      } else {
        alert(`CDC Start Failed: ${response.data.error || 'Unknown error'}\n\nMake sure Kafka is running on localhost:9092`);
      }
    } catch (err) {
      console.error('Failed to start CDC:', err);
      const msg = err.response?.data?.detail || err.message;
      alert(`Failed to start CDC consumer:\n\n${msg}\n\nChecklist:\n✓ Kafka running on localhost:9092\n✓ Backend is running\n✓ CDC consumer script exists`);
    } finally {
      setLoading(false);
    }
  };
  
  const handleStopCDC = async () => {
    setLoading(true);
    try {
      await axios.post(`${API_BASE}/cdc/consumer/stop`);
      await refreshAll();
    } catch (err) {
      console.error('Failed to stop CDC:', err);
      alert(`Failed to stop CDC consumer: ${err.response?.data?.detail || err.message}`);
    } finally {
      setLoading(false);
    }
  };
  
  useEffect(() => {
    refreshAll();
  }, []);
  
  useEffect(() => {
    if (!autoRefresh) return;
    const interval = setInterval(refreshAll, 5000);
    return () => clearInterval(interval);
  }, [autoRefresh]);
  
  const getStatusBadge = (running) => {
    return running ? (
      <span className="px-3 py-1 bg-green-100 text-green-700 rounded-full text-sm font-semibold flex items-center">
        <CheckCircle className="w-4 h-4 mr-1" />
        Running
      </span>
    ) : (
      <span className="px-3 py-1 bg-gray-200 text-gray-700 rounded-full text-sm font-semibold flex items-center">
        <AlertCircle className="w-4 h-4 mr-1" />
        Stopped
      </span>
    );
  };

  return (
    <div className="space-y-6">
      {/* Error Display */}
      {error && (
        <div className="bg-red-50 border-2 border-red-200 rounded-xl p-4 flex items-start">
          <AlertCircle className="w-5 h-5 text-red-500 mr-3 flex-shrink-0 mt-0.5" />
          <div className="flex-1">
            <p className="text-red-800 font-semibold">Error</p>
            <p className="text-red-700 text-sm mt-1">{error}</p>
          </div>
        </div>
      )}

      {/* Intro Card */}
      <div className="bg-gradient-to-r from-blue-50 to-indigo-50 rounded-xl border-2 border-blue-200 p-6">
        <h2 className="text-xl font-bold text-gray-900 mb-2 flex items-center">
          <Zap className="w-6 h-6 text-blue-600 mr-2" />
          Real-time Change Data Capture
        </h2>
        <p className="text-gray-700 mb-3">
          Stream database changes in real-time using Debezium CDC. Supports PostgreSQL, MariaDB, and MySQL databases.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
          <div className="bg-white rounded-lg p-3 border border-blue-200">
            <p className="font-semibold text-blue-900"> PostgreSQL</p>
            <p className="text-gray-600">Transactional data, OLTP workloads</p>
          </div>
          <div className="bg-white rounded-lg p-3 border border-blue-200">
            <p className="font-semibold text-orange-900"> MariaDB</p>
            <p className="text-gray-600">Web applications, E-commerce</p>
          </div>
          <div className="bg-white rounded-lg p-3 border border-blue-200">
            <p className="font-semibold text-green-900"> Real-time</p>
            <p className="text-gray-600">Sub-second latency streaming</p>
          </div>
        </div>
      </div>

      {/* CDC Consumer Status */}
      <div className="bg-gradient-to-r from-yellow-50 to-orange-50 rounded-xl border-2 border-yellow-200 p-6">
        <div className="flex items-start justify-between">
          <div className="flex items-start gap-4">
            <div className={`p-4 rounded-xl ${cdcStatus?.running ? 'bg-green-500 animate-pulse' : 'bg-gray-300'}`}>
              <Zap className="w-8 h-8 text-white" />
            </div>
            <div>
              <h3 className="text-xl font-bold text-gray-900 mb-2 flex items-center gap-3">
                CDC Consumer
                {getStatusBadge(cdcStatus?.running)}
              </h3>
              <div className="space-y-1 text-sm text-gray-700">
                {cdcStatus?.running ? (
                  <>
                    <p><strong>PID:</strong> {cdcStatus.pid}</p>
                    <p><strong>Uptime:</strong> {Math.floor((cdcStatus.uptime_seconds || 0) / 60)} minutes</p>
                    <p><strong>Memory:</strong> {cdcStatus.memory_mb?.toFixed(1)} MB</p>
                    <p><strong>CPU:</strong> {cdcStatus.cpu_percent?.toFixed(1)}%</p>
                    <p className="text-gray-500 text-xs mt-2">{cdcStatus.command}</p>
                  </>
                ) : (
                  <p className="text-gray-600">CDC consumer is not running. Start it to begin streaming data changes.</p>
                )}
              </div>
            </div>
          </div>
          <div className="flex gap-2">
            {cdcStatus?.running ? (
              <button 
                onClick={handleStopCDC}
                disabled={loading}
                className="px-5 py-2.5 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:bg-gray-400 font-medium flex items-center gap-2"
              >
                <StopCircle className="w-4 h-4" />
                Stop CDC
              </button>
            ) : (
              <button 
                onClick={handleStartCDC}
                disabled={loading}
                className="px-5 py-2.5 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:bg-gray-400 font-medium flex items-center gap-2 animate-pulse"
              >
                <PlayCircle className="w-4 h-4" />
                Start CDC
              </button>
            )}
          </div>
        </div>
      </div>

      {/* System Health Grid */}
      {cdcHealth && (
        <div>
          <h3 className="text-lg font-semibold text-gray-900 mb-4">System Health Checks</h3>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            
            {cdcHealth.checks?.kafka && (
              <div className="bg-white rounded-xl border-2 border-gray-200 p-5 hover:shadow-md transition-shadow">
                <div className="flex items-start justify-between mb-3">
                  <div className="p-2 bg-purple-100 rounded-lg">
                    <Server className="w-6 h-6 text-purple-600" />
                  </div>
                  <div className={`px-2 py-1 rounded-full text-xs font-bold ${
                    cdcHealth.checks.kafka.status === 'up' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
                  }`}>
                    {cdcHealth.checks.kafka.status.toUpperCase()}
                  </div>
                </div>
                <h4 className="font-semibold text-gray-900 mb-1">Kafka Broker</h4>
                <p className="text-sm text-gray-600">{cdcHealth.checks.kafka.message}</p>
              </div>
            )}
            
            {cdcHealth.checks?.minio && (
              <div className="bg-white rounded-xl border-2 border-gray-200 p-5 hover:shadow-md transition-shadow">
                <div className="flex items-start justify-between mb-3">
                  <div className="p-2 bg-blue-100 rounded-lg">
                    <Layers className="w-6 h-6 text-blue-600" />
                  </div>
                  <div className={`px-2 py-1 rounded-full text-xs font-bold ${
                    cdcHealth.checks.minio.status === 'up' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
                  }`}>
                    {cdcHealth.checks.minio.status.toUpperCase()}
                  </div>
                </div>
                <h4 className="font-semibold text-gray-900 mb-1">MinIO Storage</h4>
                <p className="text-sm text-gray-600">{cdcHealth.checks.minio.message}</p>
              </div>
            )}
            
            {cdcHealth.checks?.cdc_consumer && (
              <div className="bg-white rounded-xl border-2 border-gray-200 p-5 hover:shadow-md transition-shadow">
                <div className="flex items-start justify-between mb-3">
                  <div className="p-2 bg-green-100 rounded-lg">
                    <Activity className="w-6 h-6 text-green-600" />
                  </div>
                  <div className={`px-2 py-1 rounded-full text-xs font-bold ${
                    cdcHealth.checks.cdc_consumer.status === 'up' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
                  }`}>
                    {cdcHealth.checks.cdc_consumer.status.toUpperCase()}
                  </div>
                </div>
                <h4 className="font-semibold text-gray-900 mb-1">CDC Consumer</h4>
                <p className="text-sm text-gray-600">{cdcHealth.checks.cdc_consumer.message}</p>
                {cdcHealth.checks.cdc_consumer.uptime_seconds && (
                  <p className="text-xs text-gray-500 mt-2">
                    Uptime: {Math.floor(cdcHealth.checks.cdc_consumer.uptime_seconds / 60)} minutes
                  </p>
                )}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Kafka Backlog */}
      <KafkaBacklog />

      {/* Setup Guide */}
      {(!cdcStatus || !cdcStatus.running) && (
        <div className="bg-yellow-50 border-2 border-yellow-300 rounded-xl p-6">
          <div className="flex items-start">
            <AlertTriangle className="w-6 h-6 text-yellow-600 mr-3 flex-shrink-0 mt-0.5" />
            <div className="flex-1">
              <h3 className="font-semibold text-yellow-900 mb-2">CDC Consumer Not Running</h3>
              <p className="text-yellow-800 text-sm mb-4">
                The CDC consumer needs to be started to stream real-time data changes from Kafka to Bronze layer.
              </p>
              <div className="space-y-2 text-sm text-yellow-900 bg-yellow-100 rounded-lg p-4">
                <p className="font-semibold">Quick Start Guide:</p>
                <ol className="list-decimal list-inside space-y-1 ml-2">
                  <li>Ensure Kafka is running on localhost:9092</li>
                  <li>Click the "Start CDC" button above</li>
                  <li>CDC consumer will begin streaming Kafka topics → Bronze Iceberg tables</li>
                  <li>Monitor progress in real-time on this page</li>
                </ol>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Action Bar */}
      <div className="flex justify-between items-center bg-gray-100 border-2 border-gray-200 rounded-xl p-5">
        <div className="text-sm text-gray-700">
          <p className="font-medium">Need help with CDC setup?</p>
          <p className="text-gray-500">Check the CDC documentation for detailed instructions</p>
        </div>
        <div className="flex gap-3">
          <button
            onClick={() => navigate(`/${domain}/bronze`)}
            className="px-4 py-2 border-2 border-gray-300 text-gray-700 rounded-lg hover:bg-white transition-colors font-medium"
          >
            View Bronze Layer
          </button>
          <button
            onClick={() => navigate(`/${domain}/silver`)}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors font-medium flex items-center gap-2"
          >
            Silver Processing
            <ArrowRight className="w-4 h-4" />
          </button>
        </div>
      </div>
    </div>
  );
}
