import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { 
  Cloud, CheckCircle, AlertCircle, PlayCircle, StopCircle,
  Wind, Clock, BarChart3, Settings, Eye, ArrowRight, Layers,
  Activity, HardDrive, FolderOpen
} from 'lucide-react';
import axios from 'axios';

const API_BASE = 'http://localhost:8000/api';
const AIRFLOW_BASE = 'http://localhost:8081/api/v1';

export default function S3CDCTab({ domain, autoRefresh, onRefreshed }) {
  const navigate = useNavigate();
  
  const [dagStatus, setDagStatus] = useState(null);
  const [dagRuns, setDagRuns] = useState([]);
  const [watermarks, setWatermarks] = useState({});
  const [kafkaHealth, setKafkaHealth] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [s3Config, setS3Config] = useState({
    aws_access_key_id: '',
    aws_secret_access_key: '',
    aws_region: 'ap-southeast-1',
    s3_bucket: '',
    prefixes: 'metadata/,data/'
  });
  
  const fetchDAGStatus = async () => {
    try {
      const response = await axios.get(
        `${AIRFLOW_BASE}/dags/syiniq_s3_batch_cdc`,
        { auth: { username: 'admin', password: 'admin123' } }
      );
      setDagStatus(response.data);
      setError(null);
    } catch (err) {
      console.error('Failed to fetch S3 DAG status:', err);
      setError('Unable to connect to Airflow. Is it running on port 8081?');
    }
  };
  
  const fetchDAGRuns = async () => {
    try {
      const response = await axios.get(
        `${AIRFLOW_BASE}/dags/syiniq_s3_batch_cdc/dagRuns?limit=10`,
        { auth: { username: 'admin', password: 'admin123' } }
      );
      setDagRuns(response.data.dag_runs || []);
    } catch (err) {
      console.error('Failed to fetch S3 DAG runs:', err);
    }
  };
  
  const fetchWatermarks = async () => {
    try {
      const response = await axios.get(`${API_BASE}/kafka/s3/watermarks`);
      setWatermarks(response.data.watermarks || {});
    } catch (err) {
      console.error('Failed to fetch S3 watermarks:', err);
    }
  };

  const checkKafkaHealth = async () => {
    try {
      const response = await axios.get(`${API_BASE}/kafka/health`);
      setKafkaHealth(response.data);
    } catch (err) {
      console.error('Failed to check Kafka health:', err);
    }
  };
  
  const refreshAll = async () => {
    setLoading(true);
    try {
      await Promise.all([
        fetchDAGStatus(),
        fetchDAGRuns(),
        fetchWatermarks(),
        checkKafkaHealth()
      ]);
      if (onRefreshed) onRefreshed();
    } finally {
      setLoading(false);
    }
  };
  
  const handleTriggerDAG = async () => {
    setLoading(true);
    try {
      const response = await axios.post(
        `${AIRFLOW_BASE}/dags/syiniq_s3_batch_cdc/dagRuns`,
        {
          conf: {
            aws_access_key_id: s3Config.aws_access_key_id,
            aws_secret_access_key: s3Config.aws_secret_access_key,
            aws_region: s3Config.aws_region,
            s3_bucket: s3Config.s3_bucket,
            prefixes: s3Config.prefixes.split(',').map(p => p.trim())
          }
        },
        { auth: { username: 'admin', password: 'admin123' } }
      );
      
      alert(` S3 DAG triggered successfully!\nRun ID: ${response.data.dag_run_id}\n\nCheck the Airflow UI for progress.`);
      await refreshAll();
    } catch (err) {
      console.error('Failed to trigger S3 DAG:', err);
      const msg = err.response?.data?.detail || err.message;
      alert(` Failed to trigger S3 DAG:\n\n${msg}\n\nMake sure Airflow is running on localhost:8081`);
    } finally {
      setLoading(false);
    }
  };
  
  const handleToggleDAG = async () => {
    setLoading(true);
    try {
      await axios.patch(
        `${AIRFLOW_BASE}/dags/syiniq_s3_batch_cdc`,
        { is_paused: !dagStatus?.is_paused },
        { auth: { username: 'admin', password: 'admin123' } }
      );
      await refreshAll();
    } catch (err) {
      console.error('Failed to toggle S3 DAG:', err);
      alert(` Failed to ${dagStatus?.is_paused ? 'unpause' : 'pause'} S3 DAG: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };
  
  const handleResetWatermarks = async () => {
    if (!confirm(' Reset all S3 watermarks? This will re-extract all objects from the beginning.')) {
      return;
    }
    
    setLoading(true);
    try {
      // Delete watermarks file via filesystem (need to add backend endpoint)
      alert(' Watermark reset endpoint not yet implemented. Manually delete:\nKafka Integration/watermarks/s3_watermarks.json');
      await refreshAll();
    } catch (err) {
      console.error('Failed to reset S3 watermarks:', err);
      alert(`Failed to reset watermarks: ${err.message}`);
    } finally {
      setLoading(false);
    }
  };
  
  const handleUpdateConfig = async () => {
    try {
      // Store config in localStorage for now (or add backend endpoint)
      localStorage.setItem('s3_cdc_config', JSON.stringify(s3Config));
      alert(' S3 configuration saved locally!\n\nUpdate the .env file in Kafka Integration folder with these values for permanent storage.');
      await refreshAll();
    } catch (err) {
      alert(`Failed to update config: ${err.message}`);
    }
  };
  
  useEffect(() => {
    // Load saved config from localStorage
    const savedConfig = localStorage.getItem('s3_cdc_config');
    if (savedConfig) {
      setS3Config(JSON.parse(savedConfig));
    }
    refreshAll();
  }, []);
  
  useEffect(() => {
    if (!autoRefresh) return;
    const interval = setInterval(refreshAll, 10000);
    return () => clearInterval(interval);
  }, [autoRefresh]);
  
  const getStatusBadge = (state) => {
    const badges = {
      'success': { color: 'green', icon: CheckCircle, text: 'Success' },
      'running': { color: 'blue', icon: Activity, text: 'Running' },
      'failed': { color: 'red', icon: AlertCircle, text: 'Failed' },
      'queued': { color: 'yellow', icon: Clock, text: 'Queued' }
    };
    
    const badge = badges[state] || { color: 'gray', icon: AlertCircle, text: state };
    const Icon = badge.icon;
    
    return (
      <span className={`px-3 py-1 bg-${badge.color}-100 text-${badge.color}-700 rounded-full text-xs font-semibold flex items-center`}>
        <Icon className="w-3 h-3 mr-1" />
        {badge.text}
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
            <p className="text-red-800 font-semibold">Connection Error</p>
            <p className="text-red-700 text-sm mt-1">{error}</p>
            <p className="text-red-600 text-xs mt-2">
              💡 Make sure Airflow webserver is running on <strong>http://localhost:8081</strong>
            </p>
          </div>
        </div>
      )}

      {/* Intro Card */}
      <div className="bg-gradient-to-r from-orange-50 to-amber-50 rounded-xl border-2 border-orange-200 p-6">
        <h2 className="text-xl font-bold text-gray-900 mb-2 flex items-center">
          <Cloud className="w-6 h-6 text-orange-600 mr-2" />
          AWS S3 Batch CDC with Apache Airflow
        </h2>
        <p className="text-gray-700 mb-3">
          Orchestrated batch extraction from AWS S3 using Airflow DAGs. Monitors S3 prefixes for new objects and streams them to your lakehouse.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
          <div className="bg-white rounded-lg p-3 border border-orange-200">
            <p className="font-semibold text-orange-900"> AWS S3</p>
            <p className="text-gray-600">Object storage, any file type</p>
          </div>
          <div className="bg-white rounded-lg p-3 border border-orange-200">
            <p className="font-semibold text-blue-900"> Scheduled</p>
            <p className="text-gray-600">Every hour (configurable)</p>
          </div>
          <div className="bg-white rounded-lg p-3 border border-orange-200">
            <p className="font-semibold text-purple-900"> Multi-Prefix</p>
            <p className="text-gray-600">Track multiple S3 folders</p>
          </div>
        </div>
      </div>

      {/* DAG Status Card */}
      <div className="bg-gradient-to-br from-orange-50 to-amber-50 rounded-xl border-2 border-orange-200 p-6">
        <div className="flex items-start justify-between">
          <div className="flex items-start gap-4">
            <div className={`p-4 rounded-xl ${dagStatus?.is_paused === false ? 'bg-orange-500 animate-pulse' : 'bg-gray-400'}`}>
              <Wind className="w-8 h-8 text-white" />
            </div>
            <div>
              <h3 className="text-xl font-bold text-gray-900 mb-2 flex items-center gap-3">
                S3 CDC DAG
                {dagStatus?.is_paused === false ? (
                  <span className="px-3 py-1 bg-green-100 text-green-700 rounded-full text-sm font-semibold flex items-center">
                    <CheckCircle className="w-4 h-4 mr-1" />
                    Active
                  </span>
                ) : (
                  <span className="px-3 py-1 bg-gray-200 text-gray-700 rounded-full text-sm font-semibold flex items-center">
                    <StopCircle className="w-4 h-4 mr-1" />
                    Paused
                  </span>
                )}
              </h3>
              <div className="space-y-1 text-sm text-gray-700">
                {dagStatus ? (
                  <>
                    <p><strong>Schedule:</strong> {dagStatus.schedule_interval || 'Manual'} (Every hour)</p>
                    <p><strong>Next Run:</strong> {dagStatus.next_dagrun ? new Date(dagStatus.next_dagrun).toLocaleString() : 'N/A'}</p>
                    <p><strong>Last Run:</strong> {dagStatus.last_run_state || 'Never'}</p>
                  </>
                ) : (
                  <p className="text-gray-600">Loading DAG information from Airflow...</p>
                )}
              </div>
            </div>
          </div>
          <div className="flex flex-col gap-2">
            <button 
              onClick={handleTriggerDAG}
              disabled={loading || !dagStatus}
              className="px-5 py-2.5 bg-orange-600 text-white rounded-lg hover:bg-orange-700 disabled:bg-gray-400 font-medium flex items-center gap-2"
            >
              <PlayCircle className="w-4 h-4" />
              Trigger DAG Now
            </button>
            <button 
              onClick={handleToggleDAG}
              disabled={loading || !dagStatus}
              className="px-5 py-2.5 bg-gray-600 text-white rounded-lg hover:bg-gray-700 disabled:bg-gray-400 font-medium flex items-center gap-2"
            >
              {dagStatus?.is_paused ? <PlayCircle className="w-4 h-4" /> : <StopCircle className="w-4 h-4" />}
              {dagStatus?.is_paused ? 'Unpause' : 'Pause'} DAG
            </button>
            <a
              href="http://localhost:8081/dags/syiniq_s3_batch_cdc/grid"
              target="_blank"
              rel="noopener noreferrer"
              className="px-5 py-2.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-medium flex items-center gap-2 text-center"
            >
              <Eye className="w-4 h-4" />
              Open in Airflow
            </a>
          </div>
        </div>
      </div>

      {/* Health Checks Grid */}
      <div>
        <h3 className="text-lg font-semibold text-gray-900 mb-4">System Health</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          
          <div className="bg-white rounded-xl border-2 border-gray-200 p-5 hover:shadow-md transition-shadow">
            <div className="flex items-start justify-between mb-3">
              <div className="p-2 bg-orange-100 rounded-lg">
                <Cloud className="w-6 h-6 text-orange-600" />
              </div>
              <div className="px-2 py-1 rounded-full text-xs font-bold bg-gray-100 text-gray-700">
                AWS S3
              </div>
            </div>
            <h4 className="font-semibold text-gray-900 mb-1">S3 Source</h4>
            <p className="text-sm text-gray-600">
              {s3Config.s3_bucket ? `Bucket: ${s3Config.s3_bucket}` : 'Not configured'}
            </p>
            {s3Config.aws_region && (
              <p className="text-xs text-gray-500 mt-2">
                Region: {s3Config.aws_region}
              </p>
            )}
          </div>
          
          <div className="bg-white rounded-xl border-2 border-gray-200 p-5 hover:shadow-md transition-shadow">
            <div className="flex items-start justify-between mb-3">
              <div className="p-2 bg-purple-100 rounded-lg">
                <Layers className="w-6 h-6 text-purple-600" />
              </div>
              <div className={`px-2 py-1 rounded-full text-xs font-bold ${
                kafkaHealth?.status === 'healthy' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
              }`}>
                {kafkaHealth?.status?.toUpperCase() || 'UNKNOWN'}
              </div>
            </div>
            <h4 className="font-semibold text-gray-900 mb-1">Kafka Broker</h4>
            <p className="text-sm text-gray-600">{kafkaHealth?.message || 'Checking connection...'}</p>
          </div>
        </div>
      </div>

      {/* S3 Configuration */}
      <div className="bg-white rounded-xl border-2 border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
          <Settings className="w-5 h-5 mr-2 text-gray-600" />
          AWS S3 Configuration
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              AWS Access Key ID
            </label>
            <input
              type="text"
              value={s3Config.aws_access_key_id}
              onChange={(e) => setS3Config({...s3Config, aws_access_key_id: e.target.value})}
              className="w-full px-4 py-2 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:outline-none font-mono text-sm"
              placeholder="AKIA..."
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              AWS Secret Access Key
            </label>
            <input
              type="password"
              value={s3Config.aws_secret_access_key}
              onChange={(e) => setS3Config({...s3Config, aws_secret_access_key: e.target.value})}
              className="w-full px-4 py-2 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:outline-none font-mono text-sm"
              placeholder="••••••••"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              AWS Region
            </label>
            <input
              type="text"
              value={s3Config.aws_region}
              onChange={(e) => setS3Config({...s3Config, aws_region: e.target.value})}
              className="w-full px-4 py-2 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:outline-none"
              placeholder="ap-southeast-1"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              S3 Bucket Name
            </label>
            <input
              type="text"
              value={s3Config.s3_bucket}
              onChange={(e) => setS3Config({...s3Config, s3_bucket: e.target.value})}
              className="w-full px-4 py-2 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:outline-none"
              placeholder="my-data-bucket"
            />
          </div>
          <div className="md:col-span-2">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              S3 Prefixes to Monitor (comma-separated)
            </label>
            <input
              type="text"
              value={s3Config.prefixes}
              onChange={(e) => setS3Config({...s3Config, prefixes: e.target.value})}
              className="w-full px-4 py-2 border-2 border-gray-300 rounded-lg focus:border-orange-500 focus:outline-none"
              placeholder="metadata/,data/,raw/"
            />
            <p className="text-xs text-gray-500 mt-1">
              💡 Each prefix becomes a separate Kafka topic: airflow.s3.&lt;prefix_name&gt;
            </p>
          </div>
        </div>
        <div className="flex justify-end gap-3 mt-4">
          <button
            onClick={handleUpdateConfig}
            className="px-5 py-2 bg-orange-600 text-white rounded-lg hover:bg-orange-700 font-medium"
          >
            Save Configuration
          </button>
        </div>
      </div>

      {/* Watermarks Status */}
      <div className="bg-white rounded-xl border-2 border-gray-200 p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold text-gray-900 flex items-center">
            <Clock className="w-5 h-5 mr-2 text-gray-600" />
            Per-Prefix Watermarks (LastModified Tracking)
          </h3>
          <button
            onClick={handleResetWatermarks}
            className="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 font-medium text-sm"
          >
            Reset All Watermarks
          </button>
        </div>
        {Object.keys(watermarks).length > 0 ? (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {Object.entries(watermarks).map(([prefix, watermark]) => (
              <div key={prefix} className="bg-gray-50 rounded-lg p-4 border border-gray-200">
                <div className="flex items-center mb-2">
                  <FolderOpen className="w-4 h-4 text-orange-500 mr-2" />
                  <p className="font-semibold text-gray-900">{prefix}</p>
                </div>
                <p className="text-sm text-gray-600">
                  {watermark ? new Date(watermark).toLocaleString() : 'No watermark'}
                </p>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-gray-500 text-sm">No watermarks found. Run the DAG to create watermarks for each S3 prefix.</p>
        )}
      </div>

      {/* Recent DAG Runs */}
      <div className="bg-white rounded-xl border-2 border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4 flex items-center">
          <BarChart3 className="w-5 h-5 mr-2 text-gray-600" />
          Recent DAG Runs
        </h3>
        {dagRuns.length > 0 ? (
          <div className="space-y-3">
            {dagRuns.slice(0, 5).map((run) => (
              <div key={run.dag_run_id} className="flex items-center justify-between p-4 bg-gray-50 rounded-lg border border-gray-200 hover:bg-gray-100 transition-colors">
                <div className="flex items-center gap-4">
                  {getStatusBadge(run.state)}
                  <div>
                    <p className="font-medium text-gray-900">{run.dag_run_id}</p>
                    <p className="text-xs text-gray-500">
                      Started: {new Date(run.start_date).toLocaleString()}
                    </p>
                  </div>
                </div>
                <div className="text-right">
                  <p className="text-sm text-gray-600">
                    Duration: {run.duration ? `${Math.floor(run.duration)}s` : 'In progress...'}
                  </p>
                  <a
                    href={`http://localhost:8081/dags/syiniq_s3_batch_cdc/grid?dag_run_id=${run.dag_run_id}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-xs text-blue-600 hover:underline"
                  >
                    View Details →
                  </a>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <p className="text-gray-500 text-sm">No DAG runs yet. Trigger the DAG to start S3 extraction.</p>
        )}
      </div>

      {/* Quick Actions */}
      <div className="flex justify-between items-center bg-gradient-to-r from-orange-100 to-amber-100 border-2 border-orange-300 rounded-xl p-5">
        <div className="text-sm text-gray-800">
          <p className="font-semibold text-lg mb-1"> Quick Start Guide</p>
          <ol className="list-decimal list-inside space-y-1 text-gray-700">
            <li>Configure AWS S3 credentials above</li>
            <li>Specify bucket and prefixes to monitor</li>
            <li>Click "Trigger DAG Now" to start extraction</li>
            <li>Data flows: S3 → Kafka → Spark → Bronze Iceberg</li>
          </ol>
        </div>
        <div className="flex gap-3">
          <button
            onClick={() => navigate(`/${domain}/bronze`)}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors font-medium flex items-center gap-2"
          >
            View Bronze Layer
            <ArrowRight className="w-4 h-4" />
          </button>
        </div>
      </div>
    </div>
  );
}
