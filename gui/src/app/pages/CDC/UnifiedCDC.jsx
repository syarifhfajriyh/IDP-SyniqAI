import { useState, useEffect } from 'react';
import { useParams, useNavigate, useSearchParams } from 'react-router-dom';
import { 
  Database, Activity, CheckCircle, AlertCircle, Zap, RefreshCw,
  Wind, FileJson, Cloud
} from 'lucide-react';
import RealtimeCDCTab from './RealtimeCDCTab';
import MongoDBCDCTab from './MongoDBCDCTab';
import S3CDCTab from './S3CDCTab';

export default function UnifiedCDC() {
  const { domain } = useParams();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  
  // Get active tab from URL or default to 'realtime'
  const [activeTab, setActiveTab] = useState(searchParams.get('source') || 'realtime');
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [lastRefreshed, setLastRefreshed] = useState(null);

  // Update URL when tab changes
  const handleTabChange = (tab) => {
    setActiveTab(tab);
    setSearchParams({ source: tab });
  };

  // Callback for child components to update refresh time
  const handleRefreshed = () => {
    setLastRefreshed(new Date().toLocaleTimeString());
  };

  const tabs = [
    {
      id: 'realtime',
      name: 'Real-time CDC',
      icon: Zap,
      description: 'Debezium CDC for PostgreSQL & MariaDB',
      color: 'yellow'
    },
    {
      id: 'mongodb',
      name: 'MongoDB CDC',
      icon: FileJson,
      description: 'Airflow-orchestrated batch extraction',
      color: 'green'
    },
    {
      id: 's3',
      name: 'AWS S3 CDC',
      icon: Cloud,
      description: 'Extract files from S3 buckets',
      color: 'orange'
    }
  ];

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-gradient-to-r from-blue-600 via-purple-600 to-indigo-600 text-white px-8 py-6 shadow-lg">
        <div className="flex items-center justify-between max-w-[1600px] mx-auto">
          <div>
            <h1 className="text-3xl font-bold flex items-center">
              <Database className="w-8 h-8 mr-3" />
              Change Data Capture (CDC)
              <span className="ml-4 px-3 py-1 bg-white/20 rounded-full text-sm font-medium">
                Multi-Source Integration
              </span>
            </h1>
            <p className="text-blue-100 mt-2">
              Capture and stream data changes from multiple databases to your lakehouse
              {lastRefreshed && ` · Last refreshed: ${lastRefreshed}`}
            </p>
          </div>
          <div className="flex items-center gap-3">
            <button
              onClick={() => setAutoRefresh(!autoRefresh)}
              className={`px-4 py-2 rounded-lg border-2 text-sm font-medium transition-all ${
                autoRefresh
                  ? 'border-white bg-white/20 text-white'
                  : 'border-white/50 bg-transparent text-white'
              }`}
            >
              <RefreshCw className={`w-4 h-4 inline mr-2 ${autoRefresh ? 'animate-spin' : ''}`} />
              Auto-refresh {autoRefresh ? 'ON' : 'OFF'}
            </button>
          </div>
        </div>
      </div>

      {/* Tab Navigation */}
      <div className="bg-white border-b border-gray-200 shadow-sm">
        <div className="max-w-[1600px] mx-auto px-8">
          <div className="flex gap-1">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              const isActive = activeTab === tab.id;
              
              return (
                <button
                  key={tab.id}
                  onClick={() => handleTabChange(tab.id)}
                  className={`flex items-center gap-3 px-6 py-4 border-b-4 transition-all ${
                    isActive
                      ? `border-${tab.color}-500 bg-${tab.color}-50 text-${tab.color}-700`
                      : 'border-transparent text-gray-600 hover:bg-gray-50 hover:text-gray-900'
                  }`}
                >
                  <Icon className={`w-5 h-5 ${isActive ? `text-${tab.color}-600` : 'text-gray-500'}`} />
                  <div className="text-left">
                    <div className={`font-semibold text-sm ${isActive ? `text-${tab.color}-900` : 'text-gray-700'}`}>
                      {tab.name}
                    </div>
                    <div className="text-xs text-gray-500">
                      {tab.description}
                    </div>
                  </div>
                </button>
              );
            })}
          </div>
        </div>
      </div>

      {/* Tab Content */}
      <div className="max-w-[1600px] mx-auto px-8 py-6">
        {activeTab === 'realtime' && (
          <RealtimeCDCTab 
            domain={domain}
            autoRefresh={autoRefresh}
            onRefreshed={handleRefreshed}
          />
        )}
        {activeTab === 's3' && (
          <S3CDCTab 
            domain={domain}
            autoRefresh={autoRefresh}
            onRefreshed={handleRefreshed}
          />
        )}
        {activeTab === 'mongodb' && (
          <MongoDBCDCTab 
            domain={domain}
            autoRefresh={autoRefresh}
            onRefreshed={handleRefreshed}
          />
        )}
      </div>
    </div>
  );
}
