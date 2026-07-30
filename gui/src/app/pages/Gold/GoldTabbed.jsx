import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { 
  LayoutDashboard, 
  Sparkles,
  Activity,
  Shield,
  Zap,
  GitBranch
} from 'lucide-react';

// Import Gold layer page components
import GoldDashboard from './GoldDashboard';
import GoldTransformation from './GoldTransformation';
import QualityMonitoring from './QualityMonitoring';
import ExploratoryAnalysis from './ExploratoryAnalysis';
import JobMonitoring from '../Silver/JobMonitoring';
import DataLineage from '../Silver/DataLineage';

/**
 * GoldTabbed - Navigation component for Gold Layer Processing
 * Includes: Overview, Transformation, Quality, EDA, Job Monitoring, Data Lineage
 */
export default function GoldTabbed() {
  const [activePage, setActivePage] = useState('overview');
  const [searchParams] = useSearchParams();

  // Check for tab parameter in URL and set active page
  useEffect(() => {
    const tabParam = searchParams.get('tab');
    if (tabParam) {
      setActivePage(tabParam);
    } else {
      setActivePage('overview');
    }
  }, [searchParams]);

  // Gold layer navigation tabs
  const navItems = [
    { id: 'overview', label: 'Overview', icon: LayoutDashboard },
    { id: 'transformation', label: 'Gold Transformation', icon: Zap },
    { id: 'quality', label: 'Quality Monitoring', icon: Shield },
    { id: 'eda', label: 'Exploratory Analysis', icon: Sparkles },
    { id: 'jobs', label: 'Job Monitoring', icon: Activity },
    { id: 'lineage', label: 'Data Lineage', icon: GitBranch }
  ];

  const renderPage = () => {
    switch(activePage) {
      case 'overview':
        return <GoldDashboard />;
      case 'transformation':
        return <GoldTransformation />;
      case 'quality':
        return <QualityMonitoring />;
      case 'eda':
        return <ExploratoryAnalysis />;
      case 'jobs':
        return <JobMonitoring />;
      case 'lineage':
        return <DataLineage />;
      default:
        return <GoldDashboard />;
    }
  };

  return (
    <div className="h-full w-full bg-gray-50">
      {/* Top Navigation Tabs */}
      <div className="bg-white border-b border-gray-200">
        <div className="flex gap-1 px-6">
          {navItems.map(item => {
            const Icon = item.icon;
            const isActive = activePage === item.id;
            return (
              <button
                key={item.id}
                onClick={() => setActivePage(item.id)}
                className={`px-4 py-3 text-sm font-medium border-b-2 transition-colors flex items-center gap-2 ${
                  isActive
                    ? 'border-blue-600 text-blue-600'
                    : 'border-transparent text-gray-600 hover:text-gray-900'
                }`}
              >
                <Icon className="w-4 h-4" />
                {item.label}
              </button>
            );
          })}
        </div>
      </div>

      {/* Page Content */}
      <div className="h-full overflow-hidden">
        {renderPage()}
      </div>
    </div>
  );
}
