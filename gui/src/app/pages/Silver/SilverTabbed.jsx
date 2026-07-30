import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { 
  LayoutDashboard,
  Database,
  GitBranch,
  Code,
  Shield,
  Activity,
  Zap,
  Image,
  FolderOpen,
  Eye,
  FileText,
  Music,
  Brain,
  Workflow,
  AlertTriangle,
  Network,
  BarChart3
} from 'lucide-react';

// Import structured data page components
import Dashboard from './Dashboard';
import DataCatalog from './DataCatalog';
import DatasetViewer from './DatasetViewer';
import StructuredTransformation from './StructuredTransformationPro';
import SQLEditorEnhanced from './SQLEditorEnhanced';
import DataQualityRules from './DataQualityRules';
import JobMonitoring from './JobMonitoring';
import DataLineage from './DataLineage';
import ERDDiagram from './ERDDiagram';
import SilverEDA from './SilverEDA';

// Import unstructured data page components
import MediaDashboard from './Unstructured/MediaDashboard';
import FileBrowser from './Unstructured/FileBrowser';
import ObjectDetection from './Unstructured/ObjectDetection';
import TextExtraction from './Unstructured/TextExtraction';
import AudioAnalysis from './Unstructured/AudioAnalysis';
import MLModels from './Unstructured/MLModels';
import FeaturePipeline from './Unstructured/FeaturePipeline';
import MediaQuality from './Unstructured/MediaQuality';

/**
 * Silver - Data Transformation System UI
 * Supports both structured and unstructured data processing
 * Mode is controlled by URL parameter or prop
 */
export default function Silver({ mode = 'structured' }) {
  const [activePage, setActivePage] = useState(mode === 'structured' ? 'dashboard' : 'media-dashboard');
  const [selectedDataset, setSelectedDataset] = useState(null);
  const [searchParams] = useSearchParams();

  // Check for tab parameter in URL and set active page
  useEffect(() => {
    const tabParam = searchParams.get('tab');
    if (tabParam) {
      setActivePage(tabParam);
    } else if (mode === 'structured') {
      setActivePage('dashboard');
    } else {
      setActivePage('media-dashboard');
    }
  }, [mode, searchParams]);

  // Structured data navigation tabs
  const structuredNavItems = [
    { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
    { id: 'catalog', label: 'Data Catalog', icon: Database },
    { id: 'erd', label: 'ERD', icon: Network },
    { id: 'quality', label: 'Data Quality', icon: Shield },
    { id: 'transformation', label: 'Transformation', icon: Zap },
    { id: 'sql', label: 'SQL Editor', icon: Code },
    { id: 'eda', label: 'Silver EDA', icon: BarChart3 },
    { id: 'jobs', label: 'Job Monitoring', icon: Activity },
    { id: 'lineage', label: 'Data Lineage', icon: GitBranch }
  ];

  // Unstructured data navigation tabs
  const unstructuredNavItems = [
    { id: 'media-dashboard', label: 'Media Dashboard', icon: Image },
    { id: 'file-browser', label: 'File Browser', icon: FolderOpen },
    { id: 'object-detection', label: 'Object Detection', icon: Eye },
    { id: 'text-extraction', label: 'Text Extraction', icon: FileText },
    { id: 'audio-analysis', label: 'Audio Analysis', icon: Music },
    { id: 'ml-models', label: 'ML Models', icon: Brain },
    { id: 'feature-pipeline', label: 'Feature Pipeline', icon: Workflow },
    { id: 'media-quality', label: 'Media Quality', icon: AlertTriangle }
  ];

  // Select navigation items based on mode
  const navItems = mode === 'structured' ? structuredNavItems : unstructuredNavItems;

  const handleDatasetSelect = (dataset) => {
    setSelectedDataset(dataset);
    setActivePage('viewer');
  };

  const renderPage = () => {
    if (mode === 'structured') {
      // Structured data pages
      switch(activePage) {
        case 'dashboard':
          return <Dashboard />;
        case 'catalog':
          return <DataCatalog onDatasetSelect={handleDatasetSelect} />;
        case 'erd':
          return <ERDDiagram />;
        case 'viewer':
          return <DatasetViewer dataset={selectedDataset} />;
        case 'transformation':
          return <StructuredTransformation />;
        case 'sql':
          return <SQLEditorEnhanced />;
        case 'eda':
          return <SilverEDA />;
        case 'quality':
          return <DataQualityRules />;
        case 'jobs':
          return <JobMonitoring />;
        case 'lineage':
          return <DataLineage />;
        default:
          return <Dashboard />;
      }
    } else {
      // Unstructured data pages
      switch(activePage) {
        case 'media-dashboard':
          return <MediaDashboard />;
        case 'file-browser':
          return <FileBrowser />;
        case 'object-detection':
          return <ObjectDetection />;
        case 'text-extraction':
          return <TextExtraction />;
        case 'audio-analysis':
          return <AudioAnalysis />;
        case 'ml-models':
          return <MLModels />;
        case 'feature-pipeline':
          return <FeaturePipeline />;
        case 'media-quality':
          return <MediaQuality />;
        default:
          return <MediaDashboard />;
      }
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
