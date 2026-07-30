import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Shield, Plus, Play, CheckCircle, XCircle, AlertTriangle, Search, Filter, TrendingUp, RefreshCw, Download, Eye, Trash2, ChevronDown, ChevronUp, Calendar, Clock } from 'lucide-react';
import axios from 'axios';

/**
 * DataQualityRules - Manage data quality rules and view validation results
 * Supports rule creation, execution, and monitoring
 */
export default function DataQualityRules() {
  const [searchParams] = useSearchParams();
  const [activeTab, setActiveTab] = useState('rules'); // 'rules', 'results', 'quarantine', 'monitoring'
  const [selectedDataset, setSelectedDataset] = useState('finance_transactions');
  const [searchQuery, setSearchQuery] = useState('');
  const [filterCategory, setFilterCategory] = useState('all');
  
  // Real data state
  const [rules, setRules] = useState([]);
  const [rulesLoading, setRulesLoading] = useState(false);
  const [executionHistory, setExecutionHistory] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [quarantineSummary, setQuarantineSummary] = useState({});
  const [quarantineRecords, setQuarantineRecords] = useState([]);
  const [quarantineLoading, setQuarantineLoading] = useState(false);
  const [quarantineFilter, setQuarantineFilter] = useState('pending'); // pending, resolved, all
  const [executing, setExecuting] = useState(false);
  const [selectedRecord, setSelectedRecord] = useState(null); // For detail modal
  const [expandedExecution, setExpandedExecution] = useState(null); // For execution detail expansion
  
  // Monitoring state
  const [qualityTrends, setQualityTrends] = useState([]);
  const [categoryMetrics, setCategoryMetrics] = useState({});
  const [monitoringLoading, setMonitoringLoading] = useState(false);
  
  // Rule Builder Modal state
  const [showRuleBuilder, setShowRuleBuilder] = useState(false);
  const [tableSchema, setTableSchema] = useState([]);
  const [newRule, setNewRule] = useState({
    rule_name: '',
    category: 'data_quality',
    rule_type: 'not_null',
    description: '',
    target_columns: [],
    condition_expression: '',
    severity: 'WARNING',
    action: 'quarantine_row',
    execution_priority: 5
  });

  // Read table/domain/source from URL params when navigating from Check Quality button
  useEffect(() => {
    const tableParam = searchParams.get('table');
    const domainParam = searchParams.get('domain');
    const sourceParam = searchParams.get('source');
    
    if (tableParam) {
      console.log('DataQualityRules received params:', {
        table: tableParam,
        domain: domainParam,
        source: sourceParam
      });
      setSelectedDataset(tableParam);
      // You can also use domainParam and sourceParam to filter data or pre-configure the quality checks
    }
  }, [searchParams]);

  // Fetch rules when selectedDataset changes
  useEffect(() => {
    const fetchRules = async () => {
      if (!selectedDataset) return;
      
      setRulesLoading(true);
      try {
        const response = await axios.get(`http://localhost:8000/api/quality-rules/tables/${selectedDataset}`, {
          params: { domain: 'finance' }
        });
        
        if (response.data.success) {
          setRules(response.data.rules || []);
          console.log(`✓ Loaded ${response.data.rules?.length || 0} quality rules for ${selectedDataset}`);
        }
      } catch (error) {
        console.error('Error fetching rules:', error);
        setRules([]);
      } finally {
        setRulesLoading(false);
      }
    };
    
    fetchRules();
  }, [selectedDataset]);

  // Fetch execution history when Results tab is opened
  useEffect(() => {
    const fetchHistory = async () => {
      if (activeTab === 'results' && selectedDataset) {
        setHistoryLoading(true);
        try {
          console.log(`Fetching execution history for ${selectedDataset}...`);
          const response = await axios.get(`http://localhost:8000/api/quality-rules/tables/${selectedDataset}/execution-history`, {
            params: { domain: 'finance', limit: 10 }
          });
          
          console.log('Execution history response:', response.data);
          console.log('Response success:', response.data.success);
          console.log('History array:', response.data.history);
          console.log('History count:', response.data.history_count);
          
          if (response.data.success) {
            setExecutionHistory(response.data.history || []);
            console.log(`✓ Loaded ${response.data.history?.length || 0} execution records`);
          }
        } catch (error) {
          console.error('Error fetching execution history:', error);
          setExecutionHistory([]);
        } finally {
          setHistoryLoading(false);
        }
      }
    };
    
    fetchHistory();
  }, [activeTab, selectedDataset]);

  // Fetch monitoring data when Monitoring tab is opened
  useEffect(() => {
    const fetchMonitoringData = async () => {
      if (activeTab === 'monitoring' && selectedDataset) {
        setMonitoringLoading(true);
        try {
          // Fetch execution history for trends (last 30 records)
          const historyResponse = await axios.get(`http://localhost:8000/api/quality-rules/tables/${selectedDataset}/execution-history`, {
            params: { domain: 'finance', limit: 30 }
          });
          
          if (historyResponse.data.success && historyResponse.data.history.length > 0) {
            const history = historyResponse.data.history;
            
            // Group by date and calculate daily average quality score
            const dailyScores = {};
            history.forEach(record => {
              const date = new Date(record.execution_timestamp).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
              const score = record.total_rows_processed > 0 
                ? (record.rows_passed / record.total_rows_processed) * 100 
                : 100;
              
              if (!dailyScores[date]) {
                dailyScores[date] = { total: 0, count: 0 };
              }
              dailyScores[date].total += score;
              dailyScores[date].count += 1;
            });
            
            // Convert to trend array (last 7 days)
            const trends = Object.entries(dailyScores)
              .map(([date, data]) => ({
                date,
                score: (data.total / data.count).toFixed(2)
              }))
              .slice(0, 7)
              .reverse();
            
            setQualityTrends(trends);
            
            // Calculate category-wise metrics from rules and execution history
            const categoryStats = {};
            rules.forEach(rule => {
              const categoryName = rule.rule_name?.split(':')[0] || 'Other';
              if (!categoryStats[categoryName]) {
                categoryStats[categoryName] = { passed: 0, total: 0, count: 0 };
              }
              categoryStats[categoryName].count += 1;
            });
            
            // Add execution stats by matching rule names
            history.forEach(record => {
              const ruleName = record.rule_name || '';
              const categoryName = ruleName.split(':')[0] || 'Other';
              if (categoryStats[categoryName]) {
                categoryStats[categoryName].total += record.total_rows_processed || 0;
                categoryStats[categoryName].passed += record.rows_passed || 0;
              }
            });
            
            setCategoryMetrics(categoryStats);
          }
        } catch (error) {
          console.error('Error fetching monitoring data:', error);
        } finally {
          setMonitoringLoading(false);
        }
      }
    };
    
    fetchMonitoringData();
  }, [activeTab, selectedDataset, rules]);

  // Fetch quarantine records when Quarantine tab is opened
  useEffect(() => {
    const fetchQuarantineRecords = async () => {
      if (activeTab === 'quarantine' && selectedDataset) {
        setQuarantineLoading(true);
        try {
          const response = await axios.get(`http://localhost:8000/api/quality-rules/tables/${selectedDataset}/quarantine`, {
            params: { 
              domain: 'finance',
              status: quarantineFilter,
              limit: 100 
            }
          });
          
          if (response.data.success) {
            setQuarantineRecords(response.data.records || []);
            console.log(`✓ Loaded ${response.data.records?.length || 0} quarantine records`);
          }
        } catch (error) {
          console.error('Error fetching quarantine records:', error);
          setQuarantineRecords([]);
        } finally {
          setQuarantineLoading(false);
        }
      }
    };
    
    fetchQuarantineRecords();
  }, [activeTab, selectedDataset, quarantineFilter]);

  // Open Rule Builder and fetch table schema
  const handleOpenRuleBuilder = async () => {
    setShowRuleBuilder(true);
    try {
      const response = await axios.get(`http://localhost:8000/api/bronze-data/schema/${selectedDataset}`, {
        params: { domain: 'finance', source: 'transactions' }
      });
      if (response.data.success && response.data.schema) {
        setTableSchema(response.data.schema);
      }
    } catch (error) {
      console.error('Error fetching table schema:', error);
      setTableSchema([]);
    }
  };

  // Create new rule
  const handleCreateRule = async () => {
    if (!newRule.rule_name || !newRule.condition_expression) {
      alert('Please provide rule name and condition expression');
      return;
    }

    try {
      const ruleData = {
        ...newRule,
        domain: 'finance',
        target_table: selectedDataset,
        created_by: 'user'
      };

      const response = await axios.post('http://localhost:8000/api/quality-rules/rules', ruleData);
      
      if (response.data.success) {
        alert(`✓ Rule "${newRule.rule_name}" created successfully!`);
        setShowRuleBuilder(false);
        // Reset form
        setNewRule({
          rule_name: '',
          category: 'data_quality',
          rule_type: 'not_null',
          description: '',
          target_columns: [],
          condition_expression: '',
          severity: 'WARNING',
          action: 'quarantine_row',
          execution_priority: 5
        });
        // Refresh rules list
        const rulesResponse = await axios.get(`http://localhost:8000/api/quality-rules/tables/${selectedDataset}`, {
          params: { domain: 'finance' }
        });
        if (rulesResponse.data.success) {
          setRules(rulesResponse.data.rules || []);
        }
      }
    } catch (error) {
      console.error('Error creating rule:', error);
      alert(`Error creating rule: ${error.response?.data?.detail || error.message}`);
    }
  };

  // Toggle rule status (activate/deactivate)
  const handleToggleRuleStatus = async (ruleId, currentIsActive) => {
    try {
      const response = await axios.put(`http://localhost:8000/api/quality-rules/rules/${ruleId}`, {
        is_active: !currentIsActive
      });
      
      if (response.data.success) {
        // Refresh rules list
        const rulesResponse = await axios.get(`http://localhost:8000/api/quality-rules/tables/${selectedDataset}`, {
          params: { domain: 'finance' }
        });
        if (rulesResponse.data.success) {
          setRules(rulesResponse.data.rules || []);
        }
      }
    } catch (error) {
      console.error('Error toggling rule status:', error);
      alert(`Error toggling rule: ${error.response?.data?.detail || error.message}`);
    }
  };

  // Execute quality check
  const handleRunQualityCheck = async () => {
    if (!selectedDataset) {
      alert('Please select a dataset first');
      return;
    }
    
    // Check if there are any active rules
    const activeRules = rules.filter(rule => rule.is_active === true);
    if (activeRules.length === 0) {
      alert(`⚠️ No active quality rules found for "${selectedDataset}"\n\nPlease:\n1. Click "+ Create Rule" button to define quality rules\n2. Make sure rules are activated (toggle should be blue)\n3. Then run the quality check\n\nNote: Rules must be active (enabled) to be executed.`);
      return;
    }
    
    setExecuting(true);
    try {
      console.log(`🔍 Running quality check on ${selectedDataset} with ${activeRules.length} active rules...`);
      
      const response = await axios.post(`http://localhost:8000/api/quality-rules/tables/${selectedDataset}/execute`, {
        table_name: selectedDataset,
        domain: 'finance',
        source: 'postgres',
        limit: null  // Check all rows
      });
      
      if (response.data.success) {
        const results = response.data.results;
        console.log('✓ Quality check complete:', results);
        
        // Check if all rows passed (suspicious if there should be issues)
        const allPassed = results.rows_failed === 0;
        const hasNullCheckRules = rules.some(r => 
          r.condition_expression && r.condition_expression.toLowerCase().includes('is not null')
        );
        
        let alertMessage = `Quality Check Complete!\n\nTotal Rows: ${results.total_rows}\nPassed: ${results.rows_passed}\nFailed: ${results.rows_failed}\nQuality Score: ${results.quality_score}%`;
        
        if (allPassed && hasNullCheckRules && results.total_rows > 0) {
          alertMessage += `\n\n⚠️ DIAGNOSTIC:\nAll rows passed, but you have null check rules.\n\nPossible causes:\n1. Your data shows "null" as TEXT (not SQL NULL)\n2. Try: COLUMN_NAME IS NOT NULL AND COLUMN_NAME != 'null'\n3. Check backend logs for detailed diagnostics`;
        }
        
        alert(alertMessage);
        
        // Refresh execution history with a small delay to ensure DB commit
        setTimeout(() => {
          setActiveTab('results');
        }, 500);
      } else {
        alert(`Quality check failed: ${response.data.error}`);
      }
    } catch (error) {
      console.error('Error executing quality check:', error);
      alert(`Error: ${error.response?.data?.error || error.message}`);
    } finally {
      setExecuting(false);
    }
  };

  // Resolve quarantine record
  const handleResolveQuarantine = async (quarantineId, resolution = "manually_resolved") => {
    try {
      const response = await axios.post(`http://localhost:8000/api/quality-rules/quarantine/${quarantineId}/resolve`, null, {
        params: {
          resolution,
          resolved_by: 'user'
        }
      });
      
      if (response.data.success) {
        alert('Quarantine record resolved successfully!');
        // Refresh quarantine list
        setQuarantineFilter('pending'); // Reset to show pending records
      }
    } catch (error) {
      console.error('Error resolving quarantine:', error);
      alert(`Error: ${error.response?.data?.detail || error.message}`);
    }
  };

  // Download quarantine data
  const handleDownloadQuarantine = async (executionId) => {
    try {
      const response = await axios.get(`http://localhost:8000/api/quality-rules/quarantine/download/${executionId}`, {
        params: {
          domain: 'finance',
          source: 'postgres',
          table_name: selectedDataset
        },
        responseType: 'blob'
      });
      
      // Create download link
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `quarantine_${executionId}.csv`);
      document.body.appendChild(link);
      link.click();
      link.remove();
      
      console.log(`✓ Downloaded quarantine data for execution: ${executionId}`);
    } catch (error) {
      console.error('Error downloading quarantine data:', error);
      alert(`Error: ${error.response?.data?.error || error.message}`);
    }
  };

  // Rule categories for filtering (data_quality is the category in DB)
  const ruleCategories = [
    { id: 'completeness', name: 'Completeness', count: rules.filter(r => r.rule_name?.includes('Completeness')).length, icon: CheckCircle, color: '#10b981' },
    { id: 'validity', name: 'Validity', count: rules.filter(r => r.rule_name?.includes('Validity')).length, icon: Shield, color: '#3b82f6' },
    { id: 'consistency', name: 'Consistency', count: rules.filter(r => r.rule_name?.includes('Consistency')).length, icon: AlertTriangle, color: '#f59e0b' },
    { id: 'uniqueness', name: 'Uniqueness', count: rules.filter(r => r.rule_name?.includes('Uniqueness')).length, icon: TrendingUp, color: '#8b5cf6' },
    { id: 'data_quality', name: 'Data Quality', count: rules.filter(r => r.category === 'data_quality').length, icon: Shield, color: '#3b82f6' }
  ];

  const getSeverityColor = (severity) => {
    const sev = severity?.toUpperCase();
    switch(sev) {
      case 'CRITICAL': return 'bg-red-100 text-red-800';
      case 'HIGH': return 'bg-orange-100 text-orange-800';
      case 'WARNING': 
      case 'MEDIUM': return 'bg-yellow-100 text-yellow-800';
      case 'INFO':
      case 'LOW': return 'bg-blue-100 text-blue-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const getActionColor = (action) => {
    switch(action) {
      case 'quarantine_row': return 'bg-red-50 text-red-700 border-red-200';
      case 'block_table': return 'bg-red-50 text-red-700 border-red-200';
      case 'warn': 
      case 'alert': return 'bg-yellow-50 text-yellow-700 border-yellow-200';
      case 'log': return 'bg-blue-50 text-blue-700 border-blue-200';
      default: return 'bg-gray-50 text-gray-700 border-gray-200';
    }
  };

  const getScoreColor = (score) => {
    if (score >= 99) return 'text-green-600';
    if (score >= 95) return 'text-yellow-600';
    return 'text-red-600';
  };

  const filteredRules = rules.filter(rule => {
    const matchesSearch = rule.rule_name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
                         rule.target_columns?.some(col => col?.toLowerCase().includes(searchQuery.toLowerCase()));
    
    // Match by rule name prefix (Completeness:, Validity:, etc.) or actual category
    let matchesCategory = filterCategory === 'all';
    if (!matchesCategory) {
      if (filterCategory === 'completeness' && rule.rule_name?.startsWith('Completeness:')) matchesCategory = true;
      if (filterCategory === 'validity' && rule.rule_name?.startsWith('Validity:')) matchesCategory = true;
      if (filterCategory === 'consistency' && rule.rule_name?.startsWith('Consistency:')) matchesCategory = true;
      if (filterCategory === 'uniqueness' && rule.rule_name?.startsWith('Uniqueness:')) matchesCategory = true;
      if (filterCategory === 'data_quality' && rule.category === 'data_quality') matchesCategory = true;
    }
    
    return matchesSearch && matchesCategory;
  });

  return (
    <div className="min-h-screen flex flex-col bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200 p-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
              <Shield className="w-6 h-6" />
              Data Quality Rules
            </h1>
            <p className="text-sm text-gray-500 mt-1">Define and monitor data quality validation rules</p>
          </div>
          <div className="flex gap-2">
            <button
              onClick={handleRunQualityCheck}
              disabled={executing || rulesLoading || rules.length === 0}
              className="px-4 py-2 bg-gradient-to-r from-green-600 to-emerald-600 text-white rounded-lg text-sm font-medium hover:from-green-700 hover:to-emerald-700 disabled:from-gray-300 disabled:to-gray-400 disabled:cursor-not-allowed flex items-center gap-2 shadow-md"
              title={rules.length === 0 ? "Create quality rules first before running checks" : "Execute all active quality rules"}
            >
              {executing ? (
                <>
                  <RefreshCw className="w-4 h-4 animate-spin" />
                  Running...
                </>
              ) : (
                <>
                  <Play className="w-4 h-4" />
                  Run Quality Check
                  {rules.length === 0 && (
                    <span className="ml-1 text-xs opacity-75">(No rules)</span>
                  )}
                </>
              )}
            </button>
          </div>
        </div>

        {/* Dataset Info (Read-only) */}
        <div className="mt-4 flex items-center gap-4">
          <div className="flex items-center gap-2 px-4 py-2 bg-blue-50 border border-blue-200 rounded-lg">
            <span className="text-sm font-medium text-blue-900">Table:</span>
            <span className="text-sm font-bold text-blue-700">{selectedDataset}</span>
          </div>
          {!rulesLoading && rules.length > 0 && (
            <div className="text-sm text-gray-600">
              ✓ {rules.length} rules loaded ({rules.filter(r => r.is_active).length} active)
            </div>
          )}
          {!rulesLoading && rules.length === 0 && (
            <div className="flex items-center gap-2 px-4 py-2 bg-yellow-50 border border-yellow-300 rounded-lg">
              <AlertTriangle className="w-4 h-4 text-yellow-600" />
              <span className="text-sm font-medium text-yellow-800">
                No rules defined - Create rules to enable quality checks
              </span>
            </div>
          )}
          {rulesLoading && (
            <div className="flex items-center gap-2 text-sm text-gray-500">
              <RefreshCw className="w-4 h-4 animate-spin" />
              Loading rules...
            </div>
          )}
        </div>
      </div>

      {/* Alert Banner when no rules exist */}
      {!rulesLoading && rules.length === 0 && activeTab === 'rules' && (
        <div className="mx-6 mt-6 bg-gradient-to-r from-blue-50 to-indigo-50 border-2 border-blue-200 rounded-xl p-6">
          <div className="flex items-start gap-4">
            <div className="flex-shrink-0">
              <div className="w-12 h-12 bg-blue-100 rounded-full flex items-center justify-center">
                <Shield className="w-6 h-6 text-blue-600" />
              </div>
            </div>
            <div className="flex-1">
              <h3 className="text-lg font-semibold text-gray-900 mb-2">
                🎯 Ready to ensure data quality?
              </h3>
              <p className="text-sm text-gray-700 mb-4">
                Quality rules help you automatically validate data, catch errors early, and maintain high data standards. 
                Start by creating your first rule for <strong>{selectedDataset}</strong>.
              </p>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-4">
                <div className="bg-white rounded-lg p-3 border border-blue-200">
                  <div className="flex items-center gap-2 mb-1">
                    <CheckCircle className="w-4 h-4 text-green-600" />
                    <span className="font-semibold text-sm text-gray-900">Validate</span>
                  </div>
                  <p className="text-xs text-gray-600">Check nulls, ranges, formats, patterns</p>
                </div>
                <div className="bg-white rounded-lg p-3 border border-blue-200">
                  <div className="flex items-center gap-2 mb-1">
                    <AlertTriangle className="w-4 h-4 text-yellow-600" />
                    <span className="font-semibold text-sm text-gray-900">Catch Errors</span>
                  </div>
                  <p className="text-xs text-gray-600">Flag bad data before it causes issues</p>
                </div>
                <div className="bg-white rounded-lg p-3 border border-blue-200">
                  <div className="flex items-center gap-2 mb-1">
                    <TrendingUp className="w-4 h-4 text-blue-600" />
                    <span className="font-semibold text-sm text-gray-900">Monitor</span>
                  </div>
                  <p className="text-xs text-gray-600">Track quality trends over time</p>
                </div>
              </div>
              <button
                onClick={() => setShowRuleBuilder(true)}
                className="px-5 py-2.5 bg-gradient-to-r from-blue-600 to-indigo-600 text-white rounded-lg hover:from-blue-700 hover:to-indigo-700 font-medium flex items-center gap-2 shadow-lg"
              >
                <Plus className="w-5 h-5" />
                Create First Quality Rule
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Tabs */}
      <div className="bg-white border-b border-gray-200">
        <div className="flex gap-1 px-6">
          {[
            { id: 'rules', label: 'Rules', count: rules.length },
            { id: 'results', label: 'Execution Results', count: executionHistory.length },
            { id: 'quarantine', label: 'Quarantine', count: quarantineRecords.length },
            { id: 'monitoring', label: 'Quality Monitoring', count: null }
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
              {tab.count !== null && (
                <span className="ml-2 px-2 py-0.5 bg-gray-100 text-gray-600 rounded-full text-xs">
                  {tab.count}
                </span>
              )}
            </button>
          ))}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto p-6 pb-24">
        {activeTab === 'rules' && (
          <div>
            {/* Action Bar */}
            <div className="mb-6 flex justify-between items-center">
              <button
                onClick={handleOpenRuleBuilder}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
              >
                <Plus className="w-5 h-5" />
                Create Rule
              </button>
            </div>

            {/* Search and Filter */}
            <div className="mb-6 flex gap-4">
              <div className="flex-1 relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400" />
                <input
                  type="text"
                  placeholder="Search rules..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <select
                value={filterCategory}
                onChange={(e) => setFilterCategory(e.target.value)}
                className="px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <option value="all">All Categories</option>
                {ruleCategories.map(cat => (
                  <option key={cat.id} value={cat.id}>{cat.name}</option>
                ))}
              </select>
            </div>

            {/* Rule Categories Overview */}
            <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4 mb-6">
              {ruleCategories.map(category => {
                const Icon = category.icon;
                return (
                  <div
                    key={category.id}
                    className={`p-4 bg-white rounded-lg border-2 transition-all cursor-pointer ${
                      filterCategory === category.id
                        ? 'border-blue-500 ring-2 ring-blue-200'
                        : 'border-gray-200 hover:border-gray-300'
                    }`}
                    onClick={() => setFilterCategory(category.id)}
                  >
                    <div className="flex items-center justify-between mb-2">
                      <Icon className="w-5 h-5" style={{ color: category.color }} />
                      <span className="text-lg font-bold text-gray-900">{category.count}</span>
                    </div>
                    <div className="text-sm font-medium text-gray-700">{category.name}</div>
                  </div>
                );
              })}
            </div>

            {/* Rules Table */}
            <div className="bg-white rounded-lg shadow border border-gray-200">
              <table className="w-full text-sm">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Rule Name</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Category</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Column</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Condition</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Severity</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Action</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Pass Rate</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {rulesLoading ? (
                    <tr>
                      <td colSpan="8" className="px-6 py-12 text-center text-gray-500">
                        <RefreshCw className="w-8 h-8 mx-auto mb-3 text-gray-400 animate-spin" />
                        <p>Loading rules...</p>
                      </td>
                    </tr>
                  ) : filteredRules.length === 0 ? (
                    <tr>
                      <td colSpan="8" className="px-6 py-16">
                        <div className="text-center">
                          <Shield className="w-16 h-16 mx-auto mb-4 text-gray-300" />
                          <h3 className="text-lg font-semibold text-gray-900 mb-2">
                            {rules.length === 0 
                              ? "No quality rules defined yet" 
                              : "No rules match your search"}
                          </h3>
                          {rules.length === 0 ? (
                            <div className="max-w-md mx-auto">
                              <p className="text-sm text-gray-600 mb-4">
                                Create quality rules to validate your data and ensure it meets your standards
                              </p>
                              <div className="bg-blue-50 border-2 border-blue-200 rounded-lg p-4 mb-4 text-left">
                                <p className="text-sm font-semibold text-blue-900 mb-2">Quick Start Guide:</p>
                                <ol className="text-sm text-blue-800 space-y-1 list-decimal list-inside">
                                  <li>Click <strong>"+ Create Rule"</strong> button above</li>
                                  <li>Choose a rule type (e.g., not null, range, regex)</li>
                                  <li>Select target column(s) from your table</li>
                                  <li>Define validation condition</li>
                                  <li>Set severity and action for violations</li>
                                  <li>Click <strong>"Run Quality Check"</strong> to validate data</li>
                                </ol>
                              </div>
                              <button
                                onClick={() => setShowRuleBuilder(true)}
                                className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-medium flex items-center gap-2 mx-auto shadow-md"
                              >
                                <Plus className="w-5 h-5" />
                                Create Your First Rule
                              </button>
                            </div>
                          ) : (
                            <p className="text-sm text-gray-600 mt-1">
                              Try adjusting your search or filter criteria
                            </p>
                          )}
                        </div>
                      </td>
                    </tr>
                  ) : (
                    filteredRules.map(rule => (
                      <tr key={rule.rule_id} className="hover:bg-gray-50">
                        <td className="px-6 py-4 whitespace-nowrap font-medium text-gray-900">
                          {rule.rule_name}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-gray-600 capitalize">
                          {rule.category}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className="px-2 py-1 bg-blue-50 text-blue-700 rounded text-xs font-mono">
                            {Array.isArray(rule.target_columns) 
                              ? rule.target_columns.join(', ') 
                              : rule.target_columns || 'N/A'}
                          </span>
                        </td>
                        <td className="px-6 py-4 text-gray-600 font-mono text-xs max-w-xs truncate">
                          {rule.condition_expression}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className={`px-2 py-1 rounded text-xs font-medium ${getSeverityColor(rule.severity)}`}>
                            {rule.severity}
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className={`px-2 py-1 rounded text-xs font-medium border ${getActionColor(rule.action)}`}>
                            {rule.action}
                          </span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <span className="text-gray-500 text-xs">-</span>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <label className="relative inline-flex items-center cursor-pointer">
                            <input
                              type="checkbox"
                              checked={rule.is_active === true}
                              className="sr-only peer"
                              onChange={() => handleToggleRuleStatus(rule.rule_id, rule.is_active)}
                            />
                            <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
                          </label>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {activeTab === 'results' && (
          <div className="space-y-6">
            {/* Summary Statistics */}
            {executionHistory.length > 0 && (
              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div className="bg-white p-5 rounded-lg shadow border border-gray-200">
                  <div className="flex items-center justify-between mb-2">
                    <p className="text-sm text-gray-600">Total Executions</p>
                    <Calendar className="w-5 h-5 text-blue-500" />
                  </div>
                  <p className="text-2xl font-bold text-gray-900">{executionHistory.length}</p>
                </div>
                <div className="bg-gradient-to-br from-green-50 to-emerald-50 p-5 rounded-lg shadow border border-green-200">
                  <div className="flex items-center justify-between mb-2">
                    <p className="text-sm text-green-700">Avg Quality Score</p>
                    <TrendingUp className="w-5 h-5 text-green-600" />
                  </div>
                  <p className="text-2xl font-bold text-green-700">
                    {executionHistory.length > 0
                      ? (executionHistory.reduce((sum, r) => 
                          sum + (r.total_rows_processed > 0 ? (r.rows_passed / r.total_rows_processed) * 100 : 100), 0
                        ) / executionHistory.length).toFixed(1)
                      : 0}%
                  </p>
                </div>
                <div className="bg-gradient-to-br from-blue-50 to-indigo-50 p-5 rounded-lg shadow border border-blue-200">
                  <div className="flex items-center justify-between mb-2">
                    <p className="text-sm text-blue-700">Total Rows Checked</p>
                    <Shield className="w-5 h-5 text-blue-600" />
                  </div>
                  <p className="text-2xl font-bold text-blue-700">
                    {executionHistory.reduce((sum, r) => sum + (r.rows_processed || 0), 0).toLocaleString()}
                  </p>
                </div>
                <div className="bg-gradient-to-br from-yellow-50 to-orange-50 p-5 rounded-lg shadow border border-yellow-200">
                  <div className="flex items-center justify-between mb-2">
                    <p className="text-sm text-yellow-700">Avg Execution Time</p>
                    <Clock className="w-5 h-5 text-yellow-600" />
                  </div>
                  <p className="text-2xl font-bold text-yellow-700">
                    {executionHistory.length > 0
                      ? Math.round(executionHistory.reduce((sum, r) => sum + (r.processing_time_ms || 0), 0) / executionHistory.length)
                      : 0}ms
                  </p>
                </div>
              </div>
            )}

            {historyLoading ? (
              <div className="text-center py-12 text-gray-500">
                <RefreshCw className="w-8 h-8 mx-auto mb-3 text-gray-400 animate-spin" />
                <p>Loading execution history...</p>
              </div>
            ) : executionHistory.length === 0 ? (
              <div className="text-center py-12 text-gray-500 bg-white rounded-lg shadow border border-gray-200">
                <CheckCircle className="w-12 h-12 mx-auto mb-3 text-gray-400" />
                <p className="font-medium">No execution history yet</p>
                <p className="text-sm mt-1">Run a quality check to see results here</p>
              </div>
            ) : (
              <div className="space-y-4">
                {executionHistory.map(result => (
                  <div key={result.execution_id} className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden hover:shadow-md transition-shadow">
                    {/* Main Execution Card */}
                    <div className="p-6">
                      <div className="flex items-center justify-between mb-4">
                        <div className="flex-1">
                          <div className="flex items-center gap-3">
                            <h3 className="text-lg font-semibold text-gray-900">{result.table_name}</h3>
                            <span className={`px-3 py-1 rounded-full text-xs font-medium ${
                              result.execution_status === 'completed' 
                                ? 'bg-green-100 text-green-800 border border-green-200' 
                                : 'bg-red-100 text-red-800 border border-red-200'
                            }`}>
                              {result.execution_status}
                            </span>
                          </div>
                          <p className="text-sm text-gray-500 mt-1 flex items-center gap-2">
                            <Calendar className="w-4 h-4" />
                            {result.execution_timestamp ? new Date(result.execution_timestamp).toLocaleString() : 'N/A'}
                            <span className="text-gray-300">•</span>
                            <Clock className="w-4 h-4" />
                            {result.processing_time_ms ? `${result.processing_time_ms}ms` : 'N/A'}
                          </p>
                        </div>
                        <div className="flex items-center gap-4">
                          <div className="text-right">
                            <p className="text-sm text-gray-500 mb-1">Quality Score</p>
                            <div className={`text-3xl font-bold ${getScoreColor(
                              result.total_rows_processed > 0 
                                ? ((result.rows_passed / result.total_rows_processed) * 100).toFixed(2)
                                : 100
                            )}`}>
                              {result.total_rows_processed > 0 
                                ? ((result.rows_passed / result.total_rows_processed) * 100).toFixed(1)
                                : 100}%
                            </div>
                          </div>
                        </div>
                      </div>

                      {/* Metrics Grid */}
                      <div className="grid grid-cols-4 gap-3 mb-4">
                        <div className="bg-gray-50 p-3 rounded-lg border border-gray-200">
                          <p className="text-xs text-gray-600 mb-1">Total Rows</p>
                          <p className="text-lg font-bold text-gray-900">{result.total_rows_processed?.toLocaleString() || 0}</p>
                        </div>
                        <div className="bg-green-50 p-3 rounded-lg border border-green-200">
                          <p className="text-xs text-green-600 mb-1">✓ Passed</p>
                          <p className="text-lg font-bold text-green-700">{result.rows_passed?.toLocaleString() || 0}</p>
                        </div>
                        <div className="bg-red-50 p-3 rounded-lg border border-red-200">
                          <p className="text-xs text-red-600 mb-1">✗ Failed</p>
                          <p className="text-lg font-bold text-red-700">{result.rows_failed?.toLocaleString() || 0}</p>
                        </div>
                        <div className="bg-yellow-50 p-3 rounded-lg border border-yellow-200">
                          <p className="text-xs text-yellow-600 mb-1">⚠ Quarantined</p>
                          <p className="text-lg font-bold text-yellow-700">{result.rows_failed?.toLocaleString() || 0}</p>
                        </div>
                      </div>

                      {/* Action Buttons */}
                      <div className="flex items-center justify-between pt-4 border-t border-gray-100">
                        <div className="flex gap-2">
                          {result.rows_failed > 0 && (
                            <button
                              onClick={() => {
                                setActiveTab('quarantine');
                                setQuarantineFilter('pending');
                              }}
                              className="flex items-center gap-2 px-3 py-2 text-sm bg-yellow-50 text-yellow-700 rounded-lg hover:bg-yellow-100 border border-yellow-200 transition-colors"
                            >
                              <AlertTriangle className="w-4 h-4" />
                              View Quarantine
                            </button>
                          )}
                          <button
                            onClick={() => setExpandedExecution(expandedExecution === result.execution_id ? null : result.execution_id)}
                            className="flex items-center gap-2 px-3 py-2 text-sm bg-blue-50 text-blue-700 rounded-lg hover:bg-blue-100 border border-blue-200 transition-colors"
                          >
                            {expandedExecution === result.execution_id ? (
                              <>
                                <ChevronUp className="w-4 h-4" />
                                Hide Details
                              </>
                            ) : (
                              <>
                                <ChevronDown className="w-4 h-4" />
                                View Details
                              </>
                            )}
                          </button>
                        </div>
                        <div className="flex gap-2">
                          <button className="flex items-center gap-2 px-3 py-2 text-sm bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors">
                            <Download className="w-4 h-4" />
                            Export
                          </button>
                        </div>
                      </div>
                    </div>

                    {/* Expanded Details */}
                    {expandedExecution === result.execution_id && (
                      <div className="bg-gray-50 p-6 border-t border-gray-200">
                        <h4 className="text-sm font-semibold text-gray-900 mb-3">Execution Details</h4>
                        <div className="space-y-2 text-sm">
                          <div className="flex justify-between py-2 border-b border-gray-200">
                            <span className="text-gray-600">Execution ID:</span>
                            <span className="font-mono text-gray-900">{result.execution_id}</span>
                          </div>
                          <div className="flex justify-between py-2 border-b border-gray-200">
                            <span className="text-gray-600">Domain:</span>
                            <span className="font-medium text-gray-900">{result.domain || 'finance'}</span>
                          </div>
                          <div className="flex justify-between py-2 border-b border-gray-200">
                            <span className="text-gray-600">Rule ID:</span>
                            <span className="font-mono text-gray-900 text-xs">{result.rule_id}</span>
                          </div>
                          {result.error_message && (
                            <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded">
                              <p className="text-xs font-medium text-red-800 mb-1">Error Message:</p>
                              <p className="text-xs text-red-700">{result.error_message}</p>
                            </div>
                          )}
                          {result.execution_metadata && (
                            <div className="mt-4">
                              <p className="text-xs font-medium text-gray-700 mb-2">Metadata:</p>
                              <pre className="text-xs bg-white p-3 rounded border border-gray-200 overflow-x-auto">
                                {typeof result.execution_metadata === 'string' 
                                  ? result.execution_metadata 
                                  : JSON.stringify(result.execution_metadata, null, 2)}
                              </pre>
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}

        {activeTab === 'quarantine' && (
          <div>
            {/* Quarantine Summary */}
            <div className="mb-6 grid grid-cols-1 md:grid-cols-4 gap-4">
              <div className="bg-white p-4 rounded-lg shadow border border-gray-200">
                <p className="text-sm text-gray-600 mb-1">Total Quarantined</p>
                <p className="text-2xl font-bold text-gray-900">{quarantineRecords.length}</p>
              </div>
              <div className="bg-yellow-50 p-4 rounded-lg shadow border border-yellow-200">
                <p className="text-sm text-yellow-700 mb-1">Pending Review</p>
                <p className="text-2xl font-bold text-yellow-800">
                  {quarantineRecords.filter(r => r.status === 'pending').length}
                </p>
              </div>
              <div className="bg-green-50 p-4 rounded-lg shadow border border-green-200">
                <p className="text-sm text-green-700 mb-1">Resolved</p>
                <p className="text-2xl font-bold text-green-800">
                  {quarantineRecords.filter(r => r.status === 'resolved').length}
                </p>
              </div>
              <div className="bg-red-50 p-4 rounded-lg shadow border border-red-200">
                <p className="text-sm text-red-700 mb-1">Critical Severity</p>
                <p className="text-2xl font-bold text-red-800">
                  {quarantineRecords.filter(r => r.severity === 'critical').length}
                </p>
              </div>
            </div>

            {/* Filter Bar */}
            <div className="mb-4 flex gap-4">
              <div className="flex gap-2">
                <button
                  onClick={() => setQuarantineFilter('all')}
                  className={`px-4 py-2 rounded-lg text-sm font-medium ${
                    quarantineFilter === 'all'
                      ? 'bg-blue-600 text-white'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  All Records
                </button>
                <button
                  onClick={() => setQuarantineFilter('pending')}
                  className={`px-4 py-2 rounded-lg text-sm font-medium ${
                    quarantineFilter === 'pending'
                      ? 'bg-yellow-600 text-white'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  Pending
                </button>
                <button
                  onClick={() => setQuarantineFilter('resolved')}
                  className={`px-4 py-2 rounded-lg text-sm font-medium ${
                    quarantineFilter === 'resolved'
                      ? 'bg-green-600 text-white'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  Resolved
                </button>
              </div>
            </div>

            {/* Quarantine Records Table */}
            <div className="bg-white rounded-lg shadow border border-gray-200">
              {quarantineLoading ? (
                <div className="text-center py-12 text-gray-500">
                  <RefreshCw className="w-8 h-8 mx-auto mb-3 text-gray-400 animate-spin" />
                  <p>Loading quarantine records...</p>
                </div>
              ) : quarantineRecords.length === 0 ? (
                <div className="text-center py-12 text-gray-500">
                  <CheckCircle className="w-12 h-12 mx-auto mb-3 text-gray-400" />
                  <p className="font-semibold">No records in quarantine</p>
                  <p className="text-sm mt-1">All data passed quality checks! 🎉</p>
                </div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Timestamp</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Rule</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Category</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Failure Reason</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Severity</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {quarantineRecords.map(record => (
                        <tr key={record.quarantine_id} className="hover:bg-gray-50">
                          <td className="px-6 py-4 whitespace-nowrap text-gray-900">
                            {record.quarantined_at 
                              ? new Date(record.quarantined_at).toLocaleString()
                              : 'N/A'}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap font-medium text-gray-900">
                            {record.rule_name || 'Unknown Rule'}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-gray-600 capitalize">
                            {record.category || 'N/A'}
                          </td>
                          <td className="px-6 py-4 text-gray-600 max-w-xs truncate">
                            {record.failure_reason || 'N/A'}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <span className={`px-2 py-1 rounded text-xs font-medium ${
                              record.severity === 'critical' 
                                ? 'bg-red-100 text-red-800'
                                : record.severity === 'high'
                                ? 'bg-orange-100 text-orange-800'
                                : 'bg-yellow-100 text-yellow-800'
                            }`}>
                              {record.severity?.toUpperCase() || 'N/A'}
                            </span>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <span className={`px-2 py-1 rounded text-xs font-medium ${
                              record.status === 'resolved'
                                ? 'bg-green-100 text-green-800'
                                : 'bg-yellow-100 text-yellow-800'
                            }`}>
                              {record.status || 'pending'}
                            </span>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap">
                            <div className="flex gap-2">
                              <button
                                onClick={() => setSelectedRecord(record)}
                                className="text-blue-600 hover:text-blue-800"
                                title="View Details"
                              >
                                <Eye className="w-4 h-4" />
                              </button>
                              {record.status === 'pending' && (
                                <button
                                  onClick={() => handleResolveQuarantine(record.quarantine_id)}
                                  className="text-green-600 hover:text-green-800"
                                  title="Mark as Resolved"
                                >
                                  <CheckCircle className="w-4 h-4" />
                                </button>
                              )}
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            {/* Detail Modal */}
            {selectedRecord && (
              <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50" onClick={() => setSelectedRecord(null)}>
                <div className="bg-white rounded-lg shadow-xl max-w-2xl w-full m-4 max-h-[80vh] overflow-y-auto" onClick={(e) => e.stopPropagation()}>
                  <div className="p-6 border-b border-gray-200">
                    <div className="flex items-center justify-between">
                      <h2 className="text-xl font-bold text-gray-900">Quarantine Record Details</h2>
                      <button onClick={() => setSelectedRecord(null)} className="text-gray-400 hover:text-gray-600">
                        <XCircle className="w-6 h-6" />
                      </button>
                    </div>
                  </div>
                  
                  <div className="p-6 space-y-4">
                    <div>
                      <h3 className="text-sm font-semibold text-gray-700 mb-2">Rule Information</h3>
                      <div className="bg-gray-50 p-4 rounded-lg space-y-2">
                        <p><span className="font-medium">Rule:</span> {selectedRecord.rule_name}</p>
                        <p><span className="font-medium">Category:</span> {selectedRecord.category}</p>
                        <p><span className="font-medium">Failure Reason:</span> {selectedRecord.failure_reason}</p>
                        <p><span className="font-medium">Severity:</span> 
                          <span className={`ml-2 px-2 py-1 rounded text-xs font-medium ${
                            selectedRecord.severity === 'critical' ? 'bg-red-100 text-red-800' : 'bg-orange-100 text-orange-800'
                          }`}>
                            {selectedRecord.severity?.toUpperCase()}
                          </span>
                        </p>
                      </div>
                    </div>
                    
                    <div>
                      <h3 className="text-sm font-semibold text-gray-700 mb-2">Failed Record Data</h3>
                      <div className="bg-gray-50 p-4 rounded-lg">
                        <pre className="text-xs overflow-x-auto">
                          {selectedRecord.failed_row_data 
                            ? JSON.stringify(JSON.parse(selectedRecord.failed_row_data), null, 2)
                            : 'No data available'}
                        </pre>
                      </div>
                    </div>
                    
                    <div>
                      <h3 className="text-sm font-semibold text-gray-700 mb-2">Metadata</h3>
                      <div className="bg-gray-50 p-4 rounded-lg space-y-2 text-sm">
                        <p><span className="font-medium">Quarantined At:</span> {new Date(selectedRecord.quarantined_at).toLocaleString()}</p>
                        <p><span className="font-medium">Status:</span> {selectedRecord.status}</p>
                        {selectedRecord.quarantine_metadata && (
                          <p className="text-xs text-gray-600 mt-2">
                            <span className="font-medium">S3 Location:</span><br/>
                            {JSON.parse(selectedRecord.quarantine_metadata)?.s3_path || 'N/A'}
                          </p>
                        )}
                      </div>
                    </div>
                  </div>
                  
                  <div className="p-6 border-t border-gray-200 flex gap-3">
                    {selectedRecord.status === 'pending' && (
                      <button
                        onClick={() => {
                          handleResolveQuarantine(selectedRecord.quarantine_id);
                          setSelectedRecord(null);
                        }}
                        className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center gap-2"
                      >
                        <CheckCircle className="w-4 h-4" />
                        Mark as Resolved
                      </button>
                    )}
                    <button
                      onClick={() => setSelectedRecord(null)}
                      className="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300"
                    >
                      Close
                    </button>
                  </div>
                </div>
              </div>
            )}
          </div>
        )}

        {activeTab === 'monitoring' && (
          <div>
            {monitoringLoading ? (
              <div className="text-center py-12 text-gray-500">
                <RefreshCw className="w-8 h-8 mx-auto mb-3 text-gray-400 animate-spin" />
                <p>Loading monitoring data...</p>
              </div>
            ) : (
              <>
                {/* Quality Score Trend */}
                <div className="bg-white rounded-lg shadow border border-gray-200 p-6 mb-6">
                  <h3 className="text-lg font-semibold text-gray-900 mb-4">Data Quality Score Trend</h3>
                  {qualityTrends.length > 0 ? (
                    <div className="flex items-end gap-4 h-48">
                      {qualityTrends.map((trend, idx) => (
                        <div key={idx} className="flex-1 flex flex-col items-center">
                          <div
                            className="w-full bg-blue-500 rounded-t hover:bg-blue-600 transition-colors cursor-pointer"
                            style={{ height: `${trend.score}%` }}
                            title={`${trend.date}: ${trend.score}%`}
                          />
                          <div className="mt-2 text-xs font-medium text-gray-600">{trend.date}</div>
                          <div className="text-xs font-bold text-blue-600">{trend.score}%</div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div className="text-center py-8 text-gray-500">
                      <TrendingUp className="w-12 h-12 mx-auto mb-3 text-gray-400" />
                      <p>No quality trend data yet</p>
                      <p className="text-sm mt-1">Run quality checks to see trends</p>
                    </div>
                  )}
                </div>

                {/* Quality Dimensions */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  {ruleCategories.map(category => {
                    const Icon = category.icon;
                    const categoryName = category.name;
                    const metrics = categoryMetrics[categoryName] || { passed: 0, total: 0, count: 0 };
                    const passRate = metrics.total > 0 ? ((metrics.passed / metrics.total) * 100).toFixed(1) : 0;
                    
                    return (
                      <div key={category.id} className="bg-white rounded-lg shadow border border-gray-200 p-6">
                        <div className="flex items-center gap-3 mb-4">
                          <div
                            className="w-10 h-10 rounded-lg flex items-center justify-center"
                            style={{ backgroundColor: `${category.color}20` }}
                          >
                            <Icon className="w-5 h-5" style={{ color: category.color }} />
                          </div>
                          <div>
                            <h3 className="font-semibold text-gray-900">{categoryName}</h3>
                            <p className="text-sm text-gray-500">{category.count} active rules</p>
                          </div>
                        </div>
                        <div className="space-y-2">
                          <div className="flex items-center justify-between text-sm">
                            <span className="text-gray-600">Pass Rate</span>
                            <span className="font-bold text-green-600">{passRate}%</span>
                          </div>
                          <div className="w-full bg-gray-200 rounded-full h-2">
                            <div
                              className="h-2 rounded-full transition-all"
                              style={{ backgroundColor: category.color, width: `${passRate}%` }}
                            />
                          </div>
                          {metrics.total > 0 && (
                            <p className="text-xs text-gray-500 mt-2">
                              {metrics.passed.toLocaleString()} / {metrics.total.toLocaleString()} rows passed
                            </p>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </>
            )}
          </div>
        )}
      </div>

      {/* Rule Builder Modal */}
      {showRuleBuilder && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50" onClick={() => setShowRuleBuilder(false)}>
          <div className="bg-white rounded-lg shadow-xl max-w-3xl w-full m-4 max-h-[90vh] overflow-y-auto" onClick={(e) => e.stopPropagation()}>
            <div className="p-6 border-b border-gray-200">
              <div className="flex items-center justify-between">
                <h2 className="text-xl font-bold text-gray-900">Create Quality Rule</h2>
                <button onClick={() => setShowRuleBuilder(false)} className="text-gray-400 hover:text-gray-600">
                  <XCircle className="w-6 h-6" />
                </button>
              </div>
              <p className="text-sm text-gray-500 mt-1">Define a custom data quality rule for {selectedDataset}</p>
            </div>
            
            <div className="p-6 space-y-6">
              {/* Rule Name */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Rule Name *</label>
                <input
                  type="text"
                  value={newRule.rule_name}
                  onChange={(e) => setNewRule({ ...newRule, rule_name: e.target.value })}
                  placeholder="e.g., Customer ID Not Null Check"
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              {/* Category and Rule Type */}
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">Category *</label>
                  <select
                    value={newRule.category}
                    onChange={(e) => setNewRule({ ...newRule, category: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="data_quality">Data Quality</option>
                    <option value="validation">Validation</option>
                    <option value="transformation">Transformation</option>
                    <option value="compliance">Compliance</option>
                    <option value="schema_validation">Schema Validation</option>
                    <option value="referential_integrity">Referential Integrity</option>
                    <option value="anomaly_detection">Anomaly Detection</option>
                    <option value="masking">Data Masking</option>
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">Rule Type *</label>
                  <select
                    value={newRule.rule_type}
                    onChange={(e) => setNewRule({ ...newRule, rule_type: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="not_null">Not Null Check</option>
                    <option value="range_check">Range Validation</option>
                    <option value="regex_format">Format/Regex Validation</option>
                    <option value="enum_validation">Enum Validation</option>
                    <option value="unique">Uniqueness Check</option>
                    <option value="foreign_key">Foreign Key Check</option>
                    <option value="cross_column_logic">Cross-Column Logic</option>
                    <option value="cross_table_validation">Cross-Table Validation</option>
                    <option value="sql_expression">SQL Expression</option>
                    <option value="data_type_check">Data Type Check</option>
                    <option value="anomaly_detection">Anomaly Detection</option>
                    <option value="masking_rule">Masking Rule</option>
                  </select>
                </div>
              </div>

              {/* Description */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Description</label>
                <textarea
                  value={newRule.description}
                  onChange={(e) => setNewRule({ ...newRule, description: e.target.value })}
                  placeholder="Optional: Describe what this rule checks"
                  rows={2}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>

              {/* Target Columns */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">Target Columns</label>
                <div className="border border-gray-300 rounded-lg p-3 max-h-40 overflow-y-auto">
                  {tableSchema.length > 0 ? (
                    <div className="space-y-2">
                      {tableSchema.map((col, idx) => (
                        <label key={idx} className="flex items-center gap-2 cursor-pointer hover:bg-gray-50 p-2 rounded">
                          <input
                            type="checkbox"
                            checked={newRule.target_columns.includes(col.column_name || col.name)}
                            onChange={(e) => {
                              const colName = col.column_name || col.name;
                              if (e.target.checked) {
                                setNewRule({ ...newRule, target_columns: [...newRule.target_columns, colName] });
                              } else {
                                setNewRule({ ...newRule, target_columns: newRule.target_columns.filter(c => c !== colName) });
                              }
                            }}
                            className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                          />
                          <span className="text-sm font-mono">{col.column_name || col.name}</span>
                          <span className="text-xs text-gray-500">({col.column_type || col.type})</span>
                        </label>
                      ))}
                    </div>
                  ) : (
                    <p className="text-sm text-gray-500 text-center py-4">Loading schema...</p>
                  )}
                </div>
                <p className="text-xs text-gray-500 mt-1">
                  Selected: {newRule.target_columns.length > 0 ? newRule.target_columns.join(', ') : 'None'}
                </p>
              </div>

              {/* Condition Expression */}
              <div>
                <div className="flex items-center justify-between mb-2">
                  <label className="block text-sm font-medium text-gray-700">Condition Expression (SQL WHERE clause) *</label>
                  {newRule.target_columns.length > 0 && (
                    <button
                      type="button"
                      onClick={() => {
                        const col = newRule.target_columns[0];
                        let template = '';
                        
                        switch(newRule.rule_type) {
                          case 'not_null':
                            template = `${col} IS NOT NULL AND ${col} != 'null'`;
                            break;
                          case 'range_check':
                            template = `${col} >= 0 AND ${col} <= 1000`;
                            break;
                          case 'unique':
                            template = `${col} IS NOT NULL AND ${col} != 'null'`;
                            break;
                          default:
                            template = `${col} IS NOT NULL AND ${col} != 'null'`;
                        }
                        
                        setNewRule({ ...newRule, condition_expression: template });
                      }}
                      className="px-3 py-1 text-xs bg-blue-100 text-blue-700 rounded hover:bg-blue-200 font-medium"
                    >
                      Generate Template
                    </button>
                  )}
                </div>
                <textarea
                  value={newRule.condition_expression}
                  onChange={(e) => setNewRule({ ...newRule, condition_expression: e.target.value })}
                  placeholder="e.g., PRODUCT_NAME IS NOT NULL AND PRODUCT_NAME != 'null'"
                  rows={3}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm"
                />
                <div className="mt-2 bg-blue-50 border border-blue-200 rounded-lg p-3">
                  <p className="text-xs font-semibold text-blue-900 mb-2">⚠️ IMPORTANT: Write PASSING conditions (what makes data VALID):</p>
                  <div className="space-y-1 text-xs text-blue-800">
                    <div className="flex items-start gap-2">
                      <span className="text-green-600 font-bold">✓</span>
                      <div>
                        <span className="font-semibold">No Nulls:</span> <code className="bg-white px-1 py-0.5 rounded">PRODUCT_NAME IS NOT NULL AND PRODUCT_NAME != 'null'</code>
                        <div className="text-xs text-orange-600 mt-1">⚠️ If data shows "null" as TEXT, add: <code className="bg-white px-1 py-0.5">!= 'null'</code></div>
                      </div>
                    </div>
                    <div className="flex items-start gap-2">
                      <span className="text-green-600 font-bold">✓</span>
                      <div>
                        <span className="font-semibold">Range Check:</span> <code className="bg-white px-1 py-0.5 rounded">AMOUNT &gt;= 0 AND AMOUNT &lt; 10000</code>
                      </div>
                    </div>
                    <div className="flex items-start gap-2">
                      <span className="text-green-600 font-bold">✓</span>
                      <div>
                        <span className="font-semibold">Required Field:</span> <code className="bg-white px-1 py-0.5 rounded">USER_ID IS NOT NULL AND USER_ID &gt; 0</code>
                      </div>
                    </div>
                    <div className="flex items-start gap-2">
                      <span className="text-red-600 font-bold">✗</span>
                      <div>
                        <span className="font-semibold">Wrong (finds errors):</span> <code className="bg-white px-1 py-0.5 rounded line-through">PRODUCT_NAME IS NULL</code>
                      </div>
                    </div>
                  </div>
                  <p className="text-xs text-blue-700 mt-2 italic">
                    The system will automatically find rows that DON'T match your condition and quarantine them.
                  </p>
                </div>
              </div>

              {/* Severity and Action */}
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">Severity *</label>
                  <select
                    value={newRule.severity}
                    onChange={(e) => setNewRule({ ...newRule, severity: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="CRITICAL">Critical</option>
                    <option value="HIGH">High</option>
                    <option value="WARNING">Warning</option>
                    <option value="INFO">Info</option>
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-2">Action *</label>
                  <select
                    value={newRule.action}
                    onChange={(e) => setNewRule({ ...newRule, action: e.target.value })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="quarantine_row">Quarantine Row</option>
                    <option value="log">Log Only</option>
                    <option value="block_table">Block Table</option>
                  </select>
                </div>
              </div>

              {/* Execution Priority */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Execution Priority (1-10, lower = higher priority)
                </label>
                <input
                  type="number"
                  min="1"
                  max="10"
                  value={newRule.execution_priority}
                  onChange={(e) => setNewRule({ ...newRule, execution_priority: parseInt(e.target.value) || 5 })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
            </div>
            
            <div className="p-6 border-t border-gray-200 flex gap-3">
              <button
                onClick={handleCreateRule}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2"
              >
                <CheckCircle className="w-4 h-4" />
                Create Rule
              </button>
              <button
                onClick={() => setShowRuleBuilder(false)}
                className="px-4 py-2 bg-gray-200 text-gray-700 rounded-lg hover:bg-gray-300"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
