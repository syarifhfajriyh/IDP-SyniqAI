import React, { useState, useEffect } from 'react';
import { Activity, Database, AlertCircle, RefreshCw, CheckCircle, Edit2, Trash2, List } from 'lucide-react';

const API_BASE = 'http://localhost:8000/api';

const KafkaBacklog = () => {
  const [messages, setMessages] = useState([]);
  const [topics, setTopics] = useState([]);
  const [selectedTopic, setSelectedTopic] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [refreshing, setRefreshing] = useState(false);

  // Fetch available CDC topics
  const fetchTopics = async () => {
    try {
      const response = await fetch(`${API_BASE}/kafka/cdc/topics`);
      const data = await response.json();
      
      if (data.success && data.topics.length > 0) {
        setTopics(data.topics);
        // Auto-select first topic if none selected
        if (!selectedTopic && data.topics.length > 0) {
          setSelectedTopic(data.topics[0]);
        }
      }
    } catch (err) {
      console.error('Error fetching topics:', err);
    }
  };

  const fetchMessages = async () => {
    if (!selectedTopic) return;
    
    try {
      setRefreshing(true);
      const response = await fetch(`${API_BASE}/kafka/cdc/messages?topic=${encodeURIComponent(selectedTopic)}&limit=20&offset=earliest`);
      const data = await response.json();
      
      if (data.success) {
        setMessages(data.messages);
        setError(null);
      } else {
        setError(data.error || 'Failed to fetch messages');
      }
    } catch (err) {
      console.error('Error fetching Kafka messages:', err);
      setError('Failed to connect to backend');
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    fetchTopics();
  }, []);

  useEffect(() => {
    if (selectedTopic) {
      fetchMessages();
      // Auto-refresh every 10 seconds
      const interval = setInterval(fetchMessages, 10000);
      return () => clearInterval(interval);
    }
  }, [selectedTopic]);

  const getOperationIcon = (operation) => {
    switch (operation) {
      case 'INSERT':
        return <CheckCircle className="w-4 h-4" />;
      case 'UPDATE':
        return <Edit2 className="w-4 h-4" />;
      case 'DELETE':
        return <Trash2 className="w-4 h-4" />;
      default:
        return <Activity className="w-4 h-4" />;
    }
  };

  const getOperationColor = (operation) => {
    switch (operation) {
      case 'INSERT':
        return 'bg-green-100 text-green-700 border-green-300';
      case 'UPDATE':
        return 'bg-blue-100 text-blue-700 border-blue-300';
      case 'DELETE':
        return 'bg-red-100 text-red-700 border-red-300';
      default:
        return 'bg-gray-100 text-gray-700 border-gray-300';
    }
  };

  const formatTimestamp = (timestamp) => {
    if (!timestamp) return 'N/A';
    const date = new Date(timestamp);
    return date.toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  };

  const getChangedFields = (before, after) => {
    if (!before || !after) return [];
    
    const changes = [];
    Object.keys(after).forEach(key => {
      if (before[key] !== after[key]) {
        changes.push({
          field: key,
          before: before[key],
          after: after[key]
        });
      }
    });
    
    return changes;
  };

  if (loading) {
    return (
      <div className="bg-white rounded-xl border-2 border-gray-200 p-8">
        <div className="flex items-center justify-center">
          <Activity className="w-6 h-6 text-blue-600 animate-spin mr-3" />
          <p className="text-gray-600">Loading CDC messages...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-xl border-2 border-red-200 p-6">
        <div className="flex items-start">
          <AlertCircle className="w-5 h-5 text-red-600 mr-3 flex-shrink-0 mt-0.5" />
          <div>
            <h3 className="text-red-900 font-semibold mb-1">Error Loading CDC Messages</h3>
            <p className="text-sm text-red-700">{error}</p>
            <button
              onClick={fetchMessages}
              className="mt-3 px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 text-sm font-medium"
            >
              Retry
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-xl border-2 border-gray-200">
      <div className="px-6 py-4 border-b border-gray-200">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h3 className="text-lg font-semibold text-gray-900 flex items-center">
              <Database className="w-5 h-5 mr-2 text-purple-600" />
              Kafka CDC Message Backlog
            </h3>
            <p className="text-sm text-gray-500 mt-0.5">
              Real-time CDC operations captured from PostgreSQL
            </p>
          </div>
          <div className="flex items-center gap-3">
            <span className="px-3 py-1 bg-purple-100 text-purple-700 rounded-full text-sm font-semibold">
              {messages.length} message{messages.length !== 1 ? 's' : ''}
            </span>
            <button
              onClick={fetchMessages}
              disabled={refreshing || !selectedTopic}
              className={`p-2 rounded-lg border-2 border-gray-300 hover:bg-gray-50 transition-colors ${
                refreshing || !selectedTopic ? 'cursor-not-allowed opacity-50' : ''
              }`}
              title="Refresh messages"
            >
              <RefreshCw className={`w-4 h-4 text-gray-600 ${refreshing ? 'animate-spin' : ''}`} />
            </button>
          </div>
        </div>
        
        {/* Topic Selector */}
        <div className="flex items-center gap-3">
          <List className="w-4 h-4 text-gray-500" />
          <label className="text-sm font-medium text-gray-700">Topic:</label>
          <select
            value={selectedTopic}
            onChange={(e) => setSelectedTopic(e.target.value)}
            className="flex-1 px-3 py-2 border-2 border-gray-300 rounded-lg text-sm font-mono bg-white hover:border-purple-400 focus:border-purple-500 focus:ring-2 focus:ring-purple-200 outline-none transition-colors"
          >
            {topics.length === 0 ? (
              <option value="">No CDC topics available</option>
            ) : (
              topics.map(topic => (
                <option key={topic} value={topic}>
                  {topic}
                </option>
              ))
            )}
          </select>
        </div>
      </div>
      
      <div className="p-6">
        {messages.length === 0 ? (
          <div className="text-center py-12">
            <Database className="w-16 h-16 text-gray-300 mx-auto mb-4" />
            <p className="text-gray-600 font-medium mb-2">
              {!selectedTopic ? 'Select a topic to view messages' : 'No CDC messages yet'}
            </p>
            <p className="text-sm text-gray-500">
              {!selectedTopic 
                ? 'Choose a Kafka topic from the dropdown above'
                : 'CDC operations (INSERT/UPDATE/DELETE) will appear here'}
            </p>
          </div>
        ) : (
          <div className="space-y-3">
            {messages.map((msg, idx) => {
              const changes = getChangedFields(msg.before, msg.after);
              const displayData = msg.after || msg.before || {};
              
              return (
                <div
                  key={`${msg.offset}-${idx}`}
                  className="border-2 border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow"
                >
                  <div className="flex items-start justify-between mb-3">
                    <div className="flex items-center gap-3">
                      <div className={`px-3 py-1.5 rounded-lg border-2 font-semibold text-sm flex items-center gap-1.5 ${getOperationColor(msg.operation_name)}`}>
                        {getOperationIcon(msg.operation_name)}
                        {msg.operation_name}
                      </div>
                      <div className="flex items-center gap-2">
                        <span className="text-sm text-gray-500">DB:</span>
                        <span className="px-2 py-1 bg-purple-100 text-purple-700 rounded font-mono text-sm font-semibold">
                          {msg.database || 'unknown'}
                        </span>
                        <span className="text-gray-400">|</span>
                        <span className="text-sm text-gray-500">Table:</span>
                        <span className="px-2 py-1 bg-gray-100 text-gray-700 rounded font-mono text-sm">
                          {msg.table}
                        </span>
                      </div>
                    </div>
                    <div className="text-right">
                      <p className="text-xs text-gray-500">Offset {msg.offset}</p>
                      <p className="text-xs text-gray-600 mt-0.5">{formatTimestamp(msg.timestamp)}</p>
                    </div>
                  </div>

                  {/* Display data fields - Generic for any table */}
                  {Object.keys(displayData).length > 0 && (
                    <div className="bg-gray-50 rounded-lg p-3 border border-gray-200">
                      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3 text-sm">
                        {Object.entries(displayData).slice(0, 8).map(([key, value]) => (
                          <div key={key}>
                            <p className="text-gray-500 font-medium mb-1 text-xs uppercase tracking-wide">
                              {key.replace(/_/g, ' ')}
                            </p>
                            <p className="font-semibold text-gray-900 truncate" title={String(value)}>
                              {value !== null && value !== undefined ? String(value) : 'null'}
                            </p>
                          </div>
                        ))}
                      </div>
                      {Object.keys(displayData).length > 8 && (
                        <p className="text-xs text-gray-500 mt-2 text-center">
                          ... and {Object.keys(displayData).length - 8} more field{Object.keys(displayData).length - 8 > 1 ? 's' : ''}
                        </p>
                      )}
                    </div>
                  )}

                  {/* Show changes for UPDATE operations */}
                  {msg.operation_name === 'UPDATE' && changes.length > 0 && (
                    <div className="mt-3 pt-3 border-t border-gray-200">
                      <p className="text-xs font-semibold text-gray-600 mb-2">CHANGES:</p>
                      <div className="flex flex-wrap gap-2">
                        {changes.map((change, changeIdx) => (
                          <div key={changeIdx} className="px-2 py-1 bg-blue-50 border border-blue-200 rounded text-xs">
                            <span className="font-medium text-gray-700">{change.field}:</span>
                            <span className="text-red-600 line-through ml-1">{String(change.before)}</span>
                            <span className="mx-1">→</span>
                            <span className="text-green-600 font-semibold">{String(change.after)}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
};

export default KafkaBacklog;
