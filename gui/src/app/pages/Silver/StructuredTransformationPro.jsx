import React, { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';
import axios from 'axios';
import { 
  Database, Code, Play, Plus, Trash2, ChevronRight, Settings, 
  Eye, Download, Upload, GitBranch, Clock, DollarSign, AlertTriangle,
  Filter, Percent, Hash, Calendar, Split, Merge, TrendingUp, BarChart3,
  Box, Shuffle, Layers, Target, Zap, CheckCircle, XCircle, Loader, HelpCircle, Info
} from 'lucide-react';
import PipelineBuilder from './PipelineBuilder';

const API_BASE = 'http://localhost:8000/api';

// Help Tooltip Component
const HelpTooltip = ({ title, children }) => {
  const [show, setShow] = useState(false);
  
  return (
    <div className="relative inline-block">
      <span
        role="button"
        tabIndex={0}
        onMouseEnter={() => setShow(true)}
        onMouseLeave={() => setShow(false)}
        onClick={() => setShow(!show)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            setShow(!show);
          }
        }}
        className="ml-1 text-gray-400 hover:text-blue-600 focus:outline-none cursor-pointer inline-flex items-center"
      >
        <HelpCircle className="w-4 h-4" />
      </span>
      
      {show && (
        <div className="absolute left-0 top-6 z-50 w-80 p-3 bg-gray-900 text-white text-xs rounded-lg shadow-xl border border-gray-700">
          {title && <div className="font-semibold mb-1 text-blue-300">{title}</div>}
          <div className="text-gray-200">{children}</div>
          <div className="absolute -top-1 left-4 w-2 h-2 bg-gray-900 border-l border-t border-gray-700 transform rotate-45"></div>
        </div>
      )}
    </div>
  );
};

// Info Box Component
const InfoBox = ({ type = 'info', children }) => {
  const colors = {
    info: 'bg-blue-50 border-blue-200 text-blue-800',
    warning: 'bg-yellow-50 border-yellow-200 text-yellow-800',
    success: 'bg-green-50 border-green-200 text-green-800'
  };
  
  return (
    <div className={`p-4 rounded-lg border ${colors[type]} mb-6`}>
      <div className="flex items-start space-x-2">
        <Info className="w-5 h-5 mt-0.5 flex-shrink-0" />
        <div className="text-sm">{children}</div>
      </div>
    </div>
  );
};

// Comprehensive Transformation Types (Industry Standard)
const TRANSFORMATION_TYPES = {
  // 1. Data Cleaning
  cleaning: {
    category: 'Data Cleaning',
    icon: Filter,
    color: 'blue',
    operations: [
      { id: 'remove_nulls', name: 'Remove Null Values', params: ['columns', 'strategy'] },
      { id: 'fill_nulls', name: 'Fill Missing Values', params: ['columns', 'fill_value', 'method'] },
      { id: 'remove_duplicates', name: 'Remove Duplicates', params: ['columns', 'keep'] },
      { id: 'remove_outliers', name: 'Remove Outliers', params: ['columns', 'method', 'threshold'] },
      { id: 'fix_typos', name: 'Fix Data Quality Issues', params: ['columns', 'pattern', 'replacement'] }
    ]
  },
  
  // 2. Data Normalization & Scaling
  normalization: {
    category: 'Normalization & Scaling',
    icon: Percent,
    color: 'purple',
    operations: [
      { id: 'minmax_scale', name: 'Min-Max Scaling (0-1)', params: ['columns', 'min', 'max'] },
      { id: 'zscore_normalize', name: 'Z-Score Standardization', params: ['columns'] },
      { id: 'robust_scale', name: 'Robust Scaling (IQR)', params: ['columns'] },
      { id: 'maxabs_scale', name: 'Max Absolute Scaling', params: ['columns'] },
      { id: 'unit_vector', name: 'Unit Vector Normalization', params: ['columns'] }
    ]
  },
  
  // 3. Data Encoding
  encoding: {
    category: 'Categorical Encoding',
    icon: Hash,
    color: 'green',
    operations: [
      { id: 'one_hot', name: 'One-Hot Encoding', params: ['columns', 'prefix'] },
      { id: 'label_encode', name: 'Label Encoding', params: ['columns', 'mapping'] },
      { id: 'binary_encode', name: 'Binary Encoding', params: ['columns'] },
      { id: 'target_encode', name: 'Target Encoding', params: ['columns', 'target'] },
      { id: 'frequency_encode', name: 'Frequency Encoding', params: ['columns'] }
    ]
  },
  
  // 4. Aggregation
  aggregation: {
    category: 'Aggregation',
    icon: BarChart3,
    color: 'orange',
    operations: [
      { id: 'group_sum', name: 'Sum by Group', params: ['group_by', 'agg_columns'] },
      { id: 'group_avg', name: 'Average by Group', params: ['group_by', 'agg_columns'] },
      { id: 'group_count', name: 'Count by Group', params: ['group_by'] },
      { id: 'group_min_max', name: 'Min/Max by Group', params: ['group_by', 'agg_columns'] },
      { id: 'rolling_agg', name: 'Rolling Window Aggregation', params: ['columns', 'window', 'function'] }
    ]
  },
  
  // 5. Data Discretization (Binning)
  discretization: {
    category: 'Discretization & Binning',
    icon: Box,
    color: 'red',
    operations: [
      { id: 'equal_width_bin', name: 'Equal Width Binning', params: ['columns', 'bins'] },
      { id: 'equal_freq_bin', name: 'Equal Frequency Binning', params: ['columns', 'bins'] },
      { id: 'custom_bin', name: 'Custom Bin Ranges', params: ['columns', 'bins', 'labels'] },
      { id: 'quantile_bin', name: 'Quantile-based Binning', params: ['columns', 'quantiles'] }
    ]
  },
  
  // 6. Feature Engineering
  feature_engineering: {
    category: 'Feature Engineering',
    icon: Zap,
    color: 'yellow',
    operations: [
      { id: 'extract_datetime', name: 'Extract Date Parts', params: ['column', 'parts'] },
      { id: 'calculate_age', name: 'Calculate Age/Duration', params: ['date_column', 'reference_date'] },
      { id: 'combine_features', name: 'Combine Columns', params: ['columns', 'operation', 'new_name'] },
      { id: 'polynomial_features', name: 'Polynomial Features', params: ['columns', 'degree'] },
      { id: 'interaction_features', name: 'Interaction Features', params: ['columns'] }
    ]
  },
  
  // 7. Data Integration & Merging
  integration: {
    category: 'Data Integration',
    icon: Merge,
    color: 'indigo',
    operations: [
      { id: 'join_inner', name: 'Inner Join (⭐ Spark)', params: ['right_table', 'on'] },
      { id: 'join_left', name: 'Left Join (⭐ Spark)', params: ['right_table', 'on'] },
      { id: 'join_right', name: 'Right Join (⭐ Spark)', params: ['right_table', 'on'] },
      { id: 'join_outer', name: 'Outer Join (⭐ Spark)', params: ['right_table', 'on'] },
      { id: 'union', name: 'Union Tables', params: ['tables'] },
      { id: 'lookup', name: 'Lookup/Enrich', params: ['lookup_table', 'key', 'columns'] }
    ]
  },
  
  // 8. Parsing & Splitting
  parsing: {
    category: 'Parsing & Splitting',
    icon: Split,
    color: 'pink',
    operations: [
      { id: 'split_column', name: 'Split Column', params: ['column', 'delimiter', 'into'] },
      { id: 'extract_regex', name: 'Extract with Regex', params: ['column', 'pattern', 'group'] },
      { id: 'parse_json', name: 'Parse JSON', params: ['column', 'keys'] },
      { id: 'parse_url', name: 'Parse URL Components', params: ['column', 'parts'] }
    ]
  },
  
  // 9. Function Transformation
  function_transform: {
    category: 'Mathematical Functions',
    icon: TrendingUp,
    color: 'teal',
    operations: [
      { id: 'log_transform', name: 'Log Transformation', params: ['columns', 'base'] },
      { id: 'sqrt_transform', name: 'Square Root', params: ['columns'] },
      { id: 'power_transform', name: 'Power Transform', params: ['columns', 'exponent'] },
      { id: 'reciprocal', name: 'Reciprocal (1/x)', params: ['columns'] },
      { id: 'box_cox', name: 'Box-Cox Transform', params: ['columns', 'lambda'] }
    ]
  },
  
  // 10. Generalization
  generalization: {
    category: 'Generalization',
    icon: Layers,
    color: 'cyan',
    operations: [
      { id: 'hierarchy_rollup', name: 'Hierarchy Rollup', params: ['column', 'level', 'mapping'] },
      { id: 'categorize', name: 'Categorize Values', params: ['column', 'categories'] },
      { id: 'round_values', name: 'Round to Precision', params: ['columns', 'precision'] }
    ]
  },
  
  // 11. Pivoting
  pivoting: {
    category: 'Pivoting & Reshaping',
    icon: Shuffle,
    color: 'gray',
    operations: [
      { id: 'pivot_wide', name: 'Pivot to Wide Format', params: ['index', 'columns', 'values'] },
      { id: 'melt_long', name: 'Melt to Long Format', params: ['id_vars', 'value_vars'] }
    ]
  }
};

// AWS Glue Configuration
const WORKER_TYPES = ['G.1X', 'G.2X', 'G.4X', 'G.8X', 'Z.2X'];
const TRIGGER_TYPES = ['On-Demand', 'Scheduled (Cron)', 'Event-Driven'];
const OUTPUT_FORMATS = ['Apache Iceberg', 'Parquet'];

export default function StructuredTransformationPro() {
  const [searchParams] = useSearchParams();
  
  // Active views
  const [activeTab, setActiveTab] = useState('builder'); // builder | pipeline | script | preview
  const [selectedCategory, setSelectedCategory] = useState('cleaning');
  
  // Source context
  const [sourceContext, setSourceContext] = useState({
    tableName: '',
    domain: '',
    source: '',
    layer: 'bronze'
  });
  
  // Transformation pipeline (list of transformation steps)
  const [transformationPipeline, setTransformationPipeline] = useState([]);
  const [currentTransform, setCurrentTransform] = useState({
    type: '',
    operation: '',
    params: {},
    enabled: true
  });
  
  // Data state
  const [schema, setSchema] = useState(null);
  const [previewData, setPreviewData] = useState(null);
  const [transformedPreview, setTransformedPreview] = useState(null);
  
  // Target configuration
  const [silverTableName, setSilverTableName] = useState('');
  const [outputConfig, setOutputConfig] = useState({
    format: 'Parquet',
    s3Path: 's3://syniqai-silver/finance/',
    compression: 'snappy',
    partitionBy: []
  });
  
  // AWS Glue configuration
  const [glueConfig, setGlueConfig] = useState({
    workerType: 'G.2X',
    numberOfWorkers: 2,
    timeout: 60,
    maxRetries: 1,
    triggerType: 'On-Demand',
    cronExpression: '0 0 * * ? *'
  });
  
  // Execution state
  const [isExecuting, setIsExecuting] = useState(false);
  const [executionResult, setExecutionResult] = useState(null);
  const [error, setError] = useState(null);
  const [generatedScript, setGeneratedScript] = useState('');
  
  // Loading states
  const [loading, setLoading] = useState({ schema: false, preview: false });

  // Initialize from URL params (from Data Catalog)
  useEffect(() => {
    const tableParam = searchParams.get('table');
    const domainParam = searchParams.get('domain') || 'finance';
    const sourceParam = searchParams.get('source') || 'postgres';
    
    if (tableParam) {
      setSourceContext({
        tableName: tableParam,
        domain: domainParam,
        source: sourceParam,
        layer: 'bronze'
      });
      
      setSilverTableName(`silver.${tableParam}_transformed`);
      setOutputConfig(prev => ({
        ...prev,
        s3Path: `s3://syniqai-silver/${domainParam}/${tableParam}/`
      }));
      
      loadTableSchema(domainParam, sourceParam, tableParam);
      loadTablePreview(domainParam, sourceParam, tableParam);
    }
  }, [searchParams]);

  // Load schema
  const loadTableSchema = async (domain, source, table) => {
    setLoading(prev => ({ ...prev, schema: true }));
    try {
      const response = await axios.get(`${API_BASE}/bronze-data/schema/${table}`, {
        params: { domain, source }
      });
      if (response.data.success) {
        setSchema(response.data.schema);
      }
    } catch (err) {
      setError(`Failed to load schema: ${err.message}`);
    } finally {
      setLoading(prev => ({ ...prev, schema: false }));
    }
  };

  // Load preview data
  const loadTablePreview = async (domain, source, table, limit = 10) => {
    setLoading(prev => ({ ...prev, preview: true }));
    try {
      const response = await axios.get(`${API_BASE}/bronze-data/preview-data/${table}`, {
        params: { domain, source, limit }
      });
      if (response.data.success) {
        setPreviewData(response.data.rows);
      }
    } catch (err) {
      setError(`Failed to load preview: ${err.message}`);
    } finally {
      setLoading(prev => ({ ...prev, preview: false }));
    }
  };

  // Add transformation to pipeline
  const addTransformation = () => {
    if (!currentTransform.type || !currentTransform.operation) {
      alert('Please select transformation type and operation');
      return;
    }
    
    const newTransform = {
      ...currentTransform,
      id: Date.now(),
      order: transformationPipeline.length + 1,
      name: `Step ${transformationPipeline.length + 1}: ${getOperationName(currentTransform.type, currentTransform.operation)}`
    };
    
    setTransformationPipeline([...transformationPipeline, newTransform]);
    setCurrentTransform({ type: '', operation: '', params: {}, enabled: true });
  };

  // Remove transformation from pipeline
  const removeTransformation = (id) => {
    setTransformationPipeline(transformationPipeline.filter(t => t.id !== id));
  };

  // Toggle transformation enabled/disabled
  const toggleTransformation = (id) => {
    setTransformationPipeline(transformationPipeline.map(t => 
      t.id === id ? { ...t, enabled: !t.enabled } : t
    ));
  };

  // Get operation name
  const getOperationName = (type, operationId) => {
    const category = TRANSFORMATION_TYPES[type];
    if (!category) return operationId;
    const operation = category.operations.find(op => op.id === operationId);
    return operation ? operation.name : operationId;
  };

  // Generate PySpark script
  const generatePySparkScript = () => {
    let script = `# PySpark Transformation Script\n`;
    script += `# Source: ${sourceContext.domain}.${sourceContext.tableName}\n`;
    script += `# Target: ${silverTableName}\n`;
    script += `# Generated: ${new Date().toISOString()}\n\n`;
    
    script += `from pyspark.sql import SparkSession\n`;
    script += `from pyspark.sql import functions as F\n`;
    script += `from pyspark.sql.window import Window\n`;
    script += `from pyspark.ml.feature import MinMaxScaler, StandardScaler, VectorAssembler\n`;
    script += `from pyspark.ml.feature import OneHotEncoder, StringIndexer\n\n`;
    
    script += `# Initialize Spark Session\n`;
    script += `spark = SparkSession.builder \\\n`;
    script += `    .appName("${silverTableName}") \\\n`;
    script += `    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\\n`;
    script += `    .getOrCreate()\n\n`;
    
    script += `# Read source data\n`;
    script += `df = spark.read.parquet("s3://syniqai-bronze/${sourceContext.domain}/${sourceContext.source}/${sourceContext.tableName}/")\n`;
    script += `print(f"Initial count: {df.count()} rows")\n\n`;
    
    // Add each transformation step
    transformationPipeline.forEach((transform, index) => {
      if (!transform.enabled) {
        script += `# Step ${index + 1}: ${transform.name} [DISABLED]\n\n`;
        return;
      }
      
      script += `# Step ${index + 1}: ${transform.name}\n`;
      
      switch (transform.operation) {
        case 'remove_nulls':
          script += `df = df.dropna(subset=[${transform.params.columns?.map(c => `"${c}"`).join(', ')}])\n`;
          break;
        case 'fill_nulls':
          script += `df = df.fillna(${JSON.stringify(transform.params.fill_value)}, subset=[${transform.params.columns?.map(c => `"${c}"`).join(', ')}])\n`;
          break;
        case 'remove_duplicates':
          script += `df = df.dropDuplicates([${transform.params.columns?.map(c => `"${c}"`).join(', ')}])\n`;
          break;
        case 'minmax_scale':
          const cols = transform.params.columns || [];
          script += `# Min-Max Scaling for ${cols.join(', ')}\n`;
          cols.forEach(col => {
            script += `df = df.withColumn("${col}_scaled", (F.col("${col}") - F.min("${col}").over(Window.partitionBy())) / (F.max("${col}").over(Window.partitionBy()) - F.min("${col}").over(Window.partitionBy())))\n`;
          });
          break;
        case 'zscore_normalize':
          script += `# Z-Score Normalization\n`;
          (transform.params.columns || []).forEach(col => {
            script += `mean_${col} = df.select(F.mean("${col}")).first()[0]\n`;
            script += `stddev_${col} = df.select(F.stddev("${col}")).first()[0]\n`;
            script += `df = df.withColumn("${col}_normalized", (F.col("${col}") - mean_${col}) / stddev_${col})\n`;
          });
          break;
        case 'one_hot':
          script += `# One-Hot Encoding\n`;
          (transform.params.columns || []).forEach(col => {
            script += `indexer_${col} = StringIndexer(inputCol="${col}", outputCol="${col}_index")\n`;
            script += `encoder_${col} = OneHotEncoder(inputCol="${col}_index", outputCol="${col}_onehot")\n`;
            script += `df = indexer_${col}.fit(df).transform(df)\n`;
            script += `df = encoder_${col}.fit(df).transform(df)\n`;
          });
          break;
        case 'label_encode':
          script += `# Label Encoding\n`;
          (transform.params.columns || []).forEach(col => {
            script += `indexer = StringIndexer(inputCol="${col}", outputCol="${col}_encoded")\n`;
            script += `df = indexer.fit(df).transform(df)\n`;
          });
          break;
        case 'group_sum':
          const groupByCols = Array.isArray(transform.params.group_by) ? transform.params.group_by : [transform.params.group_by].filter(Boolean);
          const sumCols = Array.isArray(transform.params.agg_columns) ? transform.params.agg_columns : [transform.params.agg_columns].filter(Boolean);
          script += `df = df.groupBy(${groupByCols.map(c => `"${c}"`).join(', ')}).sum(${sumCols.map(c => `"${c}"`).join(', ')})\n`;
          break;
        case 'group_avg':
          const groupByColsAvg = Array.isArray(transform.params.group_by) ? transform.params.group_by : [transform.params.group_by].filter(Boolean);
          const avgCols = Array.isArray(transform.params.agg_columns) ? transform.params.agg_columns : [transform.params.agg_columns].filter(Boolean);
          script += `df = df.groupBy(${groupByColsAvg.map(c => `"${c}"`).join(', ')}).avg(${avgCols.map(c => `"${c}"`).join(', ')})\n`;
          break;
        case 'equal_width_bin':
          script += `# Equal Width Binning\n`;
          script += `df = df.withColumn("${transform.params.columns}_binned", F.floor((F.col("${transform.params.columns}") / ${transform.params.bins})))\n`;
          break;
        case 'extract_datetime':
          script += `# Extract datetime parts\n`;
          const parts = transform.params.parts || [];
          if (parts.includes('year')) script += `df = df.withColumn("year", F.year("${transform.params.column}"))\n`;
          if (parts.includes('month')) script += `df = df.withColumn("month", F.month("${transform.params.column}"))\n`;
          if (parts.includes('day')) script += `df = df.withColumn("day", F.dayofmonth("${transform.params.column}"))\n`;
          if (parts.includes('hour')) script += `df = df.withColumn("hour", F.hour("${transform.params.column}"))\n`;
          break;
        case 'split_column':
          script += `df = df.withColumn("split_col", F.split("${transform.params.column}", "${transform.params.delimiter}"))\n`;
          (transform.params.into || []).forEach((name, idx) => {
            script += `df = df.withColumn("${name}", F.col("split_col")[${idx}])\n`;
          });
          break;
        case 'log_transform':
          script += `# Log Transformation\n`;
          (transform.params.columns || []).forEach(col => {
            script += `df = df.withColumn("${col}_log", F.log("${col}"))\n`;
          });
          break;
        case 'pivot_wide':
          script += `df = df.groupBy("${transform.params.index}").pivot("${transform.params.columns}").agg(F.first("${transform.params.values}"))\n`;
          break;
        default:
          script += `# ${transform.operation} - Implementation pending\n`;
      }
      script += `\n`;
    });
    
    script += `# Write to Silver layer\n`;
    script += `df.write \\\n`;
    script += `    .format("${outputConfig.format.toLowerCase().replace('apache ', '').replace(' lake', '')}") \\\n`;
    script += `    .mode("overwrite") \\\n`;
    script += `    .option("compression", "${outputConfig.compression}") \\\n`;
    script += `    .save("${outputConfig.s3Path}")\n\n`;
    script += `print(f"✅ Transformation complete! Final count: {df.count()} rows")\n`;
    
    setGeneratedScript(script);
    return script;
  };

  // Auto-generate script when pipeline changes
  useEffect(() => {
    if (sourceContext.tableName && transformationPipeline.length > 0) {
      generatePySparkScript();
    }
  }, [transformationPipeline, outputConfig, sourceContext]);

  // Calculate AWS Glue cost
  const calculateCost = () => {
    const dpuPerWorker = { 'G.1X': 1, 'G.2X': 2, 'G.4X': 4, 'G.8X': 8, 'Z.2X': 2 };
    const totalDPU = dpuPerWorker[glueConfig.workerType] * glueConfig.numberOfWorkers;
    const costPerHour = totalDPU * 0.44;
    const estimatedCost = (costPerHour * glueConfig.timeout) / 60;
    return { totalDPU, costPerHour: costPerHour.toFixed(2), estimatedCost: estimatedCost.toFixed(2) };
  };

  const cost = calculateCost();

  // Execute transformation
  const executeTransformation = async () => {
    setIsExecuting(true);
    setError(null);
    try {
      const response = await axios.post(`${API_BASE}/silver/execute-transformation`, {
        source_table: `${sourceContext.domain}.${sourceContext.tableName}`,
        target_table: silverTableName,
        transformations: transformationPipeline.filter(t => t.enabled),
        output_config: outputConfig,
        glue_config: glueConfig
      });
      setExecutionResult(response.data);
    } catch (err) {
      setError(`Transformation failed: ${err.response?.data?.detail || err.message}`);
    } finally {
      setIsExecuting(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-100 px-6 py-4">
        <div className="max-w-7xl mx-auto">
          <h1 className="text-2xl font-bold text-gray-800">Data Transformation Studio</h1>
          <p className="text-sm text-gray-600 mt-1">
            Industry-standard ETL with 50+ transformation operations
          </p>
        </div>
      </div>

      {/* Source Context Banner */}
      {sourceContext.tableName && (
        <div className="bg-gradient-to-r from-blue-50 to-indigo-50 border-b border-blue-200 px-6 py-3">
          <div className="max-w-7xl mx-auto flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <Database className="w-5 h-5 text-blue-600" />
              <div className="flex items-center space-x-2">
                <div className="flex items-center">
                  <span className="text-sm font-medium text-gray-700">Source:</span>
                  <HelpTooltip title="Source Table">
                    This is your <strong>input table</strong> from the Bronze layer:
                    <div className="mt-1 font-mono text-xs bg-gray-800 p-2 rounded">
                      {sourceContext.domain}.{sourceContext.tableName}
                    </div>
                    <div className="mt-2">Data will be read from here and transformed.</div>
                  </HelpTooltip>
                </div>
                <span className="px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm font-mono font-semibold">
                  {sourceContext.domain}.{sourceContext.tableName}
                </span>
                <ChevronRight className="w-4 h-4 text-gray-400" />
                <div className="flex items-center">
                  <span className="text-sm font-medium text-gray-700">Target:</span>
                  <HelpTooltip title="Target Table">
                    This is your <strong>output table</strong> name for the Silver layer:
                    <ul className="mt-2 space-y-1">
                      <li>• Name your transformed table</li>
                      <li>• Will be saved to MinIO Silver bucket</li>
                      <li>• Use descriptive names like:<br/>
                        <span className="font-mono text-xs">customers_cleaned</span><br/>
                        <span className="font-mono text-xs">transactions_aggregated</span>
                      </li>
                    </ul>
                  </HelpTooltip>
                </div>
                <input
                  type="text"
                  value={silverTableName}
                  onChange={(e) => setSilverTableName(e.target.value)}
                  className="px-3 py-1 border border-gray-300 rounded-full text-sm font-mono bg-white"
                />
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Tab Navigation */}
      <div className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-6">
          <div className="flex space-x-8">
            {[
              { id: 'builder', label: 'Sequential View', icon: Settings },
              { id: 'pipeline', label: 'Visual Pipeline', icon: GitBranch },
              { id: 'script', label: 'Generated Script', icon: Code },
              { id: 'preview', label: 'Data Preview', icon: Eye }
            ].map(tab => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`py-3 px-4 border-b-2 font-medium text-sm transition-colors flex items-center space-x-2 ${
                  activeTab === tab.id
                    ? 'border-blue-600 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                <tab.icon className="w-4 h-4" />
                <span>{tab.label}</span>
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-6 py-6">
        {activeTab === 'builder' && (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* Left: Transformation Categories & Operations */}
            <div className="lg:col-span-2 space-y-6 max-h-[calc(100vh-200px)] overflow-y-auto pr-4">
              {/* Getting Started Guide */}
              <InfoBox type="info">
                <strong> Getting Started:</strong> Transform your data in 3 easy steps:
                <ol className="mt-2 ml-4 list-decimal space-y-1">
                  <li><strong>Choose a category</strong> below (e.g., Data Cleaning, Aggregation)</li>
                  <li><strong>Select an operation</strong> and configure parameters</li>
                  <li><strong>Add to pipeline</strong> and click "Execute" when ready</li>
                </ol>
              </InfoBox>

              {/* Transformation Categories */}
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <div className="flex items-center mb-4">
                  <h2 className="text-lg font-semibold text-gray-800">Select Transformation Type</h2>
                  <HelpTooltip title="Transformation Categories">
                    These are grouped operations for common data tasks:
                    <ul className="mt-2 space-y-1">
                      <li>• <strong>Data Cleaning:</strong> Fix null values, duplicates, outliers</li>
                      <li>• <strong>Normalization:</strong> Scale numbers to standard ranges</li>
                      <li>• <strong>Aggregation:</strong> Sum, average, count by groups</li>
                      <li>• <strong>Integration:</strong> Join tables, merge data</li>
                      <li>• <strong>Feature Engineering:</strong> Extract date parts, create new columns</li>
                    </ul>
                    <div className="mt-2 text-blue-300">💡 Start with Data Cleaning if you're unsure!</div>
                  </HelpTooltip>
                </div>
                <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
                  {Object.entries(TRANSFORMATION_TYPES).map(([key, category]) => {
                    const Icon = category.icon;
                    return (
                      <button
                        key={key}
                        onClick={() => setSelectedCategory(key)}
                        className={`p-4 rounded-lg border-2 transition-all ${
                          selectedCategory === key
                            ? `border-${category.color}-500 bg-${category.color}-50`
                            : 'border-gray-200 hover:border-gray-300 bg-white'
                        }`}
                      >
                        <Icon className={`w-6 h-6 mb-2 text-${category.color}-600`} />
                        <div className="text-sm font-medium text-gray-800">{category.category}</div>
                        <div className="text-xs text-gray-500 mt-1">{category.operations.length} ops</div>
                      </button>
                    );
                  })}
                </div>
              </div>

              {/* Operations for Selected Category */}
              {selectedCategory && (
                <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                  <div className="flex items-center mb-4">
                    <h2 className="text-lg font-semibold text-gray-800">
                      {TRANSFORMATION_TYPES[selectedCategory].category} Operations
                    </h2>
                    <HelpTooltip title={`${TRANSFORMATION_TYPES[selectedCategory].category} Operations`}>
                      Select an operation to apply to your data. Each operation requires specific parameters:
                      <ul className="mt-2 space-y-1">
                        <li>• <strong>columns:</strong> Which columns to apply the operation to</li>
                        <li>• <strong>method/strategy:</strong> How to perform the operation</li>
                        <li>• <strong>threshold/value:</strong> Numeric parameters for calculations</li>
                      </ul>
                      <div className="mt-2 text-blue-300">💡 You can hold Ctrl to select multiple columns!</div>
                    </HelpTooltip>
                  </div>
                  
                  <div className="space-y-4">
                    {/* Operation Selector */}
                    <div>
                      <div className="flex items-center mb-2">
                        <label className="block text-sm font-medium text-gray-700">
                          Select Operation
                        </label>
                        <HelpTooltip title="Operations">
                          Each category has multiple operations:
                          <div className="mt-1 text-gray-300">
                            Choose the one that fits your data transformation needs. Hover over parameters for more details.
                          </div>
                        </HelpTooltip>
                      </div>
                      <select
                        value={currentTransform.operation}
                        onChange={(e) => setCurrentTransform({ 
                          ...currentTransform, 
                          type: selectedCategory,
                          operation: e.target.value,
                          params: {}
                        })}
                        className="w-full px-4 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500"
                      >
                        <option value="">Choose an operation...</option>
                        {TRANSFORMATION_TYPES[selectedCategory].operations.map(op => (
                          <option key={op.id} value={op.id}>{op.name}</option>
                        ))}
                      </select>
                    </div>

                    {/* Dynamic Parameter Inputs */}
                    {currentTransform.operation && (
                      <div className="space-y-3 p-4 bg-gray-50 rounded-md">
                        <div className="flex items-center">
                          <div className="text-sm font-medium text-gray-700">Configure Parameters:</div>
                          <HelpTooltip title="Parameters">
                            <strong>Common Parameters:</strong>
                            <ul className="mt-1 space-y-1">
                              <li>• <strong>columns:</strong> Hold Ctrl/Cmd to select multiple</li>
                              <li>• <strong>fill_value:</strong> What to replace nulls with</li>
                              <li>• <strong>group_by:</strong> Columns to group by for aggregation</li>
                              <li>• <strong>right_table:</strong> Table name for joins (must exist in Bronze)</li>
                              <li>• <strong>on:</strong> Join key column name</li>
                            </ul>
                          </HelpTooltip>
                        </div>
                        
                        {TRANSFORMATION_TYPES[selectedCategory].operations
                          .find(op => op.id === currentTransform.operation)
                          ?.params.map(param => (
                            <div key={param}>
                              <label className="block text-xs text-gray-600 mb-1">{param}</label>
                              {param === 'columns' || param.includes('column') ? (
                                <select
                                  multiple={param === 'columns'}
                                  value={currentTransform.params[param] || (param === 'columns' ? [] : '')}
                                  onChange={(e) => {
                                    const value = param === 'columns' 
                                      ? Array.from(e.target.selectedOptions, option => option.value)
                                      : e.target.value;
                                    setCurrentTransform({
                                      ...currentTransform,
                                      params: { ...currentTransform.params, [param]: value }
                                    });
                                  }}
                                  className="w-full px-3 py-2 border border-gray-300 rounded text-sm"
                                >
                                  {schema?.map(col => (
                                    <option key={col.name} value={col.name}>{col.name}</option>
                                  ))}
                                </select>
                              ) : (
                                <input
                                  type="text"
                                  value={currentTransform.params[param] || ''}
                                  onChange={(e) => setCurrentTransform({
                                    ...currentTransform,
                                    params: { ...currentTransform.params, [param]: e.target.value }
                                  })}
                                  className="w-full px-3 py-2 border border-gray-300 rounded text-sm"
                                  placeholder={`Enter ${param}`}
                                />
                              )}
                            </div>
                          ))}

                        <button
                          onClick={addTransformation}
                          className="w-full mt-3 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 flex items-center justify-center space-x-2"
                        >
                          <Plus className="w-4 h-4" />
                          <span>Add to Pipeline</span>
                        </button>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* Transformation Pipeline */}
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <div className="flex items-center mb-4">
                  <h3 className="text-sm font-semibold text-gray-800">
                    Transformation Pipeline ({transformationPipeline.length} steps)
                  </h3>
                  <HelpTooltip title="Transformation Pipeline">
                    Your transformations are applied in order from top to bottom:
                    <ul className="mt-2 space-y-1">
                      <li>• <strong>Order matters!</strong> Step 1 runs first, then Step 2, etc.</li>
                      <li>• Toggle <strong>Active/Disabled</strong> to skip a step</li>
                      <li>• <strong>Remove</strong> (trash icon) to delete a step</li>
                      <li>• Add multiple steps to create complex transformations</li>
                    </ul>
                    <div className="mt-2 text-yellow-300"> Example: Clean nulls BEFORE aggregating!</div>
                  </HelpTooltip>
                </div>
                
                {transformationPipeline.length === 0 ? (
                  <div className="text-center py-8 text-gray-500">
                    <GitBranch className="w-12 h-12 mx-auto mb-3 text-gray-400" />
                    <p>No transformations added yet</p>
                    <p className="text-sm mt-1">Select a transformation type and operation above to get started</p>
                  </div>
                ) : (
                  <div className="space-y-2 max-h-[600px] overflow-y-auto pr-2">
                    {transformationPipeline.map((transform, index) => (
                      <div
                        key={transform.id}
                        className={`p-4 rounded-lg border-2 transition-all ${
                          transform.enabled 
                            ? 'border-blue-200 bg-blue-50' 
                            : 'border-gray-200 bg-gray-50 opacity-50'
                        }`}
                      >
                        <div className="flex items-start justify-between">
                          <div className="flex-1">
                            <div className="flex items-center space-x-2 mb-2">
                              <span className="flex items-center justify-center w-6 h-6 rounded-full bg-blue-600 text-white text-xs font-bold">
                                {index + 1}
                              </span>
                              <span className="font-medium text-gray-800">{transform.name}</span>
                              {transform.enabled ? (
                                <span className="flex items-center px-2 py-1 bg-green-100 text-green-700 rounded text-xs">
                                  <CheckCircle className="w-3 h-3 mr-1" />
                                  Active
                                </span>
                              ) : (
                                <span className="flex items-center px-2 py-1 bg-gray-200 text-gray-600 rounded text-xs">
                                  <XCircle className="w-3 h-3 mr-1" />
                                  Disabled
                                </span>
                              )}
                            </div>
                            <div className="ml-8 text-sm text-gray-600">
                              {Object.entries(transform.params).map(([key, value]) => (
                                <div key={key} className="flex items-center space-x-2">
                                  <span className="text-gray-500">{key}:</span>
                                  <span className="font-mono text-xs">{Array.isArray(value) ? value.join(', ') : value}</span>
                                </div>
                              ))}
                            </div>
                          </div>
                          <div className="flex items-center space-x-2">
                            <button
                              onClick={() => toggleTransformation(transform.id)}
                              className="p-1 hover:bg-white rounded"
                              title={transform.enabled ? 'Disable' : 'Enable'}
                            >
                              {transform.enabled ? (
                                <CheckCircle className="w-5 h-5 text-green-600" />
                              ) : (
                                <XCircle className="w-5 h-5 text-gray-400" />
                              )}
                            </button>
                            <button
                              onClick={() => removeTransformation(transform.id)}
                              className="p-1 hover:bg-white rounded text-red-600"
                              title="Remove"
                            >
                              <Trash2 className="w-5 h-5" />
                            </button>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>

            {/* Right: Configuration & Execution */}
            <div className="space-y-6">
              {/* Output Config */}
              <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
                <div className="flex items-center mb-4">
                  <h3 className="text-sm font-semibold text-gray-800">Output Configuration</h3>
                  <HelpTooltip title="Output Configuration">
                    Configure where and how to save your transformed data:
                    <ul className="mt-2 space-y-1">
                      <li>• <strong>Parquet:</strong> Fast columnar format (recommended for most cases)</li>
                      <li>• <strong>Apache Iceberg:</strong> Table format with versioning & time travel</li>
                      <li>• <strong>Path:</strong> Where to save in MinIO (s3://syniqai-silver/...)</li>
                    </ul>
                    <div className="mt-2 text-blue-300">💡 Use Parquet unless you need Iceberg features!</div>
                  </HelpTooltip>
                </div>
                <div className="space-y-3">
                  <div>
                    <div className="flex items-center mb-1">
                      <label className="block text-xs text-gray-600">Format</label>
                      <HelpTooltip title="Output Format">
                        <strong>Parquet:</strong> Standard columnar format, fast & efficient<br/>
                        <strong>Iceberg:</strong> Advanced table format with schema evolution
                      </HelpTooltip>
                    </div>
                    <select
                      value={outputConfig.format}
                      onChange={(e) => setOutputConfig({ ...outputConfig, format: e.target.value })}
                      className="w-full px-3 py-2 border border-gray-300 rounded text-sm"
                    >
                      {OUTPUT_FORMATS.map(format => (
                        <option key={format} value={format}>{format}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <div className="flex items-center mb-1">
                      <label className="block text-xs text-gray-600">Silver Layer Path (MinIO)</label>
                      <HelpTooltip title="MinIO Path">
                        S3-compatible path where data will be saved:
                        <div className="mt-1 font-mono text-xs bg-gray-800 p-2 rounded">
                          s3://syniqai-silver/[domain]/[table]/
                        </div>
                        <div className="mt-2">
                          • <strong>syniqai-silver</strong> = bucket name<br/>
                          • <strong>domain</strong> = finance, sales, etc.<br/>
                          • <strong>table</strong> = your table name
                        </div>
                      </HelpTooltip>
                    </div>
                    <input
                      type="text"
                      value={outputConfig.s3Path}
                      onChange={(e) => setOutputConfig({ ...outputConfig, s3Path: e.target.value })}
                      placeholder="s3://syniqai-silver/{domain}/{table}/"
                      className="w-full px-3 py-2 border border-gray-300 rounded text-sm font-mono"
                    />
                  </div>
                </div>
              </div>

              {/* Execute Button */}
              {transformationPipeline.length === 0 && (
                <InfoBox type="warning">
                  <strong> No transformations added yet!</strong>
                  <div className="mt-1">Select a category and operation above, then click "Add to Pipeline" to get started.</div>
                </InfoBox>
              )}
              <button
                onClick={executeTransformation}
                disabled={isExecuting || transformationPipeline.length === 0}
                className="w-full px-6 py-4 bg-gradient-to-r from-blue-600 to-blue-700 text-white rounded-lg hover:from-blue-700 hover:to-blue-800 disabled:opacity-50 disabled:cursor-not-allowed font-semibold flex items-center justify-center space-x-2 shadow-lg"
              >
                {transformationPipeline.length === 0 ? (
                  <HelpTooltip title="Execute Pipeline">
                    This button executes all enabled transformations in your pipeline.
                    <div className="mt-2">Add at least one transformation to enable execution.</div>
                  </HelpTooltip>
                ) : null}
                {isExecuting ? (
                  <>
                    <Loader className="w-5 h-5 animate-spin" />
                    <span>Executing...</span>
                  </>
                ) : (
                  <>
                    <Play className="w-5 h-5" />
                    <span>Execute Pipeline</span>
                  </>
                )}
              </button>

              {executionResult && (
                <div className="p-4 bg-green-50 border border-green-200 rounded-lg">
                  <div className="flex items-center space-x-2 text-green-800 font-medium mb-3">
                    <CheckCircle className="w-5 h-5" />
                    <span>Transformation Complete!</span>
                    <HelpTooltip title="Execution Results">
                      Your data transformation has finished successfully:
                      <ul className="mt-2 space-y-1">
                        <li>• <strong>Input/Output Rows:</strong> Number of records before/after transformation</li>
                        <li>• <strong>Duration:</strong> How long the transformation took</li>
                        <li>• <strong>Throughput:</strong> Processing speed (rows per second)</li>
                        <li>• <strong>Output Table:</strong> Where your data was saved</li>
                      </ul>
                      <div className="mt-2 text-blue-300">💡 Check MinIO Silver bucket to see your files!</div>
                    </HelpTooltip>
                  </div>
                  <div className="grid grid-cols-2 gap-3 text-sm">
                    <div>
                      <span className="text-gray-600">Input Rows:</span>
                      <span className="ml-2 font-mono font-semibold">{executionResult.result?.input_rows?.toLocaleString() || 'N/A'}</span>
                    </div>
                    <div>
                      <span className="text-gray-600">Output Rows:</span>
                      <span className="ml-2 font-mono font-semibold">{executionResult.result?.output_rows?.toLocaleString() || 'N/A'}</span>
                    </div>
                    <div>
                      <span className="text-gray-600">Duration:</span>
                      <span className="ml-2 font-mono font-semibold">{executionResult.result?.duration_seconds?.toFixed(1) || 'N/A'}s</span>
                    </div>
                    <div>
                      <span className="text-gray-600">Throughput:</span>
                      <span className="ml-2 font-mono font-semibold">{executionResult.result?.rows_per_second?.toLocaleString() || 'N/A'} rows/s</span>
                    </div>
                    <div className="col-span-2">
                      <span className="text-gray-600">Output Table:</span>
                      <span className="ml-2 font-mono font-semibold text-blue-600">{executionResult.result?.output_table || 'N/A'}</span>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {activeTab === 'pipeline' && (
          <div className="h-[calc(100vh-280px)]">
            <PipelineBuilder />
          </div>
        )}

        {activeTab === 'script' && (
          <div className="bg-gray-900 rounded-lg shadow-sm border border-gray-700 p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-lg font-semibold text-white flex items-center">
                <Code className="w-5 h-5 mr-2" />
                Generated PySpark Script
              </h2>
              <button
                onClick={() => navigator.clipboard.writeText(generatedScript)}
                className="px-3 py-1 bg-blue-600 text-white rounded hover:bg-blue-700 text-sm flex items-center space-x-2"
              >
                <Download className="w-4 h-4" />
                <span>Copy Script</span>
              </button>
            </div>
            <pre className="text-green-400 text-sm font-mono overflow-x-auto whitespace-pre-wrap max-h-[600px] overflow-y-auto">
              {generatedScript || '# Add transformations to generate script...'}
            </pre>
          </div>
        )}

        {activeTab === 'preview' && (
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <h2 className="text-lg font-semibold text-gray-800 mb-4 flex items-center">
              <Eye className="w-5 h-5 mr-2" />
              Data Preview
            </h2>
            {previewData && previewData.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200 text-sm">
                  <thead className="bg-gray-50">
                    <tr>
                      {Object.keys(previewData[0]).map(col => (
                        <th key={col} className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase">
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {previewData.map((row, index) => (
                      <tr key={index} className="hover:bg-gray-50">
                        {Object.values(row).map((val, i) => (
                          <td key={i} className="px-4 py-2 whitespace-nowrap text-xs text-gray-600">
                            {String(val)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-center py-8 text-gray-500">
                No preview data available
              </div>
            )}
          </div>
        )}
      </div>

      {/* Error Display */}
      {error && (
        <div className="fixed bottom-4 right-4 bg-red-50 border border-red-200 rounded-lg p-4 shadow-lg max-w-md">
          <div className="flex items-start">
            <AlertTriangle className="w-5 h-5 text-red-600 mr-2 flex-shrink-0" />
            <div className="flex-1">
              <div className="font-semibold text-red-800">Error</div>
              <div className="text-sm text-red-700 mt-1">{error}</div>
            </div>
            <button
              onClick={() => setError(null)}
              className="text-red-600 hover:text-red-800 ml-2"
            >
              ✕
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
