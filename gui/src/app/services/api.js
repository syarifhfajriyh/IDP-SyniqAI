import axios from 'axios'

const API_BASE_URL = '/api'

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

export const dashboardAPI = {
  getSummary: () => api.get('/dashboard-summary'),
  getQualitySummary: () => api.get('/quality-summary'),
  getTables: () => api.get('/tables'),
}

export const edaAPI = {
  getReport: (source, table) => api.get(`/eda/${source}/${table}`),
  getFeatureAnalysis: (table, feature) => api.get(`/eda/${table}/feature/${feature}`),
}

export const qualityAPI = {
  getMetrics: () => api.get('/quality'),
  getAlerts: () => api.get('/quality/alerts'),
  getHistory: (table) => api.get(`/quality/history/${table}`),
}

export const reportsAPI = {
  generate: (type, format) => api.post('/generate-report', { type, format }),
  download: (reportId) => api.get(`/download-report/${reportId}`, { responseType: 'blob' }),
  list: () => api.get('/reports'),
}

export const ingestionAPI = {
  getLogs: (limit = 50) => api.get(`/ingestion-logs?limit=${limit}`),
  getStatus: () => api.get('/ingestion-status'),
  testConnection: (config) => api.post('/connection/test', config),
  startIngestion: (request) => api.post('/ingestion/start', request),
  getJobStatus: (jobId) => api.get(`/ingestion/status/${jobId}`),
  getJobLogs: (jobId) => api.get(`/ingestion/logs/${jobId}`),
  listJobs: (params) => api.get('/ingestion/jobs', { params }),
}

export const minioAPI = {
  getConfig: () => api.get('/config/minio'),
  listTables: (layer) => api.get(`/tables/${layer}`),
  getTableInfo: (layer, source, entity) => api.get(`/tables/${layer}/${source}/${entity}`),
}

export const silverAPI = {
  processToSilver: (source, entity, sourceType = 'postgres') => 
    api.post('/silver/process', null, { params: { source, entity, source_type: sourceType } }),
  getJobStatus: (jobId) => api.get(`/silver/status/${jobId}`),
  listJobs: (status, limit = 50) => api.get('/silver/jobs', { params: { status, limit } }),
  viewData: (source, entity, page = 1, pageSize = 100) => 
    api.get(`/silver/view/${source}/${entity}`, { params: { page, page_size: pageSize } }),
}

export const goldAPI = {
  listTables: () => api.get('/gold/tables'),
  generateEDA: (source, entity) => api.post('/gold/eda/generate', null, { params: { source, entity } }),
  getEDAReport: (source, entity) => api.get(`/gold/eda/${source}/${entity}`),
  getEDAViz: (source, entity) => api.get(`/gold/eda/${source}/${entity}/viz`),
}

// New domain-based Bronze API
export const bronzeAPI = {
  getDomains: () => api.get('/domains'),
  getTables: (domain) => api.get(`/bronze/tables/${domain}`),
  getTableDetails: (domain, tableName) => api.get(`/bronze/table/${domain}/${tableName}`),
}

// Rules Management API
export const rulesAPI = {
  getRules: (domain, active) => api.get('/rules', { params: { domain, active } }),
  getRule: (ruleId) => api.get(`/rules/${ruleId}`),
  createRule: (ruleData) => api.post('/rules', ruleData),
  updateRule: (ruleId, ruleData) => api.put(`/rules/${ruleId}`, ruleData),
  deleteRule: (ruleId) => api.delete(`/rules/${ruleId}`),
  getExecutionHistory: (ruleId, limit = 50) => api.get(`/execution/history/${ruleId}`, { params: { limit } }),
  getFailures: (domain) => api.get(`/execution/failures/${domain}`),
}

// Quarantine Management API
export const quarantineAPI = {
  getRecords: (domain, status, limit = 50) => api.get(`/quarantine/${domain}`, { params: { status, limit } }),
  resolve: (quarantineId, action, correctedData) => 
    api.post(`/quarantine/${quarantineId}/resolve`, { action, corrected_data: correctedData }),
}

export default api
