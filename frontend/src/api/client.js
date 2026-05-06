import axios from 'axios'

const api    = axios.create({ baseURL: '/api' })
const apiLlm = axios.create({ baseURL: '/api', timeout: 660_000 })  // 11 min for LLM calls

export const dbApi = {
  list:     ()          => api.get('/db/list').then(r => r.data),
  register: (data)      => api.post('/db/register', data).then(r => r.data),
  refresh:  (alias)     => api.post(`/db/refresh/${alias}`).then(r => r.data),
  remove:   (alias)     => api.delete(`/db/${alias}`).then(r => r.data),
}

export const schemaApi = {
  list:    (alias, search='') =>
    api.get(`/schema/${alias}`, { params: { search } }).then(r => r.data),
  indexes: (alias) =>
    api.get(`/schema/${alias}/indexes`).then(r => r.data),
}

export const queryApi = {
  run: (alias, sql, limit=500, mode='direct', debug=false) =>
    api.post('/query/run', { alias, sql, limit, mode, debug }).then(r => r.data),
  deleteCache:    (alias, question) =>
    api.delete('/query/cache',     { params: { alias, question } }).then(r => r.data),
  deleteAllCache: (alias) =>
    api.delete('/query/cache/all', { params: { alias } }).then(r => r.data),
  history:        (alias, limit=50, mode='') =>
    api.get('/query/history', { params: { alias, limit, mode } }).then(r => r.data),
  deleteHistory:  (alias, id) =>
    api.delete(`/query/history/${id}`, { params: { alias } }).then(r => r.data),
  clearHistory:   (alias) =>
    api.delete('/query/history', { params: { alias } }).then(r => r.data),
}

export const graphApi = {
  edges:            (alias)             => api.get(`/graph/${alias}`).then(r => r.data),
  collectPgStat:    (alias, params)     => api.post(`/graph/${alias}/collect-pg-stat`, params).then(r => r.data),
  refreshPaths:     (alias)             => api.post(`/graph/${alias}/refresh-paths`).then(r => r.data),
  paths:            (alias, params)     => api.get(`/graph/${alias}/paths`, { params }).then(r => r.data),
  inferName:        (alias, edgeId)     => api.post(`/graph/${alias}/edge/${edgeId}/infer-name`).then(r => r.data),
  approve:          (alias, edgeId)     => api.post(`/graph/${alias}/approve/${edgeId}`).then(r => r.data),
  approveAll:       (alias)             => api.post(`/graph/${alias}/approve-all`).then(r => r.data),
  deleteInvalid:    (alias)             => api.delete(`/graph/${alias}/edges/invalid`).then(r => r.data),
  deletePaths:      (alias)             => api.delete(`/graph/${alias}/paths`).then(r => r.data),
  deletePath:       (alias, pathId)     => api.delete(`/graph/${alias}/path/${pathId}`).then(r => r.data),
  updateEdge:       (alias, edgeId, data) => api.patch(`/graph/${alias}/edge/${edgeId}`, data).then(r => r.data),
  deleteEdge:       (alias, edgeId)     => api.delete(`/graph/${alias}/edge/${edgeId}`).then(r => r.data),
}

export const rulesApi = {
  list:   (alias)           => api.get(`/rules/${alias}`).then(r => r.data),
  create: (alias, data)     => api.post(`/rules/${alias}`, data).then(r => r.data),
  toggle: (alias, ruleId, enabled) =>
    api.patch(`/rules/${alias}/${ruleId}`, null, { params: { enabled } }).then(r => r.data),
  remove: (alias, ruleId)   => api.delete(`/rules/${alias}/${ruleId}`).then(r => r.data),
}

export const pgstatApi = {
  queries:   (alias, params)          => api.get(`/pgstat/${alias}/queries`, { params }).then(r => r.data),
  collect:   (alias, data)            => api.post(`/graph/${alias}/collect-pg-stat`, data).then(r => r.data),
  reset:     (alias)                  => api.post(`/pgstat/${alias}/reset`).then(r => r.data),
  infer:     (alias, sql)             => apiLlm.post(`/pgstat/${alias}/query/infer`, { sql }).then(r => r.data),
  saveEdge:  (alias, data)            => api.post(`/pgstat/${alias}/query/save-edge`, data).then(r => r.data),
  saveCache: (alias, question, sql)   => api.post(`/pgstat/${alias}/query/save-cache`, { question, sql }).then(r => r.data),
  tune:      (alias, sql)             => apiLlm.post(`/pgstat/${alias}/query/tune`, { sql }).then(r => r.data),
  describe:  (alias, sql)             => apiLlm.post(`/pgstat/${alias}/query/describe`, { sql }).then(r => r.data),
  plan:      (alias, sql, analyze=false) => api.post(`/pgstat/${alias}/query/plan`, { sql, analyze }).then(r => r.data),
}

export const statusApi = {
  get: () => api.get('/status').then(r => r.data),
}

export const llmApi = {
  providers: ()       => api.get('/llm/providers').then(r => r.data),
  getConfig: ()       => api.get('/llm/config').then(r => r.data),
  saveConfig: (data)  => api.post('/llm/config', data).then(r => r.data),
  test:  (data)       => api.post('/llm/test',   data).then(r => r.data),
}
