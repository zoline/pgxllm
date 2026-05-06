import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import QueryPage from './pages/QueryPage'
import SchemaPage from './pages/SchemaPage'
import { GraphPage, RulesPage, PgStatAnalyzePage, PgStatTunePage } from './pages/GraphRulesPages'
import DbsPage from './pages/DbsPage'
import LLMSettingsPage from './pages/LLMSettingsPage'
import EvalPage from './pages/EvalPage'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/query" replace />} />
          <Route path="query"          element={<QueryPage />} />
          <Route path="schema"         element={<SchemaPage />} />
          <Route path="graph"          element={<GraphPage />} />
          <Route path="pgstat/analyze" element={<PgStatAnalyzePage />} />
          <Route path="pgstat/tune"    element={<PgStatTunePage />} />
          <Route path="rules"          element={<RulesPage />} />
          <Route path="eval"           element={<EvalPage />} />
          <Route path="dbs"            element={<DbsPage />} />
          <Route path="llm"            element={<LLMSettingsPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
