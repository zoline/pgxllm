import { useState, useEffect, createContext, useContext } from 'react'
import { NavLink, Outlet, useLocation } from 'react-router-dom'
import { dbApi, statusApi, llmApi } from '../api/client'

// ── DB Context ────────────────────────────────────────────
export const DbContext = createContext({ db: '', setDb: () => {}, dbs: [] })
export const useDb = () => useContext(DbContext)

// ── LLM Context ───────────────────────────────────────────
export const LlmContext = createContext({ llm: null, reloadLlm: () => {} })
export const useLlm = () => useContext(LlmContext)

// ── Styles (inline — no Tailwind needed) ──────────────────
const S = {
  app:     { display:'flex', height:'100vh', overflow:'hidden' },
  sidebar: { width:220, minWidth:220, background:'var(--navy)', display:'flex', flexDirection:'column', flexShrink:0 },
  logo:    { padding:'18px 16px 14px', color:'white', fontSize:22, fontWeight:700, letterSpacing:-0.5, borderBottom:'1px solid rgba(255,255,255,0.1)' },
  logoSub: { color:'var(--teal2)', fontSize:11, fontWeight:400, display:'block', marginTop:3 },
  section: { padding:'10px 16px 4px', fontSize:10, color:'rgba(255,255,255,0.4)', letterSpacing:1, textTransform:'uppercase' },
  navItem: { padding:'9px 16px', color:'rgba(255,255,255,0.7)', cursor:'pointer', display:'flex', alignItems:'center', gap:8, fontSize:13, textDecoration:'none', transition:'all 0.15s' },
  navItemActive: { background:'var(--teal)', color:'white', fontWeight:600 },
  main:    { flex:1, display:'flex', flexDirection:'column', overflow:'hidden' },
  topbar:  { background:'white', borderBottom:'1px solid var(--gray2)', padding:'0 20px', height:48, display:'flex', alignItems:'center', gap:12, flexShrink:0 },
  content: { flex:1, overflowY:'auto', padding:20 },
  statusDot: (ok) => ({ width:7, height:7, borderRadius:'50%', background: ok===true ? 'var(--teal2)' : ok===false ? 'var(--red)' : 'var(--gray2)', flexShrink:0 }),
  dbSelect: { padding:'6px 10px', border:'1px solid var(--gray2)', borderRadius:6, background:'white', fontSize:13, cursor:'pointer', outline:'none', color:'var(--dark)' },
}

const navLinks = [
  { to: '/query',           icon: '▶',  label: 'SQL 실행',      section: 'Query' },
  { to: '/schema',          icon: '🗂',  label: 'Schema 탐색',   section: 'Catalog' },
  { to: '/graph',           icon: '🔗',  label: 'Graph 관계',    section: 'Graph' },
  { type: 'subsection',     icon: '📊',  label: 'pg_stat 수집'  },
  { to: '/pgstat/analyze',  icon: '📋',  label: 'Query 분석',    indent: true },
  { to: '/pgstat/tune',     icon: '🔧',  label: 'Query 튜닝',    indent: true },
  { to: '/rules',           icon: '📐',  label: 'Dialect Rules', section: 'Rules' },
  { to: '/dbs',             icon: '🗄',  label: 'DB 관리',        section: 'Admin' },
  { to: '/llm',             icon: '🤖',  label: 'LLM 설정' },
]

const PROVIDER_LABELS = {
  ollama: 'Ollama', vllm: 'vLLM', lmstudio: 'LM Studio',
  openai: 'OpenAI', anthropic: 'Anthropic', watsonx: 'watsonx.ai',
}

export default function Layout() {
  const [db, setDb]       = useState('')
  const [dbs, setDbs]     = useState([])
  const [status, setStatus] = useState(null)
  const [llm, setLlm]     = useState(null)
  const location = useLocation()

  const reloadLlm = () => llmApi.getConfig().then(setLlm).catch(() => {})

  useEffect(() => {
    statusApi.get().then(setStatus).catch(() => setStatus({ internal_db: { ok: false } }))
    dbApi.list().then(list => {
      setDbs(list)
      if (!db && list.length > 0) setDb(list[0].alias)
    }).catch(() => {})
    reloadLlm()
  }, [])

  const refreshDbs = () =>
    dbApi.list().then(list => { setDbs(list) }).catch(() => {})

  const pageTitle = {
    '/query':          'SQL 실행',
    '/schema':         'Schema 탐색',
    '/graph':          'Graph 관계',
    '/pgstat/analyze': 'Query 분석',
    '/pgstat/tune':    'Query 튜닝',
    '/rules':          'Dialect Rules',
    '/dbs':            'DB 관리',
    '/llm':            'LLM 설정',
  }[location.pathname] || 'pgxllm'

  let prevSection = null

  return (
    <LlmContext.Provider value={{ llm, reloadLlm }}>
    <DbContext.Provider value={{ db, setDb, dbs, refreshDbs }}>
      <div style={S.app}>
        {/* Sidebar */}
        <div style={S.sidebar}>
          <div style={S.logo}>
            pgxllm
            <span style={S.logoSub}>Query Test UI</span>
          </div>

          {navLinks.map((link, idx) => {
            if (link.type === 'subsection') {
              return (
                <div key={`sub-${idx}`} style={{
                  ...S.navItem, opacity: 0.55, cursor: 'default', userSelect: 'none',
                }}>
                  <span style={{ width: 16, textAlign: 'center' }}>{link.icon}</span>
                  {link.label}
                </div>
              )
            }
            const showSection = link.section && link.section !== prevSection
            if (link.section) prevSection = link.section
            return (
              <div key={link.to}>
                {showSection && <div style={S.section}>{link.section}</div>}
                <NavLink
                  to={link.to}
                  style={({ isActive }) => ({
                    ...S.navItem,
                    ...(link.indent ? { paddingLeft: 32, fontSize: 12 } : {}),
                    ...(isActive ? S.navItemActive : {}),
                  })}
                >
                  <span style={{ width: 16, textAlign: 'center' }}>{link.icon}</span>
                  {link.label}
                </NavLink>
              </div>
            )
          })}

          <div style={{ flex:1 }} />

          {/* Internal DB status */}
          <div style={{ padding:'12px 16px', borderTop:'1px solid rgba(255,255,255,0.1)' }}>
            <div style={{ fontSize:11, color:'rgba(255,255,255,0.4)', marginBottom:5 }}>Internal DB</div>
            <div style={{ fontSize:12, color:'rgba(255,255,255,0.6)', display:'flex', alignItems:'center', gap:6 }}>
              <div style={S.statusDot(status?.internal_db?.ok)} />
              <span>{status?.internal_db?.dbname || (status?.internal_db?.ok === false ? 'disconnected' : '...')}</span>
            </div>
          </div>
        </div>

        {/* Main */}
        <div style={S.main}>
          {/* Topbar */}
          <div style={S.topbar}>
            <span style={{ fontSize:15, fontWeight:600 }}>{pageTitle}</span>
            {db && <span style={{ fontSize:12, color:'var(--gray)' }}>@ {db}</span>}
            <div style={{ marginLeft:'auto', display:'flex', alignItems:'center', gap:12 }}>
              {/* Current LLM badge */}
              {llm && (
                <div style={{ display:'flex', alignItems:'center', gap:5, padding:'4px 10px',
                  background:'var(--mint)', borderRadius:6, fontSize:11 }}>
                  <span>🤖</span>
                  <span style={{ fontWeight:700, color:'var(--teal)' }}>
                    {PROVIDER_LABELS[llm.provider] || llm.provider}
                  </span>
                  <span style={{ color:'var(--gray)', fontFamily:'monospace' }}>
                    {llm.model || '—'}
                  </span>
                </div>
              )}
              <select
                style={S.dbSelect}
                value={db}
                onChange={e => setDb(e.target.value)}
              >
                <option value="">— DB 선택 —</option>
                {dbs.map(d => (
                  <option key={d.alias} value={d.alias}>
                    {d.alias} ({d.host}/{d.dbname}){!d.is_reachable ? ' ⚠' : ''}
                  </option>
                ))}
              </select>
            </div>
          </div>

          {/* Page content */}
          <div style={S.content}>
            <Outlet />
          </div>
        </div>
      </div>
    </DbContext.Provider>
    </LlmContext.Provider>
  )
}
