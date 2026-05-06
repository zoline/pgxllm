import { useState, useEffect, useCallback } from 'react'
import { useDb } from '../components/Layout'
import { queryApi } from '../api/client'
import { Card, CardHeader, CardBody, Btn, Alert, Spinner, CountBadge, Badge } from '../components/UI'

export default function QueryPage() {
  const { db } = useDb()
  const [mode, setMode]       = useState('direct')
  const [sql, setSql]         = useState('SELECT * FROM information_schema.tables\nLIMIT 20;')
  const [question, setQuestion] = useState('')
  const [result, setResult]     = useState(null)
  const [pipeResult, setPipeResult] = useState(null)
  const [error, setError]       = useState('')
  const [loading, setLoading]   = useState(false)
  const [elapsed, setElapsed]   = useState(null)
  const [debugMode, setDebugMode] = useState(false)
  const [cacheDeleting, setCacheDeleting]   = useState(false)
  const [sqlRunning, setSqlRunning]         = useState(false)
  const [pipeExecResult, setPipeExecResult] = useState(null)
  const [pipeExecError, setPipeExecError]   = useState('')
  const [history, setHistory]             = useState([])
  const [historyErr, setHistoryErr]       = useState('')
  const [showHistory, setShowHistory]     = useState(false)

  const loadHistory = useCallback(() => {
    if (!db) return
    setHistoryErr('')
    queryApi.history(db, 50, mode)
      .then(data => { setHistory(data); setHistoryErr('') })
      .catch(e => setHistoryErr(e.response?.data?.detail || e.message))
  }, [db, mode])

  useEffect(() => { if (showHistory) loadHistory() }, [showHistory, loadHistory])

  const handleKey = (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') runQuery()
  }

  async function deleteCache(all = false) {
    if (!db) return
    setCacheDeleting(true)
    try {
      let resp
      if (all) {
        resp = await queryApi.deleteAllCache(db)
      } else {
        // 실행 시점의 질문을 사용해야 정확히 일치 (textarea가 수정됐을 수 있음)
        const target = pipeResult?.originalQuestion || question
        resp = await queryApi.deleteCache(db, target)
      }
      setError('')
      setPipeResult(null)
      alert(`캐시 ${resp.deleted}건 삭제됨${all ? ' (전체)' : ''}`)
    } catch (e) {
      setError(e.response?.data?.detail || e.message)
    } finally {
      setCacheDeleting(false)
    }
  }

  async function runQuery() {
    const input = mode === 'pipeline' ? question : sql
    if (!input.trim()) { setError(mode === 'direct' ? 'SQL을 입력하세요.' : '질문을 입력하세요.'); return }
    if (!db)           { setError('상단에서 DB를 선택하세요.'); return }
    setLoading(true); setError(''); setResult(null); setPipeResult(null); setPipeExecResult(null); setPipeExecError('')
    const t0 = Date.now()
    try {
      const data = await queryApi.run(db, input, 500, mode, mode === 'pipeline' && debugMode)
      setElapsed(((Date.now() - t0) / 1000).toFixed(2))
      if (mode === 'pipeline') {
        setPipeResult({ ...data, originalQuestion: input })
      } else {
        setResult(data)
      }
    } catch (e) {
      setError(e.response?.data?.detail || e.message)
    } finally {
      setLoading(false)
      if (showHistory) loadHistory()
    }
  }

  const modeLabel = mode === 'direct' ? 'SQL 직접 실행' : 'LLM 쿼리 (자연어 → SQL)'

  return (
    <div>
      <Card>
        <CardHeader>
          <div style={{ display:'flex', gap:6, marginRight:12 }}>
            {['direct', 'pipeline'].map(m => (
              <Btn key={m} size="sm"
                variant={mode === m ? 'primary' : 'secondary'}
                onClick={() => { setMode(m); setResult(null); setPipeResult(null); setPipeExecResult(null); setPipeExecError(''); setError('') }}>
                {m === 'direct' ? '▶ SQL' : '🤖 LLM 쿼리'}
              </Btn>
            ))}
          </div>
          <span style={{ fontSize:13, fontWeight:600 }}>{modeLabel}</span>
          {mode === 'pipeline' && (
            <span style={{ fontWeight:400, fontSize:12, color:'var(--gray)', marginLeft:4 }}>
              자연어 질문 → LLM → SQL 생성
            </span>
          )}
          {mode === 'direct' && (
            <span style={{ fontWeight:400, fontSize:12, color:'var(--gray)', marginLeft:4 }}>
              SELECT / WITH / EXPLAIN  ·  Ctrl+Enter
            </span>
          )}
          {mode === 'pipeline' && (
            <button
              onClick={() => setDebugMode(v => !v)}
              style={{
                marginLeft:'auto', padding:'3px 10px', borderRadius:5,
                border:`1px solid ${debugMode ? 'var(--teal)' : 'var(--gray2)'}`,
                background: debugMode ? 'var(--mint)' : 'transparent',
                color: debugMode ? 'var(--teal)' : 'var(--gray)',
                fontSize:11, fontWeight:600, cursor:'pointer',
              }}>
              🔍 Debug
            </button>
          )}
        </CardHeader>
        <CardBody>
          {mode === 'pipeline' ? (
            <textarea
              value={question}
              onChange={e => setQuestion(e.target.value)}
              onKeyDown={handleKey}
              style={{
                width:'100%', height:80,
                fontFamily:'inherit', fontSize:14,
                border:'1px solid var(--gray2)', borderRadius:6,
                padding:'10px 12px', resize:'vertical', outline:'none',
                color:'var(--dark)', lineHeight:1.7,
              }}
              placeholder="top 10 customers who have rented the most movies&#10;&#10;Ctrl+Enter 실행"
              onFocus={e => e.target.style.borderColor='var(--teal)'}
              onBlur={e => e.target.style.borderColor='var(--gray2)'}
            />
          ) : (
            <textarea
              value={sql}
              onChange={e => setSql(e.target.value)}
              onKeyDown={handleKey}
              style={{
                width:'100%', height:140,
                fontFamily:'Consolas, monospace', fontSize:13,
                border:'1px solid var(--gray2)', borderRadius:6,
                padding:'10px 12px', resize:'vertical', outline:'none',
                color:'var(--dark)', lineHeight:1.6,
              }}
              placeholder={'SELECT * FROM public.orders LIMIT 10;\n\n-- Ctrl+Enter 실행'}
              onFocus={e => e.target.style.borderColor='var(--teal)'}
              onBlur={e => e.target.style.borderColor='var(--gray2)'}
            />
          )}
          <div style={{ display:'flex', alignItems:'center', gap:8, marginTop:8, flexWrap:'wrap' }}>
            <Btn onClick={runQuery} disabled={loading}>
              {loading ? <Spinner /> : (mode==='pipeline' ? '🤖' : '▶')}
              {mode==='pipeline' ? ' 질문 실행' : ' 실행'}
            </Btn>
            <Btn variant="secondary" size="sm"
              onClick={() => { mode==='pipeline' ? setQuestion('') : setSql(''); setResult(null); setPipeResult(null); setError('') }}>
              지우기
            </Btn>
            {mode === 'pipeline' && question.trim() && (
              <Btn variant="secondary" size="sm"
                disabled={cacheDeleting}
                onClick={() => deleteCache(false)}
                style={{ color:'#dc2626', borderColor:'#fca5a5' }}>
                {cacheDeleting ? <Spinner /> : '🗑'} 캐시 삭제
              </Btn>
            )}
            {mode === 'pipeline' && (
              <Btn variant="secondary" size="sm"
                disabled={cacheDeleting}
                onClick={() => deleteCache(true)}
                style={{ color:'#9333ea', borderColor:'#d8b4fe', fontSize:11 }}>
                전체 캐시 삭제
              </Btn>
            )}
            {elapsed && !loading && (
              <span style={{ fontSize:12, color:'var(--gray)' }}>{elapsed}s</span>
            )}
            {result && <CountBadge>{result.count} rows</CountBadge>}
            {pipeResult?.cache_hit && <Badge color="teal">cache hit</Badge>}
            <button
              onClick={() => setShowHistory(v => !v)}
              style={{
                marginLeft:'auto', padding:'3px 10px', borderRadius:5,
                border:`1px solid ${showHistory ? 'var(--navy)' : 'var(--gray2)'}`,
                background: showHistory ? 'var(--navy)' : 'transparent',
                color: showHistory ? 'white' : 'var(--gray)',
                fontSize:11, fontWeight:600, cursor:'pointer',
              }}>
              🕐 이력
            </button>
          </div>
        </CardBody>
      </Card>

      {showHistory && (
        <HistoryPanel
          history={history}
          err={historyErr}
          db={db}
          mode={mode}
          onSelect={text => {
            if (mode === 'pipeline') setQuestion(text)
            else setSql(text)
          }}
          onDelete={async (id) => {
            await queryApi.deleteHistory(db, id)
            loadHistory()
          }}
          onClear={async () => {
            await queryApi.clearHistory(db)
            loadHistory()
          }}
        />
      )}

      {error && <Alert type="error">⚠ {error}</Alert>}

      {/* Pipeline result */}
      {pipeResult && (
        <Card>
          <CardHeader>
            🤖 LLM 쿼리 결과
            {pipeResult.ok
              ? <Badge color="green" style={{ marginLeft:8 }}>성공</Badge>
              : <Badge color="red"   style={{ marginLeft:8 }}>실패</Badge>}
            {pipeResult.cache_hit && <Badge color="teal" style={{ marginLeft:6 }}>cache</Badge>}
            <span style={{ fontSize:11, color:'var(--gray)', marginLeft:'auto' }}>{pipeResult.duration_ms}ms</span>
          </CardHeader>
          <CardBody>
            {pipeResult.originalQuestion && (
              <div style={{
                background:'var(--gray3)', borderRadius:6, padding:'8px 12px',
                marginBottom:12, fontSize:13, color:'var(--dark)',
                borderLeft:'3px solid var(--teal)',
              }}>
                💬 {pipeResult.originalQuestion}
              </div>
            )}
            {pipeResult.final_sql ? (
              <>
                <div style={{ display:'flex', alignItems:'center', gap:8, marginBottom:6 }}>
                  <span style={{ fontWeight:600, fontSize:12, color:'var(--gray)' }}>생성된 SQL</span>
                  <Btn size="sm" disabled={sqlRunning} onClick={async () => {
                    setSqlRunning(true); setPipeExecResult(null); setPipeExecError('')
                    try {
                      const data = await queryApi.run(db, pipeResult.final_sql, 500, 'direct')
                      setPipeExecResult(data)
                    } catch (e) {
                      setPipeExecError(e.response?.data?.detail || e.message)
                    } finally { setSqlRunning(false) }
                  }}>
                    {sqlRunning ? <><Spinner /> 실행 중...</> : '▶ SQL 실행'}
                  </Btn>
                </div>
                <pre style={{
                  background:'var(--gray3)', border:'1px solid var(--gray2)', borderRadius:6,
                  padding:'10px 12px', fontFamily:'Consolas, monospace', fontSize:12,
                  overflowX:'auto', whiteSpace:'pre-wrap', margin:0,
                }}>
                  {pipeResult.final_sql}
                </pre>
                {pipeResult.explanation && (
                  <div style={{ marginTop:10, fontSize:13, color:'var(--gray)' }}>
                    💬 {pipeResult.explanation}
                  </div>
                )}
                {pipeExecError && (
                  <Alert type="error" style={{ marginTop:10 }}>⚠ {pipeExecError}</Alert>
                )}
                {pipeExecResult && pipeExecResult.columns?.length > 0 && (
                  <div style={{ marginTop:12 }}>
                    <div style={{ fontWeight:600, fontSize:12, color:'var(--gray)', marginBottom:6 }}>
                      실행 결과 <span style={{ color:'var(--teal)' }}>{pipeExecResult.count.toLocaleString()} rows</span>
                      {pipeExecResult.truncated && (
                        <span style={{ marginLeft:8, fontSize:11, color:'var(--orange)', fontWeight:500 }}>
                          ⚠ 상위 {pipeExecResult.count.toLocaleString()}건만 표시
                        </span>
                      )}
                    </div>
                    <div style={{ overflowX:'auto', maxHeight:360, border:'1px solid var(--gray2)', borderRadius:6 }}>
                      <ResultTable columns={pipeExecResult.columns} rows={pipeExecResult.rows} />
                    </div>
                  </div>
                )}
                {pipeExecResult && pipeExecResult.columns?.length === 0 && (
                  <Alert type="ok" style={{ marginTop:10 }}>✔ 실행 완료 (결과 없음)</Alert>
                )}
              </>
            ) : (
              <Alert type="error">SQL 생성 실패: {pipeResult.error}</Alert>
            )}
          </CardBody>
        </Card>
      )}

      {/* Debug panel */}
      {pipeResult && debugMode && pipeResult.stage_logs && (
        <DebugPanel stageLogs={pipeResult.stage_logs} />
      )}

      {/* Direct SQL result */}
      {result && result.columns.length > 0 && (
        <Card>
          <CardHeader>
            결과
            <span style={{ marginLeft:8, fontSize:12, fontWeight:400, color:'var(--gray)' }}>
              {result.count.toLocaleString()}건
            </span>
            {result.truncated && (
              <span style={{ marginLeft:8, fontSize:11, color:'var(--orange)', fontWeight:500 }}>
                ⚠ 상위 {result.count.toLocaleString()}건만 표시 (결과가 더 있습니다)
              </span>
            )}
          </CardHeader>
          <div style={{ overflowX:'auto', maxHeight:440 }}>
            <ResultTable columns={result.columns} rows={result.rows} />
          </div>
        </Card>
      )}
      {result && result.columns.length === 0 && (
        <Alert type="ok">✔ 실행 완료 (결과 없음)</Alert>
      )}
    </div>
  )
}

// ── Debug Panel ────────────────────────────────────────────────

const STAGE_TABS = ['S1', 'S2', 'S3', 'S4']

function DebugPanel({ stageLogs }) {
  const [tab, setTab] = useState('S1')

  // Index logs by stage prefix
  const byStage = {}
  for (const log of stageLogs) {
    const key = log.stage.startsWith('s1') ? 'S1'
              : log.stage.startsWith('s2') ? 'S2'
              : log.stage.startsWith('s3') ? 'S3'
              : log.stage.startsWith('s4') ? 'S4'
              : log.stage
    if (!byStage[key]) byStage[key] = []
    byStage[key].push(log)
  }

  const tabStyle = (t) => ({
    padding:'5px 14px', borderRadius:6, border:'none', cursor:'pointer',
    fontSize:12, fontWeight:600,
    background: tab===t ? 'var(--navy)' : 'var(--gray2)',
    color: tab===t ? 'white' : 'var(--gray)',
  })

  return (
    <Card>
      <CardHeader>
        🔍 LLM 쿼리 Debug
        <div style={{ display:'flex', gap:6, marginLeft:12 }}>
          {STAGE_TABS.map(t => (
            <button key={t} style={tabStyle(t)} onClick={() => setTab(t)}>
              {t}
              {byStage[t] && (
                <span style={{
                  marginLeft:4, fontSize:10,
                  color: byStage[t].every(l => l.ok) ? '#22c55e' : '#ef4444',
                }}>
                  {byStage[t].every(l => l.ok) ? '✓' : '✗'}
                </span>
              )}
            </button>
          ))}
        </div>
      </CardHeader>
      <CardBody>
        {!byStage[tab] && (
          <span style={{ fontSize:13, color:'var(--gray)' }}>이 단계 정보 없음</span>
        )}
        {byStage[tab]?.map((log, i) => (
          <StageDetail key={i} log={log} stageKey={tab} />
        ))}
      </CardBody>
    </Card>
  )
}

function StageDetail({ log, stageKey }) {
  const d = log.detail || {}
  const ms = log.duration_ms != null ? `${log.duration_ms}ms` : ''

  const headerStyle = {
    display:'flex', alignItems:'center', gap:8,
    marginBottom:10, fontSize:12, color:'var(--gray)',
  }

  if (stageKey === 'S1') {
    return (
      <div>
        <div style={headerStyle}>
          <span style={{ color: log.ok ? '#22c55e' : '#ef4444', fontWeight:700 }}>
            {log.ok ? '✓' : '✗'}
          </span>
          <span style={{ fontWeight:600 }}>S1 Question Understanding</span>
          <span>{ms}</span>
        </div>

        {d.keywords?.length > 0 && (
          <Section title="키워드">
            <div style={{ display:'flex', flexWrap:'wrap', gap:4 }}>
              {d.keywords.map((k,i) => (
                <span key={i} style={{
                  background:'var(--mint)', color:'var(--teal)', borderRadius:4,
                  padding:'2px 8px', fontSize:12, fontWeight:600,
                }}>{k}</span>
              ))}
            </div>
          </Section>
        )}

        {d.tables?.length > 0 && (
          <Section title="후보 테이블">
            <div style={{ display:'flex', flexWrap:'wrap', gap:4 }}>
              {d.tables.map((t,i) => (
                <span key={i} style={{
                  background:'var(--gray3)', color:'var(--dark)', borderRadius:4,
                  padding:'2px 8px', fontSize:12, border:'1px solid var(--gray2)',
                }}>{t}</span>
              ))}
            </div>
          </Section>
        )}

        {d.patterns?.length > 0 && (
          <Section title="매칭 패턴">
            {d.patterns.map((p,i) => (
              <div key={i} style={{
                background:'var(--gray3)', borderRadius:6, padding:'8px 10px',
                marginBottom:6, fontSize:12,
              }}>
                <div style={{ fontWeight:600, marginBottom:2 }}>
                  {p.name}
                  <span style={{ marginLeft:8, color:'var(--gray)', fontWeight:400 }}>
                    score: {p.score}
                  </span>
                </div>
                {p.instruction && (
                  <div style={{ color:'var(--gray)', whiteSpace:'pre-wrap' }}>{p.instruction}</div>
                )}
              </div>
            ))}
          </Section>
        )}
      </div>
    )
  }

  if (stageKey === 'S2') {
    return (
      <div>
        <div style={headerStyle}>
          <span style={{ color: log.ok ? '#22c55e' : '#ef4444', fontWeight:700 }}>
            {log.ok ? '✓' : '✗'}
          </span>
          <span style={{ fontWeight:600 }}>S2 Schema Linking</span>
          <span>{ms}</span>
        </div>

        {d.tables?.length > 0 && (
          <Section title="링크된 테이블">
            {d.tables.map((t,i) => (
              <div key={i} style={{
                background:'var(--gray3)', borderRadius:6, padding:'8px 10px',
                marginBottom:6, fontSize:12,
              }}>
                <span style={{ fontWeight:600 }}>{t.schema}.{t.table}</span>
                {t.columns?.length > 0 && (
                  <div style={{ color:'var(--gray)', marginTop:2 }}>
                    {t.columns.join(', ')}
                  </div>
                )}
              </div>
            ))}
          </Section>
        )}

        {d.join_hint && (
          <Section title="Join Hint">
            <CodeBlock>{d.join_hint}</CodeBlock>
          </Section>
        )}

        {d.schema_prompt && (
          <Section title="Schema Prompt">
            <CodeBlock>{d.schema_prompt}</CodeBlock>
          </Section>
        )}

        {d.rules != null && (
          <Section title="Dialect 규칙">
            <span style={{ fontSize:12, color:'var(--gray)' }}>{d.rules}개 규칙 적용</span>
          </Section>
        )}
      </div>
    )
  }

  if (stageKey === 'S3') {
    const attempt = log.stage.replace('s3_attempt', '')
    return (
      <div style={{ marginBottom: 16 }}>
        <div style={headerStyle}>
          <span style={{ color: log.ok ? '#22c55e' : '#ef4444', fontWeight:700 }}>
            {log.ok ? '✓' : '✗'}
          </span>
          <span style={{ fontWeight:600 }}>S3 SQL Generation — 시도 {attempt}</span>
          <span>{ms}</span>
        </div>

        {d.sql && (
          <Section title="생성된 SQL">
            <CodeBlock>{d.sql}</CodeBlock>
          </Section>
        )}

        {d.explanation && (
          <Section title="설명">
            <div style={{ fontSize:13, color:'var(--gray)' }}>{d.explanation}</div>
          </Section>
        )}

        {d.system_prompt && (
          <Section title="System Prompt" collapsible>
            <CodeBlock>{d.system_prompt}</CodeBlock>
          </Section>
        )}

        {d.user_prompt && (
          <Section title="User Prompt" collapsible>
            <CodeBlock>{d.user_prompt}</CodeBlock>
          </Section>
        )}

        {d.raw_response && (
          <Section title="Raw LLM Response" collapsible>
            <CodeBlock>{d.raw_response}</CodeBlock>
          </Section>
        )}
      </div>
    )
  }

  if (stageKey === 'S4') {
    const attempt = log.stage.replace('s4_attempt', '')
    return (
      <div style={{ marginBottom: 16 }}>
        <div style={headerStyle}>
          <span style={{ color: log.ok ? '#22c55e' : '#ef4444', fontWeight:700 }}>
            {log.ok ? '✓' : '✗'}
          </span>
          <span style={{ fontWeight:600 }}>S4 Validation — 시도 {attempt}</span>
          <span>{ms}</span>
        </div>

        {d.issues?.length > 0 && (
          <Section title="검증 이슈">
            {d.issues.map((iss,i) => (
              <div key={i} style={{
                display:'flex', gap:8, alignItems:'flex-start',
                background: iss.severity === 'error' ? '#fee2e2' : '#fef9c3',
                border: `1px solid ${iss.severity === 'error' ? '#fca5a5' : '#fde047'}`,
                borderRadius:6, padding:'6px 10px', marginBottom:4, fontSize:12,
              }}>
                <span style={{ fontWeight:700, color: iss.severity === 'error' ? '#dc2626' : '#ca8a04' }}>
                  [{iss.severity?.toUpperCase()}]
                </span>
                <div>
                  <span style={{ fontWeight:600 }}>{iss.rule}</span>
                  {iss.message && <div style={{ color:'#555', marginTop:2 }}>{iss.message}</div>}
                </div>
              </div>
            ))}
          </Section>
        )}

        {d.correction_hint && (
          <Section title="수정 힌트">
            <div style={{
              background:'#fff7ed', border:'1px solid #fed7aa', borderRadius:6,
              padding:'8px 10px', fontSize:12, color:'#c2410c',
            }}>
              {d.correction_hint}
            </div>
          </Section>
        )}

        {d.error && (
          <Section title="오류">
            <div style={{ fontSize:12, color:'#dc2626' }}>{d.error}</div>
          </Section>
        )}

        {log.ok && (
          <div style={{ fontSize:12, color:'#22c55e', fontWeight:600 }}>✓ 검증 통과</div>
        )}
      </div>
    )
  }

  return (
    <pre style={{ fontSize:11, color:'var(--gray)', whiteSpace:'pre-wrap' }}>
      {JSON.stringify(log, null, 2)}
    </pre>
  )
}

function Section({ title, children, collapsible }) {
  const [open, setOpen] = useState(!collapsible)
  return (
    <div style={{ marginBottom:10 }}>
      <div
        style={{
          fontWeight:600, fontSize:11, color:'var(--gray)',
          textTransform:'uppercase', letterSpacing:'0.05em',
          marginBottom:4, cursor: collapsible ? 'pointer' : 'default',
          display:'flex', alignItems:'center', gap:4,
        }}
        onClick={collapsible ? () => setOpen(v => !v) : undefined}
      >
        {collapsible && <span>{open ? '▾' : '▸'}</span>}
        {title}
      </div>
      {open && children}
    </div>
  )
}

function CodeBlock({ children }) {
  return (
    <pre style={{
      background:'var(--gray3)', border:'1px solid var(--gray2)', borderRadius:6,
      padding:'8px 10px', fontFamily:'Consolas, monospace', fontSize:11,
      overflowX:'auto', whiteSpace:'pre-wrap', margin:0, maxHeight:300, overflowY:'auto',
    }}>
      {children}
    </pre>
  )
}

// ── History Panel ──────────────────────────────────────────────

function HistoryPanel({ history, err, db, mode, onSelect, onDelete, onClear }) {
  const fmt = (iso) => {
    if (!iso) return ''
    const d = new Date(iso)
    return `${d.getMonth()+1}/${d.getDate()} ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`
  }

  return (
    <Card>
      <CardHeader>
        🕐 실행 이력
        <span style={{ fontSize:11, color:'var(--gray)', fontWeight:400, marginLeft:6 }}>
          클릭하면 입력란에 복사
        </span>
        <Btn size="sm" variant="secondary"
          style={{ marginLeft:'auto', color:'#dc2626', borderColor:'#fca5a5', fontSize:11 }}
          onClick={onClear}>
          전체 삭제
        </Btn>
      </CardHeader>
      {err ? (
        <CardBody><Alert type="error">⚠ {err}</Alert></CardBody>
      ) : history.length === 0 ? (
        <CardBody>
          <Alert type="info">이력이 없습니다. 쿼리를 실행하면 저장됩니다.</Alert>
        </CardBody>
      ) : (
        <div>
          {history.map(h => (
            <div key={h.id} style={{
              display:'flex', alignItems:'center', gap:8,
              padding:'7px 16px', borderTop:'1px solid var(--gray2)', fontSize:12,
            }}>
              <span style={{ fontSize:10, color: h.ok ? 'var(--green)' : 'var(--red)', fontWeight:700, flexShrink:0 }}>
                {h.ok ? '✓' : '✗'}
              </span>
              <span style={{ fontSize:10, color:'var(--gray)', flexShrink:0, width:72 }}>
                {fmt(h.executed_at)}
              </span>
              <span style={{ fontSize:10, flexShrink:0 }}>
                <Badge color={h.mode === 'pipeline' ? 'teal' : 'navy'}>{h.mode === 'pipeline' ? 'LLM 쿼리' : h.mode}</Badge>
              </span>
              <span
                title={h.input_text}
                onClick={() => onSelect(h.input_text)}
                style={{
                  flex:1, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap',
                  cursor:'pointer', color:'var(--dark)',
                  fontFamily: h.mode === 'direct' ? 'Consolas, monospace' : 'inherit',
                }}
                onMouseEnter={e => e.currentTarget.style.color='var(--teal)'}
                onMouseLeave={e => e.currentTarget.style.color='var(--dark)'}
              >
                {h.input_text}
              </span>
              {h.duration_ms != null && (
                <span style={{ fontSize:10, color:'var(--gray)', flexShrink:0 }}>{h.duration_ms}ms</span>
              )}
              <button
                onClick={() => onDelete(h.id)}
                style={{ background:'none', border:'none', cursor:'pointer', color:'var(--gray)', fontSize:13, padding:'0 2px' }}
                title="이력 삭제">✕</button>
            </div>
          ))}
        </div>
      )}
    </Card>
  )
}

// ── Result Table ───────────────────────────────────────────────

function ResultTable({ columns, rows }) {
  return (
    <table style={{ width:'100%', borderCollapse:'collapse', fontSize:12 }}>
      <thead>
        <tr>
          {columns.map(c => (
            <th key={c} style={{
              background:'var(--navy)', color:'white', padding:'8px 10px',
              textAlign:'left', whiteSpace:'nowrap', position:'sticky', top:0, zIndex:1,
            }}>{c}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, i) => (
          <tr key={i} style={{ background: i%2===0 ? 'white' : 'var(--gray3)' }}
            onMouseEnter={e => e.currentTarget.style.background='var(--mint)'}
            onMouseLeave={e => e.currentTarget.style.background=i%2===0?'white':'var(--gray3)'}
          >
            {row.map((v, j) => (
              <td key={j} title={v??''} style={{
                padding:'6px 10px', borderBottom:'1px solid var(--gray2)',
                whiteSpace:'nowrap', maxWidth:320, overflow:'hidden', textOverflow:'ellipsis',
                color: v===null?'var(--gray)':'var(--dark)',
                fontStyle: v===null?'italic':'normal',
              }}>
                {v===null ? 'NULL' : String(v)}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  )
}
