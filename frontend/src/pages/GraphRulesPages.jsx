import { useState, useEffect, useRef, useMemo } from 'react'
import { useDb } from '../components/Layout'
import { graphApi, rulesApi, pgstatApi } from '../api/client'
import { Card, CardHeader, CardBody, Btn, Alert, Badge, CountBadge, Spinner } from '../components/UI'
import QueryPlanModal from '../components/QueryPlanModal'

const TYPE_COLOR = { fk:'green', analyzed:'orange', inferred:'purple', manual:'teal', file:'navy' }

const TREE = {
  indent:   { paddingLeft:24, borderLeft:'2px solid var(--gray2)', marginLeft:10 },
  group:    { display:'flex', alignItems:'center', gap:6, padding:'6px 12px',
              cursor:'pointer', userSelect:'none', background:'var(--gray3)',
              borderTop:'1px solid var(--gray2)', fontSize:12, fontWeight:600 },
  row:      { display:'flex', alignItems:'center', gap:6, padding:'5px 10px',
              borderTop:'1px solid var(--gray2)', fontSize:12, cursor:'pointer' },
}

// ── EdgeRow — 인라인 편집 가능한 관계 행 ──────────────────────
function EdgeRow({ edge, db, showPending, onApprove, onDelete, onUpdate }) {
  const [editing, setEditing] = useState(false)
  const [form, setForm]       = useState({
    from_schema:   edge.from_schema   || '',
    from_table:    edge.from_table    || '',
    from_column:   edge.from_column   || '',
    to_schema:     edge.to_schema     || '',
    to_table:      edge.to_table      || '',
    to_column:     edge.to_column     || '',
    relation_name: edge.relation_name || '',
    relation_type: edge.relation_type || 'analyzed',
    confidence:    edge.confidence    ?? 0.5,
  })
  const [saving, setSaving]       = useState(false)
  const [inferring, setInferring] = useState(false)
  const [err, setErr]             = useState('')
  const set = (k, v) => setForm(f => ({ ...f, [k]: v }))

  const iSt = { padding:'4px 7px', border:'1px solid var(--gray2)', borderRadius:5, fontSize:12, outline:'none', background:'white' }

  const [inferSource, setInferSource] = useState('')

  async function handleInferName() {
    setInferring(true); setErr(''); setInferSource('')
    try {
      const res = await graphApi.inferName(db, edge.id)
      if (res.suggested_name) {
        set('relation_name', res.suggested_name)
        setInferSource(res.source || 'llm')
      }
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
    finally { setInferring(false) }
  }

  async function handleSave() {
    setSaving(true); setErr('')
    try { await onUpdate(edge.id, form); setEditing(false) }
    catch (e) { setErr(e.response?.data?.detail || e.message) }
    finally { setSaving(false) }
  }

  if (editing) {
    return (
      <div style={{ borderTop:'2px solid var(--teal)', background:'#f7fdfc' }}>
        <div style={{ padding:'12px 16px' }}>
          <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:12, marginBottom:10 }}>
            {[['FROM','from'], ['TO','to']].map(([label, pfx]) => (
              <div key={pfx}>
                <div style={{ fontSize:11, fontWeight:700, color: pfx==='from' ? 'var(--teal)' : 'var(--navy)', marginBottom:6 }}>{label}</div>
                <div style={{ display:'flex', gap:6 }}>
                  {[['schema', '0 0 90px'], ['table', 1], ['column', 1]].map(([f, flex]) => (
                    <div key={f} style={{ flex }}>
                      <label style={{ fontSize:10, color:'var(--gray)', display:'block', marginBottom:2 }}>{f}</label>
                      <input style={{ ...iSt, width:'100%' }} value={form[`${pfx}_${f}`]}
                        onChange={e => set(`${pfx}_${f}`, e.target.value)} />
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
          <div style={{ display:'flex', gap:10, alignItems:'flex-end', flexWrap:'wrap' }}>
            <div>
              <label style={{ fontSize:10, color:'var(--gray)', display:'block', marginBottom:2 }}>relation_name</label>
              <div style={{ display:'flex', gap:4 }}>
                <input style={{ ...iSt, width:150 }} value={form.relation_name} placeholder="(선택)"
                  onChange={e => set('relation_name', e.target.value)} />
                {showPending && (
                  <Btn size="sm" variant="secondary" onClick={handleInferName} disabled={inferring}
                    title="LLM으로 relation_name 추론 (캐시 → LLM → 규칙 순서)">
                    {inferring ? <Spinner /> : '💡'}
                  </Btn>
                )}
                {inferSource && (
                  <span style={{ fontSize:10, color:'var(--gray)', alignSelf:'center' }}>
                    {inferSource === 'cache' ? '📦 캐시' : inferSource === 'llm' ? '🤖 LLM' : '📐 규칙'}
                  </span>
                )}
              </div>
            </div>
            <div>
              <label style={{ fontSize:10, color:'var(--gray)', display:'block', marginBottom:2 }}>type</label>
              <select style={{ ...iSt, width:110 }} value={form.relation_type} onChange={e => set('relation_type', e.target.value)}>
                {['fk','analyzed','inferred','manual','file'].map(t => <option key={t}>{t}</option>)}
              </select>
            </div>
            <div>
              <label style={{ fontSize:10, color:'var(--gray)', display:'block', marginBottom:2 }}>confidence</label>
              <input type="number" step="0.01" min="0" max="1" style={{ ...iSt, width:80 }}
                value={form.confidence} onChange={e => set('confidence', Number(e.target.value))} />
            </div>
            <div style={{ display:'flex', gap:6, marginLeft:'auto' }}>
              <Btn size="sm" onClick={handleSave} disabled={saving}>{saving ? '저장 중...' : '💾 저장'}</Btn>
              {showPending && <Btn size="sm" onClick={() => handleSave().then(() => onApprove(edge.id))}>✔ 저장 후 승인</Btn>}
              <Btn size="sm" variant="secondary" onClick={() => { setEditing(false); setErr('') }}>취소</Btn>
            </div>
          </div>
          {err && <div style={{ marginTop:8, fontSize:12, color:'var(--red)' }}>⚠ {err}</div>}
          {edge.source_sql && (
            <details style={{ marginTop:10 }}>
              <summary style={{ fontSize:11, color:'var(--gray)', cursor:'pointer' }}>원본 SQL 보기</summary>
              <pre style={{ marginTop:6, padding:'8px 10px', background:'var(--gray3)', borderRadius:5,
                fontSize:11, fontFamily:'Consolas, monospace', whiteSpace:'pre-wrap', color:'var(--dark)' }}>
                {edge.source_sql}
              </pre>
            </details>
          )}
        </div>
      </div>
    )
  }

  return (
    <div style={{ display:'flex', alignItems:'center', gap:8, padding:'8px 16px',
      borderTop:'1px solid var(--gray2)', fontSize:12, cursor:'pointer' }}
      onClick={() => setEditing(true)} title="클릭하여 편집"
    >
      <Badge color={TYPE_COLOR[edge.relation_type] || 'gray'}>{edge.relation_type}</Badge>
      <span style={{ fontFamily:'monospace', color:'var(--navy)', flex:1 }}>
        {edge.from_schema}.<strong>{edge.from_table}</strong>.{edge.from_column}
        <span style={{ color:'var(--gray)', margin:'0 6px' }}>→</span>
        {edge.to_schema}.<strong>{edge.to_table}</strong>.{edge.to_column}
        {edge.relation_name && (
          <span style={{ marginLeft:6, color:'var(--teal)', fontStyle:'italic' }}>[{edge.relation_name}]</span>
        )}
      </span>
      {edge.is_cross_db && <Badge color="orange">cross-DB</Badge>}
      {edge.call_count > 0 && <span style={{ fontSize:11, color:'var(--gray)' }}>{edge.call_count} calls</span>}
      <span style={{ fontSize:11, color:'var(--gray)' }}>{(edge.confidence * 100).toFixed(0)}%</span>
      <span style={{ fontSize:11, color:'var(--gray)', marginRight:4 }}>✎</span>
      {showPending
        ? <><Btn size="sm" onClick={e => { e.stopPropagation(); onApprove(edge.id) }}>✔ 승인</Btn>
             <Btn size="sm" variant="secondary" onClick={e => { e.stopPropagation(); onDelete(edge.id) }}>✕</Btn></>
        : <Btn size="sm" variant="secondary" onClick={e => { e.stopPropagation(); onDelete(edge.id) }}>✕</Btn>
      }
    </div>
  )
}

// ── PathItem — 트리 내 개별 경로 ──────────────────────────
function PathItem({ path, onDelete }) {
  const [open, setOpen] = useState(false)
  const hopColor = ['var(--green)','var(--teal)','var(--orange)','var(--red)'][Math.min(path.hop_count - 1, 3)]

  return (
    <div style={{ borderTop:'1px solid var(--gray2)' }}>
      <div style={{ ...TREE.row, background:'white' }} onClick={() => setOpen(v => !v)}>
        <span style={{ fontSize:10, color:'var(--gray)', flexShrink:0 }}>└</span>
        <span style={{ fontSize:10, fontWeight:700, color: hopColor, flexShrink:0, minWidth:28 }}>
          {path.hop_count}hop
        </span>
        <span style={{ fontFamily:'monospace', color:'var(--navy)', fontSize:11, flex:1,
          overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}>
          → <strong>{path.to_table}</strong>
        </span>
        {path.is_cross_db && <Badge color="orange">cross-DB</Badge>}
        <span style={{ fontSize:10, color:'var(--gray)', flexShrink:0 }}>
          {path.join_hint || ''}
        </span>
        <Btn variant="danger" size="sm"
          onClick={e => { e.stopPropagation(); onDelete(path.id) }}
          style={{ padding:'1px 6px', fontSize:10 }}>✕</Btn>
        <span style={{ fontSize:10, color:'var(--gray)' }}>{open ? '▲' : '▼'}</span>
      </div>
      {open && (
        <div style={{ padding:'6px 14px 10px', background:'var(--gray3)', fontSize:11 }}>
          {path.path_json?.map((step, i) => (
            <div key={i} style={{ display:'flex', alignItems:'center', gap:6, marginTop: i > 0 ? 4 : 0 }}>
              <span style={{ fontSize:10, color:'var(--gray)', minWidth:16 }}>{i + 1}.</span>
              <span style={{ fontFamily:'monospace', background:'white', border:'1px solid var(--gray2)',
                borderRadius:4, padding:'1px 6px', color:'var(--dark)' }}>
                {step.from_table}<span style={{ color:'var(--gray)' }}>.{step.from_column}</span>
              </span>
              <span style={{ color:'var(--gray)', fontSize:10 }}>=</span>
              <span style={{ fontFamily:'monospace', background:'white', border:'1px solid var(--gray2)',
                borderRadius:4, padding:'1px 6px', color:'var(--dark)' }}>
                {step.to_table}<span style={{ color:'var(--gray)' }}>.{step.to_column}</span>
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

// ── PathGroup — 테이블 단위 경로 그룹 ────────────────────
function PathGroup({ tableName, paths, onDelete }) {
  const [open, setOpen] = useState(true)
  return (
    <div>
      <div style={TREE.group} onClick={() => setOpen(v => !v)}>
        <span style={{ fontSize:10, color:'var(--gray)' }}>{open ? '▼' : '▶'}</span>
        <span style={{ fontSize:13 }}>🗺</span>
        <span style={{ color:'var(--navy)' }}>{tableName}</span>
        <span style={{ fontWeight:400, fontSize:11, color:'var(--gray)', marginLeft:4 }}>({paths.length})</span>
      </div>
      {open && (
        <div style={TREE.indent}>
          {paths.map(p => <PathItem key={p.id} path={p} onDelete={onDelete} />)}
        </div>
      )}
    </div>
  )
}

// ── QueryAnalysisRow — Query별 관계 추론 ─────────────────
function QueryAnalysisRow({ query, db, onDismiss }) {
  const origSql = query.query || ''
  const [open, setOpen]               = useState(false)
  const [editedSql, setEditedSql]     = useState(origSql)
  const [sqlEditing, setSqlEditing]   = useState(false)
  const [paramValues, setParamValues] = useState({})
  const [inferring, setInferring]     = useState(false)
  const [describing, setDescribing]   = useState(false)
  const [candidates, setCandidates]   = useState(null)
  const [parseError, setParseError]   = useState(false)
  const [description, setDescription] = useState('')
  const [cacheSaving, setCacheSaving] = useState(false)
  const [cacheSaved, setCacheSaved]   = useState(false)
  const [relNames, setRelNames]       = useState({})
  const [saved, setSaved]             = useState(new Set())
  const [err, setErr]                 = useState('')

  const paramNums = useMemo(() => {
    const nums = [...editedSql.matchAll(/\$(\d+)/g)].map(m => m[1])
    return [...new Set(nums)].sort((a, b) => +a - +b)
  }, [editedSql])

  const resolvedSql = useMemo(
    () => applyParams(editedSql, paramValues),
    [editedSql, paramValues],
  )

  const setParam = (n, v) => setParamValues(prev => ({ ...prev, [n]: v }))

  async function handleInfer() {
    setInferring(true); setErr(''); setParseError(false)
    try {
      const res = await pgstatApi.infer(db, resolvedSql)
      setParseError(res.parse_error || false)
      setCandidates(res.candidates)
      const init = {}
      res.candidates.forEach((_, i) => { init[i] = '' })
      setRelNames(init)
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
    finally { setInferring(false) }
  }

  async function handleDescribe() {
    setDescribing(true); setErr(''); setCacheSaved(false)
    try {
      const res = await pgstatApi.describe(db, resolvedSql)
      setDescription(res.description)
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
    finally { setDescribing(false) }
  }

  async function handleSaveCache() {
    if (!description) return
    setCacheSaving(true)
    try {
      await pgstatApi.saveCache(db, description, resolvedSql)
      setCacheSaved(true)
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
    finally { setCacheSaving(false) }
  }

  async function handleSaveEdge(c, idx) {
    try {
      await pgstatApi.saveEdge(db, {
        ...c,
        relation_name: relNames[idx] || null,
        source_sql: resolvedSql.substring(0, 500),
      })
      setSaved(prev => new Set([...prev, idx]))
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
  }

  const iSt = { padding:'3px 8px', border:'1px solid var(--gray2)', borderRadius:5, fontSize:12, outline:'none', background:'white' }

  return (
    <Card style={{ marginBottom:6 }}>
      {/* 헤더 */}
      <div style={{ padding:'10px 16px', display:'flex', alignItems:'center', gap:10 }}>
        <span
          style={{ fontSize:10, color:'var(--gray)', flexShrink:0, cursor:'pointer' }}
          onClick={() => setOpen(v => !v)}
        >{open ? '▼' : '▶'}</span>
        <span
          style={{ fontSize:12, fontWeight:700, color:'var(--orange)', flexShrink:0, minWidth:70, cursor:'pointer' }}
          onClick={() => setOpen(v => !v)}
        >
          {query.calls?.toLocaleString()} calls
        </span>
        {query.mean_exec_time != null && (
          <span style={{ fontSize:11, color:'var(--teal)', flexShrink:0, minWidth:70, cursor:'pointer' }}
            onClick={() => setOpen(v => !v)}>
            avg {query.mean_exec_time}ms
          </span>
        )}
        <span
          style={{ fontFamily:'monospace', fontSize:11, color:'var(--dark)', flex:1,
            overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', cursor:'pointer' }}
          onClick={() => setOpen(v => !v)}
        >
          {origSql.replace(/\s+/g, ' ').substring(0, 150)}
        </span>
        {onDismiss && (
          <button
            onClick={onDismiss}
            title="이 항목 숨기기"
            style={{ background:'none', border:'none', cursor:'pointer', color:'var(--gray)',
              fontSize:14, padding:'2px 6px', flexShrink:0, lineHeight:1 }}
          >✕</button>
        )}
      </div>

      {open && (
        <div style={{ borderTop:'1px solid var(--gray2)' }}>

          {/* SQL 편집 영역 */}
          <div style={{ padding:'8px 16px', borderBottom:'1px solid var(--gray2)' }}>
            <div style={{ display:'flex', alignItems:'center', gap:8, marginBottom: sqlEditing ? 6 : 0 }}>
              <span style={{ fontSize:11, color:'var(--gray)', userSelect:'none' }}>
                🔢 SQL
                <span style={{ marginLeft:6, fontSize:10, fontStyle:'italic' }}>
                  (<span style={{ color:'#dc2626', fontWeight:700 }}>$N</span> = pg_stat_statements 플레이스홀더)
                </span>
              </span>
              {!sqlEditing ? (
                <Btn size="sm" variant="secondary" onClick={() => setSqlEditing(true)}>✏ 편집</Btn>
              ) : (
                <>
                  <Btn size="sm" onClick={() => setSqlEditing(false)}>✓ 확인</Btn>
                  <Btn size="sm" variant="secondary" onClick={() => { setEditedSql(origSql); setParamValues({}); setSqlEditing(false) }}>↩ 원본</Btn>
                </>
              )}
              {editedSql !== origSql && !sqlEditing && (
                <Badge color="orange">편집됨</Badge>
              )}
            </div>
            {sqlEditing ? (
              <SqlEditor value={editedSql} onChange={v => { setEditedSql(v); setParamValues({}) }} rows={6} />
            ) : (
              <details style={{ marginTop:4 }}>
                <summary style={{ fontSize:11, color:'var(--gray)', cursor:'pointer', userSelect:'none' }}>SQL 보기</summary>
                <pre style={{ marginTop:6, padding:'10px', background:'var(--gray3)', borderRadius:6,
                  fontSize:11, fontFamily:'Consolas, monospace', whiteSpace:'pre-wrap',
                  color:'var(--dark)', overflowX:'auto', maxHeight:200, overflowY:'auto' }}>
                  {editedSql}
                </pre>
              </details>
            )}

            {/* 파라미터 입력창 */}
            {paramNums.length > 0 && (
              <div style={{ marginTop:8, padding:'8px 10px',
                border:'1px solid #fca5a5', borderRadius:6, background:'#fff5f5' }}>
                <div style={{ fontSize:10, fontWeight:700, color:'#dc2626', marginBottom:6 }}>
                  📥 파라미터 값 입력 (비워두면 $N 그대로 전송)
                </div>
                <div style={{ display:'flex', flexWrap:'wrap', gap:'6px 12px' }}>
                  {paramNums.map(n => (
                    <label key={n} style={{ display:'flex', alignItems:'center', gap:5, fontSize:11 }}>
                      <span style={{ color:'#dc2626', fontWeight:700, fontFamily:'monospace',
                        minWidth:24, flexShrink:0 }}>${n}</span>
                      <input
                        value={paramValues[n] ?? ''}
                        onChange={e => setParam(n, e.target.value)}
                        placeholder="값 입력"
                        style={{ padding:'3px 7px', border:'1px solid #fca5a5',
                          borderRadius:4, fontSize:11, fontFamily:'monospace',
                          width:140, outline:'none', background:'white' }}
                        onFocus={e => e.target.style.borderColor = '#dc2626'}
                        onBlur={e => e.target.style.borderColor = '#fca5a5'}
                      />
                    </label>
                  ))}
                </div>
                {paramNums.some(n => paramValues[n]?.trim()) && (
                  <div style={{ marginTop:6, fontSize:9, color:'#6b7280',
                    fontFamily:'monospace', wordBreak:'break-all' }}>
                    → {resolvedSql.replace(/\s+/g, ' ').substring(0, 200)}
                  </div>
                )}
              </div>
            )}
          </div>

          <div style={{ padding:'12px 16px 16px', display:'flex', flexDirection:'column', gap:14 }}>

            {/* 자연어 질문 생성 */}
            <div>
              <div style={{ display:'flex', alignItems:'center', gap:8, marginBottom:6, flexWrap:'wrap' }}>
                <span style={{ fontSize:12, fontWeight:700, color:'var(--dark)' }}>💬 자연어 질문</span>
                <Btn size="sm" variant="secondary" onClick={handleDescribe} disabled={describing}>
                  {describing ? <><Spinner /> 생성 중...</> : '🤖 자연어 생성'}
                </Btn>
                {description && !cacheSaved && (
                  <Btn size="sm" variant="secondary" onClick={handleSaveCache} disabled={cacheSaving}>
                    {cacheSaving ? <Spinner /> : '💾 캐시 저장'}
                  </Btn>
                )}
                {cacheSaved && <Badge color="green">✔ 캐시 저장됨</Badge>}
              </div>
              {description ? (
                <textarea
                  value={description}
                  onChange={e => { setDescription(e.target.value); setCacheSaved(false) }}
                  style={{ width:'100%', padding:'10px 14px', background:'#f0fdfb',
                    border:'1px solid var(--teal)', borderRadius:6,
                    fontSize:13, color:'var(--dark)', lineHeight:1.7, fontWeight:500,
                    resize:'vertical', outline:'none', boxSizing:'border-box',
                    minHeight:60, fontFamily:'inherit' }}
                />
              ) : (
                <div style={{ padding:'8px 12px', background:'var(--gray3)', borderRadius:6,
                  fontSize:12, color:'var(--gray)', fontStyle:'italic' }}>
                  버튼을 클릭하면 자연어 질문이 여기에 표시됩니다.
                </div>
              )}
            </div>

            {/* 관계 추론 */}
            <div style={{ borderTop:'1px solid var(--gray2)', paddingTop:14 }}>
              <div style={{ display:'flex', alignItems:'center', gap:8, marginBottom:8 }}>
                <span style={{ fontSize:12, fontWeight:700, color:'var(--dark)' }}>🔗 관계 추론</span>
                <Btn onClick={handleInfer} disabled={inferring}>
                  {inferring ? <><Spinner /> 추론 중...</> : '🔍 관계 추론 실행'}
                </Btn>
                <span style={{ fontSize:11, color:'var(--gray)' }}>JOIN 조건에서 edge 후보를 추출합니다</span>
              </div>

              {err && <Alert type="error" style={{ marginBottom:8 }}>⚠ {err}</Alert>}

              {candidates !== null && (
                parseError ? (
                  <Alert type="error">
                    ⚠ SQL 파싱 불가 — SQL 편집 후 다시 시도하세요. (JOIN 조건 형식: <code>T1.col = T2.col</code>)
                  </Alert>
                ) : candidates.length === 0 ? (
                  <Alert type="info">추출된 관계가 없습니다. JOIN 조건(T1.col = T2.col)이 있는지 확인하세요.</Alert>
                ) : (
                  <div style={{ border:'1px solid var(--gray2)', borderRadius:6, overflow:'hidden' }}>
                    <div style={{ padding:'6px 12px', background:'var(--gray3)',
                      fontSize:11, fontWeight:700, color:'var(--gray)' }}>
                      추출된 관계 {candidates.length}개
                    </div>
                    {candidates.map((c, i) => {
                      const isSaved    = saved.has(i)
                      const isExisting = c.already_saved
                      const isUnver    = c.unverified
                      return (
                        <div key={i} style={{
                          padding:'10px 12px',
                          borderTop: i > 0 ? '1px solid var(--gray2)' : 'none',
                          background: isExisting ? '#f8fff8' : isUnver ? '#fffbf0' : 'white',
                        }}>
                          <div style={{ display:'flex', alignItems:'center', gap:8, marginBottom: (isExisting && !isUnver) ? 0 : 6 }}>
                            <span style={{ fontFamily:'monospace', flex:1, fontSize:12, color:'var(--navy)' }}>
                              <strong>{c.from_table}</strong>
                              <span style={{ color:'var(--gray)' }}>.{c.from_column}</span>
                              <span style={{ color:'var(--gray)', margin:'0 6px' }}>→</span>
                              <strong>{c.to_table}</strong>
                              <span style={{ color:'var(--gray)' }}>.{c.to_column}</span>
                            </span>
                            <span style={{ fontSize:11, color:'var(--gray)', flexShrink:0 }}>
                              {(c.confidence * 100).toFixed(0)}%
                            </span>
                            {isUnver && (
                              <Badge color="orange" title="schema_catalog에 등록되지 않은 테이블/컬럼입니다">
                                ⚠ 미검증
                              </Badge>
                            )}
                            {isExisting ? (
                              <Badge color={c.approved ? 'green' : 'orange'}>
                                {c.approved ? '✔ 승인됨' : '⏳ pending'}
                              </Badge>
                            ) : isSaved ? (
                              <Badge color="green">저장됨</Badge>
                            ) : (
                              <Btn size="sm" onClick={() => handleSaveEdge(c, i)}>💾 저장</Btn>
                            )}
                          </div>
                          {!isExisting && !isSaved && (
                            <div style={{ display:'flex', alignItems:'center', gap:6 }}>
                              <span style={{ fontSize:11, color:'var(--gray)', flexShrink:0, minWidth:90 }}>
                                relation_name
                              </span>
                              <input
                                value={relNames[i] || ''}
                                onChange={e => setRelNames(p => ({ ...p, [i]: e.target.value }))}
                                placeholder="(선택) has_rentals, belongs_to_customer …"
                                style={{ flex:1, maxWidth:280, ...iSt }}
                              />
                            </div>
                          )}
                        </div>
                      )
                    })}
                  </div>
                )
              )}
            </div>

          </div>
        </div>
      )}
    </Card>
  )
}

// ── SqlEditor — textarea + 하이라이트 오버레이 ──────────────
// $1, $2 등 파라미터 플레이스홀더를 빨간색으로 강조
function hlSql(text) {
  return text
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/(\$\d+)/g, '<mark style="background:transparent;color:#dc2626;font-weight:700">$1</mark>')
    + '\n'   // 마지막 줄 클리핑 방지
}

const SHARED = {
  margin: 0, padding: '8px',
  fontFamily: 'Consolas, monospace', fontSize: 12, lineHeight: 1.5,
  whiteSpace: 'pre-wrap', wordBreak: 'break-all',
  boxSizing: 'border-box', width: '100%',
}

function SqlEditor({ value, onChange, rows = 8 }) {
  const taRef  = useRef(null)
  const bgRef  = useRef(null)

  function syncScroll() {
    if (bgRef.current && taRef.current) {
      bgRef.current.scrollTop  = taRef.current.scrollTop
      bgRef.current.scrollLeft = taRef.current.scrollLeft
    }
  }

  return (
    <div style={{ position:'relative', marginTop:4,
      border:'1px solid var(--gray2)', borderRadius:6, background:'white' }}>
      {/* 하이라이트 레이어 (뒤) */}
      <div
        ref={bgRef}
        aria-hidden
        dangerouslySetInnerHTML={{ __html: hlSql(value) }}
        style={{ ...SHARED, position:'absolute', inset:0,
          overflow:'hidden', color:'#374151', pointerEvents:'none' }}
      />
      {/* 편집 레이어 (앞, 투명) */}
      <textarea
        ref={taRef}
        value={value}
        onChange={e => onChange(e.target.value)}
        onScroll={syncScroll}
        rows={rows}
        spellCheck={false}
        style={{ ...SHARED, display:'block', position:'relative',
          border:'none', outline:'none', resize:'vertical',
          background:'transparent', color:'transparent', caretColor:'#374151' }}
      />
    </div>
  )
}

// ── $N 값 치환 헬퍼 ──────────────────────────────────────────
// 숫자·bool·null은 그대로, 나머지는 single-quote로 감싸고 내부 ' → ''
function applyParams(sql, paramValues) {
  return sql.replace(/\$(\d+)/g, (orig, n) => {
    const v = (paramValues[n] ?? '').trim()
    if (!v) return orig
    if (/^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(v)) return v
    if (/^(true|false|null)$/i.test(v)) return v.toLowerCase()
    return `'${v.replace(/'/g, "''")}'`
  })
}

// ── QueryTuneRow — Query별 LLM 튜닝 ──────────────────────
function QueryTuneRow({ query, db, onDismiss }) {
  const [open, setOpen]             = useState(false)
  const [sql, setSql]               = useState(query.query || '')
  const [paramValues, setParamValues] = useState({})   // { '1': '...', '2': '...' }
  const [tuning, setTuning]         = useState(false)
  const [suggestion, setSuggestion] = useState('')
  const [err, setErr]               = useState('')
  const [planLoading, setPlanLoading] = useState(false)
  const [planData, setPlanData]       = useState(null)
  const [showPlan, setShowPlan]       = useState(false)

  // SQL에서 $N 파라미터 번호를 순서대로 추출 (중복 제거)
  const paramNums = useMemo(() => {
    const nums = [...sql.matchAll(/\$(\d+)/g)].map(m => m[1])
    return [...new Set(nums)].sort((a, b) => +a - +b)
  }, [sql])

  // 파라미터 값이 입력된 SQL (API 호출 시 사용)
  const resolvedSql = useMemo(
    () => applyParams(sql, paramValues),
    [sql, paramValues],
  )

  const setParam = (n, v) => setParamValues(prev => ({ ...prev, [n]: v }))

  async function handleTune() {
    setTuning(true); setErr('')
    try {
      const res = await pgstatApi.tune(db, resolvedSql)
      setSuggestion(res.suggestion)
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
    finally { setTuning(false) }
  }

  async function handlePlan(analyze = false) {
    setPlanLoading(true); setErr('')
    try {
      const res = await pgstatApi.plan(db, resolvedSql, analyze)
      setPlanData(res)
      setShowPlan(true)
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
    finally { setPlanLoading(false) }
  }

  return (
    <>
      {showPlan && planData && (
        <QueryPlanModal
          plan={planData.plan}
          planningTime={planData.planning_time}
          executionTime={planData.execution_time}
          analyzed={planData.analyzed}
          onClose={() => setShowPlan(false)}
          sql={resolvedSql}
        />
      )}
      <Card style={{ marginBottom:6 }}>
        <div
          style={{ padding:'10px 16px', display:'flex', alignItems:'center', gap:10 }}
        >
          <span style={{ fontSize:10, color:'var(--gray)', flexShrink:0, cursor:'pointer' }}
            onClick={() => setOpen(v => !v)}>{open ? '▼' : '▶'}</span>
          <span style={{ fontSize:12, fontWeight:700, color:'var(--orange)', flexShrink:0, minWidth:70, cursor:'pointer' }}
            onClick={() => setOpen(v => !v)}>
            {query.calls?.toLocaleString()} calls
          </span>
          {query.mean_exec_time != null && (
            <span style={{ fontSize:11, color:'var(--teal)', flexShrink:0, minWidth:70, cursor:'pointer' }}
              onClick={() => setOpen(v => !v)}>
              avg {query.mean_exec_time}ms
            </span>
          )}
          <span style={{ fontFamily:'monospace', fontSize:11, color:'var(--dark)', flex:1,
            overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', cursor:'pointer' }}
            onClick={() => setOpen(v => !v)}>
            {(query.query || '').replace(/\s+/g, ' ').substring(0, 150)}
          </span>
          {onDismiss && (
            <button
              onClick={onDismiss}
              title="이 항목 숨기기"
              style={{ background:'none', border:'none', cursor:'pointer', color:'var(--gray)',
                fontSize:14, padding:'2px 6px', flexShrink:0, lineHeight:1 }}
            >✕</button>
          )}
        </div>

        {open && (
          <div style={{ padding:'12px 16px 16px', borderTop:'1px solid var(--gray2)' }}>
            <label style={{ fontSize:11, fontWeight:600, color:'var(--gray)' }}>
              Query Text (편집 가능 ·
              <span style={{ color:'#dc2626', fontWeight:700 }}> $N </span>
              = 파라미터 플레이스홀더)
            </label>
            <SqlEditor value={sql} onChange={v => { setSql(v); setParamValues({}) }} rows={8} />

            {/* ── 파라미터 입력창 ── */}
            {paramNums.length > 0 && (
              <div style={{ marginTop:8, padding:'8px 10px',
                border:'1px solid #fca5a5', borderRadius:6, background:'#fff5f5' }}>
                <div style={{ fontSize:10, fontWeight:700, color:'#dc2626', marginBottom:6 }}>
                  📥 파라미터 값 입력 (비워두면 $N 그대로 전송)
                </div>
                <div style={{ display:'flex', flexWrap:'wrap', gap:'6px 12px' }}>
                  {paramNums.map(n => (
                    <label key={n} style={{ display:'flex', alignItems:'center', gap:5, fontSize:11 }}>
                      <span style={{ color:'#dc2626', fontWeight:700, fontFamily:'monospace',
                        minWidth:24, flexShrink:0 }}>${n}</span>
                      <input
                        value={paramValues[n] ?? ''}
                        onChange={e => setParam(n, e.target.value)}
                        placeholder="값 입력"
                        style={{ padding:'3px 7px', border:'1px solid #fca5a5',
                          borderRadius:4, fontSize:11, fontFamily:'monospace',
                          width:140, outline:'none', background:'white' }}
                        onFocus={e => e.target.style.borderColor = '#dc2626'}
                        onBlur={e => e.target.style.borderColor = '#fca5a5'}
                      />
                    </label>
                  ))}
                </div>
                {paramNums.some(n => paramValues[n]?.trim()) && (
                  <div style={{ marginTop:6, fontSize:9, color:'#6b7280',
                    fontFamily:'monospace', wordBreak:'break-all' }}>
                    → {resolvedSql.replace(/\s+/g, ' ').substring(0, 200)}
                  </div>
                )}
              </div>
            )}

            <div style={{ display:'flex', gap:8, marginTop:8, marginBottom:10, flexWrap:'wrap' }}>
              <Btn onClick={handleTune} disabled={tuning || planLoading}>
                {tuning ? <><Spinner /> 분석 중...</> : '🔧 튜닝 제안 받기'}
              </Btn>
              <Btn variant="navy" onClick={() => handlePlan(false)} disabled={planLoading || tuning}>
                {planLoading ? <><Spinner /> 로딩...</> : '📊 Plan 보기'}
              </Btn>
              <Btn variant="blue"
                onClick={() => {
                  if (window.confirm('EXPLAIN ANALYZE는 쿼리를 실제로 실행합니다.\n(결과는 롤백되어 DB에 영향 없음)\n계속하시겠습니까?'))
                    handlePlan(true)
                }}
                disabled={planLoading || tuning}>
                {planLoading ? <><Spinner /> 측정 중...</> : '⏱ Plan + 시간 측정'}
              </Btn>
            </div>

            {err && <Alert type="error" style={{ marginBottom:8 }}>⚠ {err}</Alert>}

            {suggestion && (
              <div style={{ border:'1px solid var(--gray2)', borderRadius:6, overflow:'hidden' }}>
                <div style={{ padding:'6px 12px', background:'var(--gray3)', fontSize:11,
                  fontWeight:700, color:'var(--gray)' }}>🔧 튜닝 제안</div>
                <div style={{ padding:'12px 14px', fontSize:12, whiteSpace:'pre-wrap',
                  lineHeight:1.7, color:'var(--dark)', fontFamily:'inherit' }}>
                  {suggestion}
                </div>
              </div>
            )}
          </div>
        )}
      </Card>
    </>
  )
}

// ── 공통: 쿼리 목록 컨트롤 카드 ─────────────────────────
function PgStatControls({ params, setParams, showParams, setShowParams,
                          onCollect, onRefresh, onReset, collecting, loading,
                          queryCount, msg, err, showCollect }) {
  const inputSt = { padding:'5px 8px', border:'1px solid var(--gray2)', borderRadius:6, fontSize:12, outline:'none' }
  const paramDefs = showCollect
    ? [
        { label:'TOP N queries',        key:'top',             type:'number', w:90 },
        { label:'min_calls',             key:'min_calls',       type:'number', w:90 },
        { label:'자동승인 confidence ≥', key:'auto_approve_at', type:'number', step:'0.01', min:0, max:1, w:90 },
      ]
    : [
        { label:'TOP N queries', key:'top',       type:'number', w:90 },
        { label:'min_calls',     key:'min_calls', type:'number', w:90 },
      ]

  return (
    <Card style={{ marginBottom:12 }}>
      <CardBody>
        <div style={{ display:'flex', gap:8, alignItems:'center', flexWrap:'wrap' }}>
          {showCollect && (
            <Btn size="sm" onClick={onCollect} disabled={collecting || loading}>
              {collecting ? <><Spinner /> 수집 중...</> : '🔍 pg_stat 수집 실행'}
            </Btn>
          )}
          <Btn variant="secondary" size="sm" onClick={onRefresh} disabled={loading}>
            {loading ? <Spinner /> : '↻'} 쿼리 목록 새로고침
          </Btn>
          <Btn variant="secondary" size="sm" onClick={() => setShowParams(s => !s)}>⚙ 파라미터</Btn>
          {onReset && (
            <Btn variant="red" size="sm" style={{ marginLeft:'auto' }}
              onClick={() => {
                if (window.confirm('pg_stat_statements를 초기화하시겠습니까?\n현재 DB의 모든 쿼리 통계가 삭제됩니다.'))
                  onReset()
              }}
            >
              🗑 pg_stat 초기화
            </Btn>
          )}
          {queryCount > 0 && (
            <span style={{ fontSize:12, color:'var(--gray)', marginLeft:4 }}>{queryCount}개 쿼리</span>
          )}
        </div>

        {showParams && (
          <div style={{ display:'flex', gap:12, marginTop:12, alignItems:'flex-end', flexWrap:'wrap' }}>
            {paramDefs.map(({ label, key, w, ...rest }) => (
              <div key={key}>
                <label style={{ fontSize:11, fontWeight:600, color:'var(--gray)', display:'block', marginBottom:3 }}>{label}</label>
                <input {...rest} style={{ ...inputSt, width:w }}
                  value={params[key]} onChange={e => setParams(p => ({ ...p, [key]: Number(e.target.value) }))} />
              </div>
            ))}
          </div>
        )}

        {msg && <Alert type="ok"    style={{ marginTop:10 }}>{msg}</Alert>}
        {err && <Alert type="error" style={{ marginTop:10 }}>{err}</Alert>}
      </CardBody>
    </Card>
  )
}

function PgStatEmptyState() {
  return (
    <Card><CardBody>
      <Alert type="info">
        pg_stat_statements에 JOIN 쿼리가 없거나 min_calls 기준에 맞는 쿼리가 없습니다.
        파라미터를 조정하거나 "↻ 쿼리 목록 새로고침"을 실행하세요.
      </Alert>
    </CardBody></Card>
  )
}

// ── Query 분석 페이지 ─────────────────────────────────────
export function PgStatAnalyzePage() {
  const { db } = useDb()
  const [queries, setQueries]       = useState([])
  const [loading, setLoading]       = useState(false)
  const [collecting, setCollecting] = useState(false)
  const [params, setParams]         = useState({ top: 100, min_calls: 5, auto_approve_at: 0.95 })
  const [showParams, setShowParams] = useState(false)
  const [msg, setMsg]               = useState('')
  const [err, setErr]               = useState('')

  async function loadQueries() {
    if (!db) return
    setLoading(true); setErr('')
    try {
      setQueries(await pgstatApi.queries(db, { top: params.top, min_calls: params.min_calls }))
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
    finally { setLoading(false) }
  }

  async function handleCollect() {
    setCollecting(true); setMsg(''); setErr('')
    try {
      const res = await pgstatApi.collect(db, params)
      setMsg(`✅ 수집 완료 — 후보 ${res.candidates}개, 저장 ${res.saved}개, 자동승인 ${res.auto_approved}개`)
      await loadQueries()
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
    finally { setCollecting(false) }
  }

  async function handleReset() {
    setMsg(''); setErr('')
    try {
      await pgstatApi.reset(db)
      setMsg('✅ pg_stat_statements 초기화 완료')
      setQueries([])
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
  }

  useEffect(() => { if (db) loadQueries() }, [db])

  if (!db) return <Alert type="info">상단에서 DB를 선택하세요.</Alert>

  return (
    <div>
      <PgStatControls
        params={params} setParams={setParams}
        showParams={showParams} setShowParams={setShowParams}
        onCollect={handleCollect} onRefresh={loadQueries} onReset={handleReset}
        collecting={collecting} loading={loading}
        queryCount={queries.length} msg={msg} err={err}
        showCollect={true}
      />
      {loading && <Card><CardBody style={{ color:'var(--gray)' }}>로딩 중...</CardBody></Card>}
      {!loading && queries.length === 0 && <PgStatEmptyState />}
      {!loading && queries.map((q, i) => (
        <QueryAnalysisRow
          key={q.queryid || i} query={q} db={db}
          onDismiss={() => setQueries(qs => qs.filter((_, j) => j !== i))}
        />
      ))}
    </div>
  )
}

// ── Query 튜닝 페이지 ─────────────────────────────────────
export function PgStatTunePage() {
  const { db } = useDb()
  const [queries, setQueries]   = useState([])
  const [loading, setLoading]   = useState(false)
  const [params, setParams]     = useState({ top: 100, min_calls: 5 })
  const [showParams, setShowParams] = useState(false)
  const [err, setErr]           = useState('')

  const [msg, setMsg] = useState('')

  async function loadQueries() {
    if (!db) return
    setLoading(true); setErr('')
    try {
      setQueries(await pgstatApi.queries(db, { top: params.top, min_calls: params.min_calls }))
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
    finally { setLoading(false) }
  }

  async function handleReset() {
    setMsg(''); setErr('')
    try {
      await pgstatApi.reset(db)
      setMsg('✅ pg_stat_statements 초기화 완료')
      setQueries([])
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
  }

  useEffect(() => { if (db) loadQueries() }, [db])

  if (!db) return <Alert type="info">상단에서 DB를 선택하세요.</Alert>

  return (
    <div>
      <PgStatControls
        params={params} setParams={setParams}
        showParams={showParams} setShowParams={setShowParams}
        onCollect={null} onRefresh={loadQueries} onReset={handleReset}
        collecting={false} loading={loading}
        queryCount={queries.length} msg={msg} err={err}
        showCollect={false}
      />
      {loading && <Card><CardBody style={{ color:'var(--gray)' }}>로딩 중...</CardBody></Card>}
      {!loading && queries.length === 0 && <PgStatEmptyState />}
      {!loading && queries.map((q, i) => (
        <QueryTuneRow
          key={q.queryid || i} query={q} db={db}
          onDismiss={() => setQueries(qs => qs.filter((_, j) => j !== i))}
        />
      ))}
    </div>
  )
}

// ── Graph Page ────────────────────────────────────────────
export function GraphPage() {
  const { db } = useDb()
  const [tab, setTab]               = useState('edges')   // 'edges' | 'paths'
  const [edges, setEdges]           = useState([])
  const [paths, setPaths]           = useState([])
  const [loading, setLoading]       = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [filter, setFilter]         = useState('')
  const [showPending, setShowPending] = useState(true)
  const [msg, setMsg]               = useState('')
  const [err, setErr]               = useState('')

  const load = () => {
    if (!db) return
    setLoading(true)
    graphApi.edges(db)
      .then(data => { setEdges(data); setLoading(false) })
      .catch(() => setLoading(false))
  }

  const loadPaths = () => {
    if (!db) return
    setLoading(true)
    graphApi.paths(db, filter ? { from_table: filter } : {})
      .then(data => { setPaths(data); setLoading(false) })
      .catch(() => setLoading(false))
  }

  useEffect(() => { load() }, [db])
  useEffect(() => { if (tab === 'paths') loadPaths() }, [tab, db])

  if (!db) return <Alert type="info">상단에서 DB를 선택하세요.</Alert>

  const pending  = edges.filter(e => !e.approved)
  const approved = edges.filter(e => e.approved)

  async function handleRefreshPaths() {
    setRefreshing(true); setMsg(''); setErr('')
    try {
      const res = await graphApi.refreshPaths(db)
      setMsg(`✅ 경로 재계산 완료 — ${res.paths}개 경로 생성`)
      loadPaths()
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
    finally { setRefreshing(false) }
  }

  async function handleApprove(edgeId) {
    try {
      await graphApi.approve(db, edgeId)
      setEdges(prev => prev.map(e => e.id === edgeId ? { ...e, approved: true } : e))
      setMsg('✅ 승인 완료 — 경로에 반영하려면 "🔄 경로 재계산"을 실행하세요.')
    } catch (e) { setErr(e.message) }
  }

  async function handleDeletePaths() {
    if (!window.confirm('계산된 경로(graph_paths)를 전체 삭제합니다. 계속할까요?')) return
    try {
      const res = await graphApi.deletePaths(db)
      setMsg(`🗑 경로 ${res.deleted}개 삭제됨`)
      setPaths([])
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
  }

  async function handleDeletePath(pathId) {
    try {
      await graphApi.deletePath(db, pathId)
      setPaths(prev => prev.filter(p => p.id !== pathId))
    } catch (e) { setErr(e.response?.data?.detail || e.message) }
  }

  async function handleDelete(edgeId) {
    try {
      await graphApi.deleteEdge(db, edgeId)
      setEdges(prev => prev.filter(e => e.id !== edgeId))
    } catch (e) { setErr(e.message) }
  }

  async function handleUpdate(edgeId, data) {
    await graphApi.updateEdge(db, edgeId, data)
    setEdges(prev => prev.map(e => e.id === edgeId ? { ...e, ...data } : e))
  }

  return (
    <div>
      {msg && <Alert type="ok" style={{ marginBottom:10 }}>{msg}</Alert>}
      {err && <Alert type="error" style={{ marginBottom:10 }}>{err}</Alert>}

      {/* 탭 — Edges / Paths */}
      <Card>
        <CardHeader>
          {/* 탭 버튼 */}
          {[
            { key:'edges', label:'🔗 관계 (edges)', count: edges.length },
            { key:'paths', label:'🗺 경로 (paths)', count: paths.length },
          ].map(t => (
            <button key={t.key} onClick={() => setTab(t.key)} style={{
              padding:'4px 14px', borderRadius:6, border:'none', cursor:'pointer', fontSize:12, fontWeight:600, marginRight:4,
              background: tab===t.key ? 'var(--navy)' : 'var(--gray2)',
              color: tab===t.key ? 'white' : 'var(--gray)',
            }}>
              {t.label} <CountBadge>{t.count}</CountBadge>
            </button>
          ))}

          <span style={{ fontSize:11, color:'var(--gray)', marginLeft:'auto', fontWeight:400 }}>
            {tab === 'edges' ? '행을 클릭하면 편집' : '파이프라인 S2에서 사용하는 JOIN 힌트'}
          </span>
          <Btn variant="secondary" size="sm" onClick={load} style={{ marginLeft:8 }}>↻ 새로고침</Btn>
        </CardHeader>

        {/* 필터 */}
        <CardBody style={{ paddingBottom:0 }}>
          <input value={filter}
            onChange={e => { setFilter(e.target.value) }}
            onKeyDown={e => e.key === 'Enter' && tab === 'paths' && loadPaths()}
            placeholder={tab === 'edges' ? '테이블/컬럼으로 필터...' : '테이블명으로 필터 (Enter)'}
            style={{ width:'100%', padding:'7px 10px', border:'1px solid var(--gray2)', borderRadius:6, fontSize:13, outline:'none' }}
            onFocus={e => e.target.style.borderColor='var(--teal)'}
            onBlur={e => e.target.style.borderColor='var(--gray2)'}
          />
        </CardBody>

        {loading && <div style={{ padding:16, color:'var(--gray)' }}>로딩 중...</div>}

        {/* Edges 탭 — pending/approved 토글 */}
        {tab === 'edges' && !loading && (
          <div style={{ display:'flex', borderBottom:'1px solid var(--gray2)' }}>
            {[
              { label:'pending',  count: pending.length,  active: showPending,  color:'var(--orange)', onClick:() => setShowPending(true) },
              { label:'approved', count: approved.length, active: !showPending, color:'var(--green)',  onClick:() => setShowPending(false) },
            ].map(({ label, count, active, color, onClick }) => (
              <button key={label} onClick={onClick} style={{
                flex:1, padding:'8px 0', border:'none', cursor:'pointer', fontSize:12, fontWeight:600,
                background: active ? color : 'var(--gray3)',
                color: active ? 'white' : 'var(--gray)',
                borderBottom: active ? `3px solid ${color}` : '3px solid transparent',
              }}>
                {label} <CountBadge>{count}</CountBadge>
              </button>
            ))}
          </div>
        )}

        {/* Edges 탭 — 평면 리스트 */}
        {tab === 'edges' && !loading && (() => {
          const list = (showPending ? pending : approved).filter(e =>
            !filter || [e.from_table, e.to_table, e.from_column, e.to_column].some(v => v?.includes(filter))
          )
          if (list.length === 0)
            return <CardBody><Alert type="info">{showPending ? 'pending 관계 없음.' : 'approved 관계 없음.'}</Alert></CardBody>
          return list.map(e => (
            <EdgeRow key={e.id} edge={e} db={db} showPending={showPending}
              onApprove={handleApprove} onDelete={handleDelete} onUpdate={handleUpdate} />
          ))
        })()}

        {/* Paths 탭 — 경로 재계산 / 삭제 툴바 */}
        {tab === 'paths' && !loading && (
          <CardBody style={{ paddingBottom:0, borderBottom:'1px solid var(--gray2)' }}>
            <div style={{ display:'flex', gap:8, alignItems:'center' }}>
              <Btn
                onClick={handleRefreshPaths}
                disabled={refreshing}
                title="승인된 edges로 BFS 경로를 재계산합니다."
              >
                {refreshing ? <><Spinner /> 계산 중...</> : '🔄 경로 재계산'}
              </Btn>
              <Btn variant="danger" size="sm" onClick={handleDeletePaths}>
                🗑 경로 전체 삭제
              </Btn>
              <span style={{ fontSize:11, color:'var(--gray)', marginLeft:4 }}>
                파이프라인 JOIN 힌트에 즉시 반영됩니다
              </span>
            </div>
          </CardBody>
        )}

        {/* Paths 탭 — 테이블별 트리 */}
        {tab === 'paths' && !loading && (() => {
          if (paths.length === 0)
            return <CardBody><Alert type="info">경로가 없습니다. "🔄 경로 재계산"을 실행하세요.</Alert></CardBody>
          const groups = {}
          for (const p of paths) {
            const key = p.from_table || p.from_address?.split('.').pop() || '?'
            if (!groups[key]) groups[key] = []
            groups[key].push(p)
          }
          return Object.keys(groups).sort().map(tbl => (
            <PathGroup key={tbl} tableName={tbl} paths={groups[tbl]} onDelete={handleDeletePath} />
          ))
        })()}
      </Card>
    </div>
  )
}

// ── Rules Page ────────────────────────────────────────────
const EMPTY_RULE = {
  rule_id: '', scope: 'global', instruction: '',
  forbidden_funcs: '', forbidden_sql_patterns: '', required_func: '',
  example_bad: '', example_good: '', severity: 'warning',
  table_name: '', column_name: '',
}

// rule 객체 → 폼 상태로 변환 (배열 필드를 쉼표 구분 문자열로)
function ruleToForm(r) {
  const toStr = (v) => {
    if (!v) return ''
    if (Array.isArray(v)) return v.join(', ')
    if (typeof v === 'string') { try { return JSON.parse(v).join(', ') } catch { return v } }
    return String(v)
  }
  return {
    rule_id:                r.rule_id || '',
    scope:                  r.scope || 'global',
    instruction:            r.instruction || '',
    forbidden_funcs:        toStr(r.forbidden_funcs),
    forbidden_sql_patterns: toStr(r.forbidden_sql_patterns),
    required_func:          r.required_func || '',
    example_bad:            r.example_bad || '',
    example_good:           r.example_good || '',
    severity:               r.severity || 'warning',
    table_name:             r.table_name || '',
    column_name:            r.column_name || '',
  }
}

export function RulesPage() {
  const { db } = useDb()
  const [rules, setRules]       = useState([])
  const [loading, setLoading]   = useState(false)
  const [showForm, setShowForm] = useState(false)  // 신규 추가 폼
  const [editingId, setEditingId] = useState(null) // 편집 중인 rule_id
  const [form, setForm]         = useState(EMPTY_RULE)
  const [saving, setSaving]     = useState(false)
  const [error, setError]       = useState('')

  const load = () => {
    if (!db) return
    setLoading(true)
    rulesApi.list(db)
      .then(data => { setRules(data); setLoading(false) })
      .catch(() => setLoading(false))
  }

  useEffect(() => { load() }, [db])

  if (!db) return <Alert type="info">상단에서 DB를 선택하세요.</Alert>

  const openAdd = () => {
    setEditingId(null); setForm(EMPTY_RULE); setError('')
    setShowForm(v => !v)
  }

  const openEdit = (r) => {
    setShowForm(false)
    if (editingId === r.rule_id) { setEditingId(null); return }
    setEditingId(r.rule_id)
    setForm(ruleToForm(r))
    setError('')
  }

  const closeEdit = () => { setEditingId(null); setError('') }

  const buildPayload = () => ({
    ...form,
    db_alias: form.scope === 'global' ? null : db,
    forbidden_funcs: form.forbidden_funcs
      ? form.forbidden_funcs.split(',').map(s => s.trim()).filter(Boolean)
      : [],
    forbidden_sql_patterns: form.forbidden_sql_patterns
      ? form.forbidden_sql_patterns.split(',').map(s => s.trim()).filter(Boolean)
      : [],
  })

  const handleSave = async () => {
    if (!form.rule_id.trim()) { setError('rule_id를 입력하세요.'); return }
    if (!form.instruction.trim()) { setError('instruction을 입력하세요.'); return }
    setSaving(true); setError('')
    try {
      await rulesApi.create(db, buildPayload())
      setShowForm(false); setForm(EMPTY_RULE); load()
    } catch (e) {
      setError(e.response?.data?.detail || e.message)
    } finally { setSaving(false) }
  }

  const handleUpdate = async () => {
    if (!form.instruction.trim()) { setError('instruction을 입력하세요.'); return }
    setSaving(true); setError('')
    try {
      await rulesApi.create(db, buildPayload())  // overwrite=True로 동일 엔드포인트 사용
      setEditingId(null); load()
    } catch (e) {
      setError(e.response?.data?.detail || e.message)
    } finally { setSaving(false) }
  }

  const handleToggle = async (ruleId, enabled) => {
    try { await rulesApi.toggle(db, ruleId, !enabled); load() }
    catch (e) { alert(e.response?.data?.detail || e.message) }
  }

  const handleDelete = async (ruleId) => {
    if (!confirm(`"${ruleId}" 규칙을 삭제하시겠습니까?`)) return
    try { await rulesApi.remove(db, ruleId); if (editingId === ruleId) setEditingId(null); load() }
    catch (e) { alert(e.response?.data?.detail || e.message) }
  }

  return (
    <div>
      <Card>
        <CardHeader>
          📐 Dialect Rules
          <CountBadge>{rules.length} rules</CountBadge>
          <button onClick={openAdd}
            style={{
              marginLeft:'auto', padding:'4px 12px', borderRadius:6,
              border:'1px solid var(--teal)', background: showForm ? 'var(--teal)' : 'transparent',
              color: showForm ? 'white' : 'var(--teal)', fontSize:12, fontWeight:600, cursor:'pointer',
            }}>
            {showForm ? '✕ 닫기' : '+ 규칙 추가'}
          </button>
        </CardHeader>

        {showForm && (
          <CardBody style={{ borderBottom: '2px solid var(--teal)', background: '#f0fdfb' }}>
            <RuleForm
              form={form} setForm={setForm}
              error={error} saving={saving}
              isEdit={false}
              onSave={handleSave}
              onCancel={() => { setShowForm(false); setForm(EMPTY_RULE); setError('') }}
            />
          </CardBody>
        )}

        {loading && <div style={{ padding:16, color:'var(--gray)' }}>로딩 중...</div>}

        {!loading && rules.length === 0 && (
          <CardBody>
            <Alert type="info">
              등록된 Rule 없음. + 규칙 추가 버튼으로 새 규칙을 등록하세요.
            </Alert>
          </CardBody>
        )}

        {rules.map((r, i) => {
          let forbidden = r.forbidden_funcs
          if (typeof forbidden === 'string') { try { forbidden = JSON.parse(forbidden) } catch { forbidden = [] } }
          let sqlPatterns = r.forbidden_sql_patterns
          if (typeof sqlPatterns === 'string') { try { sqlPatterns = JSON.parse(sqlPatterns) } catch { sqlPatterns = [] } }

          const isEditing = editingId === r.rule_id

          return (
            <div key={i} style={{
              borderTop: i > 0 ? '1px solid var(--gray2)' : 'none',
              background: isEditing ? '#f0fdfb' : 'transparent',
            }}>
              {/* ── 보기 모드 헤더 ── */}
              <div style={{ padding:'12px 16px', opacity: r.enabled ? 1 : 0.5 }}>
                <div style={{ display:'flex', alignItems:'center', gap:6, marginBottom:4 }}>
                  <span style={{ fontFamily:'monospace', fontSize:11, color:'var(--gray)' }}>{r.rule_id}</span>
                  <Badge color={
                    r.scope === 'global' ? 'navy' :
                    r.scope === 'db'     ? 'teal' :
                    r.scope === 'table'  ? 'green' : 'gray'
                  }>{r.scope}</Badge>
                  {r.auto_detected && <Badge color="teal">auto</Badge>}
                  {!r.enabled && <Badge color="red">disabled</Badge>}

                  <div style={{ marginLeft:'auto', display:'flex', gap:6 }}>
                    <button onClick={() => openEdit(r)}
                      style={{
                        padding:'2px 8px', borderRadius:4,
                        border: `1px solid ${isEditing ? 'var(--teal)' : 'var(--gray2)'}`,
                        background: isEditing ? 'var(--teal)' : 'transparent',
                        color: isEditing ? 'white' : 'var(--gray)',
                        fontSize:11, cursor:'pointer', fontWeight:600,
                      }}>
                      {isEditing ? '✕ 닫기' : '✏ 편집'}
                    </button>
                    <button onClick={() => handleToggle(r.rule_id, r.enabled)}
                      style={{
                        padding:'2px 8px', borderRadius:4, border:'1px solid var(--gray2)',
                        background: r.enabled ? 'var(--gray3)' : 'var(--mint)',
                        color: r.enabled ? 'var(--gray)' : 'var(--teal)',
                        fontSize:11, cursor:'pointer', fontWeight:600,
                      }}>
                      {r.enabled ? '비활성화' : '활성화'}
                    </button>
                    <button onClick={() => handleDelete(r.rule_id)}
                      style={{
                        padding:'2px 8px', borderRadius:4, border:'1px solid #fca5a5',
                        background:'transparent', color:'#dc2626',
                        fontSize:11, cursor:'pointer',
                      }}>
                      삭제
                    </button>
                  </div>
                </div>

                <div style={{ fontWeight:700, fontSize:13, color:'var(--dark)', marginBottom:2 }}>
                  {r.scope === 'column' ? `${r.table_name}.${r.column_name}` :
                   r.scope === 'table'  ? r.table_name :
                   r.scope === 'db'     ? r.db_alias || db :
                   '(global)'}
                  {r.required_func && (
                    <span style={{ marginLeft:6, fontSize:11, color:'var(--teal)', fontWeight:400 }}>
                      → use {r.required_func}
                    </span>
                  )}
                </div>
                {forbidden?.length > 0 && (
                  <div style={{ fontSize:12, color:'#dc2626', marginBottom:2 }}>
                    ✗ forbidden funcs: {forbidden.join(', ')}
                  </div>
                )}
                {sqlPatterns?.length > 0 && (
                  <div style={{ fontSize:12, color:'#c2410c', marginBottom:2 }}>
                    ✗ forbidden patterns: {sqlPatterns.join(', ')}
                  </div>
                )}
                <div style={{ fontSize:12, color:'var(--gray)', whiteSpace:'pre-line' }}>{r.instruction}</div>
                {(r.example_bad || r.example_good) && (
                  <div style={{ display:'flex', gap:8, marginTop:6, flexWrap:'wrap' }}>
                    {r.example_bad && (
                      <span style={{ padding:'3px 8px', background:'#fee2e2', borderRadius:4, fontFamily:'monospace', fontSize:11, color:'#dc2626' }}>
                        ✗ {r.example_bad}
                      </span>
                    )}
                    {r.example_good && (
                      <span style={{ padding:'3px 8px', background:'#dcfce7', borderRadius:4, fontFamily:'monospace', fontSize:11, color:'#16a34a' }}>
                        ✓ {r.example_good}
                      </span>
                    )}
                  </div>
                )}
              </div>

              {/* ── 인라인 편집 폼 ── */}
              {isEditing && (
                <div style={{ padding:'12px 16px 16px', borderTop:'1px dashed var(--teal)' }}>
                  <RuleForm
                    form={form} setForm={setForm}
                    error={error} saving={saving}
                    isEdit={true}
                    onSave={handleUpdate}
                    onCancel={closeEdit}
                  />
                </div>
              )}
            </div>
          )
        })}
      </Card>
    </div>
  )
}

// ── RuleForm — 신규 추가 / 인라인 편집 공용 폼 ──────────────
function RuleForm({ form, setForm, error, saving, isEdit, onSave, onCancel }) {
  const inp = (field) => ({
    value: form[field] ?? '',
    onChange: e => setForm(f => ({ ...f, [field]: e.target.value })),
    style: {
      width: '100%', padding: '6px 8px', fontSize: 12,
      border: '1px solid var(--gray2)', borderRadius: 5, outline: 'none',
    },
  })

  return (
    <div>
      <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:8, marginBottom:8 }}>
        <div>
          <label style={{ fontSize:11, color:'var(--gray)', fontWeight:600 }}>rule_id *</label>
          <input {...inp('rule_id')} placeholder="no_unnecessary_cte"
            readOnly={isEdit}
            style={{ ...inp('rule_id').style, background: isEdit ? 'var(--gray3)' : 'white', color: isEdit ? 'var(--gray)' : 'var(--dark)' }} />
        </div>
        <div>
          <label style={{ fontSize:11, color:'var(--gray)', fontWeight:600 }}>scope</label>
          <select value={form.scope} onChange={e => setForm(f => ({ ...f, scope: e.target.value }))}
            style={{ width:'100%', padding:'6px 8px', fontSize:12, border:'1px solid var(--gray2)', borderRadius:5 }}>
            <option value="global">global (모든 DB)</option>
            <option value="db">db (현재 DB만)</option>
            <option value="table">table</option>
            <option value="column">column</option>
          </select>
        </div>
        {(form.scope === 'table' || form.scope === 'column') && (
          <>
            <div>
              <label style={{ fontSize:11, color:'var(--gray)', fontWeight:600 }}>table_name</label>
              <input {...inp('table_name')} placeholder="orders" />
            </div>
            {form.scope === 'column' && (
              <div>
                <label style={{ fontSize:11, color:'var(--gray)', fontWeight:600 }}>column_name</label>
                <input {...inp('column_name')} placeholder="created_at" />
              </div>
            )}
          </>
        )}
      </div>

      <div style={{ marginBottom:8 }}>
        <label style={{ fontSize:11, color:'var(--gray)', fontWeight:600 }}>instruction * (LLM에 전달되는 지침)</label>
        <textarea {...inp('instruction')} rows={3}
          placeholder="CTE(WITH)는 불필요하면 사용하지 마세요."
          style={{ ...inp('instruction').style, resize:'vertical', fontFamily:'inherit', lineHeight:1.5 }} />
      </div>

      <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:8, marginBottom:8 }}>
        <div>
          <label style={{ fontSize:11, color:'var(--gray)', fontWeight:600 }}>forbidden_funcs (쉼표 구분, AST 함수명)</label>
          <input {...inp('forbidden_funcs')} placeholder="age, date_part" />
        </div>
        <div>
          <label style={{ fontSize:11, color:'var(--gray)', fontWeight:600 }}>
            forbidden_sql_patterns (쉼표 구분, 정규식)
            <span style={{ fontWeight:400, marginLeft:4, color:'var(--teal)' }}>← 연산자 감지용</span>
          </label>
          <input {...inp('forbidden_sql_patterns')} placeholder="\|\|" />
        </div>
      </div>

      <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:8, marginBottom:8 }}>
        <div>
          <label style={{ fontSize:11, color:'var(--gray)', fontWeight:600 }}>required_func</label>
          <input {...inp('required_func')} placeholder="CONCAT" />
        </div>
        <div>
          <label style={{ fontSize:11, color:'var(--gray)', fontWeight:600 }}>severity</label>
          <select value={form.severity} onChange={e => setForm(f => ({ ...f, severity: e.target.value }))}
            style={{ width:'100%', padding:'6px 8px', fontSize:12, border:'1px solid var(--gray2)', borderRadius:5 }}>
            <option value="warning">warning</option>
            <option value="error">error</option>
          </select>
        </div>
      </div>

      <div style={{ display:'grid', gridTemplateColumns:'1fr 1fr', gap:8, marginBottom:10 }}>
        <div>
          <label style={{ fontSize:11, color:'var(--gray)', fontWeight:600 }}>example_bad</label>
          <input {...inp('example_bad')} placeholder="first_name || last_name"
            style={{ ...inp('example_bad').style, fontFamily:'monospace' }} />
        </div>
        <div>
          <label style={{ fontSize:11, color:'var(--gray)', fontWeight:600 }}>example_good</label>
          <input {...inp('example_good')} placeholder="CONCAT(first_name, last_name)"
            style={{ ...inp('example_good').style, fontFamily:'monospace' }} />
        </div>
      </div>

      {error && <Alert type="error" style={{ marginBottom:8 }}>⚠ {error}</Alert>}

      <div style={{ display:'flex', gap:8 }}>
        <button onClick={onSave} disabled={saving}
          style={{ padding:'6px 16px', borderRadius:6, border:'none', background:'var(--teal)', color:'white', fontWeight:600, fontSize:13, cursor:'pointer' }}>
          {saving ? '저장 중...' : (isEdit ? '💾 수정 저장' : '💾 저장')}
        </button>
        <button onClick={onCancel}
          style={{ padding:'6px 12px', borderRadius:6, border:'1px solid var(--gray2)', background:'transparent', color:'var(--gray)', fontSize:13, cursor:'pointer' }}>
          취소
        </button>
      </div>
    </div>
  )
}
