import { useState, useEffect, useCallback } from 'react'
import { useDb } from '../components/Layout'
import { schemaApi, dbApi } from '../api/client'
import { Card, CardHeader, CardBody, Alert, CountBadge, Btn, Spinner } from '../components/UI'

const IDX_BADGE = {
  primary: { bg:'#fef9c3', color:'#854d0e', label:'PK' },
  unique:  { bg:'#ede9fe', color:'#6d28d9', label:'UQ' },
  normal:  { bg:'#f0f7ff', color:'#0369a1', label:'IDX' },
}

function IndexBadge({ type }) {
  const s = IDX_BADGE[type]
  return (
    <span style={{ padding:'1px 5px', borderRadius:3, fontSize:10,
      fontWeight:700, background:s.bg, color:s.color, marginRight:3 }}>
      {s.label}
    </span>
  )
}

export default function SchemaPage() {
  const { db } = useDb()
  const [tables, setTables]     = useState([])
  const [indexes, setIndexes]   = useState({})
  const [search, setSearch]     = useState('')
  const [open, setOpen]         = useState({})
  const [loading, setLoading]   = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [refreshMsg, setRefreshMsg] = useState('')

  const loadSchema = useCallback(() => {
    if (!db) return
    setLoading(true)
    Promise.all([
      schemaApi.list(db, search),
      search ? Promise.resolve({}) : schemaApi.indexes(db).catch(() => ({})),
    ]).then(([data, idxData]) => {
      setTables(data)
      if (!search) setIndexes(idxData)
      setLoading(false)
    }).catch(() => setLoading(false))
  }, [db, search])

  useEffect(() => { loadSchema() }, [loadSchema])

  async function handleRefresh() {
    setRefreshing(true); setRefreshMsg('')
    const t0 = performance.now()
    try {
      const res = await dbApi.refresh(db)
      const elapsed = ((performance.now() - t0) / 1000).toFixed(1)
      const at = res.refreshed_at
        ? new Date(res.refreshed_at).toLocaleString('ko-KR', { hour12: false })
        : ''
      setRefreshMsg(`완료: 테이블 ${res.tables_scanned ?? '?'}개 / ${elapsed}s / ${at}`)
      loadSchema()
    } catch (e) {
      const elapsed = ((performance.now() - t0) / 1000).toFixed(1)
      setRefreshMsg(`오류 (${elapsed}s): ${e.response?.data?.detail || e.message}`)
    } finally {
      setRefreshing(false)
    }
  }

  const toggle = (key) => setOpen(o => ({ ...o, [key]: !o[key] }))

  if (!db) return <Alert type="info">상단에서 DB를 선택하세요.</Alert>

  return (
    <div>
      <Card>
        <CardHeader>
          🗂 Schema 탐색
          <CountBadge>{tables.length} tables</CountBadge>
          <div style={{ marginLeft:'auto', display:'flex', alignItems:'center', gap:8 }}>
            {refreshMsg && (
              <span style={{ fontSize:11, color: refreshMsg.startsWith('오류') ? 'var(--red)' : 'var(--teal)' }}>
                {refreshMsg}
              </span>
            )}
            <Btn onClick={handleRefresh} disabled={refreshing || loading} size="sm">
              {refreshing ? <><Spinner /> 수집 중...</> : '↻ 스키마 재수집'}
            </Btn>
          </div>
        </CardHeader>
        <CardBody>
          <input
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="테이블 / 컬럼 / 코멘트 검색..."
            style={{
              width:'100%', padding:'7px 10px', border:'1px solid var(--gray2)',
              borderRadius:6, fontSize:13, outline:'none', marginBottom:12,
            }}
            onFocus={e => e.target.style.borderColor = 'var(--teal)'}
            onBlur={e => e.target.style.borderColor = 'var(--gray2)'}
          />

          {loading && <div style={{ color:'var(--gray)', padding:8 }}>로딩 중...</div>}

          {!loading && tables.length === 0 && (
            <Alert type="info">
              스키마 없음. ↻ 스키마 재수집 버튼을 클릭하거나
              <code> pgxllm db refresh --alias {db}</code> 를 실행하세요.
            </Alert>
          )}

          {tables.map(t => {
            const key     = `${t.schema}.${t.table}`
            const isOpen  = !!open[key]
            const idxList = indexes[key] || []

            return (
              <div key={key} style={{ border:'1px solid var(--gray2)', borderRadius:6, marginBottom:8, overflow:'hidden' }}>
                {/* Table header */}
                <div
                  onClick={() => toggle(key)}
                  style={{
                    padding:'8px 12px', background: isOpen ? 'var(--mint)' : '#f0f7ff',
                    cursor:'pointer', display:'flex', alignItems:'center', gap:8,
                    userSelect:'none',
                  }}
                >
                  <span style={{ fontSize:11, color:'var(--teal)' }}>{isOpen ? '▾' : '▸'}</span>
                  <span style={{ fontWeight:700, color:'var(--navy)', fontSize:13 }}>{t.table}</span>
                  <span style={{ color:'var(--gray)', fontSize:11 }}>{t.schema}</span>
                  {t.comment && <span style={{ color:'var(--gray)', fontSize:11, fontStyle:'italic' }}>— {t.comment}</span>}
                  <div style={{ marginLeft:'auto', display:'flex', gap:5, alignItems:'center' }}>
                    {idxList.length > 0 && (
                      <span style={{ background:'var(--mint)', color:'var(--teal)', padding:'1px 7px', borderRadius:10, fontSize:11, fontWeight:600 }}>
                        {idxList.length} idx
                      </span>
                    )}
                    <span style={{ background:'var(--gray2)', color:'var(--gray)', padding:'1px 7px', borderRadius:10, fontSize:11, fontWeight:600 }}>
                      {t.columns.length} cols
                    </span>
                  </div>
                </div>

                {isOpen && (
                  <>
                    {/* Column list */}
                    <table style={{ width:'100%', borderCollapse:'collapse', borderTop:'1px solid var(--gray2)', fontSize:12 }}>
                      <colgroup>
                        <col style={{ width:200 }} />
                        <col style={{ width:140 }} />
                        <col />
                        <col style={{ width:80 }} />
                      </colgroup>
                      <thead>
                        <tr style={{ background:'var(--gray3)' }}>
                          {['컬럼', '타입', '참조 / 코멘트', '통계'].map(h => (
                            <th key={h} style={{ padding:'5px 10px', textAlign:'left', fontWeight:600, color:'var(--gray)', fontSize:11 }}>{h}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {t.columns.map(c => (
                          <tr key={c.name} style={{ borderTop:'1px solid var(--gray2)' }}>
                            <td style={{ padding:'5px 10px', fontFamily:'monospace' }}>
                              {c.name}
                              {c.pk && <span style={{ marginLeft:4, padding:'1px 5px', borderRadius:3, fontSize:10, fontWeight:700, background:'#fef9c3', color:'#854d0e' }}>PK</span>}
                              {c.fk && <span style={{ marginLeft:4, padding:'1px 5px', borderRadius:3, fontSize:10, fontWeight:700, background:'#e0f2fe', color:'#0369a1' }}>FK</span>}
                            </td>
                            <td style={{ padding:'5px 10px', color:'var(--teal)', fontSize:11 }}>{c.type}</td>
                            <td style={{ padding:'5px 10px', color:'var(--gray)', fontSize:11 }}>
                              {c.fk_ref ? `→ ${c.fk_ref}` : (c.comment || '')}
                            </td>
                            <td style={{ padding:'5px 10px', color:'var(--gray)', fontSize:11 }}>
                              {c.n_distinct != null ? `~${Number(c.n_distinct).toFixed(0)}` : ''}
                              {c.samples?.length ? (
                                <span title={c.samples.join(', ')} style={{ marginLeft:4, cursor:'help', color:'var(--navy2)' }}>📋</span>
                              ) : ''}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>

                    {/* Index list */}
                    {idxList.length > 0 && (
                      <div style={{ borderTop:'2px solid var(--gray2)', background:'#fafafa' }}>
                        <div style={{ padding:'5px 10px', fontSize:11, fontWeight:700, color:'var(--gray)', background:'var(--gray3)' }}>
                          📇 인덱스 ({idxList.length})
                        </div>
                        <table style={{ width:'100%', borderCollapse:'collapse', fontSize:12 }}>
                          <colgroup>
                            <col style={{ width:220 }} />
                            <col style={{ width:90 }} />
                            <col style={{ width:200 }} />
                            <col />
                          </colgroup>
                          <thead>
                            <tr style={{ background:'var(--gray3)' }}>
                              {['인덱스명', '유형', '컬럼', '정의'].map(h => (
                                <th key={h} style={{ padding:'4px 10px', textAlign:'left', fontWeight:600, color:'var(--gray)', fontSize:11 }}>{h}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {idxList.map(idx => {
                              const type = idx.is_primary ? 'primary' : idx.is_unique ? 'unique' : 'normal'
                              return (
                                <tr key={idx.name} style={{ borderTop:'1px solid var(--gray2)' }}>
                                  <td style={{ padding:'4px 10px', fontFamily:'monospace', fontSize:11 }}>
                                    <IndexBadge type={type} />
                                    {idx.name}
                                  </td>
                                  <td style={{ padding:'4px 10px', fontSize:11, color:'var(--gray)' }}>
                                    {idx.is_primary ? 'PRIMARY' : idx.is_unique ? 'UNIQUE' : 'INDEX'}
                                  </td>
                                  <td style={{ padding:'4px 10px', fontFamily:'monospace', fontSize:11, color:'var(--navy)' }}>
                                    {idx.columns}
                                  </td>
                                  <td style={{ padding:'4px 10px', fontSize:10, color:'var(--gray)', fontFamily:'monospace',
                                    maxWidth:300, overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap' }}
                                    title={idx.def}>
                                    {idx.def}
                                  </td>
                                </tr>
                              )
                            })}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </>
                )}
              </div>
            )
          })}
        </CardBody>
      </Card>
    </div>
  )
}
