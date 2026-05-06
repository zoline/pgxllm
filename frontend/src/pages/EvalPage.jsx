/**
 * EvalPage — BIRD 벤치마크 평가 UI
 *
 * BIRD 공식 JSON 형식 지원:
 *   [{question_id, question, SQL, evidence, difficulty, db_id}, ...]
 */
import { useState, useEffect, useRef } from 'react'
import { useDb } from '../components/Layout'
import { evalApi } from '../api/client'

// ── Styles ────────────────────────────────────────────────────
const S = {
  card:    { background: 'white', borderRadius: 8, border: '1px solid var(--gray2)', padding: 20, marginBottom: 16 },
  h2:      { fontSize: 15, fontWeight: 700, marginBottom: 12, color: 'var(--dark)' },
  label:   { fontSize: 12, color: 'var(--gray)', marginBottom: 4, display: 'block' },
  input:   { width: '100%', padding: '7px 10px', border: '1px solid var(--gray2)', borderRadius: 6, fontSize: 13, outline: 'none', boxSizing: 'border-box' },
  select:  { padding: '7px 10px', border: '1px solid var(--gray2)', borderRadius: 6, fontSize: 13, outline: 'none', background: 'white', cursor: 'pointer' },
  btn:     (c='var(--teal)') => ({ padding: '7px 16px', background: c, color: 'white', border: 'none', borderRadius: 6, fontSize: 13, cursor: 'pointer', fontWeight: 600 }),
  btnGhost: { padding: '6px 14px', background: 'white', border: '1px solid var(--gray2)', borderRadius: 6, fontSize: 12, cursor: 'pointer' },
  row:     { display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' },
  table:   { width: '100%', borderCollapse: 'collapse', fontSize: 12 },
  th:      { padding: '6px 10px', borderBottom: '2px solid var(--gray2)', textAlign: 'left', color: 'var(--gray)', fontWeight: 600, whiteSpace: 'nowrap' },
  td:      { padding: '6px 10px', borderBottom: '1px solid var(--gray2)', verticalAlign: 'top' },
  mono:    { fontFamily: 'monospace', fontSize: 11, whiteSpace: 'pre-wrap', wordBreak: 'break-all' },
  badge:   (ok) => ({
    display: 'inline-block', padding: '1px 7px', borderRadius: 4,
    background: ok === true ? '#dcfce7' : ok === false ? '#fee2e2' : '#f3f4f6',
    color: ok === true ? '#166534' : ok === false ? '#991b1b' : '#374151',
    fontSize: 11, fontWeight: 600,
  }),
  diffBadge: (d) => ({
    display: 'inline-block', padding: '1px 6px', borderRadius: 4, fontSize: 10, fontWeight: 600,
    background: d === 'simple' ? '#e0f2fe' : d === 'moderate' ? '#fef9c3' : '#fee2e2',
    color:      d === 'simple' ? '#0369a1' : d === 'moderate' ? '#854d0e' : '#991b1b',
  }),
  progress: { marginBottom: 12, padding: '10px 14px', background: '#f0fdf4', borderRadius: 6, border: '1px solid #bbf7d0', fontSize: 13 },
  error:   { padding: '10px 14px', background: '#fef2f2', borderRadius: 6, border: '1px solid #fecaca', fontSize: 13, color: '#991b1b' },
  summary: { display: 'flex', gap: 16, flexWrap: 'wrap', marginBottom: 16 },
  sumBox:  { padding: '10px 18px', borderRadius: 8, border: '1px solid var(--gray2)', textAlign: 'center', minWidth: 110 },
}

function pct(n, total) {
  if (!total || n == null) return '—'
  return `${(n / total * 100).toFixed(1)}%`
}

// ── History panel ─────────────────────────────────────────────
function HistoryPanel({ db, onSelect }) {
  const [list, setList]       = useState([])
  const [loading, setLoading] = useState(false)

  const load = () => {
    if (!db) return
    setLoading(true)
    evalApi.list(db).then(setList).catch(() => setList([])).finally(() => setLoading(false))
  }

  useEffect(() => { load() }, [db])

  if (!db) return null
  return (
    <div style={S.card}>
      <div style={{ ...S.row, marginBottom: 12 }}>
        <span style={S.h2}>이전 평가 기록</span>
        <button style={S.btnGhost} onClick={load}>{loading ? '...' : '↻ 새로고침'}</button>
      </div>
      {list.length === 0 && !loading && <p style={{ color: 'var(--gray)', fontSize: 12 }}>기록 없음</p>}
      {list.length > 0 && (
        <table style={S.table}>
          <thead>
            <tr>
              <th style={S.th}>eval_name</th>
              <th style={S.th}>총 문항</th>
              <th style={S.th}>Baseline EX</th>
              <th style={S.th}>pgxllm EX</th>
              <th style={S.th}>마지막 실행</th>
              <th style={S.th}></th>
            </tr>
          </thead>
          <tbody>
            {list.map(r => (
              <tr key={r.eval_name} style={{ cursor: 'pointer' }} onClick={() => onSelect(r)}>
                <td style={S.td}><code>{r.eval_name}</code></td>
                <td style={S.td}>{r.total}</td>
                <td style={S.td}>
                  {r.baseline_ex != null
                    ? `${r.baseline_ex} (${pct(r.baseline_ex, r.total)})`
                    : <span style={{ color: 'var(--gray)' }}>skip</span>}
                </td>
                <td style={S.td} style={{ color: 'var(--teal)', fontWeight: 600 }}>
                  {r.pgxllm_ex != null ? `${r.pgxllm_ex} (${pct(r.pgxllm_ex, r.total)})` : '—'}
                </td>
                <td style={S.td}>{r.last_run ? r.last_run.slice(0, 16) : '—'}</td>
                <td style={S.td}>
                  <button style={S.btnGhost} onClick={e => {
                    e.stopPropagation()
                    if (!window.confirm(`'${r.eval_name}' 결과를 삭제하시겠습니까?`)) return
                    evalApi.delete(db, r.eval_name).then(load)
                  }}>삭제</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}

// ── Results detail ─────────────────────────────────────────────
function ResultsDetail({ db, evalName, summary, onClose }) {
  const [rows, setRows]         = useState([])
  const [loading, setLoading]   = useState(true)
  const [expanded, setExpanded] = useState({})
  const [filter, setFilter]     = useState('all')   // all | pgxllm_only | both_wrong

  useEffect(() => {
    evalApi.results(db, evalName).then(setRows).catch(() => setRows([])).finally(() => setLoading(false))
  }, [db, evalName])

  const toggle = (id) => setExpanded(prev => ({ ...prev, [id]: !prev[id] }))

  const filtered = rows.filter(r => {
    if (filter === 'pgxllm_only')  return !r.ex_baseline && r.ex_pgxllm
    if (filter === 'baseline_only') return r.ex_baseline && !r.ex_pgxllm
    if (filter === 'both_wrong')   return !r.ex_baseline && !r.ex_pgxllm
    if (filter === 'both_correct') return r.ex_baseline && r.ex_pgxllm
    return true
  })

  const total  = summary?.total || rows.length
  const b_ex   = summary?.baseline_ex_count
  const p_ex   = summary?.pgxllm_ex_count

  return (
    <div style={{ maxWidth: 1200 }}>
      <div style={S.card}>
        <div style={{ ...S.row, marginBottom: 16 }}>
          <span style={S.h2}>결과 상세 — <code>{evalName}</code></span>
          <button style={S.btnGhost} onClick={onClose}>← 목록으로</button>
        </div>

        <div style={S.summary}>
          <div style={S.sumBox}>
            <div style={{ fontSize: 22, fontWeight: 700 }}>{total}</div>
            <div style={{ fontSize: 11, color: 'var(--gray)' }}>총 문항</div>
          </div>
          {b_ex != null && (
            <div style={S.sumBox}>
              <div style={{ fontSize: 22, fontWeight: 700 }}>{pct(b_ex, total)}</div>
              <div style={{ fontSize: 11, color: 'var(--gray)' }}>Baseline EX</div>
              <div style={{ fontSize: 11 }}>{b_ex} / {total}</div>
            </div>
          )}
          <div style={{ ...S.sumBox, borderColor: 'var(--teal)', background: 'var(--mint)' }}>
            <div style={{ fontSize: 22, fontWeight: 700, color: 'var(--teal)' }}>{pct(p_ex, total)}</div>
            <div style={{ fontSize: 11, color: 'var(--gray)' }}>pgxllm EX</div>
            <div style={{ fontSize: 11 }}>{p_ex} / {total}</div>
          </div>
          {b_ex != null && (
            <>
              <div style={{ ...S.sumBox, background: '#f0fdf4' }}>
                <div style={{ fontSize: 18, fontWeight: 700, color: '#16a34a' }}>
                  {rows.filter(r => !r.ex_baseline && r.ex_pgxllm).length}
                </div>
                <div style={{ fontSize: 11, color: 'var(--gray)' }}>pgxllm만 정답</div>
              </div>
              <div style={{ ...S.sumBox, background: '#fef2f2' }}>
                <div style={{ fontSize: 18, fontWeight: 700, color: '#dc2626' }}>
                  {rows.filter(r => r.ex_baseline && !r.ex_pgxllm).length}
                </div>
                <div style={{ fontSize: 11, color: 'var(--gray)' }}>Baseline만 정답</div>
              </div>
            </>
          )}
        </div>

        {/* Filter */}
        <div style={{ ...S.row, marginBottom: 12 }}>
          {[
            ['all', '전체'],
            ['pgxllm_only', 'pgxllm만 정답'],
            ['baseline_only', 'Baseline만 정답'],
            ['both_wrong', '둘 다 오답'],
            ['both_correct', '둘 다 정답'],
          ].map(([v, l]) => (
            <button key={v}
              style={{ ...S.btnGhost, background: filter === v ? 'var(--teal)' : 'white', color: filter === v ? 'white' : 'var(--dark)' }}
              onClick={() => setFilter(v)}>{l} {v !== 'all' && `(${rows.filter(r => {
                if (v === 'pgxllm_only')  return !r.ex_baseline && r.ex_pgxllm
                if (v === 'baseline_only') return r.ex_baseline && !r.ex_pgxllm
                if (v === 'both_wrong')   return !r.ex_baseline && !r.ex_pgxllm
                if (v === 'both_correct') return r.ex_baseline && r.ex_pgxllm
              }).length})`}
            </button>
          ))}
        </div>

        {loading && <p style={{ color: 'var(--gray)', fontSize: 12 }}>로딩 중...</p>}
        <table style={S.table}>
          <thead>
            <tr>
              <th style={S.th}>#</th>
              <th style={S.th}>질문</th>
              <th style={S.th}>난이도</th>
              <th style={S.th}>Baseline EX</th>
              <th style={S.th}>pgxllm EX</th>
              <th style={S.th}>ms (b/p)</th>
              <th style={S.th}></th>
            </tr>
          </thead>
          <tbody>
            {filtered.map(r => (
              <>
                <tr key={r.id}>
                  <td style={S.td}>{r.question_id ?? '—'}</td>
                  <td style={{ ...S.td, maxWidth: 260 }}>
                    <div>{r.question}</div>
                    {r.hint && <div style={{ color: 'var(--gray)', fontSize: 11, marginTop: 2 }}>💡 {r.hint}</div>}
                  </td>
                  <td style={S.td}><span style={S.diffBadge(r.difficulty)}>{r.difficulty || '—'}</span></td>
                  <td style={S.td}><span style={S.badge(r.ex_baseline)}>{r.ex_baseline === true ? '✓' : r.ex_baseline === false ? '✗' : '—'}</span></td>
                  <td style={S.td}><span style={S.badge(r.ex_pgxllm)}>{r.ex_pgxllm === true ? '✓' : r.ex_pgxllm === false ? '✗' : '—'}</span></td>
                  <td style={S.td} style={{ color: 'var(--gray)', fontSize: 11 }}>{r.baseline_ms}/{r.pgxllm_ms}</td>
                  <td style={S.td}>
                    <button style={S.btnGhost} onClick={() => toggle(r.id)}>
                      {expanded[r.id] ? '접기' : 'SQL'}
                    </button>
                  </td>
                </tr>
                {expanded[r.id] && (
                  <tr key={`${r.id}-d`}>
                    <td colSpan={7} style={{ ...S.td, background: '#f8fafc', padding: '10px 16px' }}>
                      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 12 }}>
                        <div>
                          <div style={{ fontWeight: 600, fontSize: 11, marginBottom: 4, color: '#374151' }}>Gold SQL</div>
                          <pre style={{ ...S.mono, background: '#f1f5f9', padding: 8, borderRadius: 4, margin: 0 }}>{r.gold_sql || '—'}</pre>
                        </div>
                        <div>
                          <div style={{ fontWeight: 600, fontSize: 11, marginBottom: 4, color: '#92400e' }}>
                            Baseline SQL <span style={S.badge(r.ex_baseline)}>{r.ex_baseline === true ? '✓' : '✗'}</span>
                          </div>
                          <pre style={{ ...S.mono, background: '#fffbeb', padding: 8, borderRadius: 4, margin: 0 }}>{r.baseline_sql || '—'}</pre>
                          {r.error_baseline && <div style={{ color: '#dc2626', fontSize: 11, marginTop: 4 }}>{r.error_baseline}</div>}
                        </div>
                        <div>
                          <div style={{ fontWeight: 600, fontSize: 11, marginBottom: 4, color: 'var(--teal)' }}>
                            pgxllm SQL <span style={S.badge(r.ex_pgxllm)}>{r.ex_pgxllm === true ? '✓' : '✗'}</span>
                          </div>
                          <pre style={{ ...S.mono, background: '#f0fdf4', padding: 8, borderRadius: 4, margin: 0 }}>{r.pgxllm_sql || '—'}</pre>
                          {r.error_pgxllm && <div style={{ color: '#dc2626', fontSize: 11, marginTop: 4 }}>{r.error_pgxllm}</div>}
                        </div>
                      </div>
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Main page ──────────────────────────────────────────────────
export default function EvalPage() {
  const { dbs } = useDb()
  const benchmarkDbs = dbs.filter(d => d.db_type === 'benchmark')

  const [selectedDb,   setSelectedDb]   = useState('')
  const [evalName,     setEvalName]     = useState('')
  const [skipBaseline, setSkipBaseline] = useState(false)
  const [jsonText,     setJsonText]     = useState('')
  const [running,      setRunning]      = useState(false)
  const [progress,     setProgress]     = useState('')
  const [error,        setError]        = useState('')
  const [latestResult, setLatestResult] = useState(null)
  const [detailView,   setDetailView]   = useState(null)
  const fileRef = useRef()

  useEffect(() => {
    if (!selectedDb && benchmarkDbs.length > 0) setSelectedDb(benchmarkDbs[0].alias)
  }, [benchmarkDbs])

  const handleFileLoad = (e) => {
    const file = e.target.files[0]
    if (!file) return
    const reader = new FileReader()
    reader.onload = ev => setJsonText(ev.target.result)
    reader.readAsText(file)
  }

  // BIRD JSON 항목 수 계산
  const itemCount = (() => {
    if (!jsonText.trim()) return 0
    try {
      const parsed = JSON.parse(jsonText)
      const arr = Array.isArray(parsed) ? parsed : parsed.data || parsed.questions || []
      return arr.filter(it => (it.question || it.question_text) && (it.SQL || it.gold_sql || it.sql)).length
    } catch { return null }
  })()

  const handleRun = async () => {
    if (!selectedDb) { setError('Benchmark DB를 선택하세요.'); return }
    if (!jsonText.trim()) { setError('BIRD JSON 데이터를 입력하세요.'); return }
    if (itemCount === 0) { setError('유효한 항목이 없습니다. question/SQL 필드를 확인하세요.'); return }

    let items
    try {
      const parsed = JSON.parse(jsonText)
      items = Array.isArray(parsed) ? parsed : parsed.data || parsed.questions || []
    } catch (e) {
      setError(`JSON 파싱 오류: ${e.message}`)
      return
    }

    setRunning(true)
    setError('')
    setProgress(`${itemCount}개 항목 평가 중… (시간이 걸릴 수 있습니다)`)
    setLatestResult(null)

    try {
      const name = evalName.trim() || `eval_${new Date().toISOString().slice(0, 16).replace('T', '_')}`
      const res = await evalApi.run({
        db_alias:      selectedDb,
        eval_name:     name,
        items,
        skip_baseline: skipBaseline,
      })
      setLatestResult(res)
      setProgress('')
      setDetailView({ db: selectedDb, evalName: res.eval_name, summary: res })
    } catch (e) {
      setError(e.response?.data?.detail || e.message || '알 수 없는 오류')
      setProgress('')
    } finally {
      setRunning(false)
    }
  }

  if (detailView) {
    return (
      <ResultsDetail
        db={detailView.db}
        evalName={detailView.evalName}
        summary={detailView.summary}
        onClose={() => setDetailView(null)}
      />
    )
  }

  return (
    <div style={{ maxWidth: 1000 }}>

      {/* Run panel */}
      <div style={S.card}>
        <div style={S.h2}>새 평가 실행</div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14, marginBottom: 14 }}>
          <div>
            <label style={S.label}>Benchmark DB</label>
            {benchmarkDbs.length === 0 ? (
              <p style={{ fontSize: 12, color: '#dc2626' }}>
                등록된 Benchmark DB가 없습니다. DB 관리에서 <b>db_type=benchmark</b>로 등록하세요.
              </p>
            ) : (
              <select style={S.select} value={selectedDb} onChange={e => setSelectedDb(e.target.value)}>
                <option value="">— 선택 —</option>
                {benchmarkDbs.map(d => (
                  <option key={d.alias} value={d.alias}>{d.alias} ({d.dbname})</option>
                ))}
              </select>
            )}
          </div>
          <div>
            <label style={S.label}>평가 이름 (선택)</label>
            <input
              style={S.input}
              placeholder="예: bird_dev_v1  (비워두면 자동 생성)"
              value={evalName}
              onChange={e => setEvalName(e.target.value)}
            />
          </div>
        </div>

        <div style={{ marginBottom: 14 }}>
          <label style={S.label}>
            BIRD JSON 파일 업로드 또는 붙여넣기
            <span style={{ color: 'var(--gray)', fontWeight: 400, marginLeft: 6 }}>
              (공식 형식: question_id, question, SQL, evidence, difficulty)
            </span>
          </label>
          <div style={{ ...S.row, marginBottom: 8 }}>
            <input type="file" accept=".json" ref={fileRef} style={{ display: 'none' }} onChange={handleFileLoad} />
            <button style={S.btnGhost} onClick={() => fileRef.current?.click()}>📁 파일 선택</button>
            {itemCount !== null && jsonText && (
              <span style={{ fontSize: 12, color: itemCount > 0 ? 'var(--teal)' : '#dc2626' }}>
                {itemCount > 0 ? `✓ ${itemCount}개 항목` : 'JSON 파싱 오류'}
              </span>
            )}
          </div>
          <textarea
            style={{ ...S.input, height: 130, resize: 'vertical', fontFamily: 'monospace', fontSize: 11 }}
            placeholder={`[\n  {"question_id": 1, "question": "...", "SQL": "SELECT ...", "evidence": "hint", "difficulty": "simple"},\n  ...\n]`}
            value={jsonText}
            onChange={e => setJsonText(e.target.value)}
          />
        </div>

        <div style={{ ...S.row, marginBottom: 14 }}>
          <label style={{ fontSize: 13, display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer' }}>
            <input type="checkbox" checked={skipBaseline} onChange={e => setSkipBaseline(e.target.checked)} />
            pgxllm 단독 평가 (Baseline 건너뜀 — 더 빠름)
          </label>
        </div>
        <div style={{ fontSize: 11, color: 'var(--gray)', marginBottom: 12, lineHeight: 1.6 }}>
          • <b>Baseline</b>: schema_catalog를 읽어 LLM에 직접 1회 호출 (파이프라인 없음, few-shot 없음)<br/>
          • <b>pgxllm</b>: S1→S2→S3→S4 전체 파이프라인 (캐시·그래프·룰 활용)<br/>
          • <b>EX (Execution Accuracy)</b>: 실행 결과셋이 Gold SQL과 일치하는지 순서 무관 비교
        </div>

        {progress && <div style={S.progress}>⏳ {progress}</div>}
        {error    && <div style={{ ...S.error, marginBottom: 10 }}>{error}</div>}

        <button
          style={S.btn(running ? 'var(--gray)' : undefined)}
          disabled={running || !selectedDb}
          onClick={handleRun}
        >
          {running ? '실행 중...' : '▶ 평가 실행'}
        </button>
      </div>

      {/* Latest result summary */}
      {latestResult && (
        <div style={S.card}>
          <div style={{ ...S.row, marginBottom: 12 }}>
            <span style={S.h2}>최근 결과 — <code>{latestResult.eval_name}</code></span>
            <button style={S.btn()} onClick={() => setDetailView({
              db: latestResult.db_alias, evalName: latestResult.eval_name, summary: latestResult
            })}>상세 보기</button>
          </div>
          <div style={S.summary}>
            <div style={S.sumBox}>
              <div style={{ fontSize: 24, fontWeight: 700 }}>{latestResult.total}</div>
              <div style={{ fontSize: 11, color: 'var(--gray)' }}>총 문항</div>
            </div>
            {latestResult.baseline_ex_count != null && (
              <div style={S.sumBox}>
                <div style={{ fontSize: 24, fontWeight: 700 }}>
                  {latestResult.baseline_ex_rate != null ? `${(latestResult.baseline_ex_rate * 100).toFixed(1)}%` : '—'}
                </div>
                <div style={{ fontSize: 11, color: 'var(--gray)' }}>Baseline EX</div>
                <div style={{ fontSize: 11 }}>{latestResult.baseline_ex_count} / {latestResult.total}</div>
              </div>
            )}
            <div style={{ ...S.sumBox, borderColor: 'var(--teal)', background: 'var(--mint)' }}>
              <div style={{ fontSize: 24, fontWeight: 700, color: 'var(--teal)' }}>
                {latestResult.pgxllm_ex_rate != null ? `${(latestResult.pgxllm_ex_rate * 100).toFixed(1)}%` : '—'}
              </div>
              <div style={{ fontSize: 11, color: 'var(--gray)' }}>pgxllm EX</div>
              <div style={{ fontSize: 11 }}>{latestResult.pgxllm_ex_count} / {latestResult.total}</div>
            </div>
          </div>
          {latestResult.by_difficulty && Object.keys(latestResult.by_difficulty).length > 0 && (
            <div>
              <div style={{ fontSize: 12, color: 'var(--gray)', marginBottom: 6, fontWeight: 600 }}>난이도별</div>
              <div style={S.row}>
                {Object.entries(latestResult.by_difficulty).map(([diff, d]) => (
                  <div key={diff} style={{ ...S.sumBox, minWidth: 90 }}>
                    <span style={S.diffBadge(diff)}>{diff}</span>
                    <div style={{ fontSize: 13, fontWeight: 700, marginTop: 4 }}>
                      {pct(d.pgxllm, d.total)}
                    </div>
                    <div style={{ fontSize: 10, color: 'var(--gray)' }}>{d.pgxllm}/{d.total}</div>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}

      {/* History */}
      <HistoryPanel
        db={selectedDb}
        onSelect={r => setDetailView({ db: selectedDb, evalName: r.eval_name, summary: {
          total: r.total,
          baseline_ex_count: r.baseline_ex,
          pgxllm_ex_count:   r.pgxllm_ex,
          baseline_ex_rate:  r.total ? r.baseline_ex / r.total : null,
          pgxllm_ex_rate:    r.total ? r.pgxllm_ex   / r.total : null,
        }})}
      />
    </div>
  )
}
