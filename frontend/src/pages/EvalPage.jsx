/**
 * EvalPage — BIRD 벤치마크 평가 UI
 *
 * 1. Benchmark DB 선택 (db_type='benchmark' 필터)
 * 2. BIRD JSON 업로드 또는 직접 입력
 * 3. Baseline(S1+S3) / Pipeline(S1~S4) 실행 선택
 * 4. 결과 테이블 (EX accuracy, per-question 상세)
 * 5. 이전 평가 목록 조회
 */
import { useState, useEffect, useRef } from 'react'
import { useDb } from '../components/Layout'
import { dbApi, evalApi } from '../api/client'

// ── Styles ────────────────────────────────────────────────────
const S = {
  card:    { background: 'white', borderRadius: 8, border: '1px solid var(--gray2)', padding: 20, marginBottom: 16 },
  h2:      { fontSize: 15, fontWeight: 700, marginBottom: 12, color: 'var(--dark)' },
  label:   { fontSize: 12, color: 'var(--gray)', marginBottom: 4, display: 'block' },
  input:   { width: '100%', padding: '7px 10px', border: '1px solid var(--gray2)', borderRadius: 6, fontSize: 13, outline: 'none', boxSizing: 'border-box' },
  select:  { padding: '7px 10px', border: '1px solid var(--gray2)', borderRadius: 6, fontSize: 13, outline: 'none', background: 'white', cursor: 'pointer' },
  btn:     (color='var(--teal)') => ({
    padding: '7px 16px', background: color, color: 'white', border: 'none',
    borderRadius: 6, fontSize: 13, cursor: 'pointer', fontWeight: 600,
  }),
  btnGhost: { padding: '6px 14px', background: 'white', border: '1px solid var(--gray2)', borderRadius: 6, fontSize: 12, cursor: 'pointer' },
  row:     { display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' },
  tag:     (c='var(--teal)') => ({ background: c, color: 'white', borderRadius: 4, padding: '2px 8px', fontSize: 11, fontWeight: 600 }),
  table:   { width: '100%', borderCollapse: 'collapse', fontSize: 12 },
  th:      { padding: '6px 10px', borderBottom: '2px solid var(--gray2)', textAlign: 'left', color: 'var(--gray)', fontWeight: 600, whiteSpace: 'nowrap' },
  td:      { padding: '6px 10px', borderBottom: '1px solid var(--gray2)', verticalAlign: 'top' },
  green:   { color: '#16a34a', fontWeight: 700 },
  red:     { color: '#dc2626', fontWeight: 700 },
  mono:    { fontFamily: 'monospace', fontSize: 11, whiteSpace: 'pre-wrap', wordBreak: 'break-all' },
  badge:   (ok) => ({
    display: 'inline-block', padding: '1px 7px', borderRadius: 4,
    background: ok === true ? '#dcfce7' : ok === false ? '#fee2e2' : '#f3f4f6',
    color: ok === true ? '#166534' : ok === false ? '#991b1b' : '#374151',
    fontSize: 11, fontWeight: 600,
  }),
  progress: { marginBottom: 12, padding: '10px 14px', background: '#f0fdf4', borderRadius: 6, border: '1px solid #bbf7d0', fontSize: 13 },
  error:   { padding: '10px 14px', background: '#fef2f2', borderRadius: 6, border: '1px solid #fecaca', fontSize: 13, color: '#991b1b' },
  summary: { display: 'flex', gap: 20, flexWrap: 'wrap', marginBottom: 16 },
  sumBox:  { padding: '12px 20px', borderRadius: 8, border: '1px solid var(--gray2)', textAlign: 'center', minWidth: 120 },
}

function pct(n, total) {
  if (!total || n == null) return '—'
  return `${(n / total * 100).toFixed(1)}%`
}

// ── History panel ─────────────────────────────────────────────
function HistoryPanel({ db, onSelect }) {
  const [list, setList]   = useState([])
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
        <button style={S.btnGhost} onClick={load}>{loading ? '...' : '새로고침'}</button>
      </div>
      {list.length === 0 && !loading && <p style={{ color: 'var(--gray)', fontSize: 12 }}>기록 없음</p>}
      {list.length > 0 && (
        <table style={S.table}>
          <thead>
            <tr>
              <th style={S.th}>eval_name</th>
              <th style={S.th}>총 문항</th>
              <th style={S.th}>Baseline EX</th>
              <th style={S.th}>Pipeline EX</th>
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
                  {r.baseline_ex != null ? (
                    <span>{r.baseline_ex} <small style={{ color: 'var(--gray)' }}>({pct(r.baseline_ex, r.total)})</small></span>
                  ) : '—'}
                </td>
                <td style={S.td}>
                  {r.pipeline_ex != null ? (
                    <span style={S.green}>{r.pipeline_ex} <small>({pct(r.pipeline_ex, r.total)})</small></span>
                  ) : '—'}
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
  const [rows, setRows]       = useState([])
  const [loading, setLoading] = useState(true)
  const [expanded, setExpanded] = useState({})

  useEffect(() => {
    evalApi.results(db, evalName).then(setRows).catch(() => setRows([])).finally(() => setLoading(false))
  }, [db, evalName])

  const toggle = (id) => setExpanded(prev => ({ ...prev, [id]: !prev[id] }))

  return (
    <div style={S.card}>
      <div style={{ ...S.row, marginBottom: 16 }}>
        <span style={S.h2}>결과 상세 — <code>{evalName}</code></span>
        <button style={S.btnGhost} onClick={onClose}>← 목록으로</button>
      </div>

      {summary && (
        <div style={S.summary}>
          <div style={S.sumBox}>
            <div style={{ fontSize: 22, fontWeight: 700 }}>{summary.total}</div>
            <div style={{ fontSize: 11, color: 'var(--gray)' }}>총 문항</div>
          </div>
          {summary.baseline_ex != null && (
            <div style={S.sumBox}>
              <div style={{ fontSize: 22, fontWeight: 700 }}>{pct(summary.baseline_ex, summary.total)}</div>
              <div style={{ fontSize: 11, color: 'var(--gray)' }}>Baseline EX</div>
              <div style={{ fontSize: 11 }}>{summary.baseline_ex} / {summary.total}</div>
            </div>
          )}
          {summary.pipeline_ex != null && (
            <div style={{ ...S.sumBox, borderColor: 'var(--teal)', background: 'var(--mint)' }}>
              <div style={{ fontSize: 22, fontWeight: 700, color: 'var(--teal)' }}>{pct(summary.pipeline_ex, summary.total)}</div>
              <div style={{ fontSize: 11, color: 'var(--gray)' }}>Pipeline EX</div>
              <div style={{ fontSize: 11 }}>{summary.pipeline_ex} / {summary.total}</div>
            </div>
          )}
        </div>
      )}

      {loading && <p style={{ color: 'var(--gray)', fontSize: 12 }}>로딩 중...</p>}
      <table style={S.table}>
        <thead>
          <tr>
            <th style={S.th}>#</th>
            <th style={S.th}>질문</th>
            <th style={S.th}>Baseline EX</th>
            <th style={S.th}>Pipeline EX</th>
            <th style={S.th}>ms</th>
            <th style={S.th}></th>
          </tr>
        </thead>
        <tbody>
          {rows.map(r => (
            <>
              <tr key={r.id}>
                <td style={S.td}>{r.question_id ?? '—'}</td>
                <td style={{ ...S.td, maxWidth: 280 }}>{r.question}</td>
                <td style={S.td}><span style={S.badge(r.baseline_ex)}>{r.baseline_ex === true ? '✓' : r.baseline_ex === false ? '✗' : '—'}</span></td>
                <td style={S.td}><span style={S.badge(r.pipeline_ex)}>{r.pipeline_ex === true ? '✓' : r.pipeline_ex === false ? '✗' : '—'}</span></td>
                <td style={S.td}>{r.duration_ms ?? '—'}</td>
                <td style={S.td}>
                  <button style={S.btnGhost} onClick={() => toggle(r.id)}>
                    {expanded[r.id] ? '접기' : '상세'}
                  </button>
                </td>
              </tr>
              {expanded[r.id] && (
                <tr key={`${r.id}-detail`}>
                  <td colSpan={6} style={{ ...S.td, background: '#f8fafc' }}>
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 12 }}>
                      <div>
                        <div style={{ fontWeight: 600, fontSize: 11, marginBottom: 4, color: 'var(--gray)' }}>Gold SQL</div>
                        <pre style={{ ...S.mono, background: '#f1f5f9', padding: 8, borderRadius: 4, margin: 0 }}>{r.gold_sql || '—'}</pre>
                      </div>
                      <div>
                        <div style={{ fontWeight: 600, fontSize: 11, marginBottom: 4, color: '#92400e' }}>Baseline SQL</div>
                        <pre style={{ ...S.mono, background: '#fffbeb', padding: 8, borderRadius: 4, margin: 0 }}>{r.baseline_sql || '—'}</pre>
                        {r.error_baseline && <div style={{ color: '#dc2626', fontSize: 11, marginTop: 4 }}>{r.error_baseline}</div>}
                      </div>
                      <div>
                        <div style={{ fontWeight: 600, fontSize: 11, marginBottom: 4, color: 'var(--teal)' }}>Pipeline SQL</div>
                        <pre style={{ ...S.mono, background: '#f0fdf4', padding: 8, borderRadius: 4, margin: 0 }}>{r.pipeline_sql || '—'}</pre>
                        {r.error_pipeline && <div style={{ color: '#dc2626', fontSize: 11, marginTop: 4 }}>{r.error_pipeline}</div>}
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
  )
}

// ── Main page ──────────────────────────────────────────────────
export default function EvalPage() {
  const { dbs } = useDb()
  const benchmarkDbs = dbs.filter(d => d.db_type === 'benchmark')

  const [selectedDb,    setSelectedDb]    = useState('')
  const [evalName,      setEvalName]      = useState('')
  const [runBaseline,   setRunBaseline]   = useState(true)
  const [runPipeline,   setRunPipeline]   = useState(true)
  const [jsonText,      setJsonText]      = useState('')
  const [running,       setRunning]       = useState(false)
  const [progress,      setProgress]      = useState('')
  const [error,         setError]         = useState('')
  const [latestResult,  setLatestResult]  = useState(null)
  const [detailView,    setDetailView]    = useState(null)  // {db, evalName, summary}
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

  const handleRun = async () => {
    if (!selectedDb) { setError('Benchmark DB를 선택하세요.'); return }
    if (!jsonText.trim()) { setError('BIRD JSON 데이터를 입력하세요.'); return }
    if (!runBaseline && !runPipeline) { setError('Baseline 또는 Pipeline 중 하나 이상 선택하세요.'); return }

    let items
    try {
      const parsed = JSON.parse(jsonText)
      items = Array.isArray(parsed) ? parsed : parsed.data || parsed.questions || []
      // normalize field names
      items = items.map((it, idx) => ({
        question_id: it.question_id ?? it.id ?? idx,
        question:    it.question || it.question_text || '',
        gold_sql:    it.SQL || it.gold_sql || it.sql || '',
      })).filter(it => it.question && it.gold_sql)
    } catch (e) {
      setError(`JSON 파싱 오류: ${e.message}`)
      return
    }
    if (items.length === 0) { setError('유효한 항목이 없습니다. question/SQL 필드를 확인하세요.'); return }

    setRunning(true)
    setError('')
    setProgress(`${items.length}개 항목 실행 중...`)
    setLatestResult(null)

    try {
      const name = evalName.trim() || `eval_${new Date().toISOString().slice(0,16).replace('T','_')}`
      const res = await evalApi.run({
        db_alias:     selectedDb,
        eval_name:    name,
        items,
        run_baseline: runBaseline,
        run_pipeline: runPipeline,
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
    <div style={{ maxWidth: 1100 }}>

      {/* Run panel */}
      <div style={S.card}>
        <div style={S.h2}>새 평가 실행</div>

        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14, marginBottom: 14 }}>
          <div>
            <label style={S.label}>Benchmark DB</label>
            {benchmarkDbs.length === 0 ? (
              <p style={{ fontSize: 12, color: '#dc2626' }}>
                등록된 Benchmark DB가 없습니다. DB 관리에서 db_type=benchmark로 등록하세요.
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
              placeholder="예: bird_dev_v1 (비워두면 자동 생성)"
              value={evalName}
              onChange={e => setEvalName(e.target.value)}
            />
          </div>
        </div>

        <div style={{ marginBottom: 14 }}>
          <label style={S.label}>BIRD JSON 파일 업로드 또는 직접 붙여넣기</label>
          <div style={{ ...S.row, marginBottom: 8 }}>
            <input type="file" accept=".json" ref={fileRef} style={{ display: 'none' }} onChange={handleFileLoad} />
            <button style={S.btnGhost} onClick={() => fileRef.current?.click()}>📁 파일 선택</button>
            {jsonText && <span style={{ fontSize: 12, color: 'var(--gray)' }}>
              {(() => { try { const a = JSON.parse(jsonText); const items = Array.isArray(a) ? a : a.data || a.questions || []; return `${items.length}개 항목` } catch { return 'JSON 파싱 오류' } })()}
            </span>}
          </div>
          <textarea
            style={{ ...S.input, height: 120, resize: 'vertical', fontFamily: 'monospace', fontSize: 11 }}
            placeholder={`[{"question_id": 1, "question": "...", "SQL": "SELECT ..."},\n ...]`}
            value={jsonText}
            onChange={e => setJsonText(e.target.value)}
          />
        </div>

        <div style={{ ...S.row, marginBottom: 14 }}>
          <label style={{ fontSize: 13, display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer' }}>
            <input type="checkbox" checked={runBaseline} onChange={e => setRunBaseline(e.target.checked)} />
            Baseline (S1+S2+S3, 검증 없음)
          </label>
          <label style={{ fontSize: 13, display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer' }}>
            <input type="checkbox" checked={runPipeline} onChange={e => setRunPipeline(e.target.checked)} />
            Pipeline (전체 S1~S4)
          </label>
        </div>

        {progress && <div style={S.progress}>⏳ {progress}</div>}
        {error   && <div style={{ ...S.error, marginBottom: 10 }}>{error}</div>}

        <button
          style={S.btn(running ? 'var(--gray)' : 'var(--teal)')}
          disabled={running}
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
            <button
              style={S.btn()}
              onClick={() => setDetailView({ db: latestResult.db_alias, evalName: latestResult.eval_name, summary: latestResult })}
            >
              상세 보기
            </button>
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
            {latestResult.pipeline_ex_count != null && (
              <div style={{ ...S.sumBox, borderColor: 'var(--teal)', background: 'var(--mint)' }}>
                <div style={{ fontSize: 24, fontWeight: 700, color: 'var(--teal)' }}>
                  {latestResult.pipeline_ex_rate != null ? `${(latestResult.pipeline_ex_rate * 100).toFixed(1)}%` : '—'}
                </div>
                <div style={{ fontSize: 11, color: 'var(--gray)' }}>Pipeline EX</div>
                <div style={{ fontSize: 11 }}>{latestResult.pipeline_ex_count} / {latestResult.total}</div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* History */}
      <HistoryPanel
        db={selectedDb}
        onSelect={r => setDetailView({ db: selectedDb, evalName: r.eval_name, summary: r })}
      />
    </div>
  )
}
