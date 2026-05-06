import { useState } from 'react'
import { useDb } from '../components/Layout'
import { dbApi } from '../api/client'
import { Card, CardHeader, CardBody, Btn, Alert } from '../components/UI'

export default function DbsPage() {
  const { dbs, setDb, refreshDbs } = useDb()
  const [showForm, setShowForm] = useState(false)
  const [refreshing, setRefreshing] = useState({})
  const [msg, setMsg]   = useState('')
  const [err, setErr]   = useState('')

  // Form state
  const [form, setForm] = useState({
    alias: '', host: 'localhost', port: 5432,
    user: 'postgres', password: '', dbname: '',
    schema_mode: 'exclude',
    schemas: 'pg_catalog,information_schema,pg_toast',
  })

  const set = (k, v) => setForm(f => ({ ...f, [k]: v }))

  async function handleRegister(e) {
    e.preventDefault()
    setErr('')
    if (!form.alias || !form.host) { setErr('alias, host는 필수입니다.'); return }
    try {
      await dbApi.register({
        ...form,
        port: Number(form.port),
        schemas: form.schemas.split(',').map(s => s.trim()).filter(Boolean),
        dbname: form.dbname || null,
      })
      setMsg(`✅ '${form.alias}' 등록 완료`)
      setShowForm(false)
      refreshDbs()
      setForm({ alias:'', host:'localhost', port:5432, user:'postgres', password:'', dbname:'', schema_mode:'exclude', schemas:'pg_catalog,information_schema,pg_toast' })
    } catch (e) {
      setErr(e.response?.data?.detail || e.message)
    }
  }

  async function handleRefresh(alias) {
    setRefreshing(r => ({ ...r, [alias]: true }))
    setMsg(''); setErr('')
    try {
      const res = await dbApi.refresh(alias)
      if (res.ok) setMsg(`✅ ${res.summary}`)
      else        setErr(`❌ ${res.error || res.summary}`)
      refreshDbs()
    } catch (e) {
      setErr(e.message)
    } finally {
      setRefreshing(r => ({ ...r, [alias]: false }))
    }
  }

  const inputStyle = {
    padding:'7px 10px', border:'1px solid var(--gray2)',
    borderRadius:6, fontSize:13, outline:'none', width:'100%',
  }

  return (
    <div>
      {msg && <Alert type="ok">{msg}</Alert>}
      {err && <Alert type="error">{err}</Alert>}

      <div style={{ display:'flex', gap:8, marginBottom:14 }}>
        <Btn onClick={() => { setShowForm(s=>!s); setErr('') }}>
          {showForm ? '✕ 닫기' : '+ DB 등록'}
        </Btn>
        <Btn variant="secondary" size="sm" onClick={() => { refreshDbs(); setMsg('') }}>
          ↻ 새로고침
        </Btn>
      </div>

      {/* Register form */}
      {showForm && (
        <Card style={{ marginBottom:20 }}>
          <CardHeader>🗄 Target DB 등록</CardHeader>
          <CardBody>
            <form onSubmit={handleRegister}>
              {/* Row 1 */}
              <div style={{ display:'flex', gap:10, marginBottom:10 }}>
                <div style={{ flex:1 }}>
                  <label style={{ fontSize:11, fontWeight:600, color:'var(--gray)', display:'block', marginBottom:4 }}>ALIAS *</label>
                  <input style={inputStyle} value={form.alias} onChange={e=>set('alias',e.target.value)} placeholder="mydb" required />
                </div>
                <div style={{ flex:2 }}>
                  <label style={{ fontSize:11, fontWeight:600, color:'var(--gray)', display:'block', marginBottom:4 }}>HOST *</label>
                  <input style={inputStyle} value={form.host} onChange={e=>set('host',e.target.value)} placeholder="localhost" required />
                </div>
                <div style={{ width:90 }}>
                  <label style={{ fontSize:11, fontWeight:600, color:'var(--gray)', display:'block', marginBottom:4 }}>PORT</label>
                  <input style={inputStyle} value={form.port} onChange={e=>set('port',e.target.value)} type="number" />
                </div>
              </div>
              {/* Row 2 */}
              <div style={{ display:'flex', gap:10, marginBottom:10 }}>
                <div style={{ flex:1 }}>
                  <label style={{ fontSize:11, fontWeight:600, color:'var(--gray)', display:'block', marginBottom:4 }}>USER</label>
                  <input style={inputStyle} value={form.user} onChange={e=>set('user',e.target.value)} />
                </div>
                <div style={{ flex:1 }}>
                  <label style={{ fontSize:11, fontWeight:600, color:'var(--gray)', display:'block', marginBottom:4 }}>PASSWORD</label>
                  <input style={inputStyle} value={form.password} onChange={e=>set('password',e.target.value)} type="password" placeholder="(생략 가능)" />
                </div>
                <div style={{ flex:1 }}>
                  <label style={{ fontSize:11, fontWeight:600, color:'var(--gray)', display:'block', marginBottom:4 }}>DB NAME</label>
                  <input style={inputStyle} value={form.dbname} onChange={e=>set('dbname',e.target.value)} placeholder="(alias와 동일)" />
                </div>
              </div>
              {/* Row 3 */}
              <div style={{ display:'flex', gap:10, marginBottom:14 }}>
                <div style={{ flex:1 }}>
                  <label style={{ fontSize:11, fontWeight:600, color:'var(--gray)', display:'block', marginBottom:4 }}>SCHEMA MODE</label>
                  <select style={inputStyle} value={form.schema_mode} onChange={e=>set('schema_mode',e.target.value)}>
                    <option value="exclude">exclude — 전체 스캔 - 제외 목록</option>
                    <option value="include">include — 명시 목록만</option>
                  </select>
                </div>
                <div style={{ flex:2 }}>
                  <label style={{ fontSize:11, fontWeight:600, color:'var(--gray)', display:'block', marginBottom:4 }}>SCHEMAS (comma-separated)</label>
                  <input style={inputStyle} value={form.schemas} onChange={e=>set('schemas',e.target.value)} />
                </div>
              </div>
              {err && <Alert type="error">{err}</Alert>}
              <div style={{ display:'flex', gap:8 }}>
                <Btn type="submit">등록</Btn>
                <Btn variant="secondary" onClick={() => setShowForm(false)}>취소</Btn>
              </div>
            </form>
          </CardBody>
        </Card>
      )}

      {/* DB list */}
      {dbs.length === 0 && (
        <Alert type="info">등록된 Target DB가 없습니다. "+ DB 등록" 버튼으로 추가하세요.</Alert>
      )}

      {dbs.map(db => (
        <div key={db.alias} style={{
          background:'white', border:'1px solid var(--gray2)', borderRadius:8,
          padding:'14px 16px', marginBottom:10,
          display:'flex', alignItems:'center', gap:12,
        }}>
          <div style={{
            width:40, height:40, background:'var(--mint)', borderRadius:8,
            display:'flex', alignItems:'center', justifyContent:'center', fontSize:20, flexShrink:0,
          }}>🗄</div>

          <div style={{ flex:1 }}>
            <div style={{ fontWeight:700, fontSize:14 }}>{db.alias}</div>
            <div style={{ fontSize:12, color:'var(--gray)', marginTop:2 }}>
              {db.host}:{db.port} / {db.dbname}
            </div>
            <div style={{ fontSize:11, marginTop:3, display:'flex', gap:10 }}>
              <span style={{ color: db.is_reachable ? 'var(--green)' : 'var(--red)' }}>
                {db.is_reachable ? '✔ 연결됨' : '✘ 연결 불가'}
              </span>
              <span style={{ color:'var(--gray)' }}>{db.schema_mode}</span>
              {db.last_refresh_at && (
                <span style={{ color:'var(--gray)' }}>마지막 refresh: {db.last_refresh_at.substring(0,16)}</span>
              )}
              {db.schema_version_hash && (
                <span style={{ color:'var(--gray)', fontFamily:'monospace' }}>#{db.schema_version_hash}</span>
              )}
            </div>
          </div>

          <div style={{ display:'flex', gap:6 }}>
            <Btn
              variant="secondary" size="sm"
              onClick={() => handleRefresh(db.alias)}
              disabled={refreshing[db.alias]}
            >
              {refreshing[db.alias] ? '...' : '↻ Refresh'}
            </Btn>
            <Btn variant="secondary" size="sm" onClick={() => setDb(db.alias)}>
              선택
            </Btn>
          </div>
        </div>
      ))}
    </div>
  )
}
