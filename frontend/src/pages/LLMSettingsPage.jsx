import { useState, useEffect } from 'react'
import { llmApi } from '../api/client'
import { Card, CardHeader, CardBody, Btn, Spinner } from '../components/UI'
import { useLlm } from '../components/Layout'

const FIELD_LABELS = {
  base_url:   { label: 'Endpoint URL', placeholder: 'http://...' },
  model:      { label: '모델 ID',      placeholder: '모델명 입력' },
  api_key:    { label: 'API Key',      placeholder: '••••••••', type: 'password' },
  project_id: { label: 'Project ID',   placeholder: 'watsonx.ai 프로젝트 ID' },
  username:   { label: '사용자명 (CP4D)', placeholder: 'CP4D 사용자명 (IBM Cloud 시 비워두기)' },
}

const PARAM_FIELDS = [
  { key: 'timeout',     label: 'Timeout (초)',   type: 'number', min: 10,  max: 3600, step: 10  },
  { key: 'max_tokens',  label: 'Max Tokens',     type: 'number', min: 256, max: 8192, step: 256 },
  { key: 'temperature', label: 'Temperature',    type: 'number', min: 0,   max: 2,    step: 0.05 },
]

const PROVIDER_LABELS = {
  ollama: 'Ollama (Local)', vllm: 'vLLM', lmstudio: 'LM Studio',
  openai: 'OpenAI', anthropic: 'Anthropic Claude',
  watsonx: 'IBM watsonx.ai',
}

export default function LLMSettingsPage() {
  const { llm: activeLlm, reloadLlm } = useLlm()
  const [providers, setProviders] = useState([])
  const [form, setForm]           = useState({
    provider: 'ollama', base_url: '', model: '', api_key: '',
    project_id: '', username: '', verify_ssl: true,
    timeout: 600, max_tokens: 2048, temperature: 0.0,
  })
  const [loading, setLoading]   = useState(true)
  const [saving, setSaving]     = useState(false)
  const [testing, setTesting]   = useState(false)
  const [saveMsg, setSaveMsg]   = useState(null)  // { type, text }
  const [testMsg, setTestMsg]   = useState(null)
  const [showKey, setShowKey]   = useState(false)

  useEffect(() => {
    Promise.all([llmApi.providers(), llmApi.getConfig()])
      .then(([pvList, cfg]) => {
        setProviders(pvList)
        setForm(f => ({
          ...f,
          provider:    cfg.provider,
          base_url:    cfg.base_url   || '',
          model:       cfg.model      || '',
          api_key:     cfg.api_key_set ? '••••••••' : '',
          project_id:  cfg.project_id  || '',
          username:    cfg.username    || '',
          verify_ssl:  cfg.verify_ssl  ?? true,
          timeout:     cfg.timeout,
          max_tokens:  cfg.max_tokens,
          temperature: cfg.temperature,
        }))
      })
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [])

  const currentProvider = providers.find(p => p.id === form.provider)

  function onProviderChange(id) {
    const pv = providers.find(p => p.id === id)
    if (!pv) return
    setForm(f => ({
      ...f,
      provider:   id,
      base_url:   pv.default_base_url || '',
      model:      pv.default_model    || '',
      api_key:    '',
      project_id: '',
      username:   '',
      verify_ssl: true,
    }))
    setSaveMsg(null)
    setTestMsg(null)
  }

  function onField(key, val) {
    setForm(f => ({ ...f, [key]: val }))
  }

  async function handleTest() {
    setTesting(true); setTestMsg(null)
    try {
      const res = await llmApi.test(form)
      setTestMsg(res.ok
        ? { type: 'ok',    text: `연결 성공 — 모델: ${res.model}` }
        : { type: 'error', text: res.error || '연결 실패 (오류 메시지 없음)' }
      )
    } catch (e) {
      let detail = ''
      if (e.response?.data?.detail) {
        detail = e.response.data.detail
      } else if (e.code === 'ECONNABORTED' || e.code === 'ERR_CANCELED') {
        detail = '요청 시간 초과 (timeout)'
      } else if (e.code === 'ERR_NETWORK' || e.message === 'Network Error') {
        detail = `백엔드 서버에 연결할 수 없습니다 (${e.config?.url || '/api/llm/test'})`
      } else {
        detail = e.message || '알 수 없는 오류'
      }
      setTestMsg({ type: 'error', text: detail })
    } finally {
      setTesting(false)
    }
  }

  async function handleSave() {
    setSaving(true); setSaveMsg(null)
    try {
      await llmApi.saveConfig(form)
      setSaveMsg({ type: 'ok', text: '✅ 저장 완료' })
      reloadLlm()
    } catch (e) {
      const detail = e.response?.data?.detail || e.message || '저장 실패'
      setSaveMsg({ type: 'error', text: detail })
    } finally {
      setSaving(false)
    }
  }

  if (loading) return <div style={{ padding: 24, color: 'var(--gray)' }}>로딩 중...</div>

  return (
    <div>
      {/* Current active LLM card */}
      {activeLlm && (
        <Card style={{ borderLeft: '4px solid var(--teal)', marginBottom: 16 }}>
          <CardBody style={{ padding: '12px 16px' }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--gray)', textTransform: 'uppercase', letterSpacing: 0.5, marginBottom: 8 }}>
              현재 활성 LLM
            </div>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 20, alignItems: 'center' }}>
              <div>
                <span style={{ fontSize: 11, color: 'var(--gray)' }}>Provider</span>
                <div style={{ fontWeight: 700, fontSize: 14, color: 'var(--teal)' }}>
                  🤖 {PROVIDER_LABELS[activeLlm.provider] || activeLlm.provider}
                </div>
              </div>
              <div>
                <span style={{ fontSize: 11, color: 'var(--gray)' }}>Model</span>
                <div style={{ fontWeight: 600, fontSize: 13, fontFamily: 'monospace', color: 'var(--navy)' }}>
                  {activeLlm.model || '—'}
                </div>
              </div>
              {activeLlm.base_url && (
                <div>
                  <span style={{ fontSize: 11, color: 'var(--gray)' }}>Endpoint</span>
                  <div style={{ fontSize: 12, fontFamily: 'monospace', color: 'var(--dark)' }}>
                    {activeLlm.base_url}
                  </div>
                </div>
              )}
              {activeLlm.project_id && (
                <div>
                  <span style={{ fontSize: 11, color: 'var(--gray)' }}>Project ID</span>
                  <div style={{ fontSize: 12, fontFamily: 'monospace', color: 'var(--dark)' }}>
                    {activeLlm.project_id}
                  </div>
                </div>
              )}
              {activeLlm.api_key_set && (
                <div>
                  <span style={{ fontSize: 11, color: 'var(--gray)' }}>API Key</span>
                  <div style={{ fontSize: 12, color: 'var(--teal)' }}>✅ 설정됨</div>
                </div>
              )}
              <div style={{ marginLeft: 'auto', display: 'flex', gap: 16, color: 'var(--gray)', fontSize: 11 }}>
                <span>timeout: <b style={{ color: 'var(--dark)' }}>{activeLlm.timeout}s</b></span>
                <span>max_tokens: <b style={{ color: 'var(--dark)' }}>{activeLlm.max_tokens}</b></span>
                <span>temp: <b style={{ color: 'var(--dark)' }}>{activeLlm.temperature}</b></span>
              </div>
            </div>
          </CardBody>
        </Card>
      )}

      <Card>
        <CardHeader>
          🤖 LLM 설정
          <span style={{ marginLeft: 'auto', fontSize: 11, color: 'var(--gray)', fontWeight: 400 }}>
            변경 사항은 즉시 적용됩니다 (서버 재시작 불필요)
          </span>
        </CardHeader>
        <CardBody>

          {/* Provider selector */}
          <div style={{ marginBottom: 20 }}>
            <label style={labelStyle}>LLM Provider</label>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginTop: 6 }}>
              {providers.map(pv => (
                <button
                  key={pv.id}
                  onClick={() => onProviderChange(pv.id)}
                  style={{
                    padding: '7px 14px', borderRadius: 6, fontSize: 12, fontWeight: 600,
                    cursor: 'pointer', transition: 'all 0.15s', border: '1px solid',
                    ...(form.provider === pv.id
                      ? { background: 'var(--teal)', color: 'white', borderColor: 'var(--teal)' }
                      : { background: 'white', color: 'var(--dark)', borderColor: 'var(--gray2)' }
                    ),
                  }}
                >
                  {pv.label}
                </button>
              ))}
            </div>
            {currentProvider?.hint && (
              <div style={{ marginTop: 6, fontSize: 11, color: 'var(--gray)' }}>
                {currentProvider.hint}
              </div>
            )}
          </div>

          {/* Dynamic connection fields */}
          {currentProvider && (
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px 20px', marginBottom: 20 }}>
              {currentProvider.fields.map(fk => {
                const meta   = FIELD_LABELS[fk] || { label: fk, placeholder: '' }
                const isPass = meta.type === 'password'
                const reveal = isPass && showKey
                return (
                  <div key={fk} style={fk === 'base_url' ? { gridColumn: '1 / -1' } : {}}>
                    <label style={labelStyle}>{meta.label}</label>
                    <div style={{ position: 'relative' }}>
                      <input
                        type={isPass && !reveal ? 'password' : 'text'}
                        value={form[fk]}
                        onChange={e => onField(fk, e.target.value)}
                        placeholder={meta.placeholder}
                        style={{ ...inputStyle, paddingRight: isPass ? 32 : undefined }}
                        autoComplete={isPass ? 'new-password' : 'off'}
                        onFocus={e => {
                          e.target.style.borderColor = 'var(--teal)'
                          if (isPass && form[fk].startsWith('•')) onField(fk, '')
                        }}
                        onBlur={e => e.target.style.borderColor = 'var(--gray2)'}
                      />
                      {isPass && (
                        <button
                          type="button"
                          onClick={() => setShowKey(v => !v)}
                          title={showKey ? '숨기기' : '표시'}
                          style={{
                            position: 'absolute', right: 6, top: '50%', transform: 'translateY(-50%)',
                            background: 'none', border: 'none', cursor: 'pointer',
                            fontSize: 14, color: 'var(--gray)', padding: '2px 4px', lineHeight: 1,
                          }}
                        >
                          {showKey ? '🙈' : '👁'}
                        </button>
                      )}
                    </div>
                  </div>
                )
              })}
            </div>
          )}

          {/* Parameter fields */}
          <div style={{ borderTop: '1px solid var(--gray2)', paddingTop: 16, marginBottom: 16 }}>
            <div style={{ fontSize: 11, fontWeight: 700, color: 'var(--gray)', marginBottom: 10, textTransform: 'uppercase', letterSpacing: 0.5 }}>
              생성 파라미터
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '12px 20px' }}>
              {PARAM_FIELDS.map(({ key, label, min, max, step }) => (
                <div key={key}>
                  <label style={labelStyle}>{label}</label>
                  <input
                    type="number"
                    value={form[key]}
                    min={min} max={max} step={step}
                    onChange={e => onField(key, parseFloat(e.target.value))}
                    style={inputStyle}
                    onFocus={e => e.target.style.borderColor = 'var(--teal)'}
                    onBlur={e => e.target.style.borderColor = 'var(--gray2)'}
                  />
                </div>
              ))}
            </div>
          </div>

          {/* SSL 검증 */}
          <div style={{ marginBottom: 16 }}>
            <label style={{ display: 'flex', alignItems: 'center', gap: 8, cursor: 'pointer', userSelect: 'none' }}>
              <input
                type="checkbox"
                checked={form.verify_ssl}
                onChange={e => onField('verify_ssl', e.target.checked)}
                style={{ width: 15, height: 15, cursor: 'pointer' }}
              />
              <span style={{ fontSize: 12, color: 'var(--dark)' }}>SSL 인증서 검증</span>
              {!form.verify_ssl && (
                <span style={{ fontSize: 11, color: '#e67e22', background: '#fef9f0', border: '1px solid #f0d080', borderRadius: 4, padding: '1px 6px' }}>
                  ⚠️ self-signed 허용 (on-prem 전용)
                </span>
              )}
            </label>
          </div>

          {/* Actions */}
          <div style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: (testMsg || saveMsg) ? 10 : 0 }}>
            <Btn onClick={handleTest} disabled={testing || saving} variant="secondary">
              {testing ? <><Spinner size={12} style={{ borderTopColor: 'var(--teal)' }} /> 테스트 중...</> : '🔌 연결 테스트'}
            </Btn>
            <Btn onClick={handleSave} disabled={saving || testing}>
              {saving ? <><Spinner /> 저장 중...</> : '💾 저장'}
            </Btn>
          </div>

          {/* Result messages */}
          {testMsg && (
            <div style={{
              padding: '8px 12px',
              borderRadius: 6,
              fontSize: 12,
              lineHeight: 1.6,
              wordBreak: 'break-all',
              background: testMsg.type === 'ok' ? '#f0fdf4' : '#fff5f5',
              color:      testMsg.type === 'ok' ? 'var(--teal)' : '#c0392b',
              border:     `1px solid ${testMsg.type === 'ok' ? '#bbf7d0' : '#fecaca'}`,
              marginBottom: saveMsg ? 6 : 0,
            }}>
              {testMsg.type === 'ok' ? '✅ ' : '❌ '}{testMsg.text}
            </div>
          )}
          {saveMsg && (
            <div style={{
              padding: '8px 12px',
              borderRadius: 6,
              fontSize: 12,
              lineHeight: 1.6,
              wordBreak: 'break-all',
              background: saveMsg.type === 'ok' ? '#f0fdf4' : '#fff5f5',
              color:      saveMsg.type === 'ok' ? 'var(--teal)' : '#c0392b',
              border:     `1px solid ${saveMsg.type === 'ok' ? '#bbf7d0' : '#fecaca'}`,
            }}>
              {saveMsg.type === 'ok' ? '✅ ' : '❌ '}{saveMsg.text}
            </div>
          )}

        </CardBody>
      </Card>

      {/* Provider reference card */}
      <Card>
        <CardHeader>📋 Provider 참고 정보</CardHeader>
        <CardBody style={{ padding: 0 }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
            <thead>
              <tr style={{ background: 'var(--gray3)' }}>
                {['Provider', '기본 Endpoint', '대표 모델', '인증'].map(h => (
                  <th key={h} style={{ padding: '6px 12px', textAlign: 'left', fontWeight: 600, color: 'var(--gray)', fontSize: 11 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {[
                ['Ollama',           'http://localhost:11434',               'qwen2.5-coder:7b',                   '없음'],
                ['vLLM',             'http://localhost:8001/v1',             '배포한 모델명',                        'API Key (선택)'],
                ['LM Studio',        'http://localhost:1234/v1',             '로드된 모델명',                        '없음'],
                ['OpenAI',           'https://api.openai.com/v1',           'gpt-4o, gpt-4-turbo',               'OPENAI_API_KEY'],
                ['Anthropic',        'https://api.anthropic.com',           'claude-3-5-sonnet-20241022',         'ANTHROPIC_API_KEY'],
                ['IBM watsonx.ai', 'https://{region}.ml.cloud.ibm.com', 'ibm/granite-34b-code-instruct', 'IBM API Key + Project ID'],
              ].map(([pv, url, model, auth]) => (
                <tr key={pv} style={{ borderTop: '1px solid var(--gray2)' }}>
                  <td style={{ padding: '6px 12px', fontWeight: 600 }}>{pv}</td>
                  <td style={{ padding: '6px 12px', fontFamily: 'monospace', fontSize: 11, color: 'var(--teal)' }}>{url}</td>
                  <td style={{ padding: '6px 12px', fontFamily: 'monospace', fontSize: 11 }}>{model}</td>
                  <td style={{ padding: '6px 12px', color: 'var(--gray)' }}>{auth}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <div style={{ padding: '10px 12px', fontSize: 11, color: 'var(--gray)', borderTop: '1px solid var(--gray2)' }}>
            💡 watsonx.ai 지역: <code>us-south</code> | <code>eu-de</code> | <code>eu-gb</code> | <code>jp-tok</code> | <code>au-syd</code>
            &nbsp;&nbsp;예: <code>https://eu-de.ml.cloud.ibm.com</code>
          </div>
        </CardBody>
      </Card>
    </div>
  )
}

const labelStyle = {
  display: 'block', fontSize: 11, fontWeight: 700, color: 'var(--gray)',
  marginBottom: 4, textTransform: 'uppercase', letterSpacing: 0.4,
}

const inputStyle = {
  width: '100%', padding: '7px 10px', border: '1px solid var(--gray2)',
  borderRadius: 6, fontSize: 13, outline: 'none', boxSizing: 'border-box',
  fontFamily: 'inherit', color: 'var(--dark)', background: 'white',
}
