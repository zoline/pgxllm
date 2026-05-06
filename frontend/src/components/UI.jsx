// ── Shared UI primitives ──────────────────────────────────

export function Card({ children, style }) {
  return (
    <div style={{ background:'white', borderRadius:8, border:'1px solid var(--gray2)', overflow:'hidden', marginBottom:16, ...style }}>
      {children}
    </div>
  )
}

export function CardHeader({ children, style }) {
  return (
    <div style={{ padding:'11px 16px', borderBottom:'1px solid var(--gray2)', fontWeight:600, fontSize:13, display:'flex', alignItems:'center', gap:8, ...style }}>
      {children}
    </div>
  )
}

export function CardBody({ children, style }) {
  return <div style={{ padding:16, ...style }}>{children}</div>
}

// size별 고정 높이로 동일 컨텍스트 버튼 높이를 일치시킴
// 모든 variant는 border를 명시해 1px 차이로 인한 높이 불일치 방지
const BTN_SIZE = {
  sm: { height: 28, padding: '0 10px', fontSize: 11 },
  md: { height: 34, padding: '0 14px', fontSize: 12 },
  lg: { height: 40, padding: '0 18px', fontSize: 13 },
}
const BTN_VARIANT = {
  primary:   { background:'var(--teal)',   color:'white',        border:'1px solid transparent' },
  secondary: { background:'white',         color:'var(--dark)',  border:'1px solid var(--gray2)' },
  danger:    { background:'var(--red2)',   color:'var(--red)',   border:'1px solid #fca5a5' },
  ghost:     { background:'transparent',  color:'var(--teal)',  border:'1px solid var(--teal)' },
  navy:      { background:'var(--navy)',   color:'white',        border:'1px solid transparent' },
  blue:      { background:'#0369a1',      color:'white',        border:'1px solid transparent' },
  red:       { background:'#dc2626',      color:'white',        border:'1px solid transparent' },
}

export function Btn({ children, onClick, variant='primary', size='md', disabled, style }) {
  const s = BTN_SIZE[size] || BTN_SIZE.md
  const v = BTN_VARIANT[variant] || BTN_VARIANT.primary
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        height: s.height, padding: s.padding, fontSize: s.fontSize,
        fontWeight: 600, borderRadius: 6,
        cursor: disabled ? 'not-allowed' : 'pointer',
        display: 'inline-flex', alignItems: 'center', justifyContent: 'center', gap: 5,
        whiteSpace: 'nowrap', boxSizing: 'border-box', lineHeight: 1,
        opacity: disabled ? 0.5 : 1, transition: 'all 0.15s',
        ...v, ...style,
      }}
    >
      {children}
    </button>
  )
}

export function Badge({ children, color='teal', title }) {
  const colors = {
    teal:   { background:'var(--mint)',    color:'var(--teal)' },
    orange: { background:'var(--orange2)', color:'var(--orange)' },
    green:  { background:'var(--green2)',  color:'var(--green)' },
    red:    { background:'var(--red2)',    color:'var(--red)' },
    gray:   { background:'var(--gray2)',   color:'var(--gray)' },
    purple: { background:'var(--purple2)', color:'var(--purple)' },
  }
  return (
    <span title={title} style={{ padding:'2px 8px', borderRadius:10, fontSize:11, fontWeight:700, ...colors[color] }}>
      {children}
    </span>
  )
}

export function CountBadge({ children }) {
  return <span style={{ background:'var(--gray2)', color:'var(--gray)', padding:'1px 8px', borderRadius:10, fontSize:11, fontWeight:600, marginLeft:'auto' }}>{children}</span>
}

export function Alert({ children, type='info' }) {
  const colors = {
    info:  { background:'var(--mint)',    color:'var(--teal)',   border:'1px solid #99f6e4' },
    ok:    { background:'var(--green2)', color:'var(--green)',  border:'1px solid #6ee7b7' },
    error: { background:'var(--red2)',   color:'var(--red)',    border:'1px solid #fca5a5' },
    warn:  { background:'var(--orange2)',color:'var(--orange)', border:'1px solid #fcd34d' },
  }
  return <div style={{ padding:'10px 14px', borderRadius:6, fontSize:13, marginBottom:12, ...colors[type] }}>{children}</div>
}

export function Spinner({ size=14 }) {
  return (
    <span style={{
      display:'inline-block', width:size, height:size,
      border:'2px solid rgba(255,255,255,0.3)', borderTopColor:'white',
      borderRadius:'50%', animation:'spin 0.7s linear infinite',
    }} />
  )
}

// Inject keyframes once
if (typeof document !== 'undefined' && !document.getElementById('pgxllm-spin')) {
  const s = document.createElement('style')
  s.id = 'pgxllm-spin'
  s.textContent = '@keyframes spin { to { transform: rotate(360deg); } }'
  document.head.appendChild(s)
}
