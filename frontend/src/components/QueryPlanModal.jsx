import { useMemo, useRef, useState } from 'react'
import { toPng } from 'html-to-image'
import {
  ReactFlow, Background, Controls, MiniMap,
  useNodesState, useEdgesState,
  Handle, Position, MarkerType,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'

// ── Layout constants ──────────────────────────────────────────
const NODE_W = 290
const NODE_H = 130
const H_GAP  = 60
const V_GAP  = 90

// ── Per-node-type color palette ───────────────────────────────
const TYPE_PALETTE = {
  'Seq Scan':          { bg:'#fee2e2', border:'#ef4444', text:'#991b1b' },
  'Index Scan':        { bg:'#dcfce7', border:'#22c55e', text:'#166534' },
  'Index Only Scan':   { bg:'#dcfce7', border:'#22c55e', text:'#166534' },
  'Bitmap Index Scan': { bg:'#fef9c3', border:'#eab308', text:'#854d0e' },
  'Bitmap Heap Scan':  { bg:'#fef9c3', border:'#eab308', text:'#854d0e' },
  'Hash Join':         { bg:'#dbeafe', border:'#3b82f6', text:'#1e40af' },
  'Merge Join':        { bg:'#dbeafe', border:'#3b82f6', text:'#1e40af' },
  'Nested Loop':       { bg:'#ede9fe', border:'#8b5cf6', text:'#5b21b6' },
  'Sort':              { bg:'#ffedd5', border:'#f97316', text:'#9a3412' },
  'Hash':              { bg:'#ccfbf1', border:'#14b8a6', text:'#0f766e' },
  'Aggregate':         { bg:'#e0e7ff', border:'#6366f1', text:'#3730a3' },
  'Limit':             { bg:'#fce7f3', border:'#ec4899', text:'#9d174d' },
  'CTE Scan':          { bg:'#f5f3ff', border:'#a855f7', text:'#6b21a8' },
  'Subquery Scan':     { bg:'#f5f3ff', border:'#a855f7', text:'#6b21a8' },
  'Result':            { bg:'#f0fdf4', border:'#86efac', text:'#166534' },
  'Materialize':       { bg:'#fffbeb', border:'#fbbf24', text:'#92400e' },
}

function palette(nodeType) {
  return TYPE_PALETTE[nodeType] || { bg:'#f3f4f6', border:'#9ca3af', text:'#374151' }
}

// ── Heat color for exclusive % bar (green → orange → red) ────
function heatColor(ratio) {
  if (ratio > 0.5)  return '#ef4444'
  if (ratio > 0.25) return '#f97316'
  if (ratio > 0.1)  return '#eab308'
  return '#22c55e'
}

// ── Format milliseconds: µs if < 1ms, ms otherwise ───────────
function fmtMs(ms) {
  if (ms == null) return '—'
  if (ms < 0.1)   return `${(ms * 1000).toFixed(0)} µs`
  if (ms < 1)     return `${(ms * 1000).toFixed(1)} µs`
  return `${ms.toFixed(2)} ms`
}

// ── Conditions to show inside each node ───────────────────────
const COND_DEFS = [
  { key:'Index Cond',   icon:'🔑', label:'idx cond'   },
  { key:'Recheck Cond', icon:'↺',  label:'recheck'    },
  { key:'Hash Cond',    icon:'⊕',  label:'hash cond'  },
  { key:'Merge Cond',   icon:'⊕',  label:'merge cond' },
  { key:'Join Filter',  icon:'⚡', label:'join filter' },
  { key:'Filter',       icon:'▽',  label:'filter'     },
]
const ARRAY_DEFS = [
  { key:'Sort Key',  icon:'↕', label:'sort'     },
  { key:'Group Key', icon:'⊞', label:'group by' },
]

// ── Subtree width for layout ───────────────────────────────────
function subtreeWidth(plan) {
  const children = plan['Plans'] || []
  if (!children.length) return NODE_W
  const sum = children.reduce((s, c) => s + subtreeWidth(c), 0)
  return Math.max(NODE_W, sum + (children.length - 1) * H_GAP)
}

// ── Exclusive time: 자신만의 처리 시간 = 자기 total - 직접 자식들 total 합 ──
function calcExclusiveTime(p) {
  const loops    = p['Actual Loops'] ?? 1
  const myTotal  = (p['Actual Total Time'] ?? 0) * loops
  const childSum = (p['Plans'] || []).reduce((s, c) =>
    s + (c['Actual Total Time'] ?? 0) * (c['Actual Loops'] ?? 1), 0)
  return Math.max(0, myTotal - childSum)
}

// ── Build react-flow graph ─────────────────────────────────────
// Data flows upward: leaf (scan) → root (output)
// Layout: root at top (depth=0, y=0), leaves at bottom (larger y)
// Edge: source=child, target=parent → arrow points upward to parent
function buildGraph(plan, totalTime) {
  const nodes = [], edges = []
  let id = 0

  function traverse(p, depth, centerX, parentId) {
    const myId         = `n${id++}`
    const exclusiveTime = totalTime != null ? calcExclusiveTime(p) : null
    nodes.push({
      id:       myId,
      type:     'planNode',
      position: { x: centerX - NODE_W / 2, y: depth * (NODE_H + V_GAP) },
      data:     { plan: p, totalTime, exclusiveTime },
      style:    { width: NODE_W },
    })
    if (parentId) {
      const rel = p['Parent Relationship']
      edges.push({
        id:           `e-${myId}-${parentId}`,
        source:       myId,
        target:       parentId,
        sourceHandle: 'src',
        targetHandle: 'tgt',
        type:         'smoothstep',
        label:        rel && rel !== 'Outer' ? rel : undefined,
        labelStyle:      { fontSize:9, fill:'#64748b', fontWeight:600 },
        labelBgStyle:    { fill:'#f1f5f9', fillOpacity:0.9 },
        labelBgPadding:  [2, 4],
        style:        { stroke:'#64748b', strokeWidth:2 },
        markerEnd:    { type: MarkerType.ArrowClosed, color:'#64748b', width:14, height:14 },
      })
    }
    const children = p['Plans'] || []
    if (children.length) {
      const totalW = children.reduce((s, c) => s + subtreeWidth(c), 0)
                   + (children.length - 1) * H_GAP
      let curX = centerX - totalW / 2
      for (const child of children) {
        const cw = subtreeWidth(child)
        traverse(child, depth + 1, curX + cw / 2, myId)
        curX += cw + H_GAP
      }
    }
  }

  traverse(plan, 0, 0, null)
  return { nodes, edges }
}

// ── Condition row ─────────────────────────────────────────────
function CondRow({ icon, label, value }) {
  const str = Array.isArray(value) ? value.join(', ') : String(value)
  return (
    <div style={{ display:'flex', gap:4, alignItems:'flex-start', marginTop:3 }}>
      <span style={{ flexShrink:0, fontSize:9, lineHeight:'14px', color:'#6b7280' }}>{icon}</span>
      <span style={{ fontSize:9, color:'#9ca3af', minWidth:50, flexShrink:0 }}>{label}:</span>
      <span title={str} style={{
        fontSize:9, color:'#374151', fontFamily:'Consolas,monospace',
        overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', flex:1,
      }}>{str}</span>
    </div>
  )
}

// ── Custom plan node ───────────────────────────────────────────
function PlanNode({ data }) {
  const p             = data.plan
  const totalTime     = data.totalTime     // 루트 Actual Total Time × loops (전체 기준)
  const exclusiveTime = data.exclusiveTime // 이 노드만의 순수 처리 시간

  const type       = p['Node Type'] || '?'
  const { bg, border, text } = palette(type)

  const relation  = p['Relation Name'] || p['CTE Name'] || p['Subplan Name'] || p['Alias'] || ''
  const schema    = p['Schema'] || ''
  const startCost = p['Startup Cost']?.toFixed(2)
  const cost      = p['Total Cost']?.toFixed(2)
  const planRows  = p['Plan Rows']
  const actualRows   = p['Actual Rows']
  const removedRows  = p['Rows Removed by Filter']

  // ANALYZE timing
  const loops           = p['Actual Loops'] ?? 1
  const inclusiveTime   = p['Actual Total Time'] != null         // 자신 + 하위 포함 누적
                          ? p['Actual Total Time'] * loops : null
  const startupTime     = p['Actual Startup Time']               // 첫 행까지의 시간(per loop)

  // exclusive % = 이 노드 자신의 순수 처리 시간 / 전체 실행 시간
  // 모든 노드의 exclusive % 합 ≈ 100%
  const exclusiveRatio  = (exclusiveTime != null && totalTime > 0)
                          ? exclusiveTime / totalTime : null

  const hasConds  = COND_DEFS.some(d => p[d.key]) || ARRAY_DEFS.some(d => p[d.key])
  const handleSt  = { background:'#64748b', border:'none', width:8, height:8 }

  return (
    <>
      <Handle id="src" type="source" position={Position.Top}    style={handleSt} />
      <Handle id="tgt" type="target" position={Position.Bottom} style={handleSt} />

      <div style={{
        background: bg, border:`2px solid ${border}`, borderRadius:8,
        padding:'8px 10px', fontSize:11, width:NODE_W, boxSizing:'border-box',
        boxShadow:'0 2px 6px rgba(0,0,0,0.10)',
      }}>
        {/* Node type + relation */}
        <div style={{ fontWeight:700, color:text, fontSize:12, marginBottom:4,
          whiteSpace:'nowrap', overflow:'hidden', textOverflow:'ellipsis' }}>
          {type}
          {relation && (
            <span style={{ fontWeight:500, color:'#374151', marginLeft:6, fontSize:11 }}>
              {schema ? `${schema}.` : ''}{relation}
            </span>
          )}
          {p['Index Name'] && (
            <span style={{ fontWeight:400, color:text, marginLeft:5, fontSize:10, fontStyle:'italic' }}>
              [{p['Index Name']}]
            </span>
          )}
        </div>

        {/* Cost + row estimates */}
        <div style={{ display:'flex', gap:8, color:'#6b7280', fontSize:10, flexWrap:'wrap', rowGap:2 }}>
          {startCost != null && cost != null && (
            <span title="Startup..Total Cost">cost {startCost}..{cost}</span>
          )}
          {planRows != null && (
            <span title="Estimated rows">≈{planRows.toLocaleString()} rows</span>
          )}
          {actualRows != null && (
            <span title="Actual rows" style={{ color:text, fontWeight:700 }}>
              ✓{actualRows.toLocaleString()}
            </span>
          )}
          {removedRows != null && (
            <span title="Rows removed by filter" style={{ color:'#dc2626' }}>
              −{removedRows.toLocaleString()} filtered
            </span>
          )}
        </div>

        {/* ── Timing section (ANALYZE only) ── */}
        {inclusiveTime != null && (
          <div style={{ marginTop:5, padding:'5px 7px', background:'rgba(0,0,0,0.04)',
            borderRadius:5, border:`1px solid ${border}` }}>

            {/* 첫 줄: exclusive time + % */}
            <div style={{ display:'flex', alignItems:'center', gap:6, marginBottom:3 }}>
              <span style={{ fontSize:10, fontWeight:700,
                color: heatColor(exclusiveRatio ?? 0), flexShrink:0 }}>
                ⏱ {fmtMs(exclusiveTime)}
              </span>
              <span style={{ fontSize:9, color:'#9ca3af', flex:1 }}>
                (exclusive)
              </span>
              {exclusiveRatio != null && (
                <span style={{ fontSize:11, fontWeight:800,
                  color: heatColor(exclusiveRatio) }}>
                  {(exclusiveRatio * 100).toFixed(1)}%
                </span>
              )}
            </div>

            {/* Heat bar: exclusive % 기준 */}
            {exclusiveRatio != null && (
              <div style={{ height:4, background:'#e5e7eb', borderRadius:2, marginBottom:4 }}>
                <div style={{
                  height:'100%',
                  width:`${Math.min(100, exclusiveRatio * 100)}%`,
                  background: heatColor(exclusiveRatio),
                  borderRadius:2,
                }} />
              </div>
            )}

            {/* 둘째 줄: inclusive total + startup + loops */}
            <div style={{ display:'flex', gap:8, fontSize:9, color:'#9ca3af', flexWrap:'wrap' }}>
              <span title="자신 + 하위 노드 포함 누적 시간">
                total {fmtMs(inclusiveTime)}
              </span>
              {startupTime != null && (
                <span title="첫 행 반환까지의 시간 (per loop)">
                  startup {fmtMs(startupTime)}
                </span>
              )}
              {loops > 1 && (
                <span title="이 노드가 실행된 횟수">× {loops} loops</span>
              )}
            </div>
          </div>
        )}

        {/* Conditions divider */}
        {hasConds && (
          <div style={{ borderTop:`1px solid ${border}`, margin:'5px 0 2px', opacity:0.35 }} />
        )}

        {COND_DEFS.map(d => p[d.key]
          ? <CondRow key={d.key} icon={d.icon} label={d.label} value={p[d.key]} />
          : null
        )}
        {ARRAY_DEFS.map(d => p[d.key]
          ? <CondRow key={d.key} icon={d.icon} label={d.label} value={p[d.key]} />
          : null
        )}
      </div>
    </>
  )
}

const NODE_TYPES = { planNode: PlanNode }

// ── Legend ────────────────────────────────────────────────────
const LEGEND_ITEMS = [
  ['Seq Scan',    '#ef4444'],
  ['Index Scan',  '#22c55e'],
  ['Bitmap Scan', '#eab308'],
  ['Join',        '#3b82f6'],
  ['Nested Loop', '#8b5cf6'],
  ['Sort/Agg',    '#f97316'],
  ['Limit',       '#ec4899'],
  ['Other',       '#9ca3af'],
]

function Legend({ analyzed }) {
  return (
    <div style={{ display:'flex', gap:10, flexWrap:'wrap', padding:'7px 16px',
      borderTop:'1px solid var(--gray2)', fontSize:10, color:'var(--gray)',
      alignItems:'center', background:'#f8fafc' }}>
      <span style={{ fontWeight:600, color:'var(--dark)' }}>노드 유형:</span>
      {LEGEND_ITEMS.map(([label, color]) => (
        <span key={label} style={{ display:'flex', alignItems:'center', gap:3 }}>
          <span style={{ width:9, height:9, borderRadius:2, background:color,
            display:'inline-block', flexShrink:0 }} />
          {label}
        </span>
      ))}
      {analyzed && (
        <span style={{ display:'flex', alignItems:'center', gap:6, marginLeft:8,
          padding:'2px 8px', background:'#fef9c3', borderRadius:8,
          border:'1px solid #eab308', color:'#854d0e', fontWeight:600 }}>
          ⏱ % = 노드 자체 처리 시간 / 전체 (exclusive) — 합계 ≈ 100%&nbsp;
          <span style={{ color:'#22c55e' }}>■</span>&lt;10%&nbsp;
          <span style={{ color:'#eab308' }}>■</span>&lt;25%&nbsp;
          <span style={{ color:'#f97316' }}>■</span>&lt;50%&nbsp;
          <span style={{ color:'#ef4444' }}>■</span>≥50%
        </span>
      )}
      <span style={{ marginLeft:'auto', color:'#64748b' }}>
        ▲ 화살표 = 데이터 흐름 방향 (스캔 → 조인 → 결과)
      </span>
    </div>
  )
}

const FMT_OPTIONS = [
  { value: 'json', label: 'JSON',  ext: '.json', accent: '#0d9488' },
  { value: 'txt',  label: 'TEXT',  ext: '.txt',  accent: '#15803d' },
  { value: 'png',  label: 'PNG',   ext: '.png',  accent: '#1d4ed8' },
  { value: 'ppt',  label: 'PPT',   ext: '.pptx', accent: '#c2410c' },
]

// ── Modal ─────────────────────────────────────────────────────
export default function QueryPlanModal({ plan, planningTime, executionTime, analyzed, onClose, sql }) {
  const flowRef       = useRef(null)
  const [fmt, setFmt] = useState('json')
  const ts0           = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19)
  const [filename, setFilename] = useState(`query-plan-${ts0}`)
  const [busy,  setBusy]  = useState(false)
  const [sqlOpen, setSqlOpen] = useState(true)

  // ── content builders ────────────────────────────────────────
  function buildJson() {
    return JSON.stringify({ query: sql, exportedAt: new Date().toISOString(),
      planningTime, executionTime, analyzed, plan }, null, 2)
  }

  function buildText() {
    const sep = '─'.repeat(60)
    return [
      '-- Query Plan Export',
      `-- Exported   : ${new Date().toLocaleString('ko-KR')}`,
      planningTime  != null ? `-- Planning   : ${planningTime.toFixed(2)} ms`  : null,
      executionTime != null ? `-- Execution  : ${executionTime.toFixed(2)} ms` : null,
      analyzed ? '-- ANALYZE    : Yes (실제 실행, 롤백 완료)' : null,
      '', sep, '-- SQL Query', sep,
      sql || '(없음)',
      '', sep, '-- Execution Plan (JSON)', sep,
      JSON.stringify(plan, null, 2),
    ].filter(l => l !== null).join('\n')
  }

  async function capturePng() {
    if (!flowRef.current) throw new Error('Flow canvas not ready')
    return toPng(flowRef.current, { backgroundColor: '#f8fafc', pixelRatio: 2 })
  }

  async function buildPpt(imgDataUrl) {
    const PptxGenJS = (await import('pptxgenjs')).default
    const prs = new PptxGenJS()
    prs.layout = 'LAYOUT_WIDE'

    const exportedAt = new Date().toLocaleString('ko-KR')
    const footer = (slide) =>
      slide.addText(`Exported: ${exportedAt}${analyzed ? '  |  ANALYZE: 실제 실행됨' : ''}`, {
        x: 0.2, y: 7.4, w: 12.8, h: 0.25,
        fontSize: 7, color: '94A3B8', align: 'right',
      })

    const titleBar = (slide, label) => {
      slide.background = { color: 'F8FAFC' }
      slide.addShape(prs.ShapeType.rect, { x: 0, y: 0, w: '100%', h: 0.6,
        fill: { color: '0F172A' } })
      slide.addText(label, {
        x: 0.2, y: 0.05, w: 10, h: 0.5,
        fontSize: 20, bold: true, color: 'FFFFFF', fontFace: 'Arial',
      })
      // Timing badges
      let bx = 10.4
      if (planningTime != null) {
        slide.addText(`Planning: ${planningTime.toFixed(2)} ms`, {
          x: bx, y: 0.12, w: 1.9, h: 0.35,
          fontSize: 9, color: '475569', align: 'center',
          fill: { color: 'E2E8F0' }, shape: prs.ShapeType.roundRect,
        }); bx += 2.0
      }
      if (executionTime != null) {
        slide.addText(`⏱ ${executionTime.toFixed(2)} ms`, {
          x: bx, y: 0.12, w: 1.9, h: 0.35,
          fontSize: 9, bold: true, color: 'FFFFFF', align: 'center',
          fill: { color: '0369A1' }, shape: prs.ShapeType.roundRect,
        })
      }
    }

    // ── Slide 1: SQL Query ──────────────────────────────────
    const s1 = prs.addSlide()
    titleBar(s1, '📝 SQL Query  (1/2)')

    s1.addShape(prs.ShapeType.rect, { x: 0.2, y: 0.75, w: 12.8, h: 0.28,
      fill: { color: 'F1F5F9' }, line: { color: 'CBD5E1', width: 1 } })
    s1.addText('SQL', { x: 0.3, y: 0.77, w: 1, h: 0.22,
      fontSize: 9, bold: true, color: '0F172A' })

    const sqlText = sql || '(없음)'
    s1.addShape(prs.ShapeType.rect, { x: 0.2, y: 1.03, w: 12.8, h: 6.2,
      fill: { color: 'FFFFFF' }, line: { color: 'CBD5E1', width: 1 } })
    s1.addText(sqlText, { x: 0.35, y: 1.08, w: 12.5, h: 6.1,
      fontSize: 11, fontFace: 'Courier New', color: '1E293B',
      wrap: true, valign: 'top' })

    footer(s1)

    // ── Slide 2: Execution Plan ─────────────────────────────
    const s2 = prs.addSlide()
    titleBar(s2, '📊 Execution Plan  (2/2)')

    s2.addImage({ data: imgDataUrl, x: 0.2, y: 0.75, w: 12.8, h: 6.5 })

    footer(s2)

    return prs
  }

  // ── unified save ────────────────────────────────────────────
  async function handleSave() {
    if (busy) return
    setBusy(true)
    try {
      const ext  = FMT_OPTIONS.find(f => f.value === fmt)?.ext ?? '.json'
      const name = (filename.trim() || `query-plan-${ts0}`) + ext

      if (fmt === 'json' || fmt === 'txt') {
        const content  = fmt === 'json' ? buildJson() : buildText()
        const mimeType = fmt === 'json' ? 'application/json' : 'text/plain'
        await saveTextFile(name, content, mimeType)

      } else if (fmt === 'png') {
        const dataUrl = await capturePng()
        await saveBinaryFile(name, dataUrl, 'image/png')

      } else if (fmt === 'ppt') {
        const dataUrl = await capturePng()
        const prs     = await buildPpt(dataUrl)
        await prs.writeFile({ fileName: name })
      }
    } finally {
      setBusy(false)
    }
  }

  async function saveTextFile(name, content, mime) {
    if (window.showSaveFilePicker) {
      try {
        const handle = await window.showSaveFilePicker({ suggestedName: name,
          types: [{ description: mime, accept: { [mime]: [name.slice(name.lastIndexOf('.'))] } }] })
        const w = await handle.createWritable()
        await w.write(content); await w.close(); return
      } catch (e) { if (e.name === 'AbortError') return }
    }
    const a = document.createElement('a')
    a.href = URL.createObjectURL(new Blob([content], { type: mime }))
    a.download = name; a.click()
  }

  async function saveBinaryFile(name, dataUrl, mime) {
    if (window.showSaveFilePicker) {
      try {
        const handle = await window.showSaveFilePicker({ suggestedName: name,
          types: [{ description: mime, accept: { [mime]: ['.png'] } }] })
        const res = await fetch(dataUrl)
        const w = await handle.createWritable()
        await w.write(await res.blob()); await w.close(); return
      } catch (e) { if (e.name === 'AbortError') return }
    }
    const a = document.createElement('a')
    a.href = dataUrl; a.download = name; a.click()
  }

  // ── flow graph ──────────────────────────────────────────────
  const rootTotalTime = useMemo(() => {
    if (!analyzed) return null
    return plan['Actual Total Time'] ?? null
  }, [plan, analyzed])

  const { nodes: initNodes, edges: initEdges } = useMemo(
    () => buildGraph(plan, rootTotalTime),
    [plan, rootTotalTime],
  )
  const [nodes, , onNodesChange] = useNodesState(initNodes)
  const [edges, , onEdgesChange] = useEdgesState(initEdges)

  const fmtInfo = FMT_OPTIONS.find(f => f.value === fmt)

  return (
    <div
      onClick={onClose}
      style={{ position:'fixed', inset:0, background:'rgba(0,0,0,0.55)',
        zIndex:9999, display:'flex', alignItems:'center', justifyContent:'center' }}
    >
      <div
        onClick={e => e.stopPropagation()}
        style={{ background:'white', borderRadius:12,
          boxShadow:'0 8px 40px rgba(0,0,0,0.25)',
          width:'93vw', height:'90vh',
          display:'flex', flexDirection:'column', overflow:'hidden' }}
      >
        {/* ── Header ── */}
        <div style={{ padding:'10px 16px', borderBottom:'1px solid var(--gray2)',
          display:'flex', alignItems:'center', gap:10, background:'#f8fafc', flexShrink:0 }}>
          <span style={{ fontWeight:700, fontSize:14 }}>📊 Query Execution Plan</span>
          {planningTime != null && (
            <span style={{ fontSize:11, color:'var(--gray)', background:'var(--gray3)',
              padding:'2px 8px', borderRadius:10, border:'1px solid var(--gray2)' }}>
              Planning {planningTime.toFixed(2)} ms
            </span>
          )}
          {executionTime != null && (
            <span style={{ fontSize:11, fontWeight:700, color:'white',
              background:'#0369a1', padding:'2px 8px', borderRadius:10 }}>
              ⏱ 총 실행 {executionTime.toFixed(2)} ms
            </span>
          )}
          {analyzed && (
            <span style={{ fontSize:10, color:'#854d0e', background:'#fef9c3',
              padding:'2px 6px', borderRadius:8, border:'1px solid #eab308' }}>
              ANALYZE
            </span>
          )}
          <span style={{ fontSize:10, color:'#94a3b8', marginLeft:'auto' }}>
            드래그·스크롤로 탐색
          </span>
          <button onClick={onClose}
            style={{ background:'none', border:'none',
              cursor:'pointer', fontSize:20, color:'var(--gray)', lineHeight:1 }}>✕</button>
        </div>

        {/* ── SQL strip ── */}
        <div style={{ borderBottom:'1px solid var(--gray2)', background:'#fafafa', flexShrink:0 }}>
          <div
            onClick={() => setSqlOpen(v => !v)}
            style={{ padding:'5px 16px', display:'flex', alignItems:'center', gap:8,
              cursor:'pointer', userSelect:'none' }}
          >
            <span style={{ fontSize:10, color:'var(--gray)' }}>{sqlOpen ? '▼' : '▶'}</span>
            <span style={{ fontSize:11, fontWeight:700, color:'#334155' }}>SQL Query</span>
            {!sqlOpen && sql && (
              <span style={{ fontSize:11, fontFamily:'monospace', color:'#64748b',
                overflow:'hidden', textOverflow:'ellipsis', whiteSpace:'nowrap', flex:1 }}>
                {sql.replace(/\s+/g, ' ').substring(0, 120)}
              </span>
            )}
          </div>
          {sqlOpen && (
            <pre style={{ margin:0, padding:'0 16px 10px', fontSize:11,
              fontFamily:'monospace', color:'#1e293b', whiteSpace:'pre-wrap',
              wordBreak:'break-all', maxHeight:100, overflowY:'auto' }}>
              {sql || '(없음)'}
            </pre>
          )}
        </div>

        {/* ── Flow canvas ── */}
        <div ref={flowRef} style={{ flex:1, minHeight:0 }}>
          <ReactFlow
            nodes={nodes} edges={edges}
            onNodesChange={onNodesChange} onEdgesChange={onEdgesChange}
            nodeTypes={NODE_TYPES}
            fitView fitViewOptions={{ padding:0.25 }}
            proOptions={{ hideAttribution:true }}
            minZoom={0.15}
          >
            <Background gap={18} color="#e2e8f0" />
            <Controls />
            <MiniMap
              nodeColor={n => palette(n.data?.plan?.['Node Type'] || '').border}
              maskColor="rgba(241,245,249,0.7)"
            />
          </ReactFlow>
        </div>

        <Legend analyzed={analyzed} />

        {/* ── Save form ── */}
        <div style={{ padding:'10px 16px', borderTop:'1px solid var(--gray2)',
          background:'#f8fafc', display:'flex', alignItems:'center', gap:12, flexShrink:0 }}>
          {/* Format selector */}
          <div style={{ display:'flex', gap:4 }}>
            {FMT_OPTIONS.map(f => (
              <button key={f.value} onClick={() => setFmt(f.value)}
                style={{
                  padding:'4px 12px', borderRadius:5, cursor:'pointer',
                  fontSize:11, fontWeight:700, border:'1.5px solid',
                  borderColor: fmt === f.value ? f.accent : '#cbd5e1',
                  background:  fmt === f.value ? f.accent : 'white',
                  color:       fmt === f.value ? 'white'  : '#64748b',
                  transition:'all .15s',
                }}>
                {f.label}
              </button>
            ))}
          </div>

          {/* Filename input */}
          <div style={{ display:'flex', alignItems:'center', gap:4, flex:1 }}>
            <input
              value={filename}
              onChange={e => setFilename(e.target.value)}
              onKeyDown={e => e.key === 'Enter' && handleSave()}
              style={{ flex:1, padding:'4px 8px', border:'1px solid #cbd5e1', borderRadius:5,
                fontSize:11, fontFamily:'monospace', outline:'none', minWidth:0 }}
            />
            <span style={{ fontSize:11, color:'#94a3b8', whiteSpace:'nowrap' }}>
              {fmtInfo?.ext}
            </span>
          </div>

          {/* Save button */}
          <button onClick={handleSave} disabled={busy}
            style={{ padding:'5px 18px', borderRadius:6, cursor: busy ? 'wait' : 'pointer',
              fontSize:12, fontWeight:700, border:'none',
              background: busy ? '#94a3b8' : fmtInfo?.accent ?? '#0d9488',
              color:'white', whiteSpace:'nowrap', opacity: busy ? 0.7 : 1,
              transition:'background .15s' }}>
            {busy ? '저장 중...' : '💾 저장하기'}
          </button>
        </div>
      </div>
    </div>
  )
}
