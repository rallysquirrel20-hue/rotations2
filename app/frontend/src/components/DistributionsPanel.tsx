import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react'
import axios from 'axios'

interface DistStats {
  n: number
  returns: number[]
  mean: number | null
  median: number | null
  stddev: number | null
  win_rate: number | null
  p5: number | null; p25: number | null; p75: number | null; p95: number | null
}

interface DistResponse {
  ticker: string
  lookback: number
  filtered: DistStats
  baseline: DistStats
}

interface BasketsData { Themes: string[]; Sectors: string[]; Industries: string[] }

interface DistributionsPanelProps {
  apiBase: string
  activeTicker?: string | null
  allTickers?: string[]
  activeBasket?: string | null
  allBaskets?: BasketsData
}

type TriState = 'any' | 'a' | 'b'
type Op = '>' | '<' | '>=' | '<='
const OPS: Op[] = ['>', '<', '>=', '<=']

interface ThresholdState { active: boolean; op: Op; val: number }

const BLUE = 'rgb(50, 50, 255)'
const BLUE_FILL = 'rgba(50, 50, 255, 0.35)'
const PINK = 'rgb(255, 50, 150)'
const PINK_FILL = 'rgba(255, 50, 150, 0.25)'
const SOLAR_BG = '#fdf6e3'
const SOLAR_BORDER = '#eee8d5'
const SOLAR_TEXT = '#586e75'
const SOLAR_MUTED = '#93a1a1'

// --- Condition definitions ---
interface TriDef  { kind: 'tri';  key: string; label: string; a: { value: string; label: string }; b: { value: string; label: string }; basketOnly?: boolean }
interface ThDef   { kind: 'th';   key: string; label: string; opParam: string; valParam: string; defaultVal: number; min: number; max: number; step: number; basketOnly?: boolean }
type CondDef = TriDef | ThDef

const COND_DEFS: CondDef[] = [
  { kind: 'tri', key: 'breakout_state', label: 'LT Regime',      a: { value: 'breakout', label: 'Breakout' }, b: { value: 'breakdown', label: 'Breakdown' } },
  { kind: 'tri', key: 'rotation_state', label: 'Rotation',        a: { value: 'up', label: 'Up' },            b: { value: 'down', label: 'Down' } },
  { kind: 'th',  key: 'h',    label: 'H React',       opParam: 'h_op',       valParam: 'h_val',       defaultVal: 0, min: -1, max: 1, step: 0.05 },
  { kind: 'tri', key: 'h_trend',       label: 'H React trend',   a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' } },
  { kind: 'th',  key: 'p',    label: 'Price Chg',     opParam: 'p_op',       valParam: 'p_val',       defaultVal: 0, min: -1, max: 1, step: 0.05 },
  { kind: 'tri', key: 'p_trend',       label: 'Price Chg trend', a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' } },
  { kind: 'th',  key: 'l',    label: 'L React',       opParam: 'l_op',       valParam: 'l_val',       defaultVal: 0, min: -1, max: 1, step: 0.05 },
  { kind: 'tri', key: 'l_trend',       label: 'L React trend',   a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' } },
  { kind: 'tri', key: 'rv_trend',      label: 'RV trend',        a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' } },
  { kind: 'th',  key: 'breadth',  label: 'Breadth %',       opParam: 'breadth_op',  valParam: 'breadth_val',  defaultVal: 50, min: 0, max: 100, step: 5, basketOnly: true },
  { kind: 'tri', key: 'breadth_trend', label: 'Breadth trend',   a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' }, basketOnly: true },
  { kind: 'th',  key: 'breakout', label: 'Breakout %',      opParam: 'breakout_op', valParam: 'breakout_val', defaultVal: 50, min: 0, max: 100, step: 5, basketOnly: true },
  { kind: 'tri', key: 'breakout_trend', label: 'Breakout trend', a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' }, basketOnly: true },
  { kind: 'th',  key: 'corr',     label: 'Correlation %',   opParam: 'corr_op',     valParam: 'corr_val',     defaultVal: 50, min: 0, max: 100, step: 5, basketOnly: true },
  { kind: 'tri', key: 'corr_trend',    label: 'Correlation trend', a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' }, basketOnly: true },
]

const TRI_KEYS = COND_DEFS.filter(d => d.kind === 'tri').map(d => d.key)
const TH_DEFS = COND_DEFS.filter((d): d is ThDef => d.kind === 'th')

const pct = (v: number | null | undefined, digits = 2) =>
  v == null ? '--' : (v * 100).toFixed(digits) + '%'

const initTriConds = (): Record<string, TriState> =>
  Object.fromEntries(TRI_KEYS.map(k => [k, 'any' as TriState]))

const initThresholds = (): Record<string, ThresholdState> =>
  Object.fromEntries(TH_DEFS.map(d => [d.key, { active: false, op: '>' as Op, val: d.defaultVal }]))

export const DistributionsPanel: React.FC<DistributionsPanelProps> = ({
  apiBase, activeTicker, allTickers = [], activeBasket, allBaskets,
}) => {
  const [mode, setMode] = useState<'ticker' | 'basket'>(activeBasket ? 'basket' : 'ticker')
  const [ticker, setTicker] = useState<string>(activeTicker || '')
  const [tickerQuery, setTickerQuery] = useState<string>(activeTicker || '')
  const [showTickerDropdown, setShowTickerDropdown] = useState(false)
  const [basket, setBasket] = useState<string>(activeBasket || '')
  const [basketQuery, setBasketQuery] = useState<string>(activeBasket || '')
  const [showBasketDropdown, setShowBasketDropdown] = useState(false)
  const [lookback, setLookback] = useState(21)
  const [triConds, setTriConds] = useState<Record<string, TriState>>(initTriConds)
  const [thresholds, setThresholds] = useState<Record<string, ThresholdState>>(initThresholds)
  const [data, setData] = useState<DistResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (activeBasket) {
      setMode('basket'); setBasket(activeBasket); setBasketQuery(activeBasket)
    } else if (activeTicker && activeTicker !== ticker) {
      setMode('ticker'); setTicker(activeTicker); setTickerQuery(activeTicker)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTicker, activeBasket])

  const allBasketNames = useMemo(() => {
    if (!allBaskets) return []
    return [...allBaskets.Themes, ...allBaskets.Sectors, ...allBaskets.Industries]
  }, [allBaskets])

  const filteredTickerOptions = useMemo(() => {
    const q = tickerQuery.trim().toUpperCase()
    if (!q) return allTickers.slice(0, 30)
    return allTickers.filter(t => t.toUpperCase().startsWith(q)).slice(0, 30)
  }, [tickerQuery, allTickers])

  const filteredBasketOptions = useMemo(() => {
    const q = basketQuery.trim().toLowerCase()
    if (!q) return allBasketNames.slice(0, 30)
    return allBasketNames.filter(b => b.toLowerCase().includes(q)).slice(0, 30)
  }, [basketQuery, allBasketNames])

  const isBasket = mode === 'basket'
  const target = isBasket ? basket : ticker
  const visibleDefs = COND_DEFS.filter(d => !d.basketOnly || isBasket)

  // Fetch
  useEffect(() => {
    if (!target) { setData(null); return }
    const params: Record<string, string | number> = { lookback }
    if (isBasket) params.basket = target
    else params.ticker = target
    for (const def of COND_DEFS) {
      if (def.basketOnly && !isBasket) continue
      if (def.kind === 'tri') {
        const state = triConds[def.key]
        if (state === 'a') params[def.key] = def.a.value
        else if (state === 'b') params[def.key] = def.b.value
      } else {
        const th = thresholds[def.key]
        if (th?.active) {
          params[def.opParam] = th.op
          params[def.valParam] = th.val
        }
      }
    }
    setLoading(true); setError(null)
    axios.get(`${apiBase}/distribution/next-bar`, { params })
      .then(res => { setData(res.data); setLoading(false) })
      .catch(err => {
        setData(null)
        setError(err?.response?.data?.detail || err.message || 'Request failed')
        setLoading(false)
      })
  }, [target, triConds, thresholds, lookback, apiBase, isBasket])

  const setTri = useCallback((key: string, next: TriState) =>
    setTriConds(prev => ({ ...prev, [key]: next })), [])

  const setTh = useCallback((key: string, patch: Partial<ThresholdState>) =>
    setThresholds(prev => ({ ...prev, [key]: { ...prev[key], ...patch } })), [])

  const cycleOp = useCallback((key: string) =>
    setThresholds(prev => {
      const cur = prev[key]
      const idx = OPS.indexOf(cur.op)
      return { ...prev, [key]: { ...cur, active: true, op: OPS[(idx + 1) % OPS.length] } }
    }), [])

  const resetAll = () => { setTriConds(initTriConds()); setThresholds(initThresholds()) }

  // ── Canvas ──
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const [dims, setDims] = useState({ w: 800, h: 360 })
  const wrapRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const onResize = () => { if (wrapRef.current) setDims({ w: wrapRef.current.clientWidth, h: 360 }) }
    onResize(); window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [])

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return
    const dpr = window.devicePixelRatio || 1
    canvas.width = dims.w * dpr; canvas.height = dims.h * dpr; ctx.scale(dpr, dpr)
    ctx.clearRect(0, 0, dims.w, dims.h); ctx.fillStyle = SOLAR_BG; ctx.fillRect(0, 0, dims.w, dims.h)
    const pad = { top: 30, right: 30, bottom: 50, left: 20 }
    const plotW = dims.w - pad.left - pad.right, plotH = dims.h - pad.top - pad.bottom

    if (!data) {
      ctx.fillStyle = SOLAR_MUTED; ctx.font = '14px monospace'; ctx.textAlign = 'center'
      ctx.fillText(target ? (loading ? 'Loading\u2026' : (error || 'No data')) : 'Select a ticker or basket', dims.w / 2, dims.h / 2)
      return
    }
    const base = data.baseline.returns, filt = data.filtered.returns
    if (base.length === 0) {
      ctx.fillStyle = SOLAR_MUTED; ctx.font = '14px monospace'; ctx.textAlign = 'center'
      ctx.fillText('No baseline returns available', dims.w / 2, dims.h / 2); return
    }
    const sortedBase = [...base].sort((a, b) => a - b)
    const q = (p: number) => sortedBase[Math.max(0, Math.min(sortedBase.length - 1, Math.floor(p * (sortedBase.length - 1))))]
    const p1 = q(0.01), p99 = q(0.99), pad_x = (p99 - p1) * 0.1 || 0.01
    const xMin = p1 - pad_x, xMax = p99 + pad_x
    const nPoints = Math.min(200, plotW)
    const gaussianKDE = (vals: number[]) => {
      if (vals.length < 2) return { xs: [] as number[], ys: [] as number[] }
      const n = vals.length, mean = vals.reduce((a, b) => a + b, 0) / n
      const std = Math.sqrt(vals.reduce((s, v) => s + (v - mean) ** 2, 0) / n) || (xMax - xMin) * 0.02
      const h = 1.06 * std * Math.pow(n, -0.2), invH = 1 / h, coeff = 1 / (n * h * Math.sqrt(2 * Math.PI))
      const xs: number[] = [], ys: number[] = []
      for (let i = 0; i < nPoints; i++) {
        const x = xMin + (i / (nPoints - 1)) * (xMax - xMin); let d = 0
        for (const v of vals) { const z = (x - v) * invH; d += Math.exp(-0.5 * z * z) }
        xs.push(x); ys.push(d * coeff)
      }
      return { xs, ys }
    }
    const baseKDE = gaussianKDE(base), filtKDE = filt.length >= 2 ? gaussianKDE(filt) : { xs: [], ys: [] }
    let maxD = 0
    for (const y of baseKDE.ys) maxD = Math.max(maxD, y)
    for (const y of filtKDE.ys) maxD = Math.max(maxD, y)
    if (maxD === 0) return
    const xScale = (v: number) => pad.left + ((v - xMin) / (xMax - xMin)) * plotW
    const yScale = (d: number) => pad.top + plotH - (d / maxD) * plotH
    ctx.strokeStyle = SOLAR_BORDER; ctx.lineWidth = 1
    for (let i = 0; i <= 4; i++) { const y = pad.top + (plotH * i) / 4; ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + plotW, y); ctx.stroke() }
    if (xMin < 0 && xMax > 0) { ctx.strokeStyle = SOLAR_MUTED; ctx.setLineDash([3, 3]); ctx.beginPath(); ctx.moveTo(xScale(0), pad.top); ctx.lineTo(xScale(0), pad.top + plotH); ctx.stroke(); ctx.setLineDash([]) }
    const drawCurve = (xs: number[], ys: number[], fill: string, stroke: string) => {
      if (xs.length < 2) return; const bl = pad.top + plotH
      ctx.beginPath(); ctx.moveTo(xScale(xs[0]), bl); for (let i = 0; i < xs.length; i++) ctx.lineTo(xScale(xs[i]), yScale(ys[i])); ctx.lineTo(xScale(xs[xs.length - 1]), bl); ctx.closePath(); ctx.fillStyle = fill; ctx.fill()
      ctx.beginPath(); ctx.moveTo(xScale(xs[0]), yScale(ys[0])); for (let i = 1; i < xs.length; i++) ctx.lineTo(xScale(xs[i]), yScale(ys[i])); ctx.strokeStyle = stroke; ctx.lineWidth = 2; ctx.stroke()
    }
    drawCurve(baseKDE.xs, baseKDE.ys, PINK_FILL, PINK)
    if (filtKDE.xs.length > 0) drawCurve(filtKDE.xs, filtKDE.ys, BLUE_FILL, BLUE)
    const drawMedian = (v: number | null, color: string) => { if (v == null || v < xMin || v > xMax) return; const x = xScale(v); ctx.strokeStyle = color; ctx.lineWidth = 1; ctx.setLineDash([4, 3]); ctx.beginPath(); ctx.moveTo(x, pad.top); ctx.lineTo(x, pad.top + plotH); ctx.stroke(); ctx.setLineDash([]) }
    drawMedian(data.baseline.median, PINK); drawMedian(data.filtered.median, BLUE)
    ctx.fillStyle = SOLAR_TEXT; ctx.font = '10px monospace'; ctx.textAlign = 'center'
    for (let i = 0; i <= 8; i++) { const v = xMin + (i / 8) * (xMax - xMin); ctx.fillText((v * 100).toFixed(1) + '%', xScale(v), dims.h - pad.bottom + 15) }
    const legendW = 260, lx = dims.w - pad.right - legendW - 4, ly = pad.top + 4
    ctx.fillStyle = SOLAR_BG; ctx.fillRect(lx - 4, ly - 10, legendW, 44); ctx.strokeStyle = SOLAR_BORDER; ctx.lineWidth = 1; ctx.strokeRect(lx - 4, ly - 10, legendW, 44)
    const row = (i: number, sf: string, ss: string, label: string, n: number, median: number | null) => {
      const y = ly + i * 16; ctx.fillStyle = sf; ctx.fillRect(lx, y - 4, 14, 10); ctx.strokeStyle = ss; ctx.lineWidth = 1; ctx.strokeRect(lx, y - 4, 14, 10)
      ctx.fillStyle = SOLAR_TEXT; ctx.font = '10px monospace'; ctx.textAlign = 'left'; ctx.fillText(label, lx + 18, y + 4); ctx.fillText(`N=${n}`, lx + 100, y + 4); ctx.textAlign = 'right'; ctx.fillText(`med ${pct(median)}`, lx + legendW - 8, y + 4)
    }
    row(0, PINK_FILL, PINK, 'Baseline', data.baseline.n, data.baseline.median)
    row(1, BLUE_FILL, BLUE, 'Filtered', data.filtered.n, data.filtered.median)
  }, [data, dims, target, loading, error])

  const stats = data?.filtered, baseSt = data?.baseline

  return (
    <div className="distributions-panel" style={{ padding: 12, display: 'flex', flexDirection: 'column', gap: 12, height: '100%', overflow: 'auto' }}>

      {/* Controls row */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'flex-end' }}>
        <div>
          <div style={{ fontSize: 10, color: SOLAR_MUTED, marginBottom: 3 }}>MODE</div>
          <div style={{ display: 'flex', gap: 2 }}>
            <button className={`control-btn ${mode === 'ticker' ? 'primary' : ''}`} style={{ fontSize: 11, padding: '3px 10px' }} onClick={() => setMode('ticker')}>Ticker</button>
            <button className={`control-btn ${mode === 'basket' ? 'primary' : ''}`} style={{ fontSize: 11, padding: '3px 10px' }} onClick={() => setMode('basket')}>Basket</button>
          </div>
        </div>

        {mode === 'ticker' ? (
          <div style={{ position: 'relative' }}>
            <div style={{ fontSize: 10, color: SOLAR_MUTED, marginBottom: 3 }}>TICKER</div>
            <input type="text" className="backtest-input" value={tickerQuery}
              onChange={e => { setTickerQuery(e.target.value.toUpperCase()); setShowTickerDropdown(true) }}
              onFocus={() => setShowTickerDropdown(true)}
              onBlur={() => setTimeout(() => setShowTickerDropdown(false), 120)}
              onKeyDown={e => { if (e.key === 'Enter') { const q = tickerQuery.trim().toUpperCase(); if (q) { setTicker(q); setShowTickerDropdown(false) } } }}
              placeholder="AAPL" style={{ width: 120, textTransform: 'uppercase' }} />
            {showTickerDropdown && filteredTickerOptions.length > 0 && (
              <div style={{ position: 'absolute', top: '100%', left: 0, zIndex: 50, background: SOLAR_BG, border: `1px solid ${SOLAR_BORDER}`, maxHeight: 220, overflowY: 'auto', minWidth: 120 }}>
                {filteredTickerOptions.map(t => (
                  <div key={t} onMouseDown={() => { setTicker(t); setTickerQuery(t); setShowTickerDropdown(false) }}
                    style={{ padding: '3px 8px', cursor: 'pointer', fontSize: 12, fontFamily: 'monospace', background: t === ticker ? SOLAR_BORDER : 'transparent' }}>{t}</div>
                ))}
              </div>
            )}
          </div>
        ) : (
          <div style={{ position: 'relative' }}>
            <div style={{ fontSize: 10, color: SOLAR_MUTED, marginBottom: 3 }}>BASKET</div>
            <input type="text" className="backtest-input" value={basketQuery}
              onChange={e => { setBasketQuery(e.target.value); setShowBasketDropdown(true) }}
              onFocus={() => setShowBasketDropdown(true)}
              onBlur={() => setTimeout(() => setShowBasketDropdown(false), 120)}
              onKeyDown={e => { if (e.key === 'Enter') { const q = basketQuery.trim(); if (q) { setBasket(q); setShowBasketDropdown(false) } } }}
              placeholder="Search baskets..." style={{ width: 240 }} />
            {showBasketDropdown && filteredBasketOptions.length > 0 && (
              <div style={{ position: 'absolute', top: '100%', left: 0, zIndex: 50, background: SOLAR_BG, border: `1px solid ${SOLAR_BORDER}`, maxHeight: 220, overflowY: 'auto', minWidth: 240 }}>
                {filteredBasketOptions.map(b => (
                  <div key={b} onMouseDown={() => { setBasket(b); setBasketQuery(b); setShowBasketDropdown(false) }}
                    style={{ padding: '3px 8px', cursor: 'pointer', fontSize: 12, fontFamily: 'monospace', background: b === basket ? SOLAR_BORDER : 'transparent' }}>{b}</div>
                ))}
              </div>
            )}
          </div>
        )}

        <div>
          <div style={{ fontSize: 10, color: SOLAR_MUTED, marginBottom: 3 }}>TREND LOOKBACK (bars)</div>
          <input type="number" className="backtest-input" value={lookback} min={1} max={252}
            onChange={e => setLookback(Math.max(1, Number(e.target.value) || 1))} style={{ width: 70 }} />
        </div>

        <button className="control-btn" onClick={resetAll} style={{ marginLeft: 'auto' }}>Reset conditions</button>
      </div>

      {/* Conditions grid */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 8 }}>
        {visibleDefs.map(def => {
          if (def.kind === 'tri') {
            const state = triConds[def.key]
            const mkBtn = (next: TriState, label: string) => (
              <button className={`control-btn ${state === next ? 'primary' : ''}`}
                style={{ fontSize: 11, padding: '3px 10px', flex: 1 }}
                onClick={() => setTri(def.key, next)}>{label}</button>
            )
            return (
              <div key={def.key} style={{ border: `1px solid ${SOLAR_BORDER}`, padding: 6, background: SOLAR_BG }}>
                <div style={{ fontSize: 10, color: SOLAR_MUTED, marginBottom: 4 }}>{def.label.toUpperCase()}</div>
                <div style={{ display: 'flex', gap: 4 }}>
                  {mkBtn('any', 'Any')}
                  {mkBtn('a', def.a.label)}
                  {mkBtn('b', def.b.label)}
                </div>
              </div>
            )
          }
          // Threshold condition
          const th = thresholds[def.key]
          return (
            <div key={def.key} style={{ border: `1px solid ${SOLAR_BORDER}`, padding: 6, background: SOLAR_BG }}>
              <div style={{ fontSize: 10, color: SOLAR_MUTED, marginBottom: 4 }}>{def.label.toUpperCase()}</div>
              <div style={{ display: 'flex', gap: 4, alignItems: 'center' }}>
                <button className={`control-btn ${!th.active ? 'primary' : ''}`}
                  style={{ fontSize: 11, padding: '3px 10px' }}
                  onClick={() => setTh(def.key, { active: false })}>Any</button>
                <button className={`control-btn ${th.active ? 'primary' : ''}`}
                  style={{ fontSize: 11, padding: '3px 14px', fontFamily: 'monospace', minWidth: 36 }}
                  onClick={() => {
                    if (!th.active) setTh(def.key, { active: true })
                    else cycleOp(def.key)
                  }}>{th.op}</button>
                <input type="number" className="backtest-input"
                  value={th.val} min={def.min} max={def.max} step={def.step}
                  onChange={e => {
                    const v = parseFloat(e.target.value)
                    if (!isNaN(v)) setTh(def.key, { active: true, val: Math.min(def.max, Math.max(def.min, v)) })
                  }}
                  style={{ width: 60, fontSize: 11, textAlign: 'center' }} />
              </div>
            </div>
          )
        })}
      </div>

      {/* Chart */}
      <div ref={wrapRef} style={{ width: '100%', border: `1px solid ${SOLAR_BORDER}`, background: SOLAR_BG }}>
        <canvas ref={canvasRef} style={{ width: dims.w, height: dims.h, display: 'block' }} />
      </div>

      {/* Stats table */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(9, 1fr)', gap: 0, fontSize: 12, fontFamily: 'monospace',
                    border: `1px solid ${SOLAR_BORDER}`, background: SOLAR_BG }}>
        {['Sample','N','Mean','Median','Stddev','Win%','p5','p25 / p75','p95'].map(h =>
          <div key={h} style={{ padding: '6px 8px', color: SOLAR_MUTED, borderBottom: `1px solid ${SOLAR_BORDER}` }}>{h}</div>
        )}
        <div style={{ padding: '6px 8px', color: PINK }}>Baseline</div>
        <div style={{ padding: '6px 8px' }}>{baseSt?.n ?? '--'}</div>
        <div style={{ padding: '6px 8px' }}>{pct(baseSt?.mean ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(baseSt?.median ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(baseSt?.stddev ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(baseSt?.win_rate ?? null, 1)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(baseSt?.p5 ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(baseSt?.p25 ?? null)} / {pct(baseSt?.p75 ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(baseSt?.p95 ?? null)}</div>

        <div style={{ padding: '6px 8px', color: BLUE, borderTop: `1px solid ${SOLAR_BORDER}` }}>Filtered</div>
        <div style={{ padding: '6px 8px', borderTop: `1px solid ${SOLAR_BORDER}`, fontWeight: stats && stats.n > 0 && stats.n < 30 ? 'bold' : 'normal', color: stats && stats.n > 0 && stats.n < 30 ? PINK : undefined }}>{stats?.n ?? '--'}</div>
        <div style={{ padding: '6px 8px', borderTop: `1px solid ${SOLAR_BORDER}` }}>{pct(stats?.mean ?? null)}</div>
        <div style={{ padding: '6px 8px', borderTop: `1px solid ${SOLAR_BORDER}` }}>{pct(stats?.median ?? null)}</div>
        <div style={{ padding: '6px 8px', borderTop: `1px solid ${SOLAR_BORDER}` }}>{pct(stats?.stddev ?? null)}</div>
        <div style={{ padding: '6px 8px', borderTop: `1px solid ${SOLAR_BORDER}` }}>{pct(stats?.win_rate ?? null, 1)}</div>
        <div style={{ padding: '6px 8px', borderTop: `1px solid ${SOLAR_BORDER}` }}>{pct(stats?.p5 ?? null)}</div>
        <div style={{ padding: '6px 8px', borderTop: `1px solid ${SOLAR_BORDER}` }}>{pct(stats?.p25 ?? null)} / {pct(stats?.p75 ?? null)}</div>
        <div style={{ padding: '6px 8px', borderTop: `1px solid ${SOLAR_BORDER}` }}>{pct(stats?.p95 ?? null)}</div>
      </div>

      {stats && stats.n > 0 && stats.n < 30 && (
        <div style={{ fontSize: 11, color: PINK }}>Small sample — filtered N &lt; 30. Treat with caution.</div>
      )}
      {error && <div style={{ fontSize: 11, color: PINK }}>{error}</div>}
    </div>
  )
}
