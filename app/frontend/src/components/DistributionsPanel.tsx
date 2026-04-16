import React, { useState, useEffect, useMemo, useRef } from 'react'
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

interface DistributionsPanelProps {
  apiBase: string
  activeTicker?: string | null
  allTickers?: string[]
}

type TriState = 'any' | 'a' | 'b'

// Solarized palette — aligned with TVChart / BacktestPanel
const BLUE = 'rgb(50, 50, 255)'
const BLUE_FILL = 'rgba(50, 50, 255, 0.35)'
const PINK = 'rgb(255, 50, 150)'
const PINK_FILL = 'rgba(255, 50, 150, 0.25)'
const SOLAR_BG = '#fdf6e3'
const SOLAR_BORDER = '#eee8d5'
const SOLAR_TEXT = '#586e75'
const SOLAR_MUTED = '#93a1a1'

const CONDITION_DEFS: {
  key: 'breakout_state' | 'rotation_state' | 'h_sign' | 'l_sign' | 'h_trend' | 'l_trend'
  label: string
  a: { value: string; label: string }
  b: { value: string; label: string }
}[] = [
  { key: 'breakout_state', label: 'Breakout / Breakdown', a: { value: 'breakout', label: 'Breakout' }, b: { value: 'breakdown', label: 'Breakdown' } },
  { key: 'rotation_state', label: 'Rotation',             a: { value: 'up',       label: 'Up' },       b: { value: 'down',      label: 'Down' } },
  { key: 'h_sign',         label: 'H Reaction sign',      a: { value: 'above',    label: '> 0' },      b: { value: 'below',     label: '< 0' } },
  { key: 'l_sign',         label: 'L Reaction sign',      a: { value: 'above',    label: '> 0' },      b: { value: 'below',     label: '< 0' } },
  { key: 'h_trend',        label: 'H Reaction trend',     a: { value: 'increasing', label: 'Increasing' }, b: { value: 'decreasing', label: 'Decreasing' } },
  { key: 'l_trend',        label: 'L Reaction trend',     a: { value: 'increasing', label: 'Increasing' }, b: { value: 'decreasing', label: 'Decreasing' } },
]

const pct = (v: number | null | undefined, digits = 2) =>
  v == null ? '--' : (v * 100).toFixed(digits) + '%'

export const DistributionsPanel: React.FC<DistributionsPanelProps> = ({ apiBase, activeTicker, allTickers = [] }) => {
  const [ticker, setTicker] = useState<string>(activeTicker || '')
  const [tickerQuery, setTickerQuery] = useState<string>(activeTicker || '')
  const [showTickerDropdown, setShowTickerDropdown] = useState(false)
  const [lookback, setLookback] = useState(21)
  const [conds, setConds] = useState<Record<string, TriState>>({
    breakout_state: 'any', rotation_state: 'any',
    h_sign: 'any', l_sign: 'any', h_trend: 'any', l_trend: 'any',
  })
  const [data, setData] = useState<DistResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Follow sidebar ticker changes unless user explicitly overrode
  useEffect(() => {
    if (activeTicker && activeTicker !== ticker) {
      setTicker(activeTicker)
      setTickerQuery(activeTicker)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTicker])

  const filteredTickerOptions = useMemo(() => {
    const q = tickerQuery.trim().toUpperCase()
    if (!q) return allTickers.slice(0, 30)
    return allTickers.filter(t => t.toUpperCase().startsWith(q)).slice(0, 30)
  }, [tickerQuery, allTickers])

  // Fetch whenever ticker + conditions + lookback change
  useEffect(() => {
    if (!ticker) { setData(null); return }
    const params: Record<string, string | number> = { ticker, lookback }
    for (const def of CONDITION_DEFS) {
      const state = conds[def.key]
      if (state === 'a') params[def.key] = def.a.value
      else if (state === 'b') params[def.key] = def.b.value
    }
    setLoading(true)
    setError(null)
    axios.get(`${apiBase}/distribution/next-bar`, { params })
      .then(res => { setData(res.data); setLoading(false) })
      .catch(err => {
        setData(null)
        setError(err?.response?.data?.detail || err.message || 'Request failed')
        setLoading(false)
      })
  }, [ticker, conds, lookback, apiBase])

  const setCond = (key: string, next: TriState) => setConds(prev => ({ ...prev, [key]: next }))
  const resetConds = () => setConds({
    breakout_state: 'any', rotation_state: 'any',
    h_sign: 'any', l_sign: 'any', h_trend: 'any', l_trend: 'any',
  })

  // ── KDE + histogram canvas ──
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const [dims, setDims] = useState({ w: 800, h: 360 })
  const wrapRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const onResize = () => {
      if (wrapRef.current) setDims({ w: wrapRef.current.clientWidth, h: 360 })
    }
    onResize()
    window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [])

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    const dpr = window.devicePixelRatio || 1
    canvas.width = dims.w * dpr
    canvas.height = dims.h * dpr
    ctx.scale(dpr, dpr)

    ctx.clearRect(0, 0, dims.w, dims.h)
    ctx.fillStyle = SOLAR_BG
    ctx.fillRect(0, 0, dims.w, dims.h)

    const pad = { top: 30, right: 30, bottom: 50, left: 20 }
    const plotW = dims.w - pad.left - pad.right
    const plotH = dims.h - pad.top - pad.bottom

    if (!data) {
      ctx.fillStyle = SOLAR_MUTED; ctx.font = '14px monospace'; ctx.textAlign = 'center'
      ctx.fillText(ticker ? (loading ? 'Loading…' : (error || 'No data')) : 'Select a ticker', dims.w / 2, dims.h / 2)
      return
    }

    const base = data.baseline.returns
    const filt = data.filtered.returns
    if (base.length === 0) {
      ctx.fillStyle = SOLAR_MUTED; ctx.font = '14px monospace'; ctx.textAlign = 'center'
      ctx.fillText('No baseline returns available', dims.w / 2, dims.h / 2)
      return
    }

    // X domain from baseline (5th to 95th percentile with padding) so outliers don't flatten everything
    const sortedBase = [...base].sort((a, b) => a - b)
    const q = (p: number) => sortedBase[Math.max(0, Math.min(sortedBase.length - 1, Math.floor(p * (sortedBase.length - 1))))]
    const p1 = q(0.01), p99 = q(0.99)
    const pad_x = (p99 - p1) * 0.1 || 0.01
    const xMin = p1 - pad_x
    const xMax = p99 + pad_x

    const nPoints = Math.min(200, plotW)
    const gaussianKDE = (vals: number[]): { xs: number[]; ys: number[] } => {
      if (vals.length < 2) return { xs: [], ys: [] }
      const n = vals.length
      const mean = vals.reduce((a, b) => a + b, 0) / n
      const std = Math.sqrt(vals.reduce((s, v) => s + (v - mean) ** 2, 0) / n) || (xMax - xMin) * 0.02
      const h = 1.06 * std * Math.pow(n, -0.2)  // Silverman's rule
      const invH = 1 / h
      const coeff = 1 / (n * h * Math.sqrt(2 * Math.PI))
      const xs: number[] = []
      const ys: number[] = []
      for (let i = 0; i < nPoints; i++) {
        const x = xMin + (i / (nPoints - 1)) * (xMax - xMin)
        let d = 0
        for (const v of vals) { const z = (x - v) * invH; d += Math.exp(-0.5 * z * z) }
        xs.push(x); ys.push(d * coeff)
      }
      return { xs, ys }
    }

    const baseKDE = gaussianKDE(base)
    const filtKDE = filt.length >= 2 ? gaussianKDE(filt) : { xs: [], ys: [] }
    let maxDensity = 0
    for (const y of baseKDE.ys) maxDensity = Math.max(maxDensity, y)
    for (const y of filtKDE.ys) maxDensity = Math.max(maxDensity, y)
    if (maxDensity === 0) return

    const xScale = (v: number) => pad.left + ((v - xMin) / (xMax - xMin)) * plotW
    const yScale = (d: number) => pad.top + plotH - (d / maxDensity) * plotH

    // Grid
    ctx.strokeStyle = SOLAR_BORDER; ctx.lineWidth = 1
    for (let i = 0; i <= 4; i++) {
      const y = pad.top + (plotH * i) / 4
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + plotW, y); ctx.stroke()
    }
    // Zero vertical reference
    if (xMin < 0 && xMax > 0) {
      ctx.strokeStyle = SOLAR_MUTED; ctx.setLineDash([3, 3])
      ctx.beginPath(); ctx.moveTo(xScale(0), pad.top); ctx.lineTo(xScale(0), pad.top + plotH); ctx.stroke()
      ctx.setLineDash([])
    }

    // Draw baseline (pink) first, filtered (blue) on top
    const drawCurve = (xs: number[], ys: number[], fill: string, stroke: string) => {
      if (xs.length < 2) return
      const baseline = pad.top + plotH
      ctx.beginPath()
      ctx.moveTo(xScale(xs[0]), baseline)
      for (let i = 0; i < xs.length; i++) ctx.lineTo(xScale(xs[i]), yScale(ys[i]))
      ctx.lineTo(xScale(xs[xs.length - 1]), baseline)
      ctx.closePath()
      ctx.fillStyle = fill
      ctx.fill()
      ctx.beginPath()
      ctx.moveTo(xScale(xs[0]), yScale(ys[0]))
      for (let i = 1; i < xs.length; i++) ctx.lineTo(xScale(xs[i]), yScale(ys[i]))
      ctx.strokeStyle = stroke
      ctx.lineWidth = 2
      ctx.stroke()
    }
    drawCurve(baseKDE.xs, baseKDE.ys, PINK_FILL, PINK)
    if (filtKDE.xs.length > 0) drawCurve(filtKDE.xs, filtKDE.ys, BLUE_FILL, BLUE)

    // Median markers (vertical dashed lines on the plot)
    const drawMedianTick = (v: number | null, color: string) => {
      if (v == null) return
      if (v < xMin || v > xMax) return
      const x = xScale(v)
      ctx.strokeStyle = color; ctx.lineWidth = 1; ctx.setLineDash([4, 3])
      ctx.beginPath(); ctx.moveTo(x, pad.top); ctx.lineTo(x, pad.top + plotH); ctx.stroke()
      ctx.setLineDash([])
    }
    drawMedianTick(data.baseline.median, PINK)
    drawMedianTick(data.filtered.median, BLUE)

    // X axis labels
    const nLabels = 8
    ctx.fillStyle = SOLAR_TEXT; ctx.font = '10px monospace'; ctx.textAlign = 'center'
    for (let i = 0; i <= nLabels; i++) {
      const v = xMin + (i / nLabels) * (xMax - xMin)
      ctx.fillText((v * 100).toFixed(1) + '%', xScale(v), dims.h - pad.bottom + 15)
    }

    // Legend
    const legendW = 260
    const lx = dims.w - pad.right - legendW - 4
    const ly = pad.top + 4
    ctx.fillStyle = SOLAR_BG
    ctx.fillRect(lx - 4, ly - 10, legendW, 44)
    ctx.strokeStyle = SOLAR_BORDER; ctx.lineWidth = 1
    ctx.strokeRect(lx - 4, ly - 10, legendW, 44)
    const row = (i: number, swatchFill: string, swatchStroke: string, label: string, n: number, median: number | null) => {
      const y = ly + i * 16
      ctx.fillStyle = swatchFill; ctx.fillRect(lx, y - 4, 14, 10)
      ctx.strokeStyle = swatchStroke; ctx.lineWidth = 1; ctx.strokeRect(lx, y - 4, 14, 10)
      ctx.fillStyle = SOLAR_TEXT; ctx.font = '10px monospace'; ctx.textAlign = 'left'
      ctx.fillText(label, lx + 18, y + 4)
      ctx.fillText(`N=${n}`, lx + 100, y + 4)
      ctx.textAlign = 'right'
      ctx.fillText(`med ${pct(median)}`, lx + legendW - 8, y + 4)
    }
    row(0, PINK_FILL, PINK, 'Baseline',  data.baseline.n, data.baseline.median)
    row(1, BLUE_FILL, BLUE, 'Filtered',  data.filtered.n, data.filtered.median)
  }, [data, dims, ticker, loading, error])

  // ── Stats row ──
  const stats = data?.filtered
  const base = data?.baseline

  return (
    <div className="distributions-panel" style={{ padding: 12, display: 'flex', flexDirection: 'column', gap: 12, height: '100%', overflow: 'auto' }}>

      {/* Controls row */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'flex-end' }}>
        <div style={{ position: 'relative' }}>
          <div style={{ fontSize: 10, color: SOLAR_MUTED, marginBottom: 3 }}>TICKER</div>
          <input
            type="text"
            className="backtest-input"
            value={tickerQuery}
            onChange={e => { setTickerQuery(e.target.value.toUpperCase()); setShowTickerDropdown(true) }}
            onFocus={() => setShowTickerDropdown(true)}
            onBlur={() => setTimeout(() => setShowTickerDropdown(false), 120)}
            onKeyDown={e => {
              if (e.key === 'Enter') {
                const q = tickerQuery.trim().toUpperCase()
                if (q) { setTicker(q); setShowTickerDropdown(false) }
              }
            }}
            placeholder="AAPL"
            style={{ width: 120, textTransform: 'uppercase' }}
          />
          {showTickerDropdown && filteredTickerOptions.length > 0 && (
            <div style={{
              position: 'absolute', top: '100%', left: 0, zIndex: 50, background: SOLAR_BG,
              border: `1px solid ${SOLAR_BORDER}`, maxHeight: 220, overflowY: 'auto', minWidth: 120,
            }}>
              {filteredTickerOptions.map(t => (
                <div
                  key={t}
                  onMouseDown={() => { setTicker(t); setTickerQuery(t); setShowTickerDropdown(false) }}
                  style={{ padding: '3px 8px', cursor: 'pointer', fontSize: 12, fontFamily: 'monospace',
                           background: t === ticker ? SOLAR_BORDER : 'transparent' }}
                >{t}</div>
              ))}
            </div>
          )}
        </div>

        <div>
          <div style={{ fontSize: 10, color: SOLAR_MUTED, marginBottom: 3 }}>TREND LOOKBACK (bars)</div>
          <input type="number" className="backtest-input" value={lookback} min={1} max={252}
            onChange={e => setLookback(Math.max(1, Number(e.target.value) || 1))} style={{ width: 70 }} />
        </div>

        <button className="control-btn" onClick={resetConds} style={{ marginLeft: 'auto' }}>Reset conditions</button>
      </div>

      {/* Conditions grid */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))', gap: 8 }}>
        {CONDITION_DEFS.map(def => {
          const state = conds[def.key]
          const mkBtn = (next: TriState, label: string) => (
            <button
              className={`control-btn ${state === next ? 'primary' : ''}`}
              style={{ fontSize: 11, padding: '3px 10px', flex: 1 }}
              onClick={() => setCond(def.key, next)}
            >{label}</button>
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
        })}
      </div>

      {/* Chart */}
      <div ref={wrapRef} style={{ width: '100%', border: `1px solid ${SOLAR_BORDER}`, background: SOLAR_BG }}>
        <canvas ref={canvasRef} style={{ width: dims.w, height: dims.h, display: 'block' }} />
      </div>

      {/* Stats table */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(9, 1fr)', gap: 0, fontSize: 12, fontFamily: 'monospace',
                    border: `1px solid ${SOLAR_BORDER}`, background: SOLAR_BG }}>
        <div style={{ padding: '6px 8px', color: SOLAR_MUTED, borderBottom: `1px solid ${SOLAR_BORDER}` }}>Sample</div>
        <div style={{ padding: '6px 8px', color: SOLAR_MUTED, borderBottom: `1px solid ${SOLAR_BORDER}` }}>N</div>
        <div style={{ padding: '6px 8px', color: SOLAR_MUTED, borderBottom: `1px solid ${SOLAR_BORDER}` }}>Mean</div>
        <div style={{ padding: '6px 8px', color: SOLAR_MUTED, borderBottom: `1px solid ${SOLAR_BORDER}` }}>Median</div>
        <div style={{ padding: '6px 8px', color: SOLAR_MUTED, borderBottom: `1px solid ${SOLAR_BORDER}` }}>Stddev</div>
        <div style={{ padding: '6px 8px', color: SOLAR_MUTED, borderBottom: `1px solid ${SOLAR_BORDER}` }}>Win%</div>
        <div style={{ padding: '6px 8px', color: SOLAR_MUTED, borderBottom: `1px solid ${SOLAR_BORDER}` }}>p5</div>
        <div style={{ padding: '6px 8px', color: SOLAR_MUTED, borderBottom: `1px solid ${SOLAR_BORDER}` }}>p25 / p75</div>
        <div style={{ padding: '6px 8px', color: SOLAR_MUTED, borderBottom: `1px solid ${SOLAR_BORDER}` }}>p95</div>

        <div style={{ padding: '6px 8px', color: PINK }}>Baseline</div>
        <div style={{ padding: '6px 8px' }}>{base?.n ?? '--'}</div>
        <div style={{ padding: '6px 8px' }}>{pct(base?.mean ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(base?.median ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(base?.stddev ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(base?.win_rate ?? null, 1)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(base?.p5 ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(base?.p25 ?? null)} / {pct(base?.p75 ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(base?.p95 ?? null)}</div>

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
