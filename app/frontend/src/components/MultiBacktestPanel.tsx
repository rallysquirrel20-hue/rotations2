import { useState, useEffect, useRef, useCallback, useMemo } from 'react'

/* ── Interfaces ────────────────────────────────────────────────────── */

interface BacktestFilter {
  metric: string
  condition: string
  value: number | string
  source: string
}

interface LegConfig {
  target: string
  targetType: 'basket' | 'basket_tickers' | 'ticker'
  entrySignal: string
  allocationPct: number
  positionSize: number
  filters: BacktestFilter[]
}

interface Stats {
  trades: number
  win_rate: number
  avg_winner: number
  avg_loser: number
  ev: number
  profit_factor: number
  max_dd: number
  avg_bars: number
}

interface Trade {
  ticker?: string
  entry_date: string
  exit_date: string
  entry_price: number | null
  exit_price: number | null
  change: number | null
  mfe: number | null
  mae: number | null
  bars_held: number
  regime_pass: boolean
  skipped?: boolean
}

interface DailyPosition {
  trade_idx: number
  ticker?: string
  entry_date: string
  leg_target: string
  alloc: number
  weight: number
  daily_return: number
  contribution: number
}

interface DailySnapshot {
  exposure_pct: number
  equity: number
  positions: DailyPosition[]
}

interface LegResult {
  target: string
  target_type: string
  entry_signal: string
  allocation_pct: number
  direction: string
  trades: Trade[]
  trade_paths: number[][]
  stats: Stats
}

interface MultiBacktestResult {
  legs: LegResult[]
  combined: {
    equity_curve: {
      dates: string[]
      combined: number[]
      per_leg: number[][]
      buy_hold: number[]
    }
    stats: Stats
  }
  date_range: { min: string; max: string }
  skipped_entries?: { ticker?: string; entry_date: string; leg_index: number; leg_target: string }[]
  daily_positions?: Record<number, DailySnapshot>
}

interface MultiBacktestPanelProps {
  apiBase: string
  onClose: () => void
}

/* ── Constants ─────────────────────────────────────────────────────── */

const ENTRY_SIGNALS = ['Up_Rot', 'Down_Rot', 'Breakout', 'Breakdown', 'BTFD', 'STFR', 'Buy_Hold']
const EXIT_MAP: Record<string, string> = {
  Up_Rot: 'Down_Rot', Down_Rot: 'Up_Rot',
  Breakout: 'Breakdown', Breakdown: 'Breakout',
  BTFD: 'Breakdown', STFR: 'Breakout',
  Buy_Hold: 'End of Period',
}
const LEG_COLORS = ['#1565C0', '#C2185B', '#2E7D32', '#E65100', '#6A1B9A', '#00838F']
const PCT_METRICS = ['Uptrend_Pct', 'Breakout_Pct', 'Correlation_Pct', 'RV_EMA', 'Breakdown_Pct', 'Downtrend_Pct']
const BOOL_METRICS = ['Is_Breakout_Sequence', 'Trend', 'BTFD_Triggered', 'STFR_Triggered']
const POS_PRESETS = [1, 5, 10, 25, 50, 100]
const LEV_PRESETS = [100, 110, 125, 150, 200, 250]

const pctFmt = (v: number | null) => v == null ? '--' : (v * 100).toFixed(2) + '%'
const dollarFmt = (v: number) => {
  const abs = Math.abs(v)
  const sign = v < 0 ? '-' : ''
  if (abs >= 1e6) return sign + '$' + (abs / 1e6).toFixed(1) + 'M'
  if (abs >= 1e3) return sign + '$' + (abs / 1e3).toFixed(1) + 'K'
  return sign + '$' + abs.toFixed(0)
}

function defaultLeg(): LegConfig {
  return { target: '', targetType: 'basket_tickers', entrySignal: 'Breakout', allocationPct: 50, positionSize: 25, filters: [] }
}

type ResultTab = 'equity' | 'stats' | 'trades' | 'path'

/* ── Component ─────────────────────────────────────────────────────── */

export function MultiBacktestPanel({ apiBase, onClose }: MultiBacktestPanelProps) {
  // ── Basket & ticker lists ──
  const [baskets, setBaskets] = useState<Record<string, string[]>>({})
  const [allTickers, setAllTickers] = useState<string[]>([])
  useEffect(() => {
    fetch(`${apiBase}/baskets`).then(r => r.json()).then(setBaskets).catch(() => {})
    fetch(`${apiBase}/tickers`).then(r => r.json()).then(setAllTickers).catch(() => {})
  }, [apiBase])

  // ── Per-leg search state ──
  const [legSearchOpen, setLegSearchOpen] = useState<number | null>(null)
  const [legSearchQuery, setLegSearchQuery] = useState('')
  const [legSearchHighlight, setLegSearchHighlight] = useState(0)
  const [legSearchFilter, setLegSearchFilter] = useState<'all' | 'Themes' | 'Sectors' | 'Industries' | 'Tickers'>('all')
  const legSearchRef = useRef<HTMLInputElement>(null)

  const legSearchResults = useMemo(() => {
    const results: { name: string; category: string; displayName: string }[] = []
    const q = legSearchQuery.toLowerCase().trim()
    const add = (items: string[], category: string) => {
      if (legSearchFilter !== 'all' && legSearchFilter !== category) return
      for (const item of items) {
        const display = item.replace(/_/g, ' ')
        if (!q || item.toLowerCase().includes(q) || display.toLowerCase().includes(q)) {
          results.push({ name: item, category, displayName: display })
        }
      }
    }
    add(baskets.Themes || [], 'Themes')
    add(baskets.Sectors || [], 'Sectors')
    add(baskets.Industries || [], 'Industries')
    add(allTickers, 'Tickers')
    return q ? results.slice(0, 25) : results
  }, [legSearchQuery, legSearchFilter, baskets, allTickers])

  // (handleLegSearchSelect / handleLegSearchKeyDown defined after updateLeg)

  // ── Legs config ──
  const [legs, setLegs] = useState<LegConfig[]>([
    { ...defaultLeg(), allocationPct: 50 },
    { ...defaultLeg(), allocationPct: 50 },
  ])
  const updateLeg = (i: number, patch: Partial<LegConfig>) =>
    setLegs(prev => prev.map((l, idx) => idx === i ? { ...l, ...patch } : l))

  const handleLegSearchSelect = useCallback((legIdx: number, r: { name: string; category: string }) => {
    setLegSearchOpen(null)
    setLegSearchQuery('')
    setLegSearchHighlight(0)
    const isTicker = r.category === 'Tickers'
    setLegs(prev => prev.map((l, idx) => idx === legIdx ? { ...l, target: r.name, targetType: isTicker ? 'ticker' : 'basket_tickers' } : l))
  }, [])

  const handleLegSearchKeyDown = useCallback((legIdx: number, e: React.KeyboardEvent) => {
    if (e.key === 'Escape') { setLegSearchOpen(null); setLegSearchQuery(''); legSearchRef.current?.blur() }
    else if (e.key === 'ArrowDown') { e.preventDefault(); setLegSearchHighlight(prev => Math.min(prev + 1, legSearchResults.length - 1)) }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setLegSearchHighlight(prev => Math.max(prev - 1, 0)) }
    else if (e.key === 'Enter' && legSearchResults.length > 0) { handleLegSearchSelect(legIdx, legSearchResults[legSearchHighlight]) }
  }, [legSearchResults, legSearchHighlight, handleLegSearchSelect])

  const addLeg = () => {
    if (legs.length >= 6) return
    const pct = Math.floor(100 / (legs.length + 1))
    setLegs(prev => [...prev.map(l => ({ ...l, allocationPct: pct })), { ...defaultLeg(), allocationPct: 100 - pct * legs.length }])
  }
  const removeLeg = (i: number) => {
    if (legs.length <= 2) return
    setLegs(prev => {
      const next = prev.filter((_, idx) => idx !== i)
      const pct = Math.floor(100 / next.length)
      return next.map((l, idx) => ({ ...l, allocationPct: idx < next.length - 1 ? pct : 100 - pct * (next.length - 1) }))
    })
  }

  // ── Fetch date range from first leg with a target ──
  const [dataRange, setDataRange] = useState<{ min: string; max: string } | null>(null)
  useEffect(() => {
    const firstWithTarget = legs.find(l => l.target)
    if (!firstWithTarget) return
    const { target, targetType } = firstWithTarget
    fetch(`${apiBase}/date-range/${targetType}/${encodeURIComponent(target)}`)
      .then(r => r.json())
      .then((d: { min: string; max: string }) => {
        setDataRange(d)
        setStartDate(prev => prev || d.min)
        setEndDate(prev => prev || d.max)
      })
      .catch(() => {})
  }, [legs.map(l => l.target + l.targetType).join(','), apiBase])

  // ── Shared settings ──
  const [initialEquity, setInitialEquity] = useState(100000)
  const [maxLeverage, setMaxLeverage] = useState(250)
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')

  // ── Results ──
  const [result, setResult] = useState<MultiBacktestResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [showResults, setShowResults] = useState(false)
  const [resultTab, setResultTab] = useState<ResultTab>('equity')

  // ── Equity chart state ──
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const pathCanvasRef = useRef<HTMLCanvasElement>(null)
  const pathContainerRef = useRef<HTMLDivElement>(null)
  const [dims, setDims] = useState({ w: 800, h: 400 })
  const [pathDims, setPathDims] = useState({ w: 800, h: 400 })
  const [hoveredPath, setHoveredPath] = useState<number | null>(null)
  const [pathSortCol, setPathSortCol] = useState<'leg' | 'ticker' | 'date' | 'change'>('change')
  const [pathSortAsc, setPathSortAsc] = useState(false)
  const [showBuyHold, setShowBuyHold] = useState(true)
  const [visibleLegs, setVisibleLegs] = useState<Set<number>>(new Set())
  const [showConstituents, setShowConstituents] = useState(false)

  // Zoom/pan refs
  const eqViewRef = useRef({ start: 0, end: 0 })
  const [eqViewVersion, setEqViewVersion] = useState(0)
  const eqDragRef = useRef({ dragging: false, lastX: 0 })
  const eqDidDragRef = useRef(false)
  const eqScaleRef = useRef({ padLeft: 20, plotW: 0, n: 0, startIdx: 0 })

  // Constituents hover/pin
  const [eqHoverIdx, setEqHoverIdx] = useState<number | null>(null)
  const [eqPinnedIdx, setEqPinnedIdx] = useState<number | null>(null)

  // ── Allocation validation ──
  const totalAlloc = legs.reduce((s, l) => s + l.allocationPct, 0)
  const allocValid = Math.abs(totalAlloc - 100) < 1
  const allTargetsSet = legs.every(l => l.target !== '')

  // ── Run backtest ──
  const runBacktest = useCallback(async () => {
    if (!allocValid || !allTargetsSet) return
    setLoading(true)
    setError('')
    try {
      const resp = await fetch(`${apiBase}/backtest/multi`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          legs: legs.map(l => ({
            target: l.target,
            target_type: l.targetType,
            entry_signal: l.entrySignal,
            allocation_pct: l.allocationPct / 100,
            position_size: l.positionSize / 100,
            filters: l.filters,
          })),
          start_date: startDate || null,
          end_date: endDate || null,
          initial_equity: initialEquity,
          max_leverage: maxLeverage / 100,
        }),
      })
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail: resp.statusText }))
        throw new Error(err.detail || resp.statusText)
      }
      const data: MultiBacktestResult = await resp.json()
      setResult(data)
      setShowResults(true)
      setResultTab('equity')
      eqViewRef.current = { start: 0, end: data.combined.equity_curve.dates.length - 1 }
      setEqViewVersion(v => v + 1)
    } catch (e: any) {
      setError(e.message || 'Unknown error')
    } finally {
      setLoading(false)
    }
  }, [apiBase, legs, startDate, endDate, initialEquity, maxLeverage, allocValid, allTargetsSet])

  // ── ResizeObserver for equity canvas container ──
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const obs = new ResizeObserver(entries => {
      const { width, height } = entries[0].contentRect
      if (width > 0 && height > 0) setDims({ w: width, h: height })
    })
    obs.observe(el)
    return () => obs.disconnect()
  }, [resultTab, showResults])

  // ── ResizeObserver for path canvas container ──
  useEffect(() => {
    const el = pathContainerRef.current
    if (!el) return
    const obs = new ResizeObserver(entries => {
      const { width, height } = entries[0].contentRect
      if (width > 0 && height > 0) setPathDims({ w: width, h: height })
    })
    obs.observe(el)
    return () => obs.disconnect()
  }, [resultTab, showResults])

  // ── Range presets ──
  const setEqPreset = useCallback((preset: '1Y' | '3Y' | '5Y' | 'YTD' | 'All') => {
    if (!result) return
    const dates = result.combined.equity_curve.dates
    const n = dates.length
    if (n === 0) return
    let startIdx = 0
    const endIdx = n - 1
    if (preset === 'All') { startIdx = 0 }
    else {
      const lastDate = new Date(dates[n - 1] + 'T00:00:00Z')
      let cutoff: Date
      if (preset === 'YTD') cutoff = new Date(Date.UTC(lastDate.getUTCFullYear(), 0, 1))
      else {
        const years = preset === '1Y' ? 1 : preset === '3Y' ? 3 : 5
        cutoff = new Date(Date.UTC(lastDate.getUTCFullYear() - years, lastDate.getUTCMonth(), lastDate.getUTCDate()))
      }
      const cutStr = cutoff.toISOString().slice(0, 10)
      startIdx = dates.findIndex(d => d >= cutStr)
      if (startIdx < 0) startIdx = 0
    }
    eqViewRef.current = { start: startIdx, end: endIdx }
    setEqViewVersion(v => v + 1)
  }, [result])

  // ── Windowed equity data ──
  const eqWindowed = useMemo(() => {
    if (!result) return null
    const eq = result.combined.equity_curve
    const { start: s, end: e } = eqViewRef.current
    const cs = Math.max(0, Math.min(s, eq.dates.length - 1))
    const ce = Math.max(cs, Math.min(e, eq.dates.length - 1))
    // Rebase all series to 0% return from visible window start
    const rebase = (arr: number[]) => {
      const slice = arr.slice(cs, ce + 1)
      const base = slice[0] || 1
      return slice.map(v => v / base - 1)
    }
    return {
      dates: eq.dates.slice(cs, ce + 1),
      combined: rebase(eq.combined),
      per_leg: eq.per_leg.map(l => rebase(l)),
      buy_hold: rebase(eq.buy_hold),
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [result, eqViewVersion])

  // ── Draw equity curve ──
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas || !eqWindowed || resultTab !== 'equity') return
    const { dates, combined, per_leg, buy_hold } = eqWindowed
    if (!dates.length) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    const dpr = window.devicePixelRatio || 1
    canvas.width = dims.w * dpr
    canvas.height = dims.h * dpr
    ctx.scale(dpr, dpr)

    const pad = { top: 20, right: 70, bottom: 50, left: 20 }
    const plotW = dims.w - pad.left - pad.right
    const plotH = dims.h - pad.top - pad.bottom
    const n = dates.length

    // Y range (percentage returns)
    let yMin = 0, yMax = 0
    for (const v of combined) { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) }
    if (showBuyHold) for (const v of buy_hold) { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) }
    per_leg.forEach((leg, i) => { if (visibleLegs.has(i)) for (const v of leg) { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) } })
    const yPad = (yMax - yMin) * 0.1 || 0.05
    yMin -= yPad; yMax += yPad

    eqScaleRef.current = { padLeft: pad.left, plotW, n, startIdx: eqViewRef.current.start }
    const xScale = (i: number) => pad.left + (n > 1 ? (i / (n - 1)) * plotW : plotW / 2)
    const yScale = (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH

    ctx.clearRect(0, 0, dims.w, dims.h)
    ctx.fillStyle = '#fdf6e3'
    ctx.fillRect(0, 0, dims.w, dims.h)

    // Grid
    ctx.strokeStyle = '#eee8d5'; ctx.lineWidth = 1
    const nTicks = 6
    for (let i = 0; i <= nTicks; i++) {
      const v = yMin + (yMax - yMin) * (i / nTicks)
      const y = yScale(v)
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(dims.w - pad.right, y); ctx.stroke()
      ctx.fillStyle = '#657b83'; ctx.font = '10px monospace'; ctx.textAlign = 'left'
      ctx.fillText((v * 100).toFixed(1) + '%', dims.w - pad.right + 5, y + 3)
    }

    // Zero line (breakeven)
    const beY = yScale(0)
    ctx.strokeStyle = '#93a1a1'; ctx.lineWidth = 1; ctx.setLineDash([4, 4])
    ctx.beginPath(); ctx.moveTo(pad.left, beY); ctx.lineTo(dims.w - pad.right, beY); ctx.stroke()
    ctx.setLineDash([])

    // X labels
    const labelInterval = Math.max(1, Math.floor(n / 8))
    ctx.fillStyle = '#657b83'; ctx.font = '10px monospace'; ctx.textAlign = 'center'
    for (let i = 0; i < n; i += labelInterval) {
      ctx.fillText(dates[i].slice(0, 7), xScale(i), dims.h - pad.bottom + 15)
    }

    // Draw helper
    const drawLine = (data: number[], color: string, lw: number) => {
      if (data.length === 0) return
      ctx.strokeStyle = color; ctx.lineWidth = lw
      ctx.beginPath()
      for (let i = 0; i < data.length; i++) {
        const x = xScale(i), y = yScale(data[i])
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y)
      }
      ctx.stroke()
    }

    // Draw order: buy-hold → per-leg → combined
    if (showBuyHold && buy_hold.length > 0) drawLine(buy_hold, '#93a1a1', 1.5)
    per_leg.forEach((vals, i) => { if (visibleLegs.has(i)) drawLine(vals, LEG_COLORS[i % LEG_COLORS.length], 1.5) })
    drawLine(combined, '#8BC34A', 2.5)

    // Legend (values are percentage returns)
    const legendItems: { label: string; value: string; color: string }[] = []
    legendItems.push({ label: 'Combined', value: pctFmt(combined[n - 1]), color: '#8BC34A' })
    if (result) {
      result.legs.forEach((leg, i) => {
        if (visibleLegs.has(i))
          legendItems.push({ label: `${leg.target} ${leg.entry_signal}`, value: pctFmt(per_leg[i][n - 1]), color: LEG_COLORS[i % LEG_COLORS.length] })
      })
    }
    if (showBuyHold && buy_hold.length > 0) {
      legendItems.push({ label: 'Buy&Hold', value: pctFmt(buy_hold[n - 1]), color: '#93a1a1' })
    }

    if (legendItems.length > 0) {
      const lx = pad.left + 10, ly = pad.top + 10, rowH = 16
      const boxH = legendItems.length * rowH + 2
      ctx.fillStyle = '#fdf6e3'; ctx.fillRect(lx - 4, ly - 10, 250, boxH)
      ctx.strokeStyle = '#93a1a1'; ctx.lineWidth = 1; ctx.strokeRect(lx - 4, ly - 10, 250, boxH)
      legendItems.forEach((item, idx) => {
        const iy = ly + idx * rowH
        ctx.strokeStyle = item.color; ctx.lineWidth = 1.5
        ctx.beginPath(); ctx.moveTo(lx, iy); ctx.lineTo(lx + 20, iy); ctx.stroke()
        ctx.fillStyle = '#586e75'; ctx.font = 'bold 10px monospace'; ctx.textAlign = 'left'
        ctx.fillText(`${item.label} ${item.value}`, lx + 24, iy + 3)
      })
    }
  }, [eqWindowed, resultTab, dims, showBuyHold, visibleLegs, result])

  // ── Combined trade paths (all legs merged with leg index) ──
  const allPaths = useMemo(() => {
    if (!result) return []
    const items: { path: number[]; trade: Trade; legIdx: number; legTarget: string; ret: number }[] = []
    result.legs.forEach((leg, li) => {
      leg.trades.forEach((t, ti) => {
        const p = leg.trade_paths?.[ti] ?? []
        items.push({ path: p, trade: t, legIdx: li, legTarget: leg.target, ret: p.length > 0 ? p[p.length - 1] : 0 })
      })
    })
    return items
  }, [result])

  // ── Draw trade paths ──
  useEffect(() => {
    const canvas = pathCanvasRef.current
    if (!canvas || !result || resultTab !== 'path') return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    const dpr = window.devicePixelRatio || 1
    canvas.width = pathDims.w * dpr
    canvas.height = pathDims.h * dpr
    ctx.scale(dpr, dpr)
    ctx.clearRect(0, 0, pathDims.w, pathDims.h)
    ctx.fillStyle = '#fdf6e3'
    ctx.fillRect(0, 0, pathDims.w, pathDims.h)

    if (allPaths.length === 0) {
      ctx.fillStyle = '#6c757d'; ctx.font = '14px monospace'; ctx.textAlign = 'center'
      ctx.fillText('No trade paths', pathDims.w / 2, pathDims.h / 2)
      return
    }

    let maxBars = 0
    let yMin = 0, yMax = 0
    for (const { path } of allPaths) {
      maxBars = Math.max(maxBars, path.length)
      for (const v of path) { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) }
    }
    if (maxBars < 2) return
    const yPad = (yMax - yMin) * 0.1 || 0.05
    yMin -= yPad; yMax += yPad

    const pad = { top: 20, right: 60, bottom: 50, left: 20 }
    const plotW = pathDims.w - pad.left - pad.right
    const plotH = pathDims.h - pad.top - pad.bottom
    const xScale = (i: number) => pad.left + (i / (maxBars - 1)) * plotW
    const yScale = (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH

    // Grid
    ctx.strokeStyle = '#e9ecef'; ctx.lineWidth = 1
    const nTicks = 6
    for (let i = 0; i <= nTicks; i++) {
      const v = yMin + (yMax - yMin) * (i / nTicks)
      const y = yScale(v)
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pathDims.w - pad.right, y); ctx.stroke()
      ctx.fillStyle = '#6c757d'; ctx.font = '10px monospace'; ctx.textAlign = 'left'
      ctx.fillText((v * 100).toFixed(1) + '%', pathDims.w - pad.right + 5, y + 3)
    }

    // Zero line
    ctx.strokeStyle = '#adb5bd'; ctx.lineWidth = 1; ctx.setLineDash([4, 4])
    ctx.beginPath(); ctx.moveTo(pad.left, yScale(0)); ctx.lineTo(pathDims.w - pad.right, yScale(0)); ctx.stroke()
    ctx.setLineDash([])

    // X labels
    const labelInterval = Math.max(1, Math.floor(maxBars / 10))
    ctx.fillStyle = '#6c757d'; ctx.font = '10px monospace'; ctx.textAlign = 'center'
    for (let i = 0; i < maxBars; i += labelInterval) ctx.fillText(String(i), xScale(i), pathDims.h - pad.bottom + 15)

    // Rank & color
    const ranked = allPaths.map((d, gi) => ({ ...d, gi })).filter(d => d.path.length >= 2).sort((a, b) => b.ret - a.ret)
    const total = ranked.length
    const rankColor = (rank: number) => {
      if (total <= 1) return 'rgb(50, 50, 255)'
      const t = rank / (total - 1)
      if (t <= 0.5) { const s = t * 2; return `rgb(${Math.round(50 + 102 * s)}, 50, ${Math.round(255 - 53 * s)})` }
      const s = (t - 0.5) * 2; return `rgb(${Math.round(152 + 103 * s)}, 50, ${Math.round(202 - 52 * s)})`
    }

    for (let ri = 0; ri < total; ri++) {
      const { path, gi } = ranked[ri]
      if (gi === hoveredPath) continue
      const isOther = hoveredPath !== null
      ctx.strokeStyle = isOther ? '#dee2e6' : rankColor(ri)
      ctx.lineWidth = 1.2; ctx.globalAlpha = isOther ? 0.3 : 1
      ctx.beginPath()
      for (let i = 0; i < path.length; i++) { const x = xScale(i), y = yScale(path[i]); i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y) }
      ctx.stroke()
    }
    if (hoveredPath !== null) {
      const hovRank = ranked.findIndex(d => d.gi === hoveredPath)
      const entry = ranked[hovRank]
      if (entry && entry.path.length >= 2) {
        ctx.strokeStyle = rankColor(hovRank); ctx.lineWidth = 2.5; ctx.globalAlpha = 1
        ctx.beginPath()
        for (let i = 0; i < entry.path.length; i++) { const x = xScale(i), y = yScale(entry.path[i]); i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y) }
        ctx.stroke()
      }
    }
    ctx.globalAlpha = 1
  }, [allPaths, resultTab, pathDims, hoveredPath, result])

  // ── Wheel zoom + drag pan ──
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas || !result || resultTab !== 'equity') return
    const n = result.combined.equity_curve.dates.length
    if (n < 2) return

    const handleWheel = (e: WheelEvent) => {
      e.preventDefault()
      const rect = canvas.getBoundingClientRect()
      const frac = (e.clientX - rect.left) / rect.width
      const { start, end } = eqViewRef.current
      const range = end - start
      const zoomFactor = e.deltaY > 0 ? 1.15 : 0.87
      const newRange = Math.max(10, Math.min(n - 1, Math.round(range * zoomFactor)))
      const delta = newRange - range
      const leftAdj = Math.round(delta * frac)
      const rightAdj = delta - leftAdj
      let ns = start - leftAdj, ne = end + rightAdj
      if (ns < 0) { ne -= ns; ns = 0 }
      if (ne >= n) { ns -= (ne - n + 1); ne = n - 1 }
      ns = Math.max(0, ns); ne = Math.min(n - 1, ne)
      eqViewRef.current = { start: ns, end: ne }
      setEqViewVersion(v => v + 1)
    }

    const handleMouseDown = (e: MouseEvent) => {
      eqDidDragRef.current = false
      eqDragRef.current = { dragging: true, lastX: e.clientX }
      canvas.style.cursor = 'grabbing'
    }
    const handleMouseMove = (e: MouseEvent) => {
      if (!eqDragRef.current.dragging) return
      eqDidDragRef.current = true
      const rect = canvas.getBoundingClientRect()
      const dx = e.clientX - eqDragRef.current.lastX
      const { start, end } = eqViewRef.current
      const range = end - start
      const indexDelta = Math.round((dx / rect.width) * range)
      if (indexDelta === 0) return
      let ns = start - indexDelta, ne = end - indexDelta
      if (ns < 0) { ne -= ns; ns = 0 }
      if (ne >= n) { ns -= (ne - n + 1); ne = n - 1 }
      ns = Math.max(0, ns); ne = Math.min(n - 1, ne)
      eqViewRef.current = { start: ns, end: ne }
      eqDragRef.current.lastX = e.clientX
      setEqViewVersion(v => v + 1)
    }
    const handleMouseUp = () => {
      if (eqDragRef.current.dragging) { eqDragRef.current.dragging = false; canvas.style.cursor = 'grab' }
    }

    canvas.addEventListener('wheel', handleWheel, { passive: false })
    canvas.addEventListener('mousedown', handleMouseDown)
    window.addEventListener('mousemove', handleMouseMove)
    window.addEventListener('mouseup', handleMouseUp)
    return () => {
      canvas.removeEventListener('wheel', handleWheel)
      canvas.removeEventListener('mousedown', handleMouseDown)
      window.removeEventListener('mousemove', handleMouseMove)
      window.removeEventListener('mouseup', handleMouseUp)
    }
  }, [result, resultTab])

  // ── Constituents overlay: hover, pin, escape ──
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas || !result || resultTab !== 'equity' || !showConstituents) return

    const handleMove = (e: MouseEvent) => {
      if (eqDragRef.current.dragging || eqPinnedIdx != null) return
      const rect = canvas.getBoundingClientRect()
      const x = e.clientX - rect.left
      const { padLeft, plotW, n, startIdx } = eqScaleRef.current
      if (n <= 1 || plotW <= 0) return
      const idx = Math.round(((x - padLeft) / plotW) * (n - 1)) + startIdx
      setEqHoverIdx(idx >= startIdx && idx < startIdx + n ? idx : null)
    }
    const handleClick = (e: MouseEvent) => {
      if (eqDidDragRef.current) return
      const rect = canvas.getBoundingClientRect()
      const x = e.clientX - rect.left
      const { padLeft, plotW, n, startIdx } = eqScaleRef.current
      if (n <= 1 || plotW <= 0) return
      const idx = Math.round(((x - padLeft) / plotW) * (n - 1)) + startIdx
      if (idx >= startIdx && idx < startIdx + n) setEqPinnedIdx(prev => prev === idx ? null : idx)
    }
    const handleKey = (e: KeyboardEvent) => { if (e.key === 'Escape') { setEqPinnedIdx(null); setEqHoverIdx(null) } }
    const handleLeave = () => { if (eqPinnedIdx == null) setEqHoverIdx(null) }

    canvas.addEventListener('mousemove', handleMove)
    canvas.addEventListener('click', handleClick)
    canvas.addEventListener('mouseleave', handleLeave)
    window.addEventListener('keydown', handleKey)
    return () => {
      canvas.removeEventListener('mousemove', handleMove)
      canvas.removeEventListener('click', handleClick)
      canvas.removeEventListener('mouseleave', handleLeave)
      window.removeEventListener('keydown', handleKey)
    }
  }, [result, resultTab, showConstituents, eqPinnedIdx])

  // ── Trades table ──
  const [sortCol, setSortCol] = useState('entry_date')
  const [sortAsc, setSortAsc] = useState(true)
  const [tradesLegFilter, setTradesLegFilter] = useState(-1)

  const allTrades = useMemo(() => {
    if (!result) return []
    const trades: (Trade & { legTarget: string; legSignal: string; legIdx: number })[] = []
    result.legs.forEach((leg, i) => {
      leg.trades.forEach(t => trades.push({ ...t, legTarget: leg.target, legSignal: leg.entry_signal, legIdx: i }))
    })
    return trades
  }, [result])

  const filteredTrades = useMemo(() => {
    const t = tradesLegFilter < 0 ? allTrades : allTrades.filter(x => x.legIdx === tradesLegFilter)
    const col = sortCol
    const dir = sortAsc ? 1 : -1
    return [...t].sort((a: any, b: any) => {
      const av = a[col], bv = b[col]
      if (av == null && bv == null) return 0
      if (av == null) return 1
      if (bv == null) return -1
      if (typeof av === 'string') return av.localeCompare(bv) * dir
      return (av - bv) * dir
    })
  }, [allTrades, tradesLegFilter, sortCol, sortAsc])

  // ── Filter helpers ──
  const addFilter = (legIdx: number) => {
    updateLeg(legIdx, { filters: [...legs[legIdx].filters, { metric: 'Uptrend_Pct', condition: 'above', value: 50, source: 'self' }] })
  }
  const removeFilter = (legIdx: number, fIdx: number) => {
    updateLeg(legIdx, { filters: legs[legIdx].filters.filter((_, i) => i !== fIdx) })
  }
  const updateFilter = (legIdx: number, fIdx: number, field: string, val: any) => {
    const updated = legs[legIdx].filters.map((f, i) => i === fIdx ? { ...f, [field]: val } : f)
    updateLeg(legIdx, { filters: updated })
  }

  // Toggle button style helper (matches BacktestPanel)
  const toggleStyle = (active: boolean, color: string) => active
    ? { background: color, borderColor: color, color: '#fff', border: '1px solid', borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontWeight: 600 as const, fontSize: 11 }
    : { background: 'transparent', borderColor: '#ccc', color: '#657b83', border: '1px solid', borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: 11 }

  /* ━━━━━━━━━━━━━━━━━━━━━━ RENDER ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

  if (showResults && result) {
    return (
      <div className="backtest-panel">
        <div className="backtest-results-header">
          <button className="backtest-nav-btn" onClick={() => setShowResults(false)}>Configure</button>
          <span style={{ fontWeight: 600, fontSize: 13 }}>
            Multi-Basket: {result.legs.map(l => `${l.target}(${l.entry_signal})`).join(' + ')}
          </span>
          <button className="backtest-nav-btn" onClick={onClose}>Close</button>
        </div>

        <div className="summary-tabs" style={{ padding: '0 12px' }}>
          {(['equity', 'path', 'stats', 'trades'] as ResultTab[]).map(tab => (
            <button key={tab} className={`summary-tab ${resultTab === tab ? 'active' : ''}`}
              onClick={() => setResultTab(tab)}>{tab.charAt(0).toUpperCase() + tab.slice(1)}</button>
          ))}
        </div>

        <div className="backtest-body">
          <div className="backtest-main">
            <div className="backtest-content">
              {/* ── Equity tab ── */}
              {resultTab === 'equity' && (
                <>
                  <div className="analysis-date-controls">
                    <div className="bt-strat-btns">
                      <button className="bt-strat-btn active"
                        style={{ background: '#8BC34A', borderColor: '#8BC34A', color: '#fff' }}>Combined</button>
                      {result.legs.map((leg, i) => (
                        <button key={i} className={`bt-strat-btn ${visibleLegs.has(i) ? 'active' : ''}`}
                          style={visibleLegs.has(i) ? { background: LEG_COLORS[i % LEG_COLORS.length], borderColor: LEG_COLORS[i % LEG_COLORS.length], color: '#fff' } : undefined}
                          onClick={() => setVisibleLegs(prev => { const next = new Set(prev); if (next.has(i)) next.delete(i); else next.add(i); return next })}>{leg.target.replace(/_/g, ' ')}</button>
                      ))}
                      <button className={`bt-strat-btn ${showBuyHold ? 'active' : ''}`}
                        style={showBuyHold ? { background: '#93a1a1', borderColor: '#93a1a1', color: '#fff' } : undefined}
                        onClick={() => setShowBuyHold(v => !v)}>Buy Hold</button>
                      <button className={`bt-strat-btn ${showConstituents ? 'active' : ''}`}
                        style={showConstituents ? { background: '#d33682', borderColor: '#d33682', color: '#fff' } : undefined}
                        onClick={() => { setShowConstituents(v => !v); setEqPinnedIdx(null); setEqHoverIdx(null) }}>Const</button>
                    </div>
                    <div style={{ flex: 1 }} />
                    <div className="basket-returns-presets">
                      {(['1Y', '3Y', '5Y', 'YTD', 'All'] as const).map(p => (
                        <button key={p} className="basket-returns-preset-btn" onClick={() => setEqPreset(p)}>{p}</button>
                      ))}
                    </div>
                  </div>
                  <div className="backtest-chart" ref={containerRef} style={{ position: 'relative' }}>
                    <canvas ref={canvasRef} style={{ display: 'block', width: '100%', height: '100%', cursor: 'grab' }} />
                    {showConstituents && (() => {
                      const activeIdx = eqPinnedIdx ?? eqHoverIdx
                      if (activeIdx == null) return null
                      const { padLeft, plotW, n, startIdx } = eqScaleRef.current
                      const localIdx = activeIdx - startIdx
                      const crosshairX = n > 1 ? padLeft + (localIdx / (n - 1)) * plotW : padLeft
                      const showCrosshair = localIdx >= 0 && localIdx < n
                      const snapshot = result?.daily_positions?.[activeIdx]
                      const date = result?.combined.equity_curve.dates[activeIdx]
                      return (
                        <>
                          {showCrosshair && (
                            <div style={{
                              position: 'absolute', top: 0, bottom: 0, left: crosshairX, width: 1,
                              background: eqPinnedIdx != null ? 'rgb(50,50,255)' : '#93a1a1',
                              pointerEvents: 'none', zIndex: 100,
                            }} />
                          )}
                          {snapshot && date && (
                            <div className="candle-detail-overlay" style={eqPinnedIdx != null ? { pointerEvents: 'auto', borderColor: 'rgb(50,50,255)' } : undefined}>
                              <div className="candle-detail-title">
                                {date} &mdash; {dollarFmt(snapshot.equity)}
                                {eqPinnedIdx != null
                                  ? <span style={{ marginLeft: 8, fontSize: 9, color: 'rgb(50,50,255)' }}>PINNED (esc)</span>
                                  : <span style={{ marginLeft: 8, fontSize: 9, color: '#93a1a1' }}>click to pin</span>}
                              </div>
                              <div className="candle-detail-row" style={{ fontWeight: 'bold', borderBottom: '1px solid #ccc' }}>
                                <span className="ticker">Ticker</span>
                                <span className="weight">Weight</span>
                                <span className="ret">Return</span>
                                <span className="contrib">Contrib</span>
                              </div>
                              {snapshot.positions.map((p, i) => (
                                <div key={i} className="candle-detail-row">
                                  <span className="ticker" style={{ fontSize: 10 }}>{p.ticker || p.leg_target}</span>
                                  <span className="weight">{(p.weight * 100).toFixed(1)}%</span>
                                  <span className="ret" style={{ color: p.daily_return >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)' }}>
                                    {(p.daily_return * 100).toFixed(2)}%</span>
                                  <span className="contrib" style={{ color: p.contribution >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)' }}>
                                    {(p.contribution * 100).toFixed(3)}%</span>
                                </div>
                              ))}
                              <div className="candle-detail-row" style={{ borderTop: '1px solid #93a1a1', fontWeight: 'bold' }}>
                                <span className="ticker">Total</span>
                                <span className="weight">{(snapshot.exposure_pct * 100).toFixed(1)}%</span>
                                <span className="ret"></span>
                                <span className="contrib" style={{
                                  color: snapshot.positions.reduce((s, p) => s + p.contribution, 0) >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)'
                                }}>
                                  {(snapshot.positions.reduce((s, p) => s + p.contribution, 0) * 100).toFixed(3)}%</span>
                              </div>
                            </div>
                          )}
                        </>
                      )
                    })()}
                  </div>
                </>
              )}

              {/* ── Path tab ── */}
              {resultTab === 'path' && (
                <div className="backtest-path-container">
                  <div className="backtest-chart" ref={pathContainerRef}>
                    <canvas ref={pathCanvasRef} style={{ display: 'block', width: '100%', height: '100%' }} />
                  </div>
                  <div className="backtest-path-legend" style={{ width: 320, minWidth: 320 }}>
                    {(() => {
                      const rankColor = (rank: number, total: number): string => {
                        if (total <= 1) return 'rgb(50, 50, 255)'
                        const t = rank / (total - 1)
                        if (t <= 0.5) { const s = t * 2; return `rgb(${Math.round(50 + 102 * s)}, 50, ${Math.round(255 - 53 * s)})` }
                        const s = (t - 0.5) * 2; return `rgb(${Math.round(152 + 103 * s)}, 50, ${Math.round(202 - 52 * s)})`
                      }
                      const items = allPaths.map((d, gi) => ({ ...d, gi }))
                      const hasTickers = items.some(d => d.trade.ticker)
                      const colorRanked = [...items].sort((a, b) => b.ret - a.ret)
                      const colorMap = new Map(colorRanked.map((d, rank) => [d.gi, rankColor(rank, colorRanked.length)]))
                      const sorted = [...items].sort((a, b) => {
                        let cmp = 0
                        if (pathSortCol === 'leg') cmp = a.legTarget.localeCompare(b.legTarget)
                        else if (pathSortCol === 'ticker') cmp = (a.trade.ticker ?? '').localeCompare(b.trade.ticker ?? '')
                        else if (pathSortCol === 'date') cmp = a.trade.entry_date.localeCompare(b.trade.entry_date)
                        else cmp = a.ret - b.ret
                        return pathSortAsc ? cmp : -cmp
                      })
                      const toggleSort = (col: typeof pathSortCol) => {
                        if (pathSortCol === col) setPathSortAsc(v => !v)
                        else { setPathSortCol(col); setPathSortAsc(col === 'change' ? false : true) }
                      }
                      const arrow = (col: typeof pathSortCol) => pathSortCol === col ? (pathSortAsc ? ' \u25B2' : ' \u25BC') : ''
                      return (
                        <>
                          <div className="path-legend-header">
                            <span className="path-legend-col ticker" onClick={() => toggleSort('leg')}>Leg{arrow('leg')}</span>
                            {hasTickers && <span className="path-legend-col ticker" onClick={() => toggleSort('ticker')}>Ticker{arrow('ticker')}</span>}
                            <span className="path-legend-col date" onClick={() => toggleSort('date')}>Date{arrow('date')}</span>
                            <span className="path-legend-col change" onClick={() => toggleSort('change')}>Chg{arrow('change')}</span>
                          </div>
                          {sorted.map(({ trade, gi, ret, legTarget, legIdx }) => (
                            <div key={gi}
                              className={`path-legend-row ${hoveredPath === gi ? 'highlighted' : ''}`}
                              style={{ color: colorMap.get(gi) }}
                              onMouseEnter={() => setHoveredPath(gi)}
                              onMouseLeave={() => setHoveredPath(null)}>
                              <span className="path-legend-col ticker" style={{ color: LEG_COLORS[legIdx % LEG_COLORS.length] }}>{legTarget.replace(/_/g, ' ').slice(0, 10)}</span>
                              {hasTickers && <span className="path-legend-col ticker">{trade.ticker || ''}</span>}
                              <span className="path-legend-col date">{trade.entry_date.slice(2)}</span>
                              <span className="path-legend-col change">{(ret * 100).toFixed(1)}%</span>
                            </div>
                          ))}
                        </>
                      )
                    })()}
                  </div>
                </div>
              )}

              {/* ── Stats tab ── */}
              {resultTab === 'stats' && (
                <div style={{ padding: 12, overflowY: 'auto' }}>
                  <table className="backtest-stats-table" style={{ width: '100%', marginBottom: 16 }}>
                    <thead>
                      <tr>
                        <th className="backtest-stats-th"></th>
                        {result.legs.map((leg, i) => (
                          <th key={i} className="backtest-stats-th" style={{ color: LEG_COLORS[i % LEG_COLORS.length] }}>
                            {leg.target}<br /><span style={{ fontWeight: 400, fontSize: 11 }}>{leg.entry_signal} ({leg.direction})</span>
                          </th>
                        ))}
                        <th className="backtest-stats-th" style={{ color: '#8BC34A' }}>Combined</th>
                      </tr>
                    </thead>
                    <tbody>
                      {[
                        { label: 'Allocation', fmt: (_s: Stats, i: number) => (result.legs[i]?.allocation_pct * 100).toFixed(0) + '%', combined: '100%' },
                        { label: 'Trades', fmt: (s: Stats) => String(s.trades), combined: String(result.combined.stats.trades) },
                        { label: 'Win Rate', fmt: (s: Stats) => pctFmt(s.win_rate), combined: pctFmt(result.combined.stats.win_rate) },
                        { label: 'Avg Winner', fmt: (s: Stats) => pctFmt(s.avg_winner), combined: pctFmt(result.combined.stats.avg_winner) },
                        { label: 'Avg Loser', fmt: (s: Stats) => pctFmt(s.avg_loser), combined: pctFmt(result.combined.stats.avg_loser) },
                        { label: 'EV', fmt: (s: Stats) => pctFmt(s.ev), combined: pctFmt(result.combined.stats.ev) },
                        { label: 'Profit Factor', fmt: (s: Stats) => s.profit_factor.toFixed(2), combined: result.combined.stats.profit_factor.toFixed(2) },
                        { label: 'Max DD', fmt: (s: Stats) => pctFmt(s.max_dd), combined: pctFmt(result.combined.stats.max_dd) },
                        { label: 'Avg Bars', fmt: (s: Stats) => s.avg_bars.toFixed(1), combined: result.combined.stats.avg_bars.toFixed(1) },
                      ].map(row => (
                        <tr key={row.label}>
                          <td className="backtest-stats-td" style={{ fontWeight: 600 }}>{row.label}</td>
                          {result.legs.map((leg, i) => (
                            <td key={i} className="backtest-stats-td">{row.fmt(leg.stats, i)}</td>
                          ))}
                          <td className="backtest-stats-td" style={{ fontWeight: 600 }}>{row.combined}</td>
                        </tr>
                      ))}
                      {result.combined.equity_curve.dates.length > 0 && (
                        <tr>
                          <td className="backtest-stats-td" style={{ fontWeight: 600 }}>Final Equity</td>
                          {result.legs.map((_, i) => {
                            const vals = result.combined.equity_curve.per_leg[i]
                            return <td key={i} className="backtest-stats-td">{dollarFmt(vals[vals.length - 1])}</td>
                          })}
                          <td className="backtest-stats-td" style={{ fontWeight: 600 }}>
                            {dollarFmt(result.combined.equity_curve.combined[result.combined.equity_curve.combined.length - 1])}
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              )}

              {/* ── Trades tab ── */}
              {resultTab === 'trades' && (
                <div style={{ padding: '8px 12px', overflowY: 'auto', flex: 1, minHeight: 0 }}>
                  <div style={{ display: 'flex', gap: 8, marginBottom: 8, alignItems: 'center' }}>
                    <span style={{ fontSize: 12, fontWeight: 600 }}>Filter:</span>
                    <button className={`summary-tab ${tradesLegFilter === -1 ? 'active' : ''}`}
                      onClick={() => setTradesLegFilter(-1)}>All ({allTrades.length})</button>
                    {result.legs.map((leg, i) => (
                      <button key={i}
                        className={`summary-tab ${tradesLegFilter === i ? 'active' : ''}`}
                        style={{ borderBottomColor: tradesLegFilter === i ? LEG_COLORS[i] : undefined }}
                        onClick={() => setTradesLegFilter(i)}>
                        {leg.target} ({leg.trades.length})
                      </button>
                    ))}
                  </div>
                  <div className="summary-table-wrapper">
                    <table className="summary-table">
                      <thead>
                        <tr>
                          {['legTarget', 'ticker', 'entry_date', 'exit_date', 'entry_price', 'exit_price', 'change', 'bars_held'].map(col => (
                            <th key={col} className="summary-th"
                              onClick={() => { setSortCol(col); setSortAsc(sortCol === col ? !sortAsc : true) }}
                              style={{ cursor: 'pointer' }}>
                              {col === 'legTarget' ? 'Leg' : col.replace(/_/g, ' ')}
                              {sortCol === col && (sortAsc ? ' \u2191' : ' \u2193')}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {filteredTrades.slice(0, 500).map((t, i) => (
                          <tr key={i} style={{
                            background: t.skipped ? 'rgba(147,161,161,0.12)' :
                              (t.change != null && t.change > 0 ? 'rgba(50,50,255,0.06)' : 'rgba(255,50,150,0.06)')
                          }}>
                            <td className="summary-td" style={{ color: LEG_COLORS[t.legIdx % LEG_COLORS.length], fontWeight: 600, fontSize: 11 }}>
                              {t.legTarget}
                            </td>
                            <td className="summary-td">{t.ticker || ''}</td>
                            <td className="summary-td">{t.entry_date}</td>
                            <td className="summary-td">{t.exit_date}</td>
                            <td className="summary-td">{t.entry_price != null ? '$' + t.entry_price.toFixed(2) : '--'}</td>
                            <td className="summary-td">{t.exit_price != null ? '$' + t.exit_price.toFixed(2) : '--'}</td>
                            <td className="summary-td" style={{
                              color: t.skipped ? '#93a1a1' : (t.change != null && t.change > 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)'),
                              fontWeight: 600,
                            }}>
                              {t.skipped ? 'SKIP' : pctFmt(t.change)}
                            </td>
                            <td className="summary-td">{t.skipped ? '--' : t.bars_held}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    {filteredTrades.length > 500 && (
                      <div className="summary-empty">Showing first 500 of {filteredTrades.length} trades</div>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    )
  }

  /* ━━━━━━━━━━━ Configuration Mode ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

  const renderLegCard = (leg: LegConfig, i: number) => (
    <div key={i} className="multi-leg-card" style={{ borderTop: `3px solid ${LEG_COLORS[i % LEG_COLORS.length]}` }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <label className="backtest-label" style={{ color: LEG_COLORS[i % LEG_COLORS.length] }}>
          Leg {i + 1}
        </label>
        {legs.length > 2 && (
          <button className="backtest-remove-btn" onClick={() => removeLeg(i)}>X</button>
        )}
      </div>

      <div className="bt-search-container">
        <div className="bt-search-input-wrapper" onClick={() => { setLegSearchOpen(i); setLegSearchQuery(''); setTimeout(() => legSearchRef.current?.focus(), 0) }}>
          {legSearchOpen === i ? (
            <input
              ref={legSearchRef}
              className="bt-search-input"
              value={legSearchQuery}
              onChange={e => { setLegSearchQuery(e.target.value); setLegSearchHighlight(0) }}
              onKeyDown={e => handleLegSearchKeyDown(i, e)}
              onBlur={() => setTimeout(() => setLegSearchOpen(null), 200)}
              placeholder={leg.target ? leg.target.replace(/_/g, ' ') : 'Search basket or ticker...'}
              autoFocus
            />
          ) : (
            <div className="bt-search-display">
              <span className="bt-search-display-name">{leg.target ? leg.target.replace(/_/g, ' ') : 'Select target...'}</span>
              {leg.target && <span className="bt-search-display-tag">{leg.targetType === 'ticker' ? 'Ticker' : 'Basket'}</span>}
            </div>
          )}
        </div>
        {legSearchOpen === i && (
          <div className="bt-search-dropdown">
            <div className="search-filters">
              {(['all', 'Themes', 'Sectors', 'Industries', 'Tickers'] as const).map(f => (
                <button
                  key={f}
                  className={`search-filter-btn ${legSearchFilter === f ? 'active' : ''}`}
                  onMouseDown={e => { e.preventDefault(); setLegSearchFilter(f) }}
                >
                  {f === 'all' ? 'All' : f}
                </button>
              ))}
            </div>
            <div className="search-results">
              {legSearchResults.map((r, ri) => (
                <div
                  key={`${r.category}-${r.name}`}
                  className={`search-result-item ${ri === legSearchHighlight ? 'highlighted' : ''}`}
                  ref={ri === legSearchHighlight ? el => el?.scrollIntoView({ block: 'nearest' }) : undefined}
                  onMouseDown={() => handleLegSearchSelect(i, r)}
                  onMouseEnter={() => setLegSearchHighlight(ri)}
                >
                  <span className="search-result-name">{r.displayName}</span>
                  <span className="search-result-tag">{r.category}</span>
                </div>
              ))}
              {legSearchResults.length === 0 && (
                <div className="search-result-empty">No matches</div>
              )}
            </div>
          </div>
        )}
      </div>

      {leg.targetType !== 'ticker' && (
        <div className="backtest-pos-presets" style={{ marginTop: 4 }}>
          <button className={`backtest-pos-preset wide ${leg.targetType === 'basket' ? 'active' : ''}`}
            onClick={() => updateLeg(i, { targetType: 'basket' })}>Basket Signal</button>
          <button className={`backtest-pos-preset wide ${leg.targetType === 'basket_tickers' ? 'active' : ''}`}
            onClick={() => updateLeg(i, { targetType: 'basket_tickers' })}>Constituent Tickers</button>
        </div>
      )}

      <div className="backtest-section">
        <label className="backtest-label">Entry Signal</label>
        <select className="backtest-select" value={leg.entrySignal}
          onChange={e => updateLeg(i, { entrySignal: e.target.value })}>
          {ENTRY_SIGNALS.map(s => <option key={s} value={s}>{s.replace(/_/g, ' ')}</option>)}
        </select>
        <span className="backtest-hint">Exit: {EXIT_MAP[leg.entrySignal]?.replace(/_/g, ' ')}</span>
      </div>

      <div className="backtest-section">
        <label className="backtest-label">Position Sizing</label>
        <div className="backtest-sizing-row">
          <div className="backtest-sizing-field">
            <span className="backtest-hint">Alloc %</span>
            <input type="number" className="backtest-input" min={1} max={100}
              value={leg.allocationPct}
              onChange={e => updateLeg(i, { allocationPct: Number(e.target.value) })} />
          </div>
          <div className="backtest-sizing-field">
            <span className="backtest-hint">Size %</span>
            <input type="number" className="backtest-input" min={1} max={100}
              value={leg.positionSize}
              onChange={e => updateLeg(i, { positionSize: Number(e.target.value) })} />
          </div>
        </div>
        <div className="backtest-pos-presets">
          <span className="backtest-preset-label">Size:</span>
          {POS_PRESETS.map(p => (
            <button key={p}
              className={`backtest-pos-preset ${leg.positionSize === p ? 'active' : ''}`}
              onClick={() => updateLeg(i, { positionSize: p })}>{p}%</button>
          ))}
        </div>
      </div>

      <div className="backtest-section">
        <label className="backtest-label">Regime Filters</label>
        {leg.filters.map((f, fi) => {
          const isBool = BOOL_METRICS.includes(f.metric)
          return (
            <div key={fi} className="backtest-filter-row">
              <select className="backtest-select" value={f.metric}
                onChange={e => {
                  const m = e.target.value
                  updateFilter(i, fi, 'metric', m)
                  if (BOOL_METRICS.includes(m)) updateFilter(i, fi, 'condition', 'equals_true')
                }}>
                <optgroup label="Percentage">
                  {PCT_METRICS.map(m => <option key={m} value={m}>{m}</option>)}
                </optgroup>
                <optgroup label="Boolean">
                  {BOOL_METRICS.map(m => <option key={m} value={m}>{m}</option>)}
                </optgroup>
              </select>
              <select className="backtest-select" value={f.condition}
                onChange={e => updateFilter(i, fi, 'condition', e.target.value)}>
                {isBool ? (
                  <>
                    <option value="equals_true">= True</option>
                    <option value="equals_false">= False</option>
                  </>
                ) : (
                  <>
                    <option value="above">Above</option>
                    <option value="below">Below</option>
                    <option value="increasing">Increasing</option>
                    <option value="decreasing">Decreasing</option>
                  </>
                )}
              </select>
              {!isBool && f.condition !== 'increasing' && f.condition !== 'decreasing' && (
                <input type="number" className="backtest-input" value={f.value}
                  onChange={e => updateFilter(i, fi, 'value', e.target.value)} />
              )}
              <button className="backtest-remove-btn" onClick={() => removeFilter(i, fi)}>X</button>
            </div>
          )
        })}
        <button className="control-btn" onClick={() => addFilter(i)}>+ Add Filter</button>
      </div>
    </div>
  )

  return (
    <div className="backtest-panel">
      <div className="backtest-config">
        <div className="backtest-section">
          <div className="backtest-pos-presets">
            <button className="backtest-pos-preset wide" onClick={onClose}>Single Leg</button>
            <button className="backtest-pos-preset wide active">Multi-Leg</button>
          </div>
        </div>

        <div className="multi-leg-grid" style={{ gridTemplateColumns: '1fr 1fr', flex: 'none' }}>
          <div className="multi-leg-settings-card">
            <div className="backtest-section" style={{
              color: allocValid ? '#2E7D32' : '#C2185B',
              background: allocValid ? 'rgba(46,125,50,0.06)' : 'rgba(194,24,91,0.06)',
              padding: '6px 8px',
            }}>
              <span style={{ fontWeight: 600, fontSize: 12 }}>
                Total Allocation: {totalAlloc}% {allocValid ? '' : '(must equal 100%)'}
              </span>
            </div>

            <div className="backtest-section">
              <label className="backtest-label">Portfolio Settings</label>
              <div className="backtest-sizing-row">
                <div className="backtest-sizing-field">
                  <span className="backtest-hint">Max Lev %</span>
                  <input type="number" className="backtest-input" value={maxLeverage}
                    onChange={e => setMaxLeverage(Number(e.target.value))} />
                </div>
              </div>
              <div className="backtest-pos-presets">
                <span className="backtest-preset-label">Lev:</span>
                {LEV_PRESETS.map(p => (
                  <button key={p}
                    className={`backtest-pos-preset ${maxLeverage === p ? 'active' : ''}`}
                    onClick={() => setMaxLeverage(p)}>{p}%</button>
                ))}
              </div>
            </div>

            <div className="backtest-section">
              <label className="backtest-label">Date Range</label>
              <div className="backtest-filter-row">
                <input type="date" className="backtest-input" value={startDate}
                  min={dataRange?.min} max={endDate || dataRange?.max}
                  onChange={e => setStartDate(e.target.value)} style={{ width: 130 }} />
                <span className="backtest-hint">to</span>
                <input type="date" className="backtest-input" value={endDate}
                  min={startDate || dataRange?.min} max={dataRange?.max}
                  onChange={e => setEndDate(e.target.value)} style={{ width: 130 }} />
              </div>
              {dataRange && (
                <span className="backtest-hint">Data: {dataRange.min} to {dataRange.max}</span>
              )}
            </div>

            <button className="control-btn primary" onClick={runBacktest}
              disabled={loading || !allocValid || !allTargetsSet}>
              {loading ? 'Running...' : 'Run Multi-Basket Backtest'}
            </button>
            {error && <div className="backtest-error">{error}</div>}
          </div>
        </div>

        <div className="multi-leg-grid">
          {legs.map((leg, i) => renderLegCard(leg, i))}
          {legs.length < 6 && (
            <div className="multi-leg-card multi-leg-add" onClick={addLeg}>
              <span>+ Add Leg</span>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
