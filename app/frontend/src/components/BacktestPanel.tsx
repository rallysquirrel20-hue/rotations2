import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import { RangeScrollbar } from './RangeScrollbar'

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

interface PortfolioStats {
  strategy_return: number
  cagr: number
  volatility: number
  max_dd: number
  sharpe: number
  sortino: number
  contribution?: number
  allocation?: number
}

interface TradeStats {
  trades_met_criteria: number
  trades_taken: number
  trades_skipped: number
  win_rate: number
  avg_winner: number
  avg_loser: number
  ev: number
  profit_factor: number
  avg_time_winner: number
  avg_time_loser: number
}

interface Stats {
  portfolio: PortfolioStats
  trade: TradeStats
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
  entry_weight: number | null
  exit_weight: number | null
  contribution: number | null
}

interface DailyPosition {
  trade_idx: number
  ticker?: string
  entry_date: string
  leg_target: string
  alloc: number
  entry_weight: number
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
  leg_correlations?: Record<string, Record<string, number>>
}

interface BacktestPanelProps {
  apiBase: string
  target?: string
  targetType?: 'basket' | 'ticker'
  exportTrigger?: number
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

const BENCHMARK_COLORS: Record<string, string> = {
  Breakout: '#1565C0',
  Up_Rot: '#42A5F5',
  BTFD: '#90CAF9',
  Breakdown: '#C2185B',
  Down_Rot: '#F06292',
  STFR: '#F8BBD0',
}

const LIGHT_PINK = 'rgb(255, 183, 226)'
const LIGHT_BLUE = 'rgb(179, 222, 255)'

type HistMetric = 'change' | 'mfe' | 'mae'
const HIST_COLORS: Record<HistMetric, { fill: string; stroke: string; label: string }> = {
  mae:    { fill: LIGHT_PINK,           stroke: LIGHT_PINK,            label: 'MAE' },
  change: { fill: 'rgb(200, 210, 220)', stroke: 'rgb(200, 210, 220)', label: 'Change' },
  mfe:    { fill: LIGHT_BLUE,           stroke: LIGHT_BLUE,            label: 'MFE' },
}

const pctFmt = (v: number | null) => v == null ? '--' : (v * 100).toFixed(2) + '%'
const dollarFmt = (v: number) => {
  const abs = Math.abs(v)
  const sign = v < 0 ? '-' : ''
  if (abs >= 1e6) return sign + '$' + (abs / 1e6).toFixed(1) + 'M'
  if (abs >= 1e3) return sign + '$' + (abs / 1e3).toFixed(1) + 'K'
  return sign + '$' + abs.toFixed(0)
}

function defaultLeg(): LegConfig {
  return { target: '', targetType: 'basket_tickers', entrySignal: 'Breakout', allocationPct: 100, positionSize: 25, filters: [] }
}

type ResultTab = 'equity' | 'stats' | 'distribution' | 'path' | 'trades'

/* ── Component ─────────────────────────────────────────────────────── */

export function BacktestPanel({ apiBase, target, targetType, exportTrigger }: BacktestPanelProps) {
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

  // ── Legs config ──
  // Initialize with target from props if provided
  const [legs, setLegs] = useState<LegConfig[]>(() => {
    const initial = defaultLeg()
    if (target) {
      initial.target = target
      initial.targetType = targetType === 'ticker' ? 'ticker' : 'basket_tickers'
    }
    return [initial]
  })

  // Sync first leg target when props change
  useEffect(() => {
    if (target) {
      setLegs(prev => {
        if (prev.length === 1 && prev[0].target === '') {
          return [{ ...prev[0], target, targetType: targetType === 'ticker' ? 'ticker' : 'basket_tickers' as const }]
        }
        return prev
      })
    }
  }, [target, targetType])

  const isSingleLeg = legs.length === 1
  const isMultiLeg = legs.length > 1

  const updateLeg = (i: number, patch: Partial<LegConfig>) =>
    setLegs(prev => prev.map((l, idx) => idx === i ? { ...l, ...patch } : l))

  const handleLegSearchSelect = useCallback((legIdx: number, r: { name: string; category: string }) => {
    setLegSearchOpen(null)
    setLegSearchQuery('')
    setLegSearchHighlight(0)
    const isTicker = r.category === 'Tickers'
    setLegs(prev => prev.map((l, idx) => idx === legIdx ? { ...l, target: r.name, targetType: isTicker ? 'ticker' : 'basket_tickers' as const } : l))
  }, [])

  const handleLegSearchKeyDown = useCallback((legIdx: number, e: React.KeyboardEvent) => {
    if (e.key === 'Escape') { setLegSearchOpen(null); setLegSearchQuery(''); legSearchRef.current?.blur() }
    else if (e.key === 'ArrowDown') { e.preventDefault(); setLegSearchHighlight(prev => Math.min(prev + 1, legSearchResults.length - 1)) }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setLegSearchHighlight(prev => Math.max(prev - 1, 0)) }
    else if (e.key === 'Enter' && legSearchResults.length > 0) { handleLegSearchSelect(legIdx, legSearchResults[legSearchHighlight]) }
  }, [legSearchResults, legSearchHighlight, handleLegSearchSelect])

  const addLeg = () => {
    if (legs.length >= 6) return
    if (isSingleLeg) {
      // Going from 1 to 2: set allocations to 50/50
      setLegs(prev => [{ ...prev[0], allocationPct: 50 }, { ...defaultLeg(), allocationPct: 50 }])
    } else {
      const pct = Math.floor(100 / (legs.length + 1))
      setLegs(prev => [...prev.map(l => ({ ...l, allocationPct: pct })), { ...defaultLeg(), allocationPct: 100 - pct * legs.length }])
    }
  }

  const removeLeg = (i: number) => {
    if (legs.length <= 1) return
    setLegs(prev => {
      const next = prev.filter((_, idx) => idx !== i)
      if (next.length === 1) {
        // Going back to single leg: allocation is implicitly 100%
        return [{ ...next[0], allocationPct: 100 }]
      }
      const pct = Math.floor(100 / next.length)
      return next.map((l, idx) => ({ ...l, allocationPct: idx < next.length - 1 ? pct : 100 - pct * (next.length - 1) }))
    })
  }

  // ── Fetch date range from first leg with a target ──
  const [dataRange, setDataRange] = useState<{ min: string; max: string } | null>(null)
  useEffect(() => {
    const firstWithTarget = legs.find(l => l.target)
    if (!firstWithTarget) return
    const { target: t, targetType: tt } = firstWithTarget
    fetch(`${apiBase}/date-range/${tt}/${encodeURIComponent(t)}`)
      .then(r => r.json())
      .then((d: { min: string; max: string }) => {
        setDataRange(d)
        setStartDate(prev => prev || d.min)
        setEndDate(prev => prev || d.max)
      })
      .catch(() => {})
  }, [legs.map(l => l.target + l.targetType).join(','), apiBase])

  // ── Shared settings ──
  const [maxLeverage, setMaxLeverage] = useState(250)
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')

  // ── Results ──
  const [result, setResult] = useState<MultiBacktestResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [showResults, setShowResults] = useState(false)
  const [resultTab, setResultTab] = useState<ResultTab>('equity')

  // ── Log scale toggles ──
  const [eqLogScale, setEqLogScale] = useState(false)
  const [pathLogScale, setPathLogScale] = useState(false)

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
  const [showBuyHold, setShowBuyHold] = useState(false)
  const [visibleLegs, setVisibleLegs] = useState<Set<number>>(new Set())
  const [showConstituents, setShowConstituents] = useState(false)

  // ── Benchmark state (auto-calculated for single-leg only) ──
  const [benchmarks, setBenchmarks] = useState<Record<string, number[]>>({})
  const [showBenchmark, setShowBenchmark] = useState<Record<string, boolean>>({})
  const benchmarkGenRef = useRef(0)

  // ── Distribution tab state ──
  const histCanvasRef = useRef<HTMLCanvasElement>(null)
  const histContainerRef = useRef<HTMLDivElement>(null)
  const [histDims, setHistDims] = useState({ w: 800, h: 400 })
  const [histVisible, setHistVisible] = useState<Set<HistMetric>>(new Set(['change', 'mfe', 'mae']))
  const [hoveredCurve, setHoveredCurve] = useState<HistMetric | null>(null)
  const [distLegFilter, setDistLegFilter] = useState(-1) // -1 = all
  const histHitRef = useRef<{
    curves: { metric: HistMetric; pxPoints: { x: number; y: number }[] }[]
    legendRows: { metric: HistMetric; x: number; y: number; w: number; h: number }[]
    baseline: number
  }>({ curves: [], legendRows: [], baseline: 0 })

  // Zoom/pan refs
  const eqViewRef = useRef({ start: 0, end: 0 })
  const [eqViewVersion, setEqViewVersion] = useState(0)
  const eqDragRef = useRef({ dragging: false, lastX: 0 })
  const eqDidDragRef = useRef(false)
  const eqScaleRef = useRef({ padLeft: 20, plotW: 0, n: 0, startIdx: 0 })

  // Constituents hover/pin
  const [eqHoverIdx, setEqHoverIdx] = useState<number | null>(null)
  const [eqPinnedIdx, setEqPinnedIdx] = useState<number | null>(null)
  const [constSortCol, setConstSortCol] = useState<'ticker' | 'leg_target' | 'entry_date' | 'entry_weight' | 'daily_return' | 'weight' | 'contribution'>('weight')
  const [constSortAsc, setConstSortAsc] = useState(false)
  const [constMaxH, setConstMaxH] = useState<number | undefined>(undefined)

  // ── Allocation validation ──
  const totalAlloc = legs.reduce((s, l) => s + l.allocationPct, 0)
  const allocValid = isSingleLeg || Math.abs(totalAlloc - 100) < 1
  const allTargetsSet = legs.every(l => l.target !== '')

  // ── Run backtest ──
  const runBacktest = useCallback(async () => {
    if (!allocValid || !allTargetsSet) return
    setLoading(true)
    setError('')
    setBenchmarks({})
    setShowBenchmark({})
    const gen = ++benchmarkGenRef.current
    try {
      const resp = await fetch(`${apiBase}/backtest/multi`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          legs: legs.map(l => ({
            target: l.target,
            target_type: l.targetType,
            entry_signal: l.entrySignal,
            allocation_pct: isSingleLeg ? 1.0 : l.allocationPct / 100,
            position_size: l.positionSize / 100,
            filters: l.filters,
          })),
          start_date: startDate || null,
          end_date: endDate || null,
          max_leverage: maxLeverage / 100,
        }),
      })
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({ detail: resp.statusText }))
        throw new Error(err.detail || resp.statusText)
      }
      const data: MultiBacktestResult = await resp.json()

      // Auto-fire benchmarks for single-leg backtests
      if (isSingleLeg && data.legs.length > 0) {
        const leg0 = legs[0]
        const benchSignals = ENTRY_SIGNALS.filter(s => s !== 'Buy_Hold')
        const benchPromises = benchSignals.map(sig =>
          fetch(`${apiBase}/backtest/multi`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              legs: [{
                target: leg0.target,
                target_type: leg0.targetType,
                entry_signal: sig,
                allocation_pct: 1.0,
                position_size: leg0.positionSize / 100,
                filters: [],
              }],
              start_date: startDate || null,
              end_date: endDate || null,
              max_leverage: maxLeverage / 100,
            }),
          })
            .then(r => r.ok ? r.json() : null)
            .then(d => d ? { sig, curve: d.combined.equity_curve.combined as number[] } : null)
            .catch(() => null)
        )
        Promise.all(benchPromises).then(results => {
          if (benchmarkGenRef.current !== gen) return
          const newBench: Record<string, number[]> = {}
          for (const b of results) {
            if (b) newBench[b.sig] = b.curve
          }
          setBenchmarks(newBench)
        })
      }

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
  }, [apiBase, legs, startDate, endDate, maxLeverage, allocValid, allTargetsSet, isSingleLeg])

  // ── Export current tab when exportTrigger changes ──
  const prevExportTrigger = useRef(exportTrigger || 0)
  useEffect(() => {
    if (!exportTrigger || exportTrigger === prevExportTrigger.current) {
      prevExportTrigger.current = exportTrigger || 0
      return
    }
    prevExportTrigger.current = exportTrigger
    const tabLabel = resultTab
    const legLabel = legs.map(l => l.target).join('+')
    const filename = `${legLabel}_${tabLabel}.png`
    const downloadCanvas = (canvas: HTMLCanvasElement) => {
      canvas.toBlob((blob) => {
        if (!blob) return
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url; a.download = filename; a.click()
        URL.revokeObjectURL(url)
      }, 'image/png')
    }
    if (resultTab === 'equity' && canvasRef.current) downloadCanvas(canvasRef.current)
    else if (resultTab === 'distribution' && histCanvasRef.current) downloadCanvas(histCanvasRef.current)
    else if (resultTab === 'path' && pathCanvasRef.current) downloadCanvas(pathCanvasRef.current)
  }, [exportTrigger])

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

  // ── ResizeObserver for histogram canvas ──
  useEffect(() => {
    const el = histContainerRef.current
    if (!el) return
    const obs = new ResizeObserver(entries => {
      const { width, height } = entries[0].contentRect
      if (width > 0 && height > 0) setHistDims({ w: width, h: height })
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
    const rebase = (arr: number[]) => {
      const slice = arr.slice(cs, ce + 1)
      const base = slice[0] || 1
      return slice.map(v => v / base - 1)
    }
    const windowedBench: Record<string, number[]> = {}
    for (const [sig, curve] of Object.entries(benchmarks)) {
      windowedBench[sig] = rebase(curve)
    }
    return {
      dates: eq.dates.slice(cs, ce + 1),
      combined: rebase(eq.combined),
      per_leg: eq.per_leg.map(l => rebase(l)),
      buy_hold: rebase(eq.buy_hold),
      benchmarks: windowedBench,
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [result, eqViewVersion, benchmarks])

  // ── Draw equity curve ──
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas || !eqWindowed || resultTab !== 'equity') return
    const { dates, combined, per_leg, buy_hold, benchmarks: wBench } = eqWindowed
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
    // Include benchmarks in y-range
    for (const sig of ENTRY_SIGNALS) {
      if (showBenchmark[sig] && wBench[sig]) {
        for (const v of wBench[sig]) { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) }
      }
    }
    const yPad = (yMax - yMin) * 0.1 || 0.05
    yMin -= yPad; yMax += yPad

    eqScaleRef.current = { padLeft: pad.left, plotW, n, startIdx: eqViewRef.current.start }
    const xScale = (i: number) => pad.left + (n > 1 ? (i / (n - 1)) * plotW : plotW / 2)
    const logT = (v: number) => Math.log(Math.max(1 + v, 1e-8))
    const logYMin = eqLogScale ? logT(yMin) : 0
    const logYMax = eqLogScale ? logT(yMax) : 0
    const yScale = eqLogScale
      ? (v: number) => pad.top + plotH - ((logT(v) - logYMin) / (logYMax - logYMin)) * plotH
      : (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH

    ctx.clearRect(0, 0, dims.w, dims.h)
    ctx.fillStyle = '#fdf6e3'
    ctx.fillRect(0, 0, dims.w, dims.h)

    // Grid
    ctx.strokeStyle = '#eee8d5'; ctx.lineWidth = 1
    const nTicks = 6
    for (let i = 0; i <= nTicks; i++) {
      const v = eqLogScale
        ? Math.exp(logYMin + (logYMax - logYMin) * (i / nTicks)) - 1
        : yMin + (yMax - yMin) * (i / nTicks)
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
      const len = data.length
      const xS = (i: number) => pad.left + (len > 1 ? (i / (len - 1)) * plotW : plotW / 2)
      ctx.strokeStyle = color; ctx.lineWidth = lw
      ctx.beginPath()
      for (let i = 0; i < len; i++) {
        const x = xS(i), y = yScale(data[i])
        i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y)
      }
      ctx.stroke()
    }

    // Draw order: buy-hold -> benchmarks -> per-leg -> combined
    if (showBuyHold && buy_hold.length > 0) drawLine(buy_hold, '#93a1a1', 1.5)

    for (const sig of ENTRY_SIGNALS) {
      if (showBenchmark[sig] && wBench[sig] && wBench[sig].length > 0) {
        drawLine(wBench[sig], BENCHMARK_COLORS[sig], 1.5)
      }
    }

    if (isMultiLeg) {
      per_leg.forEach((vals, i) => { if (visibleLegs.has(i)) drawLine(vals, LEG_COLORS[i % LEG_COLORS.length], 1.5) })
    }
    drawLine(combined, '#8BC34A', 2.5)

    // Legend
    const legendItems: { label: string; value: string; color: string }[] = []
    legendItems.push({ label: isSingleLeg ? 'Equity' : 'Combined', value: pctFmt(combined[n - 1]), color: '#8BC34A' })
    if (result && isMultiLeg) {
      result.legs.forEach((leg, i) => {
        if (visibleLegs.has(i))
          legendItems.push({ label: `${leg.target} ${leg.entry_signal}`, value: pctFmt(per_leg[i][n - 1]), color: LEG_COLORS[i % LEG_COLORS.length] })
      })
    }
    for (const sig of ENTRY_SIGNALS) {
      if (showBenchmark[sig] && wBench[sig] && wBench[sig].length > 0) {
        legendItems.push({ label: sig.replace('_', ' '), value: pctFmt(wBench[sig][wBench[sig].length - 1]), color: BENCHMARK_COLORS[sig] })
      }
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
  }, [eqWindowed, resultTab, dims, showBuyHold, visibleLegs, result, eqLogScale, showBenchmark, isMultiLeg, isSingleLeg])

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
    const logT = (v: number) => Math.log(Math.max(1 + v, 1e-8))
    const logYMin = pathLogScale ? logT(yMin) : 0
    const logYMax = pathLogScale ? logT(yMax) : 0
    const yScale = pathLogScale
      ? (v: number) => pad.top + plotH - ((logT(v) - logYMin) / (logYMax - logYMin)) * plotH
      : (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH

    // Grid
    ctx.strokeStyle = '#e9ecef'; ctx.lineWidth = 1
    const nTicks = 6
    for (let i = 0; i <= nTicks; i++) {
      const v = pathLogScale
        ? Math.exp(logYMin + (logYMax - logYMin) * (i / nTicks)) - 1
        : yMin + (yMax - yMin) * (i / nTicks)
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
  }, [allPaths, resultTab, pathDims, hoveredPath, result, pathLogScale])

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

  // Track chart container height for constituents overlay max-height
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const ro = new ResizeObserver(() => setConstMaxH(el.clientHeight - 8))
    ro.observe(el)
    setConstMaxH(el.clientHeight - 8)
    return () => ro.disconnect()
  }, [])

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

  // ── Distribution: trades filtered by leg ──
  const distTrades = useMemo(() => {
    if (!result) return []
    if (distLegFilter < 0) {
      // All legs
      const trades: Trade[] = []
      result.legs.forEach(leg => trades.push(...leg.trades))
      return trades
    }
    return result.legs[distLegFilter]?.trades ?? []
  }, [result, distLegFilter])

  // ── Draw distribution KDE curves ──
  useEffect(() => {
    const canvas = histCanvasRef.current
    if (!canvas || !result || resultTab !== 'distribution') return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    const dpr = window.devicePixelRatio || 1
    canvas.width = histDims.w * dpr
    canvas.height = histDims.h * dpr
    ctx.scale(dpr, dpr)

    ctx.clearRect(0, 0, histDims.w, histDims.h)
    ctx.fillStyle = '#fdf6e3'
    ctx.fillRect(0, 0, histDims.w, histDims.h)

    const trades = distTrades
    const metrics = (['mae', 'change', 'mfe'] as HistMetric[]).filter(m => histVisible.has(m))
    if (metrics.length === 0) return

    const allValsByMetric: Record<string, number[]> = {}
    let globalMin = Infinity, globalMax = -Infinity
    for (const m of metrics) {
      const vals = trades.map(t => t[m]).filter((v): v is number => v !== null && v !== undefined)
      allValsByMetric[m] = vals
      for (const v of vals) { globalMin = Math.min(globalMin, v); globalMax = Math.max(globalMax, v) }
    }

    if (globalMin === Infinity) {
      ctx.fillStyle = '#6c757d'; ctx.font = '14px monospace'; ctx.textAlign = 'center'
      ctx.fillText('No data', histDims.w / 2, histDims.h / 2)
      return
    }

    const range = globalMax - globalMin || 0.01
    const xMin = globalMin - range * 0.08
    const xMax = globalMax + range * 0.08

    const pad = { top: 30, right: 30, bottom: 50, left: 20 }
    const plotW = histDims.w - pad.left - pad.right
    const plotH = histDims.h - pad.top - pad.bottom
    const nPoints = Math.min(200, plotW)

    // KDE: Gaussian kernel
    function kde(vals: number[], clampMax?: number, clampMin?: number): { xs: number[]; ys: number[] } {
      const n = vals.length
      const mean = vals.reduce((a, b) => a + b, 0) / n
      const std = Math.sqrt(vals.reduce((s, v) => s + (v - mean) ** 2, 0) / n) || range * 0.1
      const h = 0.5 * std * Math.pow(n, -0.2)
      const invH = 1 / h
      const coeff = 1 / (n * h * Math.sqrt(2 * Math.PI))

      const rawXs: number[] = []
      for (let i = 0; i < nPoints; i++) rawXs.push(xMin + (i / (nPoints - 1)) * (xMax - xMin))
      if (clampMax !== undefined && !rawXs.includes(clampMax)) rawXs.push(clampMax)
      if (clampMin !== undefined && !rawXs.includes(clampMin)) rawXs.push(clampMin)
      rawXs.sort((a, b) => a - b)

      const xs: number[] = []
      const ys: number[] = []
      for (const x of rawXs) {
        if (clampMax !== undefined && x > clampMax) continue
        if (clampMin !== undefined && x < clampMin) continue
        let density = 0
        for (const v of vals) {
          const z = (x - v) * invH
          density += Math.exp(-0.5 * z * z)
        }
        xs.push(x)
        ys.push(density * coeff)
      }
      return { xs, ys }
    }

    const median = (arr: number[]) => {
      if (arr.length === 0) return 0
      const s = [...arr].sort((a, b) => a - b)
      const mid = Math.floor(s.length / 2)
      return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2
    }

    const winIdx = new Set<number>()
    const loseIdx = new Set<number>()
    trades.forEach((t, i) => {
      if (t.change !== null && t.change !== undefined && t.change > 0) winIdx.add(i)
      else loseIdx.add(i)
    })

    const curves: { metric: HistMetric; xs: number[]; ys: number[]; medianWin: number; medianLose: number }[] = []
    let maxDensity = 0
    for (const m of metrics) {
      const vals = allValsByMetric[m]
      if (vals.length < 2) continue
      const { xs, ys } = kde(vals, m === 'mae' ? 0 : undefined, m === 'mfe' ? 0 : undefined)
      for (const y of ys) maxDensity = Math.max(maxDensity, y)
      const winVals: number[] = [], loseVals: number[] = []
      trades.forEach((t, i) => {
        const v = t[m]
        if (v === null || v === undefined) return
        if (winIdx.has(i)) winVals.push(v as number)
        else loseVals.push(v as number)
      })
      curves.push({ metric: m, xs, ys, medianWin: median(winVals), medianLose: median(loseVals) })
    }

    if (maxDensity === 0) return

    const xScale = (v: number) => pad.left + ((v - xMin) / (xMax - xMin)) * plotW
    const yScale = (d: number) => pad.top + plotH - (d / maxDensity) * plotH

    // Store pixel points for hit-testing
    const hitCurves: typeof histHitRef.current.curves = []
    for (const { metric, xs, ys } of curves) {
      hitCurves.push({ metric, pxPoints: xs.map((x, i) => ({ x: xScale(x), y: yScale(ys[i]) })) })
    }

    // Grid
    ctx.strokeStyle = '#e9ecef'; ctx.lineWidth = 1
    for (let i = 0; i <= 4; i++) {
      const y = pad.top + (plotH * i) / 4
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + plotW, y); ctx.stroke()
    }

    // Draw order: non-hovered first, hovered last
    const drawOrder = hoveredCurve
      ? [...curves.filter(c => c.metric !== hoveredCurve), ...curves.filter(c => c.metric === hoveredCurve)]
      : curves

    for (const { metric, xs, ys } of drawOrder) {
      const colors = HIST_COLORS[metric]
      const dimmed = hoveredCurve !== null && metric !== hoveredCurve
      const alpha = dimmed ? 0.2 : 1.0

      ctx.globalAlpha = alpha
      if (xs.length > 1) {
        const baseline = pad.top + plotH
        ctx.beginPath()
        ctx.moveTo(xScale(xs[0]), baseline)
        for (let i = 0; i < xs.length; i++) ctx.lineTo(xScale(xs[i]), yScale(ys[i]))
        ctx.lineTo(xScale(xs[xs.length - 1]), baseline)
        ctx.closePath()
        ctx.fillStyle = colors.fill
        ctx.fill()

        ctx.beginPath()
        ctx.moveTo(xScale(xs[0]), baseline)
        for (let i = 0; i < xs.length; i++) ctx.lineTo(xScale(xs[i]), yScale(ys[i]))
        ctx.lineTo(xScale(xs[xs.length - 1]), baseline)
        ctx.strokeStyle = colors.stroke
        ctx.lineWidth = dimmed ? 1 : 2
        ctx.stroke()
      }
      ctx.globalAlpha = 1.0
    }

    // X axis labels
    const nLabels = 8
    ctx.fillStyle = '#6c757d'; ctx.font = '10px monospace'; ctx.textAlign = 'center'
    for (let i = 0; i <= nLabels; i++) {
      const v = xMin + (i / nLabels) * (xMax - xMin)
      ctx.fillText((v * 100).toFixed(1) + '%', xScale(v), histDims.h - pad.bottom + 15)
    }

    // Legend
    const legendW = 270
    const rowH = 16
    const legendH = 12 + curves.length * rowH
    const lx = histDims.w - pad.right - legendW - 4
    const ly = pad.top + 4
    ctx.fillStyle = '#fdf6e3'
    ctx.fillRect(lx - 4, ly - 10, legendW, legendH)
    ctx.strokeStyle = '#eee8d5'; ctx.lineWidth = 1
    ctx.strokeRect(lx - 4, ly - 10, legendW, legendH)
    const legendRows: typeof histHitRef.current.legendRows = []
    for (let i = 0; i < curves.length; i++) {
      const { metric, medianWin, medianLose } = curves[i]
      const colors = HIST_COLORS[metric]
      const rowY = ly + i * rowH
      const isHovered = hoveredCurve === metric
      ctx.fillStyle = colors.fill
      ctx.fillRect(lx, rowY - 4, 14, 10)
      ctx.strokeStyle = colors.stroke; ctx.lineWidth = 1
      ctx.strokeRect(lx, rowY - 4, 14, 10)
      const font = (isHovered ? 'bold ' : '') + '10px monospace'
      ctx.font = font; ctx.fillStyle = isHovered ? colors.stroke : '#586e75'
      const colName = lx + 18, colW = lx + 62, colWVal = lx + 148, colL = lx + 156, colLVal = lx + 260
      ctx.textAlign = 'left'
      ctx.fillText(colors.label, colName, rowY + 4)
      ctx.fillText('W', colW, rowY + 4)
      ctx.textAlign = 'right'
      ctx.fillText((medianWin * 100).toFixed(2) + '%', colWVal, rowY + 4)
      ctx.textAlign = 'left'
      ctx.fillText('L', colL, rowY + 4)
      ctx.textAlign = 'right'
      ctx.fillText((medianLose * 100).toFixed(2) + '%', colLVal, rowY + 4)
      legendRows.push({ metric, x: lx - 4, y: rowY - 10, w: legendW, h: rowH })
    }

    histHitRef.current = { curves: hitCurves, legendRows, baseline: pad.top + plotH }
  }, [distTrades, resultTab, histVisible, histDims, hoveredCurve, result])

  // Mouse handler for histogram legend hover
  const handleHistMouseMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    const canvas = histCanvasRef.current
    if (!canvas) return
    const rect = canvas.getBoundingClientRect()
    const mx = e.clientX - rect.left
    const my = e.clientY - rect.top
    for (const row of histHitRef.current.legendRows) {
      if (mx >= row.x && mx <= row.x + row.w && my >= row.y && my <= row.y + row.h) {
        if (hoveredCurve !== row.metric) setHoveredCurve(row.metric)
        return
      }
    }
    if (hoveredCurve !== null) setHoveredCurve(null)
  }
  const handleHistMouseLeave = () => { setHoveredCurve(null) }

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
  const updateFilterField = (legIdx: number, fIdx: number, field: string, val: any) => {
    const updated = legs[legIdx].filters.map((f, i) => i === fIdx ? { ...f, [field]: val } : f)
    updateLeg(legIdx, { filters: updated })
  }

  /* ━━━━━━━━━━━━━━━━━━━━━━ RENDER ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

  if (showResults && result) {
    const resultIsMultiLeg = result.legs.length > 1

    return (
      <div className="backtest-panel">
        <div className="backtest-results-header">
          <button className="backtest-nav-btn" onClick={() => setShowResults(false)}>Configure</button>
          <span style={{ fontWeight: 600, fontSize: 13 }}>
            {resultIsMultiLeg
              ? result.legs.map(l => `${l.target}(${l.entry_signal})`).join(' + ')
              : `${result.legs[0].target.replace(/_/g, ' ')} -- ${result.legs[0].entry_signal.replace(/_/g, ' ')} Backtest`
            }
          </span>
          {result.date_range && (
            <span className="backtest-hint" style={{ marginLeft: 8 }}>
              {result.date_range.min} to {result.date_range.max}
            </span>
          )}
        </div>

        <div className="summary-tabs" style={{ padding: '0 12px' }}>
          {(['equity', 'stats', 'distribution', 'path', 'trades'] as ResultTab[]).map(tab => (
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
                        style={{ background: '#8BC34A', borderColor: '#8BC34A', color: '#fff' }}>
                        {isSingleLeg ? 'Equity' : 'Combined'}
                      </button>
                      {resultIsMultiLeg && result.legs.map((leg, i) => (
                        <button key={i} className={`bt-strat-btn ${visibleLegs.has(i) ? 'active' : ''}`}
                          style={visibleLegs.has(i) ? { background: LEG_COLORS[i % LEG_COLORS.length], borderColor: LEG_COLORS[i % LEG_COLORS.length], color: '#fff' } : undefined}
                          onClick={() => setVisibleLegs(prev => { const next = new Set(prev); if (next.has(i)) next.delete(i); else next.add(i); return next })}>{leg.target.replace(/_/g, ' ')}</button>
                      ))}
                      {isSingleLeg && Object.keys(benchmarks).length > 0 && (
                        <>
                          {(['Breakout', 'Up_Rot', 'BTFD', 'Breakdown', 'Down_Rot', 'STFR'] as const).map(sig => (
                            <button key={sig} className={`bt-strat-btn ${showBenchmark[sig] ? 'active' : ''}`}
                              style={showBenchmark[sig] ? { background: BENCHMARK_COLORS[sig], borderColor: BENCHMARK_COLORS[sig], color: '#fff' } : undefined}
                              onClick={() => setShowBenchmark(prev => ({ ...prev, [sig]: !prev[sig] }))}>{sig.replace('_', ' ')}</button>
                          ))}
                        </>
                      )}
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
                    <button className={`log-toggle-btn ${eqLogScale ? 'active' : ''}`} onClick={() => setEqLogScale(v => !v)}>L</button>
                    {showConstituents && (() => {
                      const activeIdx = eqPinnedIdx ?? eqHoverIdx
                      if (activeIdx == null) return null
                      const { padLeft, plotW, n, startIdx } = eqScaleRef.current
                      const localIdx = activeIdx - startIdx
                      const crosshairX = n > 1 ? padLeft + (localIdx / (n - 1)) * plotW : padLeft
                      const showCrosshair = localIdx >= 0 && localIdx < n
                      const snapshot = result?.daily_positions?.[activeIdx]
                      const date = result?.combined.equity_curve.dates[activeIdx]
                      const toggleSort = (col: typeof constSortCol) => {
                        if (constSortCol === col) setConstSortAsc(v => !v)
                        else { setConstSortCol(col); setConstSortAsc(col === 'ticker' || col === 'leg_target' || col === 'entry_date') }
                      }
                      const sortedPositions = snapshot ? [...snapshot.positions].sort((a, b) => {
                        let va: number | string, vb: number | string
                        if (constSortCol === 'ticker') { va = (a.ticker || a.leg_target || ''); vb = (b.ticker || b.leg_target || '') }
                        else if (constSortCol === 'leg_target') { va = a.leg_target || ''; vb = b.leg_target || '' }
                        else if (constSortCol === 'entry_date') { va = a.entry_date || ''; vb = b.entry_date || '' }
                        else { va = a[constSortCol] ?? 0; vb = b[constSortCol] ?? 0 }
                        const cmp = typeof va === 'string' ? va.localeCompare(vb as string) : (va as number) - (vb as number)
                        return constSortAsc ? cmp : -cmp
                      }) : []
                      const arrow = (col: typeof constSortCol) => constSortCol === col ? (constSortAsc ? ' \u25b2' : ' \u25bc') : ''
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
                            <div style={{ position: 'absolute', top: 4, left: 8, bottom: 54, pointerEvents: 'none', zIndex: 200, display: 'flex', flexDirection: 'column' }}>
                            <div className="candle-detail-overlay" style={{ ...(eqPinnedIdx != null ? { pointerEvents: 'auto' as const, borderColor: 'rgb(50,50,255)' } : {}), minWidth: resultIsMultiLeg ? 540 : 480, position: 'relative', top: 'auto', left: 'auto', maxHeight: '100%' }}>
                              <div className="candle-detail-title">
                                {date} &mdash; {dollarFmt(snapshot.equity)}
                                {eqPinnedIdx != null
                                  ? <span style={{ marginLeft: 8, fontSize: 9, color: 'rgb(50,50,255)' }}>PINNED (esc)</span>
                                  : <span style={{ marginLeft: 8, fontSize: 9, color: '#93a1a1' }}>click to pin</span>}
                              </div>
                              <div className="candle-detail-row const-header" style={{ fontWeight: 'bold', borderBottom: '1px solid #ccc', cursor: 'pointer', userSelect: 'none' }}>
                                <span className="const-ticker" onClick={() => toggleSort('ticker')}>Ticker{arrow('ticker')}</span>
                                {resultIsMultiLeg && <span className="const-leg" onClick={() => toggleSort('leg_target')}>Leg{arrow('leg_target')}</span>}
                                <span className="const-entry" onClick={() => toggleSort('entry_date')}>Entry{arrow('entry_date')}</span>
                                <span className="const-ew" onClick={() => toggleSort('entry_weight')}>Ent.Wt{arrow('entry_weight')}</span>
                                <span className="const-ret" onClick={() => toggleSort('daily_return')}>Return{arrow('daily_return')}</span>
                                <span className="const-cw" onClick={() => toggleSort('weight')}>Cur.Wt{arrow('weight')}</span>
                                <span className="const-contrib" onClick={() => toggleSort('contribution')}>Contrib{arrow('contribution')}</span>
                              </div>
                              {sortedPositions.map((p, i) => (
                                <div key={i} className="candle-detail-row">
                                  <span className="const-ticker">{p.ticker || p.leg_target}</span>
                                  {resultIsMultiLeg && <span className="const-leg" style={{ fontSize: 9 }}>{p.leg_target}</span>}
                                  <span className="const-entry">{p.entry_date ? p.entry_date.slice(5) + '-' + p.entry_date.slice(0, 4) : ''}</span>
                                  <span className="const-ew">{((p.entry_weight ?? 0) * 100).toFixed(1)}%</span>
                                  <span className="const-ret" style={{ color: p.daily_return >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)' }}>
                                    {(p.daily_return * 100).toFixed(2)}%</span>
                                  <span className="const-cw">{(p.weight * 100).toFixed(1)}%</span>
                                  <span className="const-contrib" style={{ color: p.contribution >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)' }}>
                                    {(p.contribution * 100).toFixed(3)}%</span>
                                </div>
                              ))}
                              <div className="candle-detail-row" style={{ borderTop: '1px solid #93a1a1', fontWeight: 'bold' }}>
                                <span className="const-ticker">Total</span>
                                {resultIsMultiLeg && <span className="const-leg"></span>}
                                <span className="const-entry"></span>
                                <span className="const-ew"></span>
                                <span className="const-ret"></span>
                                <span className="const-cw">{(snapshot.exposure_pct * 100).toFixed(1)}%</span>
                                <span className="const-contrib" style={{
                                  color: snapshot.positions.reduce((s, p) => s + p.contribution, 0) >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)'
                                }}>
                                  {(snapshot.positions.reduce((s, p) => s + p.contribution, 0) * 100).toFixed(3)}%</span>
                              </div>
                            </div>
                            </div>
                          )}
                        </>
                      )
                    })()}
                  </div>
                  {result && (
                    <RangeScrollbar
                      total={result.combined.equity_curve.dates.length}
                      start={eqViewRef.current.start}
                      end={eqViewRef.current.end}
                      onChange={(s, e) => { eqViewRef.current = { start: s, end: e }; setEqViewVersion(v => v + 1) }}
                    />
                  )}
                </>
              )}

              {/* ── Stats tab ── */}
              {resultTab === 'stats' && (() => {
                const cs = result.combined.stats
                const lc = result.leg_correlations
                const renderStatsTable = (
                  title: string,
                  titleColor: string,
                  portfolioRows: { label: string; values: string[]; colors?: (string | undefined)[] }[],
                  tradeRows: { label: string; values: string[]; colors?: (string | undefined)[] }[],
                ) => (
                  <div style={{ marginBottom: 20 }}>
                    <div style={{ fontWeight: 700, fontSize: 13, color: titleColor, marginBottom: 6, borderBottom: `2px solid ${titleColor}`, paddingBottom: 4 }}>{title}</div>
                    <table className="backtest-stats-table" style={{ width: '100%', marginBottom: 8 }}>
                      <thead>
                        <tr><th className="backtest-stats-th" colSpan={2} style={{ textAlign: 'left', fontSize: 11, color: '#657b83' }}>Portfolio Stats</th></tr>
                      </thead>
                      <tbody>
                        {portfolioRows.map((r, ri) => (
                          <tr key={ri}>
                            <td className="backtest-stats-td" style={{ fontWeight: 600, width: '50%' }}>{r.label}</td>
                            <td className="backtest-stats-td" style={{ color: r.colors?.[0] }}>{r.values[0]}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                    <table className="backtest-stats-table" style={{ width: '100%' }}>
                      <thead>
                        <tr><th className="backtest-stats-th" colSpan={2} style={{ textAlign: 'left', fontSize: 11, color: '#657b83' }}>Trade Stats</th></tr>
                      </thead>
                      <tbody>
                        {tradeRows.map((r, ri) => (
                          <tr key={ri}>
                            <td className="backtest-stats-td" style={{ fontWeight: 600, width: '50%' }}>{r.label}</td>
                            <td className="backtest-stats-td" style={{ color: r.colors?.[0] }}>{r.values[0]}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )

                const buildPortfolioRows = (p: PortfolioStats, legTarget?: string) => {
                  const rows: { label: string; values: string[]; colors?: (string | undefined)[] }[] = [
                    ...(p.allocation != null && resultIsMultiLeg ? [{ label: 'Allocation', values: [(p.allocation * 100).toFixed(0) + '%'] }] : []),
                    { label: 'Return', values: [pctFmt(p.strategy_return)], colors: [p.strategy_return >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)'] },
                    { label: 'CAGR', values: [pctFmt(p.cagr)], colors: [p.cagr >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)'] },
                    { label: 'Volatility', values: [pctFmt(p.volatility)] },
                    { label: 'Max Drawdown', values: [pctFmt(p.max_dd)], colors: ['rgb(255,50,150)'] },
                    { label: 'Sharpe', values: [p.sharpe.toFixed(2)] },
                    { label: 'Sortino', values: [p.sortino.toFixed(2)] },
                  ]
                  if (p.contribution != null && resultIsMultiLeg) {
                    rows.push({ label: 'Contribution', values: [pctFmt(p.contribution)], colors: [p.contribution >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)'] })
                  }
                  // Inter-leg correlations
                  if (lc && legTarget && lc[legTarget]) {
                    for (const [otherLeg, corr] of Object.entries(lc[legTarget])) {
                      rows.push({ label: `Corr: ${otherLeg.replace(/_/g, ' ')}`, values: [corr.toFixed(4)] })
                    }
                  }
                  return rows
                }

                const buildTradeRows = (t: TradeStats) => [
                  { label: 'Trades Met Criteria', values: [String(t.trades_met_criteria)] },
                  { label: 'Trades Taken', values: [String(t.trades_taken)] },
                  { label: 'Trades Skipped', values: [String(t.trades_skipped)] },
                  { label: 'Win Rate', values: [pctFmt(t.win_rate)] },
                  { label: 'Avg Winner', values: [pctFmt(t.avg_winner)], colors: ['rgb(50,50,255)'] },
                  { label: 'Avg Loser', values: [pctFmt(t.avg_loser)], colors: ['rgb(255,50,150)'] },
                  { label: 'EV', values: [pctFmt(t.ev)], colors: [t.ev >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)'] },
                  { label: 'Profit Factor', values: [t.profit_factor.toFixed(2)] },
                  { label: 'Avg Time Winner', values: [t.avg_time_winner.toFixed(1) + ' bars'] },
                  { label: 'Avg Time Loser', values: [t.avg_time_loser.toFixed(1) + ' bars'] },
                ]

                return (
                  <div style={{ padding: 12, overflowY: 'auto', display: 'flex', flexWrap: 'wrap', gap: 16, alignItems: 'flex-start' }}>
                    {result.legs.map((leg, i) =>
                      <div key={i} style={{ flex: '1 1 300px', maxWidth: 400 }}>
                        {renderStatsTable(
                          `${leg.target.replace(/_/g, ' ')} — ${leg.entry_signal.replace(/_/g, ' ')} (${leg.direction})`,
                          resultIsMultiLeg ? LEG_COLORS[i % LEG_COLORS.length] : '#657b83',
                          buildPortfolioRows(leg.stats.portfolio, leg.target),
                          buildTradeRows(leg.stats.trade),
                        )}
                      </div>
                    )}
                    {resultIsMultiLeg && (
                      <div style={{ flex: '1 1 300px', maxWidth: 400 }}>
                        {renderStatsTable(
                          'Combined Portfolio',
                          '#8BC34A',
                          buildPortfolioRows(cs.portfolio),
                          buildTradeRows(cs.trade),
                        )}
                      </div>
                    )}
                  </div>
                )
              })()}

              {/* ── Distribution tab ── */}
              {resultTab === 'distribution' && (
                <>
                  <div className="backtest-hist-toggles">
                    {resultIsMultiLeg && (
                      <div style={{ display: 'flex', gap: 4, marginRight: 12, alignItems: 'center' }}>
                        <span style={{ fontSize: 11, fontWeight: 600, color: '#657b83' }}>Leg:</span>
                        <button
                          className={`summary-tab ${distLegFilter === -1 ? 'active' : ''}`}
                          onClick={() => setDistLegFilter(-1)}>All</button>
                        {result.legs.map((leg, i) => (
                          <button key={i}
                            className={`summary-tab ${distLegFilter === i ? 'active' : ''}`}
                            style={{ borderBottomColor: distLegFilter === i ? LEG_COLORS[i] : undefined }}
                            onClick={() => setDistLegFilter(i)}>
                            {leg.target.replace(/_/g, ' ').slice(0, 12)}
                          </button>
                        ))}
                      </div>
                    )}
                    {(['mae', 'change', 'mfe'] as HistMetric[]).map(m => (
                      <button key={m}
                        className={`summary-tab ${histVisible.has(m) ? 'active' : ''}`}
                        style={histVisible.has(m) ? { borderBottomColor: HIST_COLORS[m].stroke, color: HIST_COLORS[m].stroke } : {}}
                        onClick={() => setHistVisible(prev => {
                          const next = new Set(prev)
                          if (next.has(m)) next.delete(m); else next.add(m)
                          return next
                        })}>{HIST_COLORS[m].label}</button>
                    ))}
                  </div>
                  <div className="backtest-chart" ref={histContainerRef}>
                    <canvas ref={histCanvasRef} style={{ display: 'block', width: '100%', height: '100%' }}
                      onMouseMove={handleHistMouseMove} onMouseLeave={handleHistMouseLeave} />
                  </div>
                </>
              )}

              {/* ── Path tab ── */}
              {resultTab === 'path' && (
                <div className="backtest-path-container">
                  <div className="backtest-chart" ref={pathContainerRef} style={{ position: 'relative' }}>
                    <canvas ref={pathCanvasRef} style={{ display: 'block', width: '100%', height: '100%' }} />
                    <button className={`log-toggle-btn ${pathLogScale ? 'active' : ''}`} onClick={() => setPathLogScale(v => !v)}>L</button>
                  </div>
                  <div className="backtest-path-legend" style={resultIsMultiLeg ? { width: 320, minWidth: 320 } : undefined}>
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
                            {resultIsMultiLeg && <span className="path-legend-col ticker" onClick={() => toggleSort('leg')}>Leg{arrow('leg')}</span>}
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
                              {resultIsMultiLeg && <span className="path-legend-col ticker" style={{ color: LEG_COLORS[legIdx % LEG_COLORS.length] }}>{legTarget.replace(/_/g, ' ').slice(0, 10)}</span>}
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

              {/* ── Trades tab ── */}
              {resultTab === 'trades' && (
                <div style={{ padding: '8px 12px', overflowY: 'auto', flex: 1, minHeight: 0 }}>
                  {resultIsMultiLeg && (
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
                  )}
                  <div className="summary-table-wrapper">
                    <table className="summary-table">
                      <thead>
                        <tr>
                          {[
                            ...(resultIsMultiLeg ? ['legTarget'] : []),
                            'ticker', 'entry_date', 'exit_date', 'entry_price', 'exit_price', 'change',
                            'entry_weight', 'exit_weight', 'contribution', 'bars_held',
                          ].map(col => (
                            <th key={col} className="summary-th"
                              onClick={() => { setSortCol(col); setSortAsc(sortCol === col ? !sortAsc : true) }}
                              style={{ cursor: 'pointer' }}>
                              {col === 'legTarget' ? 'Leg' : col === 'entry_weight' ? 'Ent.Wt' : col === 'exit_weight' ? 'Ex.Wt' : col === 'contribution' ? 'Contrib' : col.replace(/_/g, ' ')}
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
                            {resultIsMultiLeg && (
                              <td className="summary-td" style={{ color: LEG_COLORS[t.legIdx % LEG_COLORS.length], fontWeight: 600, fontSize: 11 }}>
                                {t.legTarget}
                              </td>
                            )}
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
                            <td className="summary-td">{t.skipped ? '--' : pctFmt(t.entry_weight)}</td>
                            <td className="summary-td">{t.skipped ? '--' : pctFmt(t.exit_weight)}</td>
                            <td className="summary-td" style={{
                              color: !t.skipped && t.contribution != null ? (t.contribution >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)') : undefined,
                              fontWeight: !t.skipped && t.contribution != null ? 600 : undefined,
                            }}>
                              {t.skipped ? '--' : pctFmt(t.contribution)}
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

  /* ━━━━━━━━━━━━━━━━━━━━━━ Configuration Mode ━━━━━━━━━━━━━━━━━━━━━━ */

  const renderLegCard = (leg: LegConfig, i: number) => (
    <div key={i} className={isSingleLeg ? 'single-leg-card' : 'multi-leg-card'}
      style={isMultiLeg ? { borderTop: `3px solid ${LEG_COLORS[i % LEG_COLORS.length]}` } : undefined}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <label className="backtest-label" style={isMultiLeg ? { color: LEG_COLORS[i % LEG_COLORS.length] } : undefined}>
          {isMultiLeg ? `Leg ${i + 1}` : 'Target'}
        </label>
        {isMultiLeg && (
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
          {isMultiLeg && (
            <div className="backtest-sizing-field">
              <span className="backtest-hint">Alloc %</span>
              <input type="number" className="backtest-input" min={1} max={100}
                value={leg.allocationPct}
                onChange={e => updateLeg(i, { allocationPct: Number(e.target.value) })} />
            </div>
          )}
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
                  updateFilterField(i, fi, 'metric', m)
                  if (BOOL_METRICS.includes(m)) updateFilterField(i, fi, 'condition', 'equals_true')
                }}>
                <optgroup label="Percentage">
                  {PCT_METRICS.map(m => <option key={m} value={m}>{m}</option>)}
                </optgroup>
                <optgroup label="Boolean">
                  {BOOL_METRICS.map(m => <option key={m} value={m}>{m}</option>)}
                </optgroup>
              </select>
              <select className="backtest-select" value={f.condition}
                onChange={e => updateFilterField(i, fi, 'condition', e.target.value)}>
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
                  onChange={e => updateFilterField(i, fi, 'value', e.target.value)} />
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
        <div className="multi-leg-grid">
          {/* Settings card — always first, same size as leg cards */}
          <div className={isSingleLeg ? 'single-leg-card' : 'multi-leg-card'}>
            {isMultiLeg && (
              <div className="backtest-section" style={{
                color: allocValid ? '#2E7D32' : '#C2185B',
                background: allocValid ? 'rgba(46,125,50,0.06)' : 'rgba(194,24,91,0.06)',
                padding: '6px 8px',
              }}>
                <span style={{ fontWeight: 600, fontSize: 12 }}>
                  Total Allocation: {totalAlloc}% {allocValid ? '' : '(must equal 100%)'}
                </span>
              </div>
            )}

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
              disabled={loading || (isMultiLeg && !allocValid) || !allTargetsSet}>
              {loading ? 'Running...' : 'Run Backtest'}
            </button>
            {error && <div className="backtest-error">{error}</div>}
          </div>

          {/* Leg cards */}
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
