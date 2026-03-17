import { useState, useMemo, useRef, useEffect, useCallback } from 'react'
import axios from 'axios'
import { createChart, ColorType, CrosshairMode, LineStyle, IChartApi } from 'lightweight-charts'
import { RangeScrollbar } from './RangeScrollbar'
import { MultiBacktestPanel } from './MultiBacktestPanel'

interface BacktestFilter {
  metric: string
  condition: string
  value: number | string
  source: string
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

interface DailyPosition {
  trade_idx: number
  ticker?: string
  entry_date: string
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

interface SkippedEntry {
  ticker?: string
  entry_date: string
  entry_price: number
  reason: string
  exposure_at_skip: number
  equity_at_skip: number
}

interface BacktestResult {
  trades: Trade[]
  trade_paths: number[][]
  equity_curve: { dates: string[]; filtered: number[]; unfiltered: number[]; buy_hold: number[] }
  stats: { filtered: Stats; unfiltered: Stats }
  date_range: { min: string; max: string }
  blew_up?: { date: string; trade_index: number; equity: string }
  daily_positions?: Record<number, DailySnapshot>
  skipped_entries?: SkippedEntry[]
}

interface BacktestPanelProps {
  target: string
  targetType: 'basket' | 'ticker'
  apiBase: string
  availableBaskets: string[]
  exportTrigger?: number
}

const ENTRY_SIGNALS = ['Up_Rot', 'Down_Rot', 'Breakout', 'Breakdown', 'BTFD', 'STFR']
const EXIT_MAP: Record<string, string> = {
  Up_Rot: 'Down_Rot', Down_Rot: 'Up_Rot',
  Breakout: 'Breakdown', Breakdown: 'Breakout',
  BTFD: 'Breakdown', STFR: 'Breakout',
}

const PCT_METRICS = ['Uptrend_Pct', 'Breakout_Pct', 'Correlation_Pct', 'RV_EMA']
const BOOL_METRICS = ['Is_Breakout_Sequence', 'Trend', 'BTFD_Triggered', 'STFR_Triggered']
const POS_PRESETS = [1, 5, 10, 25, 50, 100]
const LEV_PRESETS = [100, 110, 125, 150, 200, 250]

type TradeSortCol = 'ticker' | 'entry_date' | 'exit_date' | 'entry_price' | 'exit_price' | 'change' | 'mfe' | 'mae' | 'bars_held' | 'regime_pass' | 'skipped'
type ResultTab = 'equity' | 'trades' | 'distribution' | 'chart' | 'path' | 'stats'
type HistMetric = 'change' | 'mfe' | 'mae'

const LIGHT_PINK = 'rgb(255, 183, 226)'
const LIGHT_BLUE = 'rgb(179, 222, 255)'

const LONG_SIGNALS = new Set(['Up_Rot', 'Breakout', 'BTFD'])

const BENCHMARK_COLORS: Record<string, string> = {
  Breakout: '#1565C0',
  Up_Rot: '#42A5F5',
  BTFD: '#90CAF9',
  Breakdown: '#C2185B',
  Down_Rot: '#F06292',
  STFR: '#F8BBD0',
}

const parseTime = (dateStr: string) => {
  const s = dateStr.includes('T') ? dateStr : dateStr + 'T00:00:00'
  const d = new Date(s.endsWith('Z') ? s : s + 'Z')
  return Math.floor(d.getTime() / 1000) as any
}

const HIST_COLORS: Record<HistMetric, { fill: string; stroke: string; label: string }> = {
  mae:    { fill: LIGHT_PINK,           stroke: LIGHT_PINK,            label: 'MAE' },
  change: { fill: 'rgb(200, 210, 220)', stroke: 'rgb(200, 210, 220)', label: 'Change' },
  mfe:    { fill: LIGHT_BLUE,           stroke: LIGHT_BLUE,            label: 'MFE' },
}

function pctFmt(v: number | null): string {
  if (v === null) return '--'
  return (v * 100).toFixed(2) + '%'
}

function dollarFmt(v: number): string {
  const abs = Math.abs(v)
  const sign = v < 0 ? '-' : ''
  if (abs >= 1_000_000) return sign + '$' + (abs / 1_000_000).toFixed(2) + 'M'
  if (abs >= 1_000) return sign + '$' + (abs / 1_000).toFixed(0) + 'K'
  return sign + '$' + abs.toFixed(0)
}

export function BacktestPanel({ target, targetType, apiBase, availableBaskets, exportTrigger }: BacktestPanelProps) {
  // Chart overlay toggles (local to backtest)
  const [showPivots, setShowPivots] = useState(true)
  const [showTargets, setShowTargets] = useState(true)
  const [showVolume, setShowVolume] = useState(false)
  const [showBreadth, setShowBreadth] = useState(false)
  const [showBreakout, setShowBreakout] = useState(false)
  const [showCorrelation, setShowCorrelation] = useState(false)
  const [showRV, setShowRV] = useState(true)

  // Multi-leg mode
  const [multiMode, setMultiMode] = useState(false)

  // Basket selector for single-leg mode (allows testing any basket regardless of sidebar selection)
  const [allBaskets, setAllBaskets] = useState<Record<string, string[]>>({})
  const [selectedTarget, setSelectedTarget] = useState(target)
  const [selectedTargetType, setSelectedTargetType] = useState(targetType)

  useEffect(() => {
    fetch(`${apiBase}/baskets`).then(r => r.json()).then(setAllBaskets).catch(() => {})
  }, [apiBase])

  // Sync with props when they change (user navigated to different item)
  useEffect(() => { setSelectedTarget(target); setSelectedTargetType(targetType) }, [target, targetType])

  // Config state
  const [entrySignal, setEntrySignal] = useState('Breakout')
  const [filters, setFilters] = useState<BacktestFilter[]>([])
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [dataRange, setDataRange] = useState<{ min: string; max: string } | null>(null)
  const [initialEquity, setInitialEquity] = useState(100000)
  const [positionSize, setPositionSize] = useState(100)
  const [maxLeverage, setMaxLeverage] = useState(250)
  const [useConstituents, setUseConstituents] = useState(false)

  // Results state
  const [result, setResult] = useState<BacktestResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [showResults, setShowResults] = useState(false)

  // Results sub-state
  const [resultTab, setResultTab] = useState<ResultTab>('equity')
  const [sortCol, setSortCol] = useState<TradeSortCol>('entry_date')
  const [sortAsc, setSortAsc] = useState(true)
  const [histVisible, setHistVisible] = useState<Set<HistMetric>>(new Set(['change', 'mfe', 'mae']))
  const [hoveredCurve, setHoveredCurve] = useState<HistMetric | null>(null)
  const [showFiltered, setShowFiltered] = useState(true)
  const [showBuyHold, setShowBuyHold] = useState(false)
  const [benchmarks, setBenchmarks] = useState<Record<string, number[]>>({})
  const [showBenchmark, setShowBenchmark] = useState<Record<string, boolean>>({})
  const benchmarkGenRef = useRef(0)

  // Equity curve view range (index-based, ref for synchronous mutation in mouse handlers)
  const eqViewRef = useRef({ start: 0, end: 0 })
  const [eqViewVersion, setEqViewVersion] = useState(0)
  const eqDragRef = useRef({ dragging: false, lastX: 0 })

  // Canvas refs — equity curve
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const [dims, setDims] = useState({ w: 800, h: 400 })

  // Canvas refs — histogram
  const histCanvasRef = useRef<HTMLCanvasElement>(null)
  const histContainerRef = useRef<HTMLDivElement>(null)
  const [histDims, setHistDims] = useState({ w: 800, h: 400 })
  // Chart tab refs
  const priceChartRef = useRef<HTMLDivElement>(null)
  const chartInstanceRef = useRef<IChartApi | null>(null)
  const btChartsRef = useRef<Record<string, IChartApi | null>>({})
  const btSeriesRef = useRef<Record<string, any>>({})
  const volChartRef = useRef<HTMLDivElement>(null)
  const breadthChartRef = useRef<HTMLDivElement>(null)
  const breakoutChartRef = useRef<HTMLDivElement>(null)
  const corrChartRef = useRef<HTMLDivElement>(null)
  const rvChartRef = useRef<HTMLDivElement>(null)
  const btSavedRangeRef = useRef<{ from: number; to: number } | null>(null)
  const [tradeIndex, setTradeIndex] = useState(0)

  // Path tab refs
  const pathCanvasRef = useRef<HTMLCanvasElement>(null)
  const pathContainerRef = useRef<HTMLDivElement>(null)
  const [pathDims, setPathDims] = useState({ w: 800, h: 400 })
  const [hoveredPath, setHoveredPath] = useState<number | null>(null)
  const [pathSortCol, setPathSortCol] = useState<'ticker' | 'date' | 'change'>('change')
  const [pathSortAsc, setPathSortAsc] = useState(false)

  // Constituents overlay state
  const [showConstituents, setShowConstituents] = useState(false)
  const [eqHoverIdx, setEqHoverIdx] = useState<number | null>(null)
  const [eqPinnedIdx, setEqPinnedIdx] = useState<number | null>(null)
  const eqScaleRef = useRef({ padLeft: 20, plotW: 0, n: 0, startIdx: 0 })
  const eqDidDragRef = useRef(false)

  // Chart ticker navigation (for basket_tickers mode)
  const [chartTickerIdx, setChartTickerIdx] = useState(0)

  // Store computed curve pixel paths + legend rects for mouse hit-testing
  const histHitRef = useRef<{
    curves: { metric: HistMetric; pxPoints: { x: number; y: number }[] }[]
    legendRows: { metric: HistMetric; x: number; y: number; w: number; h: number }[]
    baseline: number // y pixel of the bottom of the plot area
  }>({ curves: [], legendRows: [], baseline: 0 })

  // Export current tab when exportTrigger changes
  const prevExportTrigger = useRef(exportTrigger || 0)
  useEffect(() => {
    if (!exportTrigger || exportTrigger === prevExportTrigger.current) {
      prevExportTrigger.current = exportTrigger || 0
      return
    }
    prevExportTrigger.current = exportTrigger

    const tabLabel = resultTab
    const filename = `${selectedTarget}_${entrySignal}_${tabLabel}.png`

    const downloadCanvas = (canvas: HTMLCanvasElement) => {
      canvas.toBlob((blob) => {
        if (!blob) return
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = filename
        a.click()
        URL.revokeObjectURL(url)
      }, 'image/png')
    }

    if (resultTab === 'chart') {
      const chart = chartInstanceRef.current
      if (chart) {
        try { downloadCanvas(chart.takeScreenshot()) } catch {}
      }
    } else if (resultTab === 'equity') {
      if (canvasRef.current) downloadCanvas(canvasRef.current)
    } else if (resultTab === 'distribution') {
      if (histCanvasRef.current) downloadCanvas(histCanvasRef.current)
    } else if (resultTab === 'path') {
      if (pathCanvasRef.current) downloadCanvas(pathCanvasRef.current)
    }
  }, [exportTrigger])

  // Fetch available date range when target changes
  useEffect(() => {
    let cancelled = false
    axios.get(`${apiBase}/date-range/${selectedTargetType}/${selectedTarget}`)
      .then(res => {
        if (cancelled) return
        setDataRange(res.data)
        setStartDate(res.data.min)
        setEndDate(res.data.max)
      })
      .catch(() => {
        if (!cancelled) setDataRange(null)
      })
    return () => { cancelled = true }
  }, [selectedTarget, selectedTargetType, apiBase])

  const addFilter = () => {
    setFilters(prev => [...prev, { metric: 'Uptrend_Pct', condition: 'above', value: 50, source: 'self' }])
  }

  const removeFilter = (i: number) => {
    setFilters(prev => prev.filter((_, idx) => idx !== i))
  }

  const updateFilter = (i: number, field: string, val: string | number) => {
    setFilters(prev => prev.map((f, idx) => {
      if (idx !== i) return f
      const updated = { ...f, [field]: val }
      // Reset condition when switching metric type
      if (field === 'metric') {
        if (BOOL_METRICS.includes(val as string)) {
          updated.condition = 'equals_true'
          updated.value = ''
        } else {
          updated.condition = 'above'
          updated.value = 50
        }
      }
      return updated
    }))
  }

  const runBacktest = async () => {
    setLoading(true)
    setError(null)
    setBenchmarks({})
    const gen = ++benchmarkGenRef.current
    try {
      const effectiveType = (selectedTargetType === 'basket' && useConstituents) ? 'basket_tickers' : selectedTargetType
      const body = {
        target: selectedTarget,
        target_type: effectiveType,
        entry_signal: entrySignal,
        filters: filters.map(f => ({
          metric: f.metric,
          condition: f.condition,
          value: BOOL_METRICS.includes(f.metric) ? null : Number(f.value),
          source: f.source,
        })),
        start_date: startDate || null,
        end_date: endDate || null,
        initial_equity: initialEquity,
        position_size: positionSize / 100,
        max_leverage: maxLeverage / 100,
        include_positions: true,
      }

      // Fire main + 6 benchmarks all in parallel
      const benchBody = {
        target: selectedTarget,
        target_type: effectiveType,
        filters: [],
        start_date: startDate || null,
        end_date: endDate || null,
        initial_equity: initialEquity,
        position_size: positionSize / 100,
        max_leverage: maxLeverage / 100,
        benchmarks_only: true,
      }

      const mainPromise = axios.post(`${apiBase}/backtest`, body)

      const benchPromises = ENTRY_SIGNALS.map(sig =>
        axios.post(`${apiBase}/backtest`, { ...benchBody, entry_signal: sig })
          .then(r => ({ sig, curve: r.data.equity_curve.unfiltered as number[] }))
          .catch(err => { console.error(`Benchmark ${sig} failed:`, err?.response?.data?.detail || err?.message); return null })
      )

      // Wait for everything
      const [res, ...benchResults] = await Promise.all([mainPromise, ...benchPromises])

      if (benchmarkGenRef.current !== gen) return
      const newBench: Record<string, number[]> = {}
      for (const b of benchResults) {
        if (b) newBench[b.sig] = b.curve
      }
      setBenchmarks(newBench)
      setResult(res.data)
      setShowResults(true)
      setResultTab('equity')
    } catch (e: any) {
      setError(e?.response?.data?.detail || e?.message || 'Backtest failed')
    } finally {
      setLoading(false)
    }
  }

  // Reset equity view range when new results arrive
  useEffect(() => {
    if (result) {
      eqViewRef.current = { start: 0, end: result.equity_curve.dates.length - 1 }
      setEqViewVersion(v => v + 1)
    }
  }, [result])

  // Equity view range presets
  const findEqDateIndex = useCallback((targetDate: string, dates: string[]): number => {
    for (let i = 0; i < dates.length; i++) {
      if (dates[i] >= targetDate) return i
    }
    return dates.length - 1
  }, [])

  const setEqPreset = useCallback((preset: '1Y' | '3Y' | '5Y' | 'YTD' | 'All') => {
    if (!result) return
    const dates = result.equity_curve.dates
    if (!dates.length) return
    const endIdx = dates.length - 1
    let startIdx = 0

    if (preset !== 'All') {
      const d = new Date(dates[endIdx])
      if (preset === 'YTD') {
        startIdx = findEqDateIndex(`${d.getFullYear()}-01-01`, dates)
      } else {
        const years = preset === '1Y' ? 1 : preset === '3Y' ? 3 : 5
        d.setFullYear(d.getFullYear() - years)
        startIdx = findEqDateIndex(d.toISOString().slice(0, 10), dates)
      }
    }

    eqViewRef.current = { start: startIdx, end: endIdx }
    setEqViewVersion(v => v + 1)
  }, [result, findEqDateIndex])

  // Sorted trades
  const sortedTrades = useMemo(() => {
    if (!result) return []
    const arr = [...result.trades]
    arr.sort((a, b) => {
      const av = a[sortCol], bv = b[sortCol]
      if (typeof av === 'string' && typeof bv === 'string') return sortAsc ? av.localeCompare(bv) : bv.localeCompare(av)
      if (typeof av === 'boolean' && typeof bv === 'boolean') return sortAsc ? (av === bv ? 0 : av ? -1 : 1) : (av === bv ? 0 : av ? 1 : -1)
      const an = av === null || av === undefined ? -Infinity : Number(av)
      const bn = bv === null || bv === undefined ? -Infinity : Number(bv)
      return sortAsc ? an - bn : bn - an
    })
    return arr
  }, [result, sortCol, sortAsc])

  // Unique tickers from trades (for multi-ticker chart navigation)
  const chartTickers = useMemo(() => {
    if (!result) return []
    const seen = new Set<string>()
    const out: string[] = []
    for (const t of result.trades) {
      if (t.ticker && !seen.has(t.ticker)) { seen.add(t.ticker); out.push(t.ticker) }
    }
    return out.sort()
  }, [result])

  const handleSort = (col: TradeSortCol) => {
    if (sortCol === col) setSortAsc(!sortAsc)
    else { setSortCol(col); setSortAsc(col === 'entry_date' || col === 'exit_date') }
  }

  // ResizeObserver for equity canvas
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

  // ResizeObserver for histogram canvas
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

  // ResizeObserver for path canvas
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

  // Windowed equity data (sliced to visible range)
  const eqWindowed = useMemo(() => {
    if (!result) return null
    const { dates, filtered, unfiltered, buy_hold } = result.equity_curve
    const { start: s, end: e } = eqViewRef.current
    const cs = Math.max(0, Math.min(s, dates.length - 1))
    const ce = Math.max(cs, Math.min(e, dates.length - 1))
    const windowedBench: Record<string, number[]> = {}
    for (const [sig, curve] of Object.entries(benchmarks)) {
      windowedBench[sig] = curve.slice(cs, ce + 1)
    }
    return {
      dates: dates.slice(cs, ce + 1),
      filtered: filtered.slice(cs, ce + 1),
      unfiltered: unfiltered.slice(cs, ce + 1),
      buy_hold: buy_hold ? buy_hold.slice(cs, ce + 1) : [],
      benchmarks: windowedBench,
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [result, eqViewVersion, benchmarks])

  // Draw equity curve (dollar-based)
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas || !eqWindowed || resultTab !== 'equity') return
    const { dates, filtered, buy_hold, benchmarks: wBench } = eqWindowed
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

    // Y range from visible series only
    let yMin = initialEquity, yMax = initialEquity
    if (showFiltered) for (const v of filtered) { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) }
    if (showBuyHold && buy_hold) for (const v of buy_hold) { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) }
    for (const sig of ENTRY_SIGNALS) {
      if (showBenchmark[sig] && wBench[sig]) {
        for (const v of wBench[sig]) { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) }
      }
    }
    const yPad = (yMax - yMin) * 0.1 || initialEquity * 0.1
    yMin -= yPad; yMax += yPad

    const n = dates.length
    eqScaleRef.current = { padLeft: pad.left, plotW, n, startIdx: eqViewRef.current.start }
    const xScale = (i: number) => pad.left + (n > 1 ? (i / (n - 1)) * plotW : plotW / 2)
    const yScale = (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH

    ctx.clearRect(0, 0, dims.w, dims.h)
    ctx.fillStyle = '#fdf6e3'
    ctx.fillRect(0, 0, dims.w, dims.h)

    // Grid
    ctx.strokeStyle = '#eee8d5'
    ctx.lineWidth = 1
    const nTicks = 6
    for (let i = 0; i <= nTicks; i++) {
      const v = yMin + (yMax - yMin) * (i / nTicks)
      const y = yScale(v)
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(dims.w - pad.right, y); ctx.stroke()
      ctx.fillStyle = '#657b83'; ctx.font = '10px monospace'; ctx.textAlign = 'left'
      ctx.fillText(dollarFmt(v), dims.w - pad.right + 5, y + 3)
    }

    // Breakeven line at initial equity
    const beY = yScale(initialEquity)
    ctx.strokeStyle = '#93a1a1'; ctx.lineWidth = 1; ctx.setLineDash([4, 4])
    ctx.beginPath(); ctx.moveTo(pad.left, beY); ctx.lineTo(dims.w - pad.right, beY); ctx.stroke()
    ctx.setLineDash([])

    // X axis labels
    const labelInterval = Math.max(1, Math.floor(n / 8))
    ctx.fillStyle = '#657b83'; ctx.font = '10px monospace'; ctx.textAlign = 'center'
    for (let i = 0; i < n; i += labelInterval) {
      ctx.fillText(dates[i].slice(0, 7), xScale(i), dims.h - pad.bottom + 15)
    }

    // Helper: draw a solid line (uses data's own length for x-scaling)
    const drawLine = (data: number[], color: string, lw: number) => {
      const len = data.length
      if (len === 0) return
      const xS = (i: number) => pad.left + (len > 1 ? (i / (len - 1)) * plotW : plotW / 2)
      ctx.strokeStyle = color
      ctx.lineWidth = lw
      ctx.beginPath()
      for (let i = 0; i < len; i++) {
        const x = xS(i), y = yScale(data[i])
        if (i === 0) ctx.moveTo(x, y)
        else ctx.lineTo(x, y)
      }
      ctx.stroke()
    }

    // Draw order (back to front): Buy & Hold → 6 benchmarks → Filtered
    if (showBuyHold && buy_hold && buy_hold.length > 0) {
      drawLine(buy_hold, '#93a1a1', 1.5)
    }

    for (const sig of ENTRY_SIGNALS) {
      if (showBenchmark[sig] && wBench[sig] && wBench[sig].length > 0) {
        drawLine(wBench[sig], BENCHMARK_COLORS[sig], 1.5)
      }
    }

    if (showFiltered) {
      drawLine(filtered, '#8BC34A', 2.5)
    }

    // Legend
    const legendItems: { label: string; value: string; color: string }[] = []
    if (showFiltered) {
      const v = filtered[n - 1]
      const ret = (v - initialEquity) / initialEquity
      legendItems.push({ label: 'Filtered', value: `${dollarFmt(v)} (${pctFmt(ret)})`, color: '#8BC34A' })
    }
    for (const sig of ENTRY_SIGNALS) {
      if (showBenchmark[sig] && wBench[sig] && wBench[sig].length > 0) {
        const v = wBench[sig][wBench[sig].length - 1]
        const ret = (v - initialEquity) / initialEquity
        legendItems.push({ label: sig.replace('_', ' '), value: `${dollarFmt(v)} (${pctFmt(ret)})`, color: BENCHMARK_COLORS[sig] })
      }
    }
    if (showBuyHold && buy_hold && buy_hold.length > 0) {
      const v = buy_hold[buy_hold.length - 1]
      const ret = (v - initialEquity) / initialEquity
      legendItems.push({ label: 'Buy&Hold', value: `${dollarFmt(v)} (${pctFmt(ret)})`, color: '#93a1a1' })
    }

    if (legendItems.length > 0) {
      const lx = pad.left + 10, ly = pad.top + 10
      const rowH = 16
      const boxH = legendItems.length * rowH + 2
      ctx.fillStyle = '#fdf6e3'
      ctx.fillRect(lx - 4, ly - 10, 250, boxH)
      ctx.strokeStyle = '#93a1a1'; ctx.lineWidth = 1
      ctx.strokeRect(lx - 4, ly - 10, 250, boxH)
      legendItems.forEach((item, idx) => {
        const iy = ly + idx * rowH
        ctx.strokeStyle = item.color; ctx.lineWidth = 1.5
        ctx.beginPath(); ctx.moveTo(lx, iy); ctx.lineTo(lx + 20, iy); ctx.stroke()
        ctx.fillStyle = '#586e75'; ctx.font = 'bold 10px monospace'; ctx.textAlign = 'left'
        ctx.fillText(`${item.label} ${item.value}`, lx + 24, iy + 3)
      })
    }

  }, [eqWindowed, resultTab, dims, initialEquity, showFiltered, showBuyHold, showBenchmark])

  // Equity curve: wheel zoom + click-drag pan
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas || !result || resultTab !== 'equity') return
    const n = result.equity_curve.dates.length
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
      let ns = start - leftAdj
      let ne = end + rightAdj
      if (ns < 0) { ne -= ns; ns = 0 }
      if (ne >= n) { ns -= (ne - n + 1); ne = n - 1 }
      ns = Math.max(0, ns)
      ne = Math.min(n - 1, ne)
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
      let ns = start - indexDelta
      let ne = end - indexDelta
      if (ns < 0) { ne -= ns; ns = 0 }
      if (ne >= n) { ns -= (ne - n + 1); ne = n - 1 }
      ns = Math.max(0, ns)
      ne = Math.min(n - 1, ne)
      eqViewRef.current = { start: ns, end: ne }
      eqDragRef.current.lastX = e.clientX
      setEqViewVersion(v => v + 1)
    }

    const handleMouseUp = () => {
      if (eqDragRef.current.dragging) {
        eqDragRef.current.dragging = false
        canvas.style.cursor = 'grab'
      }
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

  // Constituents overlay: hover, pin, escape handlers
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
      if (idx >= startIdx && idx < startIdx + n) {
        setEqPinnedIdx(prev => prev === idx ? null : idx)
      }
    }

    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') { setEqPinnedIdx(null); setEqHoverIdx(null) }
    }

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

  // Draw overlaid KDE density curves
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

    // Collect values for all visible metrics
    const metrics = (['mae', 'change', 'mfe'] as HistMetric[]).filter(m => histVisible.has(m))
    if (metrics.length === 0) return

    const allValsByMetric: Record<string, number[]> = {}
    let globalMin = Infinity, globalMax = -Infinity
    for (const m of metrics) {
      const vals = result.trades.map(t => t[m]).filter((v): v is number => v !== null && v !== undefined)
      allValsByMetric[m] = vals
      for (const v of vals) { globalMin = Math.min(globalMin, v); globalMax = Math.max(globalMax, v) }
    }

    if (globalMin === Infinity) {
      ctx.fillStyle = '#6c757d'; ctx.font = '14px monospace'; ctx.textAlign = 'center'
      ctx.fillText('No data', histDims.w / 2, histDims.h / 2)
      return
    }

    // Extend range slightly for padding
    const range = globalMax - globalMin || 0.01
    const xMin = globalMin - range * 0.08
    const xMax = globalMax + range * 0.08

    const pad = { top: 30, right: 30, bottom: 50, left: 20 }
    const plotW = histDims.w - pad.left - pad.right
    const plotH = histDims.h - pad.top - pad.bottom
    const nPoints = Math.min(200, plotW)

    // KDE: Gaussian kernel with reduced bandwidth, hard boundary clamp
    function kde(vals: number[], clampMax?: number, clampMin?: number): { xs: number[]; ys: number[] } {
      const n = vals.length
      const mean = vals.reduce((a, b) => a + b, 0) / n
      const std = Math.sqrt(vals.reduce((s, v) => s + (v - mean) ** 2, 0) / n) || range * 0.1
      const h = 0.5 * std * Math.pow(n, -0.2)
      const invH = 1 / h
      const coeff = 1 / (n * h * Math.sqrt(2 * Math.PI))

      // Build evaluation points, injecting exact boundary values
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

    // Compute KDE for each metric
    const median = (arr: number[]) => {
      if (arr.length === 0) return 0
      const s = [...arr].sort((a, b) => a - b)
      const mid = Math.floor(s.length / 2)
      return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2
    }
    // Split trades into winners/losers by trade outcome (change > 0)
    const winIdx = new Set<number>()
    const loseIdx = new Set<number>()
    result.trades.forEach((t, i) => {
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
      result.trades.forEach((t, i) => {
        const v = t[m]
        if (v === null || v === undefined) return
        if (winIdx.has(i)) winVals.push(v as number)
        else loseVals.push(v as number)
      })
      curves.push({ metric: m, xs, ys, medianWin: median(winVals), medianLose: median(loseVals) })
    }

    if (maxDensity === 0) return

    // Scales
    const xScale = (v: number) => pad.left + ((v - xMin) / (xMax - xMin)) * plotW
    const yScale = (d: number) => pad.top + plotH - (d / maxDensity) * plotH

    // Store pixel points for hit-testing
    const hitCurves: typeof histHitRef.current.curves = []
    for (const { metric, xs, ys } of curves) {
      hitCurves.push({ metric, pxPoints: xs.map((x, i) => ({ x: xScale(x), y: yScale(ys[i]) })) })
    }

    // Grid lines
    ctx.strokeStyle = '#e9ecef'; ctx.lineWidth = 1
    for (let i = 0; i <= 4; i++) {
      const y = pad.top + (plotH * i) / 4
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + plotW, y); ctx.stroke()
    }

    // Draw order: non-hovered first (dimmed), hovered last (on top)
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
        // Filled area
        ctx.beginPath()
        ctx.moveTo(xScale(xs[0]), baseline)
        for (let i = 0; i < xs.length; i++) ctx.lineTo(xScale(xs[i]), yScale(ys[i]))
        ctx.lineTo(xScale(xs[xs.length - 1]), baseline)
        ctx.closePath()
        ctx.fillStyle = colors.fill
        ctx.fill()

        // Stroke outline — follows the full boundary including vertical edges
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


    // Legend — top right
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
      // Color swatch
      ctx.fillStyle = colors.fill
      ctx.fillRect(lx, rowY - 4, 14, 10)
      ctx.strokeStyle = colors.stroke; ctx.lineWidth = 1
      ctx.strokeRect(lx, rowY - 4, 14, 10)
      // Label — fixed columns: name | W value | L value
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

  }, [result, resultTab, histVisible, histDims, hoveredCurve])

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

  // Draw path chart (trade paths from 0% to exit)
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

    const paths = result.trade_paths
    if (!paths || paths.length === 0) {
      ctx.fillStyle = '#6c757d'; ctx.font = '14px monospace'; ctx.textAlign = 'center'
      ctx.fillText('No trade paths', pathDims.w / 2, pathDims.h / 2)
      return
    }

    // Find max bars and Y range
    let maxBars = 0
    let yMin = 0, yMax = 0
    for (const p of paths) {
      maxBars = Math.max(maxBars, p.length)
      for (const v of p) { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) }
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
    const zeroY = yScale(0)
    ctx.strokeStyle = '#adb5bd'; ctx.lineWidth = 1; ctx.setLineDash([4, 4])
    ctx.beginPath(); ctx.moveTo(pad.left, zeroY); ctx.lineTo(pathDims.w - pad.right, zeroY); ctx.stroke()
    ctx.setLineDash([])

    // X axis labels (bars)
    const labelInterval = Math.max(1, Math.floor(maxBars / 10))
    ctx.fillStyle = '#6c757d'; ctx.font = '10px monospace'; ctx.textAlign = 'center'
    for (let i = 0; i < maxBars; i += labelInterval) {
      ctx.fillText(String(i), xScale(i), pathDims.h - pad.bottom + 15)
    }

    // Rank paths by final return (best first) for color gradient
    const ranked = paths.map((p, i) => ({ p, i, ret: p.length > 0 ? p[p.length - 1] : 0 }))
      .filter(d => d.p.length >= 2)
      .sort((a, b) => b.ret - a.ret)
    const totalRanked = ranked.length

    // Blue (best) -> Purple (mid) -> Pink (worst) gradient
    const rankColor = (rank: number, total: number): string => {
      if (total <= 1) return 'rgb(50, 50, 255)'
      const t = rank / (total - 1)
      if (t <= 0.5) {
        const s = t * 2
        return `rgb(${Math.round(50 + 102 * s)}, 50, ${Math.round(255 - 53 * s)})`
      } else {
        const s = (t - 0.5) * 2
        return `rgb(${Math.round(152 + 103 * s)}, 50, ${Math.round(202 - 52 * s)})`
      }
    }

    // Draw non-hovered paths first, then hovered on top
    for (let ri = 0; ri < totalRanked; ri++) {
      const { p, i: pi } = ranked[ri]
      if (pi === hoveredPath) continue
      const isOther = hoveredPath !== null
      ctx.strokeStyle = isOther ? '#dee2e6' : rankColor(ri, totalRanked)
      ctx.lineWidth = 1.2
      ctx.globalAlpha = isOther ? 0.3 : 1
      ctx.beginPath()
      for (let i = 0; i < p.length; i++) {
        const x = xScale(i), y = yScale(p[i])
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y)
      }
      ctx.stroke()
    }

    // Draw hovered path on top
    if (hoveredPath !== null) {
      const hovRank = ranked.findIndex(d => d.i === hoveredPath)
      const entry = ranked[hovRank]
      if (entry && entry.p.length >= 2) {
        ctx.strokeStyle = rankColor(hovRank, totalRanked)
        ctx.lineWidth = 2.5
        ctx.globalAlpha = 1
        ctx.beginPath()
        for (let i = 0; i < entry.p.length; i++) {
          const x = xScale(i), y = yScale(entry.p[i])
          if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y)
        }
        ctx.stroke()
      }
    }
    ctx.globalAlpha = 1

  }, [result, resultTab, pathDims, hoveredPath])

  // Navigate chart to a specific trade (uses visible trades for current ticker)
  const navigateToTrade = useCallback((idx: number) => {
    if (!result || !chartInstanceRef.current) return
    const currentTkr = chartTickers.length > 0 ? chartTickers[Math.min(chartTickerIdx, chartTickers.length - 1)] : null
    const visibleTrades = currentTkr ? result.trades.filter(t => t.ticker === currentTkr) : result.trades
    if (visibleTrades.length === 0) return
    const clamped = Math.max(0, Math.min(idx, visibleTrades.length - 1))
    setTradeIndex(clamped)
    const t = visibleTrades[clamped]
    const entryTime = new Date(t.entry_date + 'T00:00:00Z').getTime()
    const from = new Date(entryTime - 90 * 86400000)
    const to = new Date(entryTime + 270 * 86400000)
    chartInstanceRef.current.timeScale().setVisibleRange({
      from: (Math.floor(from.getTime() / 1000)) as any,
      to: (Math.floor(to.getTime() / 1000)) as any,
    })
  }, [result, chartTickers, chartTickerIdx])

  // Price chart with trade markers + indicator panes (all created upfront)
  useEffect(() => {
    if (resultTab !== 'chart' || !result || !priceChartRef.current) return

    // Save current range before teardown
    const priceChart = btChartsRef.current.price
    if (priceChart) {
      const range = priceChart.timeScale().getVisibleLogicalRange()
      if (range) btSavedRangeRef.current = { from: range.from, to: range.to }
    }

    // Clean up all previous charts
    for (const c of Object.values(btChartsRef.current)) {
      try { c?.remove() } catch {}
    }
    btChartsRef.current = {}
    btSeriesRef.current = {}
    chartInstanceRef.current = null

    const isLong = LONG_SIGNALS.has(entrySignal)
    const isMulti = chartTickers.length > 0
    const currentTicker = isMulti ? chartTickers[Math.min(chartTickerIdx, chartTickers.length - 1)] : null

    const endpoint = isMulti
      ? `tickers/${encodeURIComponent(currentTicker!)}`
      : selectedTargetType === 'basket'
        ? `baskets/${encodeURIComponent(selectedTarget)}`
        : `tickers/${encodeURIComponent(selectedTarget)}`

    const visibleTrades = isMulti
      ? result.trades.filter(t => t.ticker === currentTicker)
      : result.trades

    axios.get(`${apiBase}/${endpoint}`).then(res => {
      if (!priceChartRef.current) return
      const rawData: any[] = res.data.chart_data || []
      if (rawData.length === 0) return

      const sorted = [...rawData].sort((a, b) => String(a.Date).localeCompare(String(b.Date)))
      const times = sorted.map(d => parseTime(d.Date))
      const ohlc = sorted
        .map((d, i) => ({ time: times[i], open: Number(d.Open), high: Number(d.High), low: Number(d.Low), close: Number(d.Close) }))
        .filter(d => !isNaN(d.open))

      const chartOpts = {
        layout: { background: { type: ColorType.Solid as const, color: '#fdf6e3' }, textColor: '#586e75' },
        grid: { vertLines: { visible: false }, horzLines: { visible: false } },
        crosshair: { mode: CrosshairMode.Normal },
        rightPriceScale: { minimumWidth: 70 },
        timeScale: { borderColor: '#93a1a1', timeVisible: true, rightOffset: 10 },
        handleScroll: { mouseWheel: true, pressedMouseMove: true },
        handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true },
      }

      // Create ALL 5 charts upfront
      const pc = createChart(priceChartRef.current!, chartOpts)
      const vc = volChartRef.current     ? createChart(volChartRef.current,     { ...chartOpts, timeScale: { ...chartOpts.timeScale, timeVisible: false } }) : null
      const bc = breadthChartRef.current ? createChart(breadthChartRef.current, { ...chartOpts, timeScale: { ...chartOpts.timeScale, timeVisible: false } }) : null
      const boc = breakoutChartRef.current ? createChart(breakoutChartRef.current, { ...chartOpts, timeScale: { ...chartOpts.timeScale, timeVisible: false } }) : null
      const cc = corrChartRef.current    ? createChart(corrChartRef.current,    { ...chartOpts, timeScale: { ...chartOpts.timeScale, timeVisible: false } }) : null
      const rvc = rvChartRef.current    ? createChart(rvChartRef.current,     { ...chartOpts, timeScale: { ...chartOpts.timeScale, timeVisible: false } }) : null

      btChartsRef.current = { price: pc, volume: vc, breadth: bc, breakout: boc, correlation: cc, rv: rvc }
      chartInstanceRef.current = pc

      // Price pane
      const candleSeries = pc.addCandlestickSeries({
        upColor: 'transparent', downColor: '#586e75',
        borderVisible: true,
        borderUpColor: '#586e75', borderDownColor: '#586e75',
        wickUpColor: '#586e75', wickDownColor: '#586e75',
      })
      candleSeries.setData(ohlc)
      btSeriesRef.current.price = candleSeries

      // Pivots
      if (showPivots) {
        const rs = pc.addLineSeries({ color: 'transparent', priceLineVisible: false, crosshairMarkerVisible: false, lastValueVisible: false, pointMarkersVisible: true, pointMarkersRadius: 1, lineVisible: false } as any)
        const ss = pc.addLineSeries({ color: 'transparent', priceLineVisible: false, crosshairMarkerVisible: false, lastValueVisible: false, pointMarkersVisible: true, pointMarkersRadius: 1, lineVisible: false } as any)
        const rp: any[] = [], sp: any[] = []
        sorted.forEach((d, i) => {
          if (d.Resistance_Pivot != null && !d.Trend) rp.push({ time: times[i], value: Number(d.Resistance_Pivot), color: 'rgb(255, 50, 150)' })
          if (d.Support_Pivot != null && d.Trend) sp.push({ time: times[i], value: Number(d.Support_Pivot), color: 'rgb(50, 50, 255)' })
        })
        rs.setData(rp)
        ss.setData(sp)
      }

      // Targets
      if (showTargets) {
        const uT = sorted.filter(d => d.Upper_Target != null).map(d => ({ time: parseTime(d.Date), value: Number(d.Upper_Target) }))
        const lT = sorted.filter(d => d.Lower_Target != null).map(d => ({ time: parseTime(d.Date), value: Number(d.Lower_Target) }))
        if (uT.length) pc.addLineSeries({ color: 'rgb(50, 50, 255)', lineWidth: 2, priceLineVisible: false, crosshairMarkerVisible: false }).setData(uT)
        if (lT.length) pc.addLineSeries({ color: 'rgb(255, 50, 150)', lineWidth: 2, priceLineVisible: false, crosshairMarkerVisible: false }).setData(lT)
      }

      // Trade lines + markers
      for (const t of visibleTrades) {
        if (t.skipped || t.entry_price == null || t.exit_price == null) continue
        const isWinner = (t.change ?? 0) > 0
        const color = isWinner ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)'
        pc.addLineSeries({ color, lineWidth: 3, lineStyle: LineStyle.Dashed, priceLineVisible: false, crosshairMarkerVisible: false, lastValueVisible: false })
          .setData([{ time: parseTime(t.entry_date), value: t.entry_price }, { time: parseTime(t.exit_date), value: t.exit_price }])
      }
      const markers: any[] = []
      for (const t of visibleTrades) {
        if (t.skipped) continue
        markers.push({ time: parseTime(t.entry_date), position: isLong ? 'belowBar' : 'aboveBar', color: isLong ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)', shape: isLong ? 'arrowUp' : 'arrowDown', size: 2, text: '' })
        markers.push({ time: parseTime(t.exit_date), position: isLong ? 'aboveBar' : 'belowBar', color: isLong ? 'rgb(255, 50, 150)' : 'rgb(50, 50, 255)', shape: isLong ? 'arrowDown' : 'arrowUp', size: 2, text: '' })
      }
      if (result.skipped_entries) {
        const currentTkr = isMulti ? currentTicker : null
        for (const s of result.skipped_entries) {
          if (currentTkr && s.ticker !== currentTkr) continue
          markers.push({
            time: parseTime(s.entry_date), position: isLong ? 'belowBar' : 'aboveBar',
            color: '#93a1a1', shape: isLong ? 'arrowUp' : 'arrowDown', size: 1, text: 'skip',
          })
        }
      }
      markers.sort((a, b) => a.time - b.time)
      candleSeries.setMarkers(markers)

      // Alignment data for indicator panes
      const alignData = ohlc.map(d => ({ time: d.time, value: d.close }))
      const addAlign = (ic: IChartApi) => {
        ic.addLineSeries({ color: 'rgba(0,0,0,0)', priceLineVisible: false, crosshairMarkerVisible: false, lastValueVisible: false, priceScaleId: '__align__' }).setData(alignData)
        ic.priceScale('__align__').applyOptions({ visible: false })
      }

      // Volume pane
      if (vc) {
        addAlign(vc)
        const vs = vc.addHistogramSeries({ color: 'rgba(147, 161, 161, 0.5)', priceFormat: { type: 'volume' } })
        vs.setData(sorted.filter(d => d.Volume != null).map((d, i) => ({ time: times[i], value: Number(d.Volume) })))
        btSeriesRef.current.volume = vs
      }

      // Breadth pane
      if (bc) {
        addAlign(bc)
        const bs = bc.addLineSeries({ color: 'rgb(50, 50, 255)', lineWidth: 2, title: 'Breadth %' })
        bs.setData(sorted.filter(d => d.Uptrend_Pct != null).map((d, i) => ({ time: times[i], value: Number(d.Uptrend_Pct) })))
        btSeriesRef.current.breadth = bs
      }

      // Breakout pane
      if (boc) {
        addAlign(boc)
        const bos = boc.addLineSeries({ color: 'rgb(255, 50, 150)', lineWidth: 2, title: 'Breakout %' })
        bos.setData(sorted.filter(d => d.Breakout_Pct != null).map((d, i) => ({ time: times[i], value: Number(d.Breakout_Pct) })))
        btSeriesRef.current.breakout = bos
      }

      // Correlation pane
      if (cc) {
        addAlign(cc)
        const cs = cc.addLineSeries({ color: '#586e75', lineWidth: 2, title: 'Correlation %' })
        cs.setData(sorted.filter(d => d.Correlation_Pct != null).map((d, i) => ({ time: times[i], value: Number(d.Correlation_Pct) })))
        btSeriesRef.current.correlation = cs
      }

      // RV pane
      const ANNUALIZE = Math.sqrt(252)
      if (rvc) {
        addAlign(rvc)
        const rvs = rvc.addLineSeries({ color: '#b58900', lineWidth: 2, title: 'RV %', priceFormat: { type: 'custom', formatter: (v: number) => v.toFixed(1) + '%' } })
        rvs.setData(sorted.filter(d => d.RV_EMA != null).map((d, i) => ({ time: times[i], value: Number(d.RV_EMA) * ANNUALIZE * 100 })))
        btSeriesRef.current.rv = rvs
      }

      // Sync time scales + crosshairs across all charts
      const chartEntries: [string, IChartApi][] = Object.entries(btChartsRef.current).filter((e): e is [string, IChartApi] => e[1] != null)
      let rangeSyncing = false
      let crosshairSyncing = false
      const refMap: Record<string, React.RefObject<HTMLDivElement | null>> = { price: priceChartRef, volume: volChartRef, breadth: breadthChartRef, breakout: breakoutChartRef, correlation: corrChartRef, rv: rvChartRef }
      const seriesDataMaps: Record<string, Map<any, number>> = {}
      const priceMap = new Map<any, number>()
      ohlc.forEach(d => priceMap.set(d.time, d.close))
      seriesDataMaps.price = priceMap
      sorted.forEach((d, i) => {
        if (d.Volume != null) { if (!seriesDataMaps.volume) seriesDataMaps.volume = new Map(); seriesDataMaps.volume.set(times[i], Number(d.Volume)) }
        if (d.Uptrend_Pct != null) { if (!seriesDataMaps.breadth) seriesDataMaps.breadth = new Map(); seriesDataMaps.breadth.set(times[i], Number(d.Uptrend_Pct)) }
        if (d.Breakout_Pct != null) { if (!seriesDataMaps.breakout) seriesDataMaps.breakout = new Map(); seriesDataMaps.breakout.set(times[i], Number(d.Breakout_Pct)) }
        if (d.Correlation_Pct != null) { if (!seriesDataMaps.correlation) seriesDataMaps.correlation = new Map(); seriesDataMaps.correlation.set(times[i], Number(d.Correlation_Pct)) }
        if (d.RV_EMA != null) { if (!seriesDataMaps.rv) seriesDataMaps.rv = new Map(); seriesDataMaps.rv.set(times[i], Number(d.RV_EMA) * ANNUALIZE * 100) }
      })

      chartEntries.forEach(([, source]) => {
        source.timeScale().subscribeVisibleLogicalRangeChange(r => {
          if (rangeSyncing || !r) return
          rangeSyncing = true
          chartEntries.forEach(([, target]) => { if (target !== source) target.timeScale().setVisibleLogicalRange(r) })
          rangeSyncing = false
        })
        source.subscribeCrosshairMove(p => {
          if (crosshairSyncing) return
          crosshairSyncing = true
          try {
            chartEntries.forEach(([otherId, target]) => {
              if (target === source) return
              const container = refMap[otherId]
              if (container?.current && container.current.clientHeight === 0) return
              const s = btSeriesRef.current[otherId]
              if (!s) return
              if (!p.time) { target.clearCrosshairPosition() }
              else {
                const val = seriesDataMaps[otherId]?.get(p.time)
                if (val !== undefined) target.setCrosshairPosition(val, p.time as any, s)
              }
            })
          } finally { crosshairSyncing = false }
        })
      })

      // Restore saved range or default 1yr around first trade
      const initRange = btSavedRangeRef.current
      btSavedRangeRef.current = null
      if (initRange) {
        chartEntries.forEach(([, c]) => c.timeScale().setVisibleLogicalRange(initRange))
      } else if (visibleTrades.length > 0) {
        const firstEntry = new Date(visibleTrades[0].entry_date + 'T00:00:00Z').getTime()
        const from = new Date(firstEntry - 90 * 86400000)
        const to = new Date(firstEntry + 270 * 86400000)
        pc.timeScale().setVisibleRange({ from: (Math.floor(from.getTime() / 1000)) as any, to: (Math.floor(to.getTime() / 1000)) as any })
      } else {
        pc.timeScale().fitContent()
      }
    })

    return () => {
      const priceC = btChartsRef.current.price
      if (priceC) {
        const range = priceC.timeScale().getVisibleLogicalRange()
        if (range) btSavedRangeRef.current = { from: range.from, to: range.to }
      }
      for (const c of Object.values(btChartsRef.current)) {
        try { c?.remove() } catch {}
      }
      btChartsRef.current = {}
      btSeriesRef.current = {}
      chartInstanceRef.current = null
    }
  }, [resultTab, result, selectedTarget, selectedTargetType, entrySignal, apiBase, showPivots, showTargets, chartTickers, chartTickerIdx])

  // Resize ALL charts when visibility toggles change (flex layout needs a frame to settle)
  useEffect(() => {
    const handle = requestAnimationFrame(() => {
      const refMap: Record<string, React.RefObject<HTMLDivElement | null>> = {
        price: priceChartRef, volume: volChartRef, breadth: breadthChartRef, breakout: breakoutChartRef, correlation: corrChartRef, rv: rvChartRef,
      }
      const priceChart = btChartsRef.current.price
      const currentRange = priceChart?.timeScale().getVisibleLogicalRange()

      for (const [id, chart] of Object.entries(btChartsRef.current)) {
        if (!chart) continue
        const ref = refMap[id]
        if (ref?.current && ref.current.clientHeight > 0) {
          chart.applyOptions({ width: ref.current.clientWidth, height: ref.current.clientHeight })
          if (id !== 'price' && currentRange) {
            chart.timeScale().setVisibleLogicalRange(currentRange)
          }
        }
      }
    })
    return () => cancelAnimationFrame(handle)
  }, [showVolume, showBreadth, showBreakout, showCorrelation, showRV])

  // Config mode
  if (!showResults) {
    if (multiMode) {
      return <MultiBacktestPanel apiBase={apiBase} onClose={() => setMultiMode(false)} />
    }
    return (
      <div className="backtest-panel">
        <div className="backtest-config">
          <div className="backtest-section">
            <div className="backtest-pos-presets">
              <button className="backtest-pos-preset wide active">Single Leg</button>
              <button className="backtest-pos-preset wide" onClick={() => setMultiMode(true)}>Multi-Leg</button>
            </div>
          </div>

          <div className="backtest-section">
            <label className="backtest-label">Target</label>
            <select className="backtest-select" value={selectedTarget}
              onChange={e => {
                setSelectedTarget(e.target.value)
                setSelectedTargetType('basket')
              }}>
              {/* Current sidebar selection */}
              {selectedTargetType === 'ticker' && <option value={target}>{target} (ticker)</option>}
              {Object.entries(allBaskets).map(([group, names]) => (
                <optgroup key={group} label={group}>
                  {Array.isArray(names) && names.map(n => <option key={n} value={n}>{n.replace(/_/g, ' ')}</option>)}
                </optgroup>
              ))}
            </select>
          </div>

          <div className="backtest-section">
            <label className="backtest-label">Entry Signal</label>
            <select className="backtest-select" value={entrySignal} onChange={e => setEntrySignal(e.target.value)}>
              {ENTRY_SIGNALS.map(s => <option key={s} value={s}>{s.replace(/_/g, ' ')}</option>)}
            </select>
            <span className="backtest-hint">Exit: {EXIT_MAP[entrySignal]?.replace(/_/g, ' ')}</span>
          </div>

          {selectedTargetType === 'basket' && (
            <div className="backtest-section">
              <label className="backtest-label">Trade Source</label>
              <div className="backtest-pos-presets">
                <button className={`backtest-pos-preset wide ${!useConstituents ? 'active' : ''}`}
                  onClick={() => setUseConstituents(false)}>Basket Signal</button>
                <button className={`backtest-pos-preset wide ${useConstituents ? 'active' : ''}`}
                  onClick={() => setUseConstituents(true)}>Constituent Tickers</button>
              </div>
            </div>
          )}

          <div className="backtest-section">
            <label className="backtest-label">Position Sizing</label>
            <div className="backtest-filter-row">
              <span className="backtest-hint">Equity $</span>
              <input type="number" className="backtest-input" value={initialEquity}
                onChange={e => setInitialEquity(Number(e.target.value))} style={{ width: 100 }} />
              <span className="backtest-hint">Size %</span>
              <input type="number" className="backtest-input" value={positionSize}
                onChange={e => setPositionSize(Number(e.target.value))} style={{ width: 60 }} />
              <span className="backtest-hint">Max Lev %</span>
              <input type="number" className="backtest-input" value={maxLeverage}
                onChange={e => setMaxLeverage(Number(e.target.value))} style={{ width: 60 }} />
            </div>
            <div className="backtest-pos-presets">
              <span className="backtest-preset-label">Size:</span>
              {POS_PRESETS.map(p => (
                <button key={p}
                  className={`backtest-pos-preset ${positionSize === p ? 'active' : ''}`}
                  onClick={() => setPositionSize(p)}>{p}%</button>
              ))}
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
            <label className="backtest-label">Regime Filters</label>
            {filters.map((f, i) => {
              const isBool = BOOL_METRICS.includes(f.metric)
              return (
                <div key={i} className="backtest-filter-row">
                  <select className="backtest-select" value={f.metric}
                    onChange={e => updateFilter(i, 'metric', e.target.value)}>
                    <optgroup label="Percentage">
                      {PCT_METRICS.map(m => <option key={m} value={m}>{m}</option>)}
                    </optgroup>
                    <optgroup label="Boolean">
                      {BOOL_METRICS.map(m => <option key={m} value={m}>{m}</option>)}
                    </optgroup>
                  </select>
                  <select className="backtest-select" value={f.condition}
                    onChange={e => updateFilter(i, 'condition', e.target.value)}>
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
                      onChange={e => updateFilter(i, 'value', e.target.value)} />
                  )}
                  {targetType === 'ticker' && (
                    <select className="backtest-select" value={f.source}
                      onChange={e => updateFilter(i, 'source', e.target.value)}>
                      <option value="self">Self</option>
                      {availableBaskets.map(b => <option key={b} value={b}>{b.replace(/_/g, ' ')}</option>)}
                    </select>
                  )}
                  <button className="backtest-remove-btn" onClick={() => removeFilter(i)}>X</button>
                </div>
              )
            })}
            <button className="control-btn" onClick={addFilter}>+ Add Filter</button>
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

          <button className="control-btn primary" onClick={runBacktest} disabled={loading}>
            {loading ? 'Running...' : 'Run Backtest'}
          </button>
          {error && <div className="backtest-error">{error}</div>}
        </div>
      </div>
    )
  }

  // Results mode
  const stats = result!.stats

  const isMultiTicker = result?.trades.some(t => t.ticker) ?? false
  const tradeColumns: { key: TradeSortCol; label: string }[] = [
    ...(isMultiTicker ? [{ key: 'ticker' as TradeSortCol, label: 'Ticker' }] : []),
    { key: 'entry_date', label: 'Entry Date' },
    { key: 'exit_date', label: 'Exit Date' },
    { key: 'entry_price', label: 'Entry $' },
    { key: 'exit_price', label: 'Exit $' },
    { key: 'change', label: 'Change%' },
    { key: 'mfe', label: 'MFE' },
    { key: 'mae', label: 'MAE' },
    { key: 'bars_held', label: 'Bars' },
    { key: 'regime_pass', label: 'Filter' },
  ]

  const statRows: { label: string; filt: string; unfilt: string; filtColor?: string; unfiltColor?: string }[] = [
    { label: 'Trades', filt: String(stats.filtered.trades), unfilt: String(stats.unfiltered.trades) },
    { label: 'Win Rate', filt: pctFmt(stats.filtered.win_rate), unfilt: pctFmt(stats.unfiltered.win_rate) },
    { label: 'Avg Winner', filt: pctFmt(stats.filtered.avg_winner), unfilt: pctFmt(stats.unfiltered.avg_winner), filtColor: 'rgb(50,50,255)', unfiltColor: 'rgb(50,50,255)' },
    { label: 'Avg Loser', filt: pctFmt(stats.filtered.avg_loser), unfilt: pctFmt(stats.unfiltered.avg_loser), filtColor: 'rgb(255,50,150)', unfiltColor: 'rgb(255,50,150)' },
    { label: 'EV', filt: pctFmt(stats.filtered.ev), unfilt: pctFmt(stats.unfiltered.ev),
      filtColor: stats.filtered.ev >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)',
      unfiltColor: stats.unfiltered.ev >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)' },
    { label: 'PF', filt: stats.filtered.profit_factor.toFixed(2), unfilt: stats.unfiltered.profit_factor.toFixed(2) },
    { label: 'Max DD', filt: pctFmt(stats.filtered.max_dd), unfilt: pctFmt(stats.unfiltered.max_dd), filtColor: 'rgb(255,50,150)', unfiltColor: 'rgb(255,50,150)' },
    { label: 'Avg Bars', filt: stats.filtered.avg_bars.toFixed(1), unfilt: stats.unfiltered.avg_bars.toFixed(1) },
  ]

  return (
    <div className="backtest-panel">
      <div className="backtest-results-header">
        <button className="control-btn" onClick={() => setShowResults(false)}>Back</button>
        <span className="backtest-results-title">
          {target.replace(/_/g, ' ')}{isMultiTicker ? ' Tickers' : ''} -- {entrySignal.replace(/_/g, ' ')} Backtest
        </span>
        {result?.date_range && (
          <span className="backtest-hint">
            {result.date_range.min} to {result.date_range.max}
          </span>
        )}
      </div>

      {result?.blew_up && (
        <div className="backtest-blowup">
          BLEW UP on {result.blew_up.date} ({result.blew_up.equity} equity went to zero)
        </div>
      )}

      <div className="backtest-body">
        <div className="backtest-main">
          <div className="summary-tabs">
            <button className={`summary-tab ${resultTab === 'equity' ? 'active' : ''}`} onClick={() => setResultTab('equity')}>Equity</button>
            <button className={`summary-tab ${resultTab === 'stats' ? 'active' : ''}`} onClick={() => setResultTab('stats')}>Stats</button>
            <button className={`summary-tab ${resultTab === 'distribution' ? 'active' : ''}`} onClick={() => setResultTab('distribution')}>Distribution</button>
            <button className={`summary-tab ${resultTab === 'chart' ? 'active' : ''}`} onClick={() => setResultTab('chart')}>Chart</button>
            <button className={`summary-tab ${resultTab === 'path' ? 'active' : ''}`} onClick={() => setResultTab('path')}>Path</button>
            <button className={`summary-tab ${resultTab === 'trades' ? 'active' : ''}`} onClick={() => setResultTab('trades')}>Trades</button>
          </div>

          <div className="backtest-content">
            {resultTab === 'equity' ? (
              <>
                <div className="backtest-eq-toggles">
                  <button
                    style={showFiltered
                      ? { background: '#8BC34A', borderColor: '#8BC34A', color: '#fff', border: '1px solid', borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontWeight: 600, fontSize: 11 }
                      : { background: 'transparent', borderColor: '#ccc', color: '#657b83', border: '1px solid', borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: 11 }}
                    onClick={() => setShowFiltered(v => !v)}>Filtered</button>
                  {(['Breakout', 'Up_Rot', 'BTFD'] as const).map(sig => (
                    <button key={sig}
                      style={showBenchmark[sig]
                        ? { background: BENCHMARK_COLORS[sig], borderColor: BENCHMARK_COLORS[sig], color: '#fff', border: '1px solid', borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontWeight: 600, fontSize: 11 }
                        : { background: 'transparent', borderColor: '#ccc', color: '#657b83', border: '1px solid', borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: 11 }}
                      onClick={() => setShowBenchmark(prev => ({ ...prev, [sig]: !prev[sig] }))}>{sig.replace('_', ' ')}</button>
                  ))}
                  {(['Breakdown', 'Down_Rot', 'STFR'] as const).map(sig => (
                    <button key={sig}
                      style={showBenchmark[sig]
                        ? { background: BENCHMARK_COLORS[sig], borderColor: BENCHMARK_COLORS[sig], color: '#fff', border: '1px solid', borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontWeight: 600, fontSize: 11 }
                        : { background: 'transparent', borderColor: '#ccc', color: '#657b83', border: '1px solid', borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: 11 }}
                      onClick={() => setShowBenchmark(prev => ({ ...prev, [sig]: !prev[sig] }))}>{sig.replace('_', ' ')}</button>
                  ))}
                  <button
                    style={showBuyHold
                      ? { background: '#93a1a1', borderColor: '#93a1a1', color: '#fff', border: '1px solid', borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontWeight: 600, fontSize: 11 }
                      : { background: 'transparent', borderColor: '#ccc', color: '#657b83', border: '1px solid', borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: 11 }}
                    onClick={() => setShowBuyHold(v => !v)}>Buy &amp; Hold</button>
                  <button
                    style={showConstituents
                      ? { background: '#d33682', borderColor: '#d33682', color: '#fff', border: '1px solid', borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontWeight: 600, fontSize: 11 }
                      : { background: 'transparent', borderColor: '#ccc', color: '#657b83', border: '1px solid', borderRadius: 4, padding: '2px 8px', cursor: 'pointer', fontSize: 11 }}
                    onClick={() => { setShowConstituents(v => !v); setEqPinnedIdx(null); setEqHoverIdx(null) }}>Constituents</button>
                  <span className="eq-range-spacer" />
                  <div className="eq-range-btns">
                    {(['1Y', '3Y', '5Y', 'YTD', 'All'] as const).map(p => (
                      <button key={p} className="control-btn eq-range-btn" onClick={() => setEqPreset(p)}>{p}</button>
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
                    const date = result?.equity_curve.dates[activeIdx]
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
                              <span className="ticker">{isMultiTicker ? 'Ticker' : 'Entry'}</span>
                              <span className="weight">Weight</span>
                              <span className="ret">Return</span>
                              <span className="contrib">Contrib</span>
                            </div>
                            {snapshot.positions.map((p, i) => (
                              <div key={i} className="candle-detail-row">
                                <span className="ticker">{isMultiTicker ? p.ticker : p.entry_date?.slice(5)}</span>
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
                {result && (
                  <RangeScrollbar
                    total={result.equity_curve.dates.length}
                    start={eqViewRef.current.start}
                    end={eqViewRef.current.end}
                    onChange={(s, e) => { eqViewRef.current = { start: s, end: e }; setEqViewVersion(v => v + 1) }}
                  />
                )}
              </>
            ) : resultTab === 'chart' ? (
              <>
              <div className="header-toggles" style={{ padding: '4px 8px' }}>
                <label className="overlay-checkbox"><input type="checkbox" checked={showPivots} onChange={e => setShowPivots(e.target.checked)} /> Pivots</label>
                <label className="overlay-checkbox"><input type="checkbox" checked={showTargets} onChange={e => setShowTargets(e.target.checked)} /> Targets</label>
                {(chartTickers.length > 0 || targetType === 'ticker') ? (
                  <label className="overlay-checkbox"><input type="checkbox" checked={showVolume} onChange={e => setShowVolume(e.target.checked)} /> Volume</label>
                ) : (
                  <>
                    <label className="overlay-checkbox"><input type="checkbox" checked={showBreadth} onChange={e => setShowBreadth(e.target.checked)} /> Breadth%</label>
                    <label className="overlay-checkbox"><input type="checkbox" checked={showBreakout} onChange={e => setShowBreakout(e.target.checked)} /> Breakout%</label>
                    <label className="overlay-checkbox"><input type="checkbox" checked={showCorrelation} onChange={e => setShowCorrelation(e.target.checked)} /> Correlation%</label>
                  </>
                )}
                <label className="overlay-checkbox"><input type="checkbox" checked={showRV} onChange={e => setShowRV(e.target.checked)} /> RV</label>
                {result && result.trades.length > 0 && (
                  <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 6 }}>
                    {chartTickers.length > 0 && (
                      <>
                        <button className="backtest-nav-btn" onClick={() => { setChartTickerIdx(i => Math.max(0, i - 1)); setTradeIndex(0) }} disabled={chartTickerIdx <= 0}>&larr;</button>
                        <select className="backtest-nav-select" value={chartTickerIdx}
                          onChange={e => { setChartTickerIdx(Number(e.target.value)); setTradeIndex(0) }}>
                          {chartTickers.map((tkr, i) => <option key={tkr} value={i}>{tkr}</option>)}
                        </select>
                        <button className="backtest-nav-btn" onClick={() => { setChartTickerIdx(i => Math.min(chartTickers.length - 1, i + 1)); setTradeIndex(0) }} disabled={chartTickerIdx >= chartTickers.length - 1}>&rarr;</button>
                        <span style={{ borderLeft: '1px solid var(--border-color)', height: 16, margin: '0 4px' }} />
                      </>
                    )}
                    {(() => {
                      const currentTkr = chartTickers.length > 0 ? chartTickers[Math.min(chartTickerIdx, chartTickers.length - 1)] : null
                      const tickerTrades = currentTkr ? result.trades.filter(t => t.ticker === currentTkr) : result.trades
                      const count = tickerTrades.length
                      return (
                        <>
                          <button className="backtest-nav-btn" onClick={() => navigateToTrade(tradeIndex - 1)} disabled={tradeIndex <= 0}>&larr;</button>
                          <select className="backtest-nav-select" value={tradeIndex}
                            onChange={e => navigateToTrade(Number(e.target.value))}>
                            {tickerTrades.map((t, i) => <option key={i} value={i}>{t.entry_date}</option>)}
                          </select>
                          <button className="backtest-nav-btn" onClick={() => navigateToTrade(tradeIndex + 1)} disabled={tradeIndex >= count - 1}>&rarr;</button>
                        </>
                      )
                    })()}
                  </div>
                )}
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0 }}>
              <div className="backtest-chart" style={{ position: 'relative', flex: 1, minHeight: '100px' }}>
                <div ref={priceChartRef} style={{ width: '100%', height: '100%' }} />
              </div>
              {[
                { id: 'volume',      ref: volChartRef,     visible: showVolume && (chartTickers.length > 0 || targetType === 'ticker') },
                { id: 'breadth',     ref: breadthChartRef, visible: showBreadth && !(chartTickers.length > 0 || targetType === 'ticker') },
                { id: 'breakout',    ref: breakoutChartRef, visible: showBreakout && !(chartTickers.length > 0 || targetType === 'ticker') },
                { id: 'correlation', ref: corrChartRef,    visible: showCorrelation && !(chartTickers.length > 0 || targetType === 'ticker') },
                { id: 'rv',          ref: rvChartRef,     visible: showRV },
              ].map(pane => (
                <div key={pane.id}>
                  {pane.visible && <div style={{ height: '4px', background: '#93a1a1', flexShrink: 0 }} />}
                  <div ref={pane.ref} style={{ height: pane.visible ? '120px' : 0, flexShrink: 0, overflow: 'hidden' }} />
                </div>
              ))}
              </div>
              </>
            ) : resultTab === 'distribution' ? (
              <>
                <div className="backtest-hist-toggles">
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
            ) : resultTab === 'path' ? (
              <div className="backtest-path-container">
                <div className="backtest-chart" ref={pathContainerRef}>
                  <canvas ref={pathCanvasRef} style={{ display: 'block', width: '100%', height: '100%' }} />
                </div>
                <div className="backtest-path-legend">
                  {(() => {
                    if (!result) return null
                    const pathRankColor = (rank: number, total: number): string => {
                      if (total <= 1) return 'rgb(50, 50, 255)'
                      const t = rank / (total - 1)
                      if (t <= 0.5) {
                        const s = t * 2
                        return `rgb(${Math.round(50 + 102 * s)}, 50, ${Math.round(255 - 53 * s)})`
                      } else {
                        const s = (t - 0.5) * 2
                        return `rgb(${Math.round(152 + 103 * s)}, 50, ${Math.round(202 - 52 * s)})`
                      }
                    }
                    const items = result.trades.map((t, i) => ({
                      t, i, ret: (result.trade_paths[i] ?? []).slice(-1)[0] ?? 0
                    }))
                    // Color rank always by return (best=blue, worst=pink)
                    const colorRanked = [...items].sort((a, b) => b.ret - a.ret)
                    const colorMap = new Map(colorRanked.map((d, rank) => [d.i, pathRankColor(rank, colorRanked.length)]))
                    // Sort by selected column
                    const sorted = [...items].sort((a, b) => {
                      let cmp = 0
                      if (pathSortCol === 'ticker') cmp = (a.t.ticker ?? '').localeCompare(b.t.ticker ?? '')
                      else if (pathSortCol === 'date') cmp = a.t.entry_date.localeCompare(b.t.entry_date)
                      else cmp = a.ret - b.ret
                      return pathSortAsc ? cmp : -cmp
                    })
                    const isMulti = result.trades.some(t => t.ticker)
                    const toggleSort = (col: 'ticker' | 'date' | 'change') => {
                      if (pathSortCol === col) setPathSortAsc(v => !v)
                      else { setPathSortCol(col); setPathSortAsc(col === 'change' ? false : true) }
                    }
                    const arrow = (col: 'ticker' | 'date' | 'change') => pathSortCol === col ? (pathSortAsc ? ' \u25B2' : ' \u25BC') : ''
                    return (
                      <>
                        <div className="path-legend-header">
                          {isMulti && <span className="path-legend-col ticker" onClick={() => toggleSort('ticker')}>Ticker{arrow('ticker')}</span>}
                          <span className="path-legend-col date" onClick={() => toggleSort('date')}>Date{arrow('date')}</span>
                          <span className="path-legend-col change" onClick={() => toggleSort('change')}>Chg{arrow('change')}</span>
                        </div>
                        {sorted.map(({ t, i, ret }) => (
                          <div key={i}
                            className={`path-legend-row ${hoveredPath === i ? 'highlighted' : ''}`}
                            style={{ color: colorMap.get(i) }}
                            onMouseEnter={() => setHoveredPath(i)}
                            onMouseLeave={() => setHoveredPath(null)}>
                            {isMulti && <span className="path-legend-col ticker">{t.ticker}</span>}
                            <span className="path-legend-col date">{t.entry_date.slice(2)}</span>
                            <span className="path-legend-col change">{(ret * 100).toFixed(1)}%</span>
                          </div>
                        ))}
                      </>
                    )
                  })()}
                </div>
              </div>
            ) : resultTab === 'stats' ? (
              <div style={{ padding: 16, overflow: 'auto' }}>
                <table className="backtest-stats-table" style={{ fontSize: 13 }}>
                  <thead>
                    <tr>
                      <th className="backtest-stats-th" style={{ fontSize: 11, padding: '4px 12px' }}></th>
                      <th className="backtest-stats-th" style={{ fontSize: 11, padding: '4px 12px' }}>Filtered</th>
                      <th className="backtest-stats-th" style={{ fontSize: 11, padding: '4px 12px' }}>All</th>
                    </tr>
                  </thead>
                  <tbody>
                    {statRows.map(r => (
                      <tr key={r.label}>
                        <td className="backtest-stats-td label" style={{ padding: '4px 12px' }}>{r.label}</td>
                        <td className="backtest-stats-td" style={{ color: r.filtColor, padding: '4px 12px' }}><b>{r.filt}</b></td>
                        <td className="backtest-stats-td" style={{ color: r.unfiltColor, padding: '4px 12px' }}><b>{r.unfilt}</b></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="summary-table-wrapper">
                <table className="summary-table">
                  <thead>
                    <tr>
                      {tradeColumns.map(col => (
                        <th key={col.key} className="summary-th" onClick={() => handleSort(col.key)} style={{ cursor: 'pointer' }}>
                          {col.label} {sortCol === col.key ? (sortAsc ? '\u25B2' : '\u25BC') : ''}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sortedTrades.map((t, i) => {
                      const isSkipped = t.skipped === true
                      const isWinner = !isSkipped && t.change !== null && t.change > 0
                      const rowBg = isSkipped ? 'rgba(147, 161, 161, 0.12)' : isWinner ? 'rgba(50, 50, 255, 0.06)' : 'rgba(255, 50, 150, 0.06)'
                      return (
                        <tr key={i} style={{ backgroundColor: rowBg }}>
                          {isMultiTicker && <td className="summary-td" style={{ fontWeight: 'bold' }}>{t.ticker}</td>}
                          <td className="summary-td">{t.entry_date}</td>
                          <td className="summary-td">{isSkipped ? '--' : t.exit_date}</td>
                          <td className="summary-td">{t.entry_price !== null ? '$' + t.entry_price.toFixed(2) : '--'}</td>
                          <td className="summary-td">{isSkipped ? '--' : (t.exit_price !== null ? '$' + t.exit_price.toFixed(2) : '--')}</td>
                          <td className="summary-td" style={{ color: isSkipped ? '#93a1a1' : isWinner ? 'rgb(50,50,255)' : 'rgb(255,50,150)', fontWeight: 'bold' }}>
                            {isSkipped ? <span style={{ background: '#93a1a1', color: '#fff', padding: '1px 4px', borderRadius: 3, fontSize: 9 }}>SKIP</span> : pctFmt(t.change)}
                          </td>
                          <td className="summary-td">{isSkipped ? '--' : pctFmt(t.mfe)}</td>
                          <td className="summary-td">{isSkipped ? '--' : pctFmt(t.mae)}</td>
                          <td className="summary-td">{isSkipped ? '--' : t.bars_held}</td>
                          <td className="summary-td" style={{ color: t.regime_pass ? 'rgb(50,50,255)' : 'rgb(255,50,150)', fontWeight: 'bold' }}>
                            {t.regime_pass ? '\u2713' : '\u2717'}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
                {sortedTrades.length === 0 && <div className="summary-empty">No trades found</div>}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
