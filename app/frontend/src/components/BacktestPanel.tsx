import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import { RangeScrollbar } from './RangeScrollbar'

/* ── Interfaces ────────────────────────────────────────────────────── */

interface BacktestFilter {
  metric: string
  condition: string
  value: number | string
  source: string
  lookback: number
}

interface LegConfig {
  target: string
  targetType: 'basket' | 'basket_tickers' | 'ticker' | 'etf'
  entrySignal: string
  exitSignal: string | null
  stopSignal: string | null
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
  targetType?: 'basket' | 'ticker' | 'etf'
  exportTrigger?: number
}

/* ── Constants ─────────────────────────────────────────────────────── */

const ENTRY_SIGNALS = ['Breakout', 'Breakdown', 'Up_Rot', 'Down_Rot', 'BTFD', 'STFR', 'Long', 'Short']
const EXIT_SIGNAL_OPTIONS = ['Up_Rot', 'Down_Rot', 'Breakout', 'Breakdown', 'BTFD', 'STFR']
const DEFAULT_EXIT_MAP: Record<string, string> = {
  Up_Rot: 'Down_Rot', Down_Rot: 'Up_Rot',
  Breakout: 'Breakdown', Breakdown: 'Breakout',
  BTFD: 'Breakdown', STFR: 'Breakout',
  Long: 'End of Period', Short: 'End of Period',
}
const LEG_COLORS = ['#1565C0', '#C2185B', '#2E7D32', '#E65100', '#6A1B9A', '#00838F']
const METRIC_DISPLAY: Record<string, string> = {
  Is_Breakout_Sequence: 'Long Term Uptrend', Uptrend_Pct: 'Breadth %',
  Breakout_Pct: 'Breakout %', Correlation_Pct: 'Correlation %',
  RV_EMA: 'Realized Vol', Volume: 'Volume', Return: 'Return',
  // Basket-prefixed versions for constituents mode
  'basket:Uptrend_Pct': 'Basket Breadth %', 'basket:Breakout_Pct': 'Basket Breakout %',
  'basket:Correlation_Pct': 'Basket Correlation %', 'basket:RV_EMA': 'Basket Realized Vol',
  'basket:Return': 'Basket Return', 'basket:Is_Breakout_Sequence': 'Basket LT Uptrend',
}
const BASKET_STAT_METRICS = ['Uptrend_Pct', 'Breakout_Pct', 'Correlation_Pct', 'RV_EMA', 'Return']
const BASKET_BOOL_METRICS = ['Is_Breakout_Sequence']
const TICKER_STAT_METRICS = ['Volume', 'RV_EMA', 'Return']
const TICKER_BOOL_METRICS = ['Is_Breakout_Sequence']
// Prefixed versions for the Basket optgroup in constituents mode
const CONSTITUENTS_BASKET_STAT = BASKET_STAT_METRICS.map(m => `basket:${m}`)
const CONSTITUENTS_BASKET_BOOL = BASKET_BOOL_METRICS.map(m => `basket:${m}`)
function isBasketPrefixed(metric: string) { return metric.startsWith('basket:') }
function stripBasketPrefix(metric: string) { return metric.replace(/^basket:/, '') }
const LOOKBACK_PRESETS = [
  { label: '1M', value: 21 },
  { label: '3M', value: 63 },
  { label: '6M', value: 126 },
  { label: '1Y', value: 252 },
]
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

const BACKTEST_DIRECTION: Record<string, string> = {
  Breakout: 'long', Breakdown: 'short', Up_Rot: 'long', Down_Rot: 'short', BTFD: 'long', STFR: 'short',
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
  return { target: '', targetType: 'basket_tickers', entrySignal: 'Breakout', exitSignal: null, stopSignal: null, allocationPct: 100, positionSize: 25, filters: [] }
}

type ResultTab = 'equity' | 'stats' | 'distribution' | 'path' | 'trades' | 'multi_returns' | 'single_returns'

/* ── Backtest Returns View ─────────────────────────────────────────── */

const RETURNS_PRESETS = [
  { label: '1M', days: 30 }, { label: '3M', days: 90 }, { label: '6M', days: 182 },
  { label: 'YTD', days: -1 }, { label: '1Y', days: 365 }, { label: '3Y', days: 1095 },
  { label: '5Y', days: 1825 }, { label: 'ALL', days: 0 },
] as const

function BacktestReturnsView({ dates, series, viewMode = 'single', exportTrigger, exportLabel }: {
  dates: string[]
  series: { key: string; label: string; curve: number[]; color: string }[]
  viewMode?: 'single' | 'multi'
  exportTrigger?: number
  exportLabel?: string
}) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const [dims, setDims] = useState({ w: 800, h: 400 })
  const [selectedKey, setSelectedKey] = useState('strategy')
  const [barPeriod, setBarPeriod] = useState<'1D'|'1W'|'1M'|'1Q'|'1Y'>('1D')
  const [hoveredName, setHoveredName] = useState<string | null>(null)
  const [legendSortCol, setLegendSortCol] = useState<'name' | 'change'>('change')
  const [legendSortAsc, setLegendSortAsc] = useState(false)
  const [chartView, setChartView] = useState<'bar' | 'line'>('line')
  const [visibleKeys, setVisibleKeys] = useState<Set<string>>(new Set())
  // Init all visible on mount
  useEffect(() => { setVisibleKeys(new Set(series.map(s => s.key))) }, [series.length])
  const [activePreset, setActivePreset] = useState('ALL')
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)
  const [scrollUnit, setScrollUnit] = useState<'1D'|'1W'|'1M'|'1Y'>('1Y')

  const dateBoundsMin = dates.length > 0 ? dates[0] : ''
  const dateBoundsMax = dates.length > 0 ? dates[dates.length - 1] : ''

  // Init date range
  useEffect(() => {
    if (dates.length > 0) {
      setStartDate(dates[0])
      setEndDate(dates[dates.length - 1])
    }
  }, [dates])

  // ResizeObserver
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const obs = new ResizeObserver(entries => {
      const { width, height } = entries[0].contentRect
      if (width > 0 && height > 0) setDims({ w: width, h: height })
    })
    obs.observe(el)
    return () => obs.disconnect()
  }, [])

  // Compute period returns from equity curve
  const barData = useMemo(() => {
    const sel = series.find(s => s.key === selectedKey)
    if (!sel || dates.length === 0) return { dates: [] as string[], returns: [] as number[] }

    // Slice to date range
    let si = 0, ei = dates.length - 1
    for (let i = 0; i < dates.length; i++) { if (dates[i] >= startDate) { si = i; break } }
    for (let i = dates.length - 1; i >= 0; i--) { if (dates[i] <= endDate) { ei = i; break } }
    if (si > 0) si-- // anchor row
    const slicedDates = dates.slice(si, ei + 1)
    const slicedCurve = sel.curve.slice(si, ei + 1)
    if (slicedCurve.length < 2) return { dates: [] as string[], returns: [] as number[] }

    if (barPeriod === '1D') {
      const retDates: string[] = []
      const rets: number[] = []
      for (let i = 1; i < slicedCurve.length; i++) {
        const prev = slicedCurve[i - 1]
        const cur = slicedCurve[i]
        if (prev !== 0) {
          retDates.push(slicedDates[i])
          rets.push(cur / prev - 1)
        }
      }
      // Filter to only dates >= startDate (drop anchor)
      const out = retDates.reduce<{ dates: string[]; returns: number[] }>((acc, d, i) => {
        if (d >= startDate) { acc.dates.push(d); acc.returns.push(rets[i]) }
        return acc
      }, { dates: [], returns: [] })
      return out
    }

    // Aggregate into period buckets
    const pad = (n: number) => n < 10 ? '0' + n : '' + n
    const getPeriodKey = (dateStr: string) => {
      const [y, m, d] = dateStr.split('-').map(Number)
      if (barPeriod === '1W') {
        const dt = new Date(y, m - 1, d)
        const dow = dt.getDay()
        const mon = new Date(dt)
        mon.setDate(dt.getDate() - ((dow + 6) % 7))
        return `${mon.getFullYear()}-${pad(mon.getMonth() + 1)}-${pad(mon.getDate())}`
      }
      if (barPeriod === '1M') return `${y}-${pad(m)}`
      if (barPeriod === '1Q') return `${y}-Q${Math.ceil(m / 3)}`
      return `${y}` // 1Y
    }

    // Group by period, take first and last curve value in each period
    const periods: { key: string; lastDate: string; firstVal: number; lastVal: number }[] = []
    let curKey = ''
    let firstVal = 0
    let lastDate = ''
    for (let i = 0; i < slicedCurve.length; i++) {
      const pk = getPeriodKey(slicedDates[i])
      if (pk !== curKey) {
        if (curKey && i > 0) {
          periods.push({ key: curKey, lastDate, firstVal, lastVal: slicedCurve[i - 1] })
        }
        curKey = pk
        firstVal = slicedCurve[i]
      }
      lastDate = slicedDates[i]
    }
    if (curKey) periods.push({ key: curKey, lastDate, firstVal, lastVal: slicedCurve[slicedCurve.length - 1] })

    // Compute returns, skip first period (anchor)
    const retDates: string[] = []
    const rets: number[] = []
    for (let i = 1; i < periods.length; i++) {
      const prev = periods[i - 1].lastVal
      if (prev !== 0) {
        retDates.push(periods[i].lastDate)
        rets.push(periods[i].lastVal / prev - 1)
      }
    }
    return { dates: retDates.filter(d => d >= startDate), returns: rets.slice(rets.length - retDates.filter(d => d >= startDate).length) }
  }, [series, selectedKey, dates, startDate, endDate, barPeriod])

  // Multi-strat: rebased cumulative series within date range
  const multiData = useMemo(() => {
    if (viewMode !== 'multi' || dates.length === 0) return { dates: [] as string[], series: [] as { key: string; label: string; color: string; values: number[]; lastVal: number }[] }
    let si = 0, ei = dates.length - 1
    for (let i = 0; i < dates.length; i++) { if (dates[i] >= startDate) { si = i; break } }
    for (let i = dates.length - 1; i >= 0; i--) { if (dates[i] <= endDate) { ei = i; break } }
    if (si > ei) return { dates: [] as string[], series: [] as { key: string; label: string; color: string; values: number[]; lastVal: number }[] }
    const slicedDates = dates.slice(si, ei + 1)
    const out = series.map(s => {
      const sliced = s.curve.slice(si, ei + 1)
      const base = sliced[0] || 1
      const rebased = sliced.map(v => v / base - 1)
      return { key: s.key, label: s.label, color: s.color, values: rebased, lastVal: rebased[rebased.length - 1] || 0 }
    })
    return { dates: slicedDates, series: out }
  }, [viewMode, series, dates, startDate, endDate])

  // Filter multiData series by visibility
  const visibleMultiSeries = useMemo(() => multiData.series.filter(s => visibleKeys.has(s.key)), [multiData.series, visibleKeys])

  // Multi-bar: period returns for each visible strategy (for bar chart)
  const multiBarData = useMemo(() => {
    if (viewMode !== 'multi' || dates.length === 0) return [] as { key: string; label: string; color: string; ret: number }[]
    let si = 0, ei = dates.length - 1
    for (let i = 0; i < dates.length; i++) { if (dates[i] >= startDate) { si = i; break } }
    for (let i = dates.length - 1; i >= 0; i--) { if (dates[i] <= endDate) { ei = i; break } }
    if (si > 0) si--
    return series.filter(s => visibleKeys.has(s.key)).map(s => {
      const sliced = s.curve.slice(si, ei + 1)
      if (sliced.length < 2 || sliced[0] === 0) return { key: s.key, label: s.label, color: s.color, ret: 0 }
      return { key: s.key, label: s.label, color: s.color, ret: sliced[sliced.length - 1] / sliced[0] - 1 }
    })
  }, [viewMode, series, dates, startDate, endDate, visibleKeys])

  const sortedMultiSeries = useMemo(() => {
    const s = [...visibleMultiSeries]
    s.sort((a, b) => {
      let cmp = legendSortCol === 'name' ? a.label.localeCompare(b.label) : a.lastVal - b.lastVal
      return legendSortAsc ? cmp : -cmp
    })
    return s
  }, [visibleMultiSeries, legendSortCol, legendSortAsc])

  // Canvas rendering
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
    ctx.fillStyle = '#fdf6e3'
    ctx.fillRect(0, 0, dims.w, dims.h)

    if (viewMode === 'multi' && chartView === 'line') {
      // Rebased cumulative line chart
      const vs = visibleMultiSeries
      if (multiData.dates.length === 0 || vs.length === 0) return
      const p = { top: 20, right: 60, bottom: 50, left: 20 }
      const plotW = dims.w - p.left - p.right
      const plotH = dims.h - p.top - p.bottom
      let yMin = 0, yMax = 0
      vs.forEach(s => s.values.forEach(v => { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) }))
      const yPad = (yMax - yMin) * 0.1 || 0.05
      yMin -= yPad; yMax += yPad
      const numDates = multiData.dates.length
      const xScale = (i: number) => p.left + (numDates > 1 ? (i / (numDates - 1)) * plotW : plotW / 2)
      const yScale = (v: number) => p.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH
      ctx.strokeStyle = '#e9ecef'; ctx.lineWidth = 1
      for (let i = 0; i <= 6; i++) {
        const v = yMin + (yMax - yMin) * (i / 6)
        const y = yScale(v)
        ctx.beginPath(); ctx.moveTo(p.left, y); ctx.lineTo(dims.w - p.right, y); ctx.stroke()
        ctx.fillStyle = '#6c757d'; ctx.font = '10px monospace'; ctx.textAlign = 'left'
        ctx.fillText((v * 100).toFixed(1) + '%', dims.w - p.right + 5, y + 3)
      }
      ctx.strokeStyle = '#adb5bd'; ctx.lineWidth = 1; ctx.setLineDash([4, 4])
      ctx.beginPath(); ctx.moveTo(p.left, yScale(0)); ctx.lineTo(dims.w - p.right, yScale(0)); ctx.stroke()
      ctx.setLineDash([])
      const labelInterval = Math.max(1, Math.floor(numDates / 8))
      ctx.fillStyle = '#6c757d'; ctx.font = '10px monospace'; ctx.textAlign = 'center'
      for (let i = 0; i < numDates; i += labelInterval) ctx.fillText(multiData.dates[i].slice(0, 7), xScale(i), dims.h - p.bottom + 15)
      vs.forEach(s => {
        const isHovered = hoveredName === s.key
        const isOther = hoveredName !== null && !isHovered
        ctx.strokeStyle = isOther ? '#dee2e6' : s.color
        ctx.lineWidth = isHovered ? 2.5 : 1.5
        ctx.globalAlpha = isOther ? 0.3 : 1
        ctx.beginPath()
        let started = false
        s.values.forEach((v, i) => {
          const x = xScale(i), y = yScale(v)
          if (!started) { ctx.moveTo(x, y); started = true } else ctx.lineTo(x, y)
        })
        ctx.stroke()
        ctx.globalAlpha = 1
      })
    } else if (viewMode === 'multi' && chartView === 'bar') {
      // Multi-strat bar chart — one bar per strategy, sorted worst→best (left→right)
      const items = [...multiBarData].sort((a, b) => a.ret - b.ret)
      const n = items.length
      if (n === 0) return
      const labelFontSize = Math.min(11, Math.max(7, Math.floor((dims.w - 110) / n * 0.7 * 0.7)))
      ctx.font = `${labelFontSize}px monospace`
      let maxLabelW = 0
      for (const it of items) { const w = ctx.measureText(it.label).width; if (w > maxLabelW) maxLabelW = w }
      const dynamicBottom = Math.min(Math.ceil(maxLabelW * 0.707) + 16, Math.floor(dims.h * 0.4))
      const p = { top: 12, right: 50, bottom: dynamicBottom, left: 60 }
      const plotW = dims.w - p.left - p.right
      const plotH = dims.h - p.top - p.bottom
      const barW = Math.max(2, Math.min(40, (plotW / n) * 0.75))
      const gap = (plotW - barW * n) / (n + 1)
      let yMin = 0, yMax = 0
      items.forEach(b => { yMin = Math.min(yMin, b.ret); yMax = Math.max(yMax, b.ret) })
      const yPad = (yMax - yMin) * 0.1 || 0.005
      yMin -= yPad; yMax += yPad
      const yScale = (v: number) => p.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH
      const zeroY = yScale(0)
      ctx.strokeStyle = '#e9ecef'; ctx.lineWidth = 1
      for (let i = 0; i <= 6; i++) {
        const v = yMin + (yMax - yMin) * (i / 6)
        const y = yScale(v)
        ctx.beginPath(); ctx.moveTo(p.left, y); ctx.lineTo(dims.w - p.right, y); ctx.stroke()
        ctx.fillStyle = '#6c757d'; ctx.font = '10px monospace'; ctx.textAlign = 'right'
        ctx.fillText((v * 100).toFixed(2) + '%', p.left - 5, y + 3)
      }
      ctx.strokeStyle = '#adb5bd'; ctx.lineWidth = 1; ctx.setLineDash([4, 4])
      ctx.beginPath(); ctx.moveTo(p.left, zeroY); ctx.lineTo(dims.w - p.right, zeroY); ctx.stroke()
      ctx.setLineDash([])
      for (let i = 0; i < n; i++) {
        const x = p.left + gap + i * (barW + gap)
        const val = items[i].ret
        const bTop = val >= 0 ? yScale(val) : zeroY
        const bBot = val >= 0 ? zeroY : yScale(val)
        const isHovered = hoveredName === items[i].key
        ctx.fillStyle = isHovered ? items[i].color : (val >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)')
        ctx.fillRect(x, bTop, barW, Math.max(1, bBot - bTop))
      }
      ctx.save()
      ctx.fillStyle = '#586e75'; ctx.font = `${labelFontSize}px monospace`; ctx.textAlign = 'right'
      for (let i = 0; i < n; i++) {
        const x = p.left + gap + i * (barW + gap) + barW / 2
        ctx.save(); ctx.translate(x, dims.h - p.bottom + 8); ctx.rotate(-Math.PI / 4)
        ctx.fillText(items[i].label, 0, 0); ctx.restore()
      }
      ctx.restore()
    } else {
      // Bar chart (single strat)
      const n = barData.returns.length
      if (n === 0) return
      const p = { top: 12, right: 50, bottom: 60, left: 20 }
      const plotW = dims.w - p.left - p.right
      const plotH = dims.h - p.top - p.bottom
      const barW = Math.max(1, (plotW / n) * 0.75)
      const gap = (plotW - barW * n) / (n + 1)
      let yMin = 0, yMax = 0
      barData.returns.forEach(r => { yMin = Math.min(yMin, r); yMax = Math.max(yMax, r) })
      const yPad = (yMax - yMin) * 0.1 || 0.005
      yMin -= yPad; yMax += yPad
      const yScale = (v: number) => p.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH
      const zeroY = yScale(0)
      ctx.strokeStyle = '#e9ecef'; ctx.lineWidth = 1
      for (let i = 0; i <= 6; i++) {
        const v = yMin + (yMax - yMin) * (i / 6)
        const y = yScale(v)
        ctx.beginPath(); ctx.moveTo(p.left, y); ctx.lineTo(dims.w - p.right, y); ctx.stroke()
        ctx.fillStyle = '#6c757d'; ctx.font = '10px monospace'; ctx.textAlign = 'left'
        ctx.fillText((v * 100).toFixed(2) + '%', dims.w - p.right + 5, y + 3)
      }
      ctx.strokeStyle = '#adb5bd'; ctx.lineWidth = 1; ctx.setLineDash([4, 4])
      ctx.beginPath(); ctx.moveTo(p.left, zeroY); ctx.lineTo(dims.w - p.right, zeroY); ctx.stroke()
      ctx.setLineDash([])
      for (let i = 0; i < n; i++) {
        const x = p.left + gap + i * (barW + gap)
        const val = barData.returns[i]
        const bTop = val >= 0 ? yScale(val) : zeroY
        const bBot = val >= 0 ? zeroY : yScale(val)
        const isHovered = hoveredIdx === i
        ctx.fillStyle = isHovered ? (series.find(s => s.key === selectedKey)?.color || '#657b83') : (val >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)')
        ctx.fillRect(x, bTop, barW, Math.max(1, bBot - bTop))
      }
      ctx.fillStyle = '#93a1a1'; ctx.font = '9px monospace'; ctx.textAlign = 'center'
      const nLabels = Math.min(8, n)
      for (let li = 0; li < nLabels; li++) {
        const idx = n === 1 ? 0 : Math.round(li * (n - 1) / (nLabels - 1))
        ctx.fillText(barData.dates[idx], p.left + gap + idx * (barW + gap) + barW / 2, dims.h - p.bottom + 14)
      }
    }
  }, [barData, dims, hoveredIdx, series, selectedKey, viewMode, multiData, hoveredName, chartView, visibleMultiSeries, multiBarData])

  // Mouse hover
  const handleMouseMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    if (viewMode === 'multi') return // hover via legend
    if (!canvasRef.current) return
    const rect = canvasRef.current.getBoundingClientRect()
    const mx = (e.clientX - rect.left) * (dims.w / rect.width)
    const n = barData.returns.length
    if (!n) return
    const p = { left: 60, right: 50 }
    const plotW = dims.w - p.left - p.right
    const barW = Math.max(1, (plotW / n) * 0.75)
    const gap = (plotW - barW * n) / (n + 1)
    let found = -1
    for (let i = 0; i < n; i++) {
      const x = p.left + gap + i * (barW + gap)
      if (mx >= x && mx <= x + barW) { found = i; break }
    }
    setHoveredIdx(found >= 0 ? found : null)
  }

  const hovered = hoveredIdx !== null && barData.dates[hoveredIdx]
    ? { date: barData.dates[hoveredIdx], ret: barData.returns[hoveredIdx] }
    : null

  const handlePreset = (p: typeof RETURNS_PRESETS[number]) => {
    if (!dateBoundsMax) return
    setActivePreset(p.label)
    if (p.days === 0) { setStartDate(dateBoundsMin); setEndDate(dateBoundsMax); return }
    if (p.days === -1) { setStartDate(`${dateBoundsMax.slice(0, 4)}-01-01`); setEndDate(dateBoundsMax); return }
    const d = new Date(dateBoundsMax)
    d.setDate(d.getDate() - p.days)
    const pad2 = (n: number) => n < 10 ? '0' + n : '' + n
    const s = `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`
    setStartDate(s < dateBoundsMin ? dateBoundsMin : s)
    setEndDate(dateBoundsMax)
  }

  const scrollPeriod = (dir: number) => {
    if (!dateBoundsMax || !dateBoundsMin) return
    const pad2 = (n: number) => n < 10 ? '0' + n : '' + n
    const fmt = (d: Date) => `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`
    const parse = (s: string) => { const [y, m, d] = s.split('-').map(Number); return new Date(y, m - 1, d) }
    const anchor = parse(startDate || dateBoundsMax)
    let ns: string, ne: string

    if (scrollUnit === '1D') {
      const cur = endDate || dateBoundsMax
      // Use trading dates from the backtest
      const idx = dates.indexOf(cur)
      const target = idx >= 0 ? idx + dir : -1
      if (target < 0 || target >= dates.length) return
      ns = ne = dates[target]
    } else if (scrollUnit === '1W') {
      const dow = anchor.getDay()
      const mon = new Date(anchor)
      mon.setDate(anchor.getDate() - ((dow + 6) % 7))
      mon.setDate(mon.getDate() + dir * 7)
      const fri = new Date(mon); fri.setDate(mon.getDate() + 4)
      ns = fmt(mon); ne = fmt(fri)
    } else if (scrollUnit === '1M') {
      const newMonth = anchor.getMonth() + dir
      const s = new Date(anchor.getFullYear(), newMonth, 1)
      const e = new Date(s.getFullYear(), s.getMonth() + 1, 0)
      ns = fmt(s); ne = fmt(e)
    } else {
      const yr = anchor.getFullYear() + dir
      ns = `${yr}-01-01`; ne = `${yr}-12-31`
    }
    if (ne < dateBoundsMin || ns > dateBoundsMax) return
    if (ns < dateBoundsMin) ns = dateBoundsMin
    if (ne > dateBoundsMax) ne = dateBoundsMax
    setStartDate(ns); setEndDate(ne); setActivePreset('')
  }

  // Export
  const prevExportTrigger2 = useRef(exportTrigger || 0)
  useEffect(() => {
    if (!exportTrigger || exportTrigger === prevExportTrigger2.current) {
      prevExportTrigger2.current = exportTrigger || 0; return
    }
    prevExportTrigger2.current = exportTrigger
    const canvas = canvasRef.current
    if (!canvas) return

    const dpr = window.devicePixelRatio || 1
    const chartW = canvas.width / dpr
    const chartH = canvas.height / dpr
    const dateRange = `${startDate} – ${endDate}`

    if (viewMode === 'multi' && chartView === 'line') {
      // Multi-strat line: include right legend panel
      const rightW = 180
      const totalW = chartW + rightW
      const composite = document.createElement('canvas')
      composite.width = totalW * dpr; composite.height = canvas.height
      const cCtx = composite.getContext('2d')
      if (!cCtx) return
      cCtx.drawImage(canvas, 0, 0)
      cCtx.scale(dpr, dpr)
      // Right panel
      cCtx.fillStyle = '#fdf6e3'; cCtx.fillRect(chartW, 0, rightW, chartH)
      cCtx.strokeStyle = '#93a1a1'; cCtx.lineWidth = 1
      cCtx.beginPath(); cCtx.moveTo(chartW + 0.5, 0); cCtx.lineTo(chartW + 0.5, chartH); cCtx.stroke()
      const rX = chartW + 8
      cCtx.fillStyle = '#586e75'; cCtx.font = 'bold 10px monospace'; cCtx.textBaseline = 'top'
      cCtx.textAlign = 'left'; cCtx.fillText('Strategy', rX, 8)
      cCtx.textAlign = 'right'; cCtx.fillText('Chg', totalW - 8, 8)
      cCtx.font = '10px monospace'
      const legendItems = sortedMultiSeries
      for (let i = 0; i < legendItems.length && i * 16 + 28 < chartH; i++) {
        const s = legendItems[i]
        const y = 28 + i * 16
        cCtx.fillStyle = s.color; cCtx.textAlign = 'left'; cCtx.fillText(s.label, rX, y)
        cCtx.fillStyle = s.lastVal >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)'
        cCtx.textAlign = 'right'; cCtx.fillText((s.lastVal * 100).toFixed(1) + '%', totalW - 8, y)
      }
      // Labels on chart
      cCtx.fillStyle = '#586e75'; cCtx.font = '11px monospace'; cCtx.textAlign = 'left'
      cCtx.fillText(exportLabel || '', 12, 6)
      cCtx.font = 'bold 11px monospace'; cCtx.textAlign = 'right'
      cCtx.fillText(dateRange, chartW - 60, 6)
      composite.toBlob(blob => {
        if (!blob) return
        const url = URL.createObjectURL(blob); const a = document.createElement('a')
        a.href = url; a.download = `multi_strat_returns.png`; a.click(); URL.revokeObjectURL(url)
      }, 'image/png')
    } else {
      // Single-strat bar or multi-strat bar: just labels on chart
      const selSeries = series.find(s => s.key === selectedKey)
      const stratLabel = viewMode === 'single' ? `${selSeries?.label || ''} ${barPeriod}` : ''
      const padR = viewMode === 'multi' ? 50 : 50
      const composite = document.createElement('canvas')
      composite.width = canvas.width; composite.height = canvas.height
      const cCtx = composite.getContext('2d')
      if (!cCtx) return
      cCtx.drawImage(canvas, 0, 0)
      cCtx.scale(dpr, dpr)
      cCtx.fillStyle = '#586e75'; cCtx.textBaseline = 'top'
      cCtx.font = '11px monospace'; cCtx.textAlign = 'left'
      cCtx.fillText(exportLabel ? `${exportLabel}  ${stratLabel}` : stratLabel, 12, 6)
      cCtx.font = 'bold 11px monospace'; cCtx.textAlign = 'right'
      cCtx.fillText(dateRange, chartW - padR, 6)
      composite.toBlob(blob => {
        if (!blob) return
        const url = URL.createObjectURL(blob); const a = document.createElement('a')
        a.href = url; a.download = `${viewMode}_strat_returns.png`; a.click(); URL.revokeObjectURL(url)
      }, 'image/png')
    }
  }, [exportTrigger])

  return (
    <div style={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0 }}>
      <div className="analysis-date-controls">
        {viewMode === 'multi' && (
          <>
            <div className="basket-returns-presets">
              {series.map(s => (
                <button key={s.key} className={`basket-returns-preset-btn ${visibleKeys.has(s.key) ? 'active' : ''}`}
                  style={visibleKeys.has(s.key) ? { background: s.color, borderColor: s.color, color: '#fff' } : undefined}
                  onClick={() => setVisibleKeys(prev => { const next = new Set(prev); if (next.has(s.key)) next.delete(s.key); else next.add(s.key); return next })}>{s.label}</button>
              ))}
            </div>
            <span style={{ color: 'var(--border-color)', margin: '0 1px' }}>|</span>
            <button className={`basket-returns-preset-btn ${chartView === 'bar' ? 'active' : ''}`} onClick={() => setChartView('bar')}>BAR</button>
            <button className={`basket-returns-preset-btn ${chartView === 'line' ? 'active' : ''}`} onClick={() => setChartView('line')}>LINE</button>
            <span style={{ color: 'var(--border-color)', margin: '0 1px' }}>|</span>
          </>
        )}
        {viewMode === 'single' && (
          <>
            <span style={{ fontSize: 9, fontWeight: 'bold', color: 'var(--text-bold)', whiteSpace: 'nowrap' }}>Strategy</span>
            <div className="basket-returns-presets">
              {series.map(s => (
                <button key={s.key} className={`basket-returns-preset-btn ${selectedKey === s.key ? 'active' : ''}`}
                  style={selectedKey === s.key ? { background: s.color, borderColor: s.color, color: '#fff' } : undefined}
                  onClick={() => setSelectedKey(s.key)}>{s.label}</button>
              ))}
            </div>
            <span style={{ color: 'var(--border-color)', margin: '0 1px' }}>|</span>
            <span style={{ fontSize: 9, fontWeight: 'bold', color: 'var(--text-bold)', whiteSpace: 'nowrap' }}>Timeframe</span>
            <div className="basket-returns-presets">
              {(['1D','1W','1M','1Q','1Y'] as const).map(p => (
                <button key={p} className={`basket-returns-preset-btn ${barPeriod === p ? 'active' : ''}`} onClick={() => setBarPeriod(p)}>{p}</button>
              ))}
            </div>
            <span style={{ color: 'var(--border-color)', margin: '0 1px' }}>|</span>
          </>
        )}
        <span style={{ fontSize: 9, fontWeight: 'bold', color: 'var(--text-bold)', whiteSpace: 'nowrap' }}>Date Range</span>
        <div className="basket-returns-presets">
          {RETURNS_PRESETS.map(p => (
            <button key={p.label} className={`basket-returns-preset-btn ${activePreset === p.label ? 'active' : ''}`} onClick={() => handlePreset(p)}>{p.label}</button>
          ))}
        </div>
        <div style={{ flex: 1 }} />
        <div className="basket-returns-presets">
          <button className="basket-returns-preset-btn" onClick={() => scrollPeriod(-1)} title="Previous period">{'\u25C0'}</button>
          {(['1D','1W','1M','1Y'] as const).map(u => (
            <button key={u} className={`basket-returns-preset-btn ${scrollUnit === u ? 'active' : ''}`} onClick={() => setScrollUnit(u)}>{u}</button>
          ))}
          <button className="basket-returns-preset-btn" onClick={() => scrollPeriod(1)} title="Next period">{'\u25B6'}</button>
        </div>
        <input type="date" className="date-input" value={startDate} min={dateBoundsMin} max={dateBoundsMax} onChange={e => { setStartDate(e.target.value); setActivePreset('') }} />
        <span style={{ fontSize: 10, color: 'var(--text-main)' }}>to</span>
        <input type="date" className="date-input" value={endDate} min={dateBoundsMin} max={dateBoundsMax} onChange={e => { setEndDate(e.target.value); setActivePreset('') }} />
      </div>
      <div style={{ flex: 1, minHeight: 0, display: 'flex' }}>
        <div style={{ flex: 1, minWidth: 0, position: 'relative' }} ref={containerRef}>
          <canvas ref={canvasRef} style={{ width: '100%', height: '100%' }}
            onMouseMove={handleMouseMove} onMouseLeave={() => { setHoveredIdx(null); setHoveredName(null) }} />
          {viewMode === 'single' && hovered && (
            <div className="candle-detail-overlay" style={{ minWidth: 140 }}>
              <div className="candle-detail-title">{hovered.date}</div>
              <div className="candle-detail-row">
                <span>Return</span>
                <span className="ret" style={{ color: hovered.ret >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>
                  {(hovered.ret * 100).toFixed(2)}%
                </span>
              </div>
            </div>
          )}
        </div>
        {viewMode === 'multi' && chartView === 'line' && (
          <div className="backtest-path-legend">
            <div className="path-legend-header">
              <span className="path-legend-col ticker" onClick={() => { if (legendSortCol === 'name') setLegendSortAsc(v => !v); else { setLegendSortCol('name'); setLegendSortAsc(true) } }}>
                Strategy{legendSortCol === 'name' ? (legendSortAsc ? ' \u25B2' : ' \u25BC') : ''}
              </span>
              <span className="path-legend-col change" onClick={() => { if (legendSortCol === 'change') setLegendSortAsc(v => !v); else { setLegendSortCol('change'); setLegendSortAsc(false) } }}>
                Chg{legendSortCol === 'change' ? (legendSortAsc ? ' \u25B2' : ' \u25BC') : ''}
              </span>
            </div>
            {sortedMultiSeries.map(s => (
              <div key={s.key}
                   className={`path-legend-row ${hoveredName === s.key ? 'highlighted' : ''}`}
                   style={{ color: s.color }}
                   onMouseEnter={() => setHoveredName(s.key)}
                   onMouseLeave={() => setHoveredName(null)}>
                <span className="path-legend-col ticker">{s.label}</span>
                <span className="path-legend-col change">{(s.lastVal * 100).toFixed(1)}%</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

/* ── Component ─────────────────────────────────────────────────────── */

export function BacktestPanel({ apiBase, target, targetType, exportTrigger }: BacktestPanelProps) {
  // ── Basket & ticker lists ──
  const [baskets, setBaskets] = useState<Record<string, string[]>>({})
  const [allTickers, setAllTickers] = useState<string[]>([])
  const [etfTickers, setEtfTickers] = useState<string[]>([])
  useEffect(() => {
    fetch(`${apiBase}/baskets`).then(r => r.json()).then(setBaskets).catch(() => {})
    fetch(`${apiBase}/tickers`).then(r => r.json()).then(setAllTickers).catch(() => {})
    fetch(`${apiBase}/etfs`).then(r => r.json()).then(setEtfTickers).catch(() => {})
  }, [apiBase])

  // ── Per-leg search state ──
  const [legSearchOpen, setLegSearchOpen] = useState<number | null>(null)
  const [legSearchQuery, setLegSearchQuery] = useState('')
  const [legSearchHighlight, setLegSearchHighlight] = useState(0)
  const [legSearchFilter, setLegSearchFilter] = useState<'all' | 'Themes' | 'Sectors' | 'Industries' | 'Tickers' | 'ETFs'>('all')
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
    add(etfTickers, 'ETFs')
    return q ? results.slice(0, 25) : results
  }, [legSearchQuery, legSearchFilter, baskets, allTickers, etfTickers])

  // ── Legs config ──
  // Initialize with target from props if provided
  const [legs, setLegs] = useState<LegConfig[]>(() => {
    const initial = defaultLeg()
    if (target) {
      initial.target = target
      initial.targetType = targetType === 'ticker' ? 'ticker' : targetType === 'etf' ? 'etf' : 'basket_tickers'
    }
    return [initial]
  })

  // Sync first leg target when props change
  useEffect(() => {
    if (target) {
      setLegs(prev => {
        if (prev.length === 1 && prev[0].target === '') {
          return [{ ...prev[0], target, targetType: (targetType === 'ticker' ? 'ticker' : targetType === 'etf' ? 'etf' : 'basket_tickers') as LegConfig['targetType'] }]
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
    const tt = r.category === 'ETFs' ? 'etf' : r.category === 'Tickers' ? 'ticker' : 'basket_tickers'
    setLegs(prev => prev.map((l, idx) => idx === legIdx ? { ...l, target: r.name, targetType: tt as LegConfig['targetType'] } : l))
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

  // ── Fetch date ranges for all legs and compute effective range ──
  const [dataRange, setDataRange] = useState<{ min: string; max: string } | null>(null)
  const legTargetKey = legs.map(l => `${l.targetType}:${l.target}`).join(',')
  useEffect(() => {
    const withTarget = legs.filter(l => l.target)
    if (withTarget.length === 0) { setDataRange(null); return }
    let cancelled = false
    Promise.all(withTarget.map(l =>
      fetch(`${apiBase}/date-range/${l.targetType}/${encodeURIComponent(l.target)}`)
        .then(r => r.ok ? r.json() as Promise<{ min: string; max: string }> : null)
        .catch(() => null)
    )).then(ranges => {
      if (cancelled) return
      const valid = ranges.filter((r): r is { min: string; max: string } => r !== null)
      if (valid.length === 0) return
      // Effective start = latest min (newest initial date); effective end = latest max
      const effectiveMin = valid.reduce((acc, r) => r.min > acc ? r.min : acc, valid[0].min)
      const effectiveMax = valid.reduce((acc, r) => r.max > acc ? r.max : acc, valid[0].max)
      // Picker bounds = union (widest range across all legs)
      const boundsMin = valid.reduce((acc, r) => r.min < acc ? r.min : acc, valid[0].min)
      setDataRange({ min: boundsMin, max: effectiveMax })
      setStartDate(effectiveMin)
      setEndDate(effectiveMax)
    })
    return () => { cancelled = true }
  }, [legTargetKey, apiBase])

  // ── Shared settings ──
  const [maxLeverage, setMaxLeverage] = useState(250)
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')

  // ── Results ──
  const [result, setResult] = useState<MultiBacktestResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [btProgress, setBtProgress] = useState(0)
  const [btProgressMsg, setBtProgressMsg] = useState('')
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
  const [benchmarkStats, setBenchmarkStats] = useState<Record<string, Stats>>({})
  const [showBenchmark, setShowBenchmark] = useState<Record<string, boolean>>({})
  const benchmarkGenRef = useRef(0)

  // ── Stats tab sort state ──
  const [statsSortRow, setStatsSortRow] = useState<string | null>(null)
  const [statsSortAsc, setStatsSortAsc] = useState(false)

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
  const exitsValid = legs.every(l =>
    ['Long', 'Short'].includes(l.entrySignal) ||
    (l.exitSignal !== 'none' || (l.stopSignal !== null && l.stopSignal !== 'none'))
  )

  // ── Run backtest ──
  const runBacktest = useCallback(async () => {
    if (!allocValid || !allTargetsSet || !exitsValid) return
    setLoading(true)
    setBtProgress(0)
    setBtProgressMsg('')
    setError('')
    setBenchmarks({})
    setBenchmarkStats({})
    setShowBenchmark({})
    const gen = ++benchmarkGenRef.current

    const hasBenchmarks = isSingleLeg
    const totalTasks = hasBenchmarks ? 2 : 1  // main + batch benchmarks
    let completed = 0
    let animTarget = 0
    const estimatedMs = 15000
    const startTime = Date.now()
    const incrementProgress = () => {
      completed++
      animTarget = Math.max(animTarget, Math.round(completed / totalTasks * 100))
    }

    // Smooth progress: blends elapsed-time estimate with completion-based floor
    const progressTimer = setInterval(() => {
      const elapsed = Date.now() - startTime
      const timePct = 90 * (1 - Math.exp(-elapsed / (estimatedMs * 0.4)))
      const completionPct = Math.round(completed / totalTasks * 100)
      animTarget = Math.max(animTarget, completionPct)
      setBtProgress(Math.round(Math.max(timePct, animTarget)))
    }, 200)

    try {
      // Build main request body
      const mainBody = {
        legs: legs.map(l => ({
          target: l.target,
          target_type: l.targetType,
          entry_signal: l.entrySignal,
          exit_signal: l.exitSignal && l.exitSignal !== 'none' && !l.exitSignal.startsWith('rv_') ? l.exitSignal : undefined,
          exit_rv_multiple: l.exitSignal?.startsWith('rv_') ? parseFloat(l.exitSignal.slice(3)) : undefined,
          no_exit_target: l.exitSignal === 'none' ? true : undefined,
          stop_signal: l.stopSignal && l.stopSignal !== 'none' && !l.stopSignal.startsWith('rv_') && !l.stopSignal.startsWith('trv_') ? l.stopSignal : undefined,
          stop_rv_multiple: l.stopSignal?.startsWith('rv_') && !l.stopSignal.startsWith('trv_') ? parseFloat(l.stopSignal.slice(3)) : undefined,
          trailing_stop_rv_multiple: l.stopSignal?.startsWith('trv_') ? parseFloat(l.stopSignal.slice(4)) : undefined,
          allocation_pct: isSingleLeg ? 1.0 : l.allocationPct / 100,
          position_size: l.positionSize / 100,
          filters: l.filters.map(f => ({ ...f, metric: stripBasketPrefix(f.metric), lookback: f.lookback || 21 })),
        })),
        start_date: startDate || null,
        end_date: endDate || null,
        max_leverage: maxLeverage / 100,
      }

      // Fire main backtest
      const mainPromise = fetch(`${apiBase}/backtest/multi`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(mainBody),
      }).then(async r => {
        if (!r.ok) { const err = await r.json().catch(() => ({ detail: r.statusText })); throw new Error(err.detail || r.statusText) }
        return r.json() as Promise<MultiBacktestResult>
      }).then(data => {
        setResult(data)
        setShowResults(true)
        setResultTab('equity')
        eqViewRef.current = { start: 0, end: data.combined.equity_curve.dates.length - 1 }
        setEqViewVersion(v => v + 1)
        incrementProgress()
      })

      // Fire single batch benchmarks request (loads parquet once, loops signals)
      const benchPromise = hasBenchmarks ? fetch(`${apiBase}/backtest/benchmarks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          target: legs[0].target,
          target_type: legs[0].targetType,
          position_size: legs[0].positionSize / 100,
          max_leverage: maxLeverage / 100,
          start_date: startDate || null,
          end_date: endDate || null,
        }),
      })
        .then(r => r.ok ? r.json() : null)
        .then(d => {
          incrementProgress()
          if (benchmarkGenRef.current !== gen) return
          if (d && d.benchmarks) {
            setBenchmarks(d.benchmarks as Record<string, number[]>)
            if (d.stats) setBenchmarkStats(d.stats as Record<string, Stats>)
          }
        })
        .catch(() => { incrementProgress() })
      : Promise.resolve()

      await Promise.all([mainPromise, benchPromise])
    } catch (e: any) {
      setError(e.message || 'Unknown error')
    } finally {
      clearInterval(progressTimer)
      setBtProgress(100)
      setLoading(false)
    }
  }, [apiBase, legs, startDate, endDate, maxLeverage, allocValid, allTargetsSet, isSingleLeg, exitsValid])

  // Common export labels (used by export effect and render)
  const titleLeft = useMemo(() => {
    const legDescs = legs.map(l => {
      const target = l.target.replace(/_/g, ' ')
      const tickerFlag = l.targetType === 'basket_tickers' ? ' (Tickers)' : ''
      const entry = l.entrySignal
      const exitParts: string[] = []
      if (l.exitSignal) exitParts.push(l.exitSignal)
      if (l.stopSignal) exitParts.push(`Stop:${l.stopSignal}`)
      const exit = exitParts.length > 0 ? exitParts.join('+') : 'Default'
      const alloc = legs.length > 1 ? ` Alloc:${l.allocationPct}%` : ''
      return `${target}${tickerFlag} Entry:${entry} Exit:${exit} Pos:${l.positionSize}%${alloc}`
    })
    return legDescs.join('  |  ') + `  Lev:${maxLeverage}%`
  }, [legs, maxLeverage])

  // ── Export current tab when exportTrigger changes ──
  const prevExportTrigger = useRef(exportTrigger || 0)
  useEffect(() => {
    if (!exportTrigger || exportTrigger === prevExportTrigger.current) {
      prevExportTrigger.current = exportTrigger || 0
      return
    }
    prevExportTrigger.current = exportTrigger

    const dateRange = result?.date_range ? `${result.date_range.min} – ${result.date_range.max}` : `${startDate} – ${endDate}`
    const filenameBase = legs.map(l => l.target).join('+')

    // CSV export helper
    const downloadCSV = (content: string, fname: string) => {
      const blob = new Blob([content], { type: 'text/csv' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url; a.download = fname; a.click()
      URL.revokeObjectURL(url)
    }

    // PNG export helper with labels (padRight = chart's right padding for y-axis alignment)
    const downloadCanvas = (canvas: HTMLCanvasElement, tabName: string, padRight = 60) => {
      const dpr = window.devicePixelRatio || 1
      const cW = canvas.width / dpr
      const composite = document.createElement('canvas')
      composite.width = canvas.width
      composite.height = canvas.height
      const cCtx = composite.getContext('2d')
      if (!cCtx) return
      cCtx.drawImage(canvas, 0, 0)
      cCtx.scale(dpr, dpr)
      cCtx.fillStyle = '#586e75'
      cCtx.textBaseline = 'top'
      cCtx.font = '11px monospace'
      cCtx.textAlign = 'left'
      cCtx.fillText(titleLeft, 12, 6)
      cCtx.font = 'bold 11px monospace'
      cCtx.textAlign = 'right'
      cCtx.fillText(dateRange, cW - padRight, 6)
      composite.toBlob((blob) => {
        if (!blob) return
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url; a.download = `${filenameBase}_${tabName}.png`; a.click()
        URL.revokeObjectURL(url)
      }, 'image/png')
    }

    if (resultTab === 'equity' && canvasRef.current) downloadCanvas(canvasRef.current, 'equity', 60)
    else if (resultTab === 'distribution' && histCanvasRef.current) downloadCanvas(histCanvasRef.current, 'distribution', 30)
    else if (resultTab === 'path' && pathCanvasRef.current) {
      // Path: include right-side legend panel
      const canvas = pathCanvasRef.current
      const dpr = window.devicePixelRatio || 1
      const chartW = canvas.width / dpr
      const chartH = canvas.height / dpr
      const rightW = 240
      const totalW = chartW + rightW
      const composite = document.createElement('canvas')
      composite.width = totalW * dpr; composite.height = canvas.height
      const cCtx = composite.getContext('2d')
      if (cCtx) {
        cCtx.drawImage(canvas, 0, 0)
        cCtx.scale(dpr, dpr)
        // Right panel background + separator
        cCtx.fillStyle = '#fdf6e3'; cCtx.fillRect(chartW, 0, rightW, chartH)
        cCtx.strokeStyle = '#93a1a1'; cCtx.lineWidth = 1
        cCtx.beginPath(); cCtx.moveTo(chartW + 0.5, 0); cCtx.lineTo(chartW + 0.5, chartH); cCtx.stroke()
        // Header
        const rX = chartW + 8
        const resultIsMulti = result && result.legs.length > 1
        cCtx.fillStyle = '#586e75'; cCtx.font = 'bold 10px monospace'; cCtx.textBaseline = 'top'
        cCtx.textAlign = 'left'
        if (resultIsMulti) cCtx.fillText('Leg', rX, 8)
        const hasTickers = allPaths.some(d => d.trade.ticker)
        const tickerX = resultIsMulti ? rX + 55 : rX
        if (hasTickers) cCtx.fillText('Ticker', tickerX, 8)
        const dateX = hasTickers ? tickerX + 50 : tickerX
        cCtx.fillText('Date', dateX, 8)
        cCtx.textAlign = 'right'; cCtx.fillText('Chg', totalW - 8, 8)
        // Rows
        cCtx.font = '9px monospace'
        const colorRanked = [...allPaths].sort((a, b) => b.ret - a.ret)
        const rankColor = (rank: number, total: number): string => {
          if (total <= 1) return 'rgb(50, 50, 255)'
          const t = rank / (total - 1)
          if (t <= 0.5) { const s = t * 2; return `rgb(${Math.round(50 + 102 * s)}, 50, ${Math.round(255 - 53 * s)})` }
          const s = (t - 0.5) * 2; return `rgb(${Math.round(152 + 103 * s)}, 50, ${Math.round(202 - 52 * s)})`
        }
        const colorMap = new Map(colorRanked.map((d, rank) => [allPaths.indexOf(d), rankColor(rank, colorRanked.length)]))
        for (let i = 0; i < allPaths.length && i * 14 + 26 < chartH; i++) {
          const d = allPaths[i]
          const y = 26 + i * 14
          const color = colorMap.get(i) || '#586e75'
          cCtx.fillStyle = color; cCtx.textAlign = 'left'
          if (resultIsMulti) cCtx.fillText(d.legTarget.replace(/_/g, ' ').slice(0, 8), rX, y)
          if (hasTickers) cCtx.fillText(d.trade.ticker || '', tickerX, y)
          cCtx.fillText(d.trade.entry_date.slice(2), dateX, y)
          cCtx.fillStyle = d.ret >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)'
          cCtx.textAlign = 'right'; cCtx.fillText((d.ret * 100).toFixed(1) + '%', totalW - 8, y)
        }
        // Labels on chart
        cCtx.fillStyle = '#586e75'; cCtx.font = '11px monospace'; cCtx.textAlign = 'left'
        cCtx.fillText(titleLeft, 12, 6)
        cCtx.font = 'bold 11px monospace'; cCtx.textAlign = 'right'
        cCtx.fillText(dateRange, chartW - 70, 6)
        composite.toBlob(blob => {
          if (!blob) return
          const url = URL.createObjectURL(blob); const a = document.createElement('a')
          a.href = url; a.download = `${filenameBase}_path.png`; a.click(); URL.revokeObjectURL(url)
        }, 'image/png')
      }
    } else if (resultTab === 'stats' && result) {
      const rows = [`${titleLeft},,,${dateRange}`, '']
      // Header
      const statKeys = ['strategy_return', 'cagr', 'volatility', 'max_dd', 'sharpe', 'sortino'] as const
      const tradeKeys = ['trades_met_criteria', 'trades_taken', 'trades_skipped', 'win_rate', 'avg_winner', 'avg_loser', 'ev'] as const
      rows.push(['Strategy', ...statKeys.map(k => k.replace(/_/g, ' ')), ...tradeKeys.map(k => k.replace(/_/g, ' '))].join(','))
      for (const leg of result.legs) {
        const ps = leg.stats.portfolio
        const ts = leg.stats.trades
        rows.push([
          leg.target,
          ...statKeys.map(k => ps[k] != null ? String(ps[k]) : ''),
          ...tradeKeys.map(k => ts[k] != null ? String(ts[k]) : ''),
        ].join(','))
      }
      if (result.combined) {
        const ps = result.combined.stats.portfolio
        const ts = result.combined.stats.trades
        rows.push([
          'Combined',
          ...statKeys.map(k => ps[k] != null ? String(ps[k]) : ''),
          ...tradeKeys.map(k => ts[k] != null ? String(ts[k]) : ''),
        ].join(','))
      }
      downloadCSV(rows.join('\n'), `${filenameBase}_stats.csv`)
    } else if (resultTab === 'trades' && result) {
      const rows = [`${titleLeft},,,${dateRange}`, '']
      rows.push(['Leg', 'Ticker', 'Entry Date', 'Exit Date', 'Entry Price', 'Exit Price', 'Return', 'Bars Held', 'MFE', 'MAE'].join(','))
      for (const leg of result.legs) {
        for (const t of leg.trades) {
          if (!t.regime_pass) continue
          rows.push([
            leg.target, t.ticker ?? '', t.entry_date, t.exit_date,
            t.entry_price?.toFixed(2) ?? '', t.exit_price?.toFixed(2) ?? '',
            t.change != null ? (t.change * 100).toFixed(2) + '%' : '',
            t.bars_held ?? '',
            t.mfe != null ? (t.mfe * 100).toFixed(2) + '%' : '',
            t.mae != null ? (t.mae * 100).toFixed(2) + '%' : '',
          ].join(','))
        }
      }
      downloadCSV(rows.join('\n'), `${filenameBase}_trades.csv`)
    }
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
  const [showAllTrades, setShowAllTrades] = useState(false)

  const allTrades = useMemo(() => {
    if (!result) return []
    const trades: (Trade & { legTarget: string; legSignal: string; legIdx: number })[] = []
    result.legs.forEach((leg, i) => {
      leg.trades.forEach(t => trades.push({ ...t, legTarget: leg.target, legSignal: leg.entry_signal, legIdx: i }))
    })
    return trades
  }, [result])

  const takenTrades = useMemo(() => allTrades.filter(t => t.regime_pass), [allTrades])
  const hasFilters = result ? result.legs.some(l => l.trades.some(t => !t.regime_pass)) : false

  const filteredTrades = useMemo(() => {
    const base = showAllTrades ? allTrades : takenTrades
    const t = tradesLegFilter < 0 ? base : base.filter(x => x.legIdx === tradesLegFilter)
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
    const leg = legs[legIdx]
    const defaultMetric = leg.targetType === 'basket' ? BASKET_STAT_METRICS[0] : TICKER_STAT_METRICS[0]
    updateLeg(legIdx, { filters: [...leg.filters, { metric: defaultMetric, condition: 'above', value: 50, source: 'self', lookback: 21 }] })
  }
  const removeFilter = (legIdx: number, fIdx: number) => {
    updateLeg(legIdx, { filters: legs[legIdx].filters.filter((_, i) => i !== fIdx) })
  }
  const updateFilterField = (legIdx: number, fIdx: number, field: string | Record<string, any>, val?: any) => {
    const patch = typeof field === 'string' ? { [field]: val } : field
    const updated = legs[legIdx].filters.map((f, i) => i === fIdx ? { ...f, ...patch } : f)
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
          {(['equity', 'stats', 'distribution', 'path', 'trades', 'multi_returns', 'single_returns'] as ResultTab[]).map(tab => {
            const label = tab === 'multi_returns' ? 'Multi-Strat Returns' : tab === 'single_returns' ? 'Single-Strat Returns' : tab.charAt(0).toUpperCase() + tab.slice(1)
            return (
              <button key={tab} className={`summary-tab ${resultTab === tab ? 'active' : ''}`}
                onClick={() => setResultTab(tab)}>{label}</button>
            )
          })}
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

                // Build columns: each column is a strategy with its Stats
                type StatsCol = { key: string; label: string; color: string; stats: Stats }
                const columns: StatsCol[] = []

                if (resultIsMultiLeg) {
                  // Multi-leg: one column per leg + combined
                  result.legs.forEach((leg, i) => {
                    columns.push({
                      key: `leg_${i}`,
                      label: `${leg.target.replace(/_/g, ' ')} ${leg.entry_signal.replace(/_/g, ' ')}`,
                      color: LEG_COLORS[i % LEG_COLORS.length],
                      stats: leg.stats,
                    })
                  })
                  columns.push({ key: 'combined', label: 'Combined', color: '#8BC34A', stats: cs })
                } else {
                  // Single-leg: equity (combined) + all benchmarks
                  const leg = result.legs[0]
                  columns.push({
                    key: 'main',
                    label: `${leg.target.replace(/_/g, ' ')} ${leg.entry_signal.replace(/_/g, ' ')}`,
                    color: '#8BC34A',
                    stats: cs,
                  })
                  for (const sig of ['Breakout', 'Breakdown', 'Up_Rot', 'Down_Rot', 'BTFD', 'STFR']) {
                    if (benchmarkStats[sig]) {
                      columns.push({
                        key: sig,
                        label: sig.replace(/_/g, ' '),
                        color: BENCHMARK_COLORS[sig] || '#657b83',
                        stats: benchmarkStats[sig],
                      })
                    }
                  }
                }

                // Buy & Hold: compute portfolio stats from equity curve
                const bhCurve = result.combined.equity_curve.buy_hold
                if (bhCurve && bhCurve.length >= 2) {
                  const first = bhCurve[0], last = bhCurve[bhCurve.length - 1]
                  const stratRet = first > 0 ? last / first - 1 : 0
                  const dailyRets: number[] = []
                  for (let i = 1; i < bhCurve.length; i++) {
                    if (bhCurve[i - 1] > 0) dailyRets.push(bhCurve[i] / bhCurve[i - 1] - 1)
                  }
                  const years = dailyRets.length / 252
                  const cagr = years > 0 && first > 0 && last > 0 ? Math.pow(last / first, 1 / years) - 1 : 0
                  const mean = dailyRets.length > 0 ? dailyRets.reduce((s, v) => s + v, 0) / dailyRets.length : 0
                  const std = dailyRets.length > 1 ? Math.sqrt(dailyRets.reduce((s, v) => s + (v - mean) ** 2, 0) / dailyRets.length) : 0
                  const vol = std * Math.sqrt(252)
                  const sharpe = std > 0 ? (mean / std) * Math.sqrt(252) : 0
                  const downside = dailyRets.filter(r => r < 0)
                  const downStd = downside.length > 1 ? Math.sqrt(downside.reduce((s, v) => s + v ** 2, 0) / downside.length) : 0
                  const sortino = downStd > 0 ? (mean / downStd) * Math.sqrt(252) : 0
                  let maxDd = 0, peak = bhCurve[0]
                  for (const v of bhCurve) { peak = Math.max(peak, v); if (peak > 0) maxDd = Math.max(maxDd, (peak - v) / peak) }
                  columns.push({
                    key: 'buy_hold', label: 'Buy & Hold', color: '#93a1a1',
                    stats: {
                      portfolio: { strategy_return: stratRet, cagr, volatility: vol, max_dd: maxDd, sharpe, sortino },
                      trade: { trades_met_criteria: 1, trades_taken: 1, trades_skipped: 0, win_rate: stratRet > 0 ? 1 : 0, avg_winner: stratRet > 0 ? stratRet : 0, avg_loser: stratRet <= 0 ? stratRet : 0, ev: stratRet, profit_factor: 0, avg_time_winner: dailyRets.length, avg_time_loser: 0 },
                    },
                  })
                }

                // Row definitions: { key, label, section, getValue, getColor }
                type RowDef = { key: string; label: string; section: 'portfolio' | 'trade'; getValue: (s: Stats) => string; getRaw: (s: Stats) => number; getColor?: (s: Stats) => string | undefined }
                const rows: RowDef[] = [
                  ...(resultIsMultiLeg ? [{
                    key: 'allocation', label: 'Allocation', section: 'portfolio' as const,
                    getValue: (s: Stats) => s.portfolio.allocation != null ? (s.portfolio.allocation * 100).toFixed(0) + '%' : '--',
                    getRaw: (s: Stats) => s.portfolio.allocation ?? 0,
                  }] : []),
                  { key: 'return', label: 'Return', section: 'portfolio', getValue: s => pctFmt(s.portfolio.strategy_return), getRaw: s => s.portfolio.strategy_return, getColor: s => s.portfolio.strategy_return >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)' },
                  { key: 'cagr', label: 'CAGR', section: 'portfolio', getValue: s => pctFmt(s.portfolio.cagr), getRaw: s => s.portfolio.cagr, getColor: s => s.portfolio.cagr >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)' },
                  { key: 'volatility', label: 'Volatility', section: 'portfolio', getValue: s => pctFmt(s.portfolio.volatility), getRaw: s => s.portfolio.volatility },
                  { key: 'max_dd', label: 'Max Drawdown', section: 'portfolio', getValue: s => pctFmt(s.portfolio.max_dd), getRaw: s => s.portfolio.max_dd, getColor: () => 'rgb(255,50,150)' },
                  { key: 'sharpe', label: 'Sharpe', section: 'portfolio', getValue: s => s.portfolio.sharpe.toFixed(2), getRaw: s => s.portfolio.sharpe },
                  { key: 'sortino', label: 'Sortino', section: 'portfolio', getValue: s => s.portfolio.sortino.toFixed(2), getRaw: s => s.portfolio.sortino },
                  ...(resultIsMultiLeg ? [{
                    key: 'contribution', label: 'Contribution', section: 'portfolio' as const,
                    getValue: (s: Stats) => s.portfolio.contribution != null ? pctFmt(s.portfolio.contribution) : '--',
                    getRaw: (s: Stats) => s.portfolio.contribution ?? 0,
                    getColor: (s: Stats) => (s.portfolio.contribution ?? 0) >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)',
                  }] : []),
                  { key: 'trades_met', label: 'Trades Met Criteria', section: 'trade', getValue: s => String(s.trade.trades_met_criteria), getRaw: s => s.trade.trades_met_criteria },
                  { key: 'trades_taken', label: 'Trades Taken', section: 'trade', getValue: s => String(s.trade.trades_taken), getRaw: s => s.trade.trades_taken },
                  { key: 'trades_skipped', label: 'Trades Skipped', section: 'trade', getValue: s => String(s.trade.trades_skipped), getRaw: s => s.trade.trades_skipped },
                  { key: 'win_rate', label: 'Win Rate', section: 'trade', getValue: s => pctFmt(s.trade.win_rate), getRaw: s => s.trade.win_rate },
                  { key: 'avg_winner', label: 'Avg Winner', section: 'trade', getValue: s => pctFmt(s.trade.avg_winner), getRaw: s => s.trade.avg_winner, getColor: () => 'rgb(50,50,255)' },
                  { key: 'avg_loser', label: 'Avg Loser', section: 'trade', getValue: s => pctFmt(s.trade.avg_loser), getRaw: s => s.trade.avg_loser, getColor: () => 'rgb(255,50,150)' },
                  { key: 'ev', label: 'EV', section: 'trade', getValue: s => pctFmt(s.trade.ev), getRaw: s => s.trade.ev, getColor: s => s.trade.ev >= 0 ? 'rgb(50,50,255)' : 'rgb(255,50,150)' },
                  { key: 'pf', label: 'Profit Factor', section: 'trade', getValue: s => s.trade.profit_factor.toFixed(2), getRaw: s => s.trade.profit_factor },
                  { key: 'avg_time_w', label: 'Avg Time Winner', section: 'trade', getValue: s => s.trade.avg_time_winner.toFixed(1) + ' bars', getRaw: s => s.trade.avg_time_winner },
                  { key: 'avg_time_l', label: 'Avg Time Loser', section: 'trade', getValue: s => s.trade.avg_time_loser.toFixed(1) + ' bars', getRaw: s => s.trade.avg_time_loser },
                ]

                // Inter-leg correlations removed for now — too many columns for the layout

                // Sort strategies (rows) by clicking a stat column header
                const sortedStrategies = statsSortRow
                  ? [...columns].sort((a, b) => {
                      const rd = rows.find(r => r.key === statsSortRow)
                      if (!rd) return 0
                      const va = rd.getRaw(a.stats), vb = rd.getRaw(b.stats)
                      return statsSortAsc ? va - vb : vb - va
                    })
                  : columns

                const handleColClick = (rowKey: string) => {
                  if (statsSortRow === rowKey) setStatsSortAsc(v => !v)
                  else { setStatsSortRow(rowKey); setStatsSortAsc(false) }
                }

                const portfolioRows = rows.filter(r => r.section === 'portfolio')
                const tradeRows = rows.filter(r => r.section === 'trade')
                const arrow = (key: string) => statsSortRow === key ? (statsSortAsc ? ' \u25B2' : ' \u25BC') : ''

                const renderColHeaders = (label: string, sectionRows: RowDef[], isFirst: boolean) => (
                  <th className="backtest-stats-th" style={{ textAlign: 'center', fontSize: 9, color: '#93a1a1', letterSpacing: 1, background: 'var(--bg-sidebar)', borderBottom: '1px solid var(--border-color)', padding: '4px 6px',
                    ...(!isFirst ? { borderLeft: '1px solid #d6ceb5' } : {}) }} colSpan={sectionRows.length}>
                    {label}
                  </th>
                )

                return (
                  <div style={{ padding: 12, overflowX: 'auto', overflowY: 'auto' }}>
                    <table className="backtest-stats-table" style={{ borderCollapse: 'collapse' }}>
                      <thead>
                        {/* Section group headers */}
                        <tr>
                          <th className="backtest-stats-th" style={{ background: 'var(--bg-main)', borderRight: '1px solid #d6ceb5' }} />
                          {renderColHeaders('PORTFOLIO', portfolioRows, true)}
                          {renderColHeaders('TRADE', tradeRows, false)}
                        </tr>
                        {/* Individual stat column headers */}
                        <tr>
                          <th className="backtest-stats-th" style={{ textAlign: 'center', verticalAlign: 'middle', whiteSpace: 'nowrap', position: 'sticky', left: 0, background: 'var(--bg-main)', zIndex: 1, borderRight: '1px solid #d6ceb5' }}>
                            Strategy
                          </th>
                          {[...portfolioRows, ...tradeRows].map((r, ri) => (
                            <th key={r.key} className="backtest-stats-th"
                              style={{ padding: 0, verticalAlign: 'middle',
                                ...(ri === portfolioRows.length ? { borderLeft: '1px solid #d6ceb5' } : {}) }}
                              onClick={() => handleColClick(r.key)}>
                              <div style={{ width: 0, minWidth: '100%', boxSizing: 'border-box', padding: '3px 4px',
                                fontSize: 9, textAlign: 'center', cursor: 'pointer', userSelect: 'none',
                                whiteSpace: 'normal', wordBreak: 'break-word', lineHeight: '1.2' }}>
                                {r.label}{arrow(r.key)}
                              </div>
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {sortedStrategies.map(strat => (
                          <tr key={strat.key}>
                            <td className="backtest-stats-td" style={{ fontWeight: 700, whiteSpace: 'nowrap', borderLeft: `3px solid ${strat.color}`, borderRight: '1px solid #d6ceb5', color: strat.color, paddingLeft: 8 }}>
                              {strat.label}
                            </td>
                            {[...portfolioRows, ...tradeRows].map((r, ri) => (
                              <td key={r.key} className="backtest-stats-td" style={{ textAlign: 'right', whiteSpace: 'nowrap', color: r.getColor?.(strat.stats),
                                ...(ri === portfolioRows.length ? { borderLeft: '1px solid #d6ceb5' } : {}) }}>
                                {r.getValue(strat.stats)}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
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

              {/* ── Multi-Strat Returns tab ── */}
              {(resultTab === 'multi_returns' || resultTab === 'single_returns') && (() => {
                const eq = result.combined.equity_curve
                const allSeries: { key: string; label: string; curve: number[]; color: string }[] = []
                if (resultIsMultiLeg) {
                  allSeries.push({ key: 'combined', label: 'Combined', curve: eq.combined, color: '#8BC34A' })
                  result.legs.forEach((leg, i) => {
                    if (eq.per_leg[i]) {
                      allSeries.push({ key: `leg_${i}`, label: `${leg.target} ${leg.entry_signal}`, curve: eq.per_leg[i], color: LEG_COLORS[i % LEG_COLORS.length] })
                    }
                  })
                } else {
                  allSeries.push({ key: 'strategy', label: `${result.legs[0].target} ${result.legs[0].entry_signal}`, curve: eq.combined, color: '#8BC34A' })
                }
                allSeries.push({ key: 'buy_hold', label: 'Buy & Hold', curve: eq.buy_hold, color: '#93a1a1' })
                for (const sig of Object.keys(benchmarks)) {
                  if (benchmarks[sig]?.length > 0) {
                    allSeries.push({ key: sig, label: sig.replace(/_/g, ' '), curve: benchmarks[sig], color: BENCHMARK_COLORS[sig] || '#657b83' })
                  }
                }
                const rTitleLeft = titleLeft
                return <BacktestReturnsView dates={eq.dates} series={allSeries} viewMode={resultTab === 'multi_returns' ? 'multi' : 'single'} exportTrigger={exportTrigger} exportLabel={rTitleLeft} />
              })()}
              {/* ── Trades tab ── */}
              {resultTab === 'trades' && (
                <div style={{ padding: '8px 12px', overflowY: 'auto', flex: 1, minHeight: 0 }}>
                  <div style={{ display: 'flex', gap: 8, marginBottom: 8, alignItems: 'center', flexWrap: 'wrap' }}>
                    {resultIsMultiLeg && (
                      <>
                        <span style={{ fontSize: 12, fontWeight: 600 }}>Leg:</span>
                        <button className={`summary-tab ${tradesLegFilter === -1 ? 'active' : ''}`}
                          onClick={() => setTradesLegFilter(-1)}>All</button>
                        {result.legs.map((leg, i) => (
                          <button key={i}
                            className={`summary-tab ${tradesLegFilter === i ? 'active' : ''}`}
                            style={{ borderBottomColor: tradesLegFilter === i ? LEG_COLORS[i] : undefined }}
                            onClick={() => setTradesLegFilter(i)}>
                            {leg.target}
                          </button>
                        ))}
                      </>
                    )}
                    {hasFilters && (
                      <>
                        <span style={{ fontSize: 12, fontWeight: 600, marginLeft: resultIsMultiLeg ? 12 : 0 }}>Show:</span>
                        <button className={`summary-tab ${!showAllTrades ? 'active' : ''}`}
                          onClick={() => setShowAllTrades(false)}>Taken ({takenTrades.length})</button>
                        <button className={`summary-tab ${showAllTrades ? 'active' : ''}`}
                          onClick={() => setShowAllTrades(true)}>All ({allTrades.length})</button>
                      </>
                    )}
                  </div>
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
          {isMultiLeg ? `Leg ${i + 1}` : 'Ticker / Basket'}
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
              {leg.target && <span className="bt-search-display-tag">{leg.targetType === 'ticker' ? 'Ticker' : leg.targetType === 'etf' ? 'ETF' : 'Basket'}</span>}
            </div>
          )}
        </div>
        {legSearchOpen === i && (
          <div className="bt-search-dropdown">
            <div className="search-filters">
              {(['all', 'Themes', 'Sectors', 'Industries', 'Tickers', 'ETFs'] as const).map(f => (
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

      {leg.targetType !== 'ticker' && leg.targetType !== 'etf' && (
        <div className="backtest-pos-presets" style={{ marginTop: 4 }}>
          <button className={`backtest-pos-preset wide ${leg.targetType === 'basket' ? 'active' : ''}`}
            onClick={() => {
              const validMetrics = [...BASKET_STAT_METRICS, ...BASKET_BOOL_METRICS]
              const cleanedFilters = leg.filters.filter(f => validMetrics.includes(f.metric))
              updateLeg(i, { targetType: 'basket', filters: cleanedFilters })
            }}>Basket</button>
          <button className={`backtest-pos-preset wide ${leg.targetType === 'basket_tickers' ? 'active' : ''}`}
            onClick={() => {
              const validMetrics = [...TICKER_STAT_METRICS, ...TICKER_BOOL_METRICS]
              const cleanedFilters = leg.filters.filter(f => validMetrics.includes(f.metric))
              updateLeg(i, { targetType: 'basket_tickers', filters: cleanedFilters })
            }}>Constituents</button>
        </div>
      )}

      <div className="backtest-section">
        <label className="backtest-label">Entry Signal</label>
        <select className="backtest-select" value={leg.entrySignal}
          onChange={e => updateLeg(i, { entrySignal: e.target.value, exitSignal: null, stopSignal: null })}>
          {ENTRY_SIGNALS.map(s => <option key={s} value={s}>{s === 'Long' ? 'Long' : s === 'Short' ? 'Short' : s.replace(/_/g, ' ')}</option>)}
        </select>
        {['Long', 'Short'].includes(leg.entrySignal) && (
          <span className="backtest-hint">Exit: End of Period</span>
        )}
      </div>

      {!['Long', 'Short'].includes(leg.entrySignal) && (
        <div className="backtest-section">
          <div className="backtest-sizing-row">
            <div className="backtest-sizing-field" style={{ flex: 1 }}>
              <label className="backtest-label">Exit Target</label>
              <select className="backtest-select"
                value={leg.exitSignal === null ? 'default' : leg.exitSignal === 'none' ? 'none' : leg.exitSignal?.startsWith('rv_') ? 'rv' : leg.exitSignal}
                onChange={e => {
                  const v = e.target.value
                  if (v === 'rv') updateLeg(i, { exitSignal: 'rv_2' })
                  else if (v === 'default') updateLeg(i, { exitSignal: null })
                  else if (v === 'none') updateLeg(i, { exitSignal: 'none' })
                  else updateLeg(i, { exitSignal: v })
                }}>
                <option value="default">{DEFAULT_EXIT_MAP[leg.entrySignal]?.replace(/_/g, ' ')} (Default)</option>
                <option value="none">None</option>
                <optgroup label="Signal">
                  {EXIT_SIGNAL_OPTIONS.filter(s => s !== DEFAULT_EXIT_MAP[leg.entrySignal] && s !== leg.stopSignal)
                    .map(s => <option key={s} value={s}>{s.replace(/_/g, ' ')}</option>)}
                </optgroup>
                <optgroup label="Price Level">
                  <option value="rv">RVol Target</option>
                </optgroup>
              </select>
              {leg.exitSignal?.startsWith('rv_') && (
                <div style={{ display: 'flex', alignItems: 'center', gap: 4, marginTop: 2, width: '100%' }}>
                  <input type="number" step="0.5" min="0.1" className="backtest-input"
                    value={parseFloat(leg.exitSignal.slice(3)) || 2}
                    onChange={e => updateLeg(i, { exitSignal: `rv_${e.target.value}` })}
                    style={{ width: '100%' }} />
                </div>
              )}
            </div>
            <div className="backtest-sizing-field" style={{ flex: 1 }}>
              <label className="backtest-label">Stop</label>
              <select className="backtest-select"
                value={leg.stopSignal === null ? 'none' : leg.stopSignal?.startsWith('trv_') ? 'trv' : leg.stopSignal?.startsWith('rv_') ? 'rv' : leg.stopSignal}
                onChange={e => {
                  const v = e.target.value
                  if (v === 'rv') updateLeg(i, { stopSignal: 'rv_1.5' })
                  else if (v === 'trv') updateLeg(i, { stopSignal: 'trv_1.5' })
                  else if (v === 'none') updateLeg(i, { stopSignal: null })
                  else updateLeg(i, { stopSignal: v })
                }}>
                <option value="none">None</option>
                <optgroup label="Signal">
                  {EXIT_SIGNAL_OPTIONS.filter(s => s !== (leg.exitSignal || DEFAULT_EXIT_MAP[leg.entrySignal]))
                    .map(s => <option key={s} value={s}>{s.replace(/_/g, ' ')}</option>)}
                </optgroup>
                <optgroup label="Price Level">
                  <option value="rv">RVol Stop</option>
                  <option value="trv">Trailing RVol Stop</option>
                </optgroup>
              </select>
              {(leg.stopSignal?.startsWith('rv_') || leg.stopSignal?.startsWith('trv_')) && (
                <div style={{ display: 'flex', alignItems: 'center', gap: 4, marginTop: 2, width: '100%' }}>
                  <input type="number" step="0.5" min="0.1" className="backtest-input"
                    value={parseFloat(leg.stopSignal.replace('trv_', '').replace('rv_', '')) || 1.5}
                    onChange={e => {
                      const prefix = leg.stopSignal?.startsWith('trv_') ? 'trv_' : 'rv_'
                      updateLeg(i, { stopSignal: `${prefix}${e.target.value}` })
                    }}
                    style={{ width: '100%' }} />
                </div>
              )}
            </div>
          </div>
        </div>
      )}

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
        <label className="backtest-label">Filters</label>
        {leg.filters.map((f, fi) => {
          const isBasket = leg.targetType === 'basket'
          const isConstituents = leg.targetType === 'basket_tickers'
          const statMetrics = isBasket ? BASKET_STAT_METRICS : TICKER_STAT_METRICS
          const boolMetrics = isBasket ? BASKET_BOOL_METRICS : TICKER_BOOL_METRICS
          const rawMetric = stripBasketPrefix(f.metric)
          const isBool = [...BASKET_BOOL_METRICS, ...TICKER_BOOL_METRICS].includes(rawMetric)
          const isReturn = rawMetric === 'Return'
          const showLookback = f.condition === 'increasing' || f.condition === 'decreasing' || isReturn
          return (
            <div key={fi} style={{ marginBottom: 6 }}>
              <div className="backtest-filter-row">
                <select className="backtest-select" value={f.metric}
                  onChange={e => {
                    const m = e.target.value
                    const raw = stripBasketPrefix(m)
                    const patch: Record<string, any> = { metric: m }
                    if (BASKET_BOOL_METRICS.includes(raw) || TICKER_BOOL_METRICS.includes(raw)) patch.condition = 'equals_true'
                    if (raw === 'Return') patch.lookback = 252
                    patch.source = isBasketPrefixed(m) ? leg.target : 'self'
                    updateFilterField(i, fi, patch)
                  }}>
                  <optgroup label="Stat">
                    {statMetrics.map(m => <option key={m} value={m}>{METRIC_DISPLAY[m] || m}</option>)}
                  </optgroup>
                  <optgroup label="Boolean">
                    {boolMetrics.map(m => <option key={m} value={m}>{METRIC_DISPLAY[m] || m}</option>)}
                  </optgroup>
                  {isConstituents && (
                    <>
                      <optgroup label="Basket">
                        {CONSTITUENTS_BASKET_STAT.map(m => <option key={m} value={m}>{METRIC_DISPLAY[m] || m}</option>)}
                      </optgroup>
                      <optgroup label="Basket Boolean">
                        {CONSTITUENTS_BASKET_BOOL.map(m => <option key={m} value={m}>{METRIC_DISPLAY[m] || m}</option>)}
                      </optgroup>
                    </>
                  )}
                </select>
                <select className="backtest-select" value={f.condition}
                  onChange={e => updateFilterField(i, fi, 'condition', e.target.value)}>
                  {isBool ? (
                    <>
                      <option value="equals_true">= True</option>
                      <option value="equals_false">= False</option>
                    </>
                  ) : isReturn ? (
                    <>
                      <option value="above">Above</option>
                      <option value="below">Below</option>
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
                {!isBool && (f.condition === 'above' || f.condition === 'below') && (
                  <input type="number" className="backtest-input" value={f.value}
                    onChange={e => updateFilterField(i, fi, 'value', e.target.value)}
                    style={{ width: 60 }} />
                )}
                <button className="backtest-remove-btn" onClick={() => removeFilter(i, fi)}>X</button>
              </div>
              {showLookback && (
                <div className="backtest-pos-presets" style={{ marginTop: 2 }}>
                  <span className="backtest-preset-label">{isReturn ? 'Period:' : 'Lookback:'}</span>
                  {LOOKBACK_PRESETS.map(p => (
                    <button key={p.value}
                      className={`backtest-pos-preset ${f.lookback === p.value ? 'active' : ''}`}
                      onClick={() => updateFilterField(i, fi, 'lookback', p.value)}>{p.label}</button>
                  ))}
                  <input type="number" className="backtest-input" value={f.lookback}
                    onChange={e => updateFilterField(i, fi, 'lookback', Number(e.target.value))}
                    style={{ width: 50, marginLeft: 4 }} />
                </div>
              )}
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

            <div className="bt-run-row">
              <button className="control-btn primary" onClick={runBacktest}
                disabled={loading || (isMultiLeg && !allocValid) || !allTargetsSet || !exitsValid}>
                {loading ? `${btProgress}%` : 'Run Backtest'}
              </button>
              {loading && (
                <div className="bt-progress-container">
                  <div className="bt-progress-bar" style={{ width: `${btProgress}%` }} />
                  {btProgressMsg && <span className="bt-progress-msg">{btProgressMsg}</span>}
                </div>
              )}
            </div>
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
