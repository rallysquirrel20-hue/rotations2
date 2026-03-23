import React, { useState, useMemo, useRef, useEffect, useCallback } from 'react'
import axios from 'axios'

// ── Types ──
interface AnalogItem {
  start: string
  end: string
  similarity: number
  similarity_breakdown: Record<string, number>
  returns: Record<string, number | null>
  forward: Record<string, Record<string, number | null> | null>
  forward_series: { dates: string[]; baskets: Record<string, (number | null)[]> }
}

interface AggHorizon {
  mean: number; median: number; min: number; max: number; std: number; count: number
  per_basket: Record<string, { mean: number; median: number; min: number; max: number; std: number; count: number }>
}

interface AnalogsResponse {
  current: {
    start: string; end: string
    returns: Record<string, number | null>
    metrics: Record<string, Record<string, number | null> | number | null>
    ranks: Record<string, Record<string, number | null>>
    basket_count: number
  } | null
  analogs: AnalogItem[]
  aggregate: Record<string, AggHorizon | null>
  date_range: { min: string; max: string }
  message?: string
}

interface QueryMatch {
  date: string
  forward: Record<string, Record<string, number | null> | null>
  forward_series: { dates: string[]; baskets: Record<string, (number | null)[]> }
}

interface QueryResponse {
  matches: QueryMatch[]
  match_count: number
  total_searched: number
  aggregate: Record<string, AggHorizon | null>
  baskets: string[]
  date_range: { min: string; max: string }
}

interface Condition {
  id: number
  basket: string    // slug, "*sectors", "*themes", "*industries"
  metric: string    // return, uptrend_pct, breakout_pct, correlation_pct, rv_ema
  timeframe: string // 1D, 1W, 1M, 1Q, 1Y
  operator: string  // positive, negative, top_n, bottom_n, above, below
  value: number
}

interface BasketsData { Themes: string[]; Sectors: string[]; Industries: string[] }

interface AnalogsPanelProps {
  apiBase: string
  exportTrigger?: number
  allBaskets?: BasketsData
}

type AnalogTab = 'summary' | 'forward' | 'aggregate'

const METRIC_OPTIONS = [
  { value: 'return', label: 'Return' },
  { value: 'rv_ema', label: 'Volatility' },
  { value: 'correlation_pct', label: 'Correlation' },
  { value: 'uptrend_pct', label: 'Breadth' },
  { value: 'breakout_pct', label: 'Breakout' },
]

const TF_OPTIONS = [
  { value: '1D', label: '1D' },
  { value: '1W', label: '1W' },
  { value: '1M', label: '1M' },
  { value: '1Q', label: '1Q' },
  { value: '1Y', label: '1Y' },
]

const OPERATOR_OPTIONS = [
  { value: 'top_n', label: 'Top N' },
  { value: 'bottom_n', label: 'Bottom N' },
  { value: 'above', label: 'Above' },
  { value: 'below', label: 'Below' },
]

// Map summary grid factors to condition metric + timeframe
const FACTOR_TO_COND: Record<string, { metric: string; timeframe: string }> = {}
for (const tf of ['1D', '1W', '1M', '1Q', '1Y', '3Y']) {
  FACTOR_TO_COND[`returns_${tf}`] = { metric: 'return', timeframe: tf }
  FACTOR_TO_COND[`rv_ema_${tf}`] = { metric: 'rv_ema', timeframe: tf }
  FACTOR_TO_COND[`correlation_pct_${tf}`] = { metric: 'correlation_pct', timeframe: tf }
  FACTOR_TO_COND[`uptrend_pct_${tf}`] = { metric: 'uptrend_pct', timeframe: tf }
  FACTOR_TO_COND[`breakout_pct_${tf}`] = { metric: 'breakout_pct', timeframe: tf }
}

function rankColor(rank: number, total: number): string {
  if (total <= 1) return 'rgb(50, 50, 255)'
  const t = rank / (total - 1)
  // rgb(50,50,255) blue → rgb(152,50,202) purple → rgb(255,50,150) pink
  if (t <= 0.5) {
    const s = t * 2
    return `rgb(${Math.round(50 + 102 * s)}, 50, ${Math.round(255 - 53 * s)})`
  } else {
    const s = (t - 0.5) * 2
    return `rgb(${Math.round(152 + 103 * s)}, 50, ${Math.round(202 - 52 * s)})`
  }
}

function fmtPct(v: number | null | undefined): string {
  if (v === null || v === undefined) return '--'
  return (v * 100).toFixed(2) + '%'
}

export function AnalogsPanel({ apiBase, exportTrigger }: AnalogsPanelProps) {
  const [tab, setTab] = useState<AnalogTab>('summary')
  const [loading, setLoading] = useState(false)
  const [data, setData] = useState<AnalogsResponse | null>(null)

  // Query state
  const [conditions, setConditions] = useState<Condition[]>([])
  const [condIdCounter, setCondIdCounter] = useState(0)
  const [queryData, setQueryData] = useState<QueryResponse | null>(null)
  const [queryLoading, setQueryLoading] = useState(false)
  const [selectedMatch, setSelectedMatch] = useState<number | null>(null)

  // Canvas refs
  const fwdCanvasRef = useRef<HTMLCanvasElement>(null)
  const fwdContainerRef = useRef<HTMLDivElement>(null)
  const [fwdDims, setFwdDims] = useState({ w: 800, h: 400 })
  const [fwdHovered, setFwdHovered] = useState<string | null>(null)
  const [fwdLogScale, setFwdLogScale] = useState(false)
  const [fwdHorizon, setFwdHorizon] = useState<number>(252) // max trading days to show
  const [fwdSortCol, setFwdSortCol] = useState<'basket' | 'change'>('change')
  const [fwdSortAsc, setFwdSortAsc] = useState(false)
  const [summarySortCol, setSummarySortCol] = useState<string>('returns_1Y')
  const [summarySortAsc, setSummarySortAsc] = useState(true)
  const [summaryShowRanks, setSummaryShowRanks] = useState(false)
  const [aggSelectedBasket, setAggSelectedBasket] = useState<string | null>(null)

  const [renderTick, setRenderTick] = useState(0)
  useEffect(() => { const t = setTimeout(() => setRenderTick(v => v + 1), 50); return () => clearTimeout(t) }, [tab, selectedMatch, aggSelectedBasket])
  const [aggHorizon, setAggHorizon] = useState<number>(252)
  const aggCanvasRef = useRef<HTMLCanvasElement>(null)
  const aggContainerRef = useRef<HTMLDivElement>(null)
  const [aggDims, setAggDims] = useState({ w: 800, h: 400 })
  const [aggLogScale, setAggLogScale] = useState(false)
  const [aggMatchSortCol, setAggMatchSortCol] = useState<'date' | 'change'>('change')
  const [aggMatchSortAsc, setAggMatchSortAsc] = useState(false)
  const [aggHoveredMatch, setAggHoveredMatch] = useState<number | null>(null)
  const [aggBasketSortCol, setAggBasketSortCol] = useState<'basket' | 'change'>('change')
  const [aggBasketSortAsc, setAggBasketSortAsc] = useState(false)

  // Fetch analogs data (for summary grid) — always all baskets, 1Y window
  useEffect(() => {
    if (!apiBase) return
    setLoading(true)
    // First get date range, then fetch analogs with 1Y window
    axios.get(`${apiBase}/baskets/returns`)
      .then(res => {
        const dr = res.data.date_range
        if (!dr?.max) return
        const d = new Date(dr.max)
        d.setDate(d.getDate() - 365)
        const start = d.toISOString().slice(0, 10)
        const params = new URLSearchParams({ start, end: dr.max, mode: 'analogs', group: 'all' })
        return axios.get(`${apiBase}/baskets/returns?${params}`)
      })
      .then(res => { if (res) setData(res.data) })
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }, [apiBase])

  // Sorted basket slugs from current returns
  const sortedSlugs = useMemo(() => {
    if (!data?.current) return []
    return Object.entries(data.current.returns)
      .filter(([, v]) => v !== null)
      .sort((a, b) => (a[1] as number) - (b[1] as number))
      .map(([k]) => k)
  }, [data])

  // All basket slugs for condition dropdown
  const allBasketOptions = useMemo(() => {
    const slugs = queryData?.baskets ?? sortedSlugs
    const groups = [
      { value: '*sectors', label: 'All Sectors' },
      { value: '*themes', label: 'All Themes' },
      { value: '*industries', label: 'All Industries' },
    ]
    return [...groups, ...slugs.filter(s => typeof s === 'string').map(s => ({ value: s, label: s.replace(/_/g, ' ') }))]
  }, [sortedSlugs, queryData])

  // ResizeObserver for forward canvas
  useEffect(() => {
    const el = fwdContainerRef.current
    if (!el) return
    const obs = new ResizeObserver(entries => {
      const { width, height } = entries[0].contentRect
      if (width > 0 && height > 0) setFwdDims({ w: width, h: height })
    })
    obs.observe(el)
    return () => obs.disconnect()
  }, [tab])

  // ResizeObserver for aggregate canvas
  useEffect(() => {
    const el = aggContainerRef.current
    if (!el) return
    const obs = new ResizeObserver(entries => {
      const { width, height } = entries[0].contentRect
      if (width > 0 && height > 0) setAggDims({ w: width, h: height })
    })
    obs.observe(el)
    return () => obs.disconnect()
  }, [tab])

  // ── Condition management ──
  const addCondition = (basket: string, factor: string, rank: number, B: number) => {
    const op = rank <= B / 2 ? 'bottom_n' : 'top_n'
    const mapped = FACTOR_TO_COND[factor] || { metric: 'return', timeframe: '1D' }
    setConditions(prev => [...prev, {
      id: condIdCounter, basket, metric: mapped.metric, timeframe: mapped.timeframe,
      operator: op, value: 5,
    }])
    setCondIdCounter(prev => prev + 1)
  }

  const addEmptyCondition = () => {
    setConditions(prev => [...prev, {
      id: condIdCounter, basket: sortedSlugs[0] || '',
      metric: 'return', timeframe: '1D', operator: 'below', value: 0,
    }])
    setCondIdCounter(prev => prev + 1)
  }

  const updateCondition = (id: number, field: keyof Condition, value: string | number) => {
    setConditions(prev => prev.map(c => c.id === id ? { ...c, [field]: value } : c))
  }

  const removeCondition = (id: number) => {
    setConditions(prev => prev.filter(c => c.id !== id))
  }

  const runQuery = () => {
    if (conditions.length === 0 || !apiBase) return
    setQueryLoading(true)
    const params = new URLSearchParams({
      mode: 'query', group: 'all',
      conditions: JSON.stringify(conditions.map(c => ({
        basket: c.basket, metric: `${c.metric}_${c.timeframe}`, operator: c.operator, value: c.value,
      }))),
    })
    axios.get(`${apiBase}/baskets/returns?${params}`)
      .then(res => {
        setQueryData(res.data)
        setSelectedMatch(res.data.matches?.length > 0 ? 0 : null)
        setTab('forward')
      })
      .catch(() => setQueryData({ matches: [], match_count: 0, total_searched: 0, aggregate: {}, baskets: [], date_range: { min: '', max: '' } }))
      .finally(() => setQueryLoading(false))
  }

  // ── SUMMARY: 5 metric grids ──
  type SummaryMetric = 'returns' | 'rv_ema' | 'correlation_pct' | 'uptrend_pct' | 'breakout_pct'
  const [summaryMetric, setSummaryMetric] = useState<SummaryMetric>('returns')
  const SUMMARY_METRIC_OPTIONS: { value: SummaryMetric; label: string }[] = [
    { value: 'returns', label: 'Returns' },
    { value: 'rv_ema', label: 'Volatility' },
    { value: 'correlation_pct', label: 'Correlation' },
    { value: 'uptrend_pct', label: 'Breadth' },
    { value: 'breakout_pct', label: 'Breakout%' },
  ]
  const TF_LABELS = ['1D', '1W', '1M', '1Q', '1Y', '3Y'] as const
  const summaryFactors = useMemo(() =>
    TF_LABELS.map(tf => summaryMetric === 'returns' ? `returns_${tf}` : `${summaryMetric}_${tf}`),
    [summaryMetric]
  )
  const FACTOR_LABELS: Record<string, string> = {}
  for (const tf of TF_LABELS) {
    FACTOR_LABELS[`returns_${tf}`] = tf
    FACTOR_LABELS[`rv_ema_${tf}`] = tf
    FACTOR_LABELS[`correlation_pct_${tf}`] = tf
    FACTOR_LABELS[`uptrend_pct_${tf}`] = tf
    FACTOR_LABELS[`breakout_pct_${tf}`] = tf
  }

  const summaryRows = useMemo(() => {
    if (!data?.current) return { rows: [] as { slug: string; cells: { factor: string; value: number | null; rank: number | null }[]; avgRank: number; B: number }[], factors: [] as string[], B: 0 }
    const { returns, metrics, ranks, basket_count } = data.current
    const B = basket_count
    const factors = summaryFactors.filter(f => ranks[f] !== undefined)
    const rows = sortedSlugs.map(slug => {
      const cells: { factor: string; value: number | null; rank: number | null }[] = []
      for (const f of factors) {
        let value: number | null = null
        const m = metrics[f]
        if (m && typeof m === 'object') value = (m as Record<string, number | null>)[slug] ?? null
        const rank = ranks[f]?.[slug] ?? null
        cells.push({ factor: f, value, rank })
      }
      return { slug, cells, avgRank: 0, B }
    })

    if (summarySortCol === 'name') {
      rows.sort((a, b) => summarySortAsc ? a.slug.localeCompare(b.slug) : b.slug.localeCompare(a.slug))
    } else {
      const fi = factors.indexOf(summarySortCol)
      if (fi >= 0) {
        rows.sort((a, b) => {
          const ra = a.cells[fi].rank ?? B
          const rb = b.cells[fi].rank ?? B
          return summarySortAsc ? ra - rb : rb - ra
        })
      }
    }
    return { rows, factors, B }
  }, [data, sortedSlugs, summarySortCol, summarySortAsc, summaryFactors])

  const handleSummarySort = (col: string) => {
    if (summarySortCol === col) setSummarySortAsc(prev => !prev)
    else { setSummarySortCol(col); setSummarySortAsc(false) }
  }

  // ── FORWARD TAB: cumulative line chart ──
  const fwdSeries = useMemo(() => {
    if (selectedMatch === null || !queryData) return null
    const match = queryData.matches[selectedMatch]
    if (!match?.forward_series) return null
    const fs = match.forward_series
    // Clip to selected horizon
    const clippedDates = fs.dates.slice(0, fwdHorizon)
    const clippedBaskets: Record<string, (number | null)[]> = {}
    for (const [slug, vals] of Object.entries(fs.baskets)) {
      clippedBaskets[slug] = vals.slice(0, fwdHorizon)
    }
    return { dates: clippedDates, baskets: clippedBaskets }
  }, [queryData, selectedMatch, fwdHorizon])

  useEffect(() => {
    if (tab !== 'forward' || !fwdSeries) return
    const canvas = fwdCanvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    const dpr = window.devicePixelRatio || 1
    const w = fwdDims.w, h = fwdDims.h
    canvas.width = w * dpr; canvas.height = h * dpr
    ctx.scale(dpr, dpr); ctx.clearRect(0, 0, w, h)
    ctx.fillStyle = '#fdf6e3'; ctx.fillRect(0, 0, w, h)

    const fs = fwdSeries
    const dates = fs.dates
    const nDates = dates.length
    if (nDates === 0) return

    const baskets = queryData!.baskets
    const series: { slug: string; values: (number | null)[]; finalRet: number }[] = []
    for (const slug of baskets) {
      const vals = fs.baskets[slug]
      if (!vals || vals.length === 0) continue
      const lastValid = [...vals].reverse().find(v => v !== null)
      series.push({ slug, values: vals, finalRet: lastValid ?? 0 })
    }
    series.sort((a, b) => b.finalRet - a.finalRet)

    const pad = { top: 20, right: 60, bottom: 40, left: 10 }
    const plotW = w - pad.left - pad.right, plotH = h - pad.top - pad.bottom

    let yMin = 0, yMax = 0
    series.forEach(s => s.values.forEach(v => { if (v !== null) { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) } }))
    const yPad = (yMax - yMin) * 0.1 || 0.01
    yMin -= yPad; yMax += yPad

    const xScale = (i: number) => pad.left + (i / (nDates - 1 || 1)) * plotW
    const logT = (v: number) => Math.log(Math.max(1 + v, 1e-8))
    const logYMin = fwdLogScale ? logT(yMin) : 0
    const logYMax = fwdLogScale ? logT(yMax) : 0
    const yScale = fwdLogScale
      ? (v: number) => pad.top + plotH - ((logT(v) - logYMin) / (logYMax - logYMin)) * plotH
      : (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH

    // Grid
    ctx.strokeStyle = '#e9ecef'; ctx.lineWidth = 1
    for (let i = 0; i <= 5; i++) {
      const v = fwdLogScale ? Math.exp(logYMin + (logYMax - logYMin) * (i / 5)) - 1 : yMin + (yMax - yMin) * (i / 5)
      const y = yScale(v)
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(w - pad.right, y); ctx.stroke()
      ctx.fillStyle = '#6c757d'; ctx.font = '9px monospace'; ctx.textAlign = 'right'
      ctx.fillText((v * 100).toFixed(1) + '%', w - 4, y + 3)
    }

    // Zero line
    ctx.strokeStyle = '#adb5bd'; ctx.setLineDash([4, 4])
    ctx.beginPath(); ctx.moveTo(pad.left, yScale(0)); ctx.lineTo(w - pad.right, yScale(0)); ctx.stroke()
    ctx.setLineDash([])

    // X dates
    ctx.fillStyle = '#93a1a1'; ctx.font = '9px monospace'; ctx.textAlign = 'center'
    const nLabels = Math.min(6, nDates)
    for (let li = 0; li < nLabels; li++) {
      const idx = Math.round(li * (nDates - 1) / (nLabels - 1 || 1))
      ctx.fillText(dates[idx], xScale(idx), h - pad.bottom + 14)
    }

    // Lines
    const totalSeries = series.length
    series.forEach((s, rank) => {
      const isHovered = fwdHovered === s.slug
      const isOther = fwdHovered !== null && !isHovered
      ctx.strokeStyle = isOther ? '#dee2e6' : rankColor(rank, totalSeries)
      ctx.lineWidth = isHovered ? 2.5 : 1.2
      ctx.globalAlpha = isOther ? 0.3 : 1
      ctx.beginPath()
      let inSegment = false
      s.values.forEach((v, i) => {
        if (v === null) { inSegment = false; return }
        const x = xScale(i), y = yScale(v)
        if (!inSegment) { ctx.moveTo(x, y); inSegment = true }
        else ctx.lineTo(x, y)
      })
      ctx.stroke()
      ctx.globalAlpha = 1
    })

    // Title
    const match = queryData!.matches[selectedMatch!]
    ctx.fillStyle = '#586e75'; ctx.font = 'bold 10px monospace'; ctx.textAlign = 'left'
    ctx.fillText(`FORWARD RETURNS from ${match.date}`, pad.left, 14)
  }, [tab, fwdSeries, queryData, selectedMatch, fwdDims, fwdHovered, renderTick, fwdLogScale])

  // Build ranked series for legend (sorted by return for color assignment, then user-sortable)
  const fwdRankedSeries = useMemo(() => {
    if (!fwdSeries || !queryData) return []
    const baskets = queryData.baskets
    const series: { slug: string; finalRet: number }[] = []
    for (const slug of baskets) {
      const vals = fwdSeries.baskets[slug]
      if (!vals || vals.length === 0) continue
      const lastValid = [...vals].reverse().find(v => v !== null)
      series.push({ slug, finalRet: lastValid ?? 0 })
    }
    return series.sort((a, b) => b.finalRet - a.finalRet)
  }, [fwdSeries, queryData])

  const fwdColorMap = useMemo(() => {
    const map = new Map<string, string>()
    fwdRankedSeries.forEach((s, rank) => map.set(s.slug, rankColor(rank, fwdRankedSeries.length)))
    return map
  }, [fwdRankedSeries])

  const fwdSortedLegend = useMemo(() => {
    const items = [...fwdRankedSeries]
    if (fwdSortCol === 'basket') {
      items.sort((a, b) => fwdSortAsc ? a.slug.localeCompare(b.slug) : b.slug.localeCompare(a.slug))
    } else {
      items.sort((a, b) => fwdSortAsc ? a.finalRet - b.finalRet : b.finalRet - a.finalRet)
    }
    return items
  }, [fwdRankedSeries, fwdSortCol, fwdSortAsc])

  // ── AGGREGATE TAB: mean path + stdev band per basket ──
  // Auto-select first basket when query results come in
  useEffect(() => {
    if (queryData?.baskets?.length && !aggSelectedBasket) {
      setAggSelectedBasket(queryData.baskets[0])
    }
  }, [queryData])

  // Compute avg return per basket for left sidebar
  const aggBasketList = useMemo(() => {
    if (!queryData?.baskets || !queryData.matches) return []
    const items = queryData.baskets.map(slug => {
      const vals: number[] = []
      for (const match of queryData.matches) {
        const series = match.forward_series?.baskets?.[slug]
        if (!series || series.length === 0) continue
        const clipped = series.slice(0, aggHorizon)
        const last = [...clipped].reverse().find(v => v !== null)
        if (last !== undefined && last !== null) vals.push(last)
      }
      const avg = vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : null
      return { slug, avg }
    })
    if (aggBasketSortCol === 'basket') {
      items.sort((a, b) => aggBasketSortAsc ? a.slug.localeCompare(b.slug) : b.slug.localeCompare(a.slug))
    } else {
      items.sort((a, b) => {
        const va = a.avg ?? (aggBasketSortAsc ? Infinity : -Infinity)
        const vb = b.avg ?? (aggBasketSortAsc ? Infinity : -Infinity)
        return aggBasketSortAsc ? va - vb : vb - va
      })
    }
    return items
  }, [queryData, aggHorizon, aggBasketSortCol, aggBasketSortAsc])

  // Compute per-match returns for the selected basket (for right-hand list)
  const aggMatchReturns = useMemo(() => {
    if (!queryData?.matches || !aggSelectedBasket) return []
    return queryData.matches.map((match, mi) => {
      const vals = match.forward_series?.baskets?.[aggSelectedBasket]
      if (!vals || vals.length === 0) return { mi, date: match.date, ret: null as number | null }
      const clipped = vals.slice(0, aggHorizon)
      const lastValid = [...clipped].reverse().find(v => v !== null)
      return { mi, date: match.date, ret: lastValid ?? null }
    })
  }, [queryData, aggSelectedBasket, aggHorizon])

  const aggSortedMatches = useMemo(() => {
    const items = [...aggMatchReturns]
    if (aggMatchSortCol === 'date') {
      items.sort((a, b) => aggMatchSortAsc ? a.date.localeCompare(b.date) : b.date.localeCompare(a.date))
    } else {
      items.sort((a, b) => {
        const va = a.ret ?? (aggMatchSortAsc ? Infinity : -Infinity)
        const vb = b.ret ?? (aggMatchSortAsc ? Infinity : -Infinity)
        return aggMatchSortAsc ? va - vb : vb - va
      })
    }
    return items
  }, [aggMatchReturns, aggMatchSortCol, aggMatchSortAsc])

  // Compute mean path and stdev bands
  const aggPathData = useMemo(() => {
    if (!queryData?.matches || !aggSelectedBasket) return null
    const allSeries: (number | null)[][] = []
    let maxLen = 0
    for (const match of queryData.matches) {
      const vals = match.forward_series?.baskets?.[aggSelectedBasket]
      if (!vals || vals.length === 0) continue
      const clipped = vals.slice(0, aggHorizon)
      allSeries.push(clipped)
      maxLen = Math.max(maxLen, clipped.length)
    }
    if (allSeries.length === 0 || maxLen === 0) return null

    const mean: number[] = []
    const upper: number[] = []
    const lower: number[] = []
    const dates: string[] = []
    for (let d = 0; d < maxLen; d++) {
      const vals: number[] = []
      for (const s of allSeries) {
        const v = d < s.length ? s[d] : null
        if (v !== null) vals.push(v)
      }
      if (vals.length === 0) break
      const m = vals.reduce((a, b) => a + b, 0) / vals.length
      const std = Math.sqrt(vals.reduce((a, b) => a + (b - m) ** 2, 0) / vals.length)
      mean.push(m)
      upper.push(m + std)
      lower.push(m - std)
      dates.push(String(d + 1))
    }

    // Also collect individual match lines for hover
    const individualPaths: { mi: number; values: (number | null)[] }[] = []
    queryData.matches.forEach((match, mi) => {
      const vals = match.forward_series?.baskets?.[aggSelectedBasket]
      if (!vals || vals.length === 0) return
      individualPaths.push({ mi, values: vals.slice(0, aggHorizon) })
    })

    return { mean, upper, lower, dates, count: allSeries.length, individualPaths }
  }, [queryData, aggSelectedBasket, aggHorizon])

  // Draw aggregate chart
  useEffect(() => {
    if (tab !== 'aggregate' || !aggPathData) return
    const canvas = aggCanvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    const dpr = window.devicePixelRatio || 1
    const w = aggDims.w, h = aggDims.h
    canvas.width = w * dpr; canvas.height = h * dpr
    ctx.scale(dpr, dpr); ctx.clearRect(0, 0, w, h)
    ctx.fillStyle = '#fdf6e3'; ctx.fillRect(0, 0, w, h)

    const { mean, upper, lower, dates, individualPaths } = aggPathData
    const nDates = mean.length
    if (nDates === 0) return

    const pad = { top: 20, right: 60, bottom: 40, left: 10 }
    const plotW = w - pad.left - pad.right, plotH = h - pad.top - pad.bottom

    let yMin = Infinity, yMax = -Infinity
    lower.forEach(v => { yMin = Math.min(yMin, v) })
    upper.forEach(v => { yMax = Math.max(yMax, v) })
    // Also consider individual hovered path
    if (aggHoveredMatch !== null) {
      const hp = individualPaths.find(p => p.mi === aggHoveredMatch)
      if (hp) hp.values.forEach(v => { if (v !== null) { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) } })
    }
    const yPad = (yMax - yMin) * 0.1 || 0.01
    yMin -= yPad; yMax += yPad

    const xScale = (i: number) => pad.left + (i / (nDates - 1 || 1)) * plotW
    const logT = (v: number) => Math.log(Math.max(1 + v, 1e-8))
    const logYMin = aggLogScale ? logT(yMin) : 0
    const logYMax = aggLogScale ? logT(yMax) : 0
    const yScale = aggLogScale
      ? (v: number) => pad.top + plotH - ((logT(v) - logYMin) / (logYMax - logYMin)) * plotH
      : (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH

    // Grid
    ctx.strokeStyle = '#e9ecef'; ctx.lineWidth = 1
    for (let i = 0; i <= 5; i++) {
      const v = aggLogScale ? Math.exp(logYMin + (logYMax - logYMin) * (i / 5)) - 1 : yMin + (yMax - yMin) * (i / 5)
      const y = yScale(v)
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(w - pad.right, y); ctx.stroke()
      ctx.fillStyle = '#6c757d'; ctx.font = '9px monospace'; ctx.textAlign = 'right'
      ctx.fillText((v * 100).toFixed(1) + '%', w - 4, y + 3)
    }

    // Zero line
    ctx.strokeStyle = '#adb5bd'; ctx.setLineDash([4, 4])
    ctx.beginPath(); ctx.moveTo(pad.left, yScale(0)); ctx.lineTo(w - pad.right, yScale(0)); ctx.stroke()
    ctx.setLineDash([])

    // X axis labels (day numbers)
    ctx.fillStyle = '#93a1a1'; ctx.font = '9px monospace'; ctx.textAlign = 'center'
    const nLabels = Math.min(6, nDates)
    for (let li = 0; li < nLabels; li++) {
      const idx = Math.round(li * (nDates - 1) / (nLabels - 1 || 1))
      ctx.fillText(dates[idx], xScale(idx), h - pad.bottom + 14)
    }

    // Stdev band (filled area)
    ctx.fillStyle = 'rgba(152, 50, 202, 0.12)'
    ctx.beginPath()
    for (let i = 0; i < nDates; i++) ctx.lineTo(xScale(i), yScale(upper[i]))
    for (let i = nDates - 1; i >= 0; i--) ctx.lineTo(xScale(i), yScale(lower[i]))
    ctx.closePath()
    ctx.fill()

    // Stdev boundary lines
    ctx.strokeStyle = 'rgba(152, 50, 202, 0.3)'; ctx.lineWidth = 1; ctx.setLineDash([3, 3])
    ctx.beginPath()
    for (let i = 0; i < nDates; i++) { i === 0 ? ctx.moveTo(xScale(i), yScale(upper[i])) : ctx.lineTo(xScale(i), yScale(upper[i])) }
    ctx.stroke()
    ctx.beginPath()
    for (let i = 0; i < nDates; i++) { i === 0 ? ctx.moveTo(xScale(i), yScale(lower[i])) : ctx.lineTo(xScale(i), yScale(lower[i])) }
    ctx.stroke()
    ctx.setLineDash([])

    // Hovered individual match path
    if (aggHoveredMatch !== null) {
      const hp = individualPaths.find(p => p.mi === aggHoveredMatch)
      if (hp) {
        ctx.strokeStyle = 'rgba(88, 110, 117, 0.5)'; ctx.lineWidth = 1.5
        ctx.beginPath()
        let inSeg = false
        hp.values.forEach((v, i) => {
          if (v === null || i >= nDates) { inSeg = false; return }
          const x = xScale(i), y = yScale(v)
          if (!inSeg) { ctx.moveTo(x, y); inSeg = true } else ctx.lineTo(x, y)
        })
        ctx.stroke()
      }
    }

    // Mean line
    ctx.strokeStyle = 'rgb(152, 50, 202)'; ctx.lineWidth = 2.5
    ctx.beginPath()
    for (let i = 0; i < nDates; i++) { i === 0 ? ctx.moveTo(xScale(i), yScale(mean[i])) : ctx.lineTo(xScale(i), yScale(mean[i])) }
    ctx.stroke()

    // Title
    ctx.fillStyle = '#586e75'; ctx.font = 'bold 10px monospace'; ctx.textAlign = 'left'
    ctx.fillText(`${aggSelectedBasket?.replace(/_/g, ' ').toUpperCase()} — AVG FORWARD (n=${aggPathData.count})`, pad.left, 14)
  }, [tab, aggPathData, aggDims, aggLogScale, aggSelectedBasket, renderTick, aggHoveredMatch])

  // Export
  const prevExportTrigger = useRef(exportTrigger || 0)
  useEffect(() => {
    if (!exportTrigger || exportTrigger === prevExportTrigger.current) {
      prevExportTrigger.current = exportTrigger || 0; return
    }
    prevExportTrigger.current = exportTrigger
    const canvas = tab === 'forward' ? fwdCanvasRef.current : null
    if (!canvas) return
    canvas.toBlob(blob => {
      if (!blob) return
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url; a.download = `query_${tab}.png`; a.click()
      URL.revokeObjectURL(url)
    }, 'image/png')
  }, [exportTrigger, tab])

  // ── RENDER ──
  const TABS: { key: AnalogTab; label: string; disabled?: boolean }[] = [
    { key: 'summary', label: 'Summary' },
    { key: 'forward', label: `Forward${queryData ? ` (${queryData.match_count})` : ''}` },
    { key: 'aggregate', label: 'Aggregate' },
  ]

  const needsValue = (op: string) => ['top_n', 'bottom_n', 'above', 'below'].includes(op)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0 }}>
      {/* Tab bar — matches accordion header height */}
      <div className="analysis-date-controls">
        {TABS.map(t => (
          <button key={t.key} className={`basket-returns-preset-btn ${tab === t.key ? 'active' : ''}`}
            onClick={() => setTab(t.key)} style={{ padding: '4px 10px', fontSize: 10 }}>
            {t.label}
          </button>
        ))}
        <span style={{ color: 'var(--border-color)', margin: '0 1px' }}>|</span>
        <button className="basket-returns-preset-btn" onClick={addEmptyCondition}
          style={{ padding: '4px 10px', fontSize: 10 }}>
          + Condition
        </button>
        {conditions.length > 0 && (
          <>
            <button
              className="basket-returns-preset-btn active"
              onClick={runQuery}
              disabled={queryLoading}
              style={{ padding: '4px 12px', fontSize: 10, fontWeight: 'bold', marginLeft: 4 }}
            >
              {queryLoading ? 'Searching...' : `Find Matches (${conditions.length})`}
            </button>
          </>
        )}
        {loading && <span className="analysis-loading-hint">Loading...</span>}
        <div style={{ flex: 1 }} />
        <label style={{ cursor: 'pointer', fontSize: 10, color: 'var(--text-main)' }}>
          <input type="checkbox" checked={summaryShowRanks} onChange={e => setSummaryShowRanks(e.target.checked)} style={{ marginRight: 4 }} />
          Ranks
        </label>
      </div>

      {/* Conditions panel */}
      {conditions.length > 0 && (
        <div style={{ padding: '4px 8px', borderBottom: '1px solid var(--border-color)', background: 'var(--bg-sidebar)', fontSize: 10 }}>
          {conditions.map(cond => (
            <div key={cond.id} style={{ display: 'flex', gap: 4, alignItems: 'center', marginBottom: 2 }}>
              <select value={cond.basket} onChange={e => updateCondition(cond.id, 'basket', e.target.value)}
                style={{ fontSize: 10, padding: '2px 4px', minWidth: 120, background: 'var(--bg-main)', color: 'var(--text-main)', border: '1px solid var(--border-color)' }}>
                {allBasketOptions.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
              <select value={cond.metric} onChange={e => updateCondition(cond.id, 'metric', e.target.value)}
                style={{ fontSize: 10, padding: '2px 4px', minWidth: 90, background: 'var(--bg-main)', color: 'var(--text-main)', border: '1px solid var(--border-color)' }}>
                {METRIC_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
              <select value={cond.timeframe} onChange={e => updateCondition(cond.id, 'timeframe', e.target.value)}
                style={{ fontSize: 10, padding: '2px 4px', minWidth: 40, background: 'var(--bg-main)', color: 'var(--text-main)', border: '1px solid var(--border-color)' }}>
                {TF_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
              <select value={cond.operator} onChange={e => updateCondition(cond.id, 'operator', e.target.value)}
                style={{ fontSize: 10, padding: '2px 4px', minWidth: 70, background: 'var(--bg-main)', color: 'var(--text-main)', border: '1px solid var(--border-color)' }}>
                {OPERATOR_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
              {needsValue(cond.operator) && (
                <input type="number" value={cond.value} step={cond.operator === 'above' || cond.operator === 'below' ? 0.01 : 1}
                  onChange={e => updateCondition(cond.id, 'value', parseFloat(e.target.value) || 0)}
                  style={{ fontSize: 10, padding: '2px 4px', width: 50, background: 'var(--bg-main)', color: 'var(--text-main)', border: '1px solid var(--border-color)' }} />
              )}
              <button onClick={() => removeCondition(cond.id)}
                style={{ fontSize: 10, padding: '1px 6px', cursor: 'pointer', background: 'none', border: '1px solid var(--border-color)', color: 'var(--base01)' }}>
                x
              </button>
            </div>
          ))}
        </div>
      )}

      {/* Tab content */}
      <div style={{ flex: 1, minHeight: 0, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
        {/* Summary tab */}
        {tab === 'summary' && (
          <div style={{ flex: 1, minHeight: 0, overflowY: 'auto', padding: 8 }}>
            {data?.current && summaryRows.rows.length > 0 ? (
              <>
                <div style={{ marginBottom: 8, padding: '6px 8px', background: 'var(--bg-sidebar)', borderBottom: '1px solid var(--border-color)', fontSize: 10, color: 'var(--base01)', lineHeight: 1.5 }}>
                  <strong>Current Environment</strong> ({data.current.start} to {data.current.end})
                  <br />
                  Click any cell to add a condition. Rank 1 = best.
                </div>
                <div style={{ display: 'flex', gap: 4, marginBottom: 8 }}>
                  {SUMMARY_METRIC_OPTIONS.map(opt => (
                    <button key={opt.value}
                      className={`contrib-toggle-btn ${summaryMetric === opt.value ? 'active' : ''}`}
                      onClick={() => { setSummaryMetric(opt.value); if (summaryMetric !== opt.value) { setSummarySortCol(opt.value === 'returns' ? 'returns_1Y' : `${opt.value}_1Y`); setSummarySortAsc(true) } }}>
                      {opt.label}
                    </button>
                  ))}
                </div>
                {data.current.metrics.cross_basket_corr !== undefined && data.current.metrics.cross_basket_corr !== null && (
                  <div style={{ marginBottom: 6, fontSize: 10, color: 'var(--base01)' }}>
                    Cross-Basket Correlation: <strong>{((data.current.metrics.cross_basket_corr as number) * 100).toFixed(1)}%</strong>
                  </div>
                )}
                <table className="analogs-agg-table" style={{ fontSize: 10 }}>
                  <thead>
                    <tr>
                      <th style={{ cursor: 'pointer', minWidth: 120 }} onClick={() => handleSummarySort('name')}>
                        Basket {summarySortCol === 'name' ? (summarySortAsc ? '\u25BC' : '\u25B2') : ''}
                      </th>
                      {summaryRows.factors.map(f => (
                        <th key={f} style={{ cursor: 'pointer', textAlign: 'center', minWidth: summaryShowRanks ? 90 : 65 }} onClick={() => handleSummarySort(f)}>
                          {FACTOR_LABELS[f]} {summarySortCol === f ? (summarySortAsc ? '\u25BC' : '\u25B2') : ''}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {summaryRows.rows.map(row => (
                      <tr key={row.slug}>
                        <td style={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>{row.slug.replace(/_/g, ' ')}</td>
                        {row.cells.map((cell, ci) => {
                          const bgColor = cell.rank !== null ? rankColor(cell.rank - 1, row.B) : 'transparent'
                          return (
                            <td key={ci}
                              style={{ textAlign: 'center', background: bgColor, color: '#fdf6e3', fontWeight: 'bold', cursor: 'pointer' }}
                              title={`Click to add condition: ${row.slug} ${cell.factor}`}
                              onClick={() => { if (cell.rank !== null) addCondition(row.slug, cell.factor, cell.rank, row.B) }}
                            >
                              {(() => {
                                const isReturn = cell.factor.startsWith('returns_')
                                const isRV = cell.factor.startsWith('rv_ema_')
                                const display = cell.value !== null
                                  ? isReturn ? (cell.value * 100).toFixed(1) + '%'
                                  : isRV ? (cell.value * Math.sqrt(252) * 100).toFixed(1)
                                  : cell.value.toFixed(1)
                                  : '--'
                                return summaryShowRanks ? (
                                  <span>
                                    <span style={{ opacity: 0.7, fontSize: 9 }}>{display}</span>
                                    {' \u2192 '}
                                    <span>{cell.rank ?? '--'}</span>
                                  </span>
                                ) : (
                                  <span>{display}</span>
                                )
                              })()}
                            </td>
                          )
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </>
            ) : (
              <div style={{ padding: 16, fontSize: 12, color: 'var(--base01)', fontStyle: 'italic' }}>
                {loading ? 'Loading...' : 'No data available'}
              </div>
            )}
          </div>
        )}

        {/* Forward tab */}
        {tab === 'forward' && (
          <div className="returns-container">
            {/* Left sidebar: match picker */}
            <div className="returns-legend-left contrib-sidebar">
              <div className="contrib-preset-toggle">
                {([['1M', 21], ['1Q', 63], ['1Y', 252]] as [string, number][]).map(([label, days]) => (
                  <button key={label} className={`contrib-toggle-btn ${fwdHorizon === days ? 'active' : ''}`}
                    onClick={() => setFwdHorizon(days)}>
                    {label}
                  </button>
                ))}
              </div>
              <div className="contrib-quarter-presets">
                {!queryData || !queryData.matches || queryData.matches.length === 0 ? (
                  <div style={{ padding: 8, fontSize: 10, color: 'var(--base01)', fontStyle: 'italic' }}>
                    Add conditions and click "Find Matches"
                  </div>
                ) : (
                  queryData.matches.map((match, mi) => {
                    const avgFwd = (hz: string) => {
                      const fwd = match.forward[hz]
                      if (!fwd) return null
                      const vals = Object.values(fwd).filter((v): v is number => v !== null)
                      return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : null
                    }
                    const avg1Q = avgFwd('1Q')
                    return (
                      <button key={mi}
                        className={`contrib-quarter-btn ${selectedMatch === mi ? 'active' : ''}`}
                        onClick={() => setSelectedMatch(mi)}>
                        <span>{match.date}</span>
                        {avg1Q !== null && (
                          <span style={{ float: 'right', color: selectedMatch === mi ? 'var(--base3)' : avg1Q >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>
                            {(avg1Q * 100).toFixed(1)}%
                          </span>
                        )}
                      </button>
                    )
                  })
                )}
              </div>
            </div>
            {/* Center: chart */}
            <div className="returns-right">
              <div className="returns-chart" ref={fwdContainerRef}>
                {selectedMatch !== null && queryData ? (
                  <>
                    <canvas ref={fwdCanvasRef} style={{ width: '100%', height: '100%', display: 'block' }} />
                    <button className={`log-toggle-btn ${fwdLogScale ? 'active' : ''}`} onClick={() => setFwdLogScale(v => !v)}>L</button>
                  </>
                ) : (
                  <div style={{ padding: 16, fontSize: 12, color: 'var(--base01)', fontStyle: 'italic' }}>
                    {queryData ? 'Select a match date to view forward returns' : 'Add conditions and click "Find Matches"'}
                  </div>
                )}
              </div>
            </div>
            {/* Right: basket legend */}
            <div className="backtest-path-legend">
              <div className="path-legend-header">
                <span className="path-legend-col ticker" onClick={() => {
                  if (fwdSortCol === 'basket') setFwdSortAsc(v => !v)
                  else { setFwdSortCol('basket'); setFwdSortAsc(true) }
                }}>Basket{fwdSortCol === 'basket' ? (fwdSortAsc ? ' \u25B2' : ' \u25BC') : ''}</span>
                <span className="path-legend-col change" onClick={() => {
                  if (fwdSortCol === 'change') setFwdSortAsc(v => !v)
                  else { setFwdSortCol('change'); setFwdSortAsc(false) }
                }}>Chg{fwdSortCol === 'change' ? (fwdSortAsc ? ' \u25B2' : ' \u25BC') : ''}</span>
              </div>
              {fwdSortedLegend.map(s => (
                <div key={s.slug}
                  className={`path-legend-row ${fwdHovered === s.slug ? 'highlighted' : ''}`}
                  style={{ color: fwdColorMap.get(s.slug) }}
                  onMouseEnter={() => setFwdHovered(s.slug)}
                  onMouseLeave={() => setFwdHovered(null)}>
                  <span className="path-legend-col ticker">{s.slug.replace(/_/g, ' ')}</span>
                  <span className="path-legend-col change" style={{ color: s.finalRet >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>
                    {(s.finalRet * 100).toFixed(1)}%
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Aggregate tab */}
        {tab === 'aggregate' && (
          <div className="returns-container">
            {/* Left sidebar: basket picker */}
            <div className="backtest-path-legend" style={{ borderLeft: 'none', borderRight: '2px solid var(--border-color)' }}>
              <div className="contrib-preset-toggle">
                {([['1M', 21], ['1Q', 63], ['1Y', 252]] as [string, number][]).map(([label, days]) => (
                  <button key={label} className={`contrib-toggle-btn ${aggHorizon === days ? 'active' : ''}`}
                    onClick={() => setAggHorizon(days)}>
                    {label}
                  </button>
                ))}
              </div>
              <div className="path-legend-header">
                <span className="path-legend-col ticker" onClick={() => {
                  if (aggBasketSortCol === 'basket') setAggBasketSortAsc(v => !v)
                  else { setAggBasketSortCol('basket'); setAggBasketSortAsc(true) }
                }}>Basket{aggBasketSortCol === 'basket' ? (aggBasketSortAsc ? ' \u25B2' : ' \u25BC') : ''}</span>
                <span className="path-legend-col change" onClick={() => {
                  if (aggBasketSortCol === 'change') setAggBasketSortAsc(v => !v)
                  else { setAggBasketSortCol('change'); setAggBasketSortAsc(false) }
                }}>Avg{aggBasketSortCol === 'change' ? (aggBasketSortAsc ? ' \u25B2' : ' \u25BC') : ''}</span>
              </div>
              {aggBasketList.length === 0 ? (
                <div style={{ padding: 8, fontSize: 10, color: 'var(--base01)', fontStyle: 'italic' }}>
                  Run a query first
                </div>
              ) : (
                aggBasketList.map(({ slug, avg }) => (
                  <div key={slug}
                    className={`path-legend-row ${aggSelectedBasket === slug ? 'highlighted' : ''}`}
                    style={{ cursor: 'pointer', fontWeight: aggSelectedBasket === slug ? 'bold' : undefined }}
                    onClick={() => setAggSelectedBasket(slug)}>
                    <span className="path-legend-col ticker">{slug.replace(/_/g, ' ')}</span>
                    <span className="path-legend-col change" style={{
                      color: avg === null ? 'var(--base01)' : avg >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)',
                    }}>
                      {avg !== null ? (avg * 100).toFixed(1) + '%' : '--'}
                    </span>
                  </div>
                ))
              )}
            </div>
            {/* Center: chart */}
            <div className="returns-right">
              <div className="returns-chart" ref={aggContainerRef}>
                {aggPathData ? (
                  <>
                    <canvas ref={aggCanvasRef} style={{ width: '100%', height: '100%', display: 'block' }} />
                    <button className={`log-toggle-btn ${aggLogScale ? 'active' : ''}`} onClick={() => setAggLogScale(v => !v)}>L</button>
                  </>
                ) : (
                  <div style={{ padding: 16, fontSize: 12, color: 'var(--base01)', fontStyle: 'italic' }}>
                    {queryData ? 'Select a basket to view average forward path' : 'Run a query first'}
                  </div>
                )}
              </div>
            </div>
            {/* Right: match dates with returns */}
            <div className="backtest-path-legend">
              <div className="path-legend-header">
                <span className="path-legend-col ticker" onClick={() => {
                  if (aggMatchSortCol === 'date') setAggMatchSortAsc(v => !v)
                  else { setAggMatchSortCol('date'); setAggMatchSortAsc(true) }
                }}>Date{aggMatchSortCol === 'date' ? (aggMatchSortAsc ? ' \u25B2' : ' \u25BC') : ''}</span>
                <span className="path-legend-col change" onClick={() => {
                  if (aggMatchSortCol === 'change') setAggMatchSortAsc(v => !v)
                  else { setAggMatchSortCol('change'); setAggMatchSortAsc(false) }
                }}>Chg{aggMatchSortCol === 'change' ? (aggMatchSortAsc ? ' \u25B2' : ' \u25BC') : ''}</span>
              </div>
              {aggSortedMatches.map(m => (
                <div key={m.mi}
                  className={`path-legend-row ${aggHoveredMatch === m.mi ? 'highlighted' : ''}`}
                  onMouseEnter={() => setAggHoveredMatch(m.mi)}
                  onMouseLeave={() => setAggHoveredMatch(null)}>
                  <span className="path-legend-col ticker">{m.date}</span>
                  <span className="path-legend-col change" style={{
                    color: m.ret === null ? 'var(--base01)' : m.ret >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)',
                  }}>
                    {m.ret !== null ? (m.ret * 100).toFixed(1) + '%' : '--'}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
