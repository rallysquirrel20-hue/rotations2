import React, { useState, useMemo, useRef, useEffect, useCallback } from 'react'
import axios from 'axios'

// Types
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
  mean: number
  median: number
  min: number
  max: number
  std: number
  count: number
  per_basket: Record<string, { mean: number; median: number; min: number; max: number; std: number; count: number }>
}

interface AnalogsResponse {
  current: {
    start: string
    end: string
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

interface BasketsData { Themes: string[]; Sectors: string[]; Industries: string[] }

interface AnalogsPanelProps {
  apiBase: string
  exportTrigger?: number
  allBaskets?: BasketsData
}

type AnalogTab = 'summary' | 'analogs' | 'comparison' | 'forward' | 'aggregate'
type GroupFilter = 'all' | 'themes' | 'sectors' | 'industries'

const PRESETS = [
  { label: '1Q', days: 63 },
  { label: '1Y', days: 365 },
  { label: '3Y', days: 1095 },
  { label: '5Y', days: 1825 },
] as const

function rankColor(rank: number, total: number): string {
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

function fmtPct(v: number | null | undefined): string {
  if (v === null || v === undefined) return '--'
  return (v * 100).toFixed(2) + '%'
}

export function AnalogsPanel({ apiBase, exportTrigger }: AnalogsPanelProps) {
  const [tab, setTab] = useState<AnalogTab>('summary')
  const [group, setGroup] = useState<GroupFilter>('all')
  const [dateBounds, setDateBounds] = useState<{ min: string; max: string }>({ min: '', max: '' })
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [activePreset, setActivePreset] = useState('1Y')
  const [loading, setLoading] = useState(false)
  const [data, setData] = useState<AnalogsResponse | null>(null)
  const [selectedAnalog, setSelectedAnalog] = useState<number | null>(null)
  const [threshold, setThreshold] = useState(0)

  // Canvas refs
  const analogsContainerRef = useRef<HTMLDivElement>(null)
  const compCanvasRef = useRef<HTMLCanvasElement>(null)
  const compContainerRef = useRef<HTMLDivElement>(null)
  const fwdCanvasRef = useRef<HTMLCanvasElement>(null)
  const fwdContainerRef = useRef<HTMLDivElement>(null)
  const aggContainerRef = useRef<HTMLDivElement>(null)
  // Force re-render after tab switch so canvas refs are available
  const [renderTick, setRenderTick] = useState(0)
  useEffect(() => { const t = setTimeout(() => setRenderTick(v => v + 1), 50); return () => clearTimeout(t) }, [tab, selectedAnalog])
  const miniCanvasRefs = useRef<Map<number, HTMLCanvasElement>>(new Map())

  const [compDims, setCompDims] = useState({ w: 800, h: 400 })
  const [fwdDims, setFwdDims] = useState({ w: 800, h: 400 })
  const [fwdHovered, setFwdHovered] = useState<string | null>(null)
  const [summarySortCol, setSummarySortCol] = useState<string>('returns')
  const [summarySortAsc, setSummarySortAsc] = useState(false)
  const [summaryShowRanks, setSummaryShowRanks] = useState(false)
  const [aggExpanded, setAggExpanded] = useState<Set<string>>(new Set())

  // Fetch date bounds
  useEffect(() => {
    if (!apiBase) return
    axios.get(`${apiBase}/baskets/returns`)
      .then(res => {
        const dr = res.data.date_range
        if (dr) {
          setDateBounds(dr)
          const d = new Date(dr.max)
          d.setDate(d.getDate() - 365)
          const s = d.toISOString().slice(0, 10)
          setStartDate(s < dr.min ? dr.min : s)
          setEndDate(dr.max)
        }
      })
      .catch(() => {})
  }, [apiBase])

  // Fetch analogs data
  useEffect(() => {
    if (!apiBase || !startDate || !endDate) return
    setLoading(true)
    const params = new URLSearchParams({
      start: startDate, end: endDate, mode: 'analogs', group,
      threshold: threshold.toString(),
    })
    axios.get(`${apiBase}/baskets/returns?${params}`)
      .then(res => {
        setData(res.data)
        setSelectedAnalog(null)
      })
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }, [apiBase, startDate, endDate, group, threshold])

  // Sorted basket slugs from current returns
  const sortedSlugs = useMemo(() => {
    if (!data?.current) return []
    return Object.entries(data.current.returns)
      .filter(([, v]) => v !== null)
      .sort((a, b) => (a[1] as number) - (b[1] as number))
      .map(([k]) => k)
  }, [data])

  const handlePreset = (p: typeof PRESETS[number]) => {
    if (!dateBounds.max) return
    setActivePreset(p.label)
    const d = new Date(dateBounds.max)
    d.setDate(d.getDate() - p.days)
    const s = d.toISOString().slice(0, 10)
    setStartDate(s < dateBounds.min ? dateBounds.min : s)
    setEndDate(dateBounds.max)
  }

  // ResizeObservers
  useEffect(() => {
    const els = [
      { ref: compContainerRef, setter: setCompDims },
      { ref: fwdContainerRef, setter: setFwdDims },
    ]
    const observers: ResizeObserver[] = []
    els.forEach(({ ref, setter }) => {
      const el = ref.current
      if (!el) return
      const obs = new ResizeObserver(entries => {
        const { width, height } = entries[0].contentRect
        if (width > 0 && height > 0) setter({ w: width, h: height })
      })
      obs.observe(el)
      observers.push(obs)
    })
    return () => observers.forEach(o => o.disconnect())
  }, [tab])

  // ── SUMMARY TAB: Fingerprint ranking table ──
  const SUMMARY_FACTORS = ['returns', 'uptrend_pct', 'breakout_pct', 'correlation_pct', 'rv_ema', 'returns_1Q', 'returns_1Y', 'returns_3Y', 'returns_5Y'] as const
  const FACTOR_LABELS: Record<string, string> = {
    returns: 'Period Ret', uptrend_pct: 'Breadth%', breakout_pct: 'BO%',
    correlation_pct: 'Corr%', rv_ema: 'RV%',
    returns_1Q: '1Q Ret', returns_1Y: '1Y Ret', returns_3Y: '3Y Ret', returns_5Y: '5Y Ret',
  }

  // Build sorted rows for summary table
  const summaryRows = useMemo(() => {
    if (!data?.current) return []
    const { returns, metrics, ranks, basket_count } = data.current
    const B = basket_count

    // Build available factors (ones with rank data)
    const factors = SUMMARY_FACTORS.filter(f => {
      if (f === 'returns') return ranks.returns !== undefined
      return ranks[f] !== undefined
    })

    // Build rows: one per basket
    const rows = sortedSlugs.map(slug => {
      const cells: { factor: string; value: number | null; rank: number | null }[] = []
      for (const f of factors) {
        let value: number | null = null
        if (f === 'returns') value = returns[slug] ?? null
        else {
          const m = metrics[f]
          if (m && typeof m === 'object') value = (m as Record<string, number | null>)[slug] ?? null
        }
        const rank = ranks[f]?.[slug] ?? null
        cells.push({ factor: f, value, rank })
      }
      // Average rank across all factors (the "fingerprint position")
      const validRanks = cells.filter(c => c.rank !== null).map(c => c.rank as number)
      const avgRank = validRanks.length > 0 ? validRanks.reduce((a, b) => a + b, 0) / validRanks.length : B
      return { slug, cells, avgRank, B }
    })

    // Sort
    if (summarySortCol === 'avg_rank') {
      rows.sort((a, b) => summarySortAsc ? a.avgRank - b.avgRank : b.avgRank - a.avgRank)
    } else if (summarySortCol === 'name') {
      rows.sort((a, b) => summarySortAsc ? a.slug.localeCompare(b.slug) : b.slug.localeCompare(a.slug))
    } else {
      // Sort by a specific factor's rank
      const fi = factors.indexOf(summarySortCol as typeof factors[number])
      if (fi >= 0) {
        rows.sort((a, b) => {
          const ra = a.cells[fi].rank ?? B
          const rb = b.cells[fi].rank ?? B
          return summarySortAsc ? ra - rb : rb - ra
        })
      }
    }

    return { rows, factors, B }
  }, [data, sortedSlugs, summarySortCol, summarySortAsc])

  const handleSummarySort = (col: string) => {
    if (summarySortCol === col) setSummarySortAsc(prev => !prev)
    else { setSummarySortCol(col); setSummarySortAsc(false) }
  }

  // ── ANALOGS TAB: mini bar charts ──
  useEffect(() => {
    if (tab !== 'analogs' || !data?.analogs.length || !data.current) return
    data.analogs.forEach((analog, ai) => {
      const canvas = miniCanvasRefs.current.get(ai)
      if (!canvas) return
      const ctx = canvas.getContext('2d')
      if (!ctx) return
      const rect = canvas.getBoundingClientRect()
      const dpr = window.devicePixelRatio || 1
      const w = rect.width, h = 80
      canvas.width = w * dpr
      canvas.height = h * dpr
      ctx.scale(dpr, dpr)
      ctx.clearRect(0, 0, w, h)
      ctx.fillStyle = '#fdf6e3'
      ctx.fillRect(0, 0, w, h)

      const items = sortedSlugs
        .filter(k => analog.returns[k] !== null && analog.returns[k] !== undefined)
        .map(k => ({ name: k, ret: analog.returns[k] as number }))
      const n = items.length
      if (n === 0) return
      const pad = { top: 4, right: 4, bottom: 4, left: 4 }
      const plotW = w - pad.left - pad.right
      const plotH = h - pad.top - pad.bottom
      const barW = Math.max(2, (plotW / n) * 0.75)
      const gap = (plotW - barW * n) / (n + 1)

      let yMin = 0, yMax = 0
      items.forEach(b => { yMin = Math.min(yMin, b.ret); yMax = Math.max(yMax, b.ret) })
      const yPad = (yMax - yMin) * 0.1 || 0.005
      yMin -= yPad; yMax += yPad
      const yScale = (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH
      const zeroY = yScale(0)

      ctx.strokeStyle = '#adb5bd'; ctx.lineWidth = 0.5; ctx.setLineDash([2, 2])
      ctx.beginPath(); ctx.moveTo(pad.left, zeroY); ctx.lineTo(w - pad.right, zeroY); ctx.stroke()
      ctx.setLineDash([])

      for (let i = 0; i < n; i++) {
        const x = pad.left + gap + i * (barW + gap)
        const val = items[i].ret
        const bTop = val >= 0 ? yScale(val) : zeroY
        const bBot = val >= 0 ? zeroY : yScale(val)
        const barHeight = Math.max(1, bBot - bTop)
        ctx.fillStyle = val >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)'
        ctx.fillRect(x, bTop, barW, barHeight)
      }
    })
  }, [data, tab, sortedSlugs, renderTick])

  // ── COMPARISON TAB: side-by-side bar charts ──
  useEffect(() => {
    if (tab !== 'comparison' || !data?.current || selectedAnalog === null) return
    const analog = data.analogs[selectedAnalog]
    if (!analog) return
    const canvas = compCanvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    const dpr = window.devicePixelRatio || 1
    const w = compDims.w, h = compDims.h
    canvas.width = w * dpr
    canvas.height = h * dpr
    ctx.scale(dpr, dpr)
    ctx.clearRect(0, 0, w, h)
    ctx.fillStyle = '#fdf6e3'
    ctx.fillRect(0, 0, w, h)

    const halfW = w / 2 - 8
    const barChartH = h * 0.6
    const breakdownH = h - barChartH - 20

    // Draw a bar chart helper
    const drawBars = (
      _ctx: CanvasRenderingContext2D, offsetX: number, chartW: number, chartH: number,
      items: { name: string; ret: number }[], title: string
    ) => {
      const n = items.length
      if (n === 0) return
      const pad = { top: 24, right: 8, bottom: 50, left: 50 }
      const plotW = chartW - pad.left - pad.right
      const plotH = chartH - pad.top - pad.bottom
      const barW = Math.max(2, Math.min(30, (plotW / n) * 0.75))
      const gap = (plotW - barW * n) / (n + 1)

      let yMin = 0, yMax = 0
      items.forEach(b => { yMin = Math.min(yMin, b.ret); yMax = Math.max(yMax, b.ret) })
      const yPad = (yMax - yMin) * 0.1 || 0.005
      yMin -= yPad; yMax += yPad
      const yScale = (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH
      const zeroY = yScale(0)

      // Title
      _ctx.fillStyle = '#586e75'; _ctx.font = 'bold 10px monospace'; _ctx.textAlign = 'center'
      _ctx.fillText(title, offsetX + chartW / 2, 14)

      // Y axis
      _ctx.strokeStyle = '#e9ecef'; _ctx.lineWidth = 1
      for (let i = 0; i <= 4; i++) {
        const v = yMin + (yMax - yMin) * (i / 4)
        const y = yScale(v)
        _ctx.beginPath(); _ctx.moveTo(offsetX + pad.left, y); _ctx.lineTo(offsetX + chartW - pad.right, y); _ctx.stroke()
        _ctx.fillStyle = '#6c757d'; _ctx.font = '9px monospace'; _ctx.textAlign = 'right'
        _ctx.fillText((v * 100).toFixed(1) + '%', offsetX + pad.left - 4, y + 3)
      }

      // Zero line
      _ctx.strokeStyle = '#adb5bd'; _ctx.setLineDash([4, 4])
      _ctx.beginPath(); _ctx.moveTo(offsetX + pad.left, zeroY); _ctx.lineTo(offsetX + chartW - pad.right, zeroY); _ctx.stroke()
      _ctx.setLineDash([])

      // Bars
      for (let i = 0; i < n; i++) {
        const x = offsetX + pad.left + gap + i * (barW + gap)
        const val = items[i].ret
        const bTop = val >= 0 ? yScale(val) : zeroY
        const bBot = val >= 0 ? zeroY : yScale(val)
        _ctx.fillStyle = val >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)'
        _ctx.fillRect(x, bTop, barW, Math.max(1, bBot - bTop))
      }

      // X labels
      const labelFontSize = Math.min(9, Math.max(6, Math.floor(plotW / n * 0.6)))
      _ctx.save()
      _ctx.fillStyle = '#586e75'; _ctx.font = `${labelFontSize}px monospace`; _ctx.textAlign = 'right'
      for (let i = 0; i < n; i++) {
        const x = offsetX + pad.left + gap + i * (barW + gap) + barW / 2
        _ctx.save()
        _ctx.translate(x, chartH - pad.bottom + 8)
        _ctx.rotate(-Math.PI / 4)
        _ctx.fillText(items[i].name.replace(/_/g, ' ').slice(0, 14), 0, 0)
        _ctx.restore()
      }
      _ctx.restore()
    }

    // Current bars
    const currentItems = sortedSlugs.map(k => ({ name: k, ret: (data.current!.returns[k] ?? 0) as number }))
    drawBars(ctx, 0, halfW, barChartH, currentItems, `CURRENT: ${data.current!.start} to ${data.current!.end}`)

    // Analog bars
    const analogItems = sortedSlugs.map(k => ({ name: k, ret: (analog.returns[k] ?? 0) as number }))
    drawBars(ctx, halfW + 16, halfW, barChartH, analogItems, `ANALOG #${selectedAnalog + 1}: ${analog.start} to ${analog.end}`)

    // Separator
    ctx.strokeStyle = '#adb5bd'; ctx.lineWidth = 1
    ctx.beginPath(); ctx.moveTo(halfW + 8, 10); ctx.lineTo(halfW + 8, barChartH); ctx.stroke()

    // Similarity breakdown as horizontal bars
    const bk = analog.similarity_breakdown
    const bkEntries = Object.entries(bk)
    const bkY = barChartH + 10
    const bkH = breakdownH
    const bkBarH = Math.min(14, bkH / bkEntries.length - 2)
    const bkLabels: Record<string, string> = {
      returns: 'Returns', breadth: 'Breadth', breakout: 'Breakout',
      correlation: 'Correlation', volatility: 'Volatility', cross_corr: 'X-Corr',
      ret_1Q: '1Q Ret', ret_1Y: '1Y Ret', ret_3Y: '3Y Ret', ret_5Y: '5Y Ret',
    }

    ctx.fillStyle = '#586e75'; ctx.font = 'bold 9px monospace'; ctx.textAlign = 'left'
    ctx.fillText('SIMILARITY BREAKDOWN', 10, bkY)

    bkEntries.forEach(([key, val], i) => {
      const y = bkY + 12 + i * (bkBarH + 2)
      // Label
      ctx.fillStyle = '#586e75'; ctx.font = '9px monospace'; ctx.textAlign = 'right'
      ctx.fillText(bkLabels[key] || key, 90, y + bkBarH / 2 + 3)
      // Bar background
      const barMaxW = w - 200
      ctx.fillStyle = '#eee8d5'
      ctx.fillRect(100, y, barMaxW, bkBarH)
      // Bar fill
      const fillW = Math.max(0, val) * barMaxW
      ctx.fillStyle = val >= 0.5 ? 'rgb(50, 50, 255)' : val >= 0 ? '#93a1a1' : 'rgb(255, 50, 150)'
      ctx.fillRect(100, y, fillW, bkBarH)
      // Value
      ctx.fillStyle = '#586e75'; ctx.font = '9px monospace'; ctx.textAlign = 'left'
      ctx.fillText((val * 100).toFixed(0) + '%', 100 + barMaxW + 4, y + bkBarH / 2 + 3)
    })
  }, [tab, data, selectedAnalog, compDims, sortedSlugs, renderTick])

  // ── FORWARD TAB: cumulative line chart ──
  useEffect(() => {
    if (tab !== 'forward' || !data?.current || selectedAnalog === null) return
    const analog = data.analogs[selectedAnalog]
    if (!analog?.forward_series) return
    const canvas = fwdCanvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    const dpr = window.devicePixelRatio || 1
    const w = fwdDims.w, h = fwdDims.h
    canvas.width = w * dpr
    canvas.height = h * dpr
    ctx.scale(dpr, dpr)
    ctx.clearRect(0, 0, w, h)
    ctx.fillStyle = '#fdf6e3'
    ctx.fillRect(0, 0, w, h)

    const fs = analog.forward_series
    const dates = fs.dates
    const nDates = dates.length
    if (nDates === 0) return

    // Determine series to plot and their final returns for ranking
    const series: { slug: string; values: (number | null)[]; finalRet: number }[] = []
    for (const slug of sortedSlugs) {
      const vals = fs.baskets[slug]
      if (!vals || vals.length === 0) continue
      const lastValid = [...vals].reverse().find(v => v !== null)
      series.push({ slug, values: vals, finalRet: lastValid ?? 0 })
    }
    series.sort((a, b) => b.finalRet - a.finalRet)

    const pad = { top: 20, right: 120, bottom: 40, left: 60 }
    const plotW = w - pad.left - pad.right
    const plotH = h - pad.top - pad.bottom

    // Y range
    let yMin = 0, yMax = 0
    series.forEach(s => s.values.forEach(v => { if (v !== null) { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) } }))
    const yPad = (yMax - yMin) * 0.1 || 0.01
    yMin -= yPad; yMax += yPad

    const xScale = (i: number) => pad.left + (i / (nDates - 1 || 1)) * plotW
    const yScale = (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH

    // Grid + Y axis
    ctx.strokeStyle = '#e9ecef'; ctx.lineWidth = 1
    for (let i = 0; i <= 5; i++) {
      const v = yMin + (yMax - yMin) * (i / 5)
      const y = yScale(v)
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(w - pad.right, y); ctx.stroke()
      ctx.fillStyle = '#6c757d'; ctx.font = '9px monospace'; ctx.textAlign = 'right'
      ctx.fillText((v * 100).toFixed(1) + '%', pad.left - 4, y + 3)
    }

    // Zero line
    ctx.strokeStyle = '#adb5bd'; ctx.setLineDash([4, 4])
    ctx.beginPath(); ctx.moveTo(pad.left, yScale(0)); ctx.lineTo(w - pad.right, yScale(0)); ctx.stroke()
    ctx.setLineDash([])

    // X axis dates
    ctx.fillStyle = '#93a1a1'; ctx.font = '9px monospace'; ctx.textAlign = 'center'
    const nLabels = Math.min(6, nDates)
    for (let li = 0; li < nLabels; li++) {
      const idx = Math.round(li * (nDates - 1) / (nLabels - 1 || 1))
      ctx.fillText(dates[idx], xScale(idx), h - pad.bottom + 14)
    }

    // Draw lines
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

    // Legend (right side)
    ctx.font = '8px monospace'; ctx.textAlign = 'left'
    series.forEach((s, rank) => {
      const y = pad.top + rank * 12
      if (y > h - 20) return
      ctx.fillStyle = rankColor(rank, totalSeries)
      ctx.fillRect(w - pad.right + 8, y, 8, 8)
      ctx.fillStyle = fwdHovered === s.slug ? '#002b36' : '#586e75'
      ctx.fillText(s.slug.replace(/_/g, ' ').slice(0, 12), w - pad.right + 20, y + 7)
    })

    // Title
    ctx.fillStyle = '#586e75'; ctx.font = 'bold 10px monospace'; ctx.textAlign = 'left'
    ctx.fillText(`FORWARD RETURNS: Analog #${selectedAnalog + 1} (${analog.start} — ${analog.end})`, pad.left, 14)
  }, [tab, data, selectedAnalog, fwdDims, sortedSlugs, fwdHovered, renderTick])

  // Forward tab hover handler
  const handleFwdMouse = useCallback((e: React.MouseEvent<HTMLCanvasElement>) => {
    if (!fwdCanvasRef.current || !data?.current || selectedAnalog === null) return
    const analog = data.analogs[selectedAnalog]
    if (!analog?.forward_series) return
    const rect = fwdCanvasRef.current.getBoundingClientRect()
    const my = (e.clientY - rect.top) * (fwdDims.h / rect.height)

    const pad = { top: 20, right: 120 }
    const series = sortedSlugs.filter(s => analog.forward_series.baskets[s]?.length > 0)
      .map(s => {
        const vals = analog.forward_series.baskets[s]
        const lastValid = [...vals].reverse().find(v => v !== null)
        return { slug: s, finalRet: lastValid ?? 0 }
      })
      .sort((a, b) => b.finalRet - a.finalRet)

    // Check legend hover
    const legendX = fwdDims.w - pad.right + 8
    const mx = (e.clientX - rect.left) * (fwdDims.w / rect.width)
    if (mx >= legendX) {
      const idx = Math.floor((my - pad.top) / 12)
      if (idx >= 0 && idx < series.length) {
        setFwdHovered(series[idx].slug)
        return
      }
    }
    setFwdHovered(null)
  }, [data, selectedAnalog, fwdDims, sortedSlugs])

  // Export
  const prevExportTrigger = useRef(exportTrigger || 0)
  useEffect(() => {
    if (!exportTrigger || exportTrigger === prevExportTrigger.current) {
      prevExportTrigger.current = exportTrigger || 0
      return
    }
    prevExportTrigger.current = exportTrigger
    const canvasMap: Partial<Record<AnalogTab, React.RefObject<HTMLCanvasElement | null>>> = {
      comparison: compCanvasRef, forward: fwdCanvasRef,
    }
    const canvas = canvasMap[tab]?.current
    if (!canvas) return
    const fmtDate = (d: string) => { const p = d.split('-'); return `${parseInt(p[1])}_${parseInt(p[2])}_${p[0]}` }
    const name = `analogs_${tab}_${group}_${fmtDate(startDate)}_${fmtDate(endDate)}`
    canvas.toBlob(blob => {
      if (!blob) return
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url; a.download = `${name}.png`; a.click()
      URL.revokeObjectURL(url)
    }, 'image/png')
  }, [exportTrigger, tab, group, startDate, endDate])

  // ── RENDER ──
  const TABS: { key: AnalogTab; label: string }[] = [
    { key: 'summary', label: 'Summary' },
    { key: 'analogs', label: 'Analogs' },
    { key: 'comparison', label: 'Comparison' },
    { key: 'forward', label: 'Forward' },
    { key: 'aggregate', label: 'Aggregate' },
  ]

  return (
    <div style={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0 }}>
      {/* Controls bar */}
      <div className="analysis-date-controls">
        {/* Group filter */}
        {(['all', 'themes', 'sectors', 'industries'] as GroupFilter[]).map(g => (
          <button key={g} className={`basket-returns-preset-btn ${group === g ? 'active' : ''}`} onClick={() => setGroup(g)}>
            {g === 'all' ? 'ALL' : g === 'themes' ? 'T' : g === 'sectors' ? 'S' : 'I'}
          </button>
        ))}
        <span style={{ color: 'var(--border-color)', margin: '0 1px' }}>|</span>
        {/* Date presets */}
        {PRESETS.map(p => (
          <button key={p.label} className={`basket-returns-preset-btn ${activePreset === p.label ? 'active' : ''}`} onClick={() => handlePreset(p)}>
            {p.label}
          </button>
        ))}
        <span style={{ color: 'var(--border-color)', margin: '0 1px' }}>|</span>
        {/* Threshold slider */}
        <span style={{ fontSize: 9, color: 'var(--text-main)', whiteSpace: 'nowrap' }}>
          Threshold: {(threshold * 100).toFixed(0)}%
        </span>
        <input
          type="range" min="0" max="0.9" step="0.05"
          value={threshold}
          onChange={e => setThreshold(parseFloat(e.target.value))}
          style={{ width: 80, height: 14 }}
        />
        {loading && <span className="analysis-loading-hint">Loading...</span>}
        <div style={{ flex: 1 }} />
        <input type="date" className="date-input" value={startDate} min={dateBounds.min} max={dateBounds.max}
          onChange={e => { setStartDate(e.target.value); setActivePreset('') }} />
        <span style={{ fontSize: 10, color: 'var(--text-main)' }}>to</span>
        <input type="date" className="date-input" value={endDate} min={dateBounds.min} max={dateBounds.max}
          onChange={e => { setEndDate(e.target.value); setActivePreset('') }} />
      </div>

      {/* Tab bar */}
      <div className="analogs-tab-bar">
        {TABS.map(t => (
          <button
            key={t.key}
            className={`basket-returns-preset-btn ${tab === t.key ? 'active' : ''}`}
            onClick={() => setTab(t.key)}
            style={{ padding: '4px 10px', fontSize: 10 }}
          >
            {t.label}
          </button>
        ))}
        {data && (
          <span style={{ fontSize: 9, color: 'var(--base01)', marginLeft: 8 }}>
            {data.analogs.length} analog{data.analogs.length !== 1 ? 's' : ''} found
            {selectedAnalog !== null && ` | #${selectedAnalog + 1} selected`}
          </span>
        )}
      </div>

      {/* Tab content */}
      <div style={{ flex: 1, minHeight: 0, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
        {/* Summary tab — shows the ranking process that builds the current fingerprint */}
        {tab === 'summary' && (
          <div style={{ flex: 1, minHeight: 0, overflowY: 'auto', padding: 8 }}>
            {data?.current && 'rows' in summaryRows && summaryRows.rows.length > 0 ? (
              <>
                {/* Explanation header */}
                <div style={{ marginBottom: 8, padding: '6px 8px', background: 'var(--bg-sidebar)', borderBottom: '1px solid var(--border-color)', fontSize: 10, color: 'var(--base01)', lineHeight: 1.5 }}>
                  <strong>Current Environment Fingerprint</strong> ({data.current.start} to {data.current.end})
                  <br />
                  Each factor is ranked across {summaryRows.B} baskets (1 = highest, {summaryRows.B} = lowest).
                  The rank vector is the fingerprint. Historical periods with similar rank vectors (high Spearman correlation) are analogs.
                  <span style={{ float: 'right' }}>
                    <label style={{ cursor: 'pointer' }}>
                      <input type="checkbox" checked={summaryShowRanks} onChange={e => setSummaryShowRanks(e.target.checked)} style={{ marginRight: 4 }} />
                      Show ranks
                    </label>
                  </span>
                </div>
                {data.current.metrics.cross_basket_corr !== undefined && data.current.metrics.cross_basket_corr !== null && (
                  <div style={{ marginBottom: 6, fontSize: 10, color: 'var(--base01)' }}>
                    Cross-Basket Correlation: <strong>{((data.current.metrics.cross_basket_corr as number) * 100).toFixed(1)}%</strong>
                    <span style={{ marginLeft: 8, color: 'var(--base00)' }}>(scalar — compared via absolute difference, not ranked)</span>
                  </div>
                )}
                <table className="analogs-agg-table" style={{ fontSize: 10 }}>
                  <thead>
                    <tr>
                      <th style={{ cursor: 'pointer', minWidth: 120 }} onClick={() => handleSummarySort('name')}>
                        Basket {summarySortCol === 'name' ? (summarySortAsc ? '▲' : '▼') : ''}
                      </th>
                      {summaryRows.factors.map(f => (
                        <th key={f} style={{ cursor: 'pointer', textAlign: 'center', minWidth: summaryShowRanks ? 90 : 65 }} onClick={() => handleSummarySort(f)}>
                          {FACTOR_LABELS[f]} {summarySortCol === f ? (summarySortAsc ? '▲' : '▼') : ''}
                        </th>
                      ))}
                      <th style={{ cursor: 'pointer', textAlign: 'center', minWidth: 60, borderLeft: '2px solid var(--border-color)' }} onClick={() => handleSummarySort('avg_rank')}>
                        Avg Rank {summarySortCol === 'avg_rank' ? (summarySortAsc ? '▲' : '▼') : ''}
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {summaryRows.rows.map(row => (
                      <tr key={row.slug}>
                        <td style={{ fontWeight: 'bold', whiteSpace: 'nowrap' }}>{row.slug.replace(/_/g, ' ')}</td>
                        {row.cells.map((cell, ci) => {
                          const bgColor = cell.rank !== null ? rankColor(cell.rank - 1, row.B) : 'transparent'
                          return (
                            <td key={ci} style={{ textAlign: 'center', background: bgColor, color: '#fdf6e3', fontWeight: 'bold' }}>
                              {summaryShowRanks ? (
                                <span>
                                  <span style={{ opacity: 0.7, fontSize: 9 }}>{cell.value !== null ? (cell.value * 100).toFixed(1) + '%' : '--'}</span>
                                  {' → '}
                                  <span>{cell.rank ?? '--'}</span>
                                </span>
                              ) : (
                                <span>{cell.value !== null ? (cell.value * 100).toFixed(1) + '%' : '--'}</span>
                              )}
                            </td>
                          )
                        })}
                        <td style={{
                          textAlign: 'center', fontWeight: 'bold', borderLeft: '2px solid var(--border-color)',
                          background: rankColor(Math.round(row.avgRank) - 1, row.B), color: '#fdf6e3',
                        }}>
                          {row.avgRank.toFixed(1)}
                        </td>
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

        {/* Analogs tab */}
        {tab === 'analogs' && (
          <div ref={analogsContainerRef} style={{ flex: 1, overflowY: 'auto', minHeight: 0 }}>
            {data?.message && (
              <div style={{ padding: 16, fontSize: 12, color: 'var(--base01)', fontStyle: 'italic' }}>{data.message}</div>
            )}
            {data?.analogs.map((analog, ai) => {
              const isSelected = selectedAnalog === ai
              const fwdAvg = (hz: string) => {
                const fwd = analog.forward[hz]
                if (!fwd) return null
                const vals = Object.values(fwd).filter((v): v is number => v !== null)
                if (vals.length === 0) return null
                return vals.reduce((a, b) => a + b, 0) / vals.length
              }
              return (
                <div
                  key={ai}
                  className={`analog-card ${isSelected ? 'analog-card-selected' : ''}`}
                  onClick={() => { setSelectedAnalog(isSelected ? null : ai) }}
                  style={{ cursor: 'pointer' }}
                >
                  <div className="analog-card-header">
                    <span style={{ fontSize: 11, fontWeight: 'bold', color: 'var(--text-bold)' }}>
                      #{ai + 1}: {analog.start} — {analog.end}
                    </span>
                    <span className="analog-similarity">{(analog.similarity * 100).toFixed(0)}% MATCH</span>
                  </div>
                  <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', marginBottom: 4 }}>
                    {Object.entries(analog.similarity_breakdown).map(([k, v]) => {
                      const labels: Record<string, string> = {
                        returns: 'Ret', breadth: 'Brd', breakout: 'BO', correlation: 'Cor',
                        volatility: 'Vol', cross_corr: 'XC', ret_1Q: '1Q', ret_1Y: '1Y',
                        ret_3Y: '3Y', ret_5Y: '5Y',
                      }
                      return (
                        <span key={k} style={{ fontSize: 9, padding: '1px 4px', border: '1px solid var(--border-color)', color: 'var(--text-main)' }}>
                          {labels[k] || k} {(v * 100).toFixed(0)}%
                        </span>
                      )
                    })}
                  </div>
                  <canvas
                    ref={el => { if (el) miniCanvasRefs.current.set(ai, el); else miniCanvasRefs.current.delete(ai) }}
                    style={{ width: '100%', height: 80 }}
                  />
                  <div className="analog-forward-row">
                    {['1M', '3M', '6M'].map(hz => {
                      const avg = fwdAvg(hz)
                      return (
                        <span key={hz} style={{ color: avg === null ? 'var(--base01)' : avg >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>
                          +{hz}: {avg !== null ? (avg * 100).toFixed(1) + '% avg' : 'N/A'}
                        </span>
                      )
                    })}
                  </div>
                </div>
              )
            })}
            {data && data.analogs.length === 0 && !data.message && (
              <div style={{ padding: 16, fontSize: 12, color: 'var(--base01)', fontStyle: 'italic' }}>No analogs found</div>
            )}
          </div>
        )}

        {/* Comparison tab */}
        {tab === 'comparison' && (
          <div ref={compContainerRef} style={{ flex: 1, minHeight: 200, position: 'relative' }}>
            {selectedAnalog !== null ? (
              <canvas ref={compCanvasRef} style={{ width: '100%', height: '100%', display: 'block' }} />
            ) : (
              <div style={{ padding: 16, fontSize: 12, color: 'var(--base01)', fontStyle: 'italic' }}>
                Select an analog from the Analogs tab to compare
              </div>
            )}
          </div>
        )}

        {/* Forward tab */}
        {tab === 'forward' && (
          <div ref={fwdContainerRef} style={{ flex: 1, minHeight: 200, position: 'relative' }}>
            {selectedAnalog !== null ? (
              <canvas
                ref={fwdCanvasRef}
                style={{ width: '100%', height: '100%', display: 'block' }}
                onMouseMove={handleFwdMouse}
                onMouseLeave={() => setFwdHovered(null)}
              />
            ) : (
              <div style={{ padding: 16, fontSize: 12, color: 'var(--base01)', fontStyle: 'italic' }}>
                Select an analog from the Analogs tab to view forward returns
              </div>
            )}
          </div>
        )}

        {/* Aggregate tab */}
        {tab === 'aggregate' && (
          <div ref={aggContainerRef} style={{ flex: 1, overflowY: 'auto', minHeight: 0, padding: 8 }}>
            {data?.aggregate ? (
              <>
                {/* Aggregate stats table */}
                <table className="analogs-agg-table">
                  <thead>
                    <tr>
                      <th>Horizon</th>
                      <th>Mean</th>
                      <th>Median</th>
                      <th>Min</th>
                      <th>Max</th>
                      <th>Std</th>
                      <th>Count</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(data.aggregate).map(([hz, stats]) => {
                      if (!stats) return (
                        <tr key={hz}>
                          <td style={{ fontWeight: 'bold' }}>+{hz}</td>
                          <td colSpan={6} style={{ color: 'var(--base01)', fontStyle: 'italic' }}>N/A</td>
                        </tr>
                      )
                      const isExpanded = aggExpanded.has(hz)
                      return (
                        <React.Fragment key={hz}>
                          <tr onClick={() => setAggExpanded(prev => {
                            const next = new Set(prev)
                            if (next.has(hz)) next.delete(hz); else next.add(hz)
                            return next
                          })} style={{ cursor: 'pointer' }}>
                            <td style={{ fontWeight: 'bold' }}>{isExpanded ? '▼' : '▶'} +{hz}</td>
                            <td style={{ color: stats.mean >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>{fmtPct(stats.mean)}</td>
                            <td style={{ color: stats.median >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>{fmtPct(stats.median)}</td>
                            <td style={{ color: stats.min >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>{fmtPct(stats.min)}</td>
                            <td style={{ color: stats.max >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>{fmtPct(stats.max)}</td>
                            <td>{fmtPct(stats.std)}</td>
                            <td>{stats.count}</td>
                          </tr>
                          {isExpanded && stats.per_basket && Object.entries(stats.per_basket)
                            .sort((a, b) => (b[1].mean) - (a[1].mean))
                            .map(([slug, bs]) => (
                              <tr key={`${hz}-${slug}`} className="analogs-agg-subrow">
                                <td style={{ paddingLeft: 20, fontSize: 10 }}>{slug.replace(/_/g, ' ')}</td>
                                <td style={{ color: bs.mean >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)', fontSize: 10 }}>{fmtPct(bs.mean)}</td>
                                <td style={{ color: bs.median >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)', fontSize: 10 }}>{fmtPct(bs.median)}</td>
                                <td style={{ color: bs.min >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)', fontSize: 10 }}>{fmtPct(bs.min)}</td>
                                <td style={{ color: bs.max >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)', fontSize: 10 }}>{fmtPct(bs.max)}</td>
                                <td style={{ fontSize: 10 }}>{fmtPct(bs.std)}</td>
                                <td style={{ fontSize: 10 }}>{bs.count}</td>
                              </tr>
                            ))
                          }
                        </React.Fragment>
                      )
                    })}
                  </tbody>
                </table>
              </>
            ) : (
              <div style={{ padding: 16, fontSize: 12, color: 'var(--base01)', fontStyle: 'italic' }}>
                {loading ? 'Loading...' : 'No aggregate data available'}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
