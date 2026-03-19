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
  metric: string    // return_1D, return_1W, return_1M, return_1Q, return_1Y, uptrend_pct, etc.
  operator: string  // positive, negative, top_n, bottom_n, above, below
  value: number
}

interface BasketsData { Themes: string[]; Sectors: string[]; Industries: string[] }

interface AnalogsPanelProps {
  apiBase: string
  exportTrigger?: number
  allBaskets?: BasketsData
}

type AnalogTab = 'summary' | 'matches' | 'forward' | 'aggregate'

const METRIC_OPTIONS = [
  { value: 'return_1D', label: '1D Return' },
  { value: 'return_1W', label: '1W Return' },
  { value: 'return_1M', label: '1M Return' },
  { value: 'return_1Q', label: '1Q Return' },
  { value: 'return_1Y', label: '1Y Return' },
  { value: 'uptrend_pct', label: 'Uptrend%' },
  { value: 'breakout_pct', label: 'Breakout%' },
  { value: 'correlation_pct', label: 'Corr%' },
  { value: 'rv_ema', label: 'Volatility' },
]

const OPERATOR_OPTIONS = [
  { value: 'positive', label: '> 0' },
  { value: 'negative', label: '< 0' },
  { value: 'top_n', label: 'Top N' },
  { value: 'bottom_n', label: 'Bottom N' },
  { value: 'above', label: 'Above' },
  { value: 'below', label: 'Below' },
]

// Map summary grid factors to condition metrics
const FACTOR_TO_METRIC: Record<string, string> = {
  returns_1D: 'return_1D', returns_1W: 'return_1W', returns_1M: 'return_1M',
  returns_1Q: 'return_1Q', returns_1Y: 'return_1Y', returns_3Y: 'return_1Y', returns_5Y: 'return_1Y',
  uptrend_pct: 'uptrend_pct', breakout_pct: 'breakout_pct',
  correlation_pct: 'correlation_pct', rv_ema: 'rv_ema',
}

function rankColor(rank: number, total: number): string {
  if (total <= 1) return 'rgb(50, 50, 255)'
  // Rank 1 = blue (best), rank B = pink (worst) — backend handles orientation
  const t = rank / (total - 1)
  // rgb(50,50,255) blue → rgb(253,246,227) solarized bg → rgb(255,50,150) pink
  if (t <= 0.5) {
    const s = t * 2
    return `rgb(${Math.round(50 + 203 * s)}, ${Math.round(50 + 196 * s)}, ${Math.round(255 - 28 * s)})`
  } else {
    const s = (t - 0.5) * 2
    return `rgb(${Math.round(253 + 2 * s)}, ${Math.round(246 - 196 * s)}, ${Math.round(227 - 77 * s)})`
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
  const [renderTick, setRenderTick] = useState(0)
  useEffect(() => { const t = setTimeout(() => setRenderTick(v => v + 1), 50); return () => clearTimeout(t) }, [tab, selectedMatch])

  const [fwdDims, setFwdDims] = useState({ w: 800, h: 400 })
  const [fwdHovered, setFwdHovered] = useState<string | null>(null)
  const [fwdLogScale, setFwdLogScale] = useState(false)
  const [summarySortCol, setSummarySortCol] = useState<string>('returns_1Q')
  const [summarySortAsc, setSummarySortAsc] = useState(false)
  const [summaryShowRanks, setSummaryShowRanks] = useState(false)
  const [aggExpanded, setAggExpanded] = useState<Set<string>>(new Set())

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

  // ── Condition management ──
  const addCondition = (basket: string, metric: string, rank: number, B: number) => {
    const op = rank <= B / 2 ? 'bottom_n' : 'top_n'
    setConditions(prev => [...prev, {
      id: condIdCounter, basket, metric: FACTOR_TO_METRIC[metric] || metric,
      operator: op, value: 5,
    }])
    setCondIdCounter(prev => prev + 1)
  }

  const addEmptyCondition = () => {
    setConditions(prev => [...prev, {
      id: condIdCounter, basket: sortedSlugs[0] || '',
      metric: 'return_1D', operator: 'negative', value: 0,
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
        basket: c.basket, metric: c.metric, operator: c.operator, value: c.value,
      }))),
    })
    axios.get(`${apiBase}/baskets/returns?${params}`)
      .then(res => {
        setQueryData(res.data)
        setSelectedMatch(null)
        setTab('matches')
      })
      .catch(() => setQueryData({ matches: [], match_count: 0, total_searched: 0, aggregate: {}, baskets: [], date_range: { min: '', max: '' } }))
      .finally(() => setQueryLoading(false))
  }

  // ── SUMMARY: Fingerprint ranking table ──
  const SUMMARY_FACTORS = ['returns_1D', 'returns_1W', 'returns_1M', 'returns_1Q', 'returns_1Y', 'returns_3Y', 'returns_5Y', 'uptrend_pct', 'breakout_pct', 'correlation_pct', 'rv_ema'] as const
  const FACTOR_LABELS: Record<string, string> = {
    returns_1D: '1D', returns_1W: '1W', returns_1M: '1M',
    returns_1Q: '1Q', returns_1Y: '1Y', returns_3Y: '3Y', returns_5Y: '5Y',
    uptrend_pct: 'Breadth%', breakout_pct: 'BO%',
    correlation_pct: 'Corr%', rv_ema: 'RV%',
  }

  const summaryRows = useMemo(() => {
    if (!data?.current) return { rows: [] as { slug: string; cells: { factor: string; value: number | null; rank: number | null }[]; avgRank: number; B: number }[], factors: [] as string[], B: 0 }
    const { returns, metrics, ranks, basket_count } = data.current
    const B = basket_count
    const factors = (SUMMARY_FACTORS as readonly string[]).filter(f => ranks[f] !== undefined)
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
  }, [data, sortedSlugs, summarySortCol, summarySortAsc])

  const handleSummarySort = (col: string) => {
    if (summarySortCol === col) setSummarySortAsc(prev => !prev)
    else { setSummarySortCol(col); setSummarySortAsc(false) }
  }

  // ── FORWARD TAB: cumulative line chart ──
  const fwdSeries = useMemo(() => {
    if (selectedMatch === null || !queryData) return null
    const match = queryData.matches[selectedMatch]
    if (!match?.forward_series) return null
    return match.forward_series
  }, [queryData, selectedMatch])

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

    const pad = { top: 20, right: 120, bottom: 40, left: 60 }
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
      ctx.fillText((v * 100).toFixed(1) + '%', pad.left - 4, y + 3)
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

    // Legend
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
    const match = queryData!.matches[selectedMatch!]
    ctx.fillStyle = '#586e75'; ctx.font = 'bold 10px monospace'; ctx.textAlign = 'left'
    ctx.fillText(`FORWARD RETURNS from ${match.date}`, pad.left, 14)
  }, [tab, fwdSeries, queryData, selectedMatch, fwdDims, fwdHovered, renderTick, fwdLogScale])

  const handleFwdMouse = useCallback((e: React.MouseEvent<HTMLCanvasElement>) => {
    if (!fwdCanvasRef.current || !fwdSeries || !queryData) return
    const rect = fwdCanvasRef.current.getBoundingClientRect()
    const mx = (e.clientX - rect.left) * (fwdDims.w / rect.width)
    const my = (e.clientY - rect.top) * (fwdDims.h / rect.height)
    const pad = { top: 20, right: 120 }
    const legendX = fwdDims.w - pad.right + 8
    if (mx >= legendX) {
      const baskets = queryData.baskets
      const series = baskets.filter(s => fwdSeries.baskets[s]?.length > 0)
        .map(s => { const vals = fwdSeries.baskets[s]; return { slug: s, finalRet: ([...vals].reverse().find(v => v !== null)) ?? 0 } })
        .sort((a, b) => (b.finalRet as number) - (a.finalRet as number))
      const idx = Math.floor((my - pad.top) / 12)
      if (idx >= 0 && idx < series.length) { setFwdHovered(series[idx].slug); return }
    }
    setFwdHovered(null)
  }, [fwdSeries, queryData, fwdDims])

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
    { key: 'matches', label: `Matches${queryData ? ` (${queryData.match_count})` : ''}` },
    { key: 'forward', label: 'Forward' },
    { key: 'aggregate', label: 'Aggregate' },
  ]

  const needsValue = (op: string) => ['top_n', 'bottom_n', 'above', 'below'].includes(op)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0 }}>
      {/* Tab bar — matches accordion header height */}
      <div className="analysis-date-controls" style={{ height: 40 }}>
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
                {data.current.metrics.cross_basket_corr !== undefined && data.current.metrics.cross_basket_corr !== null && (
                  <div style={{ marginBottom: 6, fontSize: 10, color: 'var(--base01)' }}>
                    Cross-Basket Correlation: <strong>{((data.current.metrics.cross_basket_corr as number) * 100).toFixed(1)}%</strong>
                  </div>
                )}
                <table className="analogs-agg-table" style={{ fontSize: 10 }}>
                  <thead>
                    <tr>
                      <th style={{ cursor: 'pointer', minWidth: 120 }} onClick={() => handleSummarySort('name')}>
                        Basket {summarySortCol === 'name' ? (summarySortAsc ? '\u25B2' : '\u25BC') : ''}
                      </th>
                      {summaryRows.factors.map(f => (
                        <th key={f} style={{ cursor: 'pointer', textAlign: 'center', minWidth: summaryShowRanks ? 90 : 65 }} onClick={() => handleSummarySort(f)}>
                          {FACTOR_LABELS[f]} {summarySortCol === f ? (summarySortAsc ? '\u25B2' : '\u25BC') : ''}
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
                                const isRaw = cell.factor === 'uptrend_pct' || cell.factor === 'breakout_pct' || cell.factor === 'correlation_pct'
                                const display = cell.value !== null ? (isRaw ? cell.value.toFixed(1) + '%' : (cell.value * 100).toFixed(1) + '%') : '--'
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

        {/* Matches tab */}
        {tab === 'matches' && (
          <div style={{ flex: 1, overflowY: 'auto', minHeight: 0, padding: 8 }}>
            {!queryData ? (
              <div style={{ padding: 16, fontSize: 12, color: 'var(--base01)', fontStyle: 'italic' }}>
                Add conditions from the Summary grid and click "Find Matches"
              </div>
            ) : !queryData.matches || queryData.matches.length === 0 ? (
              <div style={{ padding: 16, fontSize: 12, color: 'var(--base01)', fontStyle: 'italic' }}>
                No matching dates found. Try relaxing your conditions.
              </div>
            ) : (
              <table className="analogs-agg-table" style={{ fontSize: 10 }}>
                <thead>
                  <tr>
                    <th style={{ minWidth: 30 }}>#</th>
                    <th style={{ minWidth: 90 }}>Date</th>
                    <th style={{ textAlign: 'center' }}>+1W</th>
                    <th style={{ textAlign: 'center' }}>+1M</th>
                    <th style={{ textAlign: 'center' }}>+3M</th>
                    <th style={{ textAlign: 'center' }}>+6M</th>
                    <th style={{ minWidth: 50 }}></th>
                  </tr>
                </thead>
                <tbody>
                  {queryData.matches.map((match, mi) => {
                    const isSelected = selectedMatch === mi
                    const avgFwd = (hz: string) => {
                      const fwd = match.forward[hz]
                      if (!fwd) return null
                      const vals = Object.values(fwd).filter((v): v is number => v !== null)
                      return vals.length > 0 ? vals.reduce((a, b) => a + b, 0) / vals.length : null
                    }
                    return (
                      <tr key={mi}
                        onClick={() => setSelectedMatch(isSelected ? null : mi)}
                        style={{ cursor: 'pointer', background: isSelected ? 'var(--bg-sidebar)' : undefined }}>
                        <td style={{ fontWeight: 'bold' }}>{mi + 1}</td>
                        <td style={{ fontWeight: 'bold' }}>{match.date}</td>
                        {['1W', '1M', '3M', '6M'].map(hz => {
                          const avg = avgFwd(hz)
                          return (
                            <td key={hz} style={{
                              textAlign: 'center',
                              color: avg === null ? 'var(--base01)' : avg >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)',
                            }}>
                              {avg !== null ? (avg * 100).toFixed(2) + '%' : '--'}
                            </td>
                          )
                        })}
                        <td>
                          <button className={`basket-returns-preset-btn ${isSelected ? 'active' : ''}`}
                            onClick={e => { e.stopPropagation(); setSelectedMatch(isSelected ? null : mi); setTab('forward') }}
                            style={{ fontSize: 9, padding: '1px 6px' }}>
                            Fwd
                          </button>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            )}
          </div>
        )}

        {/* Forward tab */}
        {tab === 'forward' && (
          <div ref={fwdContainerRef} style={{ flex: 1, minHeight: 200, position: 'relative' }}>
            {selectedMatch !== null && queryData ? (
              <>
                <canvas ref={fwdCanvasRef} style={{ width: '100%', height: '100%', display: 'block' }}
                  onMouseMove={handleFwdMouse} onMouseLeave={() => setFwdHovered(null)} />
                <button className={`log-toggle-btn ${fwdLogScale ? 'active' : ''}`} onClick={() => setFwdLogScale(v => !v)}>L</button>
              </>
            ) : (
              <div style={{ padding: 16, fontSize: 12, color: 'var(--base01)', fontStyle: 'italic' }}>
                Select a match from the Matches tab to view forward returns
              </div>
            )}
          </div>
        )}

        {/* Aggregate tab */}
        {tab === 'aggregate' && (
          <div style={{ flex: 1, overflowY: 'auto', minHeight: 0, padding: 8 }}>
            {queryData?.aggregate ? (
              <table className="analogs-agg-table">
                <thead>
                  <tr>
                    <th>Horizon</th><th>Mean</th><th>Median</th><th>Min</th><th>Max</th><th>Std</th><th>Count</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(queryData.aggregate).map(([hz, stats]) => {
                    if (!stats) return (
                      <tr key={hz}><td style={{ fontWeight: 'bold' }}>+{hz}</td><td colSpan={6} style={{ color: 'var(--base01)', fontStyle: 'italic' }}>N/A</td></tr>
                    )
                    const isExpanded = aggExpanded.has(hz)
                    return (
                      <React.Fragment key={hz}>
                        <tr onClick={() => setAggExpanded(prev => { const n = new Set(prev); if (n.has(hz)) n.delete(hz); else n.add(hz); return n })}
                          style={{ cursor: 'pointer' }}>
                          <td style={{ fontWeight: 'bold' }}>{isExpanded ? '\u25BC' : '\u25B6'} +{hz}</td>
                          <td style={{ color: stats.mean >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>{fmtPct(stats.mean)}</td>
                          <td style={{ color: stats.median >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>{fmtPct(stats.median)}</td>
                          <td style={{ color: stats.min >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>{fmtPct(stats.min)}</td>
                          <td style={{ color: stats.max >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>{fmtPct(stats.max)}</td>
                          <td>{fmtPct(stats.std)}</td>
                          <td>{stats.count}</td>
                        </tr>
                        {isExpanded && stats.per_basket && Object.entries(stats.per_basket)
                          .sort((a, b) => b[1].mean - a[1].mean)
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
            ) : (
              <div style={{ padding: 16, fontSize: 12, color: 'var(--base01)', fontStyle: 'italic' }}>
                {queryLoading ? 'Loading...' : 'Run a query to see aggregate forward returns'}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
