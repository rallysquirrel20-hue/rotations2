import { useState, useMemo, useRef, useEffect } from 'react'
import axios from 'axios'

interface CorrelationData {
  labels: string[]
  matrix: (number | null)[][]
  min_date?: string
  max_date?: string
}

interface CumulativeReturnsData {
  dates: string[]
  series: { ticker: string; values: (number | null)[]; join_date?: string | null }[]
}

interface BasketsData {
  Themes: string[]
  Sectors: string[]
  Industries: string[]
}

interface BasketSummaryProps {
  data: {
    correlation: CorrelationData
    cumulative_returns: CumulativeReturnsData
  } | null
  loading: boolean
  basketName: string
  apiBase: string
  quarterDateRange?: { from: string; to: string } | null
  exportTrigger?: number
  analysisMode?: 'intra' | 'cross'
  allBaskets?: BasketsData
  onBasketSelect?: (basket: string) => void
}

type TabType = 'correlation' | 'returns' | 'contribution' | 'cross_returns' | 'single_returns'

// Blue (best) → Grey (mid) → Pink (worst) gradient based on rank
function rankColor(rank: number, total: number): string {
  if (total <= 1) return 'rgb(50, 50, 255)'
  const t = rank / (total - 1) // 0 = best (blue), 1 = worst (pink)
  // Blue: rgb(50, 50, 255) → Mid: rgb(152, 50, 202) → Pink: rgb(255, 50, 150)
  if (t <= 0.5) {
    const s = t * 2 // 0→1 within blue→mid
    return `rgb(${Math.round(50 + 102 * s)}, 50, ${Math.round(255 - 53 * s)})`
  } else {
    const s = (t - 0.5) * 2 // 0→1 within mid→pink
    return `rgb(${Math.round(152 + 103 * s)}, 50, ${Math.round(202 - 52 * s)})`
  }
}

function corrColor(v: number | null): string {
  if (v === null) return '#fdf6e3'
  // Blue (50,50,255) for positive, pink (255,50,150) for negative, white at 0
  const clamped = Math.max(-1, Math.min(1, v))
  const t = Math.abs(clamped)
  if (clamped >= 0) {
    return `rgb(${Math.round(255 - t * 205)}, ${Math.round(255 - t * 205)}, 255)`
  } else {
    return `rgb(255, ${Math.round(255 - t * 205)}, ${Math.round(255 - t * 105)})`
  }
}

function CorrelationHeatmap({ data, basketName, apiBase, quarterDateRange }: { data: CorrelationData; basketName: string; apiBase: string; quarterDateRange?: { from: string; to: string } | null }) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [dims, setDims] = useState({ w: 600, h: 400 })
  const [corrDate, setCorrDate] = useState('')
  const [liveData, setLiveData] = useState<CorrelationData | null>(null)
  const [dateBounds, setDateBounds] = useState<{ min: string; max: string }>({ min: '', max: '' })
  const [loading, setLoading] = useState(false)

  // Fetch date bounds on mount
  useEffect(() => {
    if (!basketName || !apiBase) return
    axios.get(`${apiBase}/baskets/${encodeURIComponent(basketName)}/correlation`)
      .then(res => {
        if (res.data.min_date && res.data.max_date) {
          setDateBounds({ min: res.data.min_date, max: res.data.max_date })
        }
      })
      .catch(() => {})
  }, [basketName, apiBase])

  // Set corrDate to end of quarter range when active
  useEffect(() => {
    if (quarterDateRange) {
      setCorrDate(quarterDateRange.to)
    } else {
      setCorrDate('')
    }
  }, [quarterDateRange])

  // Fetch correlation for selected date
  useEffect(() => {
    if (!corrDate || !basketName || !apiBase) {
      setLiveData(null)
      return
    }
    setLoading(true)
    axios.get(`${apiBase}/baskets/${encodeURIComponent(basketName)}/correlation?date=${corrDate}`)
      .then(res => setLiveData(res.data))
      .catch(() => setLiveData(null))
      .finally(() => setLoading(false))
  }, [corrDate, basketName, apiBase])

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

  const active = liveData || data
  const { labels, matrix } = active

  if (!labels.length && !loading) return <div className="summary-empty">No correlation data</div>

  // Reserve space for row labels, header labels, and controls bar
  const controlBarHeight = 36
  const labelMargin = 60
  const legendHeight = 24
  const availW = dims.w - labelMargin
  const availH = dims.h - labelMargin - legendHeight - controlBarHeight
  const cellSize = labels.length ? Math.max(8, Math.min(40, Math.floor(Math.min(availW, availH) / labels.length))) : 20

  const showValues = cellSize >= 28

  return (
    <div className="corr-wrapper" ref={containerRef}>
      <div className="analysis-date-controls">
        <label className="analysis-date-label">As of:</label>
        <input
          type="date"
          className="date-input"
          value={corrDate}
          min={dateBounds.min}
          max={dateBounds.max}
          onChange={e => setCorrDate(e.target.value)}
        />
        {corrDate && <button className="control-btn" onClick={() => setCorrDate('')}>Reset</button>}
        {loading && <span className="analysis-loading-hint">Loading...</span>}
      </div>
      <div className="corr-scroll">
        <table className="corr-table" style={{ borderCollapse: 'collapse' }}>
          <thead>
            <tr>
              <th className="corr-corner"></th>
              {labels.map(l => (
                <th key={l} className="corr-header" style={{ width: cellSize, minWidth: cellSize }}>
                  <span className="corr-header-text">{l}</span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {labels.map((rowLabel, ri) => (
              <tr key={rowLabel}>
                <td className="corr-row-label">{rowLabel}</td>
                {(matrix[ri] || []).map((val, ci) => (
                  <td key={ci} className="corr-cell"
                      style={{ backgroundColor: corrColor(val), width: cellSize, height: cellSize, fontSize: Math.max(8, cellSize * 0.35) }}
                      title={`${rowLabel} / ${labels[ci]}: ${val !== null ? val.toFixed(3) : 'N/A'}`}>
                    {showValues && val !== null ? val.toFixed(2) : ''}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="corr-legend">
        <span style={{ color: 'rgb(255, 50, 150)' }}>-1.0</span>
        <div className="corr-gradient"></div>
        <span style={{ color: 'rgb(50, 50, 255)' }}>+1.0</span>
      </div>
    </div>
  )
}

function ReturnsChart({ data, quarterDateRange, exportTrigger, basketName }: { data: CumulativeReturnsData; quarterDateRange?: { from: string; to: string } | null; exportTrigger?: number; basketName?: string }) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const [hoveredTicker, setHoveredTicker] = useState<string | null>(null)
  const [dims, setDims] = useState({ w: 800, h: 400 })
  const [presetMode, setPresetMode] = useState<'Q' | 'Y'>('Q')
  const [logScale, setLogScale] = useState(false)

  // Date range state — default to quarter range if active, else 1Y lookback
  const allDates = data.dates
  const defaultEnd = allDates.length > 0 ? allDates[allDates.length - 1] : ''
  const defaultStart = useMemo(() => {
    if (!allDates.length) return ''
    const end = new Date(allDates[allDates.length - 1])
    end.setFullYear(end.getFullYear() - 1)
    const target = end.toISOString().slice(0, 10)
    for (let i = 0; i < allDates.length; i++) {
      if (allDates[i] >= target) return allDates[i]
    }
    return allDates[0]
  }, [allDates])

  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')

  // Initialize defaults when data loads or quarter range changes
  useEffect(() => {
    if (quarterDateRange) {
      setStartDate(quarterDateRange.from)
      setEndDate(quarterDateRange.to)
    } else {
      setStartDate(defaultStart)
      setEndDate(defaultEnd)
    }
  }, [defaultStart, defaultEnd, quarterDateRange])

  const dateBoundsMin = allDates.length > 0 ? allDates[0] : ''
  const dateBoundsMax = allDates.length > 0 ? allDates[allDates.length - 1] : ''

  // Quarter presets — newest first
  const quarterPresets = useMemo(() => {
    if (!dateBoundsMin || !dateBoundsMax) return []
    const presets: { label: string; start: string; end: string }[] = []
    const minD = new Date(dateBoundsMin)
    const maxD = new Date(dateBoundsMax)
    let d = new Date(maxD)
    while (true) {
      const qMonth = Math.floor(d.getMonth() / 3) * 3
      const qStart = new Date(d.getFullYear(), qMonth, 1)
      const qEnd = new Date(d.getFullYear(), qMonth + 3, 0)
      if (qStart < minD && presets.length > 0) break
      const q = Math.floor(qMonth / 3) + 1
      presets.push({
        label: `${d.getFullYear()} Q${q}`,
        start: qStart.toISOString().slice(0, 10) < dateBoundsMin ? dateBoundsMin : qStart.toISOString().slice(0, 10),
        end: qEnd.toISOString().slice(0, 10) > dateBoundsMax ? dateBoundsMax : qEnd.toISOString().slice(0, 10),
      })
      d = new Date(qStart)
      d.setDate(d.getDate() - 1)
      if (d < minD) break
    }
    return presets
  }, [dateBoundsMin, dateBoundsMax])

  // Annual presets — newest first
  const annualPresets = useMemo(() => {
    if (!dateBoundsMin || !dateBoundsMax) return []
    const presets: { label: string; start: string; end: string }[] = []
    const minYear = new Date(dateBoundsMin).getFullYear()
    const maxYear = new Date(dateBoundsMax).getFullYear()
    for (let y = maxYear; y >= minYear; y--) {
      const yStart = `${y}-01-01` < dateBoundsMin ? dateBoundsMin : `${y}-01-01`
      const yEnd = `${y}-12-31` > dateBoundsMax ? dateBoundsMax : `${y}-12-31`
      presets.push({ label: `${y}`, start: yStart, end: yEnd })
    }
    return presets
  }, [dateBoundsMin, dateBoundsMax])

  const activePresets = presetMode === 'Q' ? quarterPresets : annualPresets

  // Slice data to selected date range and re-rebase to window start
  const windowedData = useMemo(() => {
    if (!allDates.length || !startDate || !endDate) return { dates: allDates, series: data.series }
    let si = 0, ei = allDates.length - 1
    for (let i = 0; i < allDates.length; i++) {
      if (allDates[i] >= startDate) { si = i; break }
    }
    for (let i = allDates.length - 1; i >= 0; i--) {
      if (allDates[i] <= endDate) { ei = i; break }
    }
    if (si > ei) return { dates: [], series: [] }
    const slicedDates = allDates.slice(si, ei + 1)
    const slicedSeries = data.series.map(s => {
      const vals = s.values.slice(si, ei + 1)
      // Find first non-null value in window to rebase from (0% at window start or join date)
      let baseVal: number | null = null
      for (let i = 0; i < vals.length; i++) {
        if (vals[i] !== null) { baseVal = vals[i]!; break }
      }
      if (baseVal === null || baseVal === 0) return { ...s, values: vals }
      // Rebase: convert from join-date-relative to window-start-relative
      const rebased = vals.map(v => {
        if (v === null) return null
        return (1 + v) / (1 + baseVal!) - 1
      })
      return { ...s, values: rebased }
    }).filter(s => s.values.some(v => v !== null))
    return { dates: slicedDates, series: slicedSeries }
  }, [allDates, data.series, startDate, endDate])

  // Sort series by latest return value in the windowed range
  const sortedSeries = useMemo(() => {
    const withLatest = windowedData.series.map((s, origIndex) => {
      let lastVal = 0
      for (let i = s.values.length - 1; i >= 0; i--) {
        if (s.values[i] !== null) { lastVal = s.values[i]!; break }
      }
      return { ...s, origIndex, lastVal }
    })
    withLatest.sort((a, b) => b.lastVal - a.lastVal)
    return withLatest
  }, [windowedData.series])

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

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas || !windowedData.dates.length || !windowedData.series.length) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    const dpr = window.devicePixelRatio || 1
    canvas.width = dims.w * dpr
    canvas.height = dims.h * dpr
    ctx.scale(dpr, dpr)

    const pad = { top: 20, right: 60, bottom: 50, left: 20 }
    const plotW = dims.w - pad.left - pad.right
    const plotH = dims.h - pad.top - pad.bottom

    // Find Y range from windowed data
    let yMin = 0, yMax = 0
    windowedData.series.forEach(s => s.values.forEach(v => {
      if (v !== null) { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) }
    }))
    const yPad = (yMax - yMin) * 0.1 || 0.05
    yMin -= yPad; yMax += yPad

    const numDates = windowedData.dates.length
    const xScale = (i: number) => pad.left + (numDates > 1 ? (i / (numDates - 1)) * plotW : plotW / 2)
    const logT = (v: number) => Math.log(Math.max(1 + v, 1e-8))
    const logYMin = logScale ? logT(yMin) : 0
    const logYMax = logScale ? logT(yMax) : 0
    const yScale = logScale
      ? (v: number) => pad.top + plotH - ((logT(v) - logYMin) / (logYMax - logYMin)) * plotH
      : (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH

    ctx.clearRect(0, 0, dims.w, dims.h)
    ctx.fillStyle = '#fdf6e3'
    ctx.fillRect(0, 0, dims.w, dims.h)

    // Grid
    ctx.strokeStyle = '#e9ecef'
    ctx.lineWidth = 1
    const nTicks = 6
    for (let i = 0; i <= nTicks; i++) {
      const v = logScale
        ? Math.exp(logYMin + (logYMax - logYMin) * (i / nTicks)) - 1
        : yMin + (yMax - yMin) * (i / nTicks)
      const y = yScale(v)
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(dims.w - pad.right, y); ctx.stroke()
      ctx.fillStyle = '#6c757d'; ctx.font = '10px sans-serif'; ctx.textAlign = 'left'
      ctx.fillText((v * 100).toFixed(0) + '%', dims.w - pad.right + 5, y + 3)
    }

    // Zero line
    const zeroY = yScale(0)
    ctx.strokeStyle = '#adb5bd'; ctx.lineWidth = 1; ctx.setLineDash([4, 4])
    ctx.beginPath(); ctx.moveTo(pad.left, zeroY); ctx.lineTo(dims.w - pad.right, zeroY); ctx.stroke()
    ctx.setLineDash([])

    // X axis labels
    const labelInterval = Math.max(1, Math.floor(numDates / 8))
    ctx.fillStyle = '#6c757d'; ctx.font = '10px sans-serif'; ctx.textAlign = 'center'
    for (let i = 0; i < numDates; i += labelInterval) {
      const x = xScale(i)
      ctx.fillText(windowedData.dates[i].slice(0, 7), x, dims.h - pad.bottom + 15)
    }

    // Draw lines — blue (best) → grey → pink (worst) by rank
    const totalSeries = sortedSeries.length
    sortedSeries.forEach((s, rank) => {
      const isHovered = hoveredTicker === s.ticker
      const isOther = hoveredTicker !== null && !isHovered
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
  }, [windowedData, dims, hoveredTicker, sortedSeries, logScale])

  // Export when exportTrigger fires
  const prevExportTrigger = useRef(exportTrigger || 0)
  useEffect(() => {
    if (!exportTrigger || exportTrigger === prevExportTrigger.current) {
      prevExportTrigger.current = exportTrigger || 0; return
    }
    prevExportTrigger.current = exportTrigger
    const canvas = canvasRef.current
    if (!canvas) return

    const bDisplay = (basketName || 'unknown').replace(/_/g, ' ')
    const titleLeft = `${bDisplay} Cumulative Returns`
    const dateRange = startDate && endDate ? `${startDate} to ${endDate}` : ''
    const filename = `${basketName || 'basket'}_returns_${startDate}_${endDate}.png`

    const dpr = window.devicePixelRatio || 1
    const chartW = canvas.width / dpr
    const rightW = 120
    const chartH = canvas.height / dpr
    const totalW = chartW + rightW
    const composite = document.createElement('canvas')
    composite.width = totalW * dpr; composite.height = canvas.height
    const cCtx = composite.getContext('2d')
    if (!cCtx) return
    cCtx.scale(dpr, dpr)
    cCtx.fillStyle = '#fdf6e3'; cCtx.fillRect(0, 0, totalW, chartH)
    cCtx.drawImage(canvas, 0, 0, canvas.width, canvas.height, 0, 0, chartW, chartH)
    // Right legend
    const lineH = 14
    cCtx.textBaseline = 'top'; cCtx.font = '10px monospace'
    for (let i = 0; i < sortedSeries.length && i * lineH + 8 < chartH; i++) {
      const s = sortedSeries[i]
      cCtx.fillStyle = rankColor(i, sortedSeries.length)
      cCtx.textAlign = 'left'; cCtx.fillText(s.ticker, chartW + 4, 8 + i * lineH)
      cCtx.textAlign = 'right'; cCtx.fillText((s.lastVal * 100).toFixed(1) + '%', totalW - 4, 8 + i * lineH)
    }
    // Title labels
    cCtx.fillStyle = '#586e75'; cCtx.textBaseline = 'top'
    cCtx.font = '11px monospace'; cCtx.textAlign = 'left'
    cCtx.fillText(titleLeft, 12, 8)
    cCtx.font = 'bold 11px monospace'; cCtx.textAlign = 'right'
    cCtx.fillText(dateRange, chartW - 60, 8)

    composite.toBlob(blob => {
      if (!blob) return
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url; a.download = filename; a.click()
      URL.revokeObjectURL(url)
    }, 'image/png')
  }, [exportTrigger])

  return (
    <div className="returns-container">
      <div className="returns-legend-left contrib-sidebar">
        <div className="returns-date-controls">
          <input type="date" className="date-input" value={startDate} min={dateBoundsMin} max={dateBoundsMax} onChange={e => setStartDate(e.target.value)} />
          <input type="date" className="date-input" value={endDate} min={dateBoundsMin} max={dateBoundsMax} onChange={e => setEndDate(e.target.value)} />
          <div className="returns-quick-btns">
            <button className="control-btn" onClick={() => { setStartDate(defaultStart); setEndDate(defaultEnd) }}>1Y</button>
            <button className="control-btn" onClick={() => { setStartDate(dateBoundsMin); setEndDate(dateBoundsMax) }}>All</button>
          </div>
        </div>
        <div className="contrib-preset-toggle">
          <button className={`contrib-toggle-btn ${presetMode === 'Q' ? 'active' : ''}`} onClick={() => setPresetMode('Q')}>Q</button>
          <button className={`contrib-toggle-btn ${presetMode === 'Y' ? 'active' : ''}`} onClick={() => setPresetMode('Y')}>Y</button>
        </div>
        <div className="contrib-quarter-presets">
          {activePresets.map(p => (
            <button
              key={p.label}
              className={`contrib-quarter-btn ${startDate === p.start && endDate === p.end ? 'active' : ''}`}
              onClick={() => { setStartDate(p.start); setEndDate(p.end) }}
            >
              {p.label}
            </button>
          ))}
        </div>
      </div>
      <div className="returns-right">
        <div className="returns-chart" ref={containerRef}>
          <canvas ref={canvasRef} style={{ width: '100%', height: '100%' }} />
          <button className={`log-toggle-btn ${logScale ? 'active' : ''}`} onClick={() => setLogScale(v => !v)}>L</button>
        </div>
      </div>
      <div className="returns-legend-right">
        {sortedSeries.map((s, rank) => (
          <div key={s.ticker}
               className={`returns-legend-item ${hoveredTicker === s.ticker ? 'highlighted' : ''}`}
               style={{ color: rankColor(rank, sortedSeries.length) }}
               onMouseEnter={() => setHoveredTicker(s.ticker)}
               onMouseLeave={() => setHoveredTicker(null)}>
            {s.ticker} <span className="returns-legend-val">{(s.lastVal * 100).toFixed(1)}%</span>
          </div>
        ))}
      </div>
    </div>
  )
}

interface ContributionData {
  tickers: string[]
  total_contributions: number[]
  initial_weights: number[]
  final_weights: number[]
  first_dates: string[]
  last_dates: string[]
  current_weights: (number | null)[]
  equity_dates: string[]
  equity_values: number[]
  dates: string[]
  date_range: { min: string; max: string }
}

function ContributionChart({ basketName, apiBase, quarterDateRange, exportTrigger }: { basketName: string; apiBase: string; quarterDateRange?: { from: string; to: string } | null; exportTrigger?: number }) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const [dims, setDims] = useState({ w: 800, h: 400 })
  const [contribData, setContribData] = useState<ContributionData | null>(null)
  const [loading, setLoading] = useState(false)
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [dateBounds, setDateBounds] = useState<{ min: string; max: string }>({ min: '', max: '' })
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)
  const [presetMode, setPresetMode] = useState<'Q' | 'Y'>('Q')

  // Fetch date bounds on mount, set initial date range
  useEffect(() => {
    if (!basketName || !apiBase) return
    axios.get(`${apiBase}/baskets/${encodeURIComponent(basketName)}/contributions`)
      .then(res => {
        if (res.data.date_range) {
          setDateBounds(res.data.date_range)
          if (quarterDateRange) {
            setStartDate(quarterDateRange.from)
            setEndDate(quarterDateRange.to)
          } else {
            // Default to current quarter
            const max = res.data.date_range.max
            if (max) {
              const d = new Date(max)
              const qMonth = Math.floor(d.getMonth() / 3) * 3
              const qStart = new Date(d.getFullYear(), qMonth, 1).toISOString().slice(0, 10)
              setStartDate(qStart)
              setEndDate(max)
            }
          }
        }
      })
      .catch(() => {})
  }, [basketName, apiBase])

  // Update date range when quarter mode changes
  useEffect(() => {
    if (quarterDateRange) {
      setStartDate(quarterDateRange.from)
      setEndDate(quarterDateRange.to)
    }
  }, [quarterDateRange])

  // Fetch contribution data when date range changes
  useEffect(() => {
    if (!basketName || !apiBase || !startDate || !endDate) return
    setLoading(true)
    const params = new URLSearchParams()
    if (startDate) params.set('start', startDate)
    if (endDate) params.set('end', endDate)
    axios.get(`${apiBase}/baskets/${encodeURIComponent(basketName)}/contributions?${params}`)
      .then(res => setContribData(res.data))
      .catch(() => setContribData(null))
      .finally(() => setLoading(false))
  }, [basketName, apiBase, startDate, endDate])

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

  // Quarter presets — all quarters, newest first
  const quarterPresets = useMemo(() => {
    if (!dateBounds.min || !dateBounds.max) return []
    const presets: { label: string; start: string; end: string }[] = []
    const minD = new Date(dateBounds.min)
    const maxD = new Date(dateBounds.max)
    let d = new Date(maxD)
    while (true) {
      const qMonth = Math.floor(d.getMonth() / 3) * 3
      const qStart = new Date(d.getFullYear(), qMonth, 1)
      const qEnd = new Date(d.getFullYear(), qMonth + 3, 0)
      if (qStart < minD && presets.length > 0) break
      const q = Math.floor(qMonth / 3) + 1
      presets.push({
        label: `${d.getFullYear()} Q${q}`,
        start: qStart.toISOString().slice(0, 10) < dateBounds.min ? dateBounds.min : qStart.toISOString().slice(0, 10),
        end: qEnd.toISOString().slice(0, 10) > dateBounds.max ? dateBounds.max : qEnd.toISOString().slice(0, 10),
      })
      d = new Date(qStart)
      d.setDate(d.getDate() - 1)
      if (d < minD) break
    }
    return presets
  }, [dateBounds])

  // Annual presets — newest first
  const annualPresets = useMemo(() => {
    if (!dateBounds.min || !dateBounds.max) return []
    const presets: { label: string; start: string; end: string }[] = []
    const minYear = new Date(dateBounds.min).getFullYear()
    const maxYear = new Date(dateBounds.max).getFullYear()
    for (let y = maxYear; y >= minYear; y--) {
      const yStart = `${y}-01-01` < dateBounds.min ? dateBounds.min : `${y}-01-01`
      const yEnd = `${y}-12-31` > dateBounds.max ? dateBounds.max : `${y}-12-31`
      presets.push({ label: `${y}`, start: yStart, end: yEnd })
    }
    return presets
  }, [dateBounds])

  const activePresets = presetMode === 'Q' ? quarterPresets : annualPresets

  // Canvas rendering
  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas || !contribData || !contribData.tickers.length) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return

    const dpr = window.devicePixelRatio || 1
    canvas.width = dims.w * dpr
    canvas.height = dims.h * dpr
    ctx.scale(dpr, dpr)

    const { tickers, total_contributions, equity_dates, equity_values } = contribData
    const n = tickers.length
    const pad = { top: 12, right: 50, bottom: 80, left: 60 }
    const totalH = dims.h - pad.top - pad.bottom
    const hasEquity = equity_dates && equity_dates.length > 1
    const eqH = hasEquity ? Math.floor(totalH * 0.25) : 0
    const sepGap = hasEquity ? 10 : 0
    const barPlotH = totalH - eqH - sepGap

    ctx.clearRect(0, 0, dims.w, dims.h)
    ctx.fillStyle = '#fdf6e3'
    ctx.fillRect(0, 0, dims.w, dims.h)

    const plotW = dims.w - pad.left - pad.right

    // === Equity curve as % return area chart (top region) ===
    if (hasEquity) {
      const eqBot = pad.top + eqH
      // Convert cumulative equity to % return (starts at 0%)
      const pctReturns = equity_values.map(v => (v - 1) * 100)
      let eqMin = 0, eqMax = 0
      pctReturns.forEach(v => { eqMin = Math.min(eqMin, v); eqMax = Math.max(eqMax, v) })
      const eqRange = eqMax - eqMin || 1
      const eqPad = eqRange * 0.1
      eqMin -= eqPad; eqMax += eqPad
      const eqYScale = (v: number) => eqBot - ((v - eqMin) / (eqMax - eqMin)) * eqH
      const zeroEqY = eqYScale(0)

      // Compute x positions once
      const pts = pctReturns.map((v, i) => ({
        x: pad.left + (i / (pctReturns.length - 1)) * plotW,
        y: eqYScale(v),
        v,
      }))

      // Fill positive area (blue) — clip above zero line
      ctx.save()
      ctx.beginPath()
      ctx.rect(pad.left, pad.top, plotW, zeroEqY - pad.top)
      ctx.clip()
      ctx.fillStyle = 'rgba(50, 50, 255, 0.18)'
      ctx.beginPath()
      ctx.moveTo(pts[0].x, zeroEqY)
      pts.forEach(p => ctx.lineTo(p.x, p.y))
      ctx.lineTo(pts[pts.length - 1].x, zeroEqY)
      ctx.closePath()
      ctx.fill()
      ctx.restore()

      // Fill negative area (pink) — clip below zero line
      ctx.save()
      ctx.beginPath()
      ctx.rect(pad.left, zeroEqY, plotW, eqBot - zeroEqY)
      ctx.clip()
      ctx.fillStyle = 'rgba(255, 50, 150, 0.18)'
      ctx.beginPath()
      ctx.moveTo(pts[0].x, zeroEqY)
      pts.forEach(p => ctx.lineTo(p.x, p.y))
      ctx.lineTo(pts[pts.length - 1].x, zeroEqY)
      ctx.closePath()
      ctx.fill()
      ctx.restore()

      // Zero line
      ctx.strokeStyle = '#adb5bd'; ctx.lineWidth = 1; ctx.setLineDash([3, 3])
      ctx.beginPath(); ctx.moveTo(pad.left, zeroEqY); ctx.lineTo(dims.w - pad.right, zeroEqY); ctx.stroke()
      ctx.setLineDash([])

      // Grid lines + right Y-axis labels
      ctx.strokeStyle = '#e9ecef'; ctx.lineWidth = 1
      const eqTicks = 3
      for (let i = 0; i <= eqTicks; i++) {
        const v = eqMin + (eqMax - eqMin) * (i / eqTicks)
        const y = eqYScale(v)
        ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(dims.w - pad.right, y); ctx.stroke()
        ctx.fillStyle = '#6c757d'; ctx.font = '9px monospace'; ctx.textAlign = 'left'
        ctx.fillText(v.toFixed(1) + '%', dims.w - pad.right + 4, y + 3)
      }

      // Equity line — colored by sign: blue above zero, pink below
      ctx.lineWidth = 1.5
      for (let i = 1; i < pts.length; i++) {
        const endVal = pts[i].v
        ctx.strokeStyle = endVal >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)'
        ctx.beginPath()
        ctx.moveTo(pts[i - 1].x, pts[i - 1].y)
        ctx.lineTo(pts[i].x, pts[i].y)
        ctx.stroke()
      }

      // X-axis date labels for equity curve (show ~5 evenly spaced)
      const nLabels = Math.min(5, equity_dates.length)
      ctx.fillStyle = '#93a1a1'; ctx.font = '8px monospace'; ctx.textAlign = 'center'
      for (let i = 0; i < nLabels; i++) {
        const idx = Math.round(i * (equity_dates.length - 1) / (nLabels - 1))
        const x = pad.left + (idx / (equity_dates.length - 1)) * plotW
        ctx.fillText(equity_dates[idx].slice(5), x, eqBot + 9)
      }

      // Separator line
      ctx.strokeStyle = '#adb5bd'; ctx.lineWidth = 1; ctx.setLineDash([3, 3])
      ctx.beginPath(); ctx.moveTo(pad.left, eqBot + sepGap / 2); ctx.lineTo(dims.w - pad.right, eqBot + sepGap / 2); ctx.stroke()
      ctx.setLineDash([])
    }

    // === Bar chart (bottom region) ===
    const barTop = pad.top + eqH + sepGap
    const barW = Math.max(4, Math.min(40, (plotW / n) * 0.75))
    const gap = (plotW - barW * n) / (n + 1)

    let yMin = 0, yMax = 0
    total_contributions.forEach(v => { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) })
    const yPad = (yMax - yMin) * 0.1 || 0.005
    yMin -= yPad; yMax += yPad

    const yScale = (v: number) => barTop + barPlotH - ((v - yMin) / (yMax - yMin)) * barPlotH
    const zeroY = yScale(0)

    // Grid
    ctx.strokeStyle = '#e9ecef'
    ctx.lineWidth = 1
    const nTicks = 6
    for (let i = 0; i <= nTicks; i++) {
      const v = yMin + (yMax - yMin) * (i / nTicks)
      const y = yScale(v)
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(dims.w - pad.right, y); ctx.stroke()
      ctx.fillStyle = '#6c757d'; ctx.font = '10px monospace'; ctx.textAlign = 'right'
      ctx.fillText((v * 100).toFixed(2) + '%', pad.left - 5, y + 3)
    }

    // Zero line
    ctx.strokeStyle = '#adb5bd'; ctx.lineWidth = 1; ctx.setLineDash([4, 4])
    ctx.beginPath(); ctx.moveTo(pad.left, zeroY); ctx.lineTo(dims.w - pad.right, zeroY); ctx.stroke()
    ctx.setLineDash([])

    // Bars — sorted worst to best (data already sorted by backend)
    for (let i = 0; i < n; i++) {
      const x = pad.left + gap + i * (barW + gap)
      const val = total_contributions[i]
      const bTop = val >= 0 ? yScale(val) : zeroY
      const bBot = val >= 0 ? zeroY : yScale(val)
      const barHeight = Math.max(1, bBot - bTop)

      const isHovered = hoveredIdx === i
      if (val >= 0) {
        ctx.fillStyle = isHovered ? 'rgb(30, 30, 220)' : 'rgb(50, 50, 255)'
      } else {
        ctx.fillStyle = isHovered ? 'rgb(220, 30, 120)' : 'rgb(255, 50, 150)'
      }
      ctx.fillRect(x, bTop, barW, barHeight)
    }

    // X axis labels (angled tickers)
    ctx.save()
    ctx.fillStyle = '#586e75'
    ctx.font = `${Math.min(11, Math.max(8, barW * 0.8))}px monospace`
    ctx.textAlign = 'right'
    for (let i = 0; i < n; i++) {
      const x = pad.left + gap + i * (barW + gap) + barW / 2
      ctx.save()
      ctx.translate(x, dims.h - pad.bottom + 8)
      ctx.rotate(-Math.PI / 4)
      ctx.fillText(tickers[i], 0, 0)
      ctx.restore()
    }
    ctx.restore()

  }, [contribData, dims, hoveredIdx])

  // Mouse hover detection
  const handleMouseMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    if (!contribData || !canvasRef.current) return
    const rect = canvasRef.current.getBoundingClientRect()
    const scaleX = dims.w / rect.width
    const mx = (e.clientX - rect.left) * scaleX
    const n = contribData.tickers.length
    const pad = { left: 60, right: 50 }
    const plotW = dims.w - pad.left - pad.right
    const barW = Math.max(4, Math.min(40, (plotW / n) * 0.75))
    const gap = (plotW - barW * n) / (n + 1)

    let found = -1
    for (let i = 0; i < n; i++) {
      const x = pad.left + gap + i * (barW + gap)
      if (mx >= x && mx <= x + barW) { found = i; break }
    }
    setHoveredIdx(found >= 0 ? found : null)
  }

  const hovered = hoveredIdx !== null && contribData ? {
    ticker: contribData.tickers[hoveredIdx],
    contribution: contribData.total_contributions[hoveredIdx],
    initialW: contribData.initial_weights[hoveredIdx],
    finalW: contribData.final_weights[hoveredIdx],
    firstDate: contribData.first_dates?.[hoveredIdx] ?? null,
    lastDate: contribData.last_dates?.[hoveredIdx] ?? null,
    currentWeight: contribData.current_weights?.[hoveredIdx] ?? null,
  } : null

  // Export when exportTrigger fires
  const prevExportTrigger = useRef(exportTrigger || 0)
  useEffect(() => {
    if (!exportTrigger || exportTrigger === prevExportTrigger.current) {
      prevExportTrigger.current = exportTrigger || 0; return
    }
    prevExportTrigger.current = exportTrigger
    const canvas = canvasRef.current
    if (!canvas) return

    const bDisplay = (basketName || 'unknown').replace(/_/g, ' ')
    const titleLeft = `${bDisplay} Contribution`
    const dateRange = startDate && endDate ? `${startDate} to ${endDate}` : ''
    const filename = `${basketName || 'basket'}_contribution_${startDate}_${endDate}.png`

    const dpr = window.devicePixelRatio || 1
    const composite = document.createElement('canvas')
    composite.width = canvas.width; composite.height = canvas.height
    const cCtx = composite.getContext('2d')
    if (!cCtx) return
    cCtx.drawImage(canvas, 0, 0)
    cCtx.scale(dpr, dpr)
    const cW = canvas.width / dpr
    // Title labels
    cCtx.fillStyle = '#586e75'; cCtx.textBaseline = 'top'
    cCtx.font = '11px monospace'; cCtx.textAlign = 'left'
    cCtx.fillText(titleLeft, 12, 8)
    cCtx.font = 'bold 11px monospace'; cCtx.textAlign = 'right'
    cCtx.fillText(dateRange, cW - 50, 8)

    composite.toBlob(blob => {
      if (!blob) return
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url; a.download = filename; a.click()
      URL.revokeObjectURL(url)
    }, 'image/png')
  }, [exportTrigger])

  return (
    <div className="returns-container">
      <div className="returns-legend-left contrib-sidebar">
        <div className="contrib-legend-header">
          {hovered ? (
            <>
              <div className="contrib-detail-ticker">{hovered.ticker}</div>
              <div className="contrib-detail-row">
                <span>Contrib:</span>
                <span style={{ color: hovered.contribution >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>
                  {(hovered.contribution * 100).toFixed(2)}%
                </span>
              </div>
              <div className="contrib-detail-row">
                <span>Init W:</span>
                <span>{(hovered.initialW * 100).toFixed(2)}%</span>
              </div>
              <div className="contrib-detail-row">
                <span>Final W:</span>
                <span>{(hovered.finalW * 100).toFixed(2)}%</span>
              </div>
              <div className="contrib-detail-row">
                <span>Drift:</span>
                <span style={{ color: hovered.finalW - hovered.initialW >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>
                  {((hovered.finalW - hovered.initialW) * 100).toFixed(2)}%
                </span>
              </div>
              {hovered.firstDate && (
                <div className="contrib-detail-row">
                  <span>Entry:</span>
                  <span>{hovered.firstDate}</span>
                </div>
              )}
              {hovered.lastDate && (
                <div className="contrib-detail-row">
                  <span>Exit:</span>
                  <span>{hovered.currentWeight !== null ? 'Active' : hovered.lastDate}</span>
                </div>
              )}
              {hovered.currentWeight !== null && (
                <div className="contrib-detail-row">
                  <span>Cur W:</span>
                  <span>{(hovered.currentWeight * 100).toFixed(2)}%</span>
                </div>
              )}
            </>
          ) : (
            <div className="contrib-hint">Hover a bar for details</div>
          )}
        </div>
        <div className="contrib-preset-toggle">
          <button className={`contrib-toggle-btn ${presetMode === 'Q' ? 'active' : ''}`} onClick={() => setPresetMode('Q')}>Q</button>
          <button className={`contrib-toggle-btn ${presetMode === 'Y' ? 'active' : ''}`} onClick={() => setPresetMode('Y')}>Y</button>
        </div>
        <div className="contrib-quarter-presets">
          {activePresets.map(p => (
            <button
              key={p.label}
              className={`contrib-quarter-btn ${startDate === p.start && endDate === p.end ? 'active' : ''}`}
              onClick={() => { setStartDate(p.start); setEndDate(p.end) }}
            >
              {p.label}
            </button>
          ))}
        </div>
      </div>
      <div className="returns-right">
        <div className="analysis-date-controls">
          <input type="date" className="date-input" value={startDate} min={dateBounds.min} max={dateBounds.max} onChange={e => setStartDate(e.target.value)} />
          <span className="date-separator">to</span>
          <input type="date" className="date-input" value={endDate} min={dateBounds.min} max={dateBounds.max} onChange={e => setEndDate(e.target.value)} />
          {loading && <span className="analysis-loading-hint">Loading...</span>}
        </div>
        <div className="returns-chart" ref={containerRef}>
          <canvas
            ref={canvasRef}
            style={{ width: '100%', height: '100%' }}
            onMouseMove={handleMouseMove}
            onMouseLeave={() => setHoveredIdx(null)}
          />
        </div>
      </div>
    </div>
  )
}

type BasketReturnMode = 'cross' | 'daily'
type BasketReturnGroup = 'all' | 'themes' | 'sectors' | 'industries'

interface BasketReturnItem {
  name: string
  group: string
  return: number
}

const BASKET_RETURN_PRESETS = [
  { label: '1D', days: 1 },
  { label: '1W', days: 7 },
  { label: '1M', days: 30 },
  { label: '3M', days: 90 },
  { label: '6M', days: 182 },
  { label: 'YTD', days: -1 },
  { label: '1Y', days: 365 },
  { label: '3Y', days: 1095 },
  { label: '5Y', days: 1825 },
  { label: 'ALL', days: 0 },
] as const

function applyPreset(preset: typeof BASKET_RETURN_PRESETS[number], maxDate: string, minDate: string): { start: string; end: string } {
  const end = maxDate
  if (preset.days === 0) return { start: minDate, end }
  if (preset.days === -1) {
    const yr = maxDate.slice(0, 4)
    return { start: `${yr}-01-01`, end }
  }
  if (preset.days === 1) {
    // 1D = single most recent day's return (start=end=maxDate, backend anchors to prior close)
    return { start: maxDate, end: maxDate }
  }
  const d = new Date(maxDate)
  d.setDate(d.getDate() - preset.days)
  const start = d.toISOString().slice(0, 10)
  return { start: start < minDate ? minDate : start, end }
}

export function BasketReturnsChart({ apiBase, exportTrigger, mode, initialBasket }: { apiBase: string; exportTrigger?: number; mode: BasketReturnMode; initialBasket?: string }) {
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const containerRef = useRef<HTMLDivElement>(null)
  const barGeoRef = useRef<{ padLeft: number; padRight: number; barW: number; gap: number; n: number } | null>(null)
  const [dims, setDims] = useState({ w: 800, h: 400 })
  const [group, setGroup] = useState<BasketReturnGroup>('all')
  const [dateBounds, setDateBounds] = useState<{ min: string; max: string }>({ min: '', max: '' })
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [activePreset, setActivePreset] = useState('YTD')
  const [scrollUnit, setScrollUnit] = useState<'1D'|'1W'|'1M'|'1Y'>('1Y')
  const [chartView, setChartView] = useState<'bar' | 'line'>('bar')
  const [loading, setLoading] = useState(false)
  const [hoveredIdx, setHoveredIdx] = useState<number | null>(null)
  const [hoveredName, setHoveredName] = useState<string | null>(null)
  const [legendSortCol, setLegendSortCol] = useState<'name' | 'change'>('change')
  const [legendSortAsc, setLegendSortAsc] = useState(false)
  const [barPeriod, setBarPeriod] = useState<'1D'|'1W'|'1M'|'1Q'|'1Y'>('1D')
  const [metric, setMetric] = useState<'returns'|'volatility'|'correlation'>('returns')

  // Cross-basket data
  const [baskets, setBaskets] = useState<BasketReturnItem[]>([])
  // Cumulative line chart data
  const [cumDates, setCumDates] = useState<string[]>([])
  const [cumSeries, setCumSeries] = useState<{ name: string; group: string; values: (number | null)[] }[]>([])
  // Daily data
  const [dailyBasket, setDailyBasket] = useState(initialBasket || '')
  useEffect(() => { if (initialBasket) setDailyBasket(initialBasket) }, [initialBasket])
  const [dailyDates, setDailyDates] = useState<string[]>([])
  const [dailyReturns, setDailyReturns] = useState<number[]>([])
  // Available baskets for daily mode picker
  const [availableBaskets, setAvailableBaskets] = useState<string[]>([])
  // Trading dates for 1D scroll (skip holidays/weekends)
  const [tradingDates, setTradingDates] = useState<string[]>([])
  // Basket search state
  const [basketSearch, setBasketSearch] = useState('')
  const [basketSearchOpen, setBasketSearchOpen] = useState(false)
  const basketSearchRef = useRef<HTMLInputElement>(null)
  const [basketSearchHighlight, setBasketSearchHighlight] = useState(0)
  const filteredBaskets = useMemo(() => {
    if (!basketSearch.trim()) return availableBaskets
    const q = basketSearch.toLowerCase()
    return availableBaskets.filter(b => b.toLowerCase().replace(/_/g, ' ').includes(q) || b.toLowerCase().includes(q))
  }, [basketSearch, availableBaskets])

  // Fetch date bounds on mount
  useEffect(() => {
    if (!apiBase) return
    axios.get(`${apiBase}/baskets/returns`)
      .then(res => {
        const dr = res.data.date_range
        if (dr) {
          setDateBounds(dr)
          const preset = BASKET_RETURN_PRESETS.find(p => p.label === 'YTD')!
          const { start, end } = applyPreset(preset, dr.max, dr.min)
          setStartDate(start)
          setEndDate(end)
        }
        if (res.data.baskets) {
          setAvailableBaskets(res.data.baskets.map((b: BasketReturnItem) => b.name).sort())
        }
        if (res.data.trading_dates) {
          setTradingDates(res.data.trading_dates)
        }
      })
      .catch(() => {})
  }, [apiBase])

  // Fetch data when params change
  useEffect(() => {
    if (!apiBase || !startDate || !endDate) return
    setLoading(true)
    if (mode === 'cross') {
      if (chartView === 'line') {
        const params = new URLSearchParams({ start: startDate, end: endDate, mode: 'cumulative', group, metric })
        axios.get(`${apiBase}/baskets/returns?${params}`)
          .then(res => {
            setCumDates(res.data.dates || [])
            setCumSeries(res.data.series || [])
            if (res.data.series) {
              setAvailableBaskets(res.data.series.map((s: { name: string }) => s.name).sort())
            }
          })
          .catch(() => { setCumDates([]); setCumSeries([]) })
          .finally(() => setLoading(false))
      } else {
        const params = new URLSearchParams({ start: startDate, end: endDate, mode: 'period', group, metric })
        axios.get(`${apiBase}/baskets/returns?${params}`)
          .then(res => {
            const sorted = (res.data.baskets || []).sort((a: BasketReturnItem, b: BasketReturnItem) => a.return - b.return)
            setBaskets(sorted)
            if (res.data.baskets) {
              setAvailableBaskets(res.data.baskets.map((b: BasketReturnItem) => b.name).sort())
            }
          })
          .catch(() => setBaskets([]))
          .finally(() => setLoading(false))
      }
    } else {
      if (!dailyBasket) { setLoading(false); return }
      const params = new URLSearchParams({ start: startDate, end: endDate, mode: 'daily', basket: dailyBasket, bar_period: barPeriod })
      axios.get(`${apiBase}/baskets/returns?${params}`)
        .then(res => {
          setDailyDates(res.data.dates || [])
          setDailyReturns(res.data.returns || [])
        })
        .catch(() => { setDailyDates([]); setDailyReturns([]) })
        .finally(() => setLoading(false))
    }
  }, [apiBase, startDate, endDate, mode, group, dailyBasket, chartView, barPeriod, metric])

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

  // Export when exportTrigger fires
  const prevExportTrigger = useRef(exportTrigger || 0)
  useEffect(() => {
    if (!exportTrigger || exportTrigger === prevExportTrigger.current) {
      prevExportTrigger.current = exportTrigger || 0
      return
    }
    prevExportTrigger.current = exportTrigger
    const canvas = canvasRef.current
    if (!canvas) return

    // Build descriptive title and filename
    const fmtDate = (d: string) => { const p = d.split('-'); return `${parseInt(p[1])}_${parseInt(p[2])}_${p[0]}` }
    const fmtDateDisplay = (d: string) => { const p = d.split('-'); return `${parseInt(p[1])}/${parseInt(p[2])}/${p[0]}` }
    const groupLabel = group === 'all' ? 'All Baskets' : group === 'themes' ? 'Themes' : group === 'sectors' ? 'Sectors' : 'Industries'
    const metricName = metric === 'volatility' ? 'Volatility' : metric === 'correlation' ? 'Correlation' : 'Price Returns'
    const chartLabel = chartView === 'bar' ? 'Bar Chart' : 'Line Chart'
    const dateRange = startDate && endDate ? `${fmtDateDisplay(startDate)} – ${fmtDateDisplay(endDate)}` : ''

    let titleLeft: string
    let name: string
    if (mode === 'cross') {
      titleLeft = `${groupLabel} ${metricName} ${activePreset || ''} ${chartLabel}`.trim()
      name = startDate === endDate
        ? `cross_basket_returns_${group}_${fmtDate(endDate)}`
        : `cross_basket_returns_${group}_${fmtDate(startDate)}_${fmtDate(endDate)}`
    } else {
      const bName = dailyBasket || 'unknown'
      const bDisplay = bName.replace(/_/g, ' ')
      titleLeft = `${bDisplay} ${metricName} ${barPeriod} ${chartLabel}`.trim()
      name = startDate === endDate
        ? `single_basket_returns_${bName}_${fmtDate(endDate)}`
        : `single_basket_returns_${bName}_${fmtDate(startDate)}_${fmtDate(endDate)}`
    }

    // Draw labels directly onto a copy of the chart canvas
    const dpr = window.devicePixelRatio || 1
    const composite = document.createElement('canvas')
    composite.width = canvas.width
    composite.height = canvas.height
    const cCtx = composite.getContext('2d')
    if (!cCtx) return
    cCtx.drawImage(canvas, 0, 0)
    cCtx.scale(dpr, dpr)
    const cW = canvas.width / dpr
    // Title top-left (at page edge)
    cCtx.fillStyle = '#586e75'
    cCtx.font = 'bold 13px monospace'
    cCtx.textAlign = 'left'
    cCtx.textBaseline = 'top'
    cCtx.fillText(titleLeft, 12, 8)
    // Date range top-right (aligned to plot area right edge, before y-axis)
    if (dateRange) {
      cCtx.textAlign = 'right'
      cCtx.fillText(dateRange, cW - 50, 8)
    }

    composite.toBlob((blob) => {
      if (!blob) return
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `${name}.png`
      a.click()
      URL.revokeObjectURL(url)
    }, 'image/png')
  }, [exportTrigger])

  // Value formatting: returns are % change (decimal ×100), vol/corr are absolute point change
  const isPointMetric = metric === 'volatility' || metric === 'correlation'
  const fmtVal = (v: number, decimals = 2) => isPointMetric ? v.toFixed(decimals) + ' pts' : (v * 100).toFixed(decimals) + '%'
  const fmtLegend = (v: number) => isPointMetric ? v.toFixed(2) : (v * 100).toFixed(1) + '%'
  const metricLabel = metric === 'volatility' ? 'Vol Chg' : metric === 'correlation' ? 'Corr Chg' : 'Chg'

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

    // Helper: draw a sorted bar chart given name→return map
    const drawBarChart = (
      _ctx: CanvasRenderingContext2D, _w: number, _h: number,
      items: { name: string; ret: number }[],
      _hoveredIdx: number | null,
      showLabels: boolean
    ) => {
      const n = items.length
      if (n === 0) return
      const labelFontSize = showLabels ? Math.min(11, Math.max(7, Math.floor((_w - 110) / n * 0.7 * 0.7))) : 8
      _ctx.font = `${labelFontSize}px monospace`
      let maxLabelW = 0
      if (showLabels) {
        for (let i = 0; i < n; i++) {
          const w = _ctx.measureText(items[i].name.replace(/_/g, ' ')).width
          if (w > maxLabelW) maxLabelW = w
        }
      }
      const dynamicBottom = showLabels ? Math.min(Math.ceil(maxLabelW * 0.707) + 16, Math.floor(_h * 0.4)) : 8
      // Left padding needs room for the first rotated label extending left
      const dynamicLeft = showLabels ? Math.min(Math.ceil(maxLabelW * 0.707) + 8, 80) : 4
      const pad = { top: showLabels ? 12 : 4, right: showLabels ? dynamicLeft : 4, bottom: dynamicBottom, left: dynamicLeft }
      const plotW = _w - pad.left - pad.right
      const plotH = _h - pad.top - pad.bottom
      const barW = Math.max(2, Math.min(40, (plotW / n) * 0.75))
      const gap = (plotW - barW * n) / (n + 1)
      barGeoRef.current = { padLeft: pad.left, padRight: pad.right, barW, gap, n }

      let yMin = 0, yMax = 0
      items.forEach(b => { yMin = Math.min(yMin, b.ret); yMax = Math.max(yMax, b.ret) })
      const yPad = (yMax - yMin) * 0.1 || 0.005
      yMin -= yPad; yMax += yPad

      const yScale = (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH
      const zeroY = yScale(0)

      if (showLabels) {
        _ctx.strokeStyle = '#e9ecef'; _ctx.lineWidth = 1
        const nTicks = 6
        for (let i = 0; i <= nTicks; i++) {
          const v = yMin + (yMax - yMin) * (i / nTicks)
          const y = yScale(v)
          _ctx.beginPath(); _ctx.moveTo(pad.left, y); _ctx.lineTo(_w - pad.right, y); _ctx.stroke()
          _ctx.fillStyle = '#6c757d'; _ctx.font = '10px monospace'; _ctx.textAlign = 'left'
          _ctx.fillText(fmtVal(v), _w - pad.right + 5, y + 3)
        }
      }

      _ctx.strokeStyle = '#adb5bd'; _ctx.lineWidth = 1; _ctx.setLineDash([4, 4])
      _ctx.beginPath(); _ctx.moveTo(pad.left, zeroY); _ctx.lineTo(_w - pad.right, zeroY); _ctx.stroke()
      _ctx.setLineDash([])

      for (let i = 0; i < n; i++) {
        const x = pad.left + gap + i * (barW + gap)
        const val = items[i].ret
        const bTop = val >= 0 ? yScale(val) : zeroY
        const bBot = val >= 0 ? zeroY : yScale(val)
        const barHeight = Math.max(1, bBot - bTop)
        const isHovered = _hoveredIdx === i
        _ctx.fillStyle = val >= 0
          ? (isHovered ? 'rgb(30, 30, 220)' : 'rgb(50, 50, 255)')
          : (isHovered ? 'rgb(220, 30, 120)' : 'rgb(255, 50, 150)')
        _ctx.fillRect(x, bTop, barW, barHeight)
      }

      if (showLabels) {
        _ctx.save()
        _ctx.fillStyle = '#586e75'
        _ctx.font = `${labelFontSize}px monospace`
        _ctx.textAlign = 'right'
        for (let i = 0; i < n; i++) {
          const x = pad.left + gap + i * (barW + gap) + barW / 2
          _ctx.save()
          _ctx.translate(x, _h - pad.bottom + 8)
          _ctx.rotate(-Math.PI / 4)
          _ctx.fillText(items[i].name.replace(/_/g, ' '), 0, 0)
          _ctx.restore()
        }
        _ctx.restore()
      }
    }

    if (mode === 'cross' && chartView === 'line') {
      // Rebased cumulative line chart
      if (cumDates.length === 0 || cumSeries.length === 0) return
      const pad = { top: 20, right: 60, bottom: 50, left: 20 }
      const plotW = dims.w - pad.left - pad.right
      const plotH = dims.h - pad.top - pad.bottom

      let yMin = 0, yMax = 0
      cumSeries.forEach(s => s.values.forEach(v => {
        if (v !== null) { yMin = Math.min(yMin, v); yMax = Math.max(yMax, v) }
      }))
      const yPad = (yMax - yMin) * 0.1 || 0.05
      yMin -= yPad; yMax += yPad

      const numDates = cumDates.length
      const xScale = (i: number) => pad.left + (numDates > 1 ? (i / (numDates - 1)) * plotW : plotW / 2)
      const yScale = (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH

      // Grid
      ctx.strokeStyle = '#e9ecef'; ctx.lineWidth = 1
      const nTicks = 6
      for (let i = 0; i <= nTicks; i++) {
        const v = yMin + (yMax - yMin) * (i / nTicks)
        const y = yScale(v)
        ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(dims.w - pad.right, y); ctx.stroke()
        ctx.fillStyle = '#6c757d'; ctx.font = '10px monospace'; ctx.textAlign = 'left'
        ctx.fillText(fmtVal(v, 1), dims.w - pad.right + 5, y + 3)
      }

      // Zero line
      const zeroY = yScale(0)
      ctx.strokeStyle = '#adb5bd'; ctx.lineWidth = 1; ctx.setLineDash([4, 4])
      ctx.beginPath(); ctx.moveTo(pad.left, zeroY); ctx.lineTo(dims.w - pad.right, zeroY); ctx.stroke()
      ctx.setLineDash([])

      // X axis labels
      const labelInterval = Math.max(1, Math.floor(numDates / 8))
      ctx.fillStyle = '#6c757d'; ctx.font = '10px monospace'; ctx.textAlign = 'center'
      for (let i = 0; i < numDates; i += labelInterval) {
        ctx.fillText(cumDates[i].slice(0, 7), xScale(i), dims.h - pad.bottom + 15)
      }

      // Sort series by final return value (best first)
      const sorted = cumSeries.map((s, idx) => {
        let lastVal = 0
        for (let i = s.values.length - 1; i >= 0; i--) {
          if (s.values[i] !== null) { lastVal = s.values[i]!; break }
        }
        return { ...s, idx, lastVal }
      }).sort((a, b) => b.lastVal - a.lastVal)

      // Draw lines
      const totalSeries = sorted.length
      sorted.forEach((s, rank) => {
        const isHovered = hoveredName === s.name
        const isOther = hoveredName !== null && !isHovered
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
    } else if (mode === 'cross') {
      const items = baskets.map(b => ({ name: b.name, ret: b.return }))
      drawBarChart(ctx, dims.w, dims.h, items, hoveredIdx, true)
    } else {
      const n = dailyReturns.length
      if (n === 0) return
      const pad = { top: 12, right: 50, bottom: 60, left: 20 }
      const plotW = dims.w - pad.left - pad.right
      const plotH = dims.h - pad.top - pad.bottom
      // Bars fill the full plot width with 25% gap ratio
      const barW = Math.max(1, (plotW / n) * 0.75)
      const gap = (plotW - barW * n) / (n + 1)
      barGeoRef.current = { padLeft: pad.left, padRight: pad.right, barW, gap, n }

      let yMin = 0, yMax = 0
      dailyReturns.forEach(r => { yMin = Math.min(yMin, r); yMax = Math.max(yMax, r) })
      const yPad = (yMax - yMin) * 0.1 || 0.005
      yMin -= yPad; yMax += yPad

      const yScale = (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH
      const zeroY = yScale(0)

      ctx.strokeStyle = '#e9ecef'; ctx.lineWidth = 1
      const nTicks = 6
      for (let i = 0; i <= nTicks; i++) {
        const v = yMin + (yMax - yMin) * (i / nTicks)
        const y = yScale(v)
        ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(dims.w - pad.right, y); ctx.stroke()
        ctx.fillStyle = '#6c757d'; ctx.font = '10px monospace'; ctx.textAlign = 'left'
        ctx.fillText((v * 100).toFixed(2) + '%', dims.w - pad.right + 5, y + 3)
      }

      ctx.strokeStyle = '#adb5bd'; ctx.lineWidth = 1; ctx.setLineDash([4, 4])
      ctx.beginPath(); ctx.moveTo(pad.left, zeroY); ctx.lineTo(dims.w - pad.right, zeroY); ctx.stroke()
      ctx.setLineDash([])

      for (let i = 0; i < n; i++) {
        const x = pad.left + gap + i * (barW + gap)
        const val = dailyReturns[i]
        const bTop = val >= 0 ? yScale(val) : zeroY
        const bBot = val >= 0 ? zeroY : yScale(val)
        const barHeight = Math.max(1, bBot - bTop)
        const isHovered = hoveredIdx === i
        ctx.fillStyle = val >= 0
          ? (isHovered ? 'rgb(30, 30, 220)' : 'rgb(50, 50, 255)')
          : (isHovered ? 'rgb(220, 30, 120)' : 'rgb(255, 50, 150)')
        ctx.fillRect(x, bTop, barW, barHeight)
      }

      ctx.fillStyle = '#93a1a1'; ctx.font = '9px monospace'; ctx.textAlign = 'center'
      if (n === 1) {
        const x = pad.left + gap + barW / 2
        ctx.fillText(dailyDates[0], x, dims.h - pad.bottom + 14)
      } else {
        const nLabels = Math.min(6, n)
        for (let li = 0; li < nLabels; li++) {
          const idx = Math.round(li * (n - 1) / (nLabels - 1))
          const x = pad.left + gap + idx * (barW + gap) + barW / 2
          ctx.fillText(dailyDates[idx], x, dims.h - pad.bottom + 14)
        }
      }
    }
  }, [baskets, dailyReturns, dailyDates, dims, hoveredIdx, hoveredName, mode, chartView, cumDates, cumSeries, isPointMetric])

  // Sorted cumulative series for legend (memoized)
  const sortedCumSeries = useMemo(() => {
    const withLast = cumSeries.map((s, idx) => {
      let lastVal = 0
      for (let i = s.values.length - 1; i >= 0; i--) {
        if (s.values[i] !== null) { lastVal = s.values[i]!; break }
      }
      return { ...s, idx, lastVal }
    })
    withLast.sort((a, b) => {
      let cmp = 0
      if (legendSortCol === 'name') cmp = a.name.localeCompare(b.name)
      else cmp = a.lastVal - b.lastVal
      return legendSortAsc ? cmp : -cmp
    })
    return withLast
  }, [cumSeries, legendSortCol, legendSortAsc])

  // Mouse hover
  const handleMouseMove = (e: React.MouseEvent<HTMLCanvasElement>) => {
    if (!canvasRef.current) return
    if (mode === 'cross' && chartView === 'line') return // hover via legend only
    const geo = barGeoRef.current
    if (!geo || !geo.n) return
    const rect = canvasRef.current.getBoundingClientRect()
    const scaleX = dims.w / rect.width
    const mx = (e.clientX - rect.left) * scaleX

    let found = -1
    for (let i = 0; i < geo.n; i++) {
      const x = geo.padLeft + geo.gap + i * (geo.barW + geo.gap)
      if (mx >= x && mx <= x + geo.barW) { found = i; break }
    }
    setHoveredIdx(found >= 0 ? found : null)
  }

  const hovered = hoveredIdx !== null ? (mode === 'cross' && chartView === 'bar' && baskets[hoveredIdx]
    ? { name: baskets[hoveredIdx].name.replace(/_/g, ' '), ret: baskets[hoveredIdx].return, group: baskets[hoveredIdx].group }
    : mode === 'daily' && dailyDates[hoveredIdx] !== undefined
      ? { name: dailyDates[hoveredIdx], ret: dailyReturns[hoveredIdx], group: dailyBasket.replace(/_/g, ' ') }
      : null
  ) : null

  const handlePreset = (p: typeof BASKET_RETURN_PRESETS[number]) => {
    if (!dateBounds.max) return
    setActivePreset(p.label)
    const { start, end } = applyPreset(p, dateBounds.max, dateBounds.min)
    setStartDate(start)
    setEndDate(end)
  }

  const scrollPeriod = (dir: number) => {
    if (!dateBounds.max || !dateBounds.min) return
    const pad = (n: number) => n < 10 ? '0' + n : '' + n
    const fmt = (d: Date) => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
    const parse = (s: string) => { const [y, m, d] = s.split('-').map(Number); return new Date(y, m - 1, d) }
    const anchor = parse(startDate || dateBounds.max)

    let ns: string, ne: string

    if (scrollUnit === '1D') {
      const cur = endDate || dateBounds.max
      if (tradingDates.length > 0) {
        // Snap to next/prev trading date
        let idx = -1
        for (let i = 0; i < tradingDates.length; i++) {
          if (tradingDates[i] >= cur) { idx = i; break }
        }
        if (idx === -1) idx = tradingDates.length - 1
        const target = idx + dir
        if (target < 0 || target >= tradingDates.length) return
        ns = ne = tradingDates[target]
      } else {
        const d = parse(cur)
        d.setDate(d.getDate() + dir)
        ns = ne = fmt(d)
      }
    } else if (scrollUnit === '1W') {
      const dow = anchor.getDay()
      const mon = new Date(anchor)
      mon.setDate(anchor.getDate() - ((dow + 6) % 7))
      mon.setDate(mon.getDate() + dir * 7)
      const fri = new Date(mon)
      fri.setDate(mon.getDate() + 4)
      ns = fmt(mon)
      ne = fmt(fri)
    } else if (scrollUnit === '1M') {
      const newMonth = anchor.getMonth() + dir
      const s = new Date(anchor.getFullYear(), newMonth, 1)
      const e = new Date(s.getFullYear(), s.getMonth() + 1, 0)
      ns = fmt(s)
      ne = fmt(e)
    } else {
      const yr = anchor.getFullYear() + dir
      ns = `${yr}-01-01`
      ne = `${yr}-12-31`
    }

    // Block only if entire period is outside data range
    if (ne < dateBounds.min || ns > dateBounds.max) return
    // Clamp edges to data bounds
    if (ns < dateBounds.min) ns = dateBounds.min
    if (ne > dateBounds.max) ne = dateBounds.max

    setStartDate(ns)
    setEndDate(ne)
    setActivePreset('')
  }

  const selectBasket = (b: string) => {
    setDailyBasket(b)
    setBasketSearch('')
    setBasketSearchOpen(false)
  }

  const handleBasketSearchKey = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); setBasketSearchHighlight(h => Math.min(h + 1, filteredBaskets.length - 1)) }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setBasketSearchHighlight(h => Math.max(h - 1, 0)) }
    else if (e.key === 'Enter' && filteredBaskets.length > 0) { selectBasket(filteredBaskets[basketSearchHighlight]) }
    else if (e.key === 'Escape') { setBasketSearchOpen(false) }
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0 }}>
      {/* Controls bar */}
      <div className="analysis-date-controls">
        {/* Group filter + view toggle (cross mode) */}
        {mode === 'cross' && (
          <>
            {(['all', 'themes', 'sectors', 'industries'] as BasketReturnGroup[]).map(g => (
              <button key={g} className={`basket-returns-preset-btn ${group === g ? 'active' : ''}`} onClick={() => setGroup(g)}>
                {g === 'all' ? 'ALL' : g === 'themes' ? 'T' : g === 'sectors' ? 'S' : 'I'}
              </button>
            ))}
            <span style={{ color: 'var(--border-color)', margin: '0 1px' }}>|</span>
            <button className={`basket-returns-preset-btn ${metric === 'returns' ? 'active' : ''}`} onClick={() => setMetric('returns')}>Returns</button>
            <button className={`basket-returns-preset-btn ${metric === 'volatility' ? 'active' : ''}`} onClick={() => setMetric('volatility')}>Volatility</button>
            <button className={`basket-returns-preset-btn ${metric === 'correlation' ? 'active' : ''}`} onClick={() => setMetric('correlation')}>Correlation</button>
            <span style={{ color: 'var(--border-color)', margin: '0 1px' }}>|</span>
            <button className={`basket-returns-preset-btn ${chartView === 'bar' ? 'active' : ''}`} onClick={() => setChartView('bar')}>BAR</button>
            <button className={`basket-returns-preset-btn ${chartView === 'line' ? 'active' : ''}`} onClick={() => setChartView('line')}>LINE</button>
          </>
        )}
        {/* Basket search + bar period (single/daily mode) */}
        {mode === 'daily' && (
          <>
          <div className="search-container" style={{ position: 'relative' }}>
            <input
              ref={basketSearchRef}
              className="search-input"
              style={{ width: 180, fontSize: 11, padding: '3px 6px', height: 24 }}
              placeholder={dailyBasket ? dailyBasket.replace(/_/g, ' ') : 'Search basket...'}
              value={basketSearch}
              onChange={e => { setBasketSearch(e.target.value); setBasketSearchOpen(true); setBasketSearchHighlight(0) }}
              onFocus={() => setBasketSearchOpen(true)}
              onKeyDown={handleBasketSearchKey}
            />
            {basketSearchOpen && (basketSearch.trim() || true) && (
              <>
                <div className="col-filter-backdrop" onClick={() => setBasketSearchOpen(false)} />
                <div className="search-dropdown" style={{ width: 220, maxHeight: 300, top: '100%', left: 0 }}>
                  <div className="search-results">
                    {filteredBaskets.map((b, i) => (
                      <div
                        key={b}
                        className={`search-result-item ${i === basketSearchHighlight ? 'highlighted' : ''}`}
                        onMouseDown={() => selectBasket(b)}
                        onMouseEnter={() => setBasketSearchHighlight(i)}
                      >
                        <span className="search-result-name">{b.replace(/_/g, ' ')}</span>
                      </div>
                    ))}
                    {filteredBaskets.length === 0 && (
                      <div className="search-result-empty">No baskets found</div>
                    )}
                  </div>
                </div>
              </>
            )}
          </div>
          <span style={{ color: 'var(--border-color)', margin: '0 1px' }}>|</span>
          <span style={{ fontSize: 9, fontWeight: 'bold', color: 'var(--text-bold)', whiteSpace: 'nowrap' }}>Timeframe</span>
          <div className="basket-returns-presets">
            {(['1D','1W','1M','1Q','1Y'] as const).map(p => (
              <button key={p} className={`basket-returns-preset-btn ${barPeriod === p ? 'active' : ''}`} onClick={() => setBarPeriod(p)}>{p}</button>
            ))}
          </div>
          </>
        )}
        <span style={{ color: 'var(--border-color)', margin: '0 1px' }}>|</span>
        <span style={{ fontSize: 9, fontWeight: 'bold', color: 'var(--text-bold)', whiteSpace: 'nowrap' }}>Date Range</span>
        <div className="basket-returns-presets">
          {BASKET_RETURN_PRESETS.map(p => (
            <button key={p.label} className={`basket-returns-preset-btn ${activePreset === p.label ? 'active' : ''}`} onClick={() => handlePreset(p)}>
              {p.label}
            </button>
          ))}
        </div>
        {loading && <span className="analysis-loading-hint">Loading...</span>}
        <div style={{ flex: 1 }} />
        <div className="basket-returns-presets">
          <button className="basket-returns-preset-btn" onClick={() => scrollPeriod(-1)} title="Previous period">{'\u25C0'}</button>
          {(['1D','1W','1M','1Y'] as const).map(u => (
            <button key={u} className={`basket-returns-preset-btn ${scrollUnit === u ? 'active' : ''}`} onClick={() => setScrollUnit(u)}>{u}</button>
          ))}
          <button className="basket-returns-preset-btn" onClick={() => scrollPeriod(1)} title="Next period">{'\u25B6'}</button>
        </div>
        <input type="date" className="date-input" value={startDate} min={dateBounds.min} max={dateBounds.max} onChange={e => { setStartDate(e.target.value); setActivePreset('') }} />
        <span style={{ fontSize: 10, color: 'var(--text-main)' }}>to</span>
        <input type="date" className="date-input" value={endDate} min={dateBounds.min} max={dateBounds.max} onChange={e => { setEndDate(e.target.value); setActivePreset('') }} />
      </div>
      {/* Chart area */}
      <div style={{ flex: 1, minHeight: 0, display: 'flex' }}>
        <div style={{ flex: 1, minWidth: 0, position: 'relative' }} ref={containerRef}>
          <canvas
            ref={canvasRef}
            style={{ width: '100%', height: '100%' }}
            onMouseMove={handleMouseMove}
            onMouseLeave={() => { setHoveredIdx(null); setHoveredName(null) }}
          />
          {hovered && (
            <div className="candle-detail-overlay" style={{ minWidth: 140 }}>
              <div className="candle-detail-title">{hovered.name}</div>
              <div className="candle-detail-row">
                <span>{metric === 'volatility' ? 'Vol' : metric === 'correlation' ? 'Corr' : 'Return'}</span>
                <span className="ret" style={{ color: hovered.ret >= 0 ? 'rgb(50, 50, 255)' : 'rgb(255, 50, 150)' }}>
                  {fmtVal(hovered.ret)}
                </span>
              </div>
              <div className="candle-detail-row">
                <span>Group</span>
                <span className="ret">{hovered.group}</span>
              </div>
            </div>
          )}
        </div>
        {mode === 'cross' && chartView === 'line' && (
          <div className="backtest-path-legend">
            <div className="path-legend-header">
              <span className="path-legend-col ticker" onClick={() => { if (legendSortCol === 'name') setLegendSortAsc(v => !v); else { setLegendSortCol('name'); setLegendSortAsc(true) } }}>
                Basket{legendSortCol === 'name' ? (legendSortAsc ? ' \u25B2' : ' \u25BC') : ''}
              </span>
              <span className="path-legend-col change" onClick={() => { if (legendSortCol === 'change') setLegendSortAsc(v => !v); else { setLegendSortCol('change'); setLegendSortAsc(false) } }}>
                {metricLabel}{legendSortCol === 'change' ? (legendSortAsc ? ' \u25B2' : ' \u25BC') : ''}
              </span>
            </div>
            {sortedCumSeries.map((s, rank) => (
              <div key={s.name}
                   className={`path-legend-row ${hoveredName === s.name ? 'highlighted' : ''}`}
                   style={{ color: rankColor(rank, sortedCumSeries.length) }}
                   onMouseEnter={() => setHoveredName(s.name)}
                   onMouseLeave={() => setHoveredName(null)}>
                <span className="path-legend-col ticker">{s.name.replace(/_/g, ' ')}</span>
                <span className="path-legend-col change">{fmtLegend(s.lastVal)}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

export function BasketSummary({ data, loading, basketName, apiBase, quarterDateRange, exportTrigger, analysisMode = 'intra', allBaskets, onBasketSelect }: BasketSummaryProps) {
  const [tab, setTab] = useState<TabType>(analysisMode === 'cross' ? 'cross_returns' : 'correlation')
  const [intraSearch, setIntraSearch] = useState('')
  const [intraSearchOpen, setIntraSearchOpen] = useState(false)
  const [intraSearchHighlight, setIntraSearchHighlight] = useState(0)
  const intraSearchRef = useRef<HTMLInputElement>(null)

  const filteredIntraBaskets = useMemo(() => {
    const q = intraSearch.toLowerCase().trim()
    const filterList = (list: string[]) => q ? list.filter(b => b.toLowerCase().replace(/_/g, ' ').includes(q) || b.toLowerCase().includes(q)) : list
    if (!allBaskets) return []
    return [
      ...filterList(allBaskets.Themes).map(b => ({ name: b, group: 'Theme' as const })),
      ...filterList(allBaskets.Sectors).map(b => ({ name: b, group: 'Sector' as const })),
      ...filterList(allBaskets.Industries).map(b => ({ name: b, group: 'Industry' as const })),
    ]
  }, [intraSearch, allBaskets])

  // When analysisMode changes, reset to a valid default tab
  const prevMode = useRef(analysisMode)
  if (analysisMode !== prevMode.current) {
    prevMode.current = analysisMode
    if (analysisMode === 'cross' && tab !== 'cross_returns' && tab !== 'single_returns') {
      setTab('cross_returns')
    } else if (analysisMode === 'intra' && (tab === 'cross_returns' || tab === 'single_returns')) {
      setTab('correlation')
    }
  }

  // Cross-basket mode — no summary data needed
  if (analysisMode === 'cross') {
    const crossTab = tab === 'single_returns' ? 'single_returns' : 'cross_returns'
    return (
      <div className="summary-panel">
        <div className="summary-tabs">
          <button className={`summary-tab ${crossTab === 'cross_returns' ? 'active' : ''}`} onClick={() => setTab('cross_returns')}>
            Cross-Basket Returns
          </button>
          <button className={`summary-tab ${crossTab === 'single_returns' ? 'active' : ''}`} onClick={() => setTab('single_returns')}>
            Single-Basket Returns
          </button>
        </div>
        <div className="summary-content">
          <BasketReturnsChart apiBase={apiBase} exportTrigger={exportTrigger} mode={crossTab === 'cross_returns' ? 'cross' : 'daily'} />
        </div>
      </div>
    )
  }

  // Basket picker bar (shared by both states: no basket selected and basket loaded)
  const handleIntraSearchKey = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); setIntraSearchHighlight(h => Math.min(h + 1, filteredIntraBaskets.length - 1)) }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setIntraSearchHighlight(h => Math.max(h - 1, 0)) }
    else if (e.key === 'Enter' && filteredIntraBaskets.length > 0) {
      onBasketSelect?.(filteredIntraBaskets[intraSearchHighlight].name)
      setIntraSearch('')
      setIntraSearchOpen(false)
    }
    else if (e.key === 'Escape') { setIntraSearchOpen(false) }
  }
  let flatIdx = 0
  const groupedItems: { name: string; group: string; idx: number }[] = filteredIntraBaskets.map(b => ({ ...b, idx: flatIdx++ }))
  const groups = ['Theme', 'Sector', 'Industry'] as const

  const basketPickerBar = (
    <div className="analysis-date-controls" style={{ justifyContent: 'flex-start' }}>
      <span style={{ fontSize: 11, fontWeight: 'bold', textTransform: 'uppercase', color: 'var(--text-bold)', whiteSpace: 'nowrap' }}>Analyze:</span>
      <div className="search-container" style={{ position: 'relative' }}>
        <input
          ref={intraSearchRef}
          className="search-input"
          style={{ width: 250, fontSize: 12, padding: '4px 8px' }}
          placeholder={basketName ? basketName.replace(/_/g, ' ') : 'Search baskets...'}
          value={intraSearch}
          onChange={e => { setIntraSearch(e.target.value); setIntraSearchOpen(true); setIntraSearchHighlight(0) }}
          onFocus={() => setIntraSearchOpen(true)}
          onKeyDown={handleIntraSearchKey}
        />
        {intraSearchOpen && (
          <>
            <div className="col-filter-backdrop" onClick={() => setIntraSearchOpen(false)} />
            <div className="search-dropdown" style={{ width: 300, maxHeight: 400 }}>
              <div className="search-results">
                {groups.map(g => {
                  const items = groupedItems.filter(b => b.group === g)
                  if (items.length === 0) return null
                  return (
                    <div key={g}>
                      <div style={{ padding: '6px 12px', fontSize: 10, fontWeight: 'bold', textTransform: 'uppercase', color: 'var(--text-bold)', background: 'var(--bg-sidebar)', borderBottom: '1px solid var(--border-color)', letterSpacing: '0.05em' }}>{g}s</div>
                      {items.map(b => (
                        <div
                          key={b.name}
                          className={`search-result-item ${b.idx === intraSearchHighlight ? 'highlighted' : ''}`}
                          onMouseDown={() => { onBasketSelect?.(b.name); setIntraSearch(''); setIntraSearchOpen(false) }}
                          onMouseEnter={() => setIntraSearchHighlight(b.idx)}
                        >
                          <span className="search-result-name">{b.name.replace(/_/g, ' ')}</span>
                        </div>
                      ))}
                    </div>
                  )
                })}
                {groupedItems.length === 0 && (
                  <div className="search-result-empty">No baskets found</div>
                )}
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  )

  // Intrabasket mode — no basket selected yet
  if (!basketName) {
    return (
      <div className="summary-panel">
        {basketPickerBar}
        <div className="summary-content" />
      </div>
    )
  }

  // Intrabasket mode — needs summary data
  if (loading) return <div className="summary-panel">{basketPickerBar}<div className="summary-loading">Loading basket analysis...</div></div>
  if (!data) return <div className="summary-panel">{basketPickerBar}<div className="summary-empty">No summary data</div></div>

  return (
    <div className="summary-panel">
      {basketPickerBar}
      <div className="summary-tabs">
        <button className={`summary-tab ${tab === 'correlation' ? 'active' : ''}`} onClick={() => setTab('correlation')}>
          Correlation
        </button>
        <button className={`summary-tab ${tab === 'returns' ? 'active' : ''}`} onClick={() => setTab('returns')}>
          Returns
        </button>
        <button className={`summary-tab ${tab === 'contribution' ? 'active' : ''}`} onClick={() => setTab('contribution')}>
          Contribution
        </button>
      </div>
      <div className="summary-content">
        {tab === 'correlation' && <CorrelationHeatmap data={data.correlation} basketName={basketName} apiBase={apiBase} quarterDateRange={quarterDateRange} />}
        {tab === 'returns' && <ReturnsChart data={data.cumulative_returns} quarterDateRange={quarterDateRange} exportTrigger={exportTrigger} basketName={basketName} />}
        {tab === 'contribution' && <ContributionChart basketName={basketName} apiBase={apiBase} quarterDateRange={quarterDateRange} exportTrigger={exportTrigger} />}
      </div>
    </div>
  )
}
