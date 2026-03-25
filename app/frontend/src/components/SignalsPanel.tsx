import { useState, useEffect, useMemo, useRef } from 'react'
import axios from 'axios'

interface SignalRow {
  ticker: string
  name: string | null
  sector: string | null
  industry: string | null
  signal: string
  entry_date: string
  entry_price: number | null
  exit_date: string | null
  exit_price: number | null
  mae: number | null
  mfe: number | null
  pct_chg: number | null
  status: 'open' | 'closed'
}

interface SignalsPanelProps {
  apiBase: string
  exportTrigger?: number
}

type Universe = 'stocks' | 'etfs' | 'baskets'
type SortCol = 'ticker' | 'name' | 'sector' | 'industry' | 'signal' | 'entry_date' | 'entry_price' | 'exit_date' | 'exit_price' | 'mae' | 'mfe' | 'pct_chg' | 'status'

const PERIODS = ['1D', '1W', '2W', '1M', '3M', '6M', '1Y'] as const
const SIGNAL_TYPES = ['Breakout', 'Breakdown', 'Up_Rot', 'Down_Rot', 'BTFD', 'STFR'] as const
const BULL_SIGNALS = new Set(['Breakout', 'Up_Rot', 'BTFD'])

export function SignalsPanel({ apiBase, exportTrigger }: SignalsPanelProps) {
  const [universe, setUniverse] = useState<Universe>('stocks')
  const [period, setPeriod] = useState('1M')
  const [data, setData] = useState<SignalRow[]>([])
  const [loading, setLoading] = useState(false)
  const [sortCol, setSortCol] = useState<SortCol>('entry_date')
  const [sortDir, setSortDir] = useState<1 | -1>(-1)
  const [signalFilter, setSignalFilter] = useState<Set<string>>(new Set(SIGNAL_TYPES))
  const [statusFilter, setStatusFilter] = useState<'all' | 'open' | 'closed'>('all')

  useEffect(() => {
    setLoading(true)
    axios.get(`${apiBase}/signals/log`, { params: { universe, period: period.toLowerCase() } })
      .then(res => setData(res.data.signals || []))
      .catch(() => setData([]))
      .finally(() => setLoading(false))
  }, [apiBase, universe, period])

  const filtered = useMemo(() => {
    let list = data
    if (signalFilter.size < SIGNAL_TYPES.length) {
      list = list.filter(r => signalFilter.has(r.signal))
    }
    if (statusFilter !== 'all') {
      list = list.filter(r => r.status === statusFilter)
    }
    return list
  }, [data, signalFilter, statusFilter])

  const sorted = useMemo(() => {
    return [...filtered].sort((a, b) => {
      const av = a[sortCol], bv = b[sortCol]
      if (av == null && bv == null) return 0
      if (av == null) return 1
      if (bv == null) return -1
      if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * sortDir
      return String(av).localeCompare(String(bv)) * sortDir
    })
  }, [filtered, sortCol, sortDir])

  const handleSort = (col: SortCol) => {
    if (sortCol === col) setSortDir(d => d === 1 ? -1 : 1)
    else { setSortCol(col); setSortDir(col === 'entry_date' ? -1 : 1) }
  }

  const toggleSignal = (sig: string) => {
    setSignalFilter(prev => {
      const next = new Set(prev)
      if (next.has(sig)) { if (next.size > 1) next.delete(sig) }
      else next.add(sig)
      return next
    })
  }

  // Export
  const prevExportTrigger = useRef(exportTrigger || 0)
  useEffect(() => {
    if (!exportTrigger || exportTrigger === prevExportTrigger.current) {
      prevExportTrigger.current = exportTrigger || 0; return
    }
    prevExportTrigger.current = exportTrigger
    if (sorted.length === 0) return

    const uLabel = universe === 'stocks' ? 'Stocks' : universe === 'etfs' ? 'ETFs' : 'Baskets'
    const sigLabel = signalFilter.size === SIGNAL_TYPES.length ? 'All Signals' : [...signalFilter].join(', ')
    const statusLabel = statusFilter === 'all' ? 'All' : statusFilter === 'open' ? 'Open Only' : 'Closed Only'
    const title = `Signal Log — ${uLabel} ${period} | ${sigLabel} | ${statusLabel} | ${sorted.length} signals`

    const isStocks = universe === 'stocks'
    const notBaskets = universe !== 'baskets'
    const headers = ['Ticker']
    if (notBaskets) headers.push('Name')
    if (isStocks) headers.push('Sector', 'Industry')
    headers.push('Signal', 'Entry Date', 'Entry Price', 'Exit Date', 'Exit Price', 'MAE', 'MFE', '%Chg', 'Status')

    const rows = [title, '', headers.join(',')]
    for (const r of sorted) {
      const vals: string[] = [r.ticker]
      if (notBaskets) vals.push(`"${(r.name ?? '').replace(/"/g, '""')}"`)
      if (isStocks) vals.push(`"${(r.sector ?? '').replace(/"/g, '""')}"`, `"${(r.industry ?? '').replace(/"/g, '""')}"`)
      vals.push(
        r.signal,
        r.entry_date,
        r.entry_price != null ? r.entry_price.toFixed(2) : '',
        r.exit_date ?? 'Open',
        r.exit_price != null ? r.exit_price.toFixed(2) : '',
        r.mae != null ? (r.mae * 100).toFixed(2) + '%' : '',
        r.mfe != null ? (r.mfe * 100).toFixed(2) + '%' : '',
        r.pct_chg != null ? (r.pct_chg * 100).toFixed(2) + '%' : '',
        r.status === 'open' ? 'Open' : 'Closed'
      )
      rows.push(vals.join(','))
    }
    const blob = new Blob([rows.join('\n')], { type: 'text/csv' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url; a.download = `signals_${universe}_${period}_${statusFilter}.csv`; a.click()
    URL.revokeObjectURL(url)
  }, [exportTrigger])

  const arrow = (col: SortCol) => sortCol === col ? (sortDir === 1 ? ' \u25B2' : ' \u25BC') : ''

  const fmtPct = (v: number | null) => v != null ? `${v >= 0 ? '+' : ''}${(v * 100).toFixed(2)}%` : ''
  const fmtPrice = (v: number | null) => v != null ? v.toFixed(2) : ''
  const pctClass = (v: number | null) => v == null ? '' : v >= 0 ? 'sig-bull' : 'sig-bear'
  const sigClass = (s: string) => BULL_SIGNALS.has(s) ? 'sig-bull' : 'sig-bear'

  return (
    <div className="signals-panel" style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Controls row */}
      <div className="analysis-date-controls">
        <span className="analysis-date-label">Universe:</span>
        <div className="basket-returns-presets">
          {(['stocks', 'etfs', 'baskets'] as Universe[]).map(u => (
            <button key={u} className={`basket-returns-preset-btn ${universe === u ? 'active' : ''}`} onClick={() => setUniverse(u)}>
              {u === 'stocks' ? 'Stocks' : u === 'etfs' ? 'ETFs' : 'Baskets'}
            </button>
          ))}
        </div>
        <span className="analysis-date-label" style={{ marginLeft: 12 }}>Period:</span>
        <div className="basket-returns-presets">
          {PERIODS.map(p => (
            <button key={p} className={`basket-returns-preset-btn ${period === p ? 'active' : ''}`} onClick={() => setPeriod(p)}>{p}</button>
          ))}
        </div>
        <span className="analysis-date-label" style={{ marginLeft: 12 }}>Status:</span>
        <div className="basket-returns-presets">
          {(['all', 'open', 'closed'] as const).map(s => (
            <button key={s} className={`basket-returns-preset-btn ${statusFilter === s ? 'active' : ''}`} onClick={() => setStatusFilter(s)}>
              {s.charAt(0).toUpperCase() + s.slice(1)}
            </button>
          ))}
        </div>
        {loading && <span className="analysis-loading-hint" style={{ marginLeft: 'auto' }}>Loading...</span>}
        {!loading && <span style={{ marginLeft: 'auto', fontSize: 10, color: 'var(--text-muted)' }}>{sorted.length} signals</span>}
      </div>

      {/* Signal type filter row */}
      <div className="analysis-date-controls">
        <span className="analysis-date-label">Signals:</span>
        <div className="basket-returns-presets">
          {SIGNAL_TYPES.map(sig => (
            <button key={sig} className={`basket-returns-preset-btn ${signalFilter.has(sig) ? 'active' : ''}`} onClick={() => toggleSignal(sig)}>
              {sig.replace('_', ' ')}
            </button>
          ))}
        </div>
        <button className="basket-returns-preset-btn" style={{ marginLeft: 4 }}
          onClick={() => setSignalFilter(prev => prev.size === SIGNAL_TYPES.length ? new Set([SIGNAL_TYPES[0]]) : new Set(SIGNAL_TYPES))}>
          {signalFilter.size === SIGNAL_TYPES.length ? 'Clear' : 'All'}
        </button>
      </div>

      {/* Table */}
      <div style={{ flex: 1, overflow: 'auto' }}>
        <table className="signals-log-table">
          <thead>
            <tr>
              <th className="sl-th" onClick={() => handleSort('ticker')}>Ticker{arrow('ticker')}</th>
              {universe !== 'baskets' && <th className="sl-th" onClick={() => handleSort('name')}>Name{arrow('name')}</th>}
              {universe === 'stocks' && <th className="sl-th" onClick={() => handleSort('sector')}>Sector{arrow('sector')}</th>}
              {universe === 'stocks' && <th className="sl-th" onClick={() => handleSort('industry')}>Industry{arrow('industry')}</th>}
              <th className="sl-th" onClick={() => handleSort('signal')}>Signal{arrow('signal')}</th>
              <th className="sl-th" onClick={() => handleSort('entry_date')}>Entry Date{arrow('entry_date')}</th>
              <th className="sl-th sl-num" onClick={() => handleSort('entry_price')}>Entry Price{arrow('entry_price')}</th>
              <th className="sl-th" onClick={() => handleSort('exit_date')}>Exit Date{arrow('exit_date')}</th>
              <th className="sl-th sl-num" onClick={() => handleSort('exit_price')}>Exit Price{arrow('exit_price')}</th>
              <th className="sl-th sl-num" onClick={() => handleSort('mae')}>MAE{arrow('mae')}</th>
              <th className="sl-th sl-num" onClick={() => handleSort('mfe')}>MFE{arrow('mfe')}</th>
              <th className="sl-th sl-num" onClick={() => handleSort('pct_chg')}>%Chg{arrow('pct_chg')}</th>
              <th className="sl-th" onClick={() => handleSort('status')}>Status{arrow('status')}</th>
            </tr>
          </thead>
          <tbody>
            {sorted.map((r, i) => (
              <tr key={`${r.ticker}-${r.signal}-${r.entry_date}-${i}`} className="sl-row">
                <td className="sl-td sl-ticker">{r.ticker}</td>
                {universe !== 'baskets' && <td className="sl-td sl-name" title={r.name ?? ''}>{r.name ?? ''}</td>}
                {universe === 'stocks' && <td className="sl-td sl-sector">{r.sector ?? ''}</td>}
                {universe === 'stocks' && <td className="sl-td sl-industry">{r.industry ?? ''}</td>}
                <td className={`sl-td ${sigClass(r.signal)}`}>{r.signal.replace('_', ' ')}</td>
                <td className="sl-td">{r.entry_date}</td>
                <td className="sl-td sl-num">{fmtPrice(r.entry_price)}</td>
                <td className="sl-td">{r.exit_date ?? 'Open'}</td>
                <td className="sl-td sl-num">{fmtPrice(r.exit_price)}</td>
                <td className={`sl-td sl-num ${pctClass(r.mae != null ? -Math.abs(r.mae) : null)}`}>{fmtPct(r.mae)}</td>
                <td className={`sl-td sl-num ${pctClass(r.mfe)}`}>{fmtPct(r.mfe)}</td>
                <td className={`sl-td sl-num ${pctClass(r.pct_chg)}`}>{fmtPct(r.pct_chg)}</td>
                <td className={`sl-td ${r.status === 'open' ? 'sl-open' : 'sl-closed'}`}>{r.status === 'open' ? 'Open' : 'Closed'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
