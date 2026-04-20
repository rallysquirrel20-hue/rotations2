import React, { useState, useEffect, useMemo, useRef, useCallback } from 'react'
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

interface RotCtx {
  active: boolean
  pattern: 'HH' | 'LH' | 'HL' | 'LL' | null
  Time_0: number | null;  Range_0: number | null;  Change_0: number | null
  Time_0_Pct: number | null;  Range_0_Pct: number | null;  Change_0_Pct: number | null
  Time_1: number | null;  Range_1: number | null;  Change_1: number | null
  Time_2: number | null;  Range_2: number | null;  Change_2: number | null
  Time_3: number | null;  Range_3: number | null;  Change_3: number | null
}

interface CurrentContext {
  date: string | null
  close: number | null
  upper_target: number | null
  lower_target: number | null
  position: 'above_upper' | 'between' | 'below_lower' | null
  active_rotation: 'up' | 'down' | null
  active_regime: 'breakout' | 'breakdown' | null
  indicators: {
    rv_ann: number | null;       rv_pct: number | null
    h_react: number | null;      h_pct: number | null
    l_react: number | null;      l_pct: number | null
    p_react: number | null;      p_pct: number | null
    breadth: number | null;      breakout_pct: number | null;  corr_pct: number | null
  }
  rotations: Record<'UpRot' | 'DownRot' | 'Breakout' | 'Breakdown', RotCtx>
}

interface ForwardPath {
  date: string | null
  final_return: number
  series: number[]
}

interface DistResponse {
  ticker: string
  lookback: number
  horizon: number | string
  filtered: DistStats
  baseline: DistStats
  forward_paths: ForwardPath[]
  current_context?: CurrentContext
}

interface BasketsData { Themes: string[]; Sectors: string[]; Industries: string[] }

interface DistributionsPanelProps {
  apiBase: string
  activeTicker?: string | null
  allTickers?: string[]
  activeBasket?: string | null
  allBaskets?: BasketsData
}

type TriState = 'any' | 'a' | 'b'
type Op = '>' | '<' | '>=' | '<=' | '↑' | '↓'
const OPS: Op[] = ['>', '<', '>=', '<=', '↑', '↓']

interface ThresholdState { active: boolean; op: Op; val: number }

const BLUE = 'rgb(50, 50, 255)'
const BLUE_FILL = 'rgba(50, 50, 255, 0.35)'
const PINK = 'rgb(255, 50, 150)'
const PINK_FILL = 'rgba(255, 50, 150, 0.25)'
const SOLAR_BG = '#fdf6e3'
const SOLAR_BORDER = '#eee8d5'
const SOLAR_TEXT = '#586e75'
const SOLAR_MUTED = '#93a1a1'

// Tercile colors for the forward-path chart
const GREEN       = 'rgb(40, 160, 60)'
const GREEN_FILL  = 'rgba(40, 160, 60, 0.12)'
const GRAY_T      = 'rgb(140, 140, 140)'
const GRAY_FILL   = 'rgba(140, 140, 140, 0.10)'
const RED         = 'rgb(220, 50, 80)'
const RED_FILL    = 'rgba(220, 50, 80, 0.12)'

// Uniform-width control wrappers — `.control-btn` and `.backtest-input` both declare
// fixed pixel widths in index.css. Wrapping them in a flex:1 div with the inner
// control at width:100% guarantees equal column widths regardless of the class defaults.
const ROT_CELL: React.CSSProperties = { flex: 1, minWidth: 0, display: 'flex' }
const ROT_INNER: React.CSSProperties = { width: '100%', height: 28, fontSize: 11, padding: '0 6px', boxSizing: 'border-box' }

// Convert internal decimal state (e.g. 0.80) to display scale (80) when the field
// is a percentage. Backend contract stays in decimals — only the UI shows percent.
const toDisp   = (v: number, percent?: boolean) => percent ? v * 100 : v
const fromDisp = (v: number, percent?: boolean) => percent ? v / 100 : v

// --- Condition definitions ---
interface TriDef  { kind: 'tri';  key: string; label: string; a: { value: string; label: string }; b: { value: string; label: string }; basketOnly?: boolean }
interface ThDef   { kind: 'th';   key: string; label: string; opParam: string; valParam: string; defaultVal: number; min: number; max: number; step: number; basketOnly?: boolean; percent?: boolean }
type CondDef = TriDef | ThDef

const COND_DEFS: CondDef[] = [
  { kind: 'tri', key: 'breakout_state', label: 'LT Regime',      a: { value: 'breakout', label: 'Breakout' }, b: { value: 'breakdown', label: 'Breakdown' } },
  { kind: 'tri', key: 'rotation_state', label: 'Rotation',        a: { value: 'up', label: 'Up' },            b: { value: 'down', label: 'Down' } },
  { kind: 'th',  key: 'h',     label: 'H React',          opParam: 'h_op',     valParam: 'h_val',     defaultVal: 0, min: -1, max: 1, step: 0.05 },
  { kind: 'th',  key: 'h_pct', label: 'H React %ile',     opParam: 'h_pct_op', valParam: 'h_pct_val', defaultVal: 0.80, min: 0, max: 1, step: 0.05, percent: true },
  { kind: 'tri', key: 'h_trend',       label: 'H React (vs lookback)',   a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' } },
  { kind: 'th',  key: 'p',     label: 'Price Chg',        opParam: 'p_op',     valParam: 'p_val',     defaultVal: 0, min: -1, max: 1, step: 0.05 },
  { kind: 'th',  key: 'p_pct', label: 'Price Chg %ile',   opParam: 'p_pct_op', valParam: 'p_pct_val', defaultVal: 0.50, min: 0, max: 1, step: 0.05, percent: true },
  { kind: 'tri', key: 'p_trend',       label: 'Price Chg (vs lookback)', a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' } },
  { kind: 'th',  key: 'l',     label: 'L React',          opParam: 'l_op',     valParam: 'l_val',     defaultVal: 0, min: -1, max: 1, step: 0.05 },
  { kind: 'th',  key: 'l_pct', label: 'L React %ile',     opParam: 'l_pct_op', valParam: 'l_pct_val', defaultVal: 0.80, min: 0, max: 1, step: 0.05, percent: true },
  { kind: 'tri', key: 'l_trend',       label: 'L React (vs lookback)',   a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' } },
  { kind: 'th',  key: 'rv',     label: 'RV (ann %)',      opParam: 'rv_op',     valParam: 'rv_val',     defaultVal: 25,   min: 0, max: 150, step: 1 },
  { kind: 'th',  key: 'rv_pct', label: 'RV %ile',         opParam: 'rv_pct_op', valParam: 'rv_pct_val', defaultVal: 0.80, min: 0, max: 1,   step: 0.05, percent: true },
  { kind: 'tri', key: 'rv_trend',      label: 'RV (vs lookback)',        a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' } },
  { kind: 'tri', key: 'loc_upper',     label: 'Vs Upper Target', a: { value: 'above',      label: 'Above'  }, b: { value: 'below',      label: 'Below'   } },
  { kind: 'tri', key: 'loc_lower',     label: 'Vs Lower Target', a: { value: 'above',      label: 'Above'  }, b: { value: 'below',      label: 'Below'   } },
  { kind: 'th',  key: 'breadth',  label: 'Breadth %',       opParam: 'breadth_op',  valParam: 'breadth_val',  defaultVal: 50, min: 0, max: 100, step: 5, basketOnly: true },
  { kind: 'tri', key: 'breadth_trend', label: 'Breadth (vs lookback)',   a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' }, basketOnly: true },
  { kind: 'th',  key: 'breakout', label: 'Breakout %',      opParam: 'breakout_op', valParam: 'breakout_val', defaultVal: 50, min: 0, max: 100, step: 5, basketOnly: true },
  { kind: 'tri', key: 'breakout_trend', label: 'Breakout % (vs lookback)', a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' }, basketOnly: true },
  { kind: 'th',  key: 'corr',     label: 'Correlation %',   opParam: 'corr_op',     valParam: 'corr_val',     defaultVal: 50, min: 0, max: 100, step: 5, basketOnly: true },
  { kind: 'tri', key: 'corr_trend',    label: 'Correlation (vs lookback)', a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' }, basketOnly: true },
]

const TRI_KEYS = COND_DEFS.filter(d => d.kind === 'tri').map(d => d.key)
const TH_DEFS = COND_DEFS.filter((d): d is ThDef => d.kind === 'th')

// Condition groups for the collapsible UI
type CondGroup = { key: string; label: string; keys: string[]; columns?: string[][]; basketOnly?: boolean }
const COND_GROUPS: CondGroup[] = [
  {
    key: 'regime', label: 'Regime',
    keys: ['breakout_state', 'rotation_state'],
    columns: [['breakout_state'], ['rotation_state']],
  },
  {
    key: 'reactions', label: 'Reactions',
    keys: ['l', 'l_pct', 'l_trend', 'p', 'p_pct', 'p_trend', 'h', 'h_pct', 'h_trend'],
    columns: [
      ['l', 'l_pct', 'l_trend'],
      ['p', 'p_pct', 'p_trend'],
      ['h', 'h_pct', 'h_trend'],
    ],
  },
  {
    key: 'volatility', label: 'Volatility',
    keys: ['rv', 'rv_pct', 'rv_trend'],
    columns: [['rv', 'rv_pct', 'rv_trend']],
  },
  {
    key: 'location', label: 'Location',
    keys: ['loc_upper', 'loc_lower'],
    columns: [['loc_upper'], ['loc_lower']],
  },
  {
    key: 'basket', label: 'Basket Internals', basketOnly: true,
    keys: ['breadth', 'breadth_trend', 'breakout', 'breakout_trend', 'corr', 'corr_trend'],
    columns: [
      ['breadth', 'breadth_trend'],
      ['breakout', 'breakout_trend'],
      ['corr', 'corr_trend'],
    ],
  },
]
const COND_COLUMN_WIDTH = 240  // px — fixed to keep column widths consistent across groups

// --- Rotation context filter types (Phase 2) ---
const METRICS = ['Time', 'Range', 'Change'] as const
type Metric = typeof METRICS[number]
const ROT_TYPES = ['UpRot', 'DownRot', 'Breakout', 'Breakdown'] as const
type RotType = typeof ROT_TYPES[number]
const ROT_TYPE_LABELS: Record<RotType, string> = {
  UpRot: 'Up Rotation', DownRot: 'Down Rotation',
  Breakout: 'Breakout', Breakdown: 'Breakdown',
}
// Defaults per metric for the threshold / percentile value inputs.
// `percent: true` means the internal decimal value (e.g. 0.05) is shown as 5.
const ROT_DEFAULTS: Record<Metric, {
  th:  { val: number; min: number; max: number; step: number; percent?: boolean };
  pct: { val: number; step: number; percent?: boolean };
}> = {
  Time:   { th: { val: 10,   min: 0,  max: 1000, step: 1   },                 pct: { val: 0.80, step: 0.05, percent: true } },
  Range:  { th: { val: 0.05, min: 0,  max: 2,   step: 0.01, percent: true },  pct: { val: 0.80, step: 0.05, percent: true } },
  Change: { th: { val: 0,    min: -1, max: 1,   step: 0.01, percent: true },  pct: { val: 0.50, step: 0.05, percent: true } },
}

// Rotation back-indices: 0 = current (or most recently completed), 1/2/3 = back-refs.
const ROT_INDICES = [0, 1, 2, 3] as const
type RotIdx = typeof ROT_INDICES[number]
const ROT_INDEX_LABELS: Record<RotIdx, string> = {
  0: 'CURRENT (0)',
  1: '1-BACK',
  2: '2-BACK',
  3: '3-BACK',
}
// Pattern comparison keys — the "vs prior" column under each rotation index
const PATTERN_KEYS = ['0_1', '1_2', '2_3'] as const
type PatternKey = typeof PATTERN_KEYS[number]
// Map rotation index → the pattern key anchored at that index (that index vs next older).
// Index 3 has no pattern comparison (we'd need a 4-back reference).
const PATTERN_KEY_FOR_IDX: Partial<Record<RotIdx, PatternKey>> = { 0: '0_1', 1: '1_2', 2: '2_3' }

interface RotFilterState {
  threshold:  Record<RotIdx, Record<Metric, ThresholdState>>  // per rotation index × per metric
  percentile: Record<RotIdx, Record<Metric, ThresholdState>>  // per rotation index × per metric
  pattern:    Record<PatternKey, TriState>                    // HH/LH (or HL/LL) between consecutive rotations
}
type AllRotFilters = Record<RotType, RotFilterState>

// Per-type labels for the Pattern rows (HH/LH for bullish, HL/LL for bearish).
const ROT_PATTERN_LABELS: Record<RotType, { a: string; b: string; title: string }> = {
  UpRot:     { a: 'HH', b: 'LH', title: 'Peak high comparison between consecutive up rotations' },
  DownRot:   { a: 'HL', b: 'LL', title: 'Trough low comparison between consecutive down rotations' },
  Breakout:  { a: 'HH', b: 'LH', title: 'Peak high comparison between consecutive breakouts' },
  Breakdown: { a: 'HL', b: 'LL', title: 'Trough low comparison between consecutive breakdowns' },
}

const initOneRotFilters = (): RotFilterState => ({
  threshold: Object.fromEntries(ROT_INDICES.map(i => [
    i,
    Object.fromEntries(METRICS.map(m => [m, { active: false, op: '>' as Op, val: ROT_DEFAULTS[m].th.val }])) as Record<Metric, ThresholdState>,
  ])) as Record<RotIdx, Record<Metric, ThresholdState>>,
  percentile: Object.fromEntries(ROT_INDICES.map(i => [
    i,
    Object.fromEntries(METRICS.map(m => [m, { active: false, op: '>' as Op, val: ROT_DEFAULTS[m].pct.val }])) as Record<Metric, ThresholdState>,
  ])) as Record<RotIdx, Record<Metric, ThresholdState>>,
  pattern:    Object.fromEntries(PATTERN_KEYS.map(k => [k, 'any' as TriState])) as Record<PatternKey, TriState>,
})

const initRotFilters = (): AllRotFilters =>
  Object.fromEntries(ROT_TYPES.map(t => [t, initOneRotFilters()])) as AllRotFilters

const buildRotParam = (state: AllRotFilters): string | undefined => {
  const out: Record<string, unknown> = {}
  for (const t of ROT_TYPES) {
    const s = state[t]
    // Indexed threshold — keyed by rotation index
    const threshold: Record<string, Record<string, { op: Op; val: number }>> = {}
    for (const i of ROT_INDICES) {
      const inner: Record<string, { op: Op; val: number }> = {}
      for (const m of METRICS) {
        const th = s.threshold[i][m]
        if (th.active) inner[m] = { op: th.op, val: th.val }
      }
      if (Object.keys(inner).length) threshold[String(i)] = inner
    }
    const percentile: Record<string, Record<string, { op: Op; val: number }>> = {}
    for (const i of ROT_INDICES) {
      const inner: Record<string, { op: Op; val: number }> = {}
      for (const m of METRICS) {
        const pc = s.percentile[i][m]
        if (pc.active) inner[m] = { op: pc.op, val: pc.val }
      }
      if (Object.keys(inner).length) percentile[String(i)] = inner
    }
    const pattern: Record<string, string> = {}
    for (const k of PATTERN_KEYS) {
      const v = s.pattern[k]
      if (v === 'a') pattern[k] = 'higher'
      else if (v === 'b') pattern[k] = 'lower'
    }
    const hasTh  = Object.keys(threshold).length > 0
    const hasPct = Object.keys(percentile).length > 0
    const hasP   = Object.keys(pattern).length > 0
    if (hasTh || hasPct || hasP) {
      out[t] = {
        ...(hasTh  ? { threshold }  : {}),
        ...(hasPct ? { percentile } : {}),
        ...(hasP   ? { pattern }    : {}),
      }
    }
  }
  return Object.keys(out).length ? JSON.stringify(out) : undefined
}

const pct = (v: number | null | undefined, digits = 2) =>
  v == null ? '--' : (v * 100).toFixed(digits) + '%'

const initTriConds = (): Record<string, TriState> =>
  Object.fromEntries(TRI_KEYS.map(k => [k, 'any' as TriState]))

const initThresholds = (): Record<string, ThresholdState> =>
  Object.fromEntries(TH_DEFS.map(d => [d.key, { active: false, op: '>' as Op, val: d.defaultVal }]))

export const DistributionsPanel: React.FC<DistributionsPanelProps> = ({
  apiBase, activeTicker, allTickers = [], activeBasket, allBaskets,
}) => {
  const [mode, setMode] = useState<'ticker' | 'basket'>(activeBasket ? 'basket' : 'ticker')
  const [ticker, setTicker] = useState<string>(activeTicker || '')
  const [tickerQuery, setTickerQuery] = useState<string>(activeTicker || '')
  const [showTickerDropdown, setShowTickerDropdown] = useState(false)
  const [basket, setBasket] = useState<string>(activeBasket || '')
  const [basketQuery, setBasketQuery] = useState<string>(activeBasket || '')
  const [showBasketDropdown, setShowBasketDropdown] = useState(false)
  const [lookback, setLookback] = useState(21)
  const [horizon, setHorizon] = useState<string>('1')
  const [triConds, setTriConds] = useState<Record<string, TriState>>(initTriConds)
  const [thresholds, setThresholds] = useState<Record<string, ThresholdState>>(initThresholds)
  const [rotFilters, setRotFilters] = useState<AllRotFilters>(initRotFilters)
  const [openRotType, setOpenRotType] = useState<Record<RotType, boolean>>({
    UpRot: false, DownRot: false, Breakout: false, Breakdown: false,
  })
  const [openCondGroup, setOpenCondGroup] = useState<Record<string, boolean>>({
    regime: false, reactions: false, volatility: false, location: false, basket: false,
  })
  const [data, setData] = useState<DistResponse | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    if (activeBasket) {
      setMode('basket'); setBasket(activeBasket); setBasketQuery(activeBasket)
    } else if (activeTicker && activeTicker !== ticker) {
      setMode('ticker'); setTicker(activeTicker); setTickerQuery(activeTicker)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTicker, activeBasket])

  const allBasketNames = useMemo(() => {
    if (!allBaskets) return []
    return [...allBaskets.Themes, ...allBaskets.Sectors, ...allBaskets.Industries]
  }, [allBaskets])

  const filteredTickerOptions = useMemo(() => {
    const q = tickerQuery.trim().toUpperCase()
    if (!q) return allTickers.slice(0, 30)
    return allTickers.filter(t => t.toUpperCase().startsWith(q)).slice(0, 30)
  }, [tickerQuery, allTickers])

  const filteredBasketOptions = useMemo(() => {
    const q = basketQuery.trim().toLowerCase()
    if (!q) return allBasketNames.slice(0, 30)
    return allBasketNames.filter(b => b.toLowerCase().includes(q)).slice(0, 30)
  }, [basketQuery, allBasketNames])

  const isBasket = mode === 'basket'
  const target = isBasket ? basket : ticker

  // Fetch
  useEffect(() => {
    if (!target) { setData(null); return }
    const params: Record<string, string | number> = { lookback }
    if (isBasket) params.basket = target
    else params.ticker = target
    for (const def of COND_DEFS) {
      if (def.basketOnly && !isBasket) continue
      if (def.kind === 'tri') {
        const state = triConds[def.key]
        if (state === 'a') params[def.key] = def.a.value
        else if (state === 'b') params[def.key] = def.b.value
      } else {
        const th = thresholds[def.key]
        if (th?.active) {
          params[def.opParam] = th.op
          params[def.valParam] = th.val
        }
      }
    }
    const rotParam = buildRotParam(rotFilters)
    if (rotParam) params.rot = rotParam
    params.horizon = horizon
    setLoading(true); setError(null)
    axios.get(`${apiBase}/distribution/next-bar`, { params })
      .then(res => { setData(res.data); setLoading(false) })
      .catch(err => {
        setData(null)
        setError(err?.response?.data?.detail || err.message || 'Request failed')
        setLoading(false)
      })
  }, [target, triConds, thresholds, rotFilters, lookback, horizon, apiBase, isBasket])

  const setTri = useCallback((key: string, next: TriState) =>
    setTriConds(prev => ({ ...prev, [key]: next })), [])

  const setTh = useCallback((key: string, patch: Partial<ThresholdState>) =>
    setThresholds(prev => ({ ...prev, [key]: { ...prev[key], ...patch } })), [])

  const cycleOp = useCallback((key: string) =>
    setThresholds(prev => {
      const cur = prev[key]
      const idx = OPS.indexOf(cur.op)
      return { ...prev, [key]: { ...cur, active: true, op: OPS[(idx + 1) % OPS.length] } }
    }), [])

  const resetAll = () => {
    setTriConds(initTriConds()); setThresholds(initThresholds()); setRotFilters(initRotFilters())
  }

  // Rotation filter setters — per rotation index for threshold, flat for percentile
  const setRotTh = useCallback((t: RotType, idx: RotIdx, m: Metric, patch: Partial<ThresholdState>) =>
    setRotFilters(prev => ({
      ...prev,
      [t]: {
        ...prev[t],
        threshold: {
          ...prev[t].threshold,
          [idx]: { ...prev[t].threshold[idx], [m]: { ...prev[t].threshold[idx][m], ...patch } },
        },
      },
    })), [])
  const cycleRotThOp = useCallback((t: RotType, idx: RotIdx, m: Metric) =>
    setRotFilters(prev => {
      const cur = prev[t].threshold[idx][m]
      const i = OPS.indexOf(cur.op)
      return {
        ...prev,
        [t]: {
          ...prev[t],
          threshold: {
            ...prev[t].threshold,
            [idx]: { ...prev[t].threshold[idx], [m]: { ...cur, active: true, op: OPS[(i + 1) % OPS.length] } },
          },
        },
      }
    }), [])
  const setRotPct = useCallback((t: RotType, idx: RotIdx, m: Metric, patch: Partial<ThresholdState>) =>
    setRotFilters(prev => ({
      ...prev,
      [t]: {
        ...prev[t],
        percentile: {
          ...prev[t].percentile,
          [idx]: { ...prev[t].percentile[idx], [m]: { ...prev[t].percentile[idx][m], ...patch } },
        },
      },
    })), [])
  const cycleRotPctOp = useCallback((t: RotType, idx: RotIdx, m: Metric) =>
    setRotFilters(prev => {
      const cur = prev[t].percentile[idx][m]
      const i = OPS.indexOf(cur.op)
      return {
        ...prev,
        [t]: {
          ...prev[t],
          percentile: {
            ...prev[t].percentile,
            [idx]: { ...prev[t].percentile[idx], [m]: { ...cur, active: true, op: OPS[(i + 1) % OPS.length] } },
          },
        },
      }
    }), [])
  const setRotPattern = useCallback((t: RotType, key: PatternKey, next: TriState) =>
    setRotFilters(prev => ({
      ...prev,
      [t]: { ...prev[t], pattern: { ...prev[t].pattern, [key]: next } },
    })), [])

  // ── Canvas (KDE) ──
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const [dims, setDims] = useState({ w: 800, h: 360 })
  const wrapRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const onResize = () => { if (wrapRef.current) setDims({ w: wrapRef.current.clientWidth, h: 360 }) }
    onResize(); window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [])

  // ── Forward paths chart (Phase 4) ──
  const fwdCanvasRef = useRef<HTMLCanvasElement>(null)
  const fwdWrapRef   = useRef<HTMLDivElement>(null)
  const [fwdDims, setFwdDims] = useState({ w: 600, h: 560 })
  useEffect(() => {
    const onResize = () => { if (fwdWrapRef.current) setFwdDims({ w: fwdWrapRef.current.clientWidth, h: 560 }) }
    onResize(); window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [])
  const [hoveredMatchIdx, setHoveredMatchIdx] = useState<number | null>(null)
  const [fwdSortBy, setFwdSortBy]   = useState<'date' | 'return'>('return')
  const [fwdSortDir, setFwdSortDir] = useState<'asc' | 'desc'>('desc')

  // Compute tercile groups + mean paths from data.forward_paths
  const forwardTerciles = useMemo(() => {
    const paths = data?.forward_paths
    if (!paths || paths.length === 0) return null
    const sortedByReturn = [...paths]
      .map((p, originalIdx) => ({ p, originalIdx }))
      .sort((a, b) => a.p.final_return - b.p.final_return)
    const n = sortedByReturn.length
    const cutA = Math.floor(n / 3)
    const cutB = Math.floor(2 * n / 3)
    const tercileOfOriginal = new Map<number, 'bottom' | 'middle' | 'top'>()
    sortedByReturn.forEach((entry, i) => {
      const t: 'bottom' | 'middle' | 'top' = i < cutA ? 'bottom' : i < cutB ? 'middle' : 'top'
      tercileOfOriginal.set(entry.originalIdx, t)
    })
    const bottom = sortedByReturn.slice(0, cutA).map(x => x.p)
    const middle = sortedByReturn.slice(cutA, cutB).map(x => x.p)
    const top    = sortedByReturn.slice(cutB).map(x => x.p)

    const computeStats = (group: ForwardPath[]): { mean: number[]; upper: number[]; lower: number[] } => {
      if (group.length === 0 || group[0].series.length === 0) return { mean: [], upper: [], lower: [] }
      const len = group[0].series.length
      const mean = new Array(len).fill(0)
      const upper = new Array(len).fill(0)
      const lower = new Array(len).fill(0)
      for (let i = 0; i < len; i++) {
        let sum = 0; let count = 0
        for (const p of group) {
          const v = p.series[i]
          if (Number.isFinite(v)) { sum += v; count++ }
        }
        const m = count > 0 ? sum / count : 0
        let sq = 0
        for (const p of group) {
          const v = p.series[i]
          if (Number.isFinite(v)) sq += (v - m) * (v - m)
        }
        const std = count > 1 ? Math.sqrt(sq / count) : 0
        mean[i] = m; upper[i] = m + std; lower[i] = m - std
      }
      return { mean, upper, lower }
    }
    return {
      bottom: { paths: bottom, ...computeStats(bottom) },
      middle: { paths: middle, ...computeStats(middle) },
      top:    { paths: top,    ...computeStats(top)    },
      tercileOfOriginal,
      all: paths,
    }
  }, [data?.forward_paths])

  // Draw the forward-paths chart
  useEffect(() => {
    const canvas = fwdCanvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return
    const dpr = window.devicePixelRatio || 1
    canvas.width = fwdDims.w * dpr; canvas.height = fwdDims.h * dpr
    ctx.scale(dpr, dpr); ctx.clearRect(0, 0, fwdDims.w, fwdDims.h)
    ctx.fillStyle = SOLAR_BG; ctx.fillRect(0, 0, fwdDims.w, fwdDims.h)

    const pad = { top: 14, right: 60, bottom: 24, left: 10 }
    const plotW = fwdDims.w - pad.left - pad.right
    const plotH = fwdDims.h - pad.top - pad.bottom

    if (!forwardTerciles) {
      ctx.fillStyle = SOLAR_MUTED; ctx.font = '12px monospace'; ctx.textAlign = 'center'
      const msg = (typeof data?.horizon === 'string' && data?.horizon === 'rotation')
        ? 'Forward paths unavailable for Rotation horizon (variable length)'
        : 'No forward paths'
      ctx.fillText(msg, fwdDims.w / 2, fwdDims.h / 2)
      return
    }

    // Y range spans the ±1σ bands of all three terciles (+ any hovered individual path).
    let yMin = Infinity, yMax = -Infinity
    const allGroups = [forwardTerciles.bottom, forwardTerciles.middle, forwardTerciles.top]
    let nPoints = 0
    for (const g of allGroups) {
      for (const v of g.lower) if (v < yMin) yMin = v
      for (const v of g.upper) if (v > yMax) yMax = v
      if (g.mean.length > nPoints) nPoints = g.mean.length
    }
    if (hoveredMatchIdx !== null && data?.forward_paths) {
      const hp = data.forward_paths[hoveredMatchIdx]
      if (hp) for (const v of hp.series) { if (v < yMin) yMin = v; if (v > yMax) yMax = v }
    }
    if (!isFinite(yMin) || !isFinite(yMax) || nPoints === 0) return
    const yPad = (yMax - yMin) * 0.05 || 0.005
    yMin -= yPad; yMax += yPad

    const xScale = (i: number) => pad.left + (i / (nPoints - 1 || 1)) * plotW
    const yScale = (v: number) => pad.top + plotH - ((v - yMin) / (yMax - yMin)) * plotH

    // Gridlines
    ctx.strokeStyle = SOLAR_BORDER; ctx.lineWidth = 1
    for (let i = 0; i <= 5; i++) {
      const y = pad.top + (plotH * i) / 5
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + plotW, y); ctx.stroke()
      const v = yMax - ((yMax - yMin) * i) / 5
      ctx.fillStyle = SOLAR_MUTED; ctx.font = '9px monospace'; ctx.textAlign = 'right'
      ctx.fillText((v * 100).toFixed(1) + '%', fwdDims.w - 4, y + 3)
    }
    // Zero line
    if (yMin < 0 && yMax > 0) {
      ctx.strokeStyle = SOLAR_MUTED; ctx.setLineDash([4, 4])
      ctx.beginPath(); ctx.moveTo(pad.left, yScale(0)); ctx.lineTo(pad.left + plotW, yScale(0)); ctx.stroke()
      ctx.setLineDash([])
    }
    // X axis
    ctx.fillStyle = SOLAR_MUTED; ctx.font = '9px monospace'; ctx.textAlign = 'center'
    for (let li = 0; li <= 5; li++) {
      const idx = Math.round((li * (nPoints - 1)) / 5)
      ctx.fillText(String(idx), xScale(idx), fwdDims.h - pad.bottom + 14)
    }

    // ±1σ filled bands per tercile
    const drawBand = (upper: number[], lower: number[], fill: string) => {
      if (upper.length < 2) return
      ctx.fillStyle = fill
      ctx.beginPath()
      for (let i = 0; i < upper.length; i++) {
        const x = xScale(i), y = yScale(upper[i])
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y)
      }
      for (let i = lower.length - 1; i >= 0; i--) ctx.lineTo(xScale(i), yScale(lower[i]))
      ctx.closePath()
      ctx.fill()
    }
    drawBand(forwardTerciles.bottom.upper, forwardTerciles.bottom.lower, RED_FILL)
    drawBand(forwardTerciles.middle.upper, forwardTerciles.middle.lower, GRAY_FILL)
    drawBand(forwardTerciles.top.upper,    forwardTerciles.top.lower,    GREEN_FILL)

    // Hovered match path (drawn below means so means stay readable)
    if (hoveredMatchIdx !== null && data?.forward_paths) {
      const p = data.forward_paths[hoveredMatchIdx]
      if (p && p.series.length >= 2) {
        ctx.strokeStyle = SOLAR_TEXT; ctx.lineWidth = 1.5
        ctx.beginPath()
        for (let i = 0; i < p.series.length; i++) {
          const x = xScale(i), y = yScale(p.series[i])
          if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y)
        }
        ctx.stroke()
      }
    }

    // Tercile mean lines
    const drawMean = (mean: number[], color: string) => {
      if (mean.length < 2) return
      ctx.strokeStyle = color; ctx.lineWidth = 2.5
      ctx.beginPath()
      for (let i = 0; i < mean.length; i++) {
        const x = xScale(i), y = yScale(mean[i])
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y)
      }
      ctx.stroke()
    }
    drawMean(forwardTerciles.bottom.mean, RED)
    drawMean(forwardTerciles.middle.mean, GRAY_T)
    drawMean(forwardTerciles.top.mean,    GREEN)

    // Legend (top-left)
    const legend = [
      [GREEN,  `Winners mean (${forwardTerciles.top.paths.length})`],
      [GRAY_T, `Middle mean  (${forwardTerciles.middle.paths.length})`],
      [RED,    `Losers mean  (${forwardTerciles.bottom.paths.length})`],
    ] as const
    ctx.font = '10px monospace'; ctx.textAlign = 'left'
    legend.forEach(([c, label], i) => {
      const y = pad.top + 4 + i * 14
      ctx.fillStyle = c; ctx.fillRect(pad.left + 4, y, 12, 3)
      ctx.fillStyle = SOLAR_TEXT; ctx.fillText(label, pad.left + 20, y + 5)
    })
  }, [forwardTerciles, fwdDims, hoveredMatchIdx, data?.forward_paths, data?.horizon])

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    if (!ctx) return
    const dpr = window.devicePixelRatio || 1
    canvas.width = dims.w * dpr; canvas.height = dims.h * dpr; ctx.scale(dpr, dpr)
    ctx.clearRect(0, 0, dims.w, dims.h); ctx.fillStyle = SOLAR_BG; ctx.fillRect(0, 0, dims.w, dims.h)
    const pad = { top: 30, right: 30, bottom: 50, left: 20 }
    const plotW = dims.w - pad.left - pad.right, plotH = dims.h - pad.top - pad.bottom

    if (!data) {
      ctx.fillStyle = SOLAR_MUTED; ctx.font = '14px monospace'; ctx.textAlign = 'center'
      ctx.fillText(target ? (loading ? 'Loading\u2026' : (error || 'No data')) : 'Select a ticker or basket', dims.w / 2, dims.h / 2)
      return
    }
    const base = data.baseline.returns, filt = data.filtered.returns
    if (base.length === 0) {
      ctx.fillStyle = SOLAR_MUTED; ctx.font = '14px monospace'; ctx.textAlign = 'center'
      ctx.fillText('No baseline returns available', dims.w / 2, dims.h / 2); return
    }
    const sortedBase = [...base].sort((a, b) => a - b)
    const q = (p: number) => sortedBase[Math.max(0, Math.min(sortedBase.length - 1, Math.floor(p * (sortedBase.length - 1))))]
    const p1 = q(0.01), p99 = q(0.99), pad_x = (p99 - p1) * 0.1 || 0.01
    const xMin = p1 - pad_x, xMax = p99 + pad_x
    const nPoints = Math.min(200, plotW)
    const gaussianKDE = (vals: number[]) => {
      if (vals.length < 2) return { xs: [] as number[], ys: [] as number[] }
      const n = vals.length, mean = vals.reduce((a, b) => a + b, 0) / n
      const std = Math.sqrt(vals.reduce((s, v) => s + (v - mean) ** 2, 0) / n) || (xMax - xMin) * 0.02
      const h = 1.06 * std * Math.pow(n, -0.2), invH = 1 / h, coeff = 1 / (n * h * Math.sqrt(2 * Math.PI))
      const xs: number[] = [], ys: number[] = []
      for (let i = 0; i < nPoints; i++) {
        const x = xMin + (i / (nPoints - 1)) * (xMax - xMin); let d = 0
        for (const v of vals) { const z = (x - v) * invH; d += Math.exp(-0.5 * z * z) }
        xs.push(x); ys.push(d * coeff)
      }
      return { xs, ys }
    }
    const baseKDE = gaussianKDE(base), filtKDE = filt.length >= 2 ? gaussianKDE(filt) : { xs: [], ys: [] }
    let maxD = 0
    for (const y of baseKDE.ys) maxD = Math.max(maxD, y)
    for (const y of filtKDE.ys) maxD = Math.max(maxD, y)
    if (maxD === 0) return
    const xScale = (v: number) => pad.left + ((v - xMin) / (xMax - xMin)) * plotW
    const yScale = (d: number) => pad.top + plotH - (d / maxD) * plotH
    ctx.strokeStyle = SOLAR_BORDER; ctx.lineWidth = 1
    for (let i = 0; i <= 4; i++) { const y = pad.top + (plotH * i) / 4; ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + plotW, y); ctx.stroke() }
    if (xMin < 0 && xMax > 0) { ctx.strokeStyle = SOLAR_MUTED; ctx.setLineDash([3, 3]); ctx.beginPath(); ctx.moveTo(xScale(0), pad.top); ctx.lineTo(xScale(0), pad.top + plotH); ctx.stroke(); ctx.setLineDash([]) }
    const drawCurve = (xs: number[], ys: number[], fill: string, stroke: string) => {
      if (xs.length < 2) return; const bl = pad.top + plotH
      ctx.beginPath(); ctx.moveTo(xScale(xs[0]), bl); for (let i = 0; i < xs.length; i++) ctx.lineTo(xScale(xs[i]), yScale(ys[i])); ctx.lineTo(xScale(xs[xs.length - 1]), bl); ctx.closePath(); ctx.fillStyle = fill; ctx.fill()
      ctx.beginPath(); ctx.moveTo(xScale(xs[0]), yScale(ys[0])); for (let i = 1; i < xs.length; i++) ctx.lineTo(xScale(xs[i]), yScale(ys[i])); ctx.strokeStyle = stroke; ctx.lineWidth = 2; ctx.stroke()
    }
    drawCurve(baseKDE.xs, baseKDE.ys, PINK_FILL, PINK)
    if (filtKDE.xs.length > 0) drawCurve(filtKDE.xs, filtKDE.ys, BLUE_FILL, BLUE)
    const drawMedian = (v: number | null, color: string) => { if (v == null || v < xMin || v > xMax) return; const x = xScale(v); ctx.strokeStyle = color; ctx.lineWidth = 1; ctx.setLineDash([4, 3]); ctx.beginPath(); ctx.moveTo(x, pad.top); ctx.lineTo(x, pad.top + plotH); ctx.stroke(); ctx.setLineDash([]) }
    drawMedian(data.baseline.median, PINK); drawMedian(data.filtered.median, BLUE)
    ctx.fillStyle = SOLAR_TEXT; ctx.font = '10px monospace'; ctx.textAlign = 'center'
    for (let i = 0; i <= 8; i++) { const v = xMin + (i / 8) * (xMax - xMin); ctx.fillText((v * 100).toFixed(1) + '%', xScale(v), dims.h - pad.bottom + 15) }
    const legendW = 260, lx = dims.w - pad.right - legendW - 4, ly = pad.top + 4
    ctx.fillStyle = SOLAR_BG; ctx.fillRect(lx - 4, ly - 10, legendW, 44); ctx.strokeStyle = SOLAR_BORDER; ctx.lineWidth = 1; ctx.strokeRect(lx - 4, ly - 10, legendW, 44)
    const row = (i: number, sf: string, ss: string, label: string, n: number, median: number | null) => {
      const y = ly + i * 16; ctx.fillStyle = sf; ctx.fillRect(lx, y - 4, 14, 10); ctx.strokeStyle = ss; ctx.lineWidth = 1; ctx.strokeRect(lx, y - 4, 14, 10)
      ctx.fillStyle = SOLAR_TEXT; ctx.font = '10px monospace'; ctx.textAlign = 'left'; ctx.fillText(label, lx + 18, y + 4); ctx.fillText(`N=${n}`, lx + 100, y + 4); ctx.textAlign = 'right'; ctx.fillText(`med ${pct(median)}`, lx + legendW - 8, y + 4)
    }
    row(0, PINK_FILL, PINK, 'Baseline', data.baseline.n, data.baseline.median)
    row(1, BLUE_FILL, BLUE, 'Filtered', data.filtered.n, data.filtered.median)
  }, [data, dims, target, loading, error])

  const stats = data?.filtered, baseSt = data?.baseline

  return (
    <div className="distributions-panel" style={{ padding: 12, display: 'flex', flexDirection: 'column', gap: 12, height: '100%', overflow: 'auto' }}>

      {/* Controls row */}
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', alignItems: 'flex-end' }}>
        <div>
          <div style={{ fontSize: 10, color: SOLAR_MUTED, marginBottom: 3 }}>MODE</div>
          <div style={{ display: 'flex', gap: 2 }}>
            <button className={`control-btn ${mode === 'ticker' ? 'primary' : ''}`} style={{ fontSize: 11, padding: '3px 10px' }} onClick={() => setMode('ticker')}>Ticker</button>
            <button className={`control-btn ${mode === 'basket' ? 'primary' : ''}`} style={{ fontSize: 11, padding: '3px 10px' }} onClick={() => setMode('basket')}>Basket</button>
          </div>
        </div>

        {mode === 'ticker' ? (
          <div style={{ position: 'relative' }}>
            <div style={{ fontSize: 10, color: SOLAR_MUTED, marginBottom: 3 }}>TICKER</div>
            <input type="text" className="backtest-input" value={tickerQuery}
              onChange={e => { setTickerQuery(e.target.value.toUpperCase()); setShowTickerDropdown(true) }}
              onFocus={() => setShowTickerDropdown(true)}
              onBlur={() => setTimeout(() => setShowTickerDropdown(false), 120)}
              onKeyDown={e => { if (e.key === 'Enter') { const q = tickerQuery.trim().toUpperCase(); if (q) { setTicker(q); setShowTickerDropdown(false) } } }}
              placeholder="AAPL" style={{ width: 120, textTransform: 'uppercase' }} />
            {showTickerDropdown && filteredTickerOptions.length > 0 && (
              <div style={{ position: 'absolute', top: '100%', left: 0, zIndex: 50, background: SOLAR_BG, border: `1px solid ${SOLAR_BORDER}`, maxHeight: 220, overflowY: 'auto', minWidth: 120 }}>
                {filteredTickerOptions.map(t => (
                  <div key={t} onMouseDown={() => { setTicker(t); setTickerQuery(t); setShowTickerDropdown(false) }}
                    style={{ padding: '3px 8px', cursor: 'pointer', fontSize: 12, fontFamily: 'monospace', background: t === ticker ? SOLAR_BORDER : 'transparent' }}>{t}</div>
                ))}
              </div>
            )}
          </div>
        ) : (
          <div style={{ position: 'relative' }}>
            <div style={{ fontSize: 10, color: SOLAR_MUTED, marginBottom: 3 }}>BASKET</div>
            <input type="text" className="backtest-input" value={basketQuery}
              onChange={e => { setBasketQuery(e.target.value); setShowBasketDropdown(true) }}
              onFocus={() => setShowBasketDropdown(true)}
              onBlur={() => setTimeout(() => setShowBasketDropdown(false), 120)}
              onKeyDown={e => { if (e.key === 'Enter') { const q = basketQuery.trim(); if (q) { setBasket(q); setShowBasketDropdown(false) } } }}
              placeholder="Search baskets..." style={{ width: 240 }} />
            {showBasketDropdown && filteredBasketOptions.length > 0 && (
              <div style={{ position: 'absolute', top: '100%', left: 0, zIndex: 50, background: SOLAR_BG, border: `1px solid ${SOLAR_BORDER}`, maxHeight: 220, overflowY: 'auto', minWidth: 240 }}>
                {filteredBasketOptions.map(b => (
                  <div key={b} onMouseDown={() => { setBasket(b); setBasketQuery(b); setShowBasketDropdown(false) }}
                    style={{ padding: '3px 8px', cursor: 'pointer', fontSize: 12, fontFamily: 'monospace', background: b === basket ? SOLAR_BORDER : 'transparent' }}>{b}</div>
                ))}
              </div>
            )}
          </div>
        )}

        <div>
          <div style={{ fontSize: 10, color: SOLAR_MUTED, marginBottom: 3 }}>TREND LOOKBACK (bars)</div>
          <input type="number" className="backtest-input" value={lookback} min={1} max={252}
            onChange={e => setLookback(Math.max(1, Number(e.target.value) || 1))} style={{ width: 70 }} />
        </div>

        <div>
          <div style={{ fontSize: 10, color: SOLAR_MUTED, marginBottom: 3 }}>HORIZON</div>
          <div style={{ display: 'flex', gap: 2 }}>
            {(['1', '5', '21', '63', '252', 'rotation'] as const).map(h => (
              <button key={h}
                className={`control-btn ${horizon === h ? 'primary' : ''}`}
                style={{ fontSize: 11, padding: '3px 10px', width: 'auto', height: 28 }}
                onClick={() => setHorizon(h)}>
                {h === 'rotation' ? 'Rotation' : `${h}b`}
              </button>
            ))}
          </div>
        </div>

        <button className="control-btn" onClick={resetAll} style={{ marginLeft: 'auto' }}>Reset conditions</button>
      </div>

      {/* Filters — grouped collapsible */}
      <div style={{ border: `1px solid ${SOLAR_BORDER}`, background: SOLAR_BG }}>
        <div style={{ padding: '6px 8px', color: SOLAR_MUTED, fontSize: 10, borderBottom: `1px solid ${SOLAR_BORDER}` }}>
          FILTERS
        </div>
        {COND_GROUPS.map(group => {
          if (group.basketOnly && !isBasket) return null
          const groupDefs = group.keys
            .map(k => COND_DEFS.find(d => d.key === k))
            .filter((d): d is CondDef => !!d && (!d.basketOnly || isBasket))
          if (groupDefs.length === 0) return null
          const isOpen = openCondGroup[group.key]
          const activeCount = groupDefs.reduce((acc, d) => {
            if (d.kind === 'tri') return acc + (triConds[d.key] !== 'any' ? 1 : 0)
            return acc + (thresholds[d.key]?.active ? 1 : 0)
          }, 0)
          return (
            <div key={group.key} style={{ borderTop: `1px solid ${SOLAR_BORDER}` }}>
              <div
                onClick={() => setOpenCondGroup(prev => ({ ...prev, [group.key]: !prev[group.key] }))}
                style={{ padding: '4px 8px', cursor: 'pointer', fontSize: 12, fontFamily: 'monospace', display: 'flex', alignItems: 'center', gap: 6 }}
              >
                <span style={{ width: 10, color: SOLAR_MUTED }}>{isOpen ? '▼' : '▶'}</span>
                <span>{group.label}</span>
                {activeCount > 0 && (
                  <span style={{ fontSize: 10, color: BLUE, marginLeft: 4 }}>({activeCount} active)</span>
                )}
              </div>
              {isOpen && (() => {
                const renderDef = (def: CondDef) => {
                  if (def.kind === 'tri') {
                    const state = triConds[def.key]
                    const mkBtn = (next: TriState, label: string) => (
                      <div style={ROT_CELL}>
                        <button className={`control-btn ${state === next ? 'primary' : ''}`}
                          style={ROT_INNER}
                          onClick={() => setTri(def.key, next)}>{label}</button>
                      </div>
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
                  }
                  const th = thresholds[def.key]
                  return (
                    <div key={def.key} style={{ border: `1px solid ${SOLAR_BORDER}`, padding: 6, background: SOLAR_BG }}>
                      <div style={{ fontSize: 10, color: SOLAR_MUTED, marginBottom: 4 }}>{def.label.toUpperCase()}</div>
                      <div style={{ display: 'flex', gap: 4 }}>
                        <div style={ROT_CELL}>
                          <button className={`control-btn ${!th.active ? 'primary' : ''}`}
                            style={ROT_INNER}
                            onClick={() => setTh(def.key, { active: false })}>Any</button>
                        </div>
                        <div style={ROT_CELL}>
                          <button className={`control-btn ${th.active ? 'primary' : ''}`}
                            style={{ ...ROT_INNER, fontFamily: 'monospace' }}
                            onClick={() => {
                              if (!th.active) setTh(def.key, { active: true })
                              else cycleOp(def.key)
                            }}>{th.op}</button>
                        </div>
                        <div style={ROT_CELL}>
                          <input type="number" className="backtest-input"
                            value={toDisp(th.val, def.percent)}
                            min={toDisp(def.min, def.percent)}
                            max={toDisp(def.max, def.percent)}
                            step={toDisp(def.step, def.percent)}
                            onChange={e => {
                              const entered = parseFloat(e.target.value)
                              if (!isNaN(entered)) {
                                const internal = fromDisp(entered, def.percent)
                                setTh(def.key, { active: true, val: Math.min(def.max, Math.max(def.min, internal)) })
                              }
                            }}
                            style={{ ...ROT_INNER, textAlign: 'center' }} />
                        </div>
                      </div>
                    </div>
                  )
                }

                // If the group declares explicit columns, render each column as a stack.
                // All columns are fixed-width so widths match across different groups.
                if (group.columns && group.columns.length > 0) {
                  return (
                    <div style={{ padding: '6px 12px 10px 26px', display: 'grid',
                                  gridTemplateColumns: `repeat(${group.columns.length}, ${COND_COLUMN_WIDTH}px)`, gap: 8 }}>
                      {group.columns.map((col, ci) => {
                        const colDefs = col
                          .map(k => COND_DEFS.find(d => d.key === k))
                          .filter((d): d is CondDef => !!d && (!d.basketOnly || isBasket))
                        return (
                          <div key={ci} style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                            {colDefs.map(renderDef)}
                          </div>
                        )
                      })}
                    </div>
                  )
                }

                return (
                  <div style={{ padding: '6px 12px 10px 26px', display: 'grid',
                                gridTemplateColumns: `repeat(auto-fill, ${COND_COLUMN_WIDTH}px)`, gap: 8 }}>
                    {groupDefs.map(renderDef)}
                  </div>
                )
              })()}
            </div>
          )
        })}
      </div>

      {/* Rotation context filters (Phase 2) */}
      <div style={{ border: `1px solid ${SOLAR_BORDER}`, background: SOLAR_BG }}>
        <div style={{ padding: '6px 8px', color: SOLAR_MUTED, fontSize: 10, borderBottom: `1px solid ${SOLAR_BORDER}` }}>
          ROTATION CONTEXT FILTERS
        </div>
        {ROT_TYPES.map(type => {
          const isOpen = openRotType[type]
          const s = rotFilters[type]
          // Count active filters for this type
          let activeCount = 0
          for (const i of ROT_INDICES) for (const m of METRICS) {
            if (s.threshold[i][m].active) activeCount++
            if (s.percentile[i][m].active) activeCount++
          }
          for (const k of PATTERN_KEYS) if (s.pattern[k] !== 'any') activeCount++
          return (
            <div key={type} style={{ borderTop: `1px solid ${SOLAR_BORDER}` }}>
              <div
                onClick={() => setOpenRotType(prev => ({ ...prev, [type]: !prev[type] }))}
                style={{ padding: '4px 8px', cursor: 'pointer', fontSize: 12, fontFamily: 'monospace', display: 'flex', alignItems: 'center', gap: 6 }}
              >
                <span style={{ width: 10, color: SOLAR_MUTED }}>{isOpen ? '▼' : '▶'}</span>
                <span>{ROT_TYPE_LABELS[type]}</span>
                {activeCount > 0 && (
                  <span style={{ fontSize: 10, color: BLUE, marginLeft: 4 }}>({activeCount} active)</span>
                )}
              </div>
              {isOpen && (() => {
                const lbl = ROT_PATTERN_LABELS[type]

                // Render a single threshold-style row of controls (Any | op | value)
                // against a given ThresholdState + ROT_DEFAULT entry.
                const renderThresholdRow = (
                  th: ThresholdState,
                  dmin: number, dmax: number, dstep: number, dpercent: boolean | undefined,
                  onToggleAny: () => void, onCycleOp: () => void, onVal: (val: number) => void,
                ) => (
                  <div style={{ display: 'flex', gap: 4 }}>
                    <div style={ROT_CELL}>
                      <button className={`control-btn ${!th.active ? 'primary' : ''}`}
                        style={ROT_INNER}
                        onClick={onToggleAny}>Any</button>
                    </div>
                    <div style={ROT_CELL}>
                      <button className={`control-btn ${th.active ? 'primary' : ''}`}
                        style={{ ...ROT_INNER, fontFamily: 'monospace' }}
                        onClick={onCycleOp}>{th.op}</button>
                    </div>
                    <div style={ROT_CELL}>
                      <input type="number" className="backtest-input"
                        value={toDisp(th.val, dpercent)}
                        min={toDisp(dmin, dpercent)}
                        max={toDisp(dmax, dpercent)}
                        step={toDisp(dstep, dpercent)}
                        onChange={e => {
                          const entered = parseFloat(e.target.value)
                          if (!isNaN(entered)) {
                            const internal = fromDisp(entered, dpercent)
                            onVal(Math.min(dmax, Math.max(dmin, internal)))
                          }
                        }}
                        style={{ ...ROT_INNER, textAlign: 'center' }} />
                    </div>
                  </div>
                )

                return (
                  <div style={{ padding: '6px 12px 10px 26px', display: 'grid',
                                gridTemplateColumns: `110px repeat(4, ${COND_COLUMN_WIDTH}px)`,
                                gap: '4px 8px', alignItems: 'center' }}>
                    {/* Header row: rotation index columns */}
                    <div />
                    {ROT_INDICES.map(i => (
                      <div key={i} style={{ fontSize: 10, color: SOLAR_MUTED, textAlign: 'center' }}
                           title={i === 0 ? 'The current rotation (if in state) or most recently completed'
                                           : `${i} rotations ago`}>
                        {ROT_INDEX_LABELS[i]}
                      </div>
                    ))}

                    {/* For each metric: threshold row across all indices + percentile row across all indices */}
                    {METRICS.map(m => {
                      const d = ROT_DEFAULTS[m]
                      return (
                        <React.Fragment key={m}>
                          {/* Threshold row */}
                          <div style={{ fontSize: 11, color: SOLAR_TEXT, fontFamily: 'monospace' }}>{m}</div>
                          {ROT_INDICES.map(i => {
                            const th = s.threshold[i][m]
                            return (
                              <React.Fragment key={`${m}-th-${i}`}>
                                {renderThresholdRow(
                                  th, d.th.min, d.th.max, d.th.step, d.th.percent,
                                  () => setRotTh(type, i, m, { active: false }),
                                  () => th.active ? cycleRotThOp(type, i, m) : setRotTh(type, i, m, { active: true }),
                                  (v) => setRotTh(type, i, m, { active: true, val: v }),
                                )}
                              </React.Fragment>
                            )
                          })}
                          {/* Percentile row */}
                          <div style={{ fontSize: 11, color: SOLAR_MUTED, fontFamily: 'monospace' }}
                               title="Percentile rank of each rotation's value vs this ticker's history of completed rotations of the same type">
                            {m} %ile
                          </div>
                          {ROT_INDICES.map(i => {
                            const pc = s.percentile[i][m]
                            return (
                              <React.Fragment key={`${m}-pct-${i}`}>
                                {renderThresholdRow(
                                  pc, 0, 1, d.pct.step, d.pct.percent,
                                  () => setRotPct(type, i, m, { active: false }),
                                  () => pc.active ? cycleRotPctOp(type, i, m) : setRotPct(type, i, m, { active: true }),
                                  (v) => setRotPct(type, i, m, { active: true, val: v }),
                                )}
                              </React.Fragment>
                            )
                          })}
                        </React.Fragment>
                      )
                    })}

                    {/* Pattern row: HH/LH (or HL/LL) between each rotation and the one older than it.
                        Column under index 3 is empty — we'd need a 4-back reference to compare. */}
                    <div style={{ fontSize: 11, color: SOLAR_TEXT, fontFamily: 'monospace' }} title={lbl.title}>
                      Pattern vs prior
                    </div>
                    {ROT_INDICES.map(i => {
                      const key = PATTERN_KEY_FOR_IDX[i]
                      if (!key) return <div key={`pattern-${i}`} />
                      const p = s.pattern[key]
                      const a_idx = key.split('_')[0], b_idx = key.split('_')[1]
                      return (
                        <div key={`pattern-${i}`} style={{ display: 'flex', gap: 4 }}>
                          <div style={ROT_CELL}>
                            <button className={`control-btn ${p === 'any' ? 'primary' : ''}`}
                              style={ROT_INNER}
                              onClick={() => setRotPattern(type, key, 'any')}>Any</button>
                          </div>
                          <div style={ROT_CELL}>
                            <button className={`control-btn ${p === 'a' ? 'primary' : ''}`}
                              style={ROT_INNER}
                              title={`Rotation ${a_idx} is a ${lbl.a} vs rotation ${b_idx}`}
                              onClick={() => setRotPattern(type, key, 'a')}>{lbl.a}</button>
                          </div>
                          <div style={ROT_CELL}>
                            <button className={`control-btn ${p === 'b' ? 'primary' : ''}`}
                              style={ROT_INNER}
                              title={`Rotation ${a_idx} is a ${lbl.b} vs rotation ${b_idx}`}
                              onClick={() => setRotPattern(type, key, 'b')}>{lbl.b}</button>
                          </div>
                        </div>
                      )
                    })}
                  </div>
                )
              })()}
            </div>
          )
        })}
      </div>

      {/* CURRENT CONTEXT card (Phase 5) */}
      {data?.current_context && (() => {
        const cc = data.current_context!
        const num = (v: number | null | undefined, d = 2) => v == null ? '--' : v.toFixed(d)
        const pctD = (v: number | null | undefined) => v == null ? '--' : (v * 100).toFixed(1) + '%'
        const types: Array<keyof typeof cc.rotations> = ['UpRot', 'DownRot', 'Breakout', 'Breakdown']
        const patternColor = (p: string | null) =>
          p === 'HH' || p === 'HL' ? GREEN : p === 'LH' || p === 'LL' ? RED : SOLAR_MUTED
        const positionLabel = cc.position === 'above_upper' ? 'ABOVE UPPER TARGET'
          : cc.position === 'below_lower' ? 'BELOW LOWER TARGET'
          : cc.position === 'between' ? 'BETWEEN TARGETS' : '--'
        const positionColor = cc.position === 'above_upper' ? GREEN
          : cc.position === 'below_lower' ? RED : SOLAR_MUTED

        // Summary sentence
        const rotLabel = cc.active_rotation === 'up' ? 'UP ROTATION' : cc.active_rotation === 'down' ? 'DOWN ROTATION' : '—'
        const regLabel = cc.active_regime === 'breakout' ? 'BREAKOUT SEQUENCE' : cc.active_regime === 'breakdown' ? 'BREAKDOWN SEQUENCE' : '—'
        const activeBar = cc.active_rotation === 'up' ? cc.rotations.UpRot.Time_0
          : cc.active_rotation === 'down' ? cc.rotations.DownRot.Time_0 : null

        const isBasket = mode === 'basket'
        const ind = cc.indicators

        const KV: React.FC<{ label: string; value: string; sub?: string; color?: string }> = ({ label, value, sub, color }) => (
          <div style={{ padding: '4px 10px', borderRight: `1px solid ${SOLAR_BORDER}`, minWidth: 0 }}>
            <div style={{ fontSize: 9, color: SOLAR_MUTED }}>{label}</div>
            <div style={{ fontSize: 12, fontFamily: 'monospace', fontWeight: 700, color: color ?? SOLAR_TEXT, whiteSpace: 'nowrap' }}>{value}</div>
            {sub != null && <div style={{ fontSize: 9, color: SOLAR_MUTED, fontFamily: 'monospace' }}>{sub}</div>}
          </div>
        )

        return (
          <div style={{ border: `1px solid ${SOLAR_BORDER}`, background: SOLAR_BG }}>
            {/* Header */}
            <div style={{ padding: '6px 10px', borderBottom: `1px solid ${SOLAR_BORDER}`, display: 'flex', alignItems: 'baseline', gap: 12, fontSize: 11, fontFamily: 'monospace' }}>
              <span style={{ color: SOLAR_MUTED, fontSize: 10 }}>CURRENT CONTEXT</span>
              <span style={{ color: SOLAR_TEXT, fontWeight: 700 }}>{data.ticker}</span>
              <span style={{ color: SOLAR_MUTED }}>{cc.date ?? '--'}</span>
              <span style={{ color: SOLAR_TEXT }}>Close {num(cc.close)}</span>
              <span style={{ marginLeft: 'auto', color: SOLAR_MUTED, fontSize: 10 }}>
                <span style={{ color: BLUE, fontWeight: 700 }}>{rotLabel}</span>
                {activeBar != null && <span style={{ color: SOLAR_TEXT }}> · bar {activeBar}</span>}
                {' · '}
                <span style={{ color: BLUE, fontWeight: 700 }}>{regLabel}</span>
                {' · '}
                <span style={{ color: positionColor, fontWeight: 700 }}>{positionLabel}</span>
              </span>
            </div>

            {/* Indicators strip */}
            <div style={{ display: 'flex', flexWrap: 'wrap', borderBottom: `1px solid ${SOLAR_BORDER}` }}>
              <KV label="RV (ANN)"    value={num(ind.rv_ann, 1) + '%'} sub={'%ile ' + pctD(ind.rv_pct)} />
              <KV label="H REACT"     value={num(ind.h_react, 2)}      sub={'%ile ' + pctD(ind.h_pct)} />
              <KV label="L REACT"     value={num(ind.l_react, 2)}      sub={'%ile ' + pctD(ind.l_pct)} />
              <KV label="PRICE CHG"   value={num(ind.p_react, 2)}      sub={'%ile ' + pctD(ind.p_pct)} />
              <KV label="UPPER TGT"   value={num(cc.upper_target)} />
              <KV label="LOWER TGT"   value={num(cc.lower_target)} />
              {isBasket && ind.breadth      != null && <KV label="BREADTH"     value={num(ind.breadth, 1) + '%'} />}
              {isBasket && ind.breakout_pct != null && <KV label="BREAKOUT %"  value={num(ind.breakout_pct, 1) + '%'} />}
              {isBasket && ind.corr_pct     != null && <KV label="CORR %"      value={num(ind.corr_pct, 1) + '%'} />}
            </div>

            {/* Rotation context table */}
            <table style={{ width: '100%', borderCollapse: 'collapse', color: SOLAR_TEXT, fontSize: 11, fontFamily: 'monospace' }}>
              <thead>
                <tr style={{ color: SOLAR_MUTED }}>
                  <th rowSpan={2} style={{ textAlign: 'left', padding: '2px 6px', verticalAlign: 'bottom', borderRight: `1px solid ${SOLAR_BORDER}` }}>Type</th>
                  <th rowSpan={2} style={{ textAlign: 'center', padding: '2px 6px', verticalAlign: 'bottom', borderRight: `1px solid ${SOLAR_BORDER}` }}>Pattern</th>
                  <th colSpan={3} style={{ textAlign: 'center', padding: '2px 6px', borderBottom: `1px solid ${SOLAR_BORDER}`, borderRight: `1px solid ${SOLAR_BORDER}` }}>Current</th>
                  <th colSpan={3} style={{ textAlign: 'center', padding: '2px 6px', borderBottom: `1px solid ${SOLAR_BORDER}`, borderRight: `1px solid ${SOLAR_BORDER}` }}>Percentile</th>
                  <th colSpan={3} style={{ textAlign: 'center', padding: '2px 6px', borderBottom: `1px solid ${SOLAR_BORDER}`, borderRight: `1px solid ${SOLAR_BORDER}` }}>Prior 1</th>
                  <th colSpan={3} style={{ textAlign: 'center', padding: '2px 6px', borderBottom: `1px solid ${SOLAR_BORDER}`, borderRight: `1px solid ${SOLAR_BORDER}` }}>Prior 2</th>
                  <th colSpan={3} style={{ textAlign: 'center', padding: '2px 6px', borderBottom: `1px solid ${SOLAR_BORDER}` }}>Prior 3</th>
                </tr>
                <tr style={{ color: SOLAR_MUTED }}>
                  <th style={{ textAlign: 'right', padding: '2px 6px' }}>Time</th>
                  <th style={{ textAlign: 'right', padding: '2px 6px' }}>Range</th>
                  <th style={{ textAlign: 'right', padding: '2px 6px', borderRight: `1px solid ${SOLAR_BORDER}` }}>Change</th>
                  <th style={{ textAlign: 'right', padding: '2px 6px' }}>Time</th>
                  <th style={{ textAlign: 'right', padding: '2px 6px' }}>Range</th>
                  <th style={{ textAlign: 'right', padding: '2px 6px', borderRight: `1px solid ${SOLAR_BORDER}` }}>Change</th>
                  <th style={{ textAlign: 'right', padding: '2px 6px' }}>Time</th>
                  <th style={{ textAlign: 'right', padding: '2px 6px' }}>Range</th>
                  <th style={{ textAlign: 'right', padding: '2px 6px', borderRight: `1px solid ${SOLAR_BORDER}` }}>Change</th>
                  <th style={{ textAlign: 'right', padding: '2px 6px' }}>Time</th>
                  <th style={{ textAlign: 'right', padding: '2px 6px' }}>Range</th>
                  <th style={{ textAlign: 'right', padding: '2px 6px', borderRight: `1px solid ${SOLAR_BORDER}` }}>Change</th>
                  <th style={{ textAlign: 'right', padding: '2px 6px' }}>Time</th>
                  <th style={{ textAlign: 'right', padding: '2px 6px' }}>Range</th>
                  <th style={{ textAlign: 'right', padding: '2px 6px' }}>Change</th>
                </tr>
              </thead>
              <tbody>
                {types.map(t => {
                  const c = cc.rotations[t]
                  const rowBg = c.active ? 'rgba(50, 50, 255, 0.08)' : 'transparent'
                  const weight = c.active ? 700 : 400
                  const cell = (v: string, sep = false) =>
                    <td style={{ textAlign: 'right', padding: '2px 6px', borderRight: sep ? `1px solid ${SOLAR_BORDER}` : undefined }}>{v}</td>
                  return (
                    <tr key={t} style={{ borderTop: `1px solid ${SOLAR_BORDER}`, background: rowBg, fontWeight: weight }}>
                      <td style={{ padding: '2px 6px', borderRight: `1px solid ${SOLAR_BORDER}` }}>{t}</td>
                      <td style={{ padding: '2px 6px', textAlign: 'center', borderRight: `1px solid ${SOLAR_BORDER}`, color: patternColor(c.pattern), fontWeight: 700 }}>{c.pattern ?? '—'}</td>
                      {cell(num(c.Time_0, 0))}
                      {cell(pctD(c.Range_0))}
                      {cell(pctD(c.Change_0), true)}
                      {cell(pctD(c.Time_0_Pct))}
                      {cell(pctD(c.Range_0_Pct))}
                      {cell(pctD(c.Change_0_Pct), true)}
                      {cell(num(c.Time_1, 0))}
                      {cell(pctD(c.Range_1))}
                      {cell(pctD(c.Change_1), true)}
                      {cell(num(c.Time_2, 0))}
                      {cell(pctD(c.Range_2))}
                      {cell(pctD(c.Change_2), true)}
                      {cell(num(c.Time_3, 0))}
                      {cell(pctD(c.Range_3))}
                      {cell(pctD(c.Change_3))}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )
      })()}

      {/* Chart */}
      <div ref={wrapRef} style={{ width: '100%', border: `1px solid ${SOLAR_BORDER}`, background: SOLAR_BG }}>
        <canvas ref={canvasRef} style={{ width: dims.w, height: dims.h, display: 'block' }} />
      </div>

      {/* Stats table */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(9, 1fr)', gap: 0, fontSize: 12, fontFamily: 'monospace',
                    border: `1px solid ${SOLAR_BORDER}`, background: SOLAR_BG }}>
        {['Sample','N','Mean','Median','Stddev','Win%','p5','p25 / p75','p95'].map(h =>
          <div key={h} style={{ padding: '6px 8px', color: SOLAR_MUTED, borderBottom: `1px solid ${SOLAR_BORDER}` }}>{h}</div>
        )}
        <div style={{ padding: '6px 8px', color: PINK }}>Baseline</div>
        <div style={{ padding: '6px 8px' }}>{baseSt?.n ?? '--'}</div>
        <div style={{ padding: '6px 8px' }}>{pct(baseSt?.mean ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(baseSt?.median ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(baseSt?.stddev ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(baseSt?.win_rate ?? null, 1)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(baseSt?.p5 ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(baseSt?.p25 ?? null)} / {pct(baseSt?.p75 ?? null)}</div>
        <div style={{ padding: '6px 8px' }}>{pct(baseSt?.p95 ?? null)}</div>

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

      {/* Forward paths (Phase 4) — tercile-colored winner/loser chart + match list */}
      <div style={{ border: `1px solid ${SOLAR_BORDER}`, background: SOLAR_BG }}>
        <div style={{ padding: '6px 8px', color: SOLAR_MUTED, fontSize: 10, borderBottom: `1px solid ${SOLAR_BORDER}`, display: 'flex', justifyContent: 'space-between' }}>
          <span>FORWARD PATHS {data?.horizon != null ? `— horizon ${data.horizon}${typeof data.horizon === 'number' ? ' bars' : ''}` : ''}</span>
          {forwardTerciles && <span>{data?.forward_paths?.length ?? 0} matches</span>}
        </div>
        <div style={{ display: 'flex' }}>
          <div ref={fwdWrapRef} style={{ flex: 1, minWidth: 0 }}>
            <canvas ref={fwdCanvasRef} style={{ width: fwdDims.w, height: fwdDims.h, display: 'block' }}
              onMouseLeave={() => setHoveredMatchIdx(null)} />
          </div>
          <div style={{ width: 220, borderLeft: `1px solid ${SOLAR_BORDER}`, display: 'flex', flexDirection: 'column', maxHeight: fwdDims.h, overflow: 'hidden' }}>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', fontSize: 10, color: SOLAR_MUTED, borderBottom: `1px solid ${SOLAR_BORDER}`, background: SOLAR_BG, position: 'sticky', top: 0 }}>
              <div style={{ padding: '4px 8px', cursor: 'pointer' }} onClick={() => {
                if (fwdSortBy === 'date') setFwdSortDir(d => d === 'asc' ? 'desc' : 'asc')
                else { setFwdSortBy('date'); setFwdSortDir('desc') }
              }}>
                DATE {fwdSortBy === 'date' ? (fwdSortDir === 'asc' ? '↑' : '↓') : ''}
              </div>
              <div style={{ padding: '4px 8px', textAlign: 'right', cursor: 'pointer' }} onClick={() => {
                if (fwdSortBy === 'return') setFwdSortDir(d => d === 'asc' ? 'desc' : 'asc')
                else { setFwdSortBy('return'); setFwdSortDir('desc') }
              }}>
                RETURN {fwdSortBy === 'return' ? (fwdSortDir === 'asc' ? '↑' : '↓') : ''}
              </div>
            </div>
            <div style={{ overflowY: 'auto', fontFamily: 'monospace', fontSize: 11 }}>
              {(() => {
                if (!forwardTerciles || !data?.forward_paths) {
                  return <div style={{ padding: 8, color: SOLAR_MUTED, fontSize: 10 }}>No matches</div>
                }
                const entries = data.forward_paths.map((p, i) => ({ p, i }))
                entries.sort((a, b) => {
                  const dir = fwdSortDir === 'asc' ? 1 : -1
                  if (fwdSortBy === 'return') return (a.p.final_return - b.p.final_return) * dir
                  const ad = a.p.date ?? '', bd = b.p.date ?? ''
                  return (ad < bd ? -1 : ad > bd ? 1 : 0) * dir
                })
                return entries.map(({ p, i }) => {
                  const ter = forwardTerciles.tercileOfOriginal.get(i)
                  const color = ter === 'top' ? GREEN : ter === 'bottom' ? RED : GRAY_T
                  const isHover = hoveredMatchIdx === i
                  return (
                    <div key={i}
                      onMouseEnter={() => setHoveredMatchIdx(i)}
                      style={{
                        display: 'grid', gridTemplateColumns: '1fr 1fr',
                        padding: '2px 8px',
                        background: isHover ? SOLAR_BORDER : 'transparent',
                        color,
                        cursor: 'default',
                      }}
                    >
                      <div>{p.date ?? '—'}</div>
                      <div style={{ textAlign: 'right' }}>{(p.final_return * 100).toFixed(2)}%</div>
                    </div>
                  )
                })
              })()}
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}
