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
  patterns: Record<'0_1' | '1_2' | '2_3', 'HH' | 'LH' | 'HL' | 'LL' | null>
  pattern_display?: string | null
  patterns_display?: Record<'0_1' | '1_2' | '2_3', string | null>
  Time_0: number | null;  Range_0: number | null;  Change_0: number | null
  Time_0_Pct: number | null;  Range_0_Pct: number | null;  Change_0_Pct: number | null
  Time_1: number | null;  Range_1: number | null;  Change_1: number | null
  Time_1_Pct: number | null;  Range_1_Pct: number | null;  Change_1_Pct: number | null
  Time_2: number | null;  Range_2: number | null;  Change_2: number | null
  Time_2_Pct: number | null;  Range_2_Pct: number | null;  Change_2_Pct: number | null
  Time_3: number | null;  Range_3: number | null;  Change_3: number | null
  Time_3_Pct: number | null;  Range_3_Pct: number | null;  Change_3_Pct: number | null
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
    rv_delta: number | null
    h_react: number | null;      h_pct: number | null
    h_delta: number | null
    l_react: number | null;      l_pct: number | null
    l_delta: number | null
    p_react: number | null;      p_pct: number | null
    p_delta: number | null
    breadth: number | null;      breakout_pct: number | null;  corr_pct: number | null
    breadth_delta: number | null; breakout_delta: number | null; corr_delta: number | null
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
  horizon: number
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
const SHEET_GRID_BORDER = '#d8cfb7'
const SOLAR_TEXT = '#586e75'
const SOLAR_MUTED = '#7f8d92'
const SOLAR_TITLE = '#6c7a80'

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

const COND_COLUMN_WIDTH = 176  // px — fixed to keep column widths consistent across groups

type StandardSourceId =
  | 'regime'
  | 'rotation'
  | 'loc_upper'
  | 'loc_lower'
  | 'rv'
  | 'h'
  | 'p'
  | 'l'
  | 'breadth'
  | 'breakout_pct'
  | 'corr'

type StandardSourceMode = 'state' | 'value' | 'percentile' | 'trend'

interface StandardFilterSourceConfig {
  id: StandardSourceId
  label: string
  thresholdKey?: string
  percentileKey?: string
  trendKey?: string
  triKey?: string
  basketOnly?: boolean
  modeOrder: StandardSourceMode[]
}

const STANDARD_FILTER_SOURCES: StandardFilterSourceConfig[] = [
  { id: 'regime', label: 'LT Regime', triKey: 'breakout_state', modeOrder: ['state'] },
  { id: 'rotation', label: 'Rotation', triKey: 'rotation_state', modeOrder: ['state'] },
  { id: 'loc_upper', label: 'Vs Upper', triKey: 'loc_upper', modeOrder: ['state'] },
  { id: 'loc_lower', label: 'Vs Lower', triKey: 'loc_lower', modeOrder: ['state'] },
  { id: 'rv', label: 'RV', thresholdKey: 'rv', percentileKey: 'rv_pct', trendKey: 'rv_trend', modeOrder: ['value', 'percentile', 'trend'] },
  { id: 'h', label: 'H React', thresholdKey: 'h', percentileKey: 'h_pct', trendKey: 'h_trend', modeOrder: ['value', 'percentile', 'trend'] },
  { id: 'p', label: 'Price Chg', thresholdKey: 'p', percentileKey: 'p_pct', trendKey: 'p_trend', modeOrder: ['value', 'percentile', 'trend'] },
  { id: 'l', label: 'L React', thresholdKey: 'l', percentileKey: 'l_pct', trendKey: 'l_trend', modeOrder: ['value', 'percentile', 'trend'] },
  { id: 'breadth', label: 'Breadth %', thresholdKey: 'breadth', trendKey: 'breadth_trend', basketOnly: true, modeOrder: ['value', 'trend'] },
  { id: 'breakout_pct', label: 'Breakout %', thresholdKey: 'breakout', trendKey: 'breakout_trend', basketOnly: true, modeOrder: ['value', 'trend'] },
  { id: 'corr', label: 'Corr %', thresholdKey: 'corr', trendKey: 'corr_trend', basketOnly: true, modeOrder: ['value', 'trend'] },
]

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

type RotationFilterMode = 'value' | 'percentile' | 'pattern'

interface ActiveRotationFilterRow {
  id: string
  type: RotType
  idx: RotIdx
  mode: RotationFilterMode
  metric?: Metric
  patternKey?: PatternKey
}

// Per-type labels for the pattern rows.
// Bullish types use HH / HL, bearish types use HL / LL.
const ROT_PATTERN_LABELS: Record<RotType, { a: string; b: string; title: string }> = {
  UpRot:     { a: 'HH', b: 'HL', title: 'Relative high/low relationship between consecutive up rotations' },
  DownRot:   { a: 'HL', b: 'LL', title: 'Trough low comparison between consecutive down rotations' },
  Breakout:  { a: 'HH', b: 'HL', title: 'Relative high/low relationship between consecutive breakouts' },
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

const CARD_STYLE: React.CSSProperties = {
  border: `1px solid ${SOLAR_BORDER}`,
  background: SOLAR_BG,
  borderRadius: 0,
}

const CARD_HEADER_STYLE: React.CSSProperties = {
  padding: '5px 8px',
  color: SOLAR_TITLE,
  fontSize: 9,
  borderBottom: `1px solid ${SOLAR_BORDER}`,
  background: 'rgba(88, 110, 117, 0.06)',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'space-between',
  gap: 6,
  letterSpacing: 0.4,
}

const CARD_BODY_STYLE: React.CSSProperties = {
  padding: 8,
}

const FIELD_LABEL_STYLE: React.CSSProperties = {
  fontSize: 9,
  color: SOLAR_TITLE,
  marginBottom: 3,
  letterSpacing: 0.3,
}

const CONTROLS_GRID_STYLE: React.CSSProperties = {
  display: 'flex',
  gap: 8,
  flexWrap: 'wrap',
  alignItems: 'flex-end',
}

const COMPACT_BUTTON_STYLE: React.CSSProperties = {
  fontSize: 10,
  height: 24,
  padding: '0 8px',
  width: 'auto',
  boxSizing: 'border-box',
}

const COMPACT_INPUT_STYLE: React.CSSProperties = {
  height: 24,
  padding: '4px 6px',
  fontSize: 10,
  boxSizing: 'border-box',
}

const GROUP_TOGGLE_STYLE: React.CSSProperties = {
  padding: '3px 8px',
  cursor: 'pointer',
  fontSize: 11,
  fontFamily: 'monospace',
  display: 'flex',
  alignItems: 'center',
  gap: 5,
}

const GROUP_BODY_PADDING = '3px 6px 6px 14px'
const FILTER_VALUE_GRID_STYLE: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(3, minmax(0, 1fr))',
  minHeight: 22,
}

const FILTER_VALUE_CELL_STYLE: React.CSSProperties = {
  minWidth: 0,
  borderRight: `1px solid ${SOLAR_BORDER}`,
}

const FILTER_VALUE_LAST_CELL_STYLE: React.CSSProperties = {
  minWidth: 0,
}

const FILTER_CELL_INPUT_STYLE: React.CSSProperties = {
  width: '100%',
  height: '100%',
  minHeight: 22,
  padding: '0 4px',
  border: 0,
  background: SOLAR_BG,
  boxSizing: 'border-box',
  fontSize: 9,
  textAlign: 'center',
}

const SHEET_BUTTON_BASE_STYLE: React.CSSProperties = {
  width: '100%',
  minHeight: 22,
  padding: '0 4px',
  border: 0,
  borderRadius: 0,
  background: 'transparent',
  color: SOLAR_TEXT,
  fontFamily: 'monospace',
  fontSize: 9,
  boxSizing: 'border-box',
  cursor: 'pointer',
}

const FILTER_EDITOR_ROW_STYLE: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: '84px 76px 114px minmax(0, 1fr) 24px',
  borderTop: `1px solid ${SOLAR_BORDER}`,
}

const FILTER_EDITOR_CELL_STYLE: React.CSSProperties = {
  minWidth: 0,
  padding: '3px 6px',
  borderRight: `1px solid ${SOLAR_BORDER}`,
  display: 'flex',
  alignItems: 'center',
  fontFamily: 'monospace',
  fontSize: 10,
}

const FILTER_EDITOR_HEADER_CELL_STYLE: React.CSSProperties = {
  ...FILTER_EDITOR_CELL_STYLE,
  color: SOLAR_MUTED,
  fontSize: 8,
  letterSpacing: 0.3,
  background: 'rgba(147, 161, 161, 0.08)',
}

const CONTEXT_GRID_STYLE: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fit, minmax(110px, 1fr))',
}

const HORIZON_OPTIONS = ['1', '5', '21', '63', '252'] as const
type HorizonOption = typeof HORIZON_OPTIONS[number]

const SHEET_TABLE_STYLE: React.CSSProperties = {
  width: '100%',
  borderCollapse: 'collapse',
  tableLayout: 'fixed',
  color: SOLAR_TEXT,
  fontSize: 10,
  fontFamily: 'monospace',
  lineHeight: 1.1,
}

const SHEET_HEADER_CELL_STYLE: React.CSSProperties = {
  padding: '2px 5px',
  textAlign: 'center',
  color: SOLAR_TITLE,
  borderBottom: `1px solid ${SHEET_GRID_BORDER}`,
  background: 'rgba(147, 161, 161, 0.08)',
  fontSize: 7,
  letterSpacing: 0.25,
  whiteSpace: 'nowrap',
}

const SHEET_ROW_LABEL_STYLE: React.CSSProperties = {
  padding: '2px 5px',
  borderTop: `1px solid ${SHEET_GRID_BORDER}`,
  whiteSpace: 'nowrap',
  fontWeight: 700,
  textAlign: 'center',
  verticalAlign: 'middle',
}

const SHEET_CELL_STYLE: React.CSSProperties = {
  padding: '2px 5px',
  borderTop: `1px solid ${SHEET_GRID_BORDER}`,
  borderLeft: `1px solid ${SHEET_GRID_BORDER}`,
  textAlign: 'center',
  whiteSpace: 'nowrap',
  verticalAlign: 'middle',
  overflow: 'hidden',
}

const SHEET_BUTTON_CELL_STYLE: React.CSSProperties = {
  width: '100%',
  minHeight: 18,
  padding: '2px 4px',
  border: 0,
  background: 'transparent',
  color: SOLAR_TEXT,
  fontFamily: 'monospace',
  fontSize: 9,
  textAlign: 'center',
  cursor: 'pointer',
}

const modeLabel = (mode: StandardSourceMode) =>
  mode === 'state' ? 'State' : mode === 'value' ? 'Value' : mode === 'percentile' ? '%ile' : 'Trend'

const patternToTriState = (type: RotType, pattern: string | null | undefined): TriState => {
  const labels = ROT_PATTERN_LABELS[type]
  if (pattern === labels.a) return 'a'
  if (pattern === labels.b) return 'b'
  return 'any'
}

const rotationIndexLabel = (idx: RotIdx) => (
  idx === 0 ? 'Current' : `${idx}-back`
)

const rotationModeLabel = (mode: RotationFilterMode) => (
  mode === 'value' ? 'Value' : mode === 'percentile' ? '%ile' : 'Pattern'
)

const getRotationMetricValue = (ctx: RotCtx, idx: RotIdx, metric: Metric) => (
  ctx[`${metric}_${idx}` as keyof RotCtx] as number | null | undefined
)

const getRotationMetricPercentile = (ctx: RotCtx, idx: RotIdx, metric: Metric) => (
  ctx[`${metric}_${idx}_Pct` as keyof RotCtx] as number | null | undefined
)

const getRotationPatternDisplay = (ctx: RotCtx, key: PatternKey) => (
  ctx.patterns_display?.[key] ?? ctx.patterns?.[key] ?? null
)

const triStateFromCurrent = (sourceId: StandardSourceId, cc: CurrentContext): TriState => {
  switch (sourceId) {
    case 'regime':
      return cc.active_regime === 'breakout' ? 'a' : cc.active_regime === 'breakdown' ? 'b' : 'any'
    case 'rotation':
      return cc.active_rotation === 'up' ? 'a' : cc.active_rotation === 'down' ? 'b' : 'any'
    case 'loc_upper':
      return cc.position === 'above_upper' ? 'a' : cc.position == null ? 'any' : 'b'
    case 'loc_lower':
      return cc.position === 'below_lower' ? 'b' : cc.position == null ? 'any' : 'a'
    default:
      return 'any'
  }
}

const metricSnapshot = (sourceId: StandardSourceId, cc: CurrentContext): { value: number | null; percentile: number | null } => {
  const ind = cc.indicators
  switch (sourceId) {
    case 'rv':
      return { value: ind.rv_ann, percentile: ind.rv_pct }
    case 'h':
      return { value: ind.h_react, percentile: ind.h_pct }
    case 'p':
      return { value: ind.p_react, percentile: ind.p_pct }
    case 'l':
      return { value: ind.l_react, percentile: ind.l_pct }
    case 'breadth':
      return { value: ind.breadth, percentile: null }
    case 'breakout_pct':
      return { value: ind.breakout_pct, percentile: null }
    case 'corr':
      return { value: ind.corr_pct, percentile: null }
    default:
      return { value: null, percentile: null }
  }
}

const SectionCard: React.FC<{
  title: string
  right?: React.ReactNode
  children: React.ReactNode
  bodyStyle?: React.CSSProperties
}> = ({ title, right, children, bodyStyle }) => (
  <div style={CARD_STYLE}>
    <div style={CARD_HEADER_STYLE}>
      <span>{title}</span>
      {right}
    </div>
    <div style={{ ...CARD_BODY_STYLE, ...bodyStyle }}>
      {children}
    </div>
  </div>
)

const FieldLabel: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <div style={FIELD_LABEL_STYLE}>{children}</div>
)

const SheetToggleButton: React.FC<{
  active: boolean
  label: string
  onClick: () => void
  title?: string
}> = ({ active, label, onClick, title }) => (
  <button
    type="button"
    onClick={onClick}
    title={title}
    style={{
      ...SHEET_BUTTON_BASE_STYLE,
      background: active ? 'rgba(50, 50, 255, 0.10)' : 'transparent',
      color: active ? BLUE : SOLAR_TEXT,
      fontWeight: active ? 700 : 500,
    }}
  >
    {label}
  </button>
)

const ContextMetricCell: React.FC<{
  label: string
  value: string
  sub?: string
  active?: boolean
  color?: string
  onClick?: () => void
}> = ({ label, value, sub, active = false, color, onClick }) => {
  const content = (
    <>
      <div style={{ fontSize: 8, color: SOLAR_MUTED, letterSpacing: 0.25 }}>{label}</div>
      <div style={{ fontSize: 11, fontFamily: 'monospace', fontWeight: 700, color: color ?? SOLAR_TEXT, whiteSpace: 'nowrap' }}>{value}</div>
      {sub != null && <div style={{ fontSize: 8, color: SOLAR_MUTED, fontFamily: 'monospace' }}>{sub}</div>}
    </>
  )

  if (onClick) {
    return (
      <button
        type="button"
        onClick={onClick}
        style={{
          textAlign: 'left',
          padding: '4px 6px',
          border: 0,
          borderRight: `1px solid ${SOLAR_BORDER}`,
          borderBottom: `1px solid ${SOLAR_BORDER}`,
          background: active ? 'rgba(50, 50, 255, 0.08)' : SOLAR_BG,
          cursor: 'pointer',
          minWidth: 0,
        }}
      >
        {content}
      </button>
    )
  }

  return (
    <div style={{ padding: '4px 6px', borderRight: `1px solid ${SOLAR_BORDER}`, borderBottom: `1px solid ${SOLAR_BORDER}`, minWidth: 0, background: SOLAR_BG }}>
      {content}
    </div>
  )
}

const TypeaheadField: React.FC<{
  label: string
  value: string
  selectedValue: string
  options: string[]
  showDropdown: boolean
  setShowDropdown: (next: boolean) => void
  onValueChange: (next: string) => void
  onSelect: (next: string) => void
  onCommit?: (value: string) => void
  placeholder: string
  width: number
}> = ({
  label,
  value,
  selectedValue,
  options,
  showDropdown,
  setShowDropdown,
  onValueChange,
  onSelect,
  onCommit,
  placeholder,
  width,
}) => (
  <div style={{ position: 'relative' }}>
    <FieldLabel>{label}</FieldLabel>
    <input
      type="text"
      className="backtest-input"
      value={value}
      onChange={e => { onValueChange(e.target.value); setShowDropdown(true) }}
      onFocus={() => setShowDropdown(true)}
      onBlur={() => setTimeout(() => setShowDropdown(false), 120)}
      onKeyDown={e => {
        if (e.key === 'Enter') {
          const next = value.trim()
          if (next) {
            if (onCommit) onCommit(next)
            else onSelect(next)
            setShowDropdown(false)
          }
        }
      }}
      placeholder={placeholder}
      style={{ ...COMPACT_INPUT_STYLE, width }}
    />
    {showDropdown && options.length > 0 && (
      <div
        style={{
          position: 'absolute',
          top: '100%',
          left: 0,
          zIndex: 50,
          background: SOLAR_BG,
          border: `1px solid ${SOLAR_BORDER}`,
          maxHeight: 180,
          overflowY: 'auto',
          minWidth: width,
          borderRadius: 4,
          boxShadow: '0 4px 10px rgba(88, 110, 117, 0.10)',
        }}
      >
        {options.map(option => (
          <div
            key={option}
            onMouseDown={() => { onSelect(option); setShowDropdown(false) }}
            style={{
              padding: '3px 6px',
              cursor: 'pointer',
              fontSize: 11,
              fontFamily: 'monospace',
              background: option === selectedValue ? SOLAR_BORDER : 'transparent',
            }}
          >
            {option}
          </div>
        ))}
      </div>
    )}
  </div>
)

const AnyOpValueControl: React.FC<{
  state: ThresholdState
  min: number
  max: number
  step: number
  percent?: boolean
  onSetAny: () => void
  onCycleOrEnable: () => void
  onSetValue: (value: number) => void
}> = ({ state, min, max, step, percent, onSetAny, onCycleOrEnable, onSetValue }) => (
  <div style={FILTER_VALUE_GRID_STYLE}>
    <div style={FILTER_VALUE_CELL_STYLE}>
      <SheetToggleButton active={!state.active} label="Any" onClick={onSetAny} />
    </div>
    <div style={FILTER_VALUE_CELL_STYLE}>
      <SheetToggleButton active={state.active} label={state.op} onClick={onCycleOrEnable} />
    </div>
    <div style={FILTER_VALUE_LAST_CELL_STYLE}>
      <input
        type="number"
        className="backtest-input"
        value={toDisp(state.val, percent)}
        min={toDisp(min, percent)}
        max={toDisp(max, percent)}
        step={toDisp(step, percent)}
        onChange={e => {
          const entered = parseFloat(e.target.value)
          if (!isNaN(entered)) {
            const internal = fromDisp(entered, percent)
            onSetValue(Math.min(max, Math.max(min, internal)))
          }
        }}
        style={FILTER_CELL_INPUT_STYLE}
      />
    </div>
  </div>
)

const countRotationFilters = (state: RotFilterState) => {
  let activeCount = 0
  for (const idx of ROT_INDICES) {
    for (const metric of METRICS) {
      if (state.threshold[idx][metric].active) activeCount++
      if (state.percentile[idx][metric].active) activeCount++
    }
  }
  for (const key of PATTERN_KEYS) {
    if (state.pattern[key] !== 'any') activeCount++
  }
  return activeCount
}

const buildDistributionParams = ({
  lookback,
  target,
  isBasket,
  triConds,
  thresholds,
  rotFilters,
  horizon,
}: {
  lookback: number
  target: string
  isBasket: boolean
  triConds: Record<string, TriState>
  thresholds: Record<string, ThresholdState>
  rotFilters: AllRotFilters
  horizon: HorizonOption
}) => {
  const params: Record<string, string | number> = { lookback, horizon }
  if (isBasket) params.basket = target
  else params.ticker = target

  for (const def of COND_DEFS) {
    if (def.basketOnly && !isBasket) continue
    if (def.kind === 'tri') {
      const state = triConds[def.key]
      if (state === 'a') params[def.key] = def.a.value
      else if (state === 'b') params[def.key] = def.b.value
      continue
    }
    const threshold = thresholds[def.key]
    if (threshold?.active) {
      params[def.opParam] = threshold.op
      params[def.valParam] = threshold.val
    }
  }

  const rotParam = buildRotParam(rotFilters)
  if (rotParam) params.rot = rotParam
  return params
}

const buildForwardTerciles = (paths?: ForwardPath[] | null) => {
  if (!paths || paths.length === 0) return null
  const sortedByReturn = [...paths]
    .map((p, originalIdx) => ({ p, originalIdx }))
    .sort((a, b) => a.p.final_return - b.p.final_return)
  const n = sortedByReturn.length
  const cutA = Math.floor(n / 3)
  const cutB = Math.floor(2 * n / 3)
  const tercileOfOriginal = new Map<number, 'bottom' | 'middle' | 'top'>()
  sortedByReturn.forEach((entry, i) => {
    const tercile: 'bottom' | 'middle' | 'top' = i < cutA ? 'bottom' : i < cutB ? 'middle' : 'top'
    tercileOfOriginal.set(entry.originalIdx, tercile)
  })

  const computeStats = (group: ForwardPath[]): { mean: number[]; upper: number[]; lower: number[] } => {
    if (group.length === 0 || group[0].series.length === 0) return { mean: [], upper: [], lower: [] }
    const len = group[0].series.length
    const mean = new Array(len).fill(0)
    const upper = new Array(len).fill(0)
    const lower = new Array(len).fill(0)
    for (let i = 0; i < len; i++) {
      let sum = 0
      let count = 0
      for (const path of group) {
        const value = path.series[i]
        if (Number.isFinite(value)) {
          sum += value
          count++
        }
      }
      const avg = count > 0 ? sum / count : 0
      let sq = 0
      for (const path of group) {
        const value = path.series[i]
        if (Number.isFinite(value)) sq += (value - avg) * (value - avg)
      }
      const std = count > 1 ? Math.sqrt(sq / count) : 0
      mean[i] = avg
      upper[i] = avg + std
      lower[i] = avg - std
    }
    return { mean, upper, lower }
  }

  const bottom = sortedByReturn.slice(0, cutA).map(x => x.p)
  const middle = sortedByReturn.slice(cutA, cutB).map(x => x.p)
  const top = sortedByReturn.slice(cutB).map(x => x.p)

  return {
    bottom: { paths: bottom, ...computeStats(bottom) },
    middle: { paths: middle, ...computeStats(middle) },
    top: { paths: top, ...computeStats(top) },
    tercileOfOriginal,
    all: paths,
  }
}

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
  const [horizon, setHorizon] = useState<HorizonOption>('1')
  const [triConds, setTriConds] = useState<Record<string, TriState>>(initTriConds)
  const [thresholds, setThresholds] = useState<Record<string, ThresholdState>>(initThresholds)
  const [rotFilters, setRotFilters] = useState<AllRotFilters>(initRotFilters)
  const [selectedSourceIds, setSelectedSourceIds] = useState<StandardSourceId[]>([])
  const [selectedSourceModes, setSelectedSourceModes] = useState<Partial<Record<StandardSourceId, StandardSourceMode>>>({})
  const [openRotType, setOpenRotType] = useState<Record<RotType, boolean>>({
    UpRot: false, DownRot: false, Breakout: false, Breakdown: false,
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
  const currentContext = data?.current_context ?? null

  const visibleStandardSources = useMemo(
    () => STANDARD_FILTER_SOURCES.filter(source => !source.basketOnly || isBasket),
    [isBasket],
  )

  const sourceConfigMap = useMemo(
    () => Object.fromEntries(STANDARD_FILTER_SOURCES.map(source => [source.id, source])) as Record<StandardSourceId, StandardFilterSourceConfig>,
    [],
  )

  const clearSourceFilters = useCallback((source: StandardFilterSourceConfig) => {
    const triPatch: Partial<Record<string, TriState>> = {}
    if (source.triKey) triPatch[source.triKey] = 'any'
    if (source.trendKey) triPatch[source.trendKey] = 'any'
    if (Object.keys(triPatch).length > 0) {
      setTriConds(prev => ({ ...prev, ...triPatch }))
    }
    setThresholds(prev => {
      const next = { ...prev }
      if (source.thresholdKey) next[source.thresholdKey] = { ...next[source.thresholdKey], active: false }
      if (source.percentileKey) next[source.percentileKey] = { ...next[source.percentileKey], active: false }
      return next
    })
  }, [])

  const applySourceDefaults = useCallback((source: StandardFilterSourceConfig, mode: StandardSourceMode, cc: CurrentContext | null) => {
    const triPatch: Partial<Record<string, TriState>> = {}
    if (source.triKey) triPatch[source.triKey] = 'any'
    if (source.trendKey) triPatch[source.trendKey] = 'any'
    if (cc && source.triKey) triPatch[source.triKey] = triStateFromCurrent(source.id, cc)
    if (Object.keys(triPatch).length > 0) {
      setTriConds(prev => ({ ...prev, ...triPatch }))
    }

    setThresholds(prev => {
      const next = { ...prev }
      if (source.thresholdKey) next[source.thresholdKey] = { ...next[source.thresholdKey], active: false }
      if (source.percentileKey) next[source.percentileKey] = { ...next[source.percentileKey], active: false }
      if (!cc) return next

      const snap = metricSnapshot(source.id, cc)
      if (mode === 'value' && source.thresholdKey && snap.value != null) {
        next[source.thresholdKey] = { ...next[source.thresholdKey], active: true, val: snap.value }
      }
      if (mode === 'percentile' && source.percentileKey && snap.percentile != null) {
        next[source.percentileKey] = { ...next[source.percentileKey], active: true, val: snap.percentile }
      }
      return next
    })
  }, [])

  const setSourceMode = useCallback((sourceId: StandardSourceId, mode: StandardSourceMode) => {
    const source = sourceConfigMap[sourceId]
    setSelectedSourceModes(prev => ({ ...prev, [sourceId]: mode }))
    applySourceDefaults(source, mode, currentContext)
  }, [applySourceDefaults, currentContext, sourceConfigMap])

  const toggleSourceSelection = useCallback((sourceId: StandardSourceId) => {
    const source = sourceConfigMap[sourceId]
    setSelectedSourceIds(prev => {
      if (prev.includes(sourceId)) {
        clearSourceFilters(source)
        setSelectedSourceModes(modes => {
          const next = { ...modes }
          delete next[sourceId]
          return next
        })
        return prev.filter(id => id !== sourceId)
      }

      const nextMode = source.modeOrder[0]
      setSelectedSourceModes(modes => ({ ...modes, [sourceId]: nextMode }))
      applySourceDefaults(source, nextMode, currentContext)
      return [...prev, sourceId]
    })
  }, [applySourceDefaults, clearSourceFilters, currentContext, sourceConfigMap])

  const toggleSourceModeSelection = useCallback((sourceId: StandardSourceId, mode: StandardSourceMode) => {
    const source = sourceConfigMap[sourceId]
    const isSelected = selectedSourceIds.includes(sourceId)
    const currentMode = selectedSourceModes[sourceId] ?? source.modeOrder[0]

    if (isSelected && currentMode === mode) {
      clearSourceFilters(source)
      setSelectedSourceIds(prev => prev.filter(id => id !== sourceId))
      setSelectedSourceModes(prev => {
        const next = { ...prev }
        delete next[sourceId]
        return next
      })
      return
    }

    if (!isSelected) {
      setSelectedSourceIds(prev => [...prev, sourceId])
    }
    setSelectedSourceModes(prev => ({ ...prev, [sourceId]: mode }))
    applySourceDefaults(source, mode, currentContext)
  }, [applySourceDefaults, clearSourceFilters, currentContext, selectedSourceIds, selectedSourceModes, sourceConfigMap])

  // Fetch
  useEffect(() => {
    if (!target) { setData(null); return }
    const params = buildDistributionParams({
      lookback,
      target,
      isBasket,
      triConds,
      thresholds,
      rotFilters,
      horizon,
    })
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
    setTriConds(initTriConds())
    setThresholds(initThresholds())
    setRotFilters(initRotFilters())
    setSelectedSourceIds([])
    setSelectedSourceModes({})
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

  const toggleRotMetricSelection = useCallback((
    type: RotType,
    idx: RotIdx,
    metric: Metric,
    mode: Exclude<RotationFilterMode, 'pattern'>,
    currentValue: number | null | undefined,
  ) => {
    if (currentValue == null) return
    if (mode === 'value') {
      setRotFilters(prev => {
        const current = prev[type].threshold[idx][metric]
        return {
          ...prev,
          [type]: {
            ...prev[type],
            threshold: {
              ...prev[type].threshold,
              [idx]: {
                ...prev[type].threshold[idx],
                [metric]: current.active
                  ? { ...current, active: false }
                  : { ...current, active: true, val: currentValue },
              },
            },
          },
        }
      })
      return
    }

    setRotFilters(prev => {
      const current = prev[type].percentile[idx][metric]
      return {
        ...prev,
        [type]: {
          ...prev[type],
          percentile: {
            ...prev[type].percentile,
            [idx]: {
              ...prev[type].percentile[idx],
              [metric]: current.active
                ? { ...current, active: false }
                : { ...current, active: true, val: currentValue },
            },
          },
        },
      }
    })
  }, [])

  const toggleRotPatternSelection = useCallback((type: RotType, key: PatternKey, pattern: string | null | undefined) => {
    const nextState = patternToTriState(type, pattern)
    if (nextState === 'any') return
    setRotFilters(prev => {
      const current = prev[type].pattern[key]
      return {
        ...prev,
        [type]: {
          ...prev[type],
          pattern: {
            ...prev[type].pattern,
            [key]: current === 'any' ? nextState : 'any',
          },
        },
      }
    })
  }, [])

  const clearActiveRotationFilter = useCallback((row: ActiveRotationFilterRow) => {
    if (row.mode === 'pattern' && row.patternKey) {
      setRotPattern(row.type, row.patternKey, 'any')
      return
    }
    if (!row.metric) return
    if (row.mode === 'value') {
      setRotTh(row.type, row.idx, row.metric, { active: false })
      return
    }
    setRotPct(row.type, row.idx, row.metric, { active: false })
  }, [setRotPattern, setRotPct, setRotTh])

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
    return buildForwardTerciles(data?.forward_paths)
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
      ctx.fillText('No forward paths', fwdDims.w / 2, fwdDims.h / 2)
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
  }, [forwardTerciles, fwdDims, hoveredMatchIdx, data?.forward_paths])

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

  const stats = data?.filtered
  const baseSt = data?.baseline
  const sortedForwardEntries = useMemo(() => {
    if (!data?.forward_paths) return []
    const entries = data.forward_paths.map((p, i) => ({ p, i }))
    entries.sort((a, b) => {
      const dir = fwdSortDir === 'asc' ? 1 : -1
      if (fwdSortBy === 'return') return (a.p.final_return - b.p.final_return) * dir
      const ad = a.p.date ?? ''
      const bd = b.p.date ?? ''
      return (ad < bd ? -1 : ad > bd ? 1 : 0) * dir
    })
    return entries
  }, [data?.forward_paths, fwdSortBy, fwdSortDir])

  const selectedStandardSources = useMemo(
    () => visibleStandardSources.filter(source => selectedSourceIds.includes(source.id)),
    [selectedSourceIds, visibleStandardSources],
  )

  const activeRotationFilters = useMemo(() => {
    const rows: ActiveRotationFilterRow[] = []
    for (const type of ROT_TYPES) {
      const state = rotFilters[type]
      for (const idx of ROT_INDICES) {
        for (const metric of METRICS) {
          if (state.threshold[idx][metric].active) {
            rows.push({ id: `${type}:${idx}:${metric}:value`, type, idx, metric, mode: 'value' })
          }
          if (state.percentile[idx][metric].active) {
            rows.push({ id: `${type}:${idx}:${metric}:percentile`, type, idx, metric, mode: 'percentile' })
          }
        }
      }
      for (const patternKey of PATTERN_KEYS) {
        if (state.pattern[patternKey] !== 'any') {
          const idx = Number(patternKey.split('_')[0]) as RotIdx
          rows.push({ id: `${type}:${patternKey}:pattern`, type, idx, patternKey, mode: 'pattern' })
        }
      }
    }
    return rows
  }, [rotFilters])

  const formatSourceSnapshot = useCallback((sourceId: StandardSourceId) => {
    if (!currentContext) return { value: '--', sub: undefined as string | undefined }
    const snap = metricSnapshot(sourceId, currentContext)
    const positionToText = (state: TriState, aLabel: string, bLabel: string) =>
      state === 'a' ? aLabel : state === 'b' ? bLabel : '--'

    switch (sourceId) {
      case 'regime': {
        const state = triStateFromCurrent(sourceId, currentContext)
        return { value: positionToText(state, 'Breakout', 'Breakdown'), sub: undefined }
      }
      case 'rotation': {
        const state = triStateFromCurrent(sourceId, currentContext)
        return { value: positionToText(state, 'Up', 'Down'), sub: undefined }
      }
      case 'loc_upper': {
        const state = triStateFromCurrent(sourceId, currentContext)
        return { value: positionToText(state, 'Above', 'Below'), sub: currentContext.upper_target == null ? undefined : `Tgt ${currentContext.upper_target.toFixed(2)}` }
      }
      case 'loc_lower': {
        const state = triStateFromCurrent(sourceId, currentContext)
        return { value: positionToText(state, 'Above', 'Below'), sub: currentContext.lower_target == null ? undefined : `Tgt ${currentContext.lower_target.toFixed(2)}` }
      }
      case 'rv':
        return { value: snap.value == null ? '--' : `${snap.value.toFixed(1)}%`, sub: snap.percentile == null ? undefined : `%ile ${(snap.percentile * 100).toFixed(1)}%` }
      case 'breadth':
      case 'breakout_pct':
      case 'corr':
        return { value: snap.value == null ? '--' : `${snap.value.toFixed(1)}%`, sub: undefined }
      default:
        return {
          value: snap.value == null ? '--' : snap.value.toFixed(2),
          sub: snap.percentile == null ? undefined : `%ile ${(snap.percentile * 100).toFixed(1)}%`,
        }
    }
  }, [currentContext])

  const formatRotationFilterSnapshot = useCallback((row: ActiveRotationFilterRow) => {
    const labelBase = `${ROT_TYPE_LABELS[row.type]} ${rotationIndexLabel(row.idx)}`
    if (!currentContext) {
      return {
        label: row.mode === 'pattern'
          ? `${ROT_TYPE_LABELS[row.type]} ${row.patternKey?.replace('_', ' vs ') ?? 'pattern'}`
          : `${labelBase} ${row.metric}${row.mode === 'percentile' ? ' %ile' : ''}`,
        current: '--',
      }
    }

    const ctx = currentContext.rotations[row.type]
    if (row.mode === 'pattern' && row.patternKey) {
      return {
        label: `${ROT_TYPE_LABELS[row.type]} ${row.patternKey.replace('_', ' vs ')} Pattern`,
        current: getRotationPatternDisplay(ctx, row.patternKey) ?? '--',
      }
    }

    if (!row.metric) {
      return { label: labelBase, current: '--' }
    }

    const rawValue = row.mode === 'value'
      ? getRotationMetricValue(ctx, row.idx, row.metric)
      : getRotationMetricPercentile(ctx, row.idx, row.metric)

    const current = row.mode === 'percentile'
      ? pct(rawValue, 1)
      : row.metric === 'Time'
        ? (rawValue == null ? '--' : rawValue.toFixed(0))
        : pct(rawValue, 1)

    return {
      label: `${labelBase} ${row.metric}${row.mode === 'percentile' ? ' %ile' : ''}`,
      current,
    }
  }, [currentContext])

  return (
    <div className="distributions-panel" style={{ padding: 8, display: 'flex', flexDirection: 'column', gap: 8, height: '100%', overflow: 'auto' }}>
      <SectionCard
        title="SETUP"
        right={
          target ? (
            <span style={{ fontSize: 9, color: SOLAR_MUTED, fontFamily: 'monospace' }}>
              {mode.toUpperCase()} · {target} · {lookback} lookback · {horizon} bars
            </span>
          ) : null
        }
      >
        <div style={CONTROLS_GRID_STYLE}>
          <div>
            <FieldLabel>MODE</FieldLabel>
            <div style={{ display: 'flex', gap: 2 }}>
              <button className={`control-btn ${mode === 'ticker' ? 'primary' : ''}`} style={COMPACT_BUTTON_STYLE} onClick={() => setMode('ticker')}>Ticker</button>
              <button className={`control-btn ${mode === 'basket' ? 'primary' : ''}`} style={COMPACT_BUTTON_STYLE} onClick={() => setMode('basket')}>Basket</button>
            </div>
          </div>

          {mode === 'ticker' ? (
            <TypeaheadField
              label="TICKER"
              value={tickerQuery}
              selectedValue={ticker}
              options={filteredTickerOptions}
              showDropdown={showTickerDropdown}
              setShowDropdown={setShowTickerDropdown}
              onValueChange={next => setTickerQuery(next.toUpperCase())}
              onSelect={next => { setTicker(next); setTickerQuery(next) }}
              onCommit={next => setTicker(next.toUpperCase())}
              placeholder="AAPL"
              width={104}
            />
          ) : (
            <TypeaheadField
              label="BASKET"
              value={basketQuery}
              selectedValue={basket}
              options={filteredBasketOptions}
              showDropdown={showBasketDropdown}
              setShowDropdown={setShowBasketDropdown}
              onValueChange={setBasketQuery}
              onSelect={next => { setBasket(next); setBasketQuery(next) }}
              onCommit={next => setBasket(next)}
              placeholder="Search baskets..."
              width={220}
            />
          )}

          <div>
            <FieldLabel>LOOKBACK</FieldLabel>
            <input
              type="number"
              className="backtest-input"
              value={lookback}
              min={1}
              max={252}
              onChange={e => setLookback(Math.max(1, Number(e.target.value) || 1))}
              style={{ ...COMPACT_INPUT_STYLE, width: 64 }}
            />
          </div>

          <div>
            <FieldLabel>HORIZON</FieldLabel>
            <div style={{ display: 'flex', gap: 2, flexWrap: 'wrap' }}>
              {HORIZON_OPTIONS.map(h => (
                <button
                  key={h}
                  className={`control-btn ${horizon === h ? 'primary' : ''}`}
                  style={COMPACT_BUTTON_STYLE}
                  onClick={() => setHorizon(h)}
                >
                  {`${h}b`}
                </button>
              ))}
            </div>
          </div>

          <button className="control-btn" onClick={resetAll} style={{ ...COMPACT_BUTTON_STYLE, marginLeft: 'auto' }}>Reset conditions</button>
        </div>
      </SectionCard>

      {/* Filters — grouped collapsible */}
      <SectionCard
        title="ACTIVE FILTERS"
        right={<span style={{ fontSize: 9, color: SOLAR_MUTED }}>Click cells in CURRENT CONTEXT to add or remove filters from the current setup.</span>}
        bodyStyle={{ padding: 0 }}
      >
        <div style={FILTER_EDITOR_ROW_STYLE}>
          <div style={FILTER_EDITOR_HEADER_CELL_STYLE}>Field</div>
          <div style={FILTER_EDITOR_HEADER_CELL_STYLE}>Current</div>
          <div style={FILTER_EDITOR_HEADER_CELL_STYLE}>Mode</div>
          <div style={FILTER_EDITOR_HEADER_CELL_STYLE}>Condition</div>
          <div style={{ ...FILTER_EDITOR_HEADER_CELL_STYLE, borderRight: 0, justifyContent: 'center' }}>x</div>
        </div>
        {selectedStandardSources.length === 0 && activeRotationFilters.length === 0 ? (
          <div style={{ padding: '8px 10px', color: SOLAR_MUTED, fontSize: 10, fontFamily: 'monospace' }}>
            No active filters.
          </div>
        ) : (
          <>
          {selectedStandardSources.map(source => {
            const mode = selectedSourceModes[source.id] ?? source.modeOrder[0]
            const snapshot = formatSourceSnapshot(source.id)
            const triDef = source.triKey
              ? COND_DEFS.find((def): def is TriDef => def.kind === 'tri' && def.key === source.triKey)
              : undefined
            const trendDef = source.trendKey
              ? COND_DEFS.find((def): def is TriDef => def.kind === 'tri' && def.key === source.trendKey)
              : undefined
            const rawDef = source.thresholdKey ? TH_DEFS.find(def => def.key === source.thresholdKey) : undefined
            const pctDef = source.percentileKey ? TH_DEFS.find(def => def.key === source.percentileKey) : undefined

            const renderThresholdEditor = (def: ThDef) => {
              const state = thresholds[def.key]
              return (
                <div style={FILTER_VALUE_GRID_STYLE}>
                  <div style={FILTER_VALUE_CELL_STYLE}>
                    <SheetToggleButton active={!state.active} label="Any" onClick={() => setTh(def.key, { active: false })} />
                  </div>
                  <div style={FILTER_VALUE_CELL_STYLE}>
                    <SheetToggleButton
                      active={state.active}
                      label={state.op}
                      onClick={() => {
                        if (!state.active) setTh(def.key, { active: true })
                        else cycleOp(def.key)
                      }}
                    />
                  </div>
                  <div style={FILTER_VALUE_LAST_CELL_STYLE}>
                    <input
                      type="number"
                      value={toDisp(state.val, def.percent)}
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
                      style={FILTER_CELL_INPUT_STYLE}
                    />
                  </div>
                </div>
              )
            }

            const renderTriEditor = (def: TriDef) => (
              <div style={FILTER_VALUE_GRID_STYLE}>
                <div style={FILTER_VALUE_CELL_STYLE}>
                  <SheetToggleButton active={triConds[def.key] === 'any'} label="Any" onClick={() => setTri(def.key, 'any')} />
                </div>
                <div style={FILTER_VALUE_CELL_STYLE}>
                  <SheetToggleButton active={triConds[def.key] === 'a'} label={def.a.label} onClick={() => setTri(def.key, 'a')} />
                </div>
                <div style={FILTER_VALUE_LAST_CELL_STYLE}>
                  <SheetToggleButton active={triConds[def.key] === 'b'} label={def.b.label} onClick={() => setTri(def.key, 'b')} />
                </div>
              </div>
            )

            return (
              <div key={source.id} style={FILTER_EDITOR_ROW_STYLE}>
                <div style={FILTER_EDITOR_CELL_STYLE}>{source.label}</div>
                <div style={FILTER_EDITOR_CELL_STYLE}>
                  <div>
                    <div>{snapshot.value}</div>
                    {snapshot.sub && <div style={{ fontSize: 8, color: SOLAR_MUTED }}>{snapshot.sub}</div>}
                  </div>
                </div>
                <div style={{ ...FILTER_EDITOR_CELL_STYLE, padding: 0 }}>
                  <div style={{ display: 'grid', gridTemplateColumns: `repeat(${source.modeOrder.length}, minmax(0, 1fr))`, width: '100%' }}>
                    {source.modeOrder.map(entry => (
                      <div key={entry} style={{ borderRight: entry === source.modeOrder[source.modeOrder.length - 1] ? undefined : `1px solid ${SOLAR_BORDER}` }}>
                        <SheetToggleButton active={mode === entry} label={modeLabel(entry)} onClick={() => setSourceMode(source.id, entry)} />
                      </div>
                    ))}
                  </div>
                </div>
                <div style={{ ...FILTER_EDITOR_CELL_STYLE, padding: 0 }}>
                  {mode === 'state' && triDef && renderTriEditor(triDef)}
                  {mode === 'trend' && trendDef && renderTriEditor(trendDef)}
                  {mode === 'value' && rawDef && renderThresholdEditor(rawDef)}
                  {mode === 'percentile' && pctDef && renderThresholdEditor(pctDef)}
                </div>
                <div style={{ ...FILTER_EDITOR_CELL_STYLE, borderRight: 0, justifyContent: 'center', padding: 0 }}>
                  <SheetToggleButton active={false} label="×" onClick={() => toggleSourceSelection(source.id)} title={`Remove ${source.label}`} />
                </div>
              </div>
            )
          })}
          {activeRotationFilters.map(row => {
            const snapshot = formatRotationFilterSnapshot(row)
            const patternLabels = ROT_PATTERN_LABELS[row.type]

            const renderRotationThreshold = () => {
              if (!row.metric) return null
              const defaults = ROT_DEFAULTS[row.metric]
              const state = row.mode === 'value'
                ? rotFilters[row.type].threshold[row.idx][row.metric]
                : rotFilters[row.type].percentile[row.idx][row.metric]
              return (
                <AnyOpValueControl
                  state={state}
                  min={row.mode === 'value' ? defaults.th.min : 0}
                  max={row.mode === 'value' ? defaults.th.max : 1}
                  step={row.mode === 'value' ? defaults.th.step : defaults.pct.step}
                  percent={row.mode === 'value' ? defaults.th.percent : defaults.pct.percent}
                  onSetAny={() => clearActiveRotationFilter(row)}
                  onCycleOrEnable={() => {
                    if (row.mode === 'value') {
                      if (state.active) cycleRotThOp(row.type, row.idx, row.metric)
                      else setRotTh(row.type, row.idx, row.metric, { active: true })
                      return
                    }
                    if (state.active) cycleRotPctOp(row.type, row.idx, row.metric)
                    else setRotPct(row.type, row.idx, row.metric, { active: true })
                  }}
                  onSetValue={(value) => {
                    if (row.mode === 'value') setRotTh(row.type, row.idx, row.metric!, { active: true, val: value })
                    else setRotPct(row.type, row.idx, row.metric!, { active: true, val: value })
                  }}
                />
              )
            }

            const renderRotationPattern = () => {
              if (!row.patternKey) return null
              return (
                <div style={FILTER_VALUE_GRID_STYLE}>
                  <div style={FILTER_VALUE_CELL_STYLE}>
                    <SheetToggleButton active={rotFilters[row.type].pattern[row.patternKey] === 'any'} label="Any" onClick={() => clearActiveRotationFilter(row)} />
                  </div>
                  <div style={FILTER_VALUE_CELL_STYLE}>
                    <SheetToggleButton active={rotFilters[row.type].pattern[row.patternKey] === 'a'} label={patternLabels.a} onClick={() => setRotPattern(row.type, row.patternKey, 'a')} />
                  </div>
                  <div style={FILTER_VALUE_LAST_CELL_STYLE}>
                    <SheetToggleButton active={rotFilters[row.type].pattern[row.patternKey] === 'b'} label={patternLabels.b} onClick={() => setRotPattern(row.type, row.patternKey, 'b')} />
                  </div>
                </div>
              )
            }

            return (
              <div key={row.id} style={FILTER_EDITOR_ROW_STYLE}>
                <div style={FILTER_EDITOR_CELL_STYLE}>{snapshot.label}</div>
                <div style={FILTER_EDITOR_CELL_STYLE}>{snapshot.current}</div>
                <div style={FILTER_EDITOR_CELL_STYLE}>{rotationModeLabel(row.mode)}</div>
                <div style={{ ...FILTER_EDITOR_CELL_STYLE, padding: 0 }}>
                  {row.mode === 'pattern' ? renderRotationPattern() : renderRotationThreshold()}
                </div>
                <div style={{ ...FILTER_EDITOR_CELL_STYLE, borderRight: 0, justifyContent: 'center', padding: 0 }}>
                  <SheetToggleButton active={false} label="x" onClick={() => clearActiveRotationFilter(row)} title={`Remove ${snapshot.label}`} />
                </div>
              </div>
            )
          })}
          </>
        )}
      </SectionCard>

      <div style={{ display: 'none' }}>
      <SectionCard title="ROTATION CONTEXT FILTERS" bodyStyle={{ padding: 0 }}>
        {ROT_TYPES.map(type => {
          const isOpen = openRotType[type]
          const s = rotFilters[type]
          const activeCount = countRotationFilters(s)
          return (
            <div key={type} style={{ borderTop: `1px solid ${SOLAR_BORDER}` }}>
              <div
                onClick={() => setOpenRotType(prev => ({ ...prev, [type]: !prev[type] }))}
                style={GROUP_TOGGLE_STYLE}
              >
                <span style={{ width: 10, color: SOLAR_MUTED }}>{isOpen ? 'v' : '>'}</span>
                <span>{ROT_TYPE_LABELS[type]}</span>
                {activeCount > 0 && (
                  <span style={{ fontSize: 9, color: BLUE, marginLeft: 3 }}>({activeCount} active)</span>
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
                  <AnyOpValueControl
                    state={th}
                    min={dmin}
                    max={dmax}
                    step={dstep}
                    percent={dpercent}
                    onSetAny={onToggleAny}
                    onCycleOrEnable={onCycleOp}
                    onSetValue={onVal}
                  />
                )

                return (
                  <div style={{ overflowX: 'auto' }}>
                    <div style={{ padding: GROUP_BODY_PADDING, display: 'grid',
                                  gridTemplateColumns: `90px repeat(4, ${COND_COLUMN_WIDTH}px)`,
                                  gap: '3px 6px', alignItems: 'center', minWidth: 'fit-content' }}>
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
                          <div style={{ fontSize: 10, color: SOLAR_TEXT, fontFamily: 'monospace' }}>{m}</div>
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
                          <div style={{ fontSize: 10, color: SOLAR_MUTED, fontFamily: 'monospace' }}
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
                    <div style={{ fontSize: 10, color: SOLAR_TEXT, fontFamily: 'monospace' }} title={lbl.title}>
                      Pattern vs prior
                    </div>
                    {ROT_INDICES.map(i => {
                      const key = PATTERN_KEY_FOR_IDX[i]
                      if (!key) return <div key={`pattern-${i}`} />
                      const p = s.pattern[key]
                      const a_idx = key.split('_')[0], b_idx = key.split('_')[1]
                      return (
                        <div key={`pattern-${i}`} style={{ display: 'flex', gap: 3 }}>
                          <div style={ROT_CELL}>
                            <SheetToggleButton active={p === 'any'} label="Any" onClick={() => setRotPattern(type, key, 'any')} />
                          </div>
                          <div style={ROT_CELL}>
                            <SheetToggleButton
                              active={p === 'a'}
                              label={lbl.a}
                              title={`Rotation ${a_idx} is a ${lbl.a} vs rotation ${b_idx}`}
                              onClick={() => setRotPattern(type, key, 'a')}
                            />
                          </div>
                          <div style={ROT_CELL}>
                            <SheetToggleButton
                              active={p === 'b'}
                              label={lbl.b}
                              title={`Rotation ${a_idx} is a ${lbl.b} vs rotation ${b_idx}`}
                              onClick={() => setRotPattern(type, key, 'b')}
                            />
                          </div>
                        </div>
                      )
                    })}
                    </div>
                  </div>
                )
              })()}
            </div>
          )
        })}
      </SectionCard>
      </div>

      {data?.current_context && (() => {
        const cc = data.current_context!
        const ind = cc.indicators
        const num = (v: number | null | undefined, d = 2) => v == null ? '--' : v.toFixed(d)
        const pctD = (v: number | null | undefined, d = 1) => v == null ? '--' : (v * 100).toFixed(d) + '%'
        const signed = (v: number | null | undefined, d = 2, suffix = '') => {
          if (v == null) return '--'
          const prefix = v > 0 ? '+' : ''
          return `${prefix}${v.toFixed(d)}${suffix}`
        }
        const patternColor = (p: string | null | undefined) =>
          p === 'HH' || p === 'HL' || p === 'HH/HL'
            ? GREEN
            : p === 'LH' || p === 'LL' || p === 'LH/LL'
              ? RED
              : SOLAR_MUTED
        const activeBar = cc.active_rotation === 'up' ? cc.rotations.UpRot.Time_0
          : cc.active_rotation === 'down' ? cc.rotations.DownRot.Time_0 : null
        const renderSheetValue = ({
          main,
          sub,
          active = false,
          onClick,
          color,
          align = 'center',
        }: {
          main: string
          sub?: string
          active?: boolean
          onClick?: () => void
          color?: string
          align?: 'left' | 'center' | 'right'
        }) => {
          const alignItems = align === 'right' ? 'flex-end' : align === 'center' ? 'center' : 'flex-start'
          const justifyContent = align === 'right' ? 'flex-end' : align === 'center' ? 'center' : 'flex-start'
          const body = (
            <div style={{ display: 'flex', flexDirection: 'column', alignItems, justifyContent, gap: 0, lineHeight: 1.05 }}>
              <span style={{ color: color ?? (active ? BLUE : SOLAR_TEXT), fontWeight: 700 }}>{main}</span>
              {sub && <span style={{ fontSize: 7, color: SOLAR_MUTED }}>{sub}</span>}
            </div>
          )

          if (onClick) {
            return (
              <button
                type="button"
                onClick={onClick}
                style={{
                  ...SHEET_BUTTON_CELL_STYLE,
                  background: active ? 'rgba(50, 50, 255, 0.08)' : 'transparent',
                  textAlign: align,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent,
                }}
              >
                {body}
              </button>
            )
          }

          return (
            <div style={{ minHeight: 18, display: 'flex', alignItems: 'center', justifyContent }}>
              {body}
            </div>
          )
        }

        const renderOverviewRow = ({
          label,
          value,
          valueActive = false,
          onValueClick,
          delta,
          deltaActive = false,
          onDeltaClick,
          context,
          contextActive = false,
          onContextClick,
          contextColor,
          contextAlign = 'center',
        }: {
          label: string
          value: string
          valueActive?: boolean
          onValueClick?: () => void
          delta: string
          deltaActive?: boolean
          onDeltaClick?: () => void
          context: string
          contextActive?: boolean
          onContextClick?: () => void
          contextColor?: string
          contextAlign?: 'left' | 'center' | 'right'
        }) => (
          <tr key={label}>
            <td style={SHEET_ROW_LABEL_STYLE}>{label}</td>
            <td style={SHEET_CELL_STYLE}>
              {renderSheetValue({ main: value, active: valueActive, onClick: onValueClick, align: 'center' })}
            </td>
            <td style={SHEET_CELL_STYLE}>
              {renderSheetValue({ main: delta, active: deltaActive, onClick: onDeltaClick, align: 'center' })}
            </td>
            <td style={SHEET_CELL_STYLE}>
              {renderSheetValue({ main: context, active: contextActive, onClick: onContextClick, color: contextColor, align: contextAlign })}
            </td>
          </tr>
        )

        const renderRotationHistory = (title: string, type: RotType) => {
          const ctx = cc.rotations[type]
          const statusLabel = ctx.active ? 'ACTIVE' : 'INACTIVE'
          const statusColor = ctx.active ? BLUE : SOLAR_MUTED

          return (
            <div style={{ borderTop: `1px solid ${SHEET_GRID_BORDER}`, overflowX: 'auto' }}>
              <div style={{ padding: '3px 5px', color: SOLAR_MUTED, fontSize: 7, letterSpacing: 0.25 }}>{title}</div>
              <table style={SHEET_TABLE_STYLE}>
                <colgroup>
                  <col style={{ width: 88 }} />
                  <col style={{ width: 92 }} />
                  <col style={{ width: 72 }} />
                  <col style={{ width: 78 }} />
                  <col style={{ width: 78 }} />
                  <col style={{ width: 82 }} />
                  <col style={{ width: 72 }} />
                  <col style={{ width: 72 }} />
                  <col style={{ width: 72 }} />
                </colgroup>
                <thead>
                  <tr>
                    <th style={SHEET_HEADER_CELL_STYLE}>Row</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>Status</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>Time</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>Range</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>Change</th>
                    <th style={{ ...SHEET_HEADER_CELL_STYLE, textAlign: 'center' }}>H/L Rel</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>T %ile</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>R %ile</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>C %ile</th>
                  </tr>
                </thead>
                <tbody>
                  {ROT_INDICES.map(idx => {
                    const timeValue = getRotationMetricValue(ctx, idx, 'Time')
                    const rangeValue = getRotationMetricValue(ctx, idx, 'Range')
                    const changeValue = getRotationMetricValue(ctx, idx, 'Change')
                    const timePct = getRotationMetricPercentile(ctx, idx, 'Time')
                    const rangePct = getRotationMetricPercentile(ctx, idx, 'Range')
                    const changePct = getRotationMetricPercentile(ctx, idx, 'Change')
                    const patternKey = PATTERN_KEY_FOR_IDX[idx]
                    const pattern = patternKey ? getRotationPatternDisplay(ctx, patternKey) : null
                    const patternFilterValue = patternKey ? ctx.patterns?.[patternKey] ?? null : null

                    return (
                      <tr key={`${title}-${idx}`} style={{ background: idx === 0 ? 'rgba(50, 50, 255, 0.04)' : 'transparent' }}>
                        <td style={SHEET_ROW_LABEL_STYLE}>{rotationIndexLabel(idx)}</td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: idx === 0 ? statusLabel : 'Prior',
                            active: idx === 0 && ctx.active,
                            color: idx === 0 ? statusColor : SOLAR_MUTED,
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: timeValue == null ? '--' : timeValue.toFixed(0),
                            active: rotFilters[type].threshold[idx].Time.active,
                            onClick: () => toggleRotMetricSelection(type, idx, 'Time', 'value', timeValue),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(rangeValue),
                            active: rotFilters[type].threshold[idx].Range.active,
                            onClick: () => toggleRotMetricSelection(type, idx, 'Range', 'value', rangeValue),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(changeValue),
                            active: rotFilters[type].threshold[idx].Change.active,
                            onClick: () => toggleRotMetricSelection(type, idx, 'Change', 'value', changeValue),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pattern ?? '--',
                            active: !!patternKey && rotFilters[type].pattern[patternKey] !== 'any',
                            onClick: patternKey ? () => toggleRotPatternSelection(type, patternKey, patternFilterValue) : undefined,
                            color: patternColor(patternFilterValue),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(timePct),
                            active: rotFilters[type].percentile[idx].Time.active,
                            onClick: () => toggleRotMetricSelection(type, idx, 'Time', 'percentile', timePct),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(rangePct),
                            active: rotFilters[type].percentile[idx].Range.active,
                            onClick: () => toggleRotMetricSelection(type, idx, 'Range', 'percentile', rangePct),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(changePct),
                            active: rotFilters[type].percentile[idx].Change.active,
                            onClick: () => toggleRotMetricSelection(type, idx, 'Change', 'percentile', changePct),
                            align: 'center',
                          })}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )
        }

        return (
          <SectionCard
            title="CURRENT CONTEXT"
            right={
              <span style={{ fontSize: 9, color: SOLAR_MUTED, fontFamily: 'monospace' }}>
                {data.ticker} | {cc.date ?? '--'} | Close {num(cc.close)}{activeBar != null ? ` | bar ${activeBar.toFixed(0)}` : ''}
              </span>
            }
            bodyStyle={{ padding: 0 }}
          >
            <div style={{ padding: '3px 5px', color: SOLAR_MUTED, fontSize: 7, borderBottom: `1px solid ${SHEET_GRID_BORDER}`, letterSpacing: 0.25 }}>
              CLICK THE DISPLAYED CELLS TO BUILD ACTIVE FILTERS FROM THE CURRENT SETUP
            </div>
            <div style={{ overflowX: 'auto' }}>
              <div style={{ padding: '3px 5px', color: SOLAR_MUTED, fontSize: 7, letterSpacing: 0.25 }}>SEQUENCE STATUS</div>
              <table style={SHEET_TABLE_STYLE}>
                <colgroup>
                  <col style={{ width: 128 }} />
                  <col style={{ width: 84 }} />
                  <col style={{ width: 72 }} />
                  <col style={{ width: 78 }} />
                  <col style={{ width: 78 }} />
                  <col style={{ width: 82 }} />
                  <col style={{ width: 72 }} />
                  <col style={{ width: 72 }} />
                  <col style={{ width: 72 }} />
                </colgroup>
                <thead>
                  <tr>
                    <th style={SHEET_HEADER_CELL_STYLE}>Type</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>Status</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>Time</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>Range</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>Change</th>
                    <th style={{ ...SHEET_HEADER_CELL_STYLE, textAlign: 'center' }}>H/L Rel</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>T %ile</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>R %ile</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>C %ile</th>
                  </tr>
                </thead>
                <tbody>
                  {(['Breakout', 'Breakdown', 'UpRot', 'DownRot'] as const).map(type => {
                    const ctx = cc.rotations[type]
                    const pattern = getRotationPatternDisplay(ctx, '0_1')
                    const patternFilterValue = ctx.patterns?.['0_1'] ?? null
                    return (
                      <tr key={`summary-${type}`} style={{ background: ctx.active ? 'rgba(50, 50, 255, 0.04)' : 'transparent' }}>
                        <td style={SHEET_ROW_LABEL_STYLE}>{ROT_TYPE_LABELS[type]}</td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: ctx.active ? 'ACTIVE' : 'INACTIVE',
                            color: ctx.active ? BLUE : SOLAR_MUTED,
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: ctx.Time_0 == null ? '--' : ctx.Time_0.toFixed(0),
                            active: rotFilters[type].threshold[0].Time.active,
                            onClick: () => toggleRotMetricSelection(type, 0, 'Time', 'value', ctx.Time_0),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(ctx.Range_0),
                            active: rotFilters[type].threshold[0].Range.active,
                            onClick: () => toggleRotMetricSelection(type, 0, 'Range', 'value', ctx.Range_0),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(ctx.Change_0),
                            active: rotFilters[type].threshold[0].Change.active,
                            onClick: () => toggleRotMetricSelection(type, 0, 'Change', 'value', ctx.Change_0),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pattern ?? '--',
                            active: rotFilters[type].pattern['0_1'] !== 'any',
                            onClick: () => toggleRotPatternSelection(type, '0_1', patternFilterValue),
                            color: patternColor(patternFilterValue),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(ctx.Time_0_Pct),
                            active: rotFilters[type].percentile[0].Time.active,
                            onClick: () => toggleRotMetricSelection(type, 0, 'Time', 'percentile', ctx.Time_0_Pct),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(ctx.Range_0_Pct),
                            active: rotFilters[type].percentile[0].Range.active,
                            onClick: () => toggleRotMetricSelection(type, 0, 'Range', 'percentile', ctx.Range_0_Pct),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(ctx.Change_0_Pct),
                            active: rotFilters[type].percentile[0].Change.active,
                            onClick: () => toggleRotMetricSelection(type, 0, 'Change', 'percentile', ctx.Change_0_Pct),
                            align: 'center',
                          })}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
            <div style={{ overflowX: 'auto', borderTop: `1px solid ${SHEET_GRID_BORDER}` }}>
              <div style={{ padding: '3px 5px', color: SOLAR_MUTED, fontSize: 7, letterSpacing: 0.25 }}>CURRENT SNAPSHOT</div>
              <table style={SHEET_TABLE_STYLE}>
                <colgroup>
                  <col style={{ width: 132 }} />
                  <col style={{ width: 112 }} />
                  <col style={{ width: 112 }} />
                  <col style={{ width: 176 }} />
                </colgroup>
                <thead>
                  <tr>
                    <th style={SHEET_HEADER_CELL_STYLE}>Metric</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>Current</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>{`Rel ${lookback}`}</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>Selection</th>
                  </tr>
                </thead>
                <tbody>
                  {renderOverviewRow({
                    label: 'Upper Target',
                    value: num(cc.upper_target),
                    delta: '--',
                    context: formatSourceSnapshot('loc_upper').value,
                    contextActive: selectedSourceIds.includes('loc_upper') && (selectedSourceModes.loc_upper ?? sourceConfigMap.loc_upper.modeOrder[0]) === 'state',
                    onContextClick: () => toggleSourceModeSelection('loc_upper', 'state'),
                    contextColor: BLUE,
                    contextAlign: 'center',
                  })}
                  {renderOverviewRow({
                    label: 'Lower Target',
                    value: num(cc.lower_target),
                    delta: '--',
                    context: formatSourceSnapshot('loc_lower').value,
                    contextActive: selectedSourceIds.includes('loc_lower') && (selectedSourceModes.loc_lower ?? sourceConfigMap.loc_lower.modeOrder[0]) === 'state',
                    onContextClick: () => toggleSourceModeSelection('loc_lower', 'state'),
                    contextColor: BLUE,
                    contextAlign: 'center',
                  })}
                  {renderOverviewRow({
                    label: 'RV (ann)',
                    value: `${num(ind.rv_ann, 1)}%`,
                    valueActive: selectedSourceIds.includes('rv') && (selectedSourceModes.rv ?? sourceConfigMap.rv.modeOrder[0]) === 'value',
                    onValueClick: () => toggleSourceModeSelection('rv', 'value'),
                    delta: signed(ind.rv_delta, 1, 'pp'),
                    deltaActive: selectedSourceIds.includes('rv') && (selectedSourceModes.rv ?? sourceConfigMap.rv.modeOrder[0]) === 'trend',
                    onDeltaClick: () => toggleSourceModeSelection('rv', 'trend'),
                    context: `%ile ${pctD(ind.rv_pct)}`,
                    contextActive: selectedSourceIds.includes('rv') && (selectedSourceModes.rv ?? sourceConfigMap.rv.modeOrder[0]) === 'percentile',
                    onContextClick: () => toggleSourceModeSelection('rv', 'percentile'),
                  })}
                  {renderOverviewRow({
                    label: 'H React',
                    value: num(ind.h_react, 2),
                    valueActive: selectedSourceIds.includes('h') && (selectedSourceModes.h ?? sourceConfigMap.h.modeOrder[0]) === 'value',
                    onValueClick: () => toggleSourceModeSelection('h', 'value'),
                    delta: signed(ind.h_delta, 2),
                    deltaActive: selectedSourceIds.includes('h') && (selectedSourceModes.h ?? sourceConfigMap.h.modeOrder[0]) === 'trend',
                    onDeltaClick: () => toggleSourceModeSelection('h', 'trend'),
                    context: `%ile ${pctD(ind.h_pct)}`,
                    contextActive: selectedSourceIds.includes('h') && (selectedSourceModes.h ?? sourceConfigMap.h.modeOrder[0]) === 'percentile',
                    onContextClick: () => toggleSourceModeSelection('h', 'percentile'),
                  })}
                  {renderOverviewRow({
                    label: 'Price Change',
                    value: num(ind.p_react, 2),
                    valueActive: selectedSourceIds.includes('p') && (selectedSourceModes.p ?? sourceConfigMap.p.modeOrder[0]) === 'value',
                    onValueClick: () => toggleSourceModeSelection('p', 'value'),
                    delta: signed(ind.p_delta, 2),
                    deltaActive: selectedSourceIds.includes('p') && (selectedSourceModes.p ?? sourceConfigMap.p.modeOrder[0]) === 'trend',
                    onDeltaClick: () => toggleSourceModeSelection('p', 'trend'),
                    context: `%ile ${pctD(ind.p_pct)}`,
                    contextActive: selectedSourceIds.includes('p') && (selectedSourceModes.p ?? sourceConfigMap.p.modeOrder[0]) === 'percentile',
                    onContextClick: () => toggleSourceModeSelection('p', 'percentile'),
                  })}
                  {renderOverviewRow({
                    label: 'L React',
                    value: num(ind.l_react, 2),
                    valueActive: selectedSourceIds.includes('l') && (selectedSourceModes.l ?? sourceConfigMap.l.modeOrder[0]) === 'value',
                    onValueClick: () => toggleSourceModeSelection('l', 'value'),
                    delta: signed(ind.l_delta, 2),
                    deltaActive: selectedSourceIds.includes('l') && (selectedSourceModes.l ?? sourceConfigMap.l.modeOrder[0]) === 'trend',
                    onDeltaClick: () => toggleSourceModeSelection('l', 'trend'),
                    context: `%ile ${pctD(ind.l_pct)}`,
                    contextActive: selectedSourceIds.includes('l') && (selectedSourceModes.l ?? sourceConfigMap.l.modeOrder[0]) === 'percentile',
                    onContextClick: () => toggleSourceModeSelection('l', 'percentile'),
                  })}
                  {isBasket && renderOverviewRow({
                    label: 'Breadth',
                    value: `${num(ind.breadth, 1)}%`,
                    valueActive: selectedSourceIds.includes('breadth') && (selectedSourceModes.breadth ?? sourceConfigMap.breadth.modeOrder[0]) === 'value',
                    onValueClick: () => toggleSourceModeSelection('breadth', 'value'),
                    delta: signed(ind.breadth_delta, 1),
                    deltaActive: selectedSourceIds.includes('breadth') && (selectedSourceModes.breadth ?? sourceConfigMap.breadth.modeOrder[0]) === 'trend',
                    onDeltaClick: () => toggleSourceModeSelection('breadth', 'trend'),
                    context: '--',
                  })}
                  {isBasket && renderOverviewRow({
                    label: 'Breakout %',
                    value: `${num(ind.breakout_pct, 1)}%`,
                    valueActive: selectedSourceIds.includes('breakout_pct') && (selectedSourceModes.breakout_pct ?? sourceConfigMap.breakout_pct.modeOrder[0]) === 'value',
                    onValueClick: () => toggleSourceModeSelection('breakout_pct', 'value'),
                    delta: signed(ind.breakout_delta, 1),
                    deltaActive: selectedSourceIds.includes('breakout_pct') && (selectedSourceModes.breakout_pct ?? sourceConfigMap.breakout_pct.modeOrder[0]) === 'trend',
                    onDeltaClick: () => toggleSourceModeSelection('breakout_pct', 'trend'),
                    context: '--',
                  })}
                  {isBasket && renderOverviewRow({
                    label: 'Corr %',
                    value: `${num(ind.corr_pct, 1)}%`,
                    valueActive: selectedSourceIds.includes('corr') && (selectedSourceModes.corr ?? sourceConfigMap.corr.modeOrder[0]) === 'value',
                    onValueClick: () => toggleSourceModeSelection('corr', 'value'),
                    delta: signed(ind.corr_delta, 1),
                    deltaActive: selectedSourceIds.includes('corr') && (selectedSourceModes.corr ?? sourceConfigMap.corr.modeOrder[0]) === 'trend',
                    onDeltaClick: () => toggleSourceModeSelection('corr', 'trend'),
                    context: '--',
                  })}
                </tbody>
              </table>
            </div>
            {renderRotationHistory('UP ROTATION', 'UpRot')}
            {renderRotationHistory('DOWN ROTATION', 'DownRot')}
          </SectionCard>
        )
      })()}
      <div style={{ display: 'none' }}>
      {/* CURRENT CONTEXT card (Phase 5) */}
      {data?.current_context && (() => {
        const cc = data.current_context!
        const num = (v: number | null | undefined, d = 2) => v == null ? '--' : v.toFixed(d)
        const pctD = (v: number | null | undefined) => v == null ? '--' : (v * 100).toFixed(1) + '%'
        const types: Array<keyof typeof cc.rotations> = ['UpRot', 'DownRot', 'Breakout', 'Breakdown']
        const patternColor = (p: string | null) =>
          p === 'HH' || p === 'HL' || p === 'HH/HL'
            ? GREEN
            : p === 'LH' || p === 'LL' || p === 'LH/LL'
              ? RED
              : SOLAR_MUTED
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

        return (
          <div style={CARD_STYLE}>
            {/* Header */}
            <div style={{ padding: '5px 8px', borderBottom: `1px solid ${SOLAR_BORDER}`, display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', gap: '4px 10px', fontSize: 10, fontFamily: 'monospace' }}>
              <span style={{ color: SOLAR_MUTED, fontSize: 9 }}>CURRENT CONTEXT</span>
              <span style={{ color: SOLAR_TEXT, fontWeight: 700 }}>{data.ticker}</span>
              <span style={{ color: SOLAR_MUTED }}>{cc.date ?? '--'}</span>
              <span style={{ color: SOLAR_TEXT }}>Close {num(cc.close)}</span>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px 8px', color: SOLAR_MUTED, fontSize: 9 }}>
                <span style={{ color: BLUE, fontWeight: 700 }}>{rotLabel}</span>
                {activeBar != null && <span style={{ color: SOLAR_TEXT }}> · bar {activeBar}</span>}
                {' · '}
                <span style={{ color: BLUE, fontWeight: 700 }}>{regLabel}</span>
                {' · '}
                <span style={{ color: positionColor, fontWeight: 700 }}>{positionLabel}</span>
              </div>
            </div>

            {/* Indicators strip */}
            <div style={{ borderBottom: `1px solid ${SOLAR_BORDER}` }}>
              <div style={{ padding: '4px 6px', color: SOLAR_MUTED, fontSize: 8, borderBottom: `1px solid ${SOLAR_BORDER}`, letterSpacing: 0.3 }}>
                CLICK A CELL TO ADD OR REMOVE A FILTER BASED ON THE CURRENT SETUP
              </div>
              <div style={CONTEXT_GRID_STYLE}>
                <ContextMetricCell label="LT REGIME" value={formatSourceSnapshot('regime').value} active={selectedSourceIds.includes('regime')} onClick={() => toggleSourceSelection('regime')} />
                <ContextMetricCell label="ROTATION" value={formatSourceSnapshot('rotation').value} active={selectedSourceIds.includes('rotation')} onClick={() => toggleSourceSelection('rotation')} />
                <ContextMetricCell label="VS UPPER" value={formatSourceSnapshot('loc_upper').value} sub={formatSourceSnapshot('loc_upper').sub} active={selectedSourceIds.includes('loc_upper')} onClick={() => toggleSourceSelection('loc_upper')} />
                <ContextMetricCell label="VS LOWER" value={formatSourceSnapshot('loc_lower').value} sub={formatSourceSnapshot('loc_lower').sub} active={selectedSourceIds.includes('loc_lower')} onClick={() => toggleSourceSelection('loc_lower')} />
                <ContextMetricCell label="RV (ANN)" value={num(ind.rv_ann, 1) + '%'} sub={'%ile ' + pctD(ind.rv_pct)} active={selectedSourceIds.includes('rv')} onClick={() => toggleSourceSelection('rv')} />
                <ContextMetricCell label="H REACT" value={num(ind.h_react, 2)} sub={'%ile ' + pctD(ind.h_pct)} active={selectedSourceIds.includes('h')} onClick={() => toggleSourceSelection('h')} />
                <ContextMetricCell label="PRICE CHG" value={num(ind.p_react, 2)} sub={'%ile ' + pctD(ind.p_pct)} active={selectedSourceIds.includes('p')} onClick={() => toggleSourceSelection('p')} />
                <ContextMetricCell label="L REACT" value={num(ind.l_react, 2)} sub={'%ile ' + pctD(ind.l_pct)} active={selectedSourceIds.includes('l')} onClick={() => toggleSourceSelection('l')} />
                <ContextMetricCell label="UPPER TGT" value={num(cc.upper_target)} />
                <ContextMetricCell label="LOWER TGT" value={num(cc.lower_target)} />
                {isBasket && <ContextMetricCell label="BREADTH" value={num(ind.breadth, 1) + '%'} active={selectedSourceIds.includes('breadth')} onClick={() => toggleSourceSelection('breadth')} />}
                {isBasket && <ContextMetricCell label="BREAKOUT %" value={num(ind.breakout_pct, 1) + '%'} active={selectedSourceIds.includes('breakout_pct')} onClick={() => toggleSourceSelection('breakout_pct')} />}
                {isBasket && <ContextMetricCell label="CORR %" value={num(ind.corr_pct, 1) + '%'} active={selectedSourceIds.includes('corr')} onClick={() => toggleSourceSelection('corr')} />}
              </div>
            </div>

            {/* Rotation context table */}
            <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', minWidth: 720, borderCollapse: 'collapse', color: SOLAR_TEXT, fontSize: 10, fontFamily: 'monospace' }}>
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
                      <td style={{ padding: '2px 6px', textAlign: 'center', borderRight: `1px solid ${SOLAR_BORDER}`, color: patternColor(c.pattern_display ?? c.pattern), fontWeight: 700 }}>{c.pattern_display ?? c.pattern ?? '—'}</td>
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
          </div>
        )
      })()}
      </div>

      {/* Chart */}
      <SectionCard title="RETURN DISTRIBUTION" bodyStyle={{ padding: 0 }}>
        <div ref={wrapRef} style={{ width: '100%' }}>
          <canvas ref={canvasRef} style={{ width: dims.w, height: dims.h, display: 'block' }} />
        </div>
      </SectionCard>

      {/* Stats table */}
      <SectionCard title="DISTRIBUTION STATS" bodyStyle={{ padding: 0 }}>
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(9, 1fr)', gap: 0, fontSize: 11, fontFamily: 'monospace' }}>
          {['Sample','N','Mean','Median','Stddev','Win%','p5','p25 / p75','p95'].map(h =>
            <div key={h} style={{ padding: '4px 6px', color: SOLAR_MUTED, borderBottom: `1px solid ${SOLAR_BORDER}` }}>{h}</div>
          )}
          <div style={{ padding: '4px 6px', color: PINK }}>Baseline</div>
          <div style={{ padding: '4px 6px' }}>{baseSt?.n ?? '--'}</div>
          <div style={{ padding: '4px 6px' }}>{pct(baseSt?.mean ?? null)}</div>
          <div style={{ padding: '4px 6px' }}>{pct(baseSt?.median ?? null)}</div>
          <div style={{ padding: '4px 6px' }}>{pct(baseSt?.stddev ?? null)}</div>
          <div style={{ padding: '4px 6px' }}>{pct(baseSt?.win_rate ?? null, 1)}</div>
          <div style={{ padding: '4px 6px' }}>{pct(baseSt?.p5 ?? null)}</div>
          <div style={{ padding: '4px 6px' }}>{pct(baseSt?.p25 ?? null)} / {pct(baseSt?.p75 ?? null)}</div>
          <div style={{ padding: '4px 6px' }}>{pct(baseSt?.p95 ?? null)}</div>

          <div style={{ padding: '4px 6px', color: BLUE, borderTop: `1px solid ${SOLAR_BORDER}` }}>Filtered</div>
          <div style={{ padding: '4px 6px', borderTop: `1px solid ${SOLAR_BORDER}`, fontWeight: stats && stats.n > 0 && stats.n < 30 ? 'bold' : 'normal', color: stats && stats.n > 0 && stats.n < 30 ? PINK : undefined }}>{stats?.n ?? '--'}</div>
          <div style={{ padding: '4px 6px', borderTop: `1px solid ${SOLAR_BORDER}` }}>{pct(stats?.mean ?? null)}</div>
          <div style={{ padding: '4px 6px', borderTop: `1px solid ${SOLAR_BORDER}` }}>{pct(stats?.median ?? null)}</div>
          <div style={{ padding: '4px 6px', borderTop: `1px solid ${SOLAR_BORDER}` }}>{pct(stats?.stddev ?? null)}</div>
          <div style={{ padding: '4px 6px', borderTop: `1px solid ${SOLAR_BORDER}` }}>{pct(stats?.win_rate ?? null, 1)}</div>
          <div style={{ padding: '4px 6px', borderTop: `1px solid ${SOLAR_BORDER}` }}>{pct(stats?.p5 ?? null)}</div>
          <div style={{ padding: '4px 6px', borderTop: `1px solid ${SOLAR_BORDER}` }}>{pct(stats?.p25 ?? null)} / {pct(stats?.p75 ?? null)}</div>
          <div style={{ padding: '4px 6px', borderTop: `1px solid ${SOLAR_BORDER}` }}>{pct(stats?.p95 ?? null)}</div>
        </div>
      </SectionCard>

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
                if (!forwardTerciles || sortedForwardEntries.length === 0) {
                  return <div style={{ padding: 8, color: SOLAR_MUTED, fontSize: 10 }}>No matches</div>
                }
                return sortedForwardEntries.map(({ p, i }) => {
                  const tercile = forwardTerciles.tercileOfOriginal.get(i)
                  const color = tercile === 'top' ? GREEN : tercile === 'bottom' ? RED : GRAY_T
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
