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
  patterns: Record<'0_1' | '1_2' | '2_3' | '3_4', 'HH' | 'LH' | 'HL' | 'LL' | null>
  patterns_display?: Record<'0_1' | '1_2' | '2_3' | '3_4', string | null>
  Start_0: string | null; End_0: string | null
  Time_0: number | null;  Range_0: number | null;  Change_0: number | null
  Time_0_Pct: number | null;  Range_0_Pct: number | null;  Change_0_Pct: number | null
  Start_1: string | null; End_1: string | null
  Time_1: number | null;  Range_1: number | null;  Change_1: number | null
  Time_1_Pct: number | null;  Range_1_Pct: number | null;  Change_1_Pct: number | null
  Start_2: string | null; End_2: string | null
  Time_2: number | null;  Range_2: number | null;  Change_2: number | null
  Time_2_Pct: number | null;  Range_2_Pct: number | null;  Change_2_Pct: number | null
  Start_3: string | null; End_3: string | null
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
  rotation_signal: 'up' | 'down' | null
  regime_signal: 'breakout' | 'breakdown' | null
  mr_signal: 'btfd' | 'stfr' | null
  price_events: {
    cross_uprot_high: boolean
    cross_breakout_high: boolean
    cross_prev_high: boolean
    new_high_21: boolean
    new_high_63: boolean
    new_high_252: boolean
    cross_downrot_low: boolean
    cross_breakdown_low: boolean
    cross_prev_low: boolean
    new_low_21: boolean
    new_low_63: boolean
    new_low_252: boolean
  }
  indicators: {
    rv_ann: number | null;       rv_pct: number | null; rv_delta: number | null; rv_delta_pct: number | null
    h_react: number | null;      h_pct: number | null; h_delta: number | null; h_delta_pct: number | null
    l_react: number | null;      l_pct: number | null; l_delta: number | null; l_delta_pct: number | null
    p_react: number | null;      p_pct: number | null; p_delta: number | null; p_delta_pct: number | null
    breadth: number | null; breadth_pct: number | null; breadth_delta: number | null; breadth_delta_pct: number | null
    breakout_pct: number | null; breakout_level_pct: number | null; breakout_delta: number | null; breakout_delta_pct: number | null
    corr_pct: number | null; corr_level_pct: number | null; corr_delta: number | null; corr_delta_pct: number | null
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
type Op = '>' | '<' | '>=' | '<=' | 'â†‘' | 'â†“'
const OPS: Op[] = ['>', '<', '>=', '<=', 'â†‘', 'â†“']

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

const parseRgb = (rgb: string) => {
  const m = rgb.match(/rgb\(\s*(\d+),\s*(\d+),\s*(\d+)\s*\)/i)
  return m ? { r: Number(m[1]), g: Number(m[2]), b: Number(m[3]) } : { r: 88, g: 110, b: 117 }
}

const mixRgb = (from: string, to: string, t: number) => {
  const a = parseRgb(from)
  const b = parseRgb(to)
  const s = Math.max(0, Math.min(1, t))
  return `rgb(${Math.round(a.r + (b.r - a.r) * s)}, ${Math.round(a.g + (b.g - a.g) * s)}, ${Math.round(a.b + (b.b - a.b) * s)})`
}

const GRAY_TEXT = 'rgb(127, 141, 146)'

const grayCenteredColor = (score: number) => {
  const clamped = Math.max(-1, Math.min(1, score))
  if (clamped >= 0) return mixRgb(GRAY_TEXT, BLUE, clamped)
  return mixRgb(GRAY_TEXT, PINK, -clamped)
}

const relativeScore = (value: number | null | undefined, series: Array<number | null | undefined>) => {
  if (value == null || !Number.isFinite(value)) return null
  const finite = series.filter((v): v is number => v != null && Number.isFinite(v))
  if (finite.length < 2) return 0
  const min = Math.min(...finite)
  const max = Math.max(...finite)
  if (max === min) return 0
  const center = (min + max) / 2
  return Math.max(-1, Math.min(1, (value - center) / ((max - min) / 2)))
}


// Convert internal decimal state (e.g. 0.80) to display scale (80) when the field
// is a percentage. Backend contract stays in decimals â€” only the UI shows percent.
const toDisp   = (v: number, percent?: boolean) => percent ? v * 100 : v
const fromDisp = (v: number, percent?: boolean) => percent ? v / 100 : v
const toDispRounded = (v: number, percent?: boolean) => Number(toDisp(v, percent).toFixed(2))

// --- Condition definitions ---
interface TriDef  { kind: 'tri';  key: string; label: string; a: { value: string; label: string }; b: { value: string; label: string }; basketOnly?: boolean; tickerOnly?: boolean }
interface ThDef   { kind: 'th';   key: string; label: string; opParam: string; valParam: string; defaultVal: number; min: number; max: number; step: number; basketOnly?: boolean; tickerOnly?: boolean; percent?: boolean }
type CondDef = TriDef | ThDef

const COND_DEFS: CondDef[] = [
  { kind: 'tri', key: 'breakout_state', label: 'LT Regime',      a: { value: 'breakout', label: 'Breakout' }, b: { value: 'breakdown', label: 'Breakdown' } },
  { kind: 'tri', key: 'breakout_signal', label: 'LT Signal',      a: { value: 'breakout', label: 'Breakout' }, b: { value: 'breakdown', label: 'Breakdown' } },
  { kind: 'tri', key: 'rotation_state', label: 'Rotation',        a: { value: 'up', label: 'Up' },            b: { value: 'down', label: 'Down' } },
  { kind: 'tri', key: 'rotation_signal', label: 'Rotation Signal', a: { value: 'up', label: 'Up' },            b: { value: 'down', label: 'Down' } },
  { kind: 'tri', key: 'mr_signal', label: 'MR Signal', a: { value: 'btfd', label: 'BTFD' }, b: { value: 'stfr', label: 'STFR' }, tickerOnly: true },
  { kind: 'tri', key: 'cross_uprot_high', label: 'Cross > Prev Up Rotation High', a: { value: 'triggered', label: 'Triggered' }, b: { value: 'not_triggered', label: 'Not Triggered' } },
  { kind: 'tri', key: 'cross_breakout_high', label: 'Cross > Prev Breakout High', a: { value: 'triggered', label: 'Triggered' }, b: { value: 'not_triggered', label: 'Not Triggered' } },
  { kind: 'tri', key: 'cross_prev_high', label: 'Cross > Prev Candle High', a: { value: 'triggered', label: 'Triggered' }, b: { value: 'not_triggered', label: 'Not Triggered' } },
  { kind: 'tri', key: 'new_high_21', label: 'New 21-Bar High', a: { value: 'triggered', label: 'Triggered' }, b: { value: 'not_triggered', label: 'Not Triggered' } },
  { kind: 'tri', key: 'new_high_63', label: 'New 63-Bar High', a: { value: 'triggered', label: 'Triggered' }, b: { value: 'not_triggered', label: 'Not Triggered' } },
  { kind: 'tri', key: 'new_high_252', label: 'New 252-Bar High', a: { value: 'triggered', label: 'Triggered' }, b: { value: 'not_triggered', label: 'Not Triggered' } },
  { kind: 'tri', key: 'cross_downrot_low', label: 'Cross < Prev Down Rotation Low', a: { value: 'triggered', label: 'Triggered' }, b: { value: 'not_triggered', label: 'Not Triggered' } },
  { kind: 'tri', key: 'cross_breakdown_low', label: 'Cross < Prev Breakdown Low', a: { value: 'triggered', label: 'Triggered' }, b: { value: 'not_triggered', label: 'Not Triggered' } },
  { kind: 'tri', key: 'cross_prev_low', label: 'Cross < Prev Candle Low', a: { value: 'triggered', label: 'Triggered' }, b: { value: 'not_triggered', label: 'Not Triggered' } },
  { kind: 'tri', key: 'new_low_21', label: 'New 21-Bar Low', a: { value: 'triggered', label: 'Triggered' }, b: { value: 'not_triggered', label: 'Not Triggered' } },
  { kind: 'tri', key: 'new_low_63', label: 'New 63-Bar Low', a: { value: 'triggered', label: 'Triggered' }, b: { value: 'not_triggered', label: 'Not Triggered' } },
  { kind: 'tri', key: 'new_low_252', label: 'New 252-Bar Low', a: { value: 'triggered', label: 'Triggered' }, b: { value: 'not_triggered', label: 'Not Triggered' } },
  { kind: 'th',  key: 'h',     label: 'H React',          opParam: 'h_op',     valParam: 'h_val',     defaultVal: 0, min: -1, max: 1, step: 0.05 },
  { kind: 'th',  key: 'h_pct', label: 'H React %ile',     opParam: 'h_pct_op', valParam: 'h_pct_val', defaultVal: 0.80, min: 0, max: 1, step: 0.05, percent: true },
  { kind: 'th',  key: 'h_delta', label: 'H React Delta', opParam: 'h_delta_op', valParam: 'h_delta_val', defaultVal: 0, min: -1, max: 1, step: 0.05 },
  { kind: 'th',  key: 'h_delta_pct', label: 'H React Delta %ile', opParam: 'h_delta_pct_op', valParam: 'h_delta_pct_val', defaultVal: 0.80, min: 0, max: 1, step: 0.05, percent: true },
  { kind: 'tri', key: 'h_trend',       label: 'H React (vs lookback)',   a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' } },
  { kind: 'th',  key: 'p',     label: 'Price Chg',        opParam: 'p_op',     valParam: 'p_val',     defaultVal: 0, min: -1, max: 1, step: 0.05 },
  { kind: 'th',  key: 'p_pct', label: 'Price Chg %ile',   opParam: 'p_pct_op', valParam: 'p_pct_val', defaultVal: 0.50, min: 0, max: 1, step: 0.05, percent: true },
  { kind: 'th',  key: 'p_delta', label: 'Price Chg Delta', opParam: 'p_delta_op', valParam: 'p_delta_val', defaultVal: 0, min: -1, max: 1, step: 0.05 },
  { kind: 'th',  key: 'p_delta_pct', label: 'Price Chg Delta %ile', opParam: 'p_delta_pct_op', valParam: 'p_delta_pct_val', defaultVal: 0.50, min: 0, max: 1, step: 0.05, percent: true },
  { kind: 'tri', key: 'p_trend',       label: 'Price Chg (vs lookback)', a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' } },
  { kind: 'th',  key: 'l',     label: 'L React',          opParam: 'l_op',     valParam: 'l_val',     defaultVal: 0, min: -1, max: 1, step: 0.05 },
  { kind: 'th',  key: 'l_pct', label: 'L React %ile',     opParam: 'l_pct_op', valParam: 'l_pct_val', defaultVal: 0.80, min: 0, max: 1, step: 0.05, percent: true },
  { kind: 'th',  key: 'l_delta', label: 'L React Delta', opParam: 'l_delta_op', valParam: 'l_delta_val', defaultVal: 0, min: -1, max: 1, step: 0.05 },
  { kind: 'th',  key: 'l_delta_pct', label: 'L React Delta %ile', opParam: 'l_delta_pct_op', valParam: 'l_delta_pct_val', defaultVal: 0.80, min: 0, max: 1, step: 0.05, percent: true },
  { kind: 'tri', key: 'l_trend',       label: 'L React (vs lookback)',   a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' } },
  { kind: 'th',  key: 'rv',     label: 'RV (ann %)',      opParam: 'rv_op',     valParam: 'rv_val',     defaultVal: 25,   min: 0, max: 150, step: 1 },
  { kind: 'th',  key: 'rv_pct', label: 'RV %ile',         opParam: 'rv_pct_op', valParam: 'rv_pct_val', defaultVal: 0.80, min: 0, max: 1,   step: 0.05, percent: true },
  { kind: 'th',  key: 'rv_delta', label: 'RV Delta', opParam: 'rv_delta_op', valParam: 'rv_delta_val', defaultVal: 0, min: -100, max: 100, step: 1 },
  { kind: 'th',  key: 'rv_delta_pct', label: 'RV Delta %ile', opParam: 'rv_delta_pct_op', valParam: 'rv_delta_pct_val', defaultVal: 0.80, min: 0, max: 1, step: 0.05, percent: true },
  { kind: 'tri', key: 'rv_trend',      label: 'RV (vs lookback)',        a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' } },
  { kind: 'tri', key: 'loc_upper',     label: 'Vs Upper Target', a: { value: 'above',      label: 'Above'  }, b: { value: 'below',      label: 'Below'   } },
  { kind: 'tri', key: 'loc_lower',     label: 'Vs Lower Target', a: { value: 'above',      label: 'Above'  }, b: { value: 'below',      label: 'Below'   } },
  { kind: 'th',  key: 'breadth',  label: 'Breadth %',       opParam: 'breadth_op',  valParam: 'breadth_val',  defaultVal: 50, min: 0, max: 100, step: 5, basketOnly: true },
  { kind: 'th',  key: 'breadth_pct', label: 'Breadth %ile', opParam: 'breadth_pct_op', valParam: 'breadth_pct_val', defaultVal: 0.80, min: 0, max: 1, step: 0.05, basketOnly: true, percent: true },
  { kind: 'th',  key: 'breadth_delta', label: 'Breadth Delta', opParam: 'breadth_delta_op', valParam: 'breadth_delta_val', defaultVal: 0, min: -100, max: 100, step: 1, basketOnly: true },
  { kind: 'th',  key: 'breadth_delta_pct', label: 'Breadth Delta %ile', opParam: 'breadth_delta_pct_op', valParam: 'breadth_delta_pct_val', defaultVal: 0.80, min: 0, max: 1, step: 0.05, basketOnly: true, percent: true },
  { kind: 'tri', key: 'breadth_trend', label: 'Breadth (vs lookback)',   a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' }, basketOnly: true },
  { kind: 'th',  key: 'breakout', label: 'Breakout %',      opParam: 'breakout_op', valParam: 'breakout_val', defaultVal: 50, min: 0, max: 100, step: 5, basketOnly: true },
  { kind: 'th',  key: 'breakout_level_pct', label: 'Breakout Level %ile', opParam: 'breakout_level_pct_op', valParam: 'breakout_level_pct_val', defaultVal: 0.80, min: 0, max: 1, step: 0.05, basketOnly: true, percent: true },
  { kind: 'th',  key: 'breakout_delta', label: 'Breakout Delta', opParam: 'breakout_delta_op', valParam: 'breakout_delta_val', defaultVal: 0, min: -100, max: 100, step: 1, basketOnly: true },
  { kind: 'th',  key: 'breakout_delta_pct', label: 'Breakout Delta %ile', opParam: 'breakout_delta_pct_op', valParam: 'breakout_delta_pct_val', defaultVal: 0.80, min: 0, max: 1, step: 0.05, basketOnly: true, percent: true },
  { kind: 'tri', key: 'breakout_trend', label: 'Breakout % (vs lookback)', a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' }, basketOnly: true },
  { kind: 'th',  key: 'corr',     label: 'Correlation %',   opParam: 'corr_op',     valParam: 'corr_val',     defaultVal: 50, min: 0, max: 100, step: 5, basketOnly: true },
  { kind: 'th',  key: 'corr_level_pct', label: 'Correlation %ile', opParam: 'corr_level_pct_op', valParam: 'corr_level_pct_val', defaultVal: 0.80, min: 0, max: 1, step: 0.05, basketOnly: true, percent: true },
  { kind: 'th',  key: 'corr_delta', label: 'Correlation Delta', opParam: 'corr_delta_op', valParam: 'corr_delta_val', defaultVal: 0, min: -100, max: 100, step: 1, basketOnly: true },
  { kind: 'th',  key: 'corr_delta_pct', label: 'Correlation Delta %ile', opParam: 'corr_delta_pct_op', valParam: 'corr_delta_pct_val', defaultVal: 0.80, min: 0, max: 1, step: 0.05, basketOnly: true, percent: true },
  { kind: 'tri', key: 'corr_trend',    label: 'Correlation (vs lookback)', a: { value: 'increasing', label: 'Rising' }, b: { value: 'decreasing', label: 'Falling' }, basketOnly: true },
]

const TRI_KEYS = COND_DEFS.filter(d => d.kind === 'tri').map(d => d.key)
const TH_DEFS = COND_DEFS.filter((d): d is ThDef => d.kind === 'th')

const STATE_TABLE_METRIC_COL_WIDTH = 220
const STATE_TABLE_VALUE_COL_WIDTH = 180

type StandardSourceId =
  | 'regime'
  | 'regime_signal'
  | 'rotation'
  | 'rotation_signal'
  | 'mr_signal'
  | 'cross_uprot_high'
  | 'cross_breakout_high'
  | 'cross_prev_high'
  | 'new_high_21'
  | 'new_high_63'
  | 'new_high_252'
  | 'cross_downrot_low'
  | 'cross_breakdown_low'
  | 'cross_prev_low'
  | 'new_low_21'
  | 'new_low_63'
  | 'new_low_252'
  | 'loc_upper'
  | 'loc_lower'
  | 'rv'
  | 'h'
  | 'p'
  | 'l'
  | 'breadth'
  | 'breakout_pct'
  | 'corr'

type StandardSourceMode = 'state' | 'value' | 'percentile' | 'delta_value' | 'delta_percentile' | 'trend'

interface StandardFilterSourceConfig {
  id: StandardSourceId
  label: string
  thresholdKey?: string
  percentileKey?: string
  deltaValueKey?: string
  deltaPercentileKey?: string
  trendKey?: string
  triKey?: string
  basketOnly?: boolean
  tickerOnly?: boolean
  modeOrder: StandardSourceMode[]
}

const STANDARD_FILTER_SOURCES: StandardFilterSourceConfig[] = [
  { id: 'regime', label: 'LT Regime', triKey: 'breakout_state', modeOrder: ['state'] },
  { id: 'regime_signal', label: 'LT Signal', triKey: 'breakout_signal', modeOrder: ['state'] },
  { id: 'rotation', label: 'Rotation', triKey: 'rotation_state', modeOrder: ['state'] },
  { id: 'rotation_signal', label: 'Rotation Signal', triKey: 'rotation_signal', modeOrder: ['state'] },
  { id: 'mr_signal', label: 'MR Signal', triKey: 'mr_signal', tickerOnly: true, modeOrder: ['state'] },
  { id: 'cross_uprot_high', label: 'Cross > Prev Up Rotation High', triKey: 'cross_uprot_high', modeOrder: ['state'] },
  { id: 'cross_breakout_high', label: 'Cross > Prev Breakout High', triKey: 'cross_breakout_high', modeOrder: ['state'] },
  { id: 'cross_prev_high', label: 'Cross > Prev Candle High', triKey: 'cross_prev_high', modeOrder: ['state'] },
  { id: 'new_high_21', label: 'New 21-Bar High', triKey: 'new_high_21', modeOrder: ['state'] },
  { id: 'new_high_63', label: 'New 63-Bar High', triKey: 'new_high_63', modeOrder: ['state'] },
  { id: 'new_high_252', label: 'New 252-Bar High', triKey: 'new_high_252', modeOrder: ['state'] },
  { id: 'cross_downrot_low', label: 'Cross < Prev Down Rotation Low', triKey: 'cross_downrot_low', modeOrder: ['state'] },
  { id: 'cross_breakdown_low', label: 'Cross < Prev Breakdown Low', triKey: 'cross_breakdown_low', modeOrder: ['state'] },
  { id: 'cross_prev_low', label: 'Cross < Prev Candle Low', triKey: 'cross_prev_low', modeOrder: ['state'] },
  { id: 'new_low_21', label: 'New 21-Bar Low', triKey: 'new_low_21', modeOrder: ['state'] },
  { id: 'new_low_63', label: 'New 63-Bar Low', triKey: 'new_low_63', modeOrder: ['state'] },
  { id: 'new_low_252', label: 'New 252-Bar Low', triKey: 'new_low_252', modeOrder: ['state'] },
  { id: 'loc_upper', label: 'Vs Upper', triKey: 'loc_upper', modeOrder: ['state'] },
  { id: 'loc_lower', label: 'Vs Lower', triKey: 'loc_lower', modeOrder: ['state'] },
  { id: 'rv', label: 'RV', thresholdKey: 'rv', percentileKey: 'rv_pct', deltaValueKey: 'rv_delta', deltaPercentileKey: 'rv_delta_pct', trendKey: 'rv_trend', modeOrder: ['value', 'percentile', 'delta_value', 'delta_percentile'] },
  { id: 'h', label: 'H React', thresholdKey: 'h', percentileKey: 'h_pct', deltaValueKey: 'h_delta', deltaPercentileKey: 'h_delta_pct', trendKey: 'h_trend', modeOrder: ['value', 'percentile', 'delta_value', 'delta_percentile'] },
  { id: 'p', label: 'Price Chg', thresholdKey: 'p', percentileKey: 'p_pct', deltaValueKey: 'p_delta', deltaPercentileKey: 'p_delta_pct', trendKey: 'p_trend', modeOrder: ['value', 'percentile', 'delta_value', 'delta_percentile'] },
  { id: 'l', label: 'L React', thresholdKey: 'l', percentileKey: 'l_pct', deltaValueKey: 'l_delta', deltaPercentileKey: 'l_delta_pct', trendKey: 'l_trend', modeOrder: ['value', 'percentile', 'delta_value', 'delta_percentile'] },
  { id: 'breadth', label: 'Breadth %', thresholdKey: 'breadth', percentileKey: 'breadth_pct', deltaValueKey: 'breadth_delta', deltaPercentileKey: 'breadth_delta_pct', trendKey: 'breadth_trend', basketOnly: true, modeOrder: ['value', 'percentile', 'delta_value', 'delta_percentile'] },
  { id: 'breakout_pct', label: 'Breakout %', thresholdKey: 'breakout', percentileKey: 'breakout_level_pct', deltaValueKey: 'breakout_delta', deltaPercentileKey: 'breakout_delta_pct', trendKey: 'breakout_trend', basketOnly: true, modeOrder: ['value', 'percentile', 'delta_value', 'delta_percentile'] },
  { id: 'corr', label: 'Corr %', thresholdKey: 'corr', percentileKey: 'corr_level_pct', deltaValueKey: 'corr_delta', deltaPercentileKey: 'corr_delta_pct', trendKey: 'corr_trend', basketOnly: true, modeOrder: ['value', 'percentile', 'delta_value', 'delta_percentile'] },
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
// Pattern comparison keys â€” the "vs prior" column under each rotation index
const PATTERN_KEYS = ['0_1', '1_2', '2_3', '3_4'] as const
type PatternKey = typeof PATTERN_KEYS[number]
// Map rotation index â†’ the pattern key anchored at that index (that index vs next older).
const PATTERN_KEY_FOR_IDX: Partial<Record<RotIdx, PatternKey>> = { 0: '0_1', 1: '1_2', 2: '2_3', 3: '3_4' }

interface RotFilterState {
  threshold:  Record<RotIdx, Record<Metric, ThresholdState>>  // per rotation index Ã— per metric
  percentile: Record<RotIdx, Record<Metric, ThresholdState>>  // per rotation index Ã— per metric
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
  UpRot:     { a: 'HH', b: 'LH', title: 'Relative high relationship between consecutive up rotations' },
  DownRot:   { a: 'HL', b: 'LL', title: 'Trough low comparison between consecutive down rotations' },
  Breakout:  { a: 'HH', b: 'LH', title: 'Relative high relationship between consecutive breakouts' },
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
    // Indexed threshold â€” keyed by rotation index
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

const FILTER_VALUE_GRID_TWO_STYLE: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: '76px minmax(0, 1fr)',
  minHeight: 22,
  width: '100%',
}

const FILTER_VALUE_GRID_TRI_TWO_STYLE: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
  minHeight: 22,
  width: '100%',
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
  color: SOLAR_TEXT,
  fontFamily: 'monospace',
  fontSize: 9,
  fontWeight: 500,
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
  gridTemplateColumns: 'minmax(180px, 1.25fr) minmax(180px, 1.15fr) minmax(130px, 0.85fr) minmax(520px, 3.75fr) 32px',
  borderTop: `1px solid ${SOLAR_BORDER}`,
  width: '100%',
}

const FILTER_EDITOR_CELL_STYLE: React.CSSProperties = {
  minWidth: 0,
  padding: '3px 6px',
  borderRight: `1px solid ${SOLAR_BORDER}`,
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
  fontFamily: 'monospace',
  fontSize: 10,
}

const FILTER_EDITOR_HEADER_CELL_STYLE: React.CSSProperties = {
  ...FILTER_EDITOR_CELL_STYLE,
  color: SOLAR_MUTED,
  fontSize: 8,
  letterSpacing: 0.3,
  background: 'rgba(147, 161, 161, 0.08)',
  textAlign: 'center',
}

const FILTER_EDITOR_FIELD_CELL_STYLE: React.CSSProperties = {
  ...FILTER_EDITOR_CELL_STYLE,
  justifyContent: 'flex-start',
  textAlign: 'left',
}

const FILTER_EDITOR_CENTER_CELL_STYLE: React.CSSProperties = {
  ...FILTER_EDITOR_CELL_STYLE,
  justifyContent: 'center',
  textAlign: 'center',
}

const FILTER_EDITOR_CONDITION_CELL_STYLE: React.CSSProperties = {
  ...FILTER_EDITOR_CELL_STYLE,
  padding: 0,
  display: 'block',
}

const HORIZON_OPTIONS = ['1', '5', '21', '63', '252'] as const
type HorizonOption = typeof HORIZON_OPTIONS[number]

const SHEET_TABLE_STYLE: React.CSSProperties = {
  width: '100%',
  borderCollapse: 'collapse',
  tableLayout: 'fixed',
  color: SOLAR_TEXT,
  fontSize: 12,
  fontFamily: 'monospace',
  lineHeight: 1.2,
}

const SHEET_HEADER_CELL_STYLE: React.CSSProperties = {
  padding: '3px 5px',
  textAlign: 'center',
  color: SOLAR_TITLE,
  borderBottom: `1px solid ${SHEET_GRID_BORDER}`,
  background: 'rgba(147, 161, 161, 0.08)',
  fontSize: 9,
  letterSpacing: 0.25,
  whiteSpace: 'nowrap',
}

const SHEET_ROW_LABEL_STYLE: React.CSSProperties = {
  padding: '3px 5px',
  borderTop: `1px solid ${SHEET_GRID_BORDER}`,
  whiteSpace: 'nowrap',
  fontWeight: 700,
  textAlign: 'center',
  verticalAlign: 'middle',
  fontSize: 11,
}

const SHEET_CELL_STYLE: React.CSSProperties = {
  padding: '3px 5px',
  borderTop: `1px solid ${SHEET_GRID_BORDER}`,
  borderLeft: `1px solid ${SHEET_GRID_BORDER}`,
  textAlign: 'center',
  whiteSpace: 'nowrap',
  verticalAlign: 'middle',
  overflow: 'hidden',
}

const SHEET_BUTTON_CELL_STYLE: React.CSSProperties = {
  width: '100%',
  minHeight: 20,
  padding: '2px 4px',
  border: 0,
  background: 'transparent',
  color: SOLAR_TEXT,
  fontFamily: 'monospace',
  fontSize: 11,
  textAlign: 'center',
  cursor: 'pointer',
}

const modeLabel = (mode: StandardSourceMode) =>
  mode === 'state' ? 'State' : 'Value'

const normalizeSourceMode = (mode: StandardSourceMode): StandardSourceMode => (
  mode === 'trend' ? 'delta_value' : mode
)

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
  mode === 'pattern' ? 'State' : 'Value'
)

const getRotationMetricValue = (ctx: RotCtx, idx: RotIdx, metric: Metric) => (
  ctx[`${metric}_${idx}` as keyof RotCtx] as number | null | undefined
)

const getRotationMetricPercentile = (ctx: RotCtx, idx: RotIdx, metric: Metric) => (
  ctx[`${metric}_${idx}_Pct` as keyof RotCtx] as number | null | undefined
)

const getRotationDateValue = (ctx: RotCtx, idx: RotIdx, edge: 'Start' | 'End') => (
  ctx[`${edge}_${idx}` as keyof RotCtx] as string | null | undefined
)

const getRotationPatternValue = (ctx: RotCtx, key: PatternKey) => (
  ctx.patterns?.[key] ?? null
)

const triStateFromCurrent = (sourceId: StandardSourceId, cc: CurrentContext): TriState => {
  switch (sourceId) {
    case 'regime':
      return cc.active_regime === 'breakout' ? 'a' : cc.active_regime === 'breakdown' ? 'b' : 'any'
    case 'regime_signal':
      return cc.regime_signal === 'breakout' ? 'a' : cc.regime_signal === 'breakdown' ? 'b' : 'any'
    case 'rotation':
      return cc.active_rotation === 'up' ? 'a' : cc.active_rotation === 'down' ? 'b' : 'any'
    case 'rotation_signal':
      return cc.rotation_signal === 'up' ? 'a' : cc.rotation_signal === 'down' ? 'b' : 'any'
    case 'mr_signal':
      return cc.mr_signal === 'btfd' ? 'a' : cc.mr_signal === 'stfr' ? 'b' : 'any'
    case 'cross_uprot_high':
      return cc.price_events.cross_uprot_high ? 'a' : 'b'
    case 'cross_breakout_high':
      return cc.price_events.cross_breakout_high ? 'a' : 'b'
    case 'cross_prev_high':
      return cc.price_events.cross_prev_high ? 'a' : 'b'
    case 'new_high_21':
      return cc.price_events.new_high_21 ? 'a' : 'b'
    case 'new_high_63':
      return cc.price_events.new_high_63 ? 'a' : 'b'
    case 'new_high_252':
      return cc.price_events.new_high_252 ? 'a' : 'b'
    case 'cross_downrot_low':
      return cc.price_events.cross_downrot_low ? 'a' : 'b'
    case 'cross_breakdown_low':
      return cc.price_events.cross_breakdown_low ? 'a' : 'b'
    case 'cross_prev_low':
      return cc.price_events.cross_prev_low ? 'a' : 'b'
    case 'new_low_21':
      return cc.price_events.new_low_21 ? 'a' : 'b'
    case 'new_low_63':
      return cc.price_events.new_low_63 ? 'a' : 'b'
    case 'new_low_252':
      return cc.price_events.new_low_252 ? 'a' : 'b'
    case 'loc_upper':
      return cc.position === 'above_upper' ? 'a' : cc.position == null ? 'any' : 'b'
    case 'loc_lower':
      return cc.position === 'below_lower' ? 'b' : cc.position == null ? 'any' : 'a'
    default:
      return 'any'
  }
}

const metricSnapshot = (sourceId: StandardSourceId, cc: CurrentContext): { value: number | null; percentile: number | null; deltaValue: number | null; deltaPercentile: number | null } => {
  const ind = cc.indicators
  switch (sourceId) {
    case 'rv':
      return { value: ind.rv_ann, percentile: ind.rv_pct, deltaValue: ind.rv_delta, deltaPercentile: ind.rv_delta_pct }
    case 'h':
      return { value: ind.h_react, percentile: ind.h_pct, deltaValue: ind.h_delta, deltaPercentile: ind.h_delta_pct }
    case 'p':
      return { value: ind.p_react, percentile: ind.p_pct, deltaValue: ind.p_delta, deltaPercentile: ind.p_delta_pct }
    case 'l':
      return { value: ind.l_react, percentile: ind.l_pct, deltaValue: ind.l_delta, deltaPercentile: ind.l_delta_pct }
    case 'breadth':
      return { value: ind.breadth, percentile: ind.breadth_pct, deltaValue: ind.breadth_delta, deltaPercentile: ind.breadth_delta_pct }
    case 'breakout_pct':
      return { value: ind.breakout_pct, percentile: ind.breakout_level_pct, deltaValue: ind.breakout_delta, deltaPercentile: ind.breakout_delta_pct }
    case 'corr':
      return { value: ind.corr_pct, percentile: ind.corr_level_pct, deltaValue: ind.corr_delta, deltaPercentile: ind.corr_delta_pct }
    default:
      return { value: null, percentile: null, deltaValue: null, deltaPercentile: null }
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
  onCycleOrEnable: () => void
  onSetValue: (value: number) => void
}> = ({ state, min, max, step, percent, onCycleOrEnable, onSetValue }) => (
  <div style={FILTER_VALUE_GRID_TWO_STYLE}>
    <div style={FILTER_VALUE_CELL_STYLE}>
      <SheetToggleButton active label={state.op} onClick={onCycleOrEnable} />
    </div>
    <div style={FILTER_VALUE_LAST_CELL_STYLE}>
      <input
        type="number"
        className="backtest-input"
        value={toDispRounded(state.val, percent)}
        min={toDispRounded(min, percent)}
        max={toDispRounded(max, percent)}
        step={toDispRounded(step, percent)}
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
    if (def.tickerOnly && isBasket) continue
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
  const [contextSectionsOpen, setContextSectionsOpen] = useState({
    priceEvents: false,
    longTerm: false,
    breakouts: true,
    breakdowns: true,
    shortTerm: true,
    upRotations: true,
    downRotations: true,
    rotations: true,
    indicators: true,
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
    () => STANDARD_FILTER_SOURCES.filter(source => (!source.basketOnly || isBasket) && (!source.tickerOnly || !isBasket)),
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
      setTriConds(prev => ({ ...prev, ...triPatch } as Record<string, TriState>))
    }
    setThresholds(prev => {
      const next = { ...prev }
      if (source.thresholdKey) next[source.thresholdKey] = { ...next[source.thresholdKey], active: false }
      if (source.percentileKey) next[source.percentileKey] = { ...next[source.percentileKey], active: false }
      if (source.deltaValueKey) next[source.deltaValueKey] = { ...next[source.deltaValueKey], active: false }
      if (source.deltaPercentileKey) next[source.deltaPercentileKey] = { ...next[source.deltaPercentileKey], active: false }
      return next
    })
  }, [])

  const applySourceDefaults = useCallback((source: StandardFilterSourceConfig, mode: StandardSourceMode, cc: CurrentContext | null) => {
    const triPatch: Partial<Record<string, TriState>> = {}
    if (source.triKey) triPatch[source.triKey] = 'any'
    if (source.trendKey) triPatch[source.trendKey] = 'any'
    if (cc && source.triKey) triPatch[source.triKey] = triStateFromCurrent(source.id, cc)
    if (Object.keys(triPatch).length > 0) {
      setTriConds(prev => ({ ...prev, ...triPatch } as Record<string, TriState>))
    }

    setThresholds(prev => {
      const next = { ...prev }
      if (source.thresholdKey) next[source.thresholdKey] = { ...next[source.thresholdKey], active: false }
      if (source.percentileKey) next[source.percentileKey] = { ...next[source.percentileKey], active: false }
      if (source.deltaValueKey) next[source.deltaValueKey] = { ...next[source.deltaValueKey], active: false }
      if (source.deltaPercentileKey) next[source.deltaPercentileKey] = { ...next[source.deltaPercentileKey], active: false }
      if (!cc) return next

      const snap = metricSnapshot(source.id, cc)
      if (mode === 'value' && source.thresholdKey && snap.value != null) {
        next[source.thresholdKey] = { ...next[source.thresholdKey], active: true, val: snap.value }
      }
      if (mode === 'percentile' && source.percentileKey && snap.percentile != null) {
        next[source.percentileKey] = { ...next[source.percentileKey], active: true, val: snap.percentile }
      }
      if (mode === 'delta_value' && source.deltaValueKey && snap.deltaValue != null) {
        next[source.deltaValueKey] = { ...next[source.deltaValueKey], active: true, val: snap.deltaValue }
      }
      if (mode === 'delta_percentile' && source.deltaPercentileKey && snap.deltaPercentile != null) {
        next[source.deltaPercentileKey] = { ...next[source.deltaPercentileKey], active: true, val: snap.deltaPercentile }
      }
      return next
    })
  }, [])

  useEffect(() => {
    const legacyTrendSources = Object.entries(selectedSourceModes)
      .filter(([, mode]) => mode === 'trend')
      .map(([sourceId]) => sourceId as StandardSourceId)

    if (legacyTrendSources.length === 0) return

    setSelectedSourceModes(prev => {
      const next = { ...prev }
      legacyTrendSources.forEach(sourceId => {
        next[sourceId] = 'delta_value'
      })
      return next
    })

    legacyTrendSources.forEach(sourceId => {
      applySourceDefaults(sourceConfigMap[sourceId], 'delta_value', currentContext)
    })
  }, [applySourceDefaults, currentContext, selectedSourceModes, sourceConfigMap])

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
    const nextMode = normalizeSourceMode(mode)
    const isSelected = selectedSourceIds.includes(sourceId)
    const currentMode = normalizeSourceMode(selectedSourceModes[sourceId] ?? source.modeOrder[0])

    if (isSelected && currentMode === nextMode) {
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
    setSelectedSourceModes(prev => ({ ...prev, [sourceId]: nextMode }))
    applySourceDefaults(source, nextMode, currentContext)
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

  // Rotation filter setters â€” per rotation index for threshold, flat for percentile
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

  // â”€â”€ Canvas (KDE) â”€â”€
  const canvasRef = useRef<HTMLCanvasElement>(null)
  const [dims, setDims] = useState({ w: 800, h: 360 })
  const wrapRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    const onResize = () => { if (wrapRef.current) setDims({ w: wrapRef.current.clientWidth, h: 360 }) }
    onResize(); window.addEventListener('resize', onResize)
    return () => window.removeEventListener('resize', onResize)
  }, [])

  // â”€â”€ Forward paths chart (Phase 4) â”€â”€
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

    // Y range spans the Â±1Ïƒ bands of all three terciles (+ any hovered individual path).
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

    // Â±1Ïƒ filled bands per tercile
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
      case 'regime_signal': {
        const state = triStateFromCurrent(sourceId, currentContext)
        return { value: positionToText(state, 'Breakout', 'Breakdown'), sub: undefined }
      }
      case 'rotation': {
        const state = triStateFromCurrent(sourceId, currentContext)
        return { value: positionToText(state, 'Up', 'Down'), sub: undefined }
      }
      case 'rotation_signal': {
        const state = triStateFromCurrent(sourceId, currentContext)
        return { value: positionToText(state, 'Up', 'Down'), sub: undefined }
      }
      case 'mr_signal': {
        const state = triStateFromCurrent(sourceId, currentContext)
        return { value: positionToText(state, 'BTFD', 'STFR'), sub: undefined }
      }
      case 'cross_uprot_high':
      case 'cross_breakout_high':
      case 'cross_prev_high':
      case 'new_high_21':
      case 'new_high_63':
      case 'new_high_252':
      case 'cross_downrot_low':
      case 'cross_breakdown_low':
      case 'cross_prev_low': {
        const state = triStateFromCurrent(sourceId, currentContext)
        return { value: positionToText(state, 'Triggered', 'Not Triggered'), sub: undefined }
      }
      case 'new_low_21':
      case 'new_low_63':
      case 'new_low_252': {
        const state = triStateFromCurrent(sourceId, currentContext)
        return { value: positionToText(state, 'Triggered', 'Not Triggered'), sub: undefined }
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
        return { value: snap.value == null ? '--' : `${snap.value.toFixed(2)}%`, sub: snap.percentile == null ? undefined : `%ile ${(snap.percentile * 100).toFixed(2)}%` }
      case 'breadth':
      case 'breakout_pct':
      case 'corr':
        return { value: snap.value == null ? '--' : `${snap.value.toFixed(2)}%`, sub: snap.percentile == null ? undefined : `%ile ${(snap.percentile * 100).toFixed(2)}%` }
      default:
        return {
          value: snap.value == null ? '--' : snap.value.toFixed(2),
          sub: snap.percentile == null ? undefined : `%ile ${(snap.percentile * 100).toFixed(2)}%`,
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
        current: getRotationPatternValue(ctx, row.patternKey) ?? '--',
      }
    }

    if (!row.metric) {
      return { label: labelBase, current: '--' }
    }

    const rawValue = row.mode === 'value'
      ? getRotationMetricValue(ctx, row.idx, row.metric)
      : getRotationMetricPercentile(ctx, row.idx, row.metric)

    const current = row.mode === 'percentile'
      ? pct(rawValue, 2)
      : row.metric === 'Time'
        ? (rawValue == null ? '--' : rawValue.toFixed(0))
        : pct(rawValue, 2)

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
              {mode.toUpperCase()} Â· {target} Â· {lookback} lookback Â· {horizon} bars
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

      {/* Filters â€” grouped collapsible */}
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
            const storedMode = selectedSourceModes[source.id] ?? source.modeOrder[0]
            const mode = normalizeSourceMode(storedMode)
            const snapshot = (() => {
              if ((mode === 'value' || mode === 'percentile' || mode === 'delta_value' || mode === 'delta_percentile') && currentContext) {
                const snap = metricSnapshot(source.id, currentContext)
                if (mode === 'value') {
                  return {
                    value: snap.value == null
                      ? '--'
                      : source.id === 'rv' || source.id === 'breadth' || source.id === 'breakout_pct' || source.id === 'corr'
                        ? `${snap.value.toFixed(2)}%`
                        : snap.value.toFixed(2),
                    sub: undefined as string | undefined,
                  }
                }
                if (mode === 'percentile') {
                  return {
                    value: snap.percentile == null ? '--' : `${(snap.percentile * 100).toFixed(2)}%`,
                    sub: undefined as string | undefined,
                  }
                }
                if (mode === 'delta_value') {
                  return {
                    value: snap.deltaValue == null
                      ? '--'
                      : (source.id === 'breadth' || source.id === 'breakout_pct' || source.id === 'corr')
                          ? `${snap.deltaValue.toFixed(2)}%`
                          : snap.deltaValue.toFixed(2),
                    sub: undefined as string | undefined,
                  }
                }
                return {
                  value: snap.deltaPercentile == null ? '--' : `${(snap.deltaPercentile * 100).toFixed(2)}%`,
                  sub: undefined as string | undefined,
                }
              }
              const baseSnapshot = formatSourceSnapshot(source.id)
              if (mode === 'state' && (source.id === 'loc_upper' || source.id === 'loc_lower')) {
                return {
                  value: baseSnapshot.value,
                  sub: undefined as string | undefined,
                }
              }
              return baseSnapshot
            })()
            const triDef = source.triKey
              ? COND_DEFS.find((def): def is TriDef => def.kind === 'tri' && def.key === source.triKey)
              : undefined
            const rawDef = source.thresholdKey ? TH_DEFS.find(def => def.key === source.thresholdKey) : undefined
            const pctDef = source.percentileKey ? TH_DEFS.find(def => def.key === source.percentileKey) : undefined
            const deltaValueDef = source.deltaValueKey ? TH_DEFS.find(def => def.key === source.deltaValueKey) : undefined
            const deltaPctDef = source.deltaPercentileKey ? TH_DEFS.find(def => def.key === source.deltaPercentileKey) : undefined
            const fieldLabel = mode === 'state'
              ? source.label
              : mode === 'value'
                ? (rawDef?.label ?? source.label)
                : mode === 'percentile'
                  ? (pctDef?.label ?? `${source.label} %ile`)
                  : mode === 'delta_value'
                    ? (deltaValueDef?.label ?? `${source.label} Delta`)
                    : (deltaPctDef?.label ?? `${source.label} Delta %ile`)

            const renderThresholdEditor = (def: ThDef) => {
              const state = thresholds[def.key]
              return (
                <div style={FILTER_VALUE_GRID_TWO_STYLE}>
                  <div style={FILTER_VALUE_CELL_STYLE}>
                    <SheetToggleButton
                      active={state.active}
                      label={state.op}
                      onClick={() => cycleOp(def.key)}
                    />
                  </div>
                  <div style={FILTER_VALUE_LAST_CELL_STYLE}>
                    <input
                      type="number"
                      className="backtest-input"
                      value={toDispRounded(state.val, def.percent)}
                      min={toDispRounded(def.min, def.percent)}
                      max={toDispRounded(def.max, def.percent)}
                      step={toDispRounded(def.step, def.percent)}
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
              <div style={FILTER_VALUE_GRID_TRI_TWO_STYLE}>
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
                <div style={FILTER_EDITOR_FIELD_CELL_STYLE}>{fieldLabel}</div>
                <div style={FILTER_EDITOR_CENTER_CELL_STYLE}>
                  <div>
                    <div>{snapshot.value}</div>
                    {snapshot.sub && <div style={{ fontSize: 8, color: SOLAR_MUTED }}>{snapshot.sub}</div>}
                  </div>
                </div>
                <div style={FILTER_EDITOR_CENTER_CELL_STYLE}>
                  <span>{modeLabel(mode)}</span>
                </div>
                <div style={FILTER_EDITOR_CONDITION_CELL_STYLE}>
                  {mode === 'state' && triDef && renderTriEditor(triDef)}
                  {mode === 'value' && rawDef && renderThresholdEditor(rawDef)}
                  {mode === 'percentile' && pctDef && renderThresholdEditor(pctDef)}
                  {mode === 'delta_value' && deltaValueDef && renderThresholdEditor(deltaValueDef)}
                  {mode === 'delta_percentile' && deltaPctDef && renderThresholdEditor(deltaPctDef)}
                </div>
                <div style={{ ...FILTER_EDITOR_CENTER_CELL_STYLE, borderRight: 0, padding: 0 }}>
                  <SheetToggleButton active={false} label="x" onClick={() => toggleSourceSelection(source.id)} title={`Remove ${source.label}`} />
                </div>
              </div>
            )
          })}
          {activeRotationFilters.map(row => {
            const snapshot = formatRotationFilterSnapshot(row)
            const patternLabels = ROT_PATTERN_LABELS[row.type]

            const renderRotationThreshold = () => {
              if (!row.metric) return null
              const metric = row.metric
              const defaults = ROT_DEFAULTS[metric]
              const state = row.mode === 'value'
                ? rotFilters[row.type].threshold[row.idx][metric]
                : rotFilters[row.type].percentile[row.idx][metric]
              return (
                <AnyOpValueControl
                  state={state}
                  min={row.mode === 'value' ? defaults.th.min : 0}
                  max={row.mode === 'value' ? defaults.th.max : 1}
                  step={row.mode === 'value' ? defaults.th.step : defaults.pct.step}
                  percent={row.mode === 'value' ? defaults.th.percent : defaults.pct.percent}
                  onCycleOrEnable={() => {
                    if (row.mode === 'value') {
                      if (state.active) cycleRotThOp(row.type, row.idx, metric)
                      else setRotTh(row.type, row.idx, metric, { active: true })
                      return
                    }
                    if (state.active) cycleRotPctOp(row.type, row.idx, metric)
                    else setRotPct(row.type, row.idx, metric, { active: true })
                  }}
                  onSetValue={(value) => {
                    if (row.mode === 'value') setRotTh(row.type, row.idx, metric, { active: true, val: value })
                    else setRotPct(row.type, row.idx, metric, { active: true, val: value })
                  }}
                />
              )
            }

            const renderRotationPattern = () => {
              if (!row.patternKey) return null
              const patternKey = row.patternKey
              return (
                <div style={FILTER_VALUE_GRID_TRI_TWO_STYLE}>
                  <div style={FILTER_VALUE_CELL_STYLE}>
                    <SheetToggleButton active={rotFilters[row.type].pattern[patternKey] === 'a'} label={patternLabels.a} onClick={() => setRotPattern(row.type, patternKey, 'a')} />
                  </div>
                  <div style={FILTER_VALUE_LAST_CELL_STYLE}>
                    <SheetToggleButton active={rotFilters[row.type].pattern[patternKey] === 'b'} label={patternLabels.b} onClick={() => setRotPattern(row.type, patternKey, 'b')} />
                  </div>
                </div>
              )
            }

            return (
              <div key={row.id} style={FILTER_EDITOR_ROW_STYLE}>
                <div style={FILTER_EDITOR_FIELD_CELL_STYLE}>{snapshot.label}</div>
                <div style={FILTER_EDITOR_CENTER_CELL_STYLE}>{snapshot.current}</div>
                <div style={FILTER_EDITOR_CENTER_CELL_STYLE}>{rotationModeLabel(row.mode)}</div>
                <div style={FILTER_EDITOR_CONDITION_CELL_STYLE}>
                  {row.mode === 'pattern' ? renderRotationPattern() : renderRotationThreshold()}
                </div>
                <div style={{ ...FILTER_EDITOR_CENTER_CELL_STYLE, borderRight: 0, padding: 0 }}>
                  <SheetToggleButton active={false} label="x" onClick={() => clearActiveRotationFilter(row)} title={`Remove ${snapshot.label}`} />
                </div>
              </div>
            )
          })}
          </>
        )}
      </SectionCard>

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
        const stateColor = (value: string) =>
          value === 'Breakout' || value === 'Up' || value === 'Above' || value === 'BTFD'
            ? BLUE
            : value === 'Breakdown' || value === 'Down' || value === 'Below' || value === 'STFR'
              ? PINK
              : undefined
        const eventStateColor = (value: string, tone: 'bullish' | 'bearish') =>
          value === 'Triggered' ? (tone === 'bullish' ? BLUE : PINK) : undefined
        const renderSheetValue = ({
          main,
          sub,
          active = false,
          onClick,
          color,
          background,
          align = 'center',
        }: {
          main: string
          sub?: string
          active?: boolean
          onClick?: () => void
          color?: string
          background?: string
          align?: 'left' | 'center' | 'right'
        }) => {
          const alignItems = align === 'right' ? 'flex-end' : align === 'center' ? 'center' : 'flex-start'
          const justifyContent = align === 'right' ? 'flex-end' : align === 'center' ? 'center' : 'flex-start'
          const body = (
            <div style={{ display: 'flex', flexDirection: 'column', alignItems, justifyContent, gap: 0, lineHeight: 1.05 }}>
              <span style={{ color: color ?? (active ? BLUE : SOLAR_TEXT), fontWeight: 700, fontSize: 12 }}>{main}</span>
              {sub && <span style={{ fontSize: 9, color: SOLAR_MUTED }}>{sub}</span>}
            </div>
          )

          if (onClick) {
            return (
              <button
                type="button"
                onClick={onClick}
                style={{
                  ...SHEET_BUTTON_CELL_STYLE,
                  background: background ?? (active ? 'rgba(50, 50, 255, 0.14)' : 'transparent'),
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
            <div style={{ minHeight: 20, display: 'flex', alignItems: 'center', justifyContent, background: background ?? 'transparent' }}>
              {body}
            </div>
          )
        }

        const renderOverviewRow = ({
          label,
          value,
          valueActive = false,
          onValueClick,
          valueColor,
          percentile,
          percentileActive = false,
          onPercentileClick,
          percentileColor,
          delta,
          deltaActive = false,
          onDeltaClick,
          deltaColor,
          deltaPct,
          deltaPctActive = false,
          onDeltaPctClick,
          deltaPctColor,
        }: {
          label: string
          value: string
          valueActive?: boolean
          onValueClick?: () => void
          valueColor?: string
          percentile: string
          percentileActive?: boolean
          onPercentileClick?: () => void
          percentileColor?: string
          delta: string
          deltaActive?: boolean
          onDeltaClick?: () => void
          deltaColor?: string
          deltaPct: string
          deltaPctActive?: boolean
          onDeltaPctClick?: () => void
          deltaPctColor?: string
        }) => (
          <tr key={label}>
            <td style={SHEET_ROW_LABEL_STYLE}>{label}</td>
            <td style={SHEET_CELL_STYLE}>
              {renderSheetValue({ main: value, active: valueActive, onClick: onValueClick, color: valueColor, align: 'center' })}
            </td>
            <td style={SHEET_CELL_STYLE}>
              {renderSheetValue({ main: percentile, active: percentileActive, onClick: onPercentileClick, color: percentileColor, align: 'center' })}
            </td>
            <td style={SHEET_CELL_STYLE}>
              {renderSheetValue({ main: delta, active: deltaActive, onClick: onDeltaClick, color: deltaColor, align: 'center' })}
            </td>
            <td style={SHEET_CELL_STYLE}>
              {renderSheetValue({ main: deltaPct, active: deltaPctActive, onClick: onDeltaPctClick, color: deltaPctColor, align: 'center' })}
            </td>
          </tr>
        )

        const renderStateFilterRow = ({
          label,
          value,
          active = false,
          onClick,
          color,
        }: {
          label: string
          value: string
          active?: boolean
          onClick?: () => void
          color?: string
        }) => (
          <tr key={label}>
            <td style={SHEET_ROW_LABEL_STYLE}>{label}</td>
            <td style={SHEET_CELL_STYLE}>
              {renderSheetValue({ main: value, active, onClick, color, align: 'center' })}
            </td>
          </tr>
        )

        const renderRotationHistory = (title: string, type: RotType) => {
          const ctx = cc.rotations[type]
          const isBearishType = type === 'DownRot' || type === 'Breakdown'
          const activeRowShade = ctx.active
            ? (isBearishType ? 'rgba(255, 50, 150, 0.12)' : 'rgba(50, 50, 255, 0.12)')
            : 'transparent'
          const rowValues = ROT_INDICES.map(idx => ({
            idx,
            time: getRotationMetricValue(ctx, idx, 'Time'),
            range: (() => {
              const v = getRotationMetricValue(ctx, idx, 'Range')
              if (v == null) return null
              return isBearishType ? -Math.abs(v) : v
            })(),
            change: getRotationMetricValue(ctx, idx, 'Change'),
            timePct: getRotationMetricPercentile(ctx, idx, 'Time'),
            rangePct: getRotationMetricPercentile(ctx, idx, 'Range'),
            changePct: getRotationMetricPercentile(ctx, idx, 'Change'),
          }))
          const timeSeries = rowValues.map(r => r.time)
          const rangeSeries = rowValues.map(r => r.range)
          const changeSeries = rowValues.map(r => r.change)
          const valueScore = (metric: Metric, value: number | null | undefined) => {
            if (value == null) return null
            if (!isBearishType) {
              if (metric === 'Time') return relativeScore(value, timeSeries)
              if (metric === 'Range') return relativeScore(value, rangeSeries)
              return relativeScore(value, changeSeries)
            }
            if (metric === 'Time') return relativeScore(-value, timeSeries.map(v => v == null ? null : -v))
            if (metric === 'Range') return relativeScore(value, rangeSeries)
            return relativeScore(value, changeSeries)
          }
          const percentileScore = (metric: Metric, value: number | null | undefined) => {
            if (value == null) return null
            if (!isBearishType) return 2 * value - 1
            if (metric === 'Time') return 1 - 2 * value
            if (metric === 'Range') return 1 - 2 * value
            return 2 * value - 1
          }
          const metricColor = (metric: Metric, value: number | null | undefined) => {
            const score = valueScore(metric, value)
            return score == null ? undefined : grayCenteredColor(score)
          }
          const percentileColor = (metric: Metric, value: number | null | undefined) => {
            const score = percentileScore(metric, value)
            return score == null ? undefined : grayCenteredColor(score)
          }
          const patternTone = (value: string | null | undefined) =>
            value === 'HH' || value === 'HL' ? BLUE : value === 'LH' || value === 'LL' ? PINK : SOLAR_MUTED

          return (
            <div style={{ borderTop: `1px solid ${SHEET_GRID_BORDER}`, overflowX: 'auto' }}>
              <table style={SHEET_TABLE_STYLE}>
                <colgroup>
                  <col style={{ width: 108 }} />
                  <col style={{ width: 108 }} />
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
                    <th style={SHEET_HEADER_CELL_STYLE}>Start Date</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>End Date</th>
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
                    const startDate = getRotationDateValue(ctx, idx, 'Start')
                    const endDate = getRotationDateValue(ctx, idx, 'End')
                    const timeValue = getRotationMetricValue(ctx, idx, 'Time')
                    const rawRangeValue = getRotationMetricValue(ctx, idx, 'Range')
                    const rangeValue = rawRangeValue == null ? null : (isBearishType ? -Math.abs(rawRangeValue) : rawRangeValue)
                    const changeValue = getRotationMetricValue(ctx, idx, 'Change')
                    const timePct = getRotationMetricPercentile(ctx, idx, 'Time')
                    const rangePct = getRotationMetricPercentile(ctx, idx, 'Range')
                    const changePct = getRotationMetricPercentile(ctx, idx, 'Change')
                    const patternKey = PATTERN_KEY_FOR_IDX[idx]
                    const pattern = patternKey ? getRotationPatternValue(ctx, patternKey) : null
                    const patternFilterValue = patternKey ? ctx.patterns?.[patternKey] ?? null : null

                    return (
                      <tr key={`${title}-${idx}`} style={{ background: idx === 0 ? activeRowShade : 'transparent' }}>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: startDate ?? '--',
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: idx === 0 && ctx.active ? 'Active' : (endDate ?? '--'),
                            active: idx === 0 && ctx.active,
                            color: idx === 0 && ctx.active ? BLUE : SOLAR_TEXT,
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: timeValue == null ? '--' : timeValue.toFixed(0),
                            active: rotFilters[type].threshold[idx].Time.active,
                            onClick: () => toggleRotMetricSelection(type, idx, 'Time', 'value', timeValue),
                            color: metricColor('Time', timeValue),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(rangeValue),
                            active: rotFilters[type].threshold[idx].Range.active,
                            onClick: () => toggleRotMetricSelection(type, idx, 'Range', 'value', rawRangeValue),
                            color: metricColor('Range', rangeValue),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(changeValue),
                            active: rotFilters[type].threshold[idx].Change.active,
                            onClick: () => toggleRotMetricSelection(type, idx, 'Change', 'value', changeValue),
                            color: metricColor('Change', changeValue),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pattern ?? '--',
                            active: !!patternKey && rotFilters[type].pattern[patternKey] !== 'any',
                            onClick: patternKey ? () => toggleRotPatternSelection(type, patternKey, patternFilterValue) : undefined,
                            color: patternTone(patternFilterValue),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(timePct),
                            active: rotFilters[type].percentile[idx].Time.active,
                            onClick: () => toggleRotMetricSelection(type, idx, 'Time', 'percentile', timePct),
                            color: percentileColor('Time', timePct),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(rangePct),
                            active: rotFilters[type].percentile[idx].Range.active,
                            onClick: () => toggleRotMetricSelection(type, idx, 'Range', 'percentile', rangePct),
                            color: percentileColor('Range', rangePct),
                            align: 'center',
                          })}
                        </td>
                        <td style={SHEET_CELL_STYLE}>
                          {renderSheetValue({
                            main: pctD(changePct),
                            active: rotFilters[type].percentile[idx].Change.active,
                            onClick: () => toggleRotMetricSelection(type, idx, 'Change', 'percentile', changePct),
                            color: percentileColor('Change', changePct),
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

        const renderContextToggle = (label: string, key: keyof typeof contextSectionsOpen, isPrimary = false) => (
          <button
            type="button"
            onClick={() => setContextSectionsOpen(prev => ({ ...prev, [key]: !prev[key] }))}
            style={{
              width: '100%',
              padding: isPrimary ? '6px 5px' : '5px 5px',
              border: 0,
              borderTop: `1px solid ${SHEET_GRID_BORDER}`,
              borderBottom: `1px solid ${SHEET_GRID_BORDER}`,
              background: isPrimary ? 'rgba(147, 161, 161, 0.05)' : 'transparent',
              color: isPrimary ? SOLAR_TITLE : SOLAR_MUTED,
              fontSize: isPrimary ? 10 : 9,
              letterSpacing: isPrimary ? 0.35 : 0.3,
              textAlign: 'left',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              fontFamily: 'monospace',
              fontWeight: isPrimary ? 700 : 600,
            }}
          >
            <span>{label}</span>
            <span style={{ color: SOLAR_MUTED }}>{contextSectionsOpen[key] ? 'v' : '>'}</span>
          </button>
        )

        return (
          <SectionCard
            title="CURRENT CONTEXT"
            bodyStyle={{ padding: 0 }}
          >
            {renderContextToggle('ROTATIONS', 'rotations', true)}
            {contextSectionsOpen.rotations && (
              <div style={{ overflowX: 'auto', borderTop: `1px solid ${SHEET_GRID_BORDER}` }}>
              <table style={SHEET_TABLE_STYLE}>
                <colgroup>
                  <col style={{ width: STATE_TABLE_METRIC_COL_WIDTH }} />
                  <col style={{ width: STATE_TABLE_VALUE_COL_WIDTH }} />
                </colgroup>
                <thead>
                  <tr>
                    <th style={SHEET_HEADER_CELL_STYLE}>Metric</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>State</th>
                  </tr>
                </thead>
                <tbody>
                  {renderStateFilterRow({
                    label: 'LT Regime',
                    value: formatSourceSnapshot('regime').value,
                    active: selectedSourceIds.includes('regime') && normalizeSourceMode(selectedSourceModes.regime ?? sourceConfigMap.regime.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('regime', 'state'),
                    color: stateColor(formatSourceSnapshot('regime').value),
                  })}
                  {renderStateFilterRow({
                    label: 'Rotation',
                    value: formatSourceSnapshot('rotation').value,
                    active: selectedSourceIds.includes('rotation') && normalizeSourceMode(selectedSourceModes.rotation ?? sourceConfigMap.rotation.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('rotation', 'state'),
                    color: stateColor(formatSourceSnapshot('rotation').value),
                  })}
                  {renderStateFilterRow({
                    label: 'Upper Target',
                    value: formatSourceSnapshot('loc_upper').value,
                    active: selectedSourceIds.includes('loc_upper') && normalizeSourceMode(selectedSourceModes.loc_upper ?? sourceConfigMap.loc_upper.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('loc_upper', 'state'),
                    color: stateColor(formatSourceSnapshot('loc_upper').value),
                  })}
                  {renderStateFilterRow({
                    label: 'Lower Target',
                    value: formatSourceSnapshot('loc_lower').value,
                    active: selectedSourceIds.includes('loc_lower') && normalizeSourceMode(selectedSourceModes.loc_lower ?? sourceConfigMap.loc_lower.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('loc_lower', 'state'),
                    color: stateColor(formatSourceSnapshot('loc_lower').value),
                  })}
                  {renderStateFilterRow({
                    label: 'LT Signal',
                    value: formatSourceSnapshot('regime_signal').value,
                    active: selectedSourceIds.includes('regime_signal') && normalizeSourceMode(selectedSourceModes.regime_signal ?? sourceConfigMap.regime_signal.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('regime_signal', 'state'),
                    color: stateColor(formatSourceSnapshot('regime_signal').value),
                  })}
                  {renderStateFilterRow({
                    label: 'Rotation Signal',
                    value: formatSourceSnapshot('rotation_signal').value,
                    active: selectedSourceIds.includes('rotation_signal') && normalizeSourceMode(selectedSourceModes.rotation_signal ?? sourceConfigMap.rotation_signal.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('rotation_signal', 'state'),
                    color: stateColor(formatSourceSnapshot('rotation_signal').value),
                  })}
                  {!isBasket && renderStateFilterRow({
                    label: 'MR Signal',
                    value: formatSourceSnapshot('mr_signal').value,
                    active: selectedSourceIds.includes('mr_signal') && normalizeSourceMode(selectedSourceModes.mr_signal ?? sourceConfigMap.mr_signal.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('mr_signal', 'state'),
                    color: stateColor(formatSourceSnapshot('mr_signal').value),
                  })}
                </tbody>
              </table>
              </div>
            )}
            {renderContextToggle('PRICE EVENTS', 'priceEvents', true)}
            {contextSectionsOpen.priceEvents && (
              <div style={{ overflowX: 'auto', borderTop: `1px solid ${SHEET_GRID_BORDER}` }}>
              <table style={SHEET_TABLE_STYLE}>
                <colgroup>
                  <col style={{ width: STATE_TABLE_METRIC_COL_WIDTH }} />
                  <col style={{ width: STATE_TABLE_VALUE_COL_WIDTH }} />
                </colgroup>
                <thead>
                  <tr>
                    <th style={SHEET_HEADER_CELL_STYLE}>Metric</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>State</th>
                  </tr>
                </thead>
                <tbody>
                  {renderStateFilterRow({
                    label: 'Cross > Prev Up Rotation High',
                    value: formatSourceSnapshot('cross_uprot_high').value,
                    active: selectedSourceIds.includes('cross_uprot_high') && normalizeSourceMode(selectedSourceModes.cross_uprot_high ?? sourceConfigMap.cross_uprot_high.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('cross_uprot_high', 'state'),
                    color: eventStateColor(formatSourceSnapshot('cross_uprot_high').value, 'bullish'),
                  })}
                  {renderStateFilterRow({
                    label: 'Cross > Prev Breakout High',
                    value: formatSourceSnapshot('cross_breakout_high').value,
                    active: selectedSourceIds.includes('cross_breakout_high') && normalizeSourceMode(selectedSourceModes.cross_breakout_high ?? sourceConfigMap.cross_breakout_high.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('cross_breakout_high', 'state'),
                    color: eventStateColor(formatSourceSnapshot('cross_breakout_high').value, 'bullish'),
                  })}
                  {renderStateFilterRow({
                    label: 'Cross > Prev Candle High',
                    value: formatSourceSnapshot('cross_prev_high').value,
                    active: selectedSourceIds.includes('cross_prev_high') && normalizeSourceMode(selectedSourceModes.cross_prev_high ?? sourceConfigMap.cross_prev_high.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('cross_prev_high', 'state'),
                    color: eventStateColor(formatSourceSnapshot('cross_prev_high').value, 'bullish'),
                  })}
                  {renderStateFilterRow({
                    label: 'New 21-Bar High',
                    value: formatSourceSnapshot('new_high_21').value,
                    active: selectedSourceIds.includes('new_high_21') && normalizeSourceMode(selectedSourceModes.new_high_21 ?? sourceConfigMap.new_high_21.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('new_high_21', 'state'),
                    color: eventStateColor(formatSourceSnapshot('new_high_21').value, 'bullish'),
                  })}
                  {renderStateFilterRow({
                    label: 'New 63-Bar High',
                    value: formatSourceSnapshot('new_high_63').value,
                    active: selectedSourceIds.includes('new_high_63') && normalizeSourceMode(selectedSourceModes.new_high_63 ?? sourceConfigMap.new_high_63.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('new_high_63', 'state'),
                    color: eventStateColor(formatSourceSnapshot('new_high_63').value, 'bullish'),
                  })}
                  {renderStateFilterRow({
                    label: 'New 252-Bar High',
                    value: formatSourceSnapshot('new_high_252').value,
                    active: selectedSourceIds.includes('new_high_252') && normalizeSourceMode(selectedSourceModes.new_high_252 ?? sourceConfigMap.new_high_252.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('new_high_252', 'state'),
                    color: eventStateColor(formatSourceSnapshot('new_high_252').value, 'bullish'),
                  })}
                  {renderStateFilterRow({
                    label: 'Cross < Prev Down Rotation Low',
                    value: formatSourceSnapshot('cross_downrot_low').value,
                    active: selectedSourceIds.includes('cross_downrot_low') && normalizeSourceMode(selectedSourceModes.cross_downrot_low ?? sourceConfigMap.cross_downrot_low.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('cross_downrot_low', 'state'),
                    color: eventStateColor(formatSourceSnapshot('cross_downrot_low').value, 'bearish'),
                  })}
                  {renderStateFilterRow({
                    label: 'Cross < Prev Breakdown Low',
                    value: formatSourceSnapshot('cross_breakdown_low').value,
                    active: selectedSourceIds.includes('cross_breakdown_low') && normalizeSourceMode(selectedSourceModes.cross_breakdown_low ?? sourceConfigMap.cross_breakdown_low.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('cross_breakdown_low', 'state'),
                    color: eventStateColor(formatSourceSnapshot('cross_breakdown_low').value, 'bearish'),
                  })}
                  {renderStateFilterRow({
                    label: 'Cross < Prev Candle Low',
                    value: formatSourceSnapshot('cross_prev_low').value,
                    active: selectedSourceIds.includes('cross_prev_low') && normalizeSourceMode(selectedSourceModes.cross_prev_low ?? sourceConfigMap.cross_prev_low.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('cross_prev_low', 'state'),
                    color: eventStateColor(formatSourceSnapshot('cross_prev_low').value, 'bearish'),
                  })}
                  {renderStateFilterRow({
                    label: 'New 21-Bar Low',
                    value: formatSourceSnapshot('new_low_21').value,
                    active: selectedSourceIds.includes('new_low_21') && normalizeSourceMode(selectedSourceModes.new_low_21 ?? sourceConfigMap.new_low_21.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('new_low_21', 'state'),
                    color: eventStateColor(formatSourceSnapshot('new_low_21').value, 'bearish'),
                  })}
                  {renderStateFilterRow({
                    label: 'New 63-Bar Low',
                    value: formatSourceSnapshot('new_low_63').value,
                    active: selectedSourceIds.includes('new_low_63') && normalizeSourceMode(selectedSourceModes.new_low_63 ?? sourceConfigMap.new_low_63.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('new_low_63', 'state'),
                    color: eventStateColor(formatSourceSnapshot('new_low_63').value, 'bearish'),
                  })}
                  {renderStateFilterRow({
                    label: 'New 252-Bar Low',
                    value: formatSourceSnapshot('new_low_252').value,
                    active: selectedSourceIds.includes('new_low_252') && normalizeSourceMode(selectedSourceModes.new_low_252 ?? sourceConfigMap.new_low_252.modeOrder[0]) === 'state',
                    onClick: () => toggleSourceModeSelection('new_low_252', 'state'),
                    color: eventStateColor(formatSourceSnapshot('new_low_252').value, 'bearish'),
                  })}
                </tbody>
              </table>
              </div>
            )}
            {renderContextToggle('LONG TERM TREND', 'longTerm', true)}
            {contextSectionsOpen.longTerm && (
              <>
                {renderContextToggle('BREAKOUTS', 'breakouts')}
                {contextSectionsOpen.breakouts && renderRotationHistory('BREAKOUTS', 'Breakout')}
                {renderContextToggle('BREAKDOWNS', 'breakdowns')}
                {contextSectionsOpen.breakdowns && renderRotationHistory('BREAKDOWNS', 'Breakdown')}
              </>
            )}
            {renderContextToggle('SHORT TERM TREND', 'shortTerm', true)}
            {contextSectionsOpen.shortTerm && (
              <>
                {renderContextToggle('UP ROTATIONS', 'upRotations')}
                {contextSectionsOpen.upRotations && renderRotationHistory('UP ROTATIONS', 'UpRot')}
                {renderContextToggle('DOWN ROTATIONS', 'downRotations')}
                {contextSectionsOpen.downRotations && renderRotationHistory('DOWN ROTATIONS', 'DownRot')}
              </>
            )}
            {renderContextToggle('INDICATORS', 'indicators', true)}
            {contextSectionsOpen.indicators && (
              <div style={{ overflowX: 'auto', borderTop: `1px solid ${SHEET_GRID_BORDER}` }}>
              <table style={SHEET_TABLE_STYLE}>
                <colgroup>
                  <col style={{ width: 132 }} />
                  <col style={{ width: 112 }} />
                  <col style={{ width: 112 }} />
                  <col style={{ width: 112 }} />
                  <col style={{ width: 112 }} />
                </colgroup>
                <thead>
                  <tr>
                    <th style={SHEET_HEADER_CELL_STYLE}>Metric</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>Current</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>Percentile</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>{`Delta ${lookback}`}</th>
                    <th style={SHEET_HEADER_CELL_STYLE}>Delta %ile</th>
                  </tr>
                </thead>
                <tbody>
                  {renderOverviewRow({
                    label: 'RV (ann)',
                    value: `${num(ind.rv_ann, 1)}%`,
                    valueActive: selectedSourceIds.includes('rv') && (selectedSourceModes.rv ?? sourceConfigMap.rv.modeOrder[0]) === 'value',
                    onValueClick: () => toggleSourceModeSelection('rv', 'value'),
                    valueColor: ind.rv_pct == null ? undefined : grayCenteredColor(1 - 2 * ind.rv_pct),
                    percentile: pctD(ind.rv_pct),
                    percentileActive: selectedSourceIds.includes('rv') && (selectedSourceModes.rv ?? sourceConfigMap.rv.modeOrder[0]) === 'percentile',
                    onPercentileClick: () => toggleSourceModeSelection('rv', 'percentile'),
                    percentileColor: ind.rv_pct == null ? undefined : grayCenteredColor(1 - 2 * ind.rv_pct),
                    delta: signed(ind.rv_delta, 1),
                    deltaActive: selectedSourceIds.includes('rv') && normalizeSourceMode(selectedSourceModes.rv ?? sourceConfigMap.rv.modeOrder[0]) === 'delta_value',
                    onDeltaClick: () => toggleSourceModeSelection('rv', 'delta_value'),
                    deltaColor: ind.rv_delta_pct == null ? undefined : grayCenteredColor(1 - 2 * ind.rv_delta_pct),
                    deltaPct: pctD(ind.rv_delta_pct),
                    deltaPctActive: selectedSourceIds.includes('rv') && (selectedSourceModes.rv ?? sourceConfigMap.rv.modeOrder[0]) === 'delta_percentile',
                    onDeltaPctClick: () => toggleSourceModeSelection('rv', 'delta_percentile'),
                    deltaPctColor: ind.rv_delta_pct == null ? undefined : grayCenteredColor(1 - 2 * ind.rv_delta_pct),
                  })}
                  {renderOverviewRow({
                    label: 'H React',
                    value: num(ind.h_react, 2),
                    valueActive: selectedSourceIds.includes('h') && (selectedSourceModes.h ?? sourceConfigMap.h.modeOrder[0]) === 'value',
                    onValueClick: () => toggleSourceModeSelection('h', 'value'),
                    valueColor: ind.h_pct == null ? undefined : grayCenteredColor(2 * ind.h_pct - 1),
                    percentile: pctD(ind.h_pct),
                    percentileActive: selectedSourceIds.includes('h') && (selectedSourceModes.h ?? sourceConfigMap.h.modeOrder[0]) === 'percentile',
                    onPercentileClick: () => toggleSourceModeSelection('h', 'percentile'),
                    percentileColor: ind.h_pct == null ? undefined : grayCenteredColor(2 * ind.h_pct - 1),
                    delta: signed(ind.h_delta, 2),
                    deltaActive: selectedSourceIds.includes('h') && normalizeSourceMode(selectedSourceModes.h ?? sourceConfigMap.h.modeOrder[0]) === 'delta_value',
                    onDeltaClick: () => toggleSourceModeSelection('h', 'delta_value'),
                    deltaColor: ind.h_delta_pct == null ? undefined : grayCenteredColor(2 * ind.h_delta_pct - 1),
                    deltaPct: pctD(ind.h_delta_pct),
                    deltaPctActive: selectedSourceIds.includes('h') && (selectedSourceModes.h ?? sourceConfigMap.h.modeOrder[0]) === 'delta_percentile',
                    onDeltaPctClick: () => toggleSourceModeSelection('h', 'delta_percentile'),
                    deltaPctColor: ind.h_delta_pct == null ? undefined : grayCenteredColor(2 * ind.h_delta_pct - 1),
                  })}
                  {renderOverviewRow({
                    label: 'Price Change',
                    value: num(ind.p_react, 2),
                    valueActive: selectedSourceIds.includes('p') && (selectedSourceModes.p ?? sourceConfigMap.p.modeOrder[0]) === 'value',
                    onValueClick: () => toggleSourceModeSelection('p', 'value'),
                    valueColor: ind.p_pct == null ? undefined : grayCenteredColor(2 * ind.p_pct - 1),
                    percentile: pctD(ind.p_pct),
                    percentileActive: selectedSourceIds.includes('p') && (selectedSourceModes.p ?? sourceConfigMap.p.modeOrder[0]) === 'percentile',
                    onPercentileClick: () => toggleSourceModeSelection('p', 'percentile'),
                    percentileColor: ind.p_pct == null ? undefined : grayCenteredColor(2 * ind.p_pct - 1),
                    delta: signed(ind.p_delta, 2),
                    deltaActive: selectedSourceIds.includes('p') && normalizeSourceMode(selectedSourceModes.p ?? sourceConfigMap.p.modeOrder[0]) === 'delta_value',
                    onDeltaClick: () => toggleSourceModeSelection('p', 'delta_value'),
                    deltaColor: ind.p_delta_pct == null ? undefined : grayCenteredColor(2 * ind.p_delta_pct - 1),
                    deltaPct: pctD(ind.p_delta_pct),
                    deltaPctActive: selectedSourceIds.includes('p') && (selectedSourceModes.p ?? sourceConfigMap.p.modeOrder[0]) === 'delta_percentile',
                    onDeltaPctClick: () => toggleSourceModeSelection('p', 'delta_percentile'),
                    deltaPctColor: ind.p_delta_pct == null ? undefined : grayCenteredColor(2 * ind.p_delta_pct - 1),
                  })}
                  {renderOverviewRow({
                    label: 'L React',
                    value: num(ind.l_react, 2),
                    valueActive: selectedSourceIds.includes('l') && (selectedSourceModes.l ?? sourceConfigMap.l.modeOrder[0]) === 'value',
                    onValueClick: () => toggleSourceModeSelection('l', 'value'),
                    valueColor: ind.l_pct == null ? undefined : grayCenteredColor(2 * ind.l_pct - 1),
                    percentile: pctD(ind.l_pct),
                    percentileActive: selectedSourceIds.includes('l') && (selectedSourceModes.l ?? sourceConfigMap.l.modeOrder[0]) === 'percentile',
                    onPercentileClick: () => toggleSourceModeSelection('l', 'percentile'),
                    percentileColor: ind.l_pct == null ? undefined : grayCenteredColor(2 * ind.l_pct - 1),
                    delta: signed(ind.l_delta, 2),
                    deltaActive: selectedSourceIds.includes('l') && normalizeSourceMode(selectedSourceModes.l ?? sourceConfigMap.l.modeOrder[0]) === 'delta_value',
                    onDeltaClick: () => toggleSourceModeSelection('l', 'delta_value'),
                    deltaColor: ind.l_delta_pct == null ? undefined : grayCenteredColor(2 * ind.l_delta_pct - 1),
                    deltaPct: pctD(ind.l_delta_pct),
                    deltaPctActive: selectedSourceIds.includes('l') && (selectedSourceModes.l ?? sourceConfigMap.l.modeOrder[0]) === 'delta_percentile',
                    onDeltaPctClick: () => toggleSourceModeSelection('l', 'delta_percentile'),
                    deltaPctColor: ind.l_delta_pct == null ? undefined : grayCenteredColor(2 * ind.l_delta_pct - 1),
                  })}
                  {isBasket && renderOverviewRow({
                    label: 'Breadth',
                    value: `${num(ind.breadth, 1)}%`,
                    valueActive: selectedSourceIds.includes('breadth') && (selectedSourceModes.breadth ?? sourceConfigMap.breadth.modeOrder[0]) === 'value',
                    onValueClick: () => toggleSourceModeSelection('breadth', 'value'),
                    valueColor: ind.breadth == null ? undefined : grayCenteredColor(2 * (ind.breadth / 100) - 1),
                    percentile: pctD(ind.breadth_pct),
                    delta: signed(ind.breadth_delta, 1),
                    deltaActive: selectedSourceIds.includes('breadth') && normalizeSourceMode(selectedSourceModes.breadth ?? sourceConfigMap.breadth.modeOrder[0]) === 'delta_value',
                    onDeltaClick: () => toggleSourceModeSelection('breadth', 'delta_value'),
                    deltaColor: ind.breadth_delta_pct == null ? undefined : grayCenteredColor(2 * ind.breadth_delta_pct - 1),
                    deltaPct: pctD(ind.breadth_delta_pct),
                    percentileColor: ind.breadth_pct == null ? undefined : grayCenteredColor(2 * ind.breadth_pct - 1),
                    deltaPctActive: selectedSourceIds.includes('breadth') && (selectedSourceModes.breadth ?? sourceConfigMap.breadth.modeOrder[0]) === 'delta_percentile',
                    onDeltaPctClick: () => toggleSourceModeSelection('breadth', 'delta_percentile'),
                    deltaPctColor: ind.breadth_delta_pct == null ? undefined : grayCenteredColor(2 * ind.breadth_delta_pct - 1),
                  })}
                  {isBasket && renderOverviewRow({
                    label: 'Breakout %',
                    value: `${num(ind.breakout_pct, 1)}%`,
                    valueActive: selectedSourceIds.includes('breakout_pct') && (selectedSourceModes.breakout_pct ?? sourceConfigMap.breakout_pct.modeOrder[0]) === 'value',
                    onValueClick: () => toggleSourceModeSelection('breakout_pct', 'value'),
                    valueColor: ind.breakout_pct == null ? undefined : grayCenteredColor(2 * (ind.breakout_pct / 100) - 1),
                    percentile: pctD(ind.breakout_level_pct),
                    delta: signed(ind.breakout_delta, 1),
                    deltaActive: selectedSourceIds.includes('breakout_pct') && normalizeSourceMode(selectedSourceModes.breakout_pct ?? sourceConfigMap.breakout_pct.modeOrder[0]) === 'delta_value',
                    onDeltaClick: () => toggleSourceModeSelection('breakout_pct', 'delta_value'),
                    deltaColor: ind.breakout_delta_pct == null ? undefined : grayCenteredColor(2 * ind.breakout_delta_pct - 1),
                    deltaPct: pctD(ind.breakout_delta_pct),
                    percentileColor: ind.breakout_level_pct == null ? undefined : grayCenteredColor(2 * ind.breakout_level_pct - 1),
                    deltaPctActive: selectedSourceIds.includes('breakout_pct') && (selectedSourceModes.breakout_pct ?? sourceConfigMap.breakout_pct.modeOrder[0]) === 'delta_percentile',
                    onDeltaPctClick: () => toggleSourceModeSelection('breakout_pct', 'delta_percentile'),
                    deltaPctColor: ind.breakout_delta_pct == null ? undefined : grayCenteredColor(2 * ind.breakout_delta_pct - 1),
                  })}
                  {isBasket && renderOverviewRow({
                    label: 'Corr %',
                    value: `${num(ind.corr_pct, 1)}%`,
                    valueActive: selectedSourceIds.includes('corr') && (selectedSourceModes.corr ?? sourceConfigMap.corr.modeOrder[0]) === 'value',
                    onValueClick: () => toggleSourceModeSelection('corr', 'value'),
                    valueColor: ind.corr_pct == null ? undefined : grayCenteredColor(1 - 2 * (ind.corr_pct / 100)),
                    percentile: pctD(ind.corr_level_pct),
                    delta: signed(ind.corr_delta, 1),
                    deltaActive: selectedSourceIds.includes('corr') && normalizeSourceMode(selectedSourceModes.corr ?? sourceConfigMap.corr.modeOrder[0]) === 'delta_value',
                    onDeltaClick: () => toggleSourceModeSelection('corr', 'delta_value'),
                    deltaColor: ind.corr_delta_pct == null ? undefined : grayCenteredColor(1 - 2 * ind.corr_delta_pct),
                    deltaPct: pctD(ind.corr_delta_pct),
                    percentileColor: ind.corr_level_pct == null ? undefined : grayCenteredColor(1 - 2 * ind.corr_level_pct),
                    deltaPctActive: selectedSourceIds.includes('corr') && (selectedSourceModes.corr ?? sourceConfigMap.corr.modeOrder[0]) === 'delta_percentile',
                    onDeltaPctClick: () => toggleSourceModeSelection('corr', 'delta_percentile'),
                    deltaPctColor: ind.corr_delta_pct == null ? undefined : grayCenteredColor(1 - 2 * ind.corr_delta_pct),
                  })}
                </tbody>
              </table>
              </div>
            )}
          </SectionCard>
        )
      })()}
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
        <div style={{ fontSize: 11, color: PINK }}>Small sample â€” filtered N &lt; 30. Treat with caution.</div>
      )}
      {error && <div style={{ fontSize: 11, color: PINK }}>{error}</div>}

      {/* Forward paths (Phase 4) â€” tercile-colored winner/loser chart + match list */}
      <div style={{ border: `1px solid ${SOLAR_BORDER}`, background: SOLAR_BG }}>
        <div style={{ padding: '6px 8px', color: SOLAR_MUTED, fontSize: 10, borderBottom: `1px solid ${SOLAR_BORDER}`, display: 'flex', justifyContent: 'space-between' }}>
          <span>FORWARD PATHS {data?.horizon != null ? `â€” horizon ${data.horizon}${typeof data.horizon === 'number' ? ' bars' : ''}` : ''}</span>
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
                DATE {fwdSortBy === 'date' ? (fwdSortDir === 'asc' ? 'â†‘' : 'â†“') : ''}
              </div>
              <div style={{ padding: '4px 8px', textAlign: 'right', cursor: 'pointer' }} onClick={() => {
                if (fwdSortBy === 'return') setFwdSortDir(d => d === 'asc' ? 'desc' : 'asc')
                else { setFwdSortBy('return'); setFwdSortDir('desc') }
              }}>
                RETURN {fwdSortBy === 'return' ? (fwdSortDir === 'asc' ? 'â†‘' : 'â†“') : ''}
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
                      <div>{p.date ?? 'â€”'}</div>
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
