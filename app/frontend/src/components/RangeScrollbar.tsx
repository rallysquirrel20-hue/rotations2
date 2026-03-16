import { useRef, useEffect, useCallback } from 'react'

interface RangeScrollbarProps {
  /** Total number of data points */
  total: number
  /** Start index of visible range (0-based) */
  start: number
  /** End index of visible range (0-based, inclusive) */
  end: number
  /** Called when the user drags the scrollbar thumb */
  onChange: (start: number, end: number) => void
}

export function RangeScrollbar({ total, start, end, onChange }: RangeScrollbarProps) {
  const trackRef = useRef<HTMLDivElement>(null)
  const dragRef = useRef<{ type: 'thumb' | 'left' | 'right'; startX: number; origStart: number; origEnd: number } | null>(null)

  const range = end - start
  const leftPct = total > 1 ? (start / (total - 1)) * 100 : 0
  const widthPct = total > 1 ? (range / (total - 1)) * 100 : 100
  const minWidthPct = total > 1 ? Math.max(1, (10 / (total - 1)) * 100) : 100

  const handleMouseDown = useCallback((e: React.MouseEvent, type: 'thumb' | 'left' | 'right') => {
    e.preventDefault()
    e.stopPropagation()
    dragRef.current = { type, startX: e.clientX, origStart: start, origEnd: end }
    document.body.style.cursor = type === 'thumb' ? 'grabbing' : 'ew-resize'
  }, [start, end])

  useEffect(() => {
    const handleMove = (e: MouseEvent) => {
      const drag = dragRef.current
      const track = trackRef.current
      if (!drag || !track) return
      const trackW = track.getBoundingClientRect().width
      const dx = e.clientX - drag.startX
      const indexDelta = Math.round((dx / trackW) * (total - 1))

      if (drag.type === 'thumb') {
        let ns = drag.origStart + indexDelta
        let ne = drag.origEnd + indexDelta
        const r = drag.origEnd - drag.origStart
        if (ns < 0) { ns = 0; ne = r }
        if (ne >= total) { ne = total - 1; ns = ne - r }
        ns = Math.max(0, ns)
        onChange(ns, ne)
      } else if (drag.type === 'left') {
        let ns = Math.max(0, Math.min(drag.origEnd - 10, drag.origStart + indexDelta))
        onChange(ns, drag.origEnd)
      } else {
        let ne = Math.min(total - 1, Math.max(drag.origStart + 10, drag.origEnd + indexDelta))
        onChange(drag.origStart, ne)
      }
    }

    const handleUp = () => {
      if (dragRef.current) {
        document.body.style.cursor = ''
        dragRef.current = null
      }
    }

    window.addEventListener('mousemove', handleMove)
    window.addEventListener('mouseup', handleUp)
    return () => {
      window.removeEventListener('mousemove', handleMove)
      window.removeEventListener('mouseup', handleUp)
    }
  }, [total, onChange])

  if (total < 2) return null

  return (
    <div className="range-scrollbar-track" ref={trackRef}>
      <div
        className="range-scrollbar-thumb"
        style={{ left: `${leftPct}%`, width: `${Math.max(widthPct, minWidthPct)}%` }}
        onMouseDown={e => handleMouseDown(e, 'thumb')}
      >
        <div className="range-scrollbar-edge left" onMouseDown={e => handleMouseDown(e, 'left')} />
        <div className="range-scrollbar-edge right" onMouseDown={e => handleMouseDown(e, 'right')} />
      </div>
    </div>
  )
}
