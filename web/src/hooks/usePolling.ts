import { useEffect, useRef } from 'react'

// usePolling calls `fn` immediately and then every `intervalMs` while
// `enabled` is true.
//
// H-27: `fn` is held in a ref so the interval is stable across re-renders
// — callers need not memoize it. Previously `fn` was in the effect
// dependency array, so a callback whose identity changed every render
// (e.g. one closing over changing state) tore down and recreated the
// interval each render, defeating polling and re-firing the immediate
// call. The ref also lets an async `fn` overlap; we do not abort in-flight
// calls here (callers can pass their own guard), but the stable interval
// prevents the worst churn.
export function usePolling(fn: () => void, intervalMs: number, enabled = true) {
  const fnRef = useRef(fn)
  fnRef.current = fn

  useEffect(() => {
    if (!enabled) return
    fnRef.current()
    const id = setInterval(() => fnRef.current(), intervalMs)
    return () => clearInterval(id)
  }, [intervalMs, enabled])
}
