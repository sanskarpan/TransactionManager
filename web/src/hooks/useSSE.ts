import { useEffect, useRef } from 'react'

// useSSE subscribes to a server-sent-events stream with exponential
// backoff reconnect (H-26: previously a fixed 2s loop with no cap and no
// disconnect signal, so WFGPage's "Live" indicator stayed green after the
// connection dropped and reconnection was failing).
//
// Lifecycle invariants:
//   - onEvent is called for every server message while the connection is open.
//   - onConnect is called once when the stream opens (and on every successful
//     reconnect). Use it to flip a "Live" indicator true so the polling
//     fallback can be paused without flickering through the reconnect window.
//   - onDisconnect is called when the connection errors out. After it fires,
//     the hook schedules a backoff reconnect; onConnect will fire again when
//     the next stream opens.
//   - None of the callbacks are invoked after the consuming component unmounts:
//     a `closed` flag suppresses post-unmount callbacks (FE-03).
export function useSSE(
  url: string,
  onEvent: (event: MessageEvent) => void,
  onDisconnect?: () => void,
  onConnect?: () => void,
) {
  const onEventRef = useRef(onEvent)
  onEventRef.current = onEvent
  const onDisconnectRef = useRef(onDisconnect)
  onDisconnectRef.current = onDisconnect
  const onConnectRef = useRef(onConnect)
  onConnectRef.current = onConnect

  useEffect(() => {
    let es: EventSource | null = null
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined
    let attempt = 0
    let closed = false

    const connect = () => {
      if (closed) return
      es = new EventSource(url)
      es.onmessage = (e) => {
        if (!closed) onEventRef.current(e)
      }
      es.onopen = () => {
        if (closed) return
        attempt = 0
        onConnectRef.current?.()
      }
      es.onerror = () => {
        // FE-03: guard against post-unmount callbacks. Previously the
        // closed check was after the onDisconnect call, so a final
        // post-close error fired setState on an unmounted component.
        if (closed) return
        es?.close()
        onDisconnectRef.current?.()
        if (reconnectTimer) clearTimeout(reconnectTimer)
        const delay = Math.min(30_000, 1_000 * 2 ** attempt)
        attempt++
        reconnectTimer = setTimeout(connect, delay)
      }
    }

    connect()
    return () => {
      closed = true
      if (reconnectTimer) clearTimeout(reconnectTimer)
      es?.close()
    }
  }, [url])
}
