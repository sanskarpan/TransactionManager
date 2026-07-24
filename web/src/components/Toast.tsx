import { createContext, useCallback, useContext, useEffect, useRef, useState } from 'react'

interface ToastItem {
  id: number
  message: string
  type: 'success' | 'error' | 'info'
}

interface ToastContextValue {
  showToast: (message: string, type?: ToastItem['type']) => void
}

const ToastContext = createContext<ToastContextValue>({ showToast: () => {} })

export function useToast() {
  return useContext(ToastContext)
}

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<ToastItem[]>([])
  // FE-09: id counter lives in a ref (per provider instance) instead of
  // module-scope, so HMR and a second mounted provider do not collide.
  const nextIdRef = useRef(0)
  // FE-08: track every dismiss timer so the provider's unmount cleanup
  // clears them all and avoids setState-on-unmounted-component.
  const timersRef = useRef<Set<ReturnType<typeof setTimeout>>>(new Set())

  useEffect(() => {
    return () => {
      for (const t of timersRef.current) clearTimeout(t)
      timersRef.current.clear()
    }
  }, [])

  const showToast = useCallback((message: string, type: ToastItem['type'] = 'info') => {
    const id = ++nextIdRef.current
    setToasts((prev) => [...prev, { id, message, type }])
    const handle = setTimeout(() => {
      timersRef.current.delete(handle)
      setToasts((prev) => prev.filter((t) => t.id !== id))
    }, 3500)
    timersRef.current.add(handle)
  }, [])

  return (
    <ToastContext.Provider value={{ showToast }}>
      {children}
      <div className="fixed bottom-4 right-4 space-y-2 z-50">
        {toasts.map((t) => (
          <div
            key={t.id}
            className={`px-4 py-3 rounded-lg shadow-lg text-sm font-medium max-w-xs transition-all ${
              t.type === 'error' ? 'bg-red-600 text-white' :
              t.type === 'success' ? 'bg-green-600 text-white' :
              'bg-gray-800 text-white'
            }`}
          >
            {t.message}
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  )
}
