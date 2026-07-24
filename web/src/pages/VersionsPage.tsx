import { useState, useCallback, useEffect, useRef } from 'react'
import { infraApi, txnApi } from '../api'
import { usePolling } from '../hooks/usePolling'
import type { VersionChain, Transaction } from '../types'

function approxVisible(txnId: number, xmin: number, xmax: number): { visible: boolean; reason: string } {
  // Simplified client-side visibility approximation (educational only)
  if (xmin === txnId) return { visible: true, reason: 'own write' }
  if (xmin > txnId) return { visible: false, reason: `xmin ${xmin} > txn ${txnId}` }
  if (xmax !== 0 && xmax <= txnId) return { visible: false, reason: `deleted by ${xmax}` }
  return { visible: true, reason: `xmin ${xmin} committed` }
}

export function VersionsPage() {
  const [table, setTable] = useState('accounts')
  const [key, setKey] = useState('1')
  const [chain, setChain] = useState<VersionChain | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [vacuumMsg, setVacuumMsg] = useState('')
  const [activeTxns, setActiveTxns] = useState<Transaction[]>([])

  // FE-05: track the vacuum-message timer so unmount cleans it up;
  // rapid vacuum clicks otherwise stack timers that race to clear the
  // same message string.
  const vacuumTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
  useEffect(() => {
    return () => {
      if (vacuumTimerRef.current) clearTimeout(vacuumTimerRef.current)
    }
  }, [])

  const fetchActiveTxns = useCallback(async () => {
    try { setActiveTxns(await txnApi.active()) } catch {}
  }, [])
  usePolling(fetchActiveTxns, 2000)

  const fetchChain = useCallback(async () => {
    setLoading(true)
    setError('')
    try {
      const c = await infraApi.versionChain(table, key)
      setChain(c)
    } catch (e) {
      setError(String(e))
      setChain(null)
    } finally {
      setLoading(false)
    }
  }, [table, key])

  const handleVacuum = async () => {
    try {
      await infraApi.vacuum()
      setVacuumMsg('Vacuum completed!')
      if (vacuumTimerRef.current) clearTimeout(vacuumTimerRef.current)
      vacuumTimerRef.current = setTimeout(() => setVacuumMsg(''), 3000)
      if (chain) fetchChain()
    } catch (e) {
      setVacuumMsg(`Error: ${e}`)
    }
  }

  return (
    <div className="p-6 space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">MVCC Version Chains</h1>

      {/* Lookup form */}
      <div className="bg-white rounded-lg border border-gray-200 p-4 flex flex-wrap gap-4 items-end">
        <div>
          <label className="text-xs text-gray-500 block mb-1">Table</label>
          <input value={table} onChange={(e) => setTable(e.target.value)}
            className="border border-gray-200 rounded px-3 py-1.5 text-sm w-40" />
        </div>
        <div>
          <label className="text-xs text-gray-500 block mb-1">Key</label>
          <input value={key} onChange={(e) => setKey(e.target.value)}
            className="border border-gray-200 rounded px-3 py-1.5 text-sm w-32" />
        </div>
        <button onClick={fetchChain}
          className="bg-blue-600 text-white rounded px-4 py-1.5 text-sm font-medium hover:bg-blue-700">
          Load Chain
        </button>
        <div className="ml-auto flex items-end gap-3">
          <button onClick={handleVacuum}
            className="bg-amber-500 text-white rounded px-4 py-1.5 text-sm font-medium hover:bg-amber-600">
            Run Vacuum
          </button>
          {vacuumMsg && <span className="text-sm text-green-600">{vacuumMsg}</span>}
        </div>
      </div>

      {loading && <p className="text-gray-500">Loading...</p>}
      {error && <p className="text-red-600 text-sm">{error}</p>}

      {chain && (
        <>
          {/* Version timeline */}
          <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
            <div className="px-4 py-3 border-b border-gray-200">
              <h2 className="font-semibold text-gray-700">
                {chain.table} / {chain.key} — {chain.versions.length} version(s)
              </h2>
            </div>
            <div className="p-4 flex gap-4 overflow-x-auto">
              {chain.versions.map((v, i) => (
                <div key={i} className="flex-shrink-0 w-48 border border-gray-200 rounded-lg p-3 bg-gray-50">
                  <div className="flex flex-wrap gap-1 mb-2">
                    <span className={`text-xs px-1.5 py-0.5 rounded font-mono ${
                      v.xmin === 0 ? 'bg-green-100 text-green-700' : 'bg-blue-100 text-blue-700'
                    }`}>xmin={v.xmin}</span>
                    {v.xmax !== 0 && (
                      <span className="text-xs px-1.5 py-0.5 rounded font-mono bg-red-100 text-red-700">
                        xmax={v.xmax}
                      </span>
                    )}
                  </div>
                  {v.xmax !== 0 && <div className="text-xs text-gray-400 mb-1">[deleted]</div>}
                  <div className="text-xs font-mono text-gray-600 space-y-0.5">
                    {v.data.map((d, j) => <div key={j} className="truncate">{d}</div>)}
                  </div>
                  {i < chain.versions.length - 1 && (
                    <div className="text-center text-gray-400 mt-2 text-xs">← prev</div>
                  )}
                </div>
              ))}
            </div>
          </div>

          {/* Visibility matrix */}
          {activeTxns.length > 0 && (
            <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
              <div className="px-4 py-3 border-b border-gray-200">
                <h2 className="font-semibold text-gray-700">Visibility Matrix</h2>
                <p className="text-xs text-gray-400 mt-0.5">Approximate client-side visibility based on xmin/xmax</p>
              </div>
              <div className="overflow-x-auto p-4">
                <table className="text-sm">
                  <thead>
                    <tr>
                      <th className="pr-4 pb-2 text-left text-xs text-gray-500 font-medium">Transaction</th>
                      {chain.versions.map((v, i) => (
                        <th key={i} className="px-3 pb-2 text-center text-xs text-gray-500 font-medium">
                          V{i + 1}<br />
                          <span className="font-mono text-gray-400">xmin={v.xmin}</span>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {activeTxns.map((txn) => (
                      <tr key={txn.id} className="border-t border-gray-100">
                        <td className="pr-4 py-2">
                          <span className="font-mono text-gray-700">T{txn.id}</span>
                          <span className="ml-1.5 text-xs text-gray-400 uppercase">{txn.protocol}/{txn.isolation.substring(0, 2).toUpperCase()}</span>
                        </td>
                        {chain.versions.map((v, i) => {
                          const { visible, reason } = approxVisible(txn.id, v.xmin, v.xmax)
                          return (
                            <td key={i} className="px-3 py-2 text-center">
                              <span
                                title={reason}
                                className={`inline-block w-6 h-6 rounded-full text-sm flex items-center justify-center cursor-help ${
                                  visible ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-500'
                                }`}
                              >
                                {visible ? '✓' : '✗'}
                              </span>
                            </td>
                          )
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {activeTxns.length === 0 && (
            <p className="text-sm text-gray-400 italic">Start some transactions in the Playground to see the visibility matrix</p>
          )}
        </>
      )}
    </div>
  )
}
