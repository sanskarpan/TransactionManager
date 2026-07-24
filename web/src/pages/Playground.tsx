import { useState, useRef, useCallback, useEffect } from 'react'
import { txnApi, infraApi } from '../api'
import type { LockQueueSnapshot } from '../types'
import { usePolling } from '../hooks/usePolling'
import { useToast } from '../components/Toast'

interface LogEntry {
  ts: string
  txn: string
  op: string
  result: string
  error?: string
}

interface TxnTab {
  label: string
  id: number | null
  protocol: string
  isolation: string
}

const defaultTabs: TxnTab[] = [
  { label: 'T1', id: null, protocol: 'mvcc', isolation: 'read_committed' },
  { label: 'T2', id: null, protocol: 'mvcc', isolation: 'read_committed' },
  { label: 'T3', id: null, protocol: 'mvcc', isolation: 'read_committed' },
]

const QUICK_TESTS = [
  { label: 'Read acct 1', op: 'read', table: 'accounts', key: '1', values: '' },
  { label: 'Write acct 1 balance', op: 'write', table: 'accounts', key: '1', values: '[{"int":1},{"text":"owner-1"},{"float":9999},{"text":"north"}]' },
  { label: 'Scan accounts', op: 'scan', table: 'accounts', key: '', values: '' },
]

export function Playground() {
  const [tabs, setTabs] = useState<TxnTab[]>(defaultTabs)
  const [active, setActive] = useState(0)
  const [log, setLog] = useState<LogEntry[]>([])
  const [table, setTable] = useState('accounts')
  const [key, setKey] = useState('1')
  const [valueStr, setValueStr] = useState('')
  const [op, setOp] = useState<'read' | 'write' | 'scan' | 'insert' | 'delete'>('read')
  const [savepointName, setSavepointName] = useState('sp1')
  const [locks, setLocks] = useState<LockQueueSnapshot[]>([])
  const logRef = useRef<HTMLDivElement>(null)
  // FE-04: track autoscroll timers so the unmount cleanup can clear
  // them (otherwise a queued timer fires setState-equivalent on a
  // detached ref after unmount).
  const scrollTimers = useRef<Set<ReturnType<typeof setTimeout>>>(new Set())
  useEffect(() => {
    return () => {
      for (const t of scrollTimers.current) clearTimeout(t)
      scrollTimers.current.clear()
    }
  }, [])
  const { showToast } = useToast()

  const addLog = (txnLabel: string, opName: string, result: string, error?: string) => {
    const entry: LogEntry = { ts: new Date().toLocaleTimeString(), txn: txnLabel, op: opName, result, error }
    setLog((l) => [...l, entry])
    const handle = setTimeout(() => logRef.current?.scrollTo({ top: logRef.current.scrollHeight, behavior: 'smooth' }), 50)
    scrollTimers.current.add(handle)
  }

  const updateTab = (idx: number, patch: Partial<TxnTab>) =>
    setTabs((t) => t.map((tab, i) => i === idx ? { ...tab, ...patch } : tab))

  const tab = tabs[active]

  // Fetch lock state periodically
  const fetchLocks = useCallback(async () => {
    try {
      const data = await infraApi.locks()
      setLocks(Array.isArray(data) ? data : [])
    } catch {}
  }, [])
  usePolling(fetchLocks, 1000)

  // Ctrl+Enter to execute. FE-02: hold handleOp in a ref so the listener
  // is registered exactly once per mount and the latest closure is
  // always invoked, regardless of how often the deps change.
  const handleOpRef = useRef<(() => void) | null>(null)
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') handleOpRef.current?.()
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [])

  const handleBegin = async () => {
    try {
      const res = await txnApi.begin(tab.protocol, tab.isolation)
      updateTab(active, { id: res.id })
      addLog(tab.label, 'BEGIN', `txn #${res.id}`)
    } catch (e) {
      addLog(tab.label, 'BEGIN', '', String(e))
      showToast(String(e), 'error')
    }
  }

  const handleOp = async () => {
    if (!tab.id) return addLog(tab.label, op.toUpperCase(), '', 'No active transaction')
    try {
      let result = ''
      switch (op) {
        case 'read': {
          const r = await txnApi.read(tab.id, table, key)
          result = r.found ? JSON.stringify(r.values) : 'not found'
          break
        }
        case 'write': {
          const vals = JSON.parse(valueStr || '[]')
          await txnApi.write(tab.id, table, key, vals)
          result = 'ok'
          break
        }
        case 'scan': {
          const r = await txnApi.scan(tab.id, table)
          result = `${r.rows.length} rows`
          break
        }
        case 'insert': {
          const vals = JSON.parse(valueStr || '[]')
          await txnApi.insert(tab.id, table, key, vals)
          result = 'ok'
          break
        }
        case 'delete': {
          await txnApi.delete(tab.id, table, key)
          result = 'ok'
          break
        }
      }
      addLog(tab.label, op.toUpperCase(), result)
    } catch (e) {
      const msg = String(e)
      addLog(tab.label, op.toUpperCase(), '', msg)
      if (msg.includes('deadlock')) showToast('Deadlock detected — transaction aborted', 'error')
      else if (msg.includes('write_conflict')) showToast('Write conflict — try again', 'error')
      else if (msg.includes('serialization')) showToast('Serialization failure — transaction aborted', 'error')
    }
  }

  const handleCommit = async () => {
    if (!tab.id) return
    try {
      await txnApi.commit(tab.id)
      addLog(tab.label, 'COMMIT', 'ok')
      updateTab(active, { id: null })
    } catch (e) {
      addLog(tab.label, 'COMMIT', '', String(e))
      showToast(String(e), 'error')
    }
  }

  // FE-02: keep the keyboard-handler ref in sync with the latest handleOp
  // closure so the global Ctrl+Enter listener (registered once) always
  // invokes the current implementation.
  handleOpRef.current = handleOp

  const handleAbort = async () => {
    if (!tab.id) return
    try {
      await txnApi.abort(tab.id)
      addLog(tab.label, 'ABORT', 'ok')
      updateTab(active, { id: null })
    } catch (e) {
      addLog(tab.label, 'ABORT', '', String(e))
    }
  }

  const handleSavepoint = async () => {
    if (!tab.id) return
    try {
      await txnApi.savepoint(tab.id, savepointName)
      addLog(tab.label, 'SAVEPOINT', savepointName)
    } catch (e) {
      addLog(tab.label, 'SAVEPOINT', '', String(e))
    }
  }

  const handleRollbackTo = async () => {
    if (!tab.id) return
    try {
      await txnApi.rollbackTo(tab.id, savepointName)
      addLog(tab.label, 'ROLLBACK TO', savepointName)
    } catch (e) {
      addLog(tab.label, 'ROLLBACK TO', '', String(e))
    }
  }

  const applyQuickTest = (qt: typeof QUICK_TESTS[0]) => {
    setOp(qt.op as typeof op)
    setTable(qt.table)
    setKey(qt.key)
    setValueStr(qt.values)
  }

  // Filter locks relevant to the current tab's txn
  const myLocks = tab.id ? locks.filter((q) =>
    q.granted?.some((g: { txnId: number }) => g.txnId === tab.id) ||
    q.waiting?.some((w: { txnId: number }) => w.txnId === tab.id)
  ) : []

  return (
    <div className="p-4 flex gap-4 h-[calc(100vh-56px)] overflow-hidden">
      {/* Left panel: transaction controls */}
      <div className="w-72 flex flex-col gap-3 flex-shrink-0 overflow-y-auto">
        {/* Tab selector */}
        <div className="flex gap-1.5">
          {tabs.map((t, i) => (
            <button
              key={i}
              onClick={() => setActive(i)}
              className={`flex-1 py-2 rounded font-medium text-sm ${
                active === i ? 'bg-blue-600 text-white' : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
              }`}
            >
              {t.label} {t.id ? `#${t.id}` : ''}
            </button>
          ))}
        </div>

        {/* Transaction settings */}
        <div className="bg-white rounded-lg border border-gray-200 p-3 space-y-2.5">
          <div>
            <label className="text-xs text-gray-500 block mb-1">Protocol</label>
            <select value={tab.protocol} onChange={(e) => updateTab(active, { protocol: e.target.value })}
              disabled={!!tab.id} className="w-full border border-gray-200 rounded px-2 py-1.5 text-sm">
              <option value="mvcc">MVCC</option>
              <option value="2pl">2PL</option>
            </select>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">Isolation</label>
            <select value={tab.isolation} onChange={(e) => updateTab(active, { isolation: e.target.value })}
              disabled={!!tab.id} className="w-full border border-gray-200 rounded px-2 py-1.5 text-sm">
              <option value="read_uncommitted">Read Uncommitted</option>
              <option value="read_committed">Read Committed</option>
              <option value="repeatable_read">Repeatable Read</option>
              <option value="serializable">Serializable</option>
            </select>
          </div>
          <button onClick={handleBegin} disabled={!!tab.id}
            className="w-full bg-green-600 text-white rounded py-1.5 text-sm font-medium disabled:opacity-50 hover:bg-green-700">
            BEGIN
          </button>
        </div>

        {/* Quick tests */}
        <div className="bg-white rounded-lg border border-gray-200 p-3">
          <p className="text-xs text-gray-500 font-medium mb-2">Quick Tests</p>
          <div className="space-y-1">
            {QUICK_TESTS.map((qt) => (
              <button key={qt.label} onClick={() => applyQuickTest(qt)}
                className="w-full text-left text-xs px-2 py-1.5 rounded bg-gray-50 hover:bg-gray-100 text-gray-700">
                {qt.label}
              </button>
            ))}
          </div>
        </div>

        {/* Operation builder */}
        <div className="bg-white rounded-lg border border-gray-200 p-3 space-y-2.5">
          <div>
            <label className="text-xs text-gray-500 block mb-1">Operation</label>
            <select value={op} onChange={(e) => setOp(e.target.value as typeof op)}
              className="w-full border border-gray-200 rounded px-2 py-1.5 text-sm">
              <option value="read">READ</option>
              <option value="write">WRITE</option>
              <option value="scan">SCAN</option>
              <option value="insert">INSERT</option>
              <option value="delete">DELETE</option>
            </select>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">Table</label>
            <input value={table} onChange={(e) => setTable(e.target.value)}
              className="w-full border border-gray-200 rounded px-2 py-1.5 text-sm" />
          </div>
          {op !== 'scan' && (
            <div>
              <label className="text-xs text-gray-500 block mb-1">Key</label>
              <input value={key} onChange={(e) => setKey(e.target.value)}
                className="w-full border border-gray-200 rounded px-2 py-1.5 text-sm" />
            </div>
          )}
          {(op === 'write' || op === 'insert') && (
            <div>
              <label className="text-xs text-gray-500 block mb-1">Values (JSON array)</label>
              <textarea value={valueStr} onChange={(e) => setValueStr(e.target.value)}
                rows={2} placeholder='[{"int":1},{"text":"owner"},{"float":100},{"text":"north"}]'
                className="w-full border border-gray-200 rounded px-2 py-1.5 text-sm font-mono resize-none" />
            </div>
          )}
          <button onClick={handleOp} disabled={!tab.id}
            className="w-full bg-blue-600 text-white rounded py-1.5 text-sm font-medium disabled:opacity-50 hover:bg-blue-700">
            EXECUTE <span className="text-blue-200 text-xs ml-1">⌘↵</span>
          </button>
        </div>

        {/* Commit/Abort */}
        <div className="flex gap-2">
          <button onClick={handleCommit} disabled={!tab.id}
            className="flex-1 bg-blue-600 text-white rounded py-1.5 text-sm font-medium disabled:opacity-50 hover:bg-blue-700">
            COMMIT
          </button>
          <button onClick={handleAbort} disabled={!tab.id}
            className="flex-1 bg-red-600 text-white rounded py-1.5 text-sm font-medium disabled:opacity-50 hover:bg-red-700">
            ABORT
          </button>
        </div>

        {/* Savepoints */}
        <div className="bg-white rounded-lg border border-gray-200 p-3 space-y-2">
          <p className="text-xs text-gray-500 font-medium">Savepoints</p>
          <input value={savepointName} onChange={(e) => setSavepointName(e.target.value)}
            placeholder="savepoint name" className="w-full border border-gray-200 rounded px-2 py-1 text-sm" />
          <div className="flex gap-2">
            <button onClick={handleSavepoint} disabled={!tab.id}
              className="flex-1 bg-gray-600 text-white rounded py-1 text-xs font-medium disabled:opacity-50 hover:bg-gray-700">
              SAVE
            </button>
            <button onClick={handleRollbackTo} disabled={!tab.id}
              className="flex-1 bg-orange-500 text-white rounded py-1 text-xs font-medium disabled:opacity-50 hover:bg-orange-600">
              ROLLBACK TO
            </button>
          </div>
        </div>
      </div>

      {/* Middle: results log */}
      <div className="flex-1 flex flex-col min-w-0">
        <div className="flex items-center justify-between mb-2">
          <h2 className="font-semibold text-gray-700">Operation Log</h2>
          <button onClick={() => setLog([])} className="text-xs text-gray-400 hover:text-gray-600">Clear</button>
        </div>
        <div ref={logRef} className="flex-1 bg-gray-900 rounded-lg p-4 overflow-y-auto font-mono text-sm space-y-1">
          {log.length === 0 && (
            <p className="text-gray-500">Begin a transaction and execute operations... (Ctrl+Enter to execute)</p>
          )}
          {log.map((entry, i) => (
            <div key={i} className={`flex gap-3 ${entry.error ? 'text-red-400' : 'text-green-300'}`}>
              <span className="text-gray-500 flex-shrink-0">{entry.ts}</span>
              <span className="text-yellow-400 flex-shrink-0">[{entry.txn}]</span>
              <span className="text-blue-300 flex-shrink-0">{entry.op}</span>
              <span className="truncate">{entry.error ? `ERROR: ${entry.error}` : entry.result}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Right: lock status panel */}
      <div className="w-60 flex-shrink-0 flex flex-col">
        <h2 className="font-semibold text-gray-700 mb-2">
          Lock Status {tab.id ? <span className="text-xs text-gray-400">T{tab.label} #{tab.id}</span> : ''}
        </h2>
        <div className="flex-1 bg-white rounded-lg border border-gray-200 overflow-y-auto">
          {!tab.id ? (
            <p className="p-3 text-xs text-gray-400">No active transaction</p>
          ) : myLocks.length === 0 ? (
            <p className="p-3 text-xs text-gray-400">No locks held</p>
          ) : (
            <div className="divide-y divide-gray-100">
              {myLocks.map((q, i) => (
                <div key={i} className="p-2.5">
                  <p className="text-xs font-mono text-gray-600 truncate mb-1">{q.resource}</p>
                  {q.granted?.filter((g: { txnId: number }) => g.txnId === tab.id).map((g: { mode: string; status: string }, j: number) => (
                    <span key={j} className="inline-flex items-center gap-1 text-xs px-1.5 py-0.5 rounded bg-green-100 text-green-700 mr-1">
                      {g.mode} <span className="text-green-500">✓</span>
                    </span>
                  ))}
                  {q.waiting?.filter((w: { txnId: number }) => w.txnId === tab.id).map((w: { mode: string }, j: number) => (
                    <span key={j} className="inline-flex items-center gap-1 text-xs px-1.5 py-0.5 rounded bg-amber-100 text-amber-700 mr-1">
                      {w.mode} <span className="text-amber-500">⏳</span>
                    </span>
                  ))}
                </div>
              ))}
            </div>
          )}
        </div>

        {/* All active locks summary */}
        {locks.length > 0 && (
          <div className="mt-3">
            <p className="text-xs text-gray-500 mb-1">All locks ({locks.length} queues)</p>
            <div className="bg-white rounded-lg border border-gray-200 max-h-40 overflow-y-auto">
              {locks.map((q, i) => (
                <div key={i} className="px-2.5 py-1.5 border-b border-gray-50 last:border-0">
                  <p className="text-xs font-mono text-gray-500 truncate">{q.resource}</p>
                  <p className="text-xs text-gray-400">
                    {q.granted?.length ?? 0}G / {q.waiting?.length ?? 0}W
                  </p>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
