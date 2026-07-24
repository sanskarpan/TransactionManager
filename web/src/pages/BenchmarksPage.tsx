import { useState, useEffect, useRef } from 'react'
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { benchmarkApi } from '../api'
import type { BenchmarkResult } from '../types'
import { formatNumber } from '../lib/utils'

interface RunConfig {
  protocol: string
  isolation: string
  concurrency: number
  durationSec: number
  numAccounts: number
  maxRetries: number
}

interface LabeledResult extends BenchmarkResult { label: string }

// FE-07: pollJob uses an inFlight guard plus a generation counter to
// prevent (a) overlapping fetches when the previous one is still in
// flight (a slow backend) and (b) stale responses from an earlier poll
// resolving after a later one. Polling stops once the job is `done` or
// `maxPolls` consecutive misses occur (bounded CPU/network).
function pollJob(
  jobId: string,
  onDone: (r: BenchmarkResult) => void,
  onError: () => void,
): () => void {
  let inFlight = false
  let gen = 0
  let misses = 0
  const MAX_POLLS = 150 // ~5 min at 2 s
  const iv = setInterval(async () => {
    if (inFlight) return
    const myGen = ++gen
    inFlight = true
    try {
      const res = await benchmarkApi.results(jobId)
      if (myGen !== gen) return // a newer poll superseded us
      if (res.status === 'done') {
        clearInterval(iv)
        onDone(res)
      } else {
        misses = 0
      }
    } catch {
      if (++misses > MAX_POLLS) {
        clearInterval(iv)
        onError()
      }
    } finally {
      inFlight = false
    }
  }, 2000)
  return () => clearInterval(iv)
}

export function BenchmarksPage() {
  const [config, setConfig] = useState<RunConfig>({
    protocol: 'mvcc',
    isolation: 'read_committed',
    concurrency: 4,
    durationSec: 10,
    numAccounts: 100,
    maxRetries: 3,
  })
  const [results, setResults] = useState<LabeledResult[]>([])
  const [running, setRunning] = useState(false)
  const [runningLabel, setRunningLabel] = useState('')
  const cleanups = useRef<Array<() => void>>([])

  useEffect(() => () => cleanups.current.forEach((c) => c()), [])

  const runOne = async (cfg: RunConfig): Promise<void> => {
    const label = `${cfg.protocol.toUpperCase()} ${cfg.isolation.replace(/_/g, ' ')} c=${cfg.concurrency}`
    setRunningLabel(label)
    const { jobId } = await benchmarkApi.run(cfg)
    return new Promise((resolve, reject) => {
      const stop = pollJob(jobId,
        (res) => {
          setResults((prev) => [...prev, { ...res, label }])
          resolve()
        },
        reject
      )
      cleanups.current.push(stop)
    })
  }

  const startBenchmark = async () => {
    setRunning(true)
    try {
      await runOne(config)
    } catch {
      // swallow
    } finally {
      setRunning(false)
      setRunningLabel('')
    }
  }

  const startComparison = async () => {
    setRunning(true)
    try {
      await runOne({ ...config, protocol: 'mvcc' })
      await runOne({ ...config, protocol: '2pl' })
    } catch {
      // swallow
    } finally {
      setRunning(false)
      setRunningLabel('')
    }
  }

  const tpsChartData = results.map((r) => ({ name: r.label, TPS: Math.round(r.tps ?? 0) }))

  const latencyChartData = results.map((r) => ({
    name: r.label,
    P50: r.latencyP50Ms ?? 0,
    P95: r.latencyP95Ms ?? 0,
    P99: r.latencyP99Ms ?? 0,
  }))

  const abortChartData = results.map((r) => ({
    name: r.label,
    Committed: r.committed ?? 0,
    Aborted: r.aborted ?? 0,
    Conflicts: r.conflicts ?? 0,
    Deadlocks: r.deadlocks ?? 0,
  }))

  return (
    <div className="p-6 space-y-6">
      <h1 className="text-2xl font-bold text-gray-900">Benchmarks</h1>

      {/* Config form */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <h2 className="font-semibold text-gray-700 mb-4">Configuration</h2>
        <div className="grid grid-cols-3 gap-4">
          <div>
            <label className="text-xs text-gray-500 block mb-1">Protocol</label>
            <select value={config.protocol} onChange={(e) => setConfig({ ...config, protocol: e.target.value })}
              className="w-full border border-gray-200 rounded px-2 py-1.5 text-sm">
              <option value="mvcc">MVCC</option>
              <option value="2pl">2PL</option>
            </select>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">Isolation</label>
            <select value={config.isolation} onChange={(e) => setConfig({ ...config, isolation: e.target.value })}
              className="w-full border border-gray-200 rounded px-2 py-1.5 text-sm">
              <option value="read_committed">Read Committed</option>
              <option value="repeatable_read">Repeatable Read</option>
              <option value="serializable">Serializable</option>
            </select>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">Concurrency</label>
            <select value={config.concurrency} onChange={(e) => setConfig({ ...config, concurrency: +e.target.value })}
              className="w-full border border-gray-200 rounded px-2 py-1.5 text-sm">
              {[1, 2, 4, 8, 16].map((c) => <option key={c} value={c}>{c}</option>)}
            </select>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">Duration (sec)</label>
            <select value={config.durationSec} onChange={(e) => setConfig({ ...config, durationSec: +e.target.value })}
              className="w-full border border-gray-200 rounded px-2 py-1.5 text-sm">
              {[5, 10, 30, 60].map((d) => <option key={d} value={d}>{d}s</option>)}
            </select>
          </div>
          <div>
            <label className="text-xs text-gray-500 block mb-1">Accounts</label>
            <select value={config.numAccounts} onChange={(e) => setConfig({ ...config, numAccounts: +e.target.value })}
              className="w-full border border-gray-200 rounded px-2 py-1.5 text-sm">
              {[20, 100, 500, 1000].map((n) => <option key={n} value={n}>{n}</option>)}
            </select>
          </div>
          <div className="flex items-end gap-2">
            <button onClick={startBenchmark} disabled={running}
              className="flex-1 bg-blue-600 text-white rounded px-3 py-2 text-sm font-medium disabled:opacity-50 hover:bg-blue-700">
              {running ? `Running ${runningLabel}...` : 'RUN'}
            </button>
            <button onClick={startComparison} disabled={running} title="Run both MVCC and 2PL sequentially for comparison"
              className="flex-1 bg-purple-600 text-white rounded px-3 py-2 text-sm font-medium disabled:opacity-50 hover:bg-purple-700">
              COMPARE
            </button>
          </div>
        </div>
        {running && (
          <p className="text-xs text-blue-600 mt-3">Running {runningLabel}... polling every 2s</p>
        )}
      </div>

      {results.length > 0 && (
        <>
          {/* Results table */}
          <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
            <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between">
              <h2 className="font-semibold text-gray-700">Results ({results.length} runs)</h2>
              <button onClick={() => setResults([])} className="text-xs text-gray-400 hover:text-gray-600">Clear</button>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-left text-gray-500 text-xs border-b border-gray-100">
                    <th className="px-4 py-2">Config</th>
                    <th className="px-4 py-2">TPS</th>
                    <th className="px-4 py-2">Committed</th>
                    <th className="px-4 py-2">Aborted</th>
                    <th className="px-4 py-2">Conflicts</th>
                    <th className="px-4 py-2">Deadlocks</th>
                    <th className="px-4 py-2">P50</th>
                    <th className="px-4 py-2">P95</th>
                    <th className="px-4 py-2">P99</th>
                    <th className="px-4 py-2">Balance</th>
                    <th className="px-4 py-2">Duration</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map((r, i) => (
                    <tr key={i} className="border-b border-gray-50 hover:bg-gray-50">
                      <td className="px-4 py-2 font-medium">{r.label}</td>
                      <td className="px-4 py-2 font-mono">{r.tps?.toFixed(0) ?? '-'}</td>
                      <td className="px-4 py-2 font-mono">{formatNumber(r.committed ?? 0)}</td>
                      <td className="px-4 py-2 font-mono">{formatNumber(r.aborted ?? 0)}</td>
                      <td className="px-4 py-2 font-mono">{formatNumber(r.conflicts ?? 0)}</td>
                      <td className="px-4 py-2 font-mono">{formatNumber(r.deadlocks ?? 0)}</td>
                      <td className="px-4 py-2 font-mono">{r.latencyP50Ms?.toFixed(1) ?? '-'}ms</td>
                      <td className="px-4 py-2 font-mono">{r.latencyP95Ms?.toFixed(1) ?? '-'}ms</td>
                      <td className="px-4 py-2 font-mono">{r.latencyP99Ms?.toFixed(1) ?? '-'}ms</td>
                      <td className="px-4 py-2">
                        <span className={r.balanceSumOK ? 'text-green-600 font-bold' : 'text-red-600 font-bold'}>
                          {r.balanceSumOK ? '✓' : '✗'}
                        </span>
                      </td>
                      <td className="px-4 py-2 font-mono">{r.durationMs ? `${(r.durationMs / 1000).toFixed(1)}s` : '-'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* TPS chart */}
          <div className="bg-white rounded-lg border border-gray-200 p-4 shadow-sm">
            <h2 className="font-semibold text-gray-700 mb-4">Throughput (TPS)</h2>
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={tpsChartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" tick={{ fontSize: 10 }} />
                <YAxis tick={{ fontSize: 11 }} />
                <Tooltip />
                <Bar dataKey="TPS" fill="#2563eb" />
              </BarChart>
            </ResponsiveContainer>
          </div>

          {/* Latency chart */}
          <div className="bg-white rounded-lg border border-gray-200 p-4 shadow-sm">
            <h2 className="font-semibold text-gray-700 mb-4">Latency Percentiles (ms)</h2>
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={latencyChartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" tick={{ fontSize: 10 }} />
                <YAxis tick={{ fontSize: 11 }} unit="ms" />
                <Tooltip />
                <Legend />
                <Line type="monotone" dataKey="P50" stroke="#16a34a" strokeWidth={2} dot />
                <Line type="monotone" dataKey="P95" stroke="#d97706" strokeWidth={2} dot />
                <Line type="monotone" dataKey="P99" stroke="#dc2626" strokeWidth={2} dot />
              </LineChart>
            </ResponsiveContainer>
          </div>

          {/* Abort/conflict breakdown */}
          <div className="bg-white rounded-lg border border-gray-200 p-4 shadow-sm">
            <h2 className="font-semibold text-gray-700 mb-4">Abort Breakdown</h2>
            <ResponsiveContainer width="100%" height={220}>
              <BarChart data={abortChartData}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis dataKey="name" tick={{ fontSize: 10 }} />
                <YAxis tick={{ fontSize: 11 }} />
                <Tooltip />
                <Legend />
                <Bar dataKey="Committed" fill="#16a34a" />
                <Bar dataKey="Aborted" fill="#dc2626" />
                <Bar dataKey="Conflicts" fill="#d97706" />
                <Bar dataKey="Deadlocks" fill="#7c3aed" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </>
      )}
    </div>
  )
}
