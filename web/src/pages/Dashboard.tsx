import { useCallback, useRef, useState } from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts'
import { txnApi, infraApi, scenarioApi } from '../api'
import { useStore } from '../store'
import { usePolling } from '../hooks/usePolling'
import { isolationColor, isolationLabel, formatNumber } from '../lib/utils'
import { useToast } from '../components/Toast'

interface RatePoint { t: string; commits: number; aborts: number }

export function Dashboard() {
  const { metrics, activeTxns, setMetrics, setActiveTxns } = useStore()
  // H-28: rateHistory is now state (was a ref) so the chart re-renders
  // deterministically on each poll rather than relying on the incidental
  // setMetrics re-render. A `primed` ref skips the bogus first-delta spike
  // (the first poll set prev from 0 → cumulative, then computed a delta
  // equal to the entire cumulative count).
  const [rateHistory, setRateHistory] = useState<RatePoint[]>([])
  const prevCommits = useRef(0)
  const prevAborts = useRef(0)
  const primed = useRef(false)
  const [pollError, setPollError] = useState(false)
  const [actionBusy, setActionBusy] = useState(false)
  const { showToast } = useToast()

  const fetchMetrics = useCallback(async () => {
    try {
      const m = await infraApi.metrics()
      setMetrics(m)
      if (pollError) setPollError(false)
      if (!primed.current) {
        // First poll: establish the baseline without recording a rate point.
        prevCommits.current = m.txnCommits
        prevAborts.current = m.txnAborts
        primed.current = true
        return
      }
      const commits = m.txnCommits - prevCommits.current
      const aborts = m.txnAborts - prevAborts.current
      prevCommits.current = m.txnCommits
      prevAborts.current = m.txnAborts
      setRateHistory((prev) => [
        ...prev.slice(-59),
        { t: new Date().toLocaleTimeString(), commits: Math.max(0, commits), aborts: Math.max(0, aborts) },
      ])
    } catch (e) {
      // FE-05: surface persistent poll failures so the user knows the
      // dashboard is stale, rather than silently freezing.
      if (!pollError) {
        setPollError(true)
        showToast('Connection lost — metrics may be stale', 'error')
      }
      console.warn('metrics poll failed', e)
    }
  }, [setMetrics, pollError, showToast])

  const fetchActive = useCallback(async () => {
    try {
      const txns = await txnApi.active()
      setActiveTxns(txns)
    } catch {}
  }, [setActiveTxns])

  usePolling(fetchMetrics, 1000)
  usePolling(fetchActive, 2000)

  const handleDemoDeadlock = async () => {
    setActionBusy(true)
    try {
      await scenarioApi.run('deadlock_cycle', '2pl', 'read_committed')
      showToast('Deadlock scenario triggered — check the WFG page', 'info')
    } catch {
      showToast('Failed to trigger deadlock scenario', 'error')
    } finally {
      setActionBusy(false)
    }
  }

  const handleReset = async () => {
    setActionBusy(true)
    try {
      await infraApi.reset()
      showToast('Database reset and reseeded successfully', 'success')
    } catch {
      showToast('Failed to reset database', 'error')
    } finally {
      setActionBusy(false)
    }
  }

  // H-28: TPS is the most-recent per-poll commit delta (commits in the last
  // ~1s), not cumulative-commits divided by sample count.
  const lastRate = rateHistory[rateHistory.length - 1]
  const tps = lastRate ? lastRate.commits : 0
  const abortRate = metrics && (metrics.txnCommits + metrics.txnAborts) > 0
    ? ((metrics.txnAborts / (metrics.txnCommits + metrics.txnAborts)) * 100).toFixed(1)
    : '0.0'

  return (
    <div className="p-6 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
        <div className="flex gap-2">
          <button
            onClick={handleDemoDeadlock}
            disabled={actionBusy}
            className="bg-amber-500 text-white rounded px-3 py-1.5 text-sm font-medium disabled:opacity-50 hover:bg-amber-600"
          >
            Start Demo Deadlock
          </button>
          <button
            onClick={handleReset}
            disabled={actionBusy}
            className="bg-red-600 text-white rounded px-3 py-1.5 text-sm font-medium disabled:opacity-50 hover:bg-red-700"
          >
            Reset Database
          </button>
        </div>
      </div>

      {/* Metric Cards */}
      <div className="grid grid-cols-4 gap-4">
        {[
          { label: 'Active Txns', value: activeTxns.length },
          { label: 'Txns/sec', value: tps.toFixed(0) },
          { label: 'Abort Rate', value: `${abortRate}%` },
          { label: 'Deadlocks', value: formatNumber(metrics?.deadlocks ?? 0) },
        ].map(({ label, value }) => (
          <div key={label} className="bg-white rounded-lg border border-gray-200 p-4 shadow-sm">
            <p className="text-sm text-gray-500">{label}</p>
            <p className="text-3xl font-bold text-gray-900 mt-1">{value}</p>
          </div>
        ))}
      </div>

      {/* Active Transactions Table */}
      <div className="bg-white rounded-lg border border-gray-200 shadow-sm">
        <div className="px-4 py-3 border-b border-gray-200">
          <h2 className="font-semibold text-gray-700">Active Transactions</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-left text-gray-500 border-b border-gray-100">
                <th className="px-4 py-2">ID</th>
                <th className="px-4 py-2">Protocol</th>
                <th className="px-4 py-2">Isolation</th>
                <th className="px-4 py-2">Status</th>
              </tr>
            </thead>
            <tbody>
              {activeTxns.length === 0 ? (
                <tr><td colSpan={4} className="px-4 py-8 text-center text-gray-400">No active transactions</td></tr>
              ) : activeTxns.map((t) => (
                <tr key={t.id} className="border-b border-gray-50 hover:bg-gray-50">
                  <td className="px-4 py-2 font-mono">{t.id}</td>
                  <td className="px-4 py-2 uppercase text-xs font-medium">{t.protocol}</td>
                  <td className="px-4 py-2">
                    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${isolationColor(t.isolation)}`}>
                      {isolationLabel(t.isolation)}
                    </span>
                  </td>
                  <td className="px-4 py-2">
                    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${
                      t.status === 'active' ? 'bg-green-100 text-green-700' :
                      t.status === 'committed' ? 'bg-blue-100 text-blue-700' : 'bg-red-100 text-red-700'
                    }`}>{t.status}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Throughput Chart */}
      <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-4">
        <h2 className="font-semibold text-gray-700 mb-4">Throughput (last 60s)</h2>
        <ResponsiveContainer width="100%" height={200}>
          <LineChart data={rateHistory}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis dataKey="t" tick={{ fontSize: 11 }} interval="preserveStartEnd" />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="commits" stroke="#2563eb" strokeWidth={2} dot={false} name="Commits/s" />
            <Line type="monotone" dataKey="aborts" stroke="#dc2626" strokeWidth={2} dot={false} name="Aborts/s" />
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Metrics Summary */}
      {metrics && (
        <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-4">
          <h2 className="font-semibold text-gray-700 mb-3">Cumulative Metrics</h2>
          <div className="grid grid-cols-3 gap-3 text-sm">
            {Object.entries(metrics).map(([k, v]) => (
              <div key={k} className="flex justify-between">
                <span className="text-gray-500">{k}</span>
                <span className="font-mono font-medium">{formatNumber(v as number)}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
