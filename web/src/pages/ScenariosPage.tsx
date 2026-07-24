import { useState, useEffect } from 'react'
import { scenarioApi } from '../api'
import { useToast } from '../components/Toast'
import type { ScenarioInfo, ScenarioResult, ScenarioStep } from '../types'

const ANOMALY_COLORS: Record<string, string> = {
  dirty_read: 'bg-red-100 text-red-700',
  lost_update: 'bg-orange-100 text-orange-700',
  non_repeatable_read: 'bg-yellow-100 text-yellow-700',
  phantom_read: 'bg-purple-100 text-purple-700',
  write_skew: 'bg-pink-100 text-pink-700',
  deadlock_cycle: 'bg-red-100 text-red-700',
  cascade_abort: 'bg-gray-100 text-gray-700',
}

export function ScenariosPage() {
  const [scenarios, setScenarios] = useState<ScenarioInfo[]>([])
  const [selected, setSelected] = useState<ScenarioInfo | null>(null)
  const [protocol, setProtocol] = useState('mvcc')
  const [isolation, setIsolation] = useState('read_committed')
  const [result, setResult] = useState<ScenarioResult | null>(null)
  const [running, setRunning] = useState(false)
  const { showToast } = useToast()

  useEffect(() => {
    // FE-05: surface scenarios list failures (empty grid with no
    // feedback was the prior silent-failure mode).
    scenarioApi.list().then(setScenarios).catch((e) => {
      showToast(`Failed to load scenarios: ${String(e)}`, 'error')
      console.warn('scenarios list failed', e)
    })
  }, [showToast])

  const runScenario = async () => {
    if (!selected) return
    setRunning(true)
    setResult(null)
    try {
      const r = await scenarioApi.run(selected.name, protocol, isolation)
      setResult(r)
    } catch (e) {
      // FE-05: surface scenario-run failures to the user (a failed
      // scenario previously looked like "nothing happened").
      showToast(`Failed to run scenario ${selected.name}: ${String(e)}`, 'error')
      console.error(e)
    } finally {
      setRunning(false)
    }
  }

  return (
    <div className="p-6 flex gap-6">
      {/* Scenario grid */}
      <div className="w-72 flex-shrink-0">
        <h1 className="text-xl font-bold text-gray-900 mb-4">Scenarios</h1>
        <div className="space-y-2">
          {scenarios.map((s) => (
            <button
              key={s.name}
              onClick={() => { setSelected(s); setResult(null) }}
              className={`w-full text-left p-3 rounded-lg border transition-colors ${
                selected?.name === s.name
                  ? 'border-blue-500 bg-blue-50'
                  : 'border-gray-200 bg-white hover:border-gray-300'
              }`}
            >
              <div className="flex items-center justify-between mb-1">
                <span className="font-medium text-sm text-gray-900">{s.name.replace(/_/g, ' ')}</span>
                <span className={`text-xs px-1.5 py-0.5 rounded font-medium ${ANOMALY_COLORS[s.anomalyType] ?? 'bg-gray-100 text-gray-700'}`}>
                  {s.anomalyType.replace(/_/g, ' ')}
                </span>
              </div>
              <p className="text-xs text-gray-500 line-clamp-2">{s.description}</p>
            </button>
          ))}
        </div>
      </div>

      {/* Scenario player */}
      <div className="flex-1">
        {!selected ? (
          <div className="flex items-center justify-center h-64 text-gray-400">
            <p>Select a scenario to run</p>
          </div>
        ) : (
          <div className="space-y-4">
            <div className="bg-white rounded-lg border border-gray-200 p-4">
              <h2 className="font-bold text-gray-900 text-lg">{selected.name.replace(/_/g, ' ')}</h2>
              <p className="text-sm text-gray-600 mt-1">{selected.description}</p>

              <div className="flex gap-3 mt-4 items-end">
                <div>
                  <label className="text-xs text-gray-500 block mb-1">Protocol</label>
                  <select value={protocol} onChange={(e) => setProtocol(e.target.value)}
                    className="border border-gray-200 rounded px-2 py-1.5 text-sm">
                    <option value="mvcc">MVCC</option>
                    <option value="2pl">2PL</option>
                  </select>
                </div>
                <div>
                  <label className="text-xs text-gray-500 block mb-1">Isolation</label>
                  <select value={isolation} onChange={(e) => setIsolation(e.target.value)}
                    className="border border-gray-200 rounded px-2 py-1.5 text-sm">
                    <option value="read_uncommitted">Read Uncommitted</option>
                    <option value="read_committed">Read Committed</option>
                    <option value="repeatable_read">Repeatable Read</option>
                    <option value="serializable">Serializable</option>
                  </select>
                </div>
                <button
                  onClick={runScenario}
                  disabled={running}
                  className="bg-blue-600 text-white rounded px-4 py-1.5 text-sm font-medium disabled:opacity-50 hover:bg-blue-700"
                >
                  {running ? 'Running...' : 'Run Scenario'}
                </button>
              </div>
            </div>

            {result && (
              <>
                {/* Result banner */}
                <div className={`rounded-lg p-4 ${result.anomalyOccurred ? 'bg-red-50 border border-red-200' : 'bg-green-50 border border-green-200'}`}>
                  <div className="flex items-center gap-2">
                    <span className={`text-lg font-bold ${result.anomalyOccurred ? 'text-red-700' : 'text-green-700'}`}>
                      {result.anomalyOccurred ? 'Anomaly Detected' : 'Anomaly Prevented'}
                    </span>
                    <span className={`text-xs px-2 py-0.5 rounded ${ANOMALY_COLORS[result.anomalyType] ?? 'bg-gray-100 text-gray-700'}`}>
                      {result.anomalyType}
                    </span>
                  </div>
                  <p className="text-sm mt-1 text-gray-700">{result.explanation}</p>
                </div>

                {/* Step trace */}
                <div className="bg-white rounded-lg border border-gray-200">
                  <div className="px-4 py-3 border-b border-gray-200">
                    <h3 className="font-semibold text-gray-700">Execution Trace</h3>
                  </div>
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-left text-gray-500 text-xs border-b border-gray-100">
                        <th className="px-4 py-2">Txn</th>
                        <th className="px-4 py-2">Op</th>
                        <th className="px-4 py-2">Table/Key</th>
                        <th className="px-4 py-2">Result</th>
                        <th className="px-4 py-2">Note</th>
                      </tr>
                    </thead>
                    <tbody>
                      {result.steps.map((step: ScenarioStep, i: number) => (
                        <tr key={i} className={`border-b border-gray-50 ${step.error ? 'bg-red-50' : ''}`}>
                          <td className="px-4 py-1.5 font-bold text-blue-600">{step.txnLabel}</td>
                          <td className="px-4 py-1.5 font-mono text-xs uppercase">{step.op}</td>
                          <td className="px-4 py-1.5 font-mono text-xs text-gray-500">
                            {step.table}{step.key ? `/${step.key}` : ''}
                          </td>
                          <td className="px-4 py-1.5">
                            {step.error
                              ? <span className="text-red-600 text-xs">{step.error}</span>
                              : <span className="text-gray-700 text-xs font-mono">{JSON.stringify(step.result)}</span>
                            }
                          </td>
                          <td className="px-4 py-1.5 text-xs text-gray-400">{step.note}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
