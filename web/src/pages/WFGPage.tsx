import { useCallback, useRef, useState } from 'react'
import { ReactFlow, Background, Controls, MiniMap, useNodesState, useEdgesState, MarkerType } from '@xyflow/react'
import type { Node, Edge } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import Dagre from '@dagrejs/dagre'
import { infraApi, scenarioApi } from '../api'
import { usePolling } from '../hooks/usePolling'
import { useSSE } from '../hooks/useSSE'
import { useToast } from '../components/Toast'
import type { WFGState, DeadlockRecord } from '../types'

function layoutWithDagre(nodes: Node[], edges: Edge[]): Node[] {
  if (nodes.length === 0) return nodes
  const g = new Dagre.graphlib.Graph()
  g.setDefaultEdgeLabel(() => ({}))
  g.setGraph({ rankdir: 'LR', nodesep: 80, ranksep: 120 })
  nodes.forEach((n) => g.setNode(n.id, { width: 60, height: 60 }))
  edges.forEach((e) => g.setEdge(e.source, e.target))
  Dagre.layout(g)
  return nodes.map((n) => {
    const pos = g.node(n.id)
    return { ...n, position: { x: pos.x - 30, y: pos.y - 30 } }
  })
}

export function WFGPage() {
  const [wfg, setWfg] = useState<WFGState>({ nodes: [], edges: [] })
  const [deadlocks, setDeadlocks] = useState<DeadlockRecord[]>([])
  // deadlocksRef mirrors `deadlocks` so SSE/poll callbacks can read the
  // latest cycle nodes without re-subscribing on every change (avoids the
  // side-effect-in-updater anti-pattern, H-26/M-56).
  const deadlocksRef = useRef<DeadlockRecord[]>([])
  deadlocksRef.current = deadlocks
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const [simulating, setSimulating] = useState(false)
  const { showToast } = useToast()

  const [sseConnected, setSseConnected] = useState(false)

  const applyWFGData = useCallback((wfgData: WFGState, cycleNodes: Set<number>) => {
    const rawNodes: Node[] = wfgData.nodes.map((id) => ({
      id: String(id),
      position: { x: 0, y: 0 },
      data: { label: `T${id}` },
      style: {
        background: cycleNodes.has(id) ? '#fee2e2' : '#dbeafe',
        border: cycleNodes.has(id) ? '2px solid #dc2626' : '2px solid #2563eb',
        borderRadius: '50%',
        width: 60,
        height: 60,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontWeight: 600,
      },
    }))
    const rawEdges: Edge[] = wfgData.edges.map((e, i) => ({
      id: `e${i}`,
      source: String(e.from),
      target: String(e.to),
      animated: true,
      style: { stroke: '#dc2626' },
      markerEnd: { type: MarkerType.ArrowClosed, color: '#dc2626' },
    }))
    setNodes(layoutWithDagre(rawNodes, rawEdges))
    setEdges(rawEdges)
    setWfg(wfgData)
  }, [setNodes, setEdges])

  // SSE for live WFG updates. H-26/M-50: onDisconnect flips sseConnected
  // false so the polling fallback re-engages when the stream dies.
  useSSE(
    '/sse/wfg',
    useCallback(
      (e: MessageEvent) => {
        try {
          const data = JSON.parse(e.data) as WFGState
          const cycleNodes = new Set(deadlocksRef.current.flatMap((d) => d.cycle))
          applyWFGData(data, cycleNodes)
        } catch (err) {
          // FE-05: SSE payload parse failures are now visible (e.g. a
          // backend regression that sends malformed JSON) instead of
          // silently swallowed.
          console.warn('SSE wfg payload parse failed', err)
        }
      },
      [applyWFGData],
    ),
    // FE-04: the SSE "Live" indicator is now flipped in useSSE onopen so
    // it does not flicker to "Polling" during the reconnect → first-
    // message window. The old setDeadlocks((prev) => prev) force-rerender
    // hack (FE-01) was a no-op and is removed.
    useCallback(() => {
      setSseConnected(false)
    }, []),
    useCallback(() => {
      setSseConnected(true)
    }, []),
  )

  // Deadlock history still polled (less frequent)
  const fetchDeadlocks = useCallback(async () => {
    try {
      const dlData = await infraApi.deadlocks()
      setDeadlocks(dlData)
    } catch {}
  }, [])
  usePolling(fetchDeadlocks, 2000)

  // Fallback polling when SSE not connected. FE-01: also drop the
  // response if SSE reconnected mid-fetch, so a slow /api/wfg poll
  // never clobbers a fresh SSE message.
  const fetchData = useCallback(async () => {
    if (sseConnected) return
    try {
      const wfgData = await infraApi.wfg()
      if (sseConnected) return
      const cycleNodes = new Set(deadlocksRef.current.flatMap((d) => d.cycle))
      applyWFGData(wfgData, cycleNodes)
    } catch {}
  }, [sseConnected, applyWFGData])
  usePolling(fetchData, 1000)

  const handleSimulateDeadlock = async () => {
    setSimulating(true)
    try {
      await scenarioApi.run('deadlock_cycle', '2pl', 'read_committed')
      showToast('Deadlock cycle triggered — watch the graph update', 'info')
    } catch {
      showToast('Failed to trigger deadlock scenario', 'error')
    } finally {
      setSimulating(false)
    }
  }

  return (
    <div className="flex h-[calc(100vh-56px)]">
      <div className="flex-1 relative">
        {/* Controls overlay */}
        <div className="absolute top-3 left-3 z-10 flex items-center gap-2">
          <button
            onClick={handleSimulateDeadlock}
            disabled={simulating}
            className="bg-red-600 text-white rounded px-3 py-1.5 text-sm font-medium shadow hover:bg-red-700 disabled:opacity-50"
          >
            {simulating ? 'Simulating...' : 'Simulate Deadlock'}
          </button>
          <span className={`inline-flex items-center gap-1 text-xs px-2 py-1 rounded-full ${sseConnected ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-500'}`}>
            <span className={`w-1.5 h-1.5 rounded-full ${sseConnected ? 'bg-green-500' : 'bg-gray-400'}`} />
            {sseConnected ? 'Live' : 'Polling'}
          </span>
        </div>

        {wfg.nodes.length === 0 ? (
          <div className="absolute inset-0 flex items-center justify-center text-gray-400">
            <div className="text-center">
              <p className="text-lg font-medium">No active transactions</p>
              <p className="text-sm mt-1">Start some transactions in the Playground to see the wait-for graph</p>
              <button onClick={handleSimulateDeadlock} disabled={simulating}
                className="mt-4 bg-red-600 text-white rounded px-4 py-2 text-sm font-medium hover:bg-red-700 disabled:opacity-50">
                Simulate Deadlock
              </button>
            </div>
          </div>
        ) : (
          <ReactFlow nodes={nodes} edges={edges} onNodesChange={onNodesChange} onEdgesChange={onEdgesChange} fitView>
            <Background />
            <Controls />
            <MiniMap />
          </ReactFlow>
        )}
      </div>

      {/* Deadlock History Sidebar */}
      <div className="w-72 border-l border-gray-200 bg-white overflow-y-auto">
        <div className="px-4 py-3 border-b border-gray-200">
          <h2 className="font-semibold text-gray-700">Deadlock History</h2>
          {deadlocks.length > 0 && (
            <p className="text-xs text-gray-400 mt-0.5">{deadlocks.length} total</p>
          )}
        </div>
        {deadlocks.length === 0 ? (
          <p className="p-4 text-sm text-gray-400">No deadlocks detected</p>
        ) : (
          <div className="divide-y divide-gray-100">
            {deadlocks.slice().reverse().map((d) => (
              <div key={d.id} className="p-3">
                <p className="text-xs text-gray-500">{d.detectedAt}</p>
                <p className="text-sm mt-1">
                  Cycle: <span className="font-mono">{d.cycle.join(' → ')}</span>
                </p>
                <p className="text-xs text-red-600 mt-0.5">Victim: T{d.victim}</p>
                {d.victimReason && (
                  <p className="text-xs text-gray-400 mt-0.5">{d.victimReason}</p>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
