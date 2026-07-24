import { create } from 'zustand'
import type { Transaction, WFGState, MetricsSnapshot, ScenarioInfo, BenchmarkResult } from './types'

// MaxClientBenchmarkResults bounds the in-memory result history the
// frontend keeps across the lifetime of a tab (FE-02: previously
// grew without bound, increasing re-render cost).
const MaxClientBenchmarkResults = 50

interface AppStore {
  activeTxns: Transaction[]
  wfgState: WFGState
  metrics: MetricsSnapshot | null
  scenarios: ScenarioInfo[]
  benchmarkResults: BenchmarkResult[]

  setActiveTxns: (txns: Transaction[]) => void
  setWFGState: (state: WFGState) => void
  setMetrics: (m: MetricsSnapshot) => void
  setScenarios: (s: ScenarioInfo[]) => void
  addBenchmarkResult: (r: BenchmarkResult) => void
}

export const useStore = create<AppStore>((set) => ({
  activeTxns: [],
  wfgState: { nodes: [], edges: [] },
  metrics: null,
  scenarios: [],
  benchmarkResults: [],

  setActiveTxns: (txns) => set({ activeTxns: txns }),
  setWFGState: (state) => set({ wfgState: state }),
  setMetrics: (m) => set({ metrics: m }),
  setScenarios: (s) => set({ scenarios: s }),
  addBenchmarkResult: (r) =>
    set((state) => ({
      benchmarkResults: [...state.benchmarkResults, r].slice(-MaxClientBenchmarkResults),
    })),
}))
