// All transaction/lock/MVCC types matching the Go backend

export type IsolationLevel = 'read_uncommitted' | 'read_committed' | 'repeatable_read' | 'serializable'
export type Protocol = '2pl' | 'mvcc'
export type TxnStatus = 'active' | 'committed' | 'aborted'

export interface Transaction {
  id: number
  protocol: Protocol
  isolation: IsolationLevel
  status: TxnStatus
  locksHeld: number
  beginAt: string
}

export interface LockQueueSnapshot {
  resource: string
  granted: LockRequestSnapshot[]
  waiting: LockRequestSnapshot[]
}

export interface LockRequestSnapshot {
  txnId: number
  mode: string
  status: string
  requestAt: string
  grantedAt?: string
}

export interface Version {
  xmin: number
  xmax: number
  data: string[]
}

export interface VersionChain {
  table: string
  key: string
  versions: Version[]
}

export interface WFGState {
  nodes: number[]
  edges: Array<{ from: number; to: number }>
}

export interface DeadlockRecord {
  id: string
  detectedAt: string
  cycle: number[]
  victim: number
  victimReason: string
}

export interface MetricsSnapshot {
  txnBegins: number
  txnCommits: number
  txnAborts: number
  deadlocks: number
  lockTimeouts: number
  writeConflicts: number
  ssiAborts: number
  readsTotal: number
  writesTotal: number
  scansTotal: number
  versionsPruned: number
  vacuumRuns: number
}

export interface ScenarioInfo {
  name: string
  description: string
  anomalyType: string
}

export interface ScenarioStep {
  txnLabel: string
  op: string
  table: string
  key: string
  result: unknown
  error?: string
  timestamp: string
  note?: string
}

export interface ScenarioResult {
  name: string
  isolation: string
  protocol: string
  anomalyOccurred: boolean
  anomalyType: string
  explanation: string
  steps: ScenarioStep[]
}

export interface BenchmarkResult {
  jobId: string
  status: 'running' | 'done'
  committed?: number
  aborted?: number
  conflicts?: number
  deadlocks?: number
  totalTxns?: number
  tps?: number
  balanceSumOK?: boolean
  durationMs?: number
  latencyP50Ms?: number
  latencyP95Ms?: number
  latencyP99Ms?: number
}

// CommitResponse is returned by POST /api/txn/{id}/commit (H-11: previously
// mistyped as { ok: boolean } which read undefined at call sites).
export interface CommitResponse {
  id: number
  status: 'committed'
}

// AbortResponse is returned by POST /api/txn/{id}/abort.
export interface AbortResponse {
  id: number
  status: 'aborted'
}
