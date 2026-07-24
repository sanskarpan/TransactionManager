import ky from 'ky'
import type { Transaction, LockQueueSnapshot, WFGState, DeadlockRecord, MetricsSnapshot, VersionChain, ScenarioInfo, ScenarioResult, BenchmarkResult, CommitResponse, AbortResponse } from './types'

// H-29: hard timeout + zero retries so a hung backend cannot leave the UI
// in an eternal "running" state. Per-call overrides are available via
// the third arg of `api.post/get` (FE-13), e.g. for benchmark run which
// the user expects to take longer than 30s on heavy configurations.
const API_BASE = import.meta.env.VITE_API_BASE ?? '/api'
const api = ky.create({
  prefix: API_BASE,
  timeout: 30_000,
  retry: { limit: 0 },
})

export const txnApi = {
  // lockTimeoutMs is in milliseconds (matches backend BeginRequest field);
  // default 30 s = 30_000 ms. H-03: previously sent `lockTimeoutSec` which
  // the backend ignored (different field name) and would have interpreted
  // as 30 ms once the names aligned.
  begin: (protocol: string, isolation: string, lockTimeoutMs: number = 30_000) =>
    api.post('txn/begin', { json: { protocol, isolation, lockTimeoutMs } }).json<{ id: number }>(),
  commit: (id: number) => api.post(`txn/${id}/commit`).json<CommitResponse>(),
  abort: (id: number) => api.post(`txn/${id}/abort`).json<AbortResponse>(),
  status: (id: number) => api.get(`txn/${id}/status`).json<{ id: number; status: string }>(),
  active: () => api.get('txn/active').json<Transaction[]>(),
  read: (id: number, table: string, key: string) =>
    api.post(`txn/${id}/read`, { json: { table, key } }).json<{ values: unknown[]; found: boolean }>(),
  write: (id: number, table: string, key: string, values: unknown[]) =>
    api.post(`txn/${id}/write`, { json: { table, key, values } }).json<{ ok: boolean }>(),
  scan: (id: number, table: string) =>
    api.post(`txn/${id}/scan`, { json: { table } }).json<{ rows: unknown[] }>(),
  insert: (id: number, table: string, key: string, values: unknown[]) =>
    api.post(`txn/${id}/insert`, { json: { table, key, values } }).json<{ ok: boolean }>(),
  delete: (id: number, table: string, key: string) =>
    api.post(`txn/${id}/delete`, { json: { table, key } }).json<{ ok: boolean }>(),
  savepoint: (id: number, name: string) =>
    api.post(`txn/${id}/savepoint`, { json: { name } }).json<{ ok: boolean }>(),
  rollbackTo: (id: number, name: string) =>
    api.post(`txn/${id}/rollback-to`, { json: { name } }).json<{ ok: boolean }>(),
}

export const infraApi = {
  locks: () => api.get('locks').json<LockQueueSnapshot[]>(),
  lockByResource: (table: string, key: string) => api.get(`locks/${table}/${key}`).json<LockQueueSnapshot>(),
  deadlocks: () => api.get('deadlocks').json<DeadlockRecord[]>(),
  wfg: () => api.get('wfg').json<WFGState>(),
  versionChain: (table: string, key: string) => api.get(`mvcc/chain/${table}/${key}`).json<VersionChain>(),
  mvccStats: () => api.get('mvcc/stats').json<Record<string, unknown>>(),
  vacuum: () => api.post('mvcc/vacuum').json<{ ok: boolean }>(),
  metrics: () => api.get('metrics').json<MetricsSnapshot>(),
  reset: () => api.post('reset').json<{ ok: boolean; message: string }>(),
}

export const scenarioApi = {
  list: () => api.get('scenarios').json<ScenarioInfo[]>(),
  run: (name: string, protocol: string, isolation: string) =>
    api.post(`scenarios/${name}/run`, { json: { protocol, isolation } }).json<ScenarioResult>(),
}

export const benchmarkApi = {
  run: (config: { protocol: string; isolation: string; concurrency: number; durationSec: number; numAccounts: number; maxRetries: number }) =>
    api.post('benchmark/run', { json: config, timeout: false }).json<{ jobId: string; status: string }>(),
  results: (jobId: string) => api.get(`benchmark/results/${jobId}`).json<BenchmarkResult>(),
}
