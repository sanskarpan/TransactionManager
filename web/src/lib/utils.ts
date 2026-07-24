export function cn(...classes: (string | undefined | false | null)[]): string {
  return classes.filter(Boolean).join(' ')
}

export function isolationColor(level: string): string {
  switch (level) {
    case 'read_uncommitted': return 'bg-gray-100 text-gray-700'
    case 'read_committed': return 'bg-blue-100 text-blue-700'
    case 'repeatable_read': return 'bg-amber-100 text-amber-700'
    case 'serializable': return 'bg-purple-100 text-purple-700'
    default: return 'bg-gray-100 text-gray-700'
  }
}

export function isolationLabel(level: string): string {
  switch (level) {
    case 'read_uncommitted': return 'RU'
    case 'read_committed': return 'RC'
    case 'repeatable_read': return 'RR'
    case 'serializable': return 'SER'
    default: return level
  }
}

export function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(1)}s`
}

export function formatNumber(n: number): string {
  return n.toLocaleString()
}
