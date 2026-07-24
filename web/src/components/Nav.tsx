import { NavLink } from 'react-router-dom'

const links = [
  { to: '/', label: 'Dashboard' },
  { to: '/playground', label: 'Playground' },
  { to: '/wfg', label: 'Wait-For Graph' },
  { to: '/versions', label: 'Version Chains' },
  { to: '/scenarios', label: 'Scenarios' },
  { to: '/benchmarks', label: 'Benchmarks' },
]

export function Nav() {
  return (
    <nav className="border-b border-gray-200 bg-white px-6 py-3 flex items-center gap-6">
      <span className="font-bold text-gray-900 mr-4">TxnMgr</span>
      {links.map(({ to, label }) => (
        <NavLink
          key={to}
          to={to}
          end={to === '/'}
          className={({ isActive }) =>
            `text-sm font-medium transition-colors ${isActive ? 'text-blue-600' : 'text-gray-600 hover:text-gray-900'}`
          }
        >
          {label}
        </NavLink>
      ))}
    </nav>
  )
}
