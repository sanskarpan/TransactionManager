import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { Nav } from './components/Nav'
import { ErrorBoundary } from './components/ErrorBoundary'
import { ToastProvider } from './components/Toast'
import { Dashboard } from './pages/Dashboard'
import { Playground } from './pages/Playground'
import { WFGPage } from './pages/WFGPage'
import { VersionsPage } from './pages/VersionsPage'
import { ScenariosPage } from './pages/ScenariosPage'
import { BenchmarksPage } from './pages/BenchmarksPage'

export function App() {
  return (
    <ErrorBoundary>
      <ToastProvider>
        <BrowserRouter>
          <div className="min-h-screen bg-gray-50">
            <Nav />
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/playground" element={<Playground />} />
              <Route path="/wfg" element={<WFGPage />} />
              <Route path="/versions" element={<VersionsPage />} />
              <Route path="/scenarios" element={<ScenariosPage />} />
              <Route path="/benchmarks" element={<BenchmarksPage />} />
            </Routes>
          </div>
        </BrowserRouter>
      </ToastProvider>
    </ErrorBoundary>
  )
}
