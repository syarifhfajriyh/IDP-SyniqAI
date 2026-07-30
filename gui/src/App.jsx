import { Routes, Route, Navigate } from 'react-router-dom'
import AppLayout from './app/layout/AppLayout'
import DomainSelection from './app/pages/DomainSelection'
import Dashboard from './app/pages/Dashboard'
import Ingestion from './app/pages/Ingestion'
import UnifiedCDC from './app/pages/CDC/UnifiedCDC'
import Bronze from './app/pages/Bronze'
import Silver from './app/pages/Silver/SilverTabbed'
import GoldTabbed from './app/pages/Gold/GoldTabbed'
import DataMart from './app/pages/DataMart'
import Reports from './app/pages/Reports'
import Settings from './app/pages/Settings'

function App() {
  return (
    <Routes>
      {/* Domain Selection Landing Page */}
      <Route path="/" element={<DomainSelection />} />
      
      {/* Domain-specific routes */}
      <Route path="/:domain" element={<AppLayout />}>
        <Route index element={<Navigate to="dashboard" replace />} />
        <Route path="dashboard" element={<Dashboard />} />
        <Route path="ingestion" element={<Ingestion />} />
        <Route path="cdc" element={<UnifiedCDC />} />
        <Route path="bronze" element={<Bronze />} />
        <Route path="silver" element={<Silver mode="structured" />} />
        <Route path="silver/structured" element={<Silver mode="structured" />} />
        <Route path="silver/unstructured" element={<Silver mode="unstructured" />} />
        <Route path="gold/*" element={<GoldTabbed />} />
        <Route path="data-mart" element={<DataMart />} />
        <Route path="reports" element={<Reports />} />
        <Route path="settings" element={<Settings />} />
      </Route>
      
      {/* Catch-all redirect */}
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}

export default App
