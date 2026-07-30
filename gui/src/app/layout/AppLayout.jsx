import { Outlet, useParams } from 'react-router-dom'
import Sidebar from './Sidebar'
import Topbar from './Topbar'

export default function AppLayout() {
  const { domain } = useParams()

  return (
    <div className="flex h-screen bg-gray-50">
      <Sidebar domain={domain} />
      <div className="flex-1 flex flex-col overflow-hidden">
        <Topbar domain={domain} />
        <main className="flex-1 overflow-y-auto p-6">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
