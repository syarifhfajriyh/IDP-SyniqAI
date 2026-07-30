import { NavLink, useNavigate, useLocation } from 'react-router-dom'
import { useState } from 'react'
import { 
  LayoutDashboard, 
  Upload,
  Zap,
  Layers,
  Database, 
  Activity, 
  FileText, 
  Settings,
  Sparkles,
  BarChart3,
  GitBranch,
  ArrowLeft,
  Building2,
  Heart,
  LayoutGrid,
  ChevronDown,
  ChevronRight,
  Table,
  Image,
  FileJson,
  Cloud,
  Table2
} from 'lucide-react'

const navItems = [
  { path: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
  { path: 'ingestion', label: 'Data Ingestion', icon: Upload },
  { path: 'cdc', label: 'CDC', icon: Zap, hasSubmenu: true },
  { path: 'bronze', label: 'Bronze Layer', icon: Database },
  { path: 'silver', label: 'Silver Layer', icon: Layers, hasSubmenu: true },
  { path: 'gold', label: 'Gold Layer', icon: Sparkles, hasSubmenu: true },
  { path: 'data-mart', label: 'Data Mart', icon: Table2 },
  { path: 'reports', label: 'Reports', icon: FileText },
  { path: 'settings', label: 'Settings', icon: Settings },
]

export default function Sidebar({ domain }) {
  const navigate = useNavigate()
  const location = useLocation()
  const [silverExpanded, setSilverExpanded] = useState(
    location.pathname.includes('/silver')
  )
  const [cdcExpanded, setCdcExpanded] = useState(
    location.pathname.includes('/cdc')
  )
  const [goldExpanded, setGoldExpanded] = useState(
    location.pathname.includes('/gold')
  )

  const getDomainIcon = () => {
    switch(domain) {
      case 'finance': return Building2
      case 'healthcare': return Heart
      case 'general': return LayoutGrid
      default: return Database
    }
  }

  const getDomainColor = () => {
    switch(domain) {
      case 'finance': return 'text-blue-400 bg-blue-500/10'
      case 'healthcare': return 'text-emerald-400 bg-emerald-500/10'
      case 'general': return 'text-purple-400 bg-purple-500/10'
      default: return 'text-blue-400 bg-blue-500/10'
    }
  }

  const DomainIcon = getDomainIcon()
  const domainColorClass = getDomainColor()

  return (
    <aside className="w-64 bg-slate-900 text-white flex flex-col">
      <div className="p-6 border-b border-slate-800">
        <h1 className="text-2xl font-bold text-primary-400">SyniqAI</h1>
        <p className="text-sm text-gray-400 mt-1">Data Lakehouse Platform</p>
      </div>

      {/* Domain Info */}
      <div className="px-6 py-4 bg-slate-800/50 border-b border-slate-800">
        <button
          onClick={() => navigate('/')}
          className="flex items-center gap-2 text-blue-400 hover:text-blue-300 text-sm mb-3 transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
          Change Domain
        </button>
        <div className="flex items-center gap-3">
          <div className={`w-10 h-10 ${domainColorClass} rounded-lg flex items-center justify-center`}>
            <DomainIcon className="w-6 h-6" />
          </div>
          <div>
            <div className="text-white font-semibold capitalize">{domain}</div>
            <div className="text-xs text-gray-400">Active Domain</div>
          </div>
        </div>
      </div>

      <nav className="flex-1 p-4 space-y-1">
        {navItems.map((item) => {
          // Special handling for CDC with dropdown
          if (item.path === 'cdc') {
            const isActive = location.pathname.includes('/cdc')
            return (
              <div key={item.path}>
                <button
                  onClick={() => setCdcExpanded(!cdcExpanded)}
                  className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-colors ${
                    isActive
                      ? 'bg-primary-600 text-white'
                      : 'text-gray-300 hover:bg-slate-800'
                  }`}
                >
                  <item.icon size={20} />
                  <span className="font-medium">{item.label}</span>
                  {cdcExpanded ? (
                    <ChevronDown size={16} className="ml-auto" />
                  ) : (
                    <ChevronRight size={16} className="ml-auto" />
                  )}
                </button>
                
                {/* CDC Submenu */}
                {cdcExpanded && (
                  <div className="ml-4 mt-1 space-y-1">
                    <NavLink
                      to={`/${domain}/cdc?source=realtime`}
                      className={({ isActive }) =>
                        `flex items-center gap-3 px-4 py-2 rounded-lg transition-colors text-sm ${
                          isActive || location.search.includes('source=realtime') || (!location.search && location.pathname.includes('/cdc'))
                            ? 'bg-primary-500 text-white'
                            : 'text-gray-400 hover:bg-slate-800 hover:text-gray-200'
                        }`
                      }
                    >
                      <Zap size={16} />
                      <span>Real-time (Debezium)</span>
                    </NavLink>
                    <NavLink
                      to={`/${domain}/cdc?source=mongodb`}
                      className={({ isActive }) =>
                        `flex items-center gap-3 px-4 py-2 rounded-lg transition-colors text-sm ${
                          location.search.includes('source=mongodb')
                            ? 'bg-primary-500 text-white'
                            : 'text-gray-400 hover:bg-slate-800 hover:text-gray-200'
                        }`
                      }
                    >
                      <FileJson size={16} />
                      <span>MongoDB (Airflow)</span>
                    </NavLink>
                    <NavLink
                      to={`/${domain}/cdc?source=s3`}
                      className={({ isActive }) =>
                        `flex items-center gap-3 px-4 py-2 rounded-lg transition-colors text-sm ${
                          location.search.includes('source=s3')
                            ? 'bg-primary-500 text-white'
                            : 'text-gray-400 hover:bg-slate-800 hover:text-gray-200'
                        }`
                      }
                    >
                      <Cloud size={16} />
                      <span>AWS S3 (Airflow)</span>
                    </NavLink>
                  </div>
                )}
              </div>
            )
          }
          
          // Special handling for Silver Processing with dropdown
          if (item.path === 'silver') {
            const isActive = location.pathname.includes('/silver')
            return (
              <div key={item.path}>
                <button
                  onClick={() => setSilverExpanded(!silverExpanded)}
                  className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-colors ${
                    isActive
                      ? 'bg-primary-600 text-white'
                      : 'text-gray-300 hover:bg-slate-800'
                  }`}
                >
                  <item.icon size={20} />
                  <span className="font-medium">{item.label}</span>
                  {silverExpanded ? (
                    <ChevronDown size={16} className="ml-auto" />
                  ) : (
                    <ChevronRight size={16} className="ml-auto" />
                  )}
                </button>
                
                {/* Silver Submenu */}
                {silverExpanded && (
                  <div className="ml-4 mt-1 space-y-1">
                    <NavLink
                      to={`/${domain}/silver/structured`}
                      className={({ isActive }) =>
                        `flex items-center gap-3 px-4 py-2 rounded-lg transition-colors text-sm ${
                          isActive || location.pathname === `/${domain}/silver`
                            ? 'bg-primary-500 text-white'
                            : 'text-gray-400 hover:bg-slate-800 hover:text-gray-200'
                        }`
                      }
                    >
                      <Table size={16} />
                      <span>Structured</span>
                    </NavLink>
                    <NavLink
                      to={`/${domain}/silver/unstructured`}
                      className={({ isActive }) =>
                        `flex items-center gap-3 px-4 py-2 rounded-lg transition-colors text-sm ${
                          isActive
                            ? 'bg-primary-500 text-white'
                            : 'text-gray-400 hover:bg-slate-800 hover:text-gray-200'
                        }`
                      }
                    >
                      <Image size={16} />
                      <span>Unstructured</span>
                    </NavLink>
                  </div>
                )}
              </div>
            )
          }
          
          // Special handling for Gold Layer Processing with dropdown
          if (item.path === 'gold') {
            const isActive = location.pathname.includes('/gold')
            return (
              <div key={item.path}>
                <button
                  onClick={() => setGoldExpanded(!goldExpanded)}
                  className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg transition-colors ${
                    isActive
                      ? 'bg-primary-600 text-white'
                      : 'text-gray-300 hover:bg-slate-800'
                  }`}
                >
                  <item.icon size={20} />
                  <span className="font-medium">{item.label}</span>
                  {goldExpanded ? (
                    <ChevronDown size={16} className="ml-auto" />
                  ) : (
                    <ChevronRight size={16} className="ml-auto" />
                  )}
                </button>
                
                {/* Gold Submenu */}
                {goldExpanded && (
                  <div className="ml-4 mt-1 space-y-1">
                    <NavLink
                      to={`/${domain}/gold/dashboard`}
                      className={({ isActive }) =>
                        `flex items-center gap-3 px-4 py-2 rounded-lg transition-colors text-sm ${
                          isActive || location.pathname === `/${domain}/gold`
                            ? 'bg-primary-500 text-white'
                            : 'text-gray-400 hover:bg-slate-800 hover:text-gray-200'
                        }`
                      }
                    >
                      <LayoutDashboard size={16} />
                      <span>Overview</span>
                    </NavLink>
                    <NavLink
                      to={`/${domain}/gold/transformation`}
                      className={({ isActive }) =>
                        `flex items-center gap-3 px-4 py-2 rounded-lg transition-colors text-sm ${
                          isActive
                            ? 'bg-primary-500 text-white'
                            : 'text-gray-400 hover:bg-slate-800 hover:text-gray-200'
                        }`
                      }
                    >
                      <GitBranch size={16} />
                      <span>Transformation</span>
                    </NavLink>
                    <NavLink
                      to={`/${domain}/gold/quality`}
                      className={({ isActive }) =>
                        `flex items-center gap-3 px-4 py-2 rounded-lg transition-colors text-sm ${
                          isActive
                            ? 'bg-primary-500 text-white'
                            : 'text-gray-400 hover:bg-slate-800 hover:text-gray-200'
                        }`
                      }
                    >
                      <Activity size={16} />
                      <span>Quality Monitoring</span>
                    </NavLink>
                    <NavLink
                      to={`/${domain}/gold/eda`}
                      className={({ isActive }) =>
                        `flex items-center gap-3 px-4 py-2 rounded-lg transition-colors text-sm ${
                          isActive
                            ? 'bg-primary-500 text-white'
                            : 'text-gray-400 hover:bg-slate-800 hover:text-gray-200'
                        }`
                      }
                    >
                      <Sparkles size={16} />
                      <span>Exploratory Analysis</span>
                    </NavLink>
                    <NavLink
                      to={`/${domain}/gold/jobs`}
                      className={({ isActive }) =>
                        `flex items-center gap-3 px-4 py-2 rounded-lg transition-colors text-sm ${
                          isActive
                            ? 'bg-primary-500 text-white'
                            : 'text-gray-400 hover:bg-slate-800 hover:text-gray-200'
                        }`
                      }
                    >
                      <BarChart3 size={16} />
                      <span>Job Monitoring</span>
                    </NavLink>
                  </div>
                )}
              </div>
            )
          }
          
          // Regular nav items
          return (
            <NavLink
              key={item.path}
              to={`/${domain}/${item.path}`}
              className={({ isActive }) =>
                `flex items-center gap-3 px-4 py-3 rounded-lg transition-colors ${
                  isActive
                    ? 'bg-primary-600 text-white'
                    : 'text-gray-300 hover:bg-slate-800'
                }`
              }
            >
              <item.icon size={20} />
              <span className="font-medium">{item.label}</span>
            </NavLink>
          )
        })}
      </nav>

      <div className="p-4 border-t border-slate-800">
        <div className="bg-slate-800 rounded-lg p-4">
          <h3 className="font-semibold text-sm mb-2">System Status</h3>
          <div className="space-y-2 text-sm">
            <div className="flex justify-between">
              <span className="text-gray-400">Pipeline</span>
              <span className="text-green-500 font-medium">[ACTIVE]</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-400">Last Run</span>
              <span className="text-gray-300">Today 14:20</span>
            </div>
          </div>
        </div>
      </div>
    </aside>
  )
}
