import { useNavigate } from 'react-router-dom'
import { Building2, Heart, LayoutGrid } from 'lucide-react'

export default function DomainSelection() {
  const navigate = useNavigate()

  const domains = [
    {
      id: 'finance',
      name: 'FINANCE',
      icon: Building2,
      description: 'Financial data processing and analytics',
      color: 'from-blue-600 to-blue-800'
    },
    {
      id: 'healthcare',
      name: 'HEALTHCARE',
      icon: Heart,
      description: 'Healthcare data processing and analytics',
      color: 'from-emerald-600 to-emerald-800'
    },
    {
      id: 'general',
      name: 'GENERAL',
      icon: LayoutGrid,
      description: 'General purpose data processing',
      color: 'from-purple-600 to-purple-800'
    }
  ]

  const handleDomainSelect = (domainId) => {
    // Store selected domain in sessionStorage
    sessionStorage.setItem('selectedDomain', domainId)
    // Navigate to dashboard for that domain
    navigate(`/${domainId}/dashboard`)
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-blue-900 to-slate-900 flex items-center justify-center p-8">
      <div className="max-w-6xl w-full">
        {/* Header */}
        <div className="text-center mb-12">
          <h1 className="text-5xl font-bold text-white mb-4">
            SyniqAI Data Lakehouse
          </h1>
          <p className="text-xl text-blue-200">
            Select Your Data Domain
          </p>
        </div>

        {/* Domain Cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
          {domains.map((domain) => {
            const Icon = domain.icon
            return (
              <div
                key={domain.id}
                onClick={() => handleDomainSelect(domain.id)}
                className="group cursor-pointer"
              >
                <div className="bg-slate-800/50 backdrop-blur-sm border border-slate-700 rounded-2xl p-8 transition-all duration-300 hover:bg-slate-800/70 hover:border-blue-500 hover:shadow-2xl hover:shadow-blue-500/20 hover:-translate-y-2">
                  {/* Icon */}
                  <div className="flex justify-center mb-6">
                    <div className={`w-24 h-24 bg-gradient-to-br ${domain.color} rounded-full flex items-center justify-center group-hover:scale-110 transition-transform duration-300`}>
                      <Icon className="w-12 h-12 text-white" />
                    </div>
                  </div>

                  {/* Title */}
                  <h2 className="text-2xl font-bold text-white text-center mb-4 group-hover:text-blue-300 transition-colors duration-300">
                    {domain.name}
                  </h2>

                  {/* Description */}
                  <p className="text-slate-400 text-center mb-6 group-hover:text-slate-300 transition-colors duration-300">
                    {domain.description}
                  </p>

                  {/* Button */}
                  <button className="w-full bg-blue-600 hover:bg-blue-500 text-white font-semibold py-3 px-6 rounded-lg transition-all duration-300 group-hover:shadow-lg group-hover:shadow-blue-500/50">
                    Select Domain
                  </button>
                </div>
              </div>
            )
          })}
        </div>

        {/* Footer */}
        <div className="text-center mt-12 text-slate-400 text-sm">
          Each domain contains Bronze, Silver, and Gold layers for data processing
        </div>
      </div>
    </div>
  )
}
