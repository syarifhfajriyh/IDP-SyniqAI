export default function MetricCard({ label, value, delta, icon: Icon, trend }) {
  const getTrendColor = () => {
    if (!trend) return 'text-gray-500'
    return trend === 'up' ? 'text-green-600' : 'text-red-600'
  }

  return (
    <div className="metric-card">
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <p className="text-sm font-medium text-gray-600">{label}</p>
          <p className="text-3xl font-bold mt-2 text-gray-900">{value}</p>
          {delta && (
            <p className={`text-sm mt-2 ${getTrendColor()}`}>{delta}</p>
          )}
        </div>
        {Icon && (
          <div className="p-3 bg-primary-50 rounded-lg">
            <Icon size={24} className="text-primary-600" />
          </div>
        )}
      </div>
    </div>
  )
}
