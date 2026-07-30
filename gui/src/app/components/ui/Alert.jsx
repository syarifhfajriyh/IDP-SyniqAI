import { AlertCircle, CheckCircle, Info, AlertTriangle } from 'lucide-react'

const alertStyles = {
  success: {
    bg: 'bg-green-50',
    border: 'border-green-200',
    text: 'text-green-800',
    icon: CheckCircle,
  },
  error: {
    bg: 'bg-red-50',
    border: 'border-red-200',
    text: 'text-red-800',
    icon: AlertCircle,
  },
  warning: {
    bg: 'bg-orange-50',
    border: 'border-orange-200',
    text: 'text-orange-800',
    icon: AlertTriangle,
  },
  info: {
    bg: 'bg-blue-50',
    border: 'border-blue-200',
    text: 'text-blue-800',
    icon: Info,
  },
}

export default function Alert({ type = 'info', children }) {
  const style = alertStyles[type]
  const Icon = style.icon

  return (
    <div className={`${style.bg} ${style.border} border rounded-lg p-4 flex items-start gap-3`}>
      <Icon size={20} className={style.text} />
      <div className={`flex-1 text-sm ${style.text}`}>{children}</div>
    </div>
  )
}
