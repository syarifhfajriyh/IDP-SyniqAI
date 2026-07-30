export default function QualityBadge({ score }) {
  const getQualityLevel = (score) => {
    if (score >= 90) return { label: '[EXCELLENT]', class: 'quality-excellent' }
    if (score >= 75) return { label: '[GOOD]', class: 'quality-good' }
    if (score >= 60) return { label: '[FAIR]', class: 'quality-fair' }
    return { label: '[POOR]', class: 'quality-poor' }
  }

  const { label, class: className } = getQualityLevel(score)

  return (
    <span className={className}>
      {label} {score.toFixed(1)}
    </span>
  )
}
