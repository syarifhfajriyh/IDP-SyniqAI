import { useState, useEffect } from 'react'

export default function useFetch(fetchFn, dependencies = []) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  useEffect(() => {
    let cancelled = false

    const fetchData = async () => {
      try {
        setLoading(true)
        setError(null)
        const response = await fetchFn()
        if (!cancelled) {
          setData(response.data)
        }
      } catch (err) {
        if (!cancelled) {
          setError(err.message || 'An error occurred')
        }
      } finally {
        if (!cancelled) {
          setLoading(false)
        }
      }
    }

    fetchData()

    return () => {
      cancelled = true
    }
  }, dependencies)

  const refetch = async () => {
    setLoading(true)
    try {
      const response = await fetchFn()
      setData(response.data)
      setError(null)
    } catch (err) {
      setError(err.message || 'An error occurred')
    } finally {
      setLoading(false)
    }
  }

  return { data, loading, error, refetch }
}
