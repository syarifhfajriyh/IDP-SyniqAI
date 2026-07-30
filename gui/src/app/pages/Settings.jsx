import { useState } from 'react'
import Alert from '../components/ui/Alert'

export default function Settings() {
  const [saved, setSaved] = useState(false)

  const handleSave = () => {
    setSaved(true)
    setTimeout(() => setSaved(false), 3000)
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold text-gray-900">Settings</h1>
        <p className="text-gray-600 mt-1">Configure your dashboard and system preferences</p>
      </div>

      {/* General Settings */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">General Settings</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Theme</label>
            <select className="w-full border border-gray-300 rounded-lg px-4 py-2">
              <option>Light</option>
              <option>Dark</option>
              <option>Auto</option>
            </select>
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Auto-refresh Interval</label>
            <select className="w-full border border-gray-300 rounded-lg px-4 py-2">
              <option>Off</option>
              <option>1 minute</option>
              <option>5 minutes</option>
              <option>15 minutes</option>
            </select>
          </div>
        </div>
      </div>

      {/* Quality Thresholds */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Quality Thresholds</h3>
        <p className="text-sm text-gray-600 mb-4">Configure alert thresholds for quality monitoring</p>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Excellent Score Threshold (80-100)
              </label>
              <input type="range" min="80" max="100" defaultValue="90" className="w-full" />
              <div className="flex justify-between text-xs text-gray-500 mt-1">
                <span>80</span>
                <span className="font-semibold text-gray-900">90</span>
                <span>100</span>
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Good Score Threshold (60-90)
              </label>
              <input type="range" min="60" max="90" defaultValue="75" className="w-full" />
              <div className="flex justify-between text-xs text-gray-500 mt-1">
                <span>60</span>
                <span className="font-semibold text-gray-900">75</span>
                <span>90</span>
              </div>
            </div>
          </div>
          <div className="space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Completeness Warning (%)
              </label>
              <input type="range" min="50" max="100" defaultValue="80" className="w-full" />
              <div className="flex justify-between text-xs text-gray-500 mt-1">
                <span>50%</span>
                <span className="font-semibold text-gray-900">80%</span>
                <span>100%</span>
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Missing Values Alert (%)
              </label>
              <input type="range" min="5" max="50" defaultValue="20" className="w-full" />
              <div className="flex justify-between text-xs text-gray-500 mt-1">
                <span>5%</span>
                <span className="font-semibold text-gray-900">20%</span>
                <span>50%</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Data Source Configuration */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Data Source Configuration</h3>
        <div className="space-y-3">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Bronze Layer Path</label>
            <input 
              type="text" 
              defaultValue="../data lakehouse/syniq_project/bronze"
              className="w-full border border-gray-300 rounded-lg px-4 py-2"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Silver Layer Path</label>
            <input 
              type="text" 
              defaultValue="../data lakehouse/syniq_project/silver"
              className="w-full border border-gray-300 rounded-lg px-4 py-2"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">Gold Layer Path</label>
            <input 
              type="text" 
              defaultValue="../data lakehouse/syniq_project/gold"
              className="w-full border border-gray-300 rounded-lg px-4 py-2"
            />
          </div>
        </div>
      </div>

      {/* Save Buttons */}
      <div className="flex gap-4">
        <button className="btn-primary" onClick={handleSave}>
          Save Settings
        </button>
        <button className="btn-secondary">
          Reset to Default
        </button>
      </div>

      {saved && (
        <Alert type="success">
          [SUCCESS] Settings saved successfully!
        </Alert>
      )}

      {/* System Information */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">System Information</h3>
        <div className="grid grid-cols-3 gap-6 text-sm">
          <div>
            <p className="text-gray-600">Version</p>
            <p className="font-semibold mt-1">1.0.0</p>
          </div>
          <div>
            <p className="text-gray-600">Build</p>
            <p className="font-semibold mt-1">20260223</p>
          </div>
          <div>
            <p className="text-gray-600">License</p>
            <p className="font-semibold mt-1">Enterprise</p>
          </div>
        </div>
      </div>
    </div>
  )
}
