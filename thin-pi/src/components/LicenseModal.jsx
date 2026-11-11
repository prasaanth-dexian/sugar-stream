// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

import React, { useState, useEffect } from 'react';
import { invoke } from '@tauri-apps/api/core';
import {
  Shield,
  Key,
  Copy,
  CheckCircle,
  XCircle,
  X,
  Loader2,
} from 'lucide-react';

/**
 * License activation modal.
 *
 * Shows Hardware ID (copiable), license key input, feature comparison,
 * and activation feedback.
 */
export default function LicenseModal({ isOpen, onClose }) {
  const [hwid, setHwid] = useState('');
  const [licenseKey, setLicenseKey] = useState('');
  const [status, setStatus] = useState(null); // { type: 'success'|'error', message }
  const [loading, setLoading] = useState(false);
  const [copied, setCopied] = useState(false);
  const [devLicense, setDevLicense] = useState('');

  // Fetch hardware ID on open
  useEffect(() => {
    if (isOpen) {
      invoke('get_hardware_id_cmd')
        .then(setHwid)
        .catch((err) => setHwid(`Error: ${err}`));
    }
  }, [isOpen]);

  const handleCopyHwid = async () => {
    try {
      await navigator.clipboard.writeText(hwid);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Fallback
      const ta = document.createElement('textarea');
      ta.value = hwid;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  const handleActivate = async () => {
    if (!licenseKey.trim()) {
      setStatus({ type: 'error', message: 'Please enter a license key.' });
      return;
    }

    setLoading(true);
    setStatus(null);

    try {
      const result = await invoke('apply_license_key', {
        licenseStr: licenseKey.trim(),
      });
      setStatus({
        type: 'success',
        message: `License activated! Tier: ${result.tier.toUpperCase()}, Licensee: ${result.licensee}`,
      });
    } catch (err) {
      setStatus({ type: 'error', message: String(err) });
    } finally {
      setLoading(false);
    }
  };

  const handleGenerateDevLicense = async () => {
    try {
      const key = await invoke('generate_dev_license_cmd');
      setDevLicense(key);
      setLicenseKey(key);
    } catch (err) {
      setStatus({ type: 'error', message: `Dev key generation failed: ${err}` });
    }
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/60 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="relative bg-industrial-900 border border-industrial-700 rounded-xl shadow-2xl w-[520px] max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-industrial-700">
          <div className="flex items-center gap-2">
            <Shield className="w-5 h-5 text-accent" />
            <h2 className="text-sm font-semibold text-industrial-100">
              License & Settings
            </h2>
          </div>
          <button
            onClick={onClose}
            className="p-1 rounded hover:bg-industrial-700 transition-colors"
          >
            <X className="w-4 h-4 text-industrial-400" />
          </button>
        </div>

        <div className="p-4 space-y-4">
          {/* Hardware ID */}
          <div>
            <label className="text-[11px] font-semibold text-industrial-400 uppercase tracking-wider">
              Hardware ID
            </label>
            <div className="mt-1 flex items-center gap-2">
              <code className="flex-1 text-[10px] bg-industrial-800 border border-industrial-700 rounded px-3 py-2 text-industrial-300 font-mono break-all select-all">
                {hwid || 'Loading...'}
              </code>
              <button
                onClick={handleCopyHwid}
                className="p-2 rounded bg-industrial-800 border border-industrial-700 hover:bg-industrial-700 transition-colors"
                title="Copy Hardware ID"
              >
                {copied ? (
                  <CheckCircle className="w-3.5 h-3.5 text-opc-green" />
                ) : (
                  <Copy className="w-3.5 h-3.5 text-industrial-400" />
                )}
              </button>
            </div>
          </div>

          {/* License Key Input */}
          <div>
            <label className="text-[11px] font-semibold text-industrial-400 uppercase tracking-wider">
              License Key
            </label>
            <textarea
              value={licenseKey}
              onChange={(e) => setLicenseKey(e.target.value)}
              placeholder="Paste your license key here..."
              rows={3}
              className="mt-1 w-full text-xs bg-industrial-800 border border-industrial-700 rounded px-3 py-2 text-industrial-200 placeholder-industrial-500 focus:border-accent font-mono resize-none"
            />
            <div className="flex items-center gap-2 mt-2">
              <button
                onClick={handleActivate}
                disabled={loading}
                className="flex items-center gap-1.5 px-4 py-2 text-xs font-semibold bg-accent hover:bg-accent-hover text-white rounded transition-colors disabled:opacity-50"
              >
                {loading ? (
                  <Loader2 className="w-3.5 h-3.5 animate-spin" />
                ) : (
                  <Key className="w-3.5 h-3.5" />
                )}
                Activate License
              </button>
              <button
                onClick={handleGenerateDevLicense}
                className="px-3 py-2 text-xs text-industrial-400 bg-industrial-800 border border-industrial-700 rounded hover:bg-industrial-700 transition-colors"
              >
                Generate Dev Key
              </button>
            </div>
          </div>

          {/* Status Message */}
          {status && (
            <div
              className={`flex items-start gap-2 p-3 rounded text-xs ${
                status.type === 'success'
                  ? 'bg-opc-green/10 border border-opc-green/30 text-opc-green'
                  : 'bg-opc-red/10 border border-opc-red/30 text-opc-red'
              }`}
            >
              {status.type === 'success' ? (
                <CheckCircle className="w-4 h-4 shrink-0 mt-0.5" />
              ) : (
                <XCircle className="w-4 h-4 shrink-0 mt-0.5" />
              )}
              <span>{status.message}</span>
            </div>
          )}

          {/* Feature Comparison */}
          <div>
            <h3 className="text-[11px] font-semibold text-industrial-400 uppercase tracking-wider mb-2">
              Feature Comparison
            </h3>
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-industrial-700">
                  <th className="text-left py-1.5 text-industrial-400 font-medium">Feature</th>
                  <th className="text-center py-1.5 text-opc-yellow font-medium">Community</th>
                  <th className="text-center py-1.5 text-opc-green font-medium">Professional</th>
                </tr>
              </thead>
              <tbody className="text-industrial-300">
                <tr className="border-b border-industrial-800">
                  <td className="py-1.5">Active Tags</td>
                  <td className="text-center">≤ 15</td>
                  <td className="text-center">Unlimited</td>
                </tr>
                <tr className="border-b border-industrial-800">
                  <td className="py-1.5">Cloud Sync</td>
                  <td className="text-center text-opc-red">✗</td>
                  <td className="text-center text-opc-green">✓</td>
                </tr>
                <tr className="border-b border-industrial-800">
                  <td className="py-1.5">CSV Export</td>
                  <td className="text-center">Single tag</td>
                  <td className="text-center">Multi-tag batch</td>
                </tr>
                <tr className="border-b border-industrial-800">
                  <td className="py-1.5">Trend Window</td>
                  <td className="text-center">1 hour</td>
                  <td className="text-center">Unlimited</td>
                </tr>
                <tr>
                  <td className="py-1.5">Data Retention</td>
                  <td className="text-center">7 days</td>
                  <td className="text-center">30+ days</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
