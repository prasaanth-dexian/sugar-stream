// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

import React, { useState, useCallback } from 'react';
import { Settings, Activity } from 'lucide-react';
import TagTree from './TagTree';
import TrendChart from './TrendChart';
import StatusBar from './StatusBar';
import LicenseModal from './LicenseModal';
import ConnectionPanel from './ConnectionPanel';

/**
 * Three-panel industrial shell layout.
 *
 * ┌─────────────┬──────────────────────────────┐
 * │  Tag Tree   │  Trend Viewport + Toolbar    │
 * │  Sidebar    │                              │
 * │  (280px)    │                              │
 * │             │                              │
 * ├─────────────┴──────────────────────────────┤
 * │  Status Bar (40px)                         │
 * └────────────────────────────────────────────┘
 */
export default function Layout({
  selectedTags,
  onSelectionChange,
  isConnected,
  onConnect,
  licenseStatus,
}) {
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [licenseModalOpen, setLicenseModalOpen] = useState(false);

  const selectedTagIds = selectedTags.map((t) => t.tag_id);

  return (
    <div className="flex flex-col h-screen bg-industrial-950 text-industrial-100">
      {/* ── Top Bar ────────────────────────────────────────── */}
      <div className="flex items-center justify-between h-10 px-4 bg-industrial-900 border-b border-industrial-700 shrink-0">
        <div className="flex items-center gap-2">
          <Activity className="w-4 h-4 text-accent" />
          <span className="text-sm font-semibold tracking-wide">THIN PI</span>
          <span className="text-[10px] text-industrial-500 ml-1">Industrial Telemetry</span>
        </div>
        <button
          onClick={() => setLicenseModalOpen(true)}
          className="p-1.5 rounded hover:bg-industrial-700 transition-colors"
          title="Settings & License"
        >
          <Settings className="w-4 h-4 text-industrial-400" />
        </button>
      </div>

      {/* ── Main Content ──────────────────────────────────── */}
      <div className="flex flex-1 overflow-hidden">
        {/* Sidebar */}
        {sidebarOpen && (
          <div className="w-[280px] shrink-0 border-r border-industrial-700 bg-industrial-900 flex flex-col overflow-hidden">
            {!isConnected ? (
              <div className="p-3">
                <ConnectionPanel onConnect={onConnect} />
              </div>
            ) : (
              <TagTree
                selectedTags={selectedTags}
                onSelectionChange={onSelectionChange}
                isConnected={isConnected}
              />
            )}
          </div>
        )}

        {/* Toggle sidebar button */}
        <button
          onClick={() => setSidebarOpen(!sidebarOpen)}
          className="w-4 shrink-0 bg-industrial-800 hover:bg-industrial-700 border-r border-industrial-700 flex items-center justify-center transition-colors cursor-pointer"
          title={sidebarOpen ? 'Collapse sidebar' : 'Expand sidebar'}
        >
          <span className="text-industrial-500 text-[10px]">
            {sidebarOpen ? '◀' : '▶'}
          </span>
        </button>

        {/* Main Viewport */}
        <div className="flex-1 flex flex-col overflow-hidden p-3">
          <TrendChart
            selectedTags={selectedTags}
            selectedTagIds={selectedTagIds}
            isConnected={isConnected}
          />
        </div>
      </div>

      {/* ── Status Bar ────────────────────────────────────── */}
      <StatusBar licenseStatus={licenseStatus} />

      {/* ── License Modal ─────────────────────────────────── */}
      <LicenseModal
        isOpen={licenseModalOpen}
        onClose={() => setLicenseModalOpen(false)}
      />
    </div>
  );
}
