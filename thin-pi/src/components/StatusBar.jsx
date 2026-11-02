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
  Database,
  Wifi,
  WifiOff,
  Cloud,
  Shield,
  Clock,
  HardDrive,
} from 'lucide-react';
import { useInterval } from '../hooks/useInterval';
import { formatBytes, formatDuration } from '../lib/formatters';

/**
 * Status bar footer showing live system health metrics.
 *
 * Polls get_system_stats every 2 seconds.
 */
export default function StatusBar({ licenseStatus }) {
  const [stats, setStats] = useState(null);
  const [now, setNow] = useState(new Date());

  // Poll system stats
  useInterval(async () => {
    try {
      const result = await invoke('get_system_stats');
      setStats(result);
    } catch (err) {
      // Silently handle polling errors
    }
    setNow(new Date());
  }, 2000);

  // Initial fetch
  useEffect(() => {
    invoke('get_system_stats')
      .then(setStats)
      .catch(() => {});
  }, []);

  const tierColor =
    licenseStatus?.tier === 'professional'
      ? 'text-opc-green'
      : 'text-opc-yellow';

  const tierLabel =
    licenseStatus?.tier === 'professional' ? 'PRO' : 'FREE';

  return (
    <div className="h-8 shrink-0 bg-industrial-900 border-t border-industrial-700 flex items-center px-3 text-[10px] text-industrial-400 gap-1">
      {/* OPC Connection */}
      <StatusItem
        icon={stats?.opc_connected ? Wifi : WifiOff}
        iconColor={stats?.opc_connected ? 'text-opc-green' : 'text-opc-red'}
        label="OPC"
        value={
          stats?.opc_connected
            ? `${stats.opc_latency_ms}ms`
            : 'Disconnected'
        }
      />

      <Divider />

      {/* Database */}
      <StatusItem
        icon={Database}
        iconColor="text-opc-blue"
        label="DB"
        value={stats ? `${stats.db_size_mb.toFixed(1)} MB` : '—'}
      />

      <Divider />

      {/* Total Rows */}
      <StatusItem
        icon={HardDrive}
        iconColor="text-industrial-400"
        label="Rows"
        value={stats ? stats.total_rows.toLocaleString() : '—'}
      />

      <Divider />

      {/* Sync Queue */}
      <StatusItem
        icon={Cloud}
        iconColor={
          stats?.pending_sync > 0 ? 'text-opc-yellow' : 'text-industrial-500'
        }
        label="Sync"
        value={stats ? `${stats.pending_sync.toLocaleString()} pending` : '—'}
      />

      <Divider />

      {/* License */}
      <div className="flex items-center gap-1">
        <Shield className={`w-3 h-3 ${tierColor}`} />
        <span className={`font-bold ${tierColor}`}>{tierLabel}</span>
      </div>

      {/* Spacer */}
      <div className="flex-1" />

      {/* Uptime */}
      <StatusItem
        icon={Clock}
        iconColor="text-industrial-500"
        label="Up"
        value={stats ? formatDuration(stats.uptime_secs) : '—'}
      />

      <Divider />

      {/* Current Time */}
      <span className="font-mono text-industrial-300">
        {now.toLocaleTimeString('en-GB', { hour12: false })}
      </span>
    </div>
  );
}

function StatusItem({ icon: Icon, iconColor, label, value }) {
  return (
    <div className="flex items-center gap-1">
      <Icon className={`w-3 h-3 ${iconColor}`} />
      <span className="text-industrial-500">{label}:</span>
      <span className="text-industrial-300 font-mono">{value}</span>
    </div>
  );
}

function Divider() {
  return <span className="text-industrial-700 mx-1.5">│</span>;
}
