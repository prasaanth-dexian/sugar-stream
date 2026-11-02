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
  Plug,
  PlugZap,
  Play,
  Radio,
  Loader2,
  Cpu,
  Server,
  Compass,
  CheckCircle2,
  RefreshCw,
} from 'lucide-react';

/**
 * Industrial Connection Manager with Real-time Auto-Discovery.
 *
 * Automatically discovers active local OPC UA and OPC DA servers on the system,
 * alongside custom endpoints and built-in offline simulation.
 */
export default function ConnectionPanel({ onConnect }) {
  const [endpoint, setEndpoint] = useState('');
  const [connecting, setConnecting] = useState(false);
  const [discovering, setDiscovering] = useState(false);
  const [discoveredServers, setDiscoveredServers] = useState([]);
  const [error, setError] = useState(null);

  const handleDiscover = async () => {
    setDiscovering(true);
    setError(null);
    try {
      const servers = await invoke('discover_local_servers');
      setDiscoveredServers(servers || []);
    } catch (err) {
      console.warn('Auto-discovery error:', err);
    } finally {
      setDiscovering(false);
    }
  };

  useEffect(() => {
    handleDiscover();
  }, []);

  const handleConnect = async (useSimulator = false, overrideUrl = null) => {
    setConnecting(true);
    setError(null);

    try {
      const url = useSimulator ? 'simulator' : (overrideUrl || endpoint.trim());
      await invoke('connect_opc_server', { endpointUrl: url });
      onConnect?.(true);
    } catch (err) {
      setError(String(err));
      setConnecting(false);
    }
  };

  return (
    <div className="space-y-3">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Plug className="w-4 h-4 text-accent" />
          <span className="text-xs font-semibold text-industrial-200">
            Connection Manager
          </span>
        </div>
        <button
          onClick={handleDiscover}
          disabled={discovering}
          title="Auto-scan local OPC UA & DA servers"
          className="flex items-center gap-1 text-[10px] text-accent hover:text-accent-hover transition-colors disabled:opacity-50"
        >
          <RefreshCw className={`w-3 h-3 ${discovering ? 'animate-spin' : ''}`} />
          <span>Scan</span>
        </button>
      </div>

      {/* Auto-Discovered Servers Section */}
      {discoveredServers.length > 0 && (
        <div className="space-y-1.5 bg-industrial-900/60 p-2 rounded-lg border border-industrial-800">
          <div className="flex items-center gap-1.5 text-[10px] font-semibold text-industrial-300">
            <Compass className="w-3 h-3 text-accent" />
            <span>Discovered Local Servers ({discoveredServers.length})</span>
          </div>

          <div className="space-y-1">
            {discoveredServers.map((srv) => {
              const isUa = srv.server_type === 'OPC UA';
              return (
                <div
                  key={srv.endpoint}
                  className="flex items-center justify-between p-2 rounded bg-industrial-800/80 hover:bg-industrial-800 border border-industrial-700/60 transition-all group"
                >
                  <div className="min-w-0 flex-1 pr-2">
                    <div className="flex items-center gap-1.5">
                      <span
                        className={`text-[9px] px-1.5 py-0.2 rounded font-mono font-bold ${
                          isUa
                            ? 'bg-emerald-950 text-emerald-400 border border-emerald-800'
                            : 'bg-indigo-950 text-indigo-400 border border-indigo-800'
                        }`}
                      >
                        {srv.server_type}
                      </span>
                      <span className="text-[11px] font-medium text-industrial-200 truncate">
                        {srv.name}
                      </span>
                    </div>
                    <div className="text-[9px] text-industrial-400 truncate mt-0.5">
                      {srv.endpoint}
                    </div>
                  </div>

                  <button
                    onClick={() => {
                      setEndpoint(srv.endpoint);
                      handleConnect(false, srv.endpoint);
                    }}
                    disabled={connecting}
                    className={`px-2.5 py-1 text-[10px] font-semibold text-white rounded transition-colors disabled:opacity-50 ${
                      isUa
                        ? 'bg-emerald-600 hover:bg-emerald-500'
                        : 'bg-indigo-600 hover:bg-indigo-500'
                    }`}
                  >
                    Connect
                  </button>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Offline Simulator Start */}
      <button
        onClick={() => handleConnect(true)}
        disabled={connecting}
        className="w-full flex items-center justify-center gap-2 px-3 py-2 text-xs font-semibold bg-industrial-800 hover:bg-industrial-700 border border-industrial-700 text-industrial-200 rounded-lg transition-colors disabled:opacity-50"
      >
        {connecting ? (
          <Loader2 className="w-4 h-4 animate-spin" />
        ) : (
          <Play className="w-4 h-4 text-accent" />
        )}
        Start Offline Simulator
      </button>

      <div className="flex items-center gap-2 text-[10px] text-industrial-500">
        <div className="flex-1 border-t border-industrial-700" />
        <span>or custom OPC endpoint</span>
        <div className="flex-1 border-t border-industrial-700" />
      </div>

      {/* Manual OPC Endpoint */}
      <div>
        <input
          type="text"
          value={endpoint}
          onChange={(e) => setEndpoint(e.target.value)}
          placeholder="opc.tcp://... or opc.da://..."
          className="w-full text-xs bg-industrial-800 border border-industrial-700 rounded px-3 py-2 text-industrial-200 placeholder-industrial-500 focus:border-accent"
        />
      </div>

      <button
        onClick={() => handleConnect(false)}
        disabled={connecting || !endpoint.trim()}
        className="w-full flex items-center justify-center gap-2 px-3 py-2 text-xs font-medium bg-industrial-800 border border-industrial-700 text-industrial-300 rounded hover:bg-industrial-700 transition-colors disabled:opacity-50"
      >
        <PlugZap className="w-3.5 h-3.5" />
        Connect Custom Endpoint
      </button>

      {/* Error */}
      {error && (
        <div className="text-[10px] text-opc-red bg-opc-red/10 border border-opc-red/20 rounded p-2">
          {error}
        </div>
      )}

      {/* Info */}
      <div className="text-[10px] text-industrial-500 leading-relaxed">
        <Radio className="w-3 h-3 inline mr-1 text-industrial-500" />
        Auto-discovers local OPC UA (TCP) and OPC DA (COM) instances on your machine.
      </div>
    </div>
  );
}
