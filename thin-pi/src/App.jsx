// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

import React, { useState, useEffect, useCallback } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { listen } from '@tauri-apps/api/event';
import Layout from './components/Layout';

/**
 * Root application component.
 *
 * Manages global state: tag selection, connection status, license,
 * and sync status. Renders the Layout shell.
 */
export default function App() {
  // ── State ─────────────────────────────────────────────────────────
  const [selectedTags, setSelectedTags] = useState([]);
  const [isConnected, setIsConnected] = useState(false);
  const [licenseStatus, setLicenseStatus] = useState({
    tier: 'community',
    licensee: 'Community Edition',
    max_tags: 15,
    cloud_sync: false,
    expires_at: 0,
    valid: true,
  });
  const [syncStatus, setSyncStatus] = useState({
    synced_count: 0,
    pending_count: 0,
    last_error: null,
  });

  // ── Tag Selection ─────────────────────────────────────────────────
  const handleSelectionChange = useCallback(
    (newSelection) => {
      // Enforce Community license tag limit
      if (
        licenseStatus.tier === 'community' &&
        newSelection.length > licenseStatus.max_tags
      ) {
        console.warn(
          `Community license: max ${licenseStatus.max_tags} tags allowed`
        );
        return;
      }
      setSelectedTags(newSelection);

      // Subscribe to selected tags
      const tagIds = newSelection.map((t) => t.node_id);
      if (tagIds.length > 0) {
        invoke('subscribe_tags', { tagIds }).catch(console.error);
      }
    },
    [licenseStatus]
  );

  // ── Connection ────────────────────────────────────────────────────
  const handleConnect = useCallback((connected) => {
    setIsConnected(connected);
  }, []);

  // ── Sync Status Events ────────────────────────────────────────────
  useEffect(() => {
    let unlisten;

    listen('sync-status', (event) => {
      setSyncStatus(event.payload);
    }).then((fn) => {
      unlisten = fn;
    });

    return () => {
      if (unlisten) unlisten();
    };
  }, []);

  // ── Render ────────────────────────────────────────────────────────
  return (
    <Layout
      selectedTags={selectedTags}
      onSelectionChange={handleSelectionChange}
      isConnected={isConnected}
      onConnect={handleConnect}
      licenseStatus={licenseStatus}
    />
  );
}
