// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

import { useState, useEffect, useCallback, useRef } from 'react';
import { invoke } from '@tauri-apps/api/core';

/**
 * Custom hook for invoking Tauri IPC commands with loading/error state.
 *
 * @param {string} commandName - Tauri command name
 * @param {object|null} [args=null] - Arguments to pass to the command
 * @param {object} [options={}] - Configuration options
 * @param {number|null} [options.pollInterval=null] - Auto-refetch interval (ms)
 * @param {boolean} [options.enabled=true] - Whether to auto-fetch on mount
 * @param {Function} [options.onSuccess] - Callback on successful fetch
 * @param {Function} [options.onError] - Callback on error
 * @returns {{ data: any, loading: boolean, error: string|null, refetch: Function }}
 */
export function useTauriCommand(commandName, args = null, options = {}) {
  const {
    pollInterval = null,
    enabled = true,
    onSuccess,
    onError,
  } = options;

  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const mountedRef = useRef(true);

  const refetch = useCallback(async (overrideArgs) => {
    setLoading(true);
    setError(null);

    try {
      const result = await invoke(commandName, overrideArgs || args || {});
      if (mountedRef.current) {
        setData(result);
        setLoading(false);
        onSuccess?.(result);
      }
      return result;
    } catch (err) {
      const errMsg = typeof err === 'string' ? err : String(err);
      if (mountedRef.current) {
        setError(errMsg);
        setLoading(false);
        onError?.(errMsg);
      }
      throw err;
    }
  }, [commandName, JSON.stringify(args)]);

  // Initial fetch
  useEffect(() => {
    mountedRef.current = true;
    if (enabled) {
      refetch();
    }
    return () => {
      mountedRef.current = false;
    };
  }, [enabled, refetch]);

  // Polling
  useEffect(() => {
    if (!pollInterval || !enabled) return;

    const id = setInterval(() => {
      refetch().catch(() => {}); // swallow polling errors
    }, pollInterval);

    return () => clearInterval(id);
  }, [pollInterval, enabled, refetch]);

  return { data, loading, error, refetch };
}
