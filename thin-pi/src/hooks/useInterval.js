// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

import { useEffect, useRef } from 'react';

/**
 * Custom hook for running a callback on a fixed interval.
 *
 * @param {Function} callback - Function to call on each tick
 * @param {number|null} delay - Interval in milliseconds, or null to pause
 */
export function useInterval(callback, delay) {
  const savedCallback = useRef(callback);

  // Remember the latest callback
  useEffect(() => {
    savedCallback.current = callback;
  }, [callback]);

  // Set up the interval
  useEffect(() => {
    if (delay === null || delay === undefined) return;

    const id = setInterval(() => {
      savedCallback.current();
    }, delay);

    return () => clearInterval(id);
  }, [delay]);
}
