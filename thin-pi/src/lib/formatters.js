// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

/**
 * Formatting utilities for industrial telemetry display.
 */

/**
 * Format epoch milliseconds or seconds to a human-readable timestamp.
 * @param {number} epoch - Epoch timestamp (seconds or milliseconds)
 * @param {boolean} [dateOnly=false] - If true, include date component
 * @returns {string}
 */
export function formatTimestamp(epoch, dateOnly = false) {
  if (!epoch && epoch !== 0) return '—';

  // Auto-detect seconds vs milliseconds
  const ms = epoch > 1e12 ? epoch : epoch * 1000;
  const d = new Date(ms);

  if (isNaN(d.getTime())) return '—';

  const hh = String(d.getHours()).padStart(2, '0');
  const mm = String(d.getMinutes()).padStart(2, '0');
  const ss = String(d.getSeconds()).padStart(2, '0');
  const time = `${hh}:${mm}:${ss}`;

  if (!dateOnly) return time;

  const yyyy = d.getFullYear();
  const mo = String(d.getMonth() + 1).padStart(2, '0');
  const dd = String(d.getDate()).padStart(2, '0');
  return `${yyyy}-${mo}-${dd} ${time}`;
}

/**
 * Format a numeric value with fixed decimals.
 * @param {number|null|undefined} val
 * @param {number} [decimals=2]
 * @returns {string}
 */
export function formatValue(val, decimals = 2) {
  if (val === null || val === undefined || isNaN(val)) return '—';
  return Number(val).toFixed(decimals);
}

/**
 * Format a value with its engineering unit.
 * @param {number|null} val
 * @param {string} unit
 * @param {number} [decimals=2]
 * @returns {string}
 */
export function formatEngValue(val, unit, decimals = 2) {
  if (val === null || val === undefined || isNaN(val)) return '—';
  return `${Number(val).toFixed(decimals)} ${unit}`.trim();
}

/**
 * Format bytes to human-readable KB/MB/GB.
 * @param {number} bytes
 * @returns {string}
 */
export function formatBytes(bytes) {
  if (!bytes || bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const k = 1024;
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${units[i]}`;
}

/**
 * Format a duration in seconds to a human-readable string.
 * @param {number} totalSeconds
 * @returns {string}
 */
export function formatDuration(totalSeconds) {
  if (!totalSeconds || totalSeconds < 0) return '0s';

  const d = Math.floor(totalSeconds / 86400);
  const h = Math.floor((totalSeconds % 86400) / 3600);
  const m = Math.floor((totalSeconds % 3600) / 60);
  const s = Math.floor(totalSeconds % 60);

  const parts = [];
  if (d > 0) parts.push(`${d}d`);
  if (h > 0) parts.push(`${h}h`);
  if (m > 0) parts.push(`${m}m`);
  if (s > 0 || parts.length === 0) parts.push(`${s}s`);

  return parts.join(' ');
}

/**
 * Tag color palette for multi-series trend charts.
 */
export const TAG_COLORS = [
  '#06b6d4', // cyan
  '#f43f5e', // rose
  '#eab308', // yellow
  '#10b981', // emerald
  '#8b5cf6', // violet
  '#f97316', // orange
  '#ec4899', // pink
  '#14b8a6', // teal
  '#6366f1', // indigo
  '#84cc16', // lime
];
