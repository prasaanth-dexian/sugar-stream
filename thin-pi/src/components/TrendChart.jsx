// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

import React, { useState, useRef, useMemo, useCallback, useEffect } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { RotateCcw, TrendingUp } from 'lucide-react';
import UPlotChart, { wheelZoomPlugin, tooltipPlugin } from '../lib/uplot-react';
import ExportButton from './ExportButton';
import { useInterval } from '../hooks/useInterval';
import { TAG_COLORS, formatValue, formatEngValue } from '../lib/formatters';

const TIME_WINDOWS = [
  { label: '1m', seconds: 60 },
  { label: '5m', seconds: 300 },
  { label: '15m', seconds: 900 },
  { label: '1h', seconds: 3600 },
  { label: '4h', seconds: 14400 },
  { label: '24h', seconds: 86400 },
];

/**
 * Main trend viewport with uPlot canvas rendering.
 *
 * Features:
 * - Multi-tag overlay with per-unit Y axes
 * - Auto-refresh at 1s intervals
 * - LTTB downsampling via viewport_width param
 * - Time window selector
 * - Pan/zoom with wheel + drag
 * - Min/Max/Avg/Current stats panel
 */
export default function TrendChart({ selectedTags, selectedTagIds, isConnected }) {
  const chartRef = useRef(null);
  const containerRef = useRef(null);
  const [windowIdx, setWindowIdx] = useState(1); // Default 5m
  const [trendData, setTrendData] = useState(null);
  const [stats, setStats] = useState([]);

  const timeWindow = TIME_WINDOWS[windowIdx];
  const hasTags = selectedTagIds.length > 0 && isConnected;

  // Build uPlot options (memoized on tag selection changes)
  const chartOptions = useMemo(() => {
    if (!selectedTags.length) return null;

    // Group Y axes by engineering unit
    const unitMap = new Map();
    selectedTags.forEach((tag, i) => {
      const unit = tag.eng_unit || 'value';
      if (!unitMap.has(unit)) {
        unitMap.set(unit, { unit, side: unitMap.size % 2 === 0 ? 3 : 1 });
      }
    });

    const axes = [
      {
        // X axis (time)
        stroke: '#718096',
        grid: { stroke: '#1a2234', width: 1 },
        ticks: { stroke: '#243049' },
        values: (u, vals) =>
          vals.map((v) => {
            const d = new Date(v * 1000);
            return d.toLocaleTimeString('en-GB', { hour12: false });
          }),
      },
    ];

    const scales = { x: { time: false } };

    unitMap.forEach(({ unit, side }) => {
      const scaleKey = unit.replace(/[^a-zA-Z0-9]/g, '_') || 'val';
      scales[scaleKey] = { auto: true };
      axes.push({
        scale: scaleKey,
        label: unit,
        side,
        stroke: '#718096',
        grid: { stroke: axes.length === 1 ? '#1a2234' : 'transparent', width: 1 },
        ticks: { stroke: '#243049' },
        size: 60,
      });
    });

    const series = [{}]; // empty for X axis
    selectedTags.forEach((tag, i) => {
      const unit = tag.eng_unit || 'value';
      const scaleKey = unit.replace(/[^a-zA-Z0-9]/g, '_') || 'val';
      series.push({
        label: tag.name,
        scale: scaleKey,
        stroke: TAG_COLORS[i % TAG_COLORS.length],
        width: 1.5,
        points: { show: false },
      });
    });

    return {
      cursor: {
        drag: { x: true, y: false, setScale: true },
        sync: { key: 'thinpi' },
      },
      scales,
      axes,
      series,
      plugins: [wheelZoomPlugin({ factor: 0.15 }), tooltipPlugin()],
      legend: { show: false },
    };
  }, [selectedTags]);

  // Fetch trend data
  const fetchTrend = useCallback(async () => {
    if (!hasTags) return;

    const now = Date.now();
    const startTime = now - timeWindow.seconds * 1000;
    const viewportWidth = containerRef.current?.clientWidth || 1200;

    try {
      // Resolve tag_ids from the backend (tags registered by opc_engine)
      const result = await invoke('query_historical_trend', {
        tagIds: selectedTagIds,
        startTime,
        endTime: now,
        viewportWidth,
      });

      if (result && result.timestamps.length > 0) {
        // Build uPlot AlignedData: [timestamps, ...values]
        const data = [result.timestamps];
        result.series.forEach((s) => {
          data.push(s.values.map((v) => (v === null ? undefined : v)));
        });
        setTrendData(data);

        // Calculate statistics
        const newStats = result.series.map((s, i) => {
          const valid = s.values.filter((v) => v !== null && v !== undefined);
          if (valid.length === 0) {
            return { name: s.tag_name, unit: s.eng_unit, min: null, max: null, avg: null, current: null };
          }
          return {
            name: s.tag_name,
            unit: s.eng_unit,
            color: TAG_COLORS[i % TAG_COLORS.length],
            min: Math.min(...valid),
            max: Math.max(...valid),
            avg: valid.reduce((a, b) => a + b, 0) / valid.length,
            current: valid[valid.length - 1],
          };
        });
        setStats(newStats);
      }
    } catch (err) {
      console.error('Trend query failed:', err);
    }
  }, [hasTags, selectedTagIds, timeWindow]);

  // Auto-refresh every 1 second
  useInterval(fetchTrend, hasTags ? 1000 : null);

  // Fetch immediately on tag/window change
  useEffect(() => {
    fetchTrend();
  }, [fetchTrend]);

  // Get time range for export
  const now = Date.now();
  const exportStartTime = now - timeWindow.seconds * 1000;

  return (
    <div className="flex flex-col h-full gap-2" ref={containerRef}>
      {/* ── Toolbar ──────────────────────────────────────── */}
      <div className="flex items-center justify-between shrink-0">
        <div className="flex items-center gap-1.5">
          <TrendingUp className="w-4 h-4 text-accent" />
          <span className="text-sm font-semibold text-industrial-200">Trend Viewer</span>
          {trendData && trendData[0]?.length > 0 && (
            <span className="text-[10px] text-industrial-500 ml-2">
              {trendData[0].length.toLocaleString()} pts
            </span>
          )}
        </div>

        <div className="flex items-center gap-2">
          {/* Time window selector */}
          <div className="flex rounded overflow-hidden border border-industrial-700">
            {TIME_WINDOWS.map((tw, i) => (
              <button
                key={tw.label}
                onClick={() => setWindowIdx(i)}
                className={`px-2.5 py-1 text-[11px] font-medium transition-colors ${
                  i === windowIdx
                    ? 'bg-accent text-white'
                    : 'bg-industrial-800 text-industrial-400 hover:bg-industrial-700'
                }`}
              >
                {tw.label}
              </button>
            ))}
          </div>

          {/* Reset zoom */}
          <button
            onClick={() => chartRef.current?.resetZoom()}
            className="px-2.5 py-1 text-[11px] bg-industrial-800 border border-industrial-700 rounded text-industrial-300 hover:bg-industrial-700 flex items-center gap-1"
            title="Reset Zoom"
          >
            <RotateCcw className="w-3 h-3" />
            Reset
          </button>

          {/* CSV Export */}
          <ExportButton
            selectedTagIds={selectedTagIds}
            startTime={exportStartTime}
            endTime={now}
            disabled={!hasTags}
          />
        </div>
      </div>

      {/* ── Chart Area ───────────────────────────────────── */}
      <div className="flex-1 bg-industrial-900/60 rounded-lg border border-industrial-700 overflow-hidden relative">
        {!hasTags ? (
          <div className="absolute inset-0 flex flex-col items-center justify-center text-industrial-500">
            <TrendingUp className="w-12 h-12 mb-3 opacity-30" />
            <span className="text-sm">
              {isConnected
                ? 'Select tags from the sidebar to start trending'
                : 'Connect to an OPC server or start the simulator'}
            </span>
          </div>
        ) : chartOptions && trendData ? (
          <UPlotChart
            ref={chartRef}
            options={chartOptions}
            data={trendData}
            className="w-full h-full"
          />
        ) : (
          <div className="absolute inset-0 flex items-center justify-center text-industrial-500">
            <span className="text-sm">Loading trend data...</span>
          </div>
        )}
      </div>

      {/* ── Stats Bar ────────────────────────────────────── */}
      {stats.length > 0 && (
        <div className="shrink-0 grid gap-2" style={{ gridTemplateColumns: `repeat(${Math.min(stats.length, 5)}, 1fr)` }}>
          {stats.map((s, i) => (
            <div
              key={i}
              className="bg-industrial-900 border border-industrial-700 rounded px-3 py-2"
            >
              <div className="flex items-center gap-1.5 mb-1">
                <span
                  className="w-2 h-2 rounded-full"
                  style={{ background: s.color }}
                />
                <span className="text-[10px] font-semibold text-industrial-300 truncate">
                  {s.name}
                </span>
              </div>
              <div className="grid grid-cols-4 gap-1 text-[10px]">
                <div>
                  <span className="text-industrial-500">Min</span>
                  <div className="text-industrial-200 font-mono">
                    {formatEngValue(s.min, s.unit, 1)}
                  </div>
                </div>
                <div>
                  <span className="text-industrial-500">Max</span>
                  <div className="text-industrial-200 font-mono">
                    {formatEngValue(s.max, s.unit, 1)}
                  </div>
                </div>
                <div>
                  <span className="text-industrial-500">Avg</span>
                  <div className="text-industrial-200 font-mono">
                    {formatEngValue(s.avg, s.unit, 1)}
                  </div>
                </div>
                <div>
                  <span className="text-industrial-500">Now</span>
                  <div className="text-accent font-mono font-bold">
                    {formatEngValue(s.current, s.unit, 1)}
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
