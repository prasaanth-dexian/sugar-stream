// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

import React, { useEffect, useRef, useImperativeHandle, forwardRef } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';

// ── uPlot React Wrapper ───────────────────────────────────────────────────────

/**
 * High-performance uPlot wrapper for React.
 *
 * CRITICAL RULES:
 * - NEVER recreate the uPlot instance on data changes — use setData()
 * - Use ResizeObserver for responsive sizing — never pass fixed w/h
 * - Data must be uPlot.AlignedData: [timestamps, ...valueArrays]
 *
 * @param {object} props
 * @param {object} props.options - uPlot options (without width/height)
 * @param {Array} props.data - uPlot AlignedData
 * @param {string} [props.className] - Container CSS class
 * @param {Function} [props.onReady] - Callback when uPlot is initialized
 */
const UPlotChart = forwardRef(({ options, data, className = '', onReady }, ref) => {
  const containerRef = useRef(null);
  const plotRef = useRef(null);
  const optionsRef = useRef(options);

  // Expose imperative methods
  useImperativeHandle(ref, () => ({
    getInstance: () => plotRef.current,
    setData: (newData, resetScales = false) => {
      if (plotRef.current) {
        plotRef.current.setData(newData, resetScales);
      }
    },
    resetZoom: () => {
      if (plotRef.current) {
        plotRef.current.setData(plotRef.current.data, true);
      }
    },
  }));

  // Initialize uPlot on mount (or when options change)
  useEffect(() => {
    if (!containerRef.current) return;

    optionsRef.current = options;

    const el = containerRef.current;
    const w = el.clientWidth || 800;
    const h = el.clientHeight || 400;

    const plotOpts = {
      ...options,
      width: w,
      height: h,
    };

    const plot = new uPlot(plotOpts, data, el);
    plotRef.current = plot;
    onReady?.(plot);

    // Responsive resize
    const ro = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { width, height } = entry.contentRect;
        if (width > 0 && height > 0 && plotRef.current) {
          plotRef.current.setSize({
            width: Math.floor(width),
            height: Math.floor(height),
          });
        }
      }
    });
    ro.observe(el);

    return () => {
      ro.disconnect();
      plot.destroy();
      plotRef.current = null;
    };
  }, [options]);

  // Update data WITHOUT destroying the instance
  useEffect(() => {
    if (plotRef.current && data && data[0]?.length > 0) {
      plotRef.current.setData(data, false);
    }
  }, [data]);

  return (
    <div
      ref={containerRef}
      className={className}
      style={{ minHeight: '300px' }}
    />
  );
});

UPlotChart.displayName = 'UPlotChart';

// ── Wheel Zoom Plugin ─────────────────────────────────────────────────────────

/**
 * Scroll-wheel zoom plugin for uPlot.
 *
 * @param {{ factor?: number }} [opts] - Zoom speed (default 0.15 = 15% per notch)
 * @returns {object} uPlot plugin
 */
export function wheelZoomPlugin(opts = {}) {
  const factor = opts.factor ?? 0.15;

  return {
    hooks: {
      ready: (u) => {
        const over = u.over;

        over.addEventListener('wheel', (e) => {
          e.preventDefault();

          const rect = over.getBoundingClientRect();
          const mouseX = e.clientX - rect.left;
          const width = rect.width;

          if (mouseX < 0 || mouseX > width) return;

          const leftPct = mouseX / width;
          const xMin = u.scales.x.min;
          const xMax = u.scales.x.max;
          if (xMin == null || xMax == null) return;

          const xRange = xMax - xMin;
          const dir = e.deltaY < 0 ? -1 : 1;
          const zoom = xRange * factor * dir;

          const newMin = xMin + zoom * leftPct;
          const newMax = xMax - zoom * (1 - leftPct);

          if (newMax > newMin) {
            u.setScale('x', { min: newMin, max: newMax });
          }
        }, { passive: false });
      },
    },
  };
}

// ── Tooltip / Crosshair Plugin ────────────────────────────────────────────────

/**
 * Dark-themed crosshair tooltip plugin for uPlot.
 * Shows all series values at the cursor position.
 *
 * @returns {object} uPlot plugin
 */
export function tooltipPlugin() {
  let tooltipEl = null;

  return {
    hooks: {
      init: (u) => {
        tooltipEl = document.createElement('div');
        tooltipEl.style.cssText = `
          position: absolute;
          pointer-events: none;
          display: none;
          background: rgba(15, 22, 35, 0.95);
          color: #e2e8f0;
          font-size: 11px;
          font-family: 'Consolas', 'Courier New', monospace;
          padding: 8px 12px;
          border-radius: 6px;
          border: 1px solid #334155;
          box-shadow: 0 4px 12px rgba(0,0,0,0.4);
          z-index: 100;
          white-space: nowrap;
          transition: transform 60ms ease-out;
        `;
        u.root.appendChild(tooltipEl);

        u.over.addEventListener('mouseenter', () => {
          tooltipEl.style.display = 'block';
        });
        u.over.addEventListener('mouseleave', () => {
          tooltipEl.style.display = 'none';
        });
      },
      setCursor: (u) => {
        const { left, top, idx } = u.cursor;
        if (idx == null || left == null || top == null) {
          if (tooltipEl) tooltipEl.style.display = 'none';
          return;
        }

        const xVal = u.data[0][idx];
        // Format timestamp
        const d = new Date(xVal * 1000);
        const timeStr = d.toLocaleTimeString('en-GB', { hour12: false });

        let html = `<div style="color:#718096;border-bottom:1px solid #243049;padding-bottom:4px;margin-bottom:4px;font-weight:600">${timeStr}</div>`;

        for (let i = 1; i < u.series.length; i++) {
          const s = u.series[i];
          if (!s.show) continue;

          const val = u.data[i]?.[idx];
          const color = typeof s.stroke === 'function' ? s.stroke(u, i) : (s.stroke || '#06b6d4');
          const label = s.label || `Series ${i}`;
          const valStr = val != null ? Number(val).toFixed(2) : 'N/A';

          html += `<div style="display:flex;justify-content:space-between;gap:16px;line-height:1.6">`;
          html += `<span><span style="color:${color}">●</span> ${label}</span>`;
          html += `<span style="font-weight:700">${valStr}</span>`;
          html += `</div>`;
        }

        tooltipEl.innerHTML = html;
        tooltipEl.style.display = 'block';

        // Position with offset, keep in bounds
        const plotRect = u.root.getBoundingClientRect();
        let posX = left + 15;
        let posY = top + 15;

        const ttW = tooltipEl.offsetWidth;
        const ttH = tooltipEl.offsetHeight;

        if (posX + ttW > plotRect.width) posX = left - ttW - 10;
        if (posY + ttH > plotRect.height) posY = top - ttH - 10;

        tooltipEl.style.transform = `translate(${posX}px, ${posY}px)`;
      },
    },
  };
}

export default UPlotChart;
