// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

import React, { useState } from 'react';
import { invoke } from '@tauri-apps/api/core';
import { Download, Loader2 } from 'lucide-react';

/**
 * CSV export button for the active trend viewport.
 *
 * Fetches raw (non-downsampled) data for the current time window
 * and triggers a browser download of the CSV file.
 */
export default function ExportButton({ selectedTagIds, startTime, endTime, disabled }) {
  const [exporting, setExporting] = useState(false);

  const handleExport = async () => {
    if (disabled || selectedTagIds.length === 0) return;

    setExporting(true);
    try {
      const csv = await invoke('export_csv', {
        tagIds: selectedTagIds,
        startTime: Math.floor(startTime),
        endTime: Math.floor(endTime),
      });

      // Create Blob and trigger download
      const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');

      const timestamp = new Date()
        .toISOString()
        .replace(/[:.]/g, '-')
        .slice(0, 19);
      a.href = url;
      a.download = `thinpi_export_${timestamp}.csv`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err) {
      console.error('CSV export failed:', err);
    } finally {
      setExporting(false);
    }
  };

  return (
    <button
      onClick={handleExport}
      disabled={disabled || exporting}
      className="flex items-center gap-1 px-2.5 py-1 text-[11px] font-medium bg-industrial-800 border border-industrial-700 rounded text-industrial-300 hover:bg-industrial-700 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
      title="Export CSV"
    >
      {exporting ? (
        <Loader2 className="w-3 h-3 animate-spin" />
      ) : (
        <Download className="w-3 h-3" />
      )}
      CSV
    </button>
  );
}
