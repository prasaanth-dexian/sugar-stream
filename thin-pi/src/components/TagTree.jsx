// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

import React, { useState, useEffect, useMemo } from 'react';
import { invoke } from '@tauri-apps/api/core';
import {
  Search,
  Folder,
  FolderOpen,
  Tag,
  ChevronRight,
  ChevronDown,
  RefreshCw,
} from 'lucide-react';

/**
 * Hierarchical tag explorer sidebar.
 *
 * Displays Plant > Line > Machine > Tag tree structure with
 * checkboxes for tag selection, search filtering, and status dots.
 */
export default function TagTree({ selectedTags, onSelectionChange, isConnected }) {
  const [tree, setTree] = useState([]);
  const [search, setSearch] = useState('');
  const [expanded, setExpanded] = useState(new Set());
  const [loading, setLoading] = useState(false);

  // Fetch tag tree on mount or when connection changes
  useEffect(() => {
    if (isConnected) {
      fetchTags();
    }
  }, [isConnected]);

  const fetchTags = async () => {
    setLoading(true);
    try {
      const result = await invoke('browse_opc_tags');
      setTree(result || []);
      // Auto-expand all nodes
      const ids = new Set();
      const collectIds = (nodes) => {
        for (const node of nodes) {
          if (node.children?.length > 0) {
            ids.add(node.node_id);
            collectIds(node.children);
          }
        }
      };
      collectIds(result || []);
      setExpanded(ids);
    } catch (err) {
      console.error('Failed to browse tags:', err);
    } finally {
      setLoading(false);
    }
  };

  // Toggle node expansion
  const toggleExpand = (nodeId) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(nodeId)) next.delete(nodeId);
      else next.add(nodeId);
      return next;
    });
  };

  // Toggle tag selection
  const toggleTag = (node) => {
    const isSelected = selectedTags.some((t) => t.node_id === node.node_id);
    if (isSelected) {
      onSelectionChange(selectedTags.filter((t) => t.node_id !== node.node_id));
    } else {
      onSelectionChange([
        ...selectedTags,
        {
          tag_id: node.tag_id || 0,
          node_id: node.node_id,
          name: node.name,
          eng_unit: node.eng_unit,
        },
      ]);
    }
  };

  // Filter tree by search query
  const filteredTree = useMemo(() => {
    if (!search.trim()) return tree;

    const q = search.toLowerCase();
    const filterNodes = (nodes) => {
      return nodes
        .map((node) => {
          if (node.children?.length > 0) {
            const filteredChildren = filterNodes(node.children);
            if (filteredChildren.length > 0) {
              return { ...node, children: filteredChildren };
            }
          }
          if (node.name.toLowerCase().includes(q) || node.node_id.toLowerCase().includes(q)) {
            return node;
          }
          return null;
        })
        .filter(Boolean);
    };
    return filterNodes(tree);
  }, [tree, search]);

  return (
    <div className="flex flex-col h-full">
      {/* Search + Refresh */}
      <div className="p-2 border-b border-industrial-700">
        <div className="flex items-center gap-1.5">
          <div className="flex-1 relative">
            <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-industrial-500" />
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Filter tags..."
              className="w-full pl-7 pr-2 py-1.5 text-xs bg-industrial-800 border border-industrial-700 rounded text-industrial-200 placeholder-industrial-500 focus:border-accent"
            />
          </div>
          <button
            onClick={fetchTags}
            disabled={loading}
            className="p-1.5 rounded hover:bg-industrial-700 transition-colors"
            title="Refresh tags"
          >
            <RefreshCw className={`w-3.5 h-3.5 text-industrial-400 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
        <div className="text-[10px] text-industrial-500 mt-1.5 px-1">
          {selectedTags.length} tag{selectedTags.length !== 1 ? 's' : ''} selected
        </div>
      </div>

      {/* Tree */}
      <div className="flex-1 overflow-y-auto p-1">
        {filteredTree.length === 0 ? (
          <div className="text-xs text-industrial-500 p-3 text-center">
            {loading ? 'Loading tags...' : 'No tags available'}
          </div>
        ) : (
          filteredTree.map((node) => (
            <TreeNode
              key={node.node_id}
              node={node}
              depth={0}
              expanded={expanded}
              selectedTags={selectedTags}
              onToggleExpand={toggleExpand}
              onToggleTag={toggleTag}
            />
          ))
        )}
      </div>
    </div>
  );
}

function TreeNode({ node, depth, expanded, selectedTags, onToggleExpand, onToggleTag }) {
  const isFolder = node.children?.length > 0;
  const isExpanded = expanded.has(node.node_id);
  const isSelected = selectedTags.some((t) => t.node_id === node.node_id);
  const indent = depth * 16;

  return (
    <div>
      <div
        className={`flex items-center gap-1 py-1 px-1 rounded cursor-pointer text-xs hover:bg-industrial-800 transition-colors group ${
          isSelected ? 'bg-industrial-800 text-accent' : 'text-industrial-300'
        }`}
        style={{ paddingLeft: `${indent + 4}px` }}
        onClick={() => (isFolder ? onToggleExpand(node.node_id) : onToggleTag(node))}
      >
        {/* Expand/Collapse icon */}
        {isFolder ? (
          isExpanded ? (
            <ChevronDown className="w-3 h-3 text-industrial-500 shrink-0" />
          ) : (
            <ChevronRight className="w-3 h-3 text-industrial-500 shrink-0" />
          )
        ) : (
          <span className="w-3 shrink-0" />
        )}

        {/* Node icon */}
        {isFolder ? (
          isExpanded ? (
            <FolderOpen className="w-3.5 h-3.5 text-opc-yellow shrink-0" />
          ) : (
            <Folder className="w-3.5 h-3.5 text-opc-yellow shrink-0" />
          )
        ) : (
          <>
            {/* Checkbox for leaf tags */}
            <input
              type="checkbox"
              checked={isSelected}
              onChange={(e) => {
                e.stopPropagation();
                onToggleTag(node);
              }}
              className="w-3 h-3 rounded shrink-0 accent-accent"
            />
            <Tag className="w-3 h-3 text-opc-green shrink-0" />
          </>
        )}

        {/* Name */}
        <span className="truncate">{node.name}</span>

        {/* Engineering unit */}
        {!isFolder && node.eng_unit && (
          <span className="text-[9px] text-industrial-500 ml-auto shrink-0">
            {node.eng_unit}
          </span>
        )}

        {/* Quality indicator */}
        {!isFolder && (
          <span className="w-1.5 h-1.5 rounded-full bg-opc-green shrink-0 ml-1" title="Good quality" />
        )}
      </div>

      {/* Children */}
      {isFolder && isExpanded && (
        <div>
          {node.children.map((child) => (
            <TreeNode
              key={child.node_id}
              node={child}
              depth={depth + 1}
              expanded={expanded}
              selectedTags={selectedTags}
              onToggleExpand={onToggleExpand}
              onToggleTag={onToggleTag}
            />
          ))}
        </div>
      )}
    </div>
  );
}
