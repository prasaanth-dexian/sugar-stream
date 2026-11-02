// Copyright (c) 2026 Dexian India Technologies Private Limited.
// All rights reserved.
//
// This file is proprietary and confidential. Unauthorised copying,
// distribution or use of this file, via any medium, is strictly
// prohibited without the prior written permission of Dexian India
// Technologies Private Limited.

/// Largest Triangle Three Buckets (LTTB) downsampling algorithm.
///
/// Reduces large time-series datasets to a target number of points while
/// preserving visual fidelity. Critical for keeping Tauri IPC payloads
/// under ~2000 points regardless of raw dataset size.
///
/// Reference: Sveinn Steinarsson, "Downsampling Time Series for Visual
/// Representation" (2013), University of Iceland.

/// Downsample a single series of (timestamp, value) pairs to `target_points`.
///
/// Always preserves the first and last data points. Returns cloned data
/// unchanged if the input length is already ≤ `target_points`.
#[allow(dead_code)]
pub fn downsample(timestamps: &[f64], values: &[f64], target_points: usize) -> (Vec<f64>, Vec<f64>) {
    let n = timestamps.len();

    if n <= target_points || target_points < 3 {
        return (timestamps.to_vec(), values.to_vec());
    }

    let mut out_ts = Vec::with_capacity(target_points);
    let mut out_val = Vec::with_capacity(target_points);

    // Always include the first point
    out_ts.push(timestamps[0]);
    out_val.push(values[0]);

    let bucket_size = (n - 2) as f64 / (target_points - 2) as f64;

    let mut prev_selected_idx: usize = 0;

    for i in 0..(target_points - 2) {
        // Current bucket boundaries
        let bucket_start = ((i as f64 * bucket_size) + 1.0).floor() as usize;
        let bucket_end = (((i + 1) as f64 * bucket_size) + 1.0).floor().min(n as f64) as usize;

        // Next bucket boundaries (for computing average)
        let next_bucket_start = (((i + 1) as f64 * bucket_size) + 1.0).floor() as usize;
        let next_bucket_end = (((i + 2) as f64 * bucket_size) + 1.0).floor().min(n as f64) as usize;

        // Compute average point of the NEXT bucket
        let mut avg_x = 0.0_f64;
        let mut avg_y = 0.0_f64;
        let next_count = (next_bucket_end - next_bucket_start).max(1);
        for j in next_bucket_start..next_bucket_end.min(n) {
            avg_x += timestamps[j];
            avg_y += values[j];
        }
        avg_x /= next_count as f64;
        avg_y /= next_count as f64;

        // Previous selected point
        let prev_x = timestamps[prev_selected_idx];
        let prev_y = values[prev_selected_idx];

        // Find the point in current bucket forming the largest triangle
        let mut max_area = -1.0_f64;
        let mut best_idx = bucket_start;

        for j in bucket_start..bucket_end.min(n) {
            // Triangle area = 0.5 * |x_prev*(y_j - y_avg) + x_j*(y_avg - y_prev) + x_avg*(y_prev - y_j)|
            let area = ((prev_x * (values[j] - avg_y))
                + (timestamps[j] * (avg_y - prev_y))
                + (avg_x * (prev_y - values[j])))
                .abs()
                * 0.5;

            if area > max_area {
                max_area = area;
                best_idx = j;
            }
        }

        out_ts.push(timestamps[best_idx]);
        out_val.push(values[best_idx]);
        prev_selected_idx = best_idx;
    }

    // Always include the last point
    out_ts.push(timestamps[n - 1]);
    out_val.push(values[n - 1]);

    (out_ts, out_val)
}

/// Downsample multiple series using aligned timestamp indices.
///
/// Uses the first non-null series to determine which indices to keep,
/// then applies those same indices to all other series. This ensures
/// all series in a TrendData response share the same timestamp array.
pub fn downsample_multi_series(
    timestamps: &[f64],
    all_values: &[Vec<Option<f64>>],
    target_points: usize,
) -> (Vec<f64>, Vec<Vec<Option<f64>>>) {
    let n = timestamps.len();

    if n <= target_points || target_points < 3 || all_values.is_empty() {
        return (timestamps.to_vec(), all_values.to_vec());
    }

    // Find the first series with data to use as the reference for LTTB
    let reference_values: Vec<f64> = all_values
        .iter()
        .find(|s| s.iter().any(|v| v.is_some()))
        .map(|s| s.iter().map(|v| v.unwrap_or(0.0)).collect())
        .unwrap_or_else(|| vec![0.0; n]);

    // Run LTTB on the reference series to get selected indices
    let selected_indices = select_indices(timestamps, &reference_values, target_points);

    // Apply selected indices to timestamps and all series
    let out_ts: Vec<f64> = selected_indices.iter().map(|&i| timestamps[i]).collect();
    let out_series: Vec<Vec<Option<f64>>> = all_values
        .iter()
        .map(|series| selected_indices.iter().map(|&i| series[i]).collect())
        .collect();

    (out_ts, out_series)
}

/// Internal: run LTTB and return the selected indices rather than values.
fn select_indices(timestamps: &[f64], values: &[f64], target_points: usize) -> Vec<usize> {
    let n = timestamps.len();
    let mut indices = Vec::with_capacity(target_points);

    // First point
    indices.push(0);

    let bucket_size = (n - 2) as f64 / (target_points - 2) as f64;
    let mut prev_selected_idx: usize = 0;

    for i in 0..(target_points - 2) {
        let bucket_start = ((i as f64 * bucket_size) + 1.0).floor() as usize;
        let bucket_end = (((i + 1) as f64 * bucket_size) + 1.0).floor().min(n as f64) as usize;

        let next_bucket_start = (((i + 1) as f64 * bucket_size) + 1.0).floor() as usize;
        let next_bucket_end = (((i + 2) as f64 * bucket_size) + 1.0).floor().min(n as f64) as usize;

        let mut avg_x = 0.0_f64;
        let mut avg_y = 0.0_f64;
        let next_count = (next_bucket_end - next_bucket_start).max(1);
        for j in next_bucket_start..next_bucket_end.min(n) {
            avg_x += timestamps[j];
            avg_y += values[j];
        }
        avg_x /= next_count as f64;
        avg_y /= next_count as f64;

        let prev_x = timestamps[prev_selected_idx];
        let prev_y = values[prev_selected_idx];

        let mut max_area = -1.0_f64;
        let mut best_idx = bucket_start;

        for j in bucket_start..bucket_end.min(n) {
            let area = ((prev_x * (values[j] - avg_y))
                + (timestamps[j] * (avg_y - prev_y))
                + (avg_x * (prev_y - values[j])))
                .abs()
                * 0.5;

            if area > max_area {
                max_area = area;
                best_idx = j;
            }
        }

        indices.push(best_idx);
        prev_selected_idx = best_idx;
    }

    // Last point
    indices.push(n - 1);
    indices
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_passthrough_small_data() {
        let ts = vec![1.0, 2.0, 3.0];
        let vals = vec![10.0, 20.0, 15.0];
        let (out_ts, out_vals) = downsample(&ts, &vals, 5);
        assert_eq!(out_ts, ts);
        assert_eq!(out_vals, vals);
    }

    #[test]
    fn test_downsample_preserves_endpoints() {
        let n = 10_000;
        let ts: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let vals: Vec<f64> = (0..n).map(|i| (i as f64 * 0.01).sin()).collect();

        let (out_ts, out_vals) = downsample(&ts, &vals, 100);

        assert_eq!(out_ts.len(), 100);
        assert_eq!(out_vals.len(), 100);
        assert_eq!(out_ts[0], ts[0]);
        assert_eq!(*out_ts.last().unwrap(), *ts.last().unwrap());
    }

    #[test]
    fn test_multi_series_alignment() {
        let n = 1000;
        let ts: Vec<f64> = (0..n).map(|i| i as f64).collect();
        let s1: Vec<Option<f64>> = (0..n).map(|i| Some((i as f64 * 0.1).sin())).collect();
        let s2: Vec<Option<f64>> = (0..n).map(|i| Some((i as f64 * 0.05).cos())).collect();

        let (out_ts, out_series) = downsample_multi_series(&ts, &[s1, s2], 50);

        assert_eq!(out_ts.len(), 50);
        assert_eq!(out_series.len(), 2);
        assert_eq!(out_series[0].len(), 50);
        assert_eq!(out_series[1].len(), 50);
    }
}
