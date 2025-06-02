//! stats.rs: show nemscraper’s CPU/memory usage plus the other stats.
//!
//! - Queries `ps -C nemscraper -o %cpu= -o %mem=` to get per‐process CPU and memory usage for any "nemscraper" instances, summing them.
//! - Counts files in `assets/history` matching `*Proc*` and `*Down*`, excluding `.tmp`.
//! - Computes and prints the processed‐vs‐downloaded percentage.
//! - Summarizes on‐disk usage (blocks×512 bytes) for each top‐level subdirectory of `assets/`, excluding `.tmp`.
//! - Finally, lists any “nemscraper” PIDs and their full command lines via `pgrep -fl nemscraper`.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Convert a byte count into a human-readable string (B, K, M, G, T, P).
fn human_readable(mut bytes: u64) -> String {
    let units = ["B", "K", "M", "G", "T", "P"];
    let mut i = 0;
    let mut value = bytes as f64;
    while value >= 1024.0 && i < units.len() - 1 {
        value /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{}{}", bytes, units[i])
    } else {
        // If the value is effectively an integer at this unit, omit decimals.
        if (value - value.trunc()).abs() < 0.05 {
            format!("{:.0}{}", value, units[i])
        } else {
            format!("{:.1}{}", value, units[i])
        }
    }
}

/// Use `ps -C nemscraper -o %cpu= -o %mem=` to gather CPU and memory usage
/// across all "nemscraper" processes. Sums each column over all lines.
///
/// Returns:
/// - `Ok(Some((cpu_sum, mem_sum)))` if at least one line of output (i.e., at least one process),
/// - `Ok(None)` if no matching lines (no nemscraper running),
/// - `Err(_)` on any failure invoking or parsing `ps`.
fn get_nemscraper_usage() -> io::Result<Option<(f64, f64)>> {
    // The `=` after %cpu and %mem suppresses headers.
    let output = Command::new("ps")
        .args(&["-C", "nemscraper", "-o", "%cpu=", "-o", "%mem="])
        .output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut cpu_sum = 0.0;
    let mut mem_sum = 0.0;
    let mut found = false;

    for line in stdout.lines().map(str::trim).filter(|l| !l.is_empty()) {
        // Each line should contain two numbers, e.g. " 0.3  1.2"
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            if let (Ok(cpu), Ok(mem)) = (parts[0].parse::<f64>(), parts[1].parse::<f64>()) {
                cpu_sum += cpu;
                mem_sum += mem;
                found = true;
            }
        }
    }

    if found {
        Ok(Some((cpu_sum, mem_sum)))
    } else {
        Ok(None)
    }
}

/// Recursively traverse `path`, accumulating on‐disk usage (blocks×512 bytes) for each
/// top‐level subdirectory under `assets/`. Skips any file ending in ".tmp".
///
/// - `top_name`: `None` at `assets/`; becomes `Some(dir_name)` when we descend one level.
/// - `map`: accumulates `top_name -> total_bytes_on_disk`.
fn traverse(path: &Path, top_name: Option<&OsStr>, map: &mut HashMap<String, u64>) {
    let read_dir = match fs::read_dir(path) {
        Ok(rd) => rd,
        Err(_) => return, // cannot read this directory
    };

    for entry in read_dir.filter_map(Result::ok) {
        let p = entry.path();
        let file_name_os = entry.file_name();
        let file_name = file_name_os.to_string_lossy();

        if p.is_dir() {
            // If no top_name yet, this directory’s name becomes the group key.
            let new_top = if top_name.is_none() {
                Some(file_name_os.as_os_str())
            } else {
                top_name
            };
            traverse(&p, new_top, map);
        } else if p.is_file() {
            // Skip any ".tmp" file
            if file_name.ends_with(".tmp") {
                continue;
            }
            // Only count once we've descended into a top-level dir
            if let Some(top_os) = top_name {
                if let Ok(meta) = fs::metadata(&p) {
                    // meta.blocks() is number of 512-byte blocks allocated
                    let bytes = meta.blocks().saturating_mul(512);
                    let key = top_os.to_string_lossy().into_owned();
                    *map.entry(key).or_default() += bytes;
                }
            }
        }
    }
}

fn main() {
    // ─── 1) Get nemscraper CPU and memory usage
    println!("=== nemscraper Usage ===");
    match get_nemscraper_usage() {
        Ok(Some((cpu_pct, mem_pct))) => {
            println!("CPU:    {:5.1}%", cpu_pct);
            println!("Memory: {:5.1}%", mem_pct);
        }
        Ok(None) => {
            println!("nemscraper is not running");
        }
        Err(_) => {
            println!("nemscraper usage: N/A");
        }
    }

    // ─── 2) Count "*Proc*" and "*Down*" in "assets/history", excluding ".tmp"
    let history_dir = PathBuf::from("assets/history");
    let mut proc_count = 0u64;
    let mut down_count = 0u64;

    if let Ok(entries) = fs::read_dir(&history_dir) {
        for entry in entries.filter_map(Result::ok) {
            let file_name = entry.file_name().to_string_lossy().into_owned();
            if file_name.ends_with(".tmp") {
                continue;
            }
            if file_name.contains("Proc") {
                proc_count += 1;
            }
            if file_name.contains("Down") {
                down_count += 1;
            }
        }
    }

    // ─── 3) Compute processed percentage
    let pct = if down_count > 0 {
        (proc_count * 100) / down_count
    } else {
        0
    };

    println!();
    println!("=== Processed Percentage ===");
    println!("Processed files:  {}", proc_count);
    println!("Downloaded files: {}", down_count);
    println!("Complete:         {}%", pct);

    // ─── 4) Disk usage for each top-level asset directory (excluding ".tmp")
    let assets_dir = PathBuf::from("assets");
    let mut usage_map: HashMap<String, u64> = HashMap::new();
    if assets_dir.exists() {
        traverse(&assets_dir, None, &mut usage_map);
    }

    println!();
    println!("=== Disk Usage (excluding *.tmp) ===");
    let mut keys: Vec<&String> = usage_map.keys().collect();
    keys.sort();
    for key in keys {
        let bytes = usage_map[key];
        println!("{:>6}\t./assets/{}", human_readable(bytes), key);
    }
}
