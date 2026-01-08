use crate::error::{AppError, AppResult};

use super::types::RuntimeSnapshot;

use std::fs;
use std::time::{Duration, Instant};

/// RuntimeSampler reads OS/process signals and produces `RuntimeSnapshot`.
///
/// Linux implementation via /proc:
/// - RSS MB:          /proc/self/status (VmRSS)
/// - Avail mem MB:    /proc/meminfo (MemAvailable)
/// - FD count:        /proc/self/fd (directory entries)
/// - FD limit:        /proc/self/limits (Max open files)
///
/// CPU %:
/// - optional proc-based approximation via CpuSampler (see below)
#[derive(Debug)]
pub struct RuntimeSampler {
    fd_limit: u64,
    cpu: Option<CpuSampler>,
}

impl RuntimeSampler {
    /// Create a sampler.
    ///
    /// If `enable_cpu` is true, CPU% will be approximated from /proc deltas.
    pub fn new(enable_cpu: bool) -> AppResult<Self> {
        #[cfg(target_os = "linux")]
        {
            let fd_limit = read_fd_limit_linux()?;
            let cpu = if enable_cpu {
                Some(CpuSampler::new()?)
            } else {
                None
            };

            Ok(Self { fd_limit, cpu })
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = enable_cpu;
            Err(AppError::Disabled(
                "runtime sampler is only implemented for linux (/proc)".into(),
            ))
        }
    }

    /// Sample runtime stats into a snapshot.
    ///
    /// You pass `tick_drift_ms` from the guard loop (sampler doesn't measure drift).
    pub fn sample(&mut self, tick_drift_ms: u64) -> AppResult<RuntimeSnapshot> {
        #[cfg(target_os = "linux")]
        {
            let rss_mb = read_rss_mb_linux()?;
            let avail_mb = read_mem_available_mb_linux()?;
            let fd_pct = read_fd_pct_linux(self.fd_limit)?;

            let cpu_pct = if let Some(cpu) = self.cpu.as_mut() {
                cpu.sample_cpu_pct().unwrap_or(0) // best-effort, never fail the whole snapshot
            } else {
                0
            };

            Ok(RuntimeSnapshot {
                rss_mb,
                avail_mb,
                fd_pct,
                cpu_pct,
                tick_drift_ms,
            })
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = tick_drift_ms;
            Err(AppError::Disabled(
                "runtime sampler is only implemented for linux (/proc)".into(),
            ))
        }
    }
}

// ==================================================
// Linux helpers (/proc)
// ==================================================

fn read_rss_mb_linux() -> AppResult<u64> {
    // Parse "VmRSS:    123456 kB" from /proc/self/status
    let s = fs::read_to_string("/proc/self/status")?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            // rest like: "   123456 kB"
            let kb = parse_first_u64(rest)?;
            return Ok(kb / 1024);
        }
    }
    Err(AppError::Internal(
        "failed to read VmRSS from /proc/self/status".into(),
    ))
}

fn read_mem_available_mb_linux() -> AppResult<u64> {
    // Parse "MemAvailable:  12345678 kB" from /proc/meminfo
    let s = fs::read_to_string("/proc/meminfo")?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MemAvailable:") {
            let kb = parse_first_u64(rest)?;
            return Ok(kb / 1024);
        }
    }
    Err(AppError::Internal(
        "failed to read MemAvailable from /proc/meminfo".into(),
    ))
}

fn read_fd_limit_linux() -> AppResult<u64> {
    // /proc/self/limits line example:
    // "Max open files            1024                 4096                 files"
    let s = fs::read_to_string("/proc/self/limits")?;
    for line in s.lines() {
        if line.starts_with("Max open files") {
            // split_whitespace yields: ["Max","open","files","1024","4096","files"]
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 4 {
                // "soft" limit at index 3; can be "unlimited"
                let soft = parts[3];
                if soft.eq_ignore_ascii_case("unlimited") {
                    // choose a very large number; if unlimited, fd_pct becomes mostly meaningless
                    return Ok(1_000_000);
                }
                let lim: u64 = soft.parse().map_err(|_| {
                    AppError::Internal("failed to parse Max open files limit".into())
                })?;
                return Ok(lim.max(1));
            }
        }
    }
    Err(AppError::Internal(
        "failed to read Max open files from /proc/self/limits".into(),
    ))
}

fn read_fd_pct_linux(fd_limit: u64) -> AppResult<u64> {
    let n = fs::read_dir("/proc/self/fd")?.count() as u64;
    if fd_limit == 0 {
        return Ok(0);
    }
    let pct = (n.saturating_mul(100)) / fd_limit;
    Ok(pct.min(100))
}

fn parse_first_u64(s: &str) -> AppResult<u64> {
    // Extract first number in a string like "   12345 kB"
    let num = s
        .split_whitespace()
        .next()
        .ok_or_else(|| AppError::Internal("failed to parse numeric value from /proc".into()))?;
    num.parse::<u64>()
        .map_err(|_| AppError::Internal("failed to parse u64 from /proc".into()))
}

// ==================================================
// Optional CPU sampler (best-effort)
// ==================================================

/// Lightweight CPU% approximation from /proc/stat and /proc/self/stat.
///
/// Notes:
/// - This is "good enough" for sustained overload detection.
/// - It uses deltas between samples, so first sample returns 0.
/// - If anything fails, caller can treat it as 0.
#[derive(Debug)]
pub struct CpuSampler {
    last: Option<CpuSample>,
}

#[derive(Debug, Clone, Copy)]
struct CpuSample {
    at: Instant,
    // total CPU time across all CPUs (jiffies)
    total_jiffies: u64,
    // process CPU time (utime+stime) (jiffies)
    proc_jiffies: u64,
}

impl CpuSampler {
    pub fn new() -> AppResult<Self> {
        Ok(Self { last: None })
    }

    pub fn sample_cpu_pct(&mut self) -> AppResult<u64> {
        #[cfg(target_os = "linux")]
        {
            let now = Instant::now();
            let total = read_total_cpu_jiffies_linux()?;
            let proc = read_process_cpu_jiffies_linux()?;

            let cur = CpuSample {
                at: now,
                total_jiffies: total,
                proc_jiffies: proc,
            };

            let Some(prev) = self.last else {
                self.last = Some(cur);
                return Ok(0);
            };

            self.last = Some(cur);

            let dt = now.duration_since(prev.at);
            if dt < Duration::from_millis(100) {
                // too soon; noisy
                return Ok(0);
            }

            let d_total = total.saturating_sub(prev.total_jiffies);
            let d_proc = proc.saturating_sub(prev.proc_jiffies);

            if d_total == 0 {
                return Ok(0);
            }

            // This yields process CPU share *across all CPUs*.
            // In container settings it’s still a useful "high usage" detector.
            let pct = (d_proc.saturating_mul(100)) / d_total;
            Ok(pct.min(100))
        }

        #[cfg(not(target_os = "linux"))]
        {
            Err(AppError::Disabled(
                "cpu sampler is only implemented for linux (/proc)".into(),
            ))
        }
    }
}

fn read_total_cpu_jiffies_linux() -> AppResult<u64> {
    // /proc/stat first line: "cpu  3357 0 4313 1362393 ..."
    let s = fs::read_to_string("/proc/stat")?;
    let line = s
        .lines()
        .next()
        .ok_or_else(|| AppError::Internal("failed to read /proc/stat cpu line".into()))?;

    let mut it = line.split_whitespace();
    let tag = it.next().unwrap_or("");
    if tag != "cpu" {
        return Err(AppError::Internal(
            "unexpected /proc/stat format (missing 'cpu')".into(),
        ));
    }

    let mut sum: u64 = 0;
    for field in it {
        if let Ok(v) = field.parse::<u64>() {
            sum = sum.saturating_add(v);
        } else {
            break;
        }
    }
    Ok(sum)
}

fn read_process_cpu_jiffies_linux() -> AppResult<u64> {
    // /proc/self/stat fields:
    // 14=utime, 15=stime (1-indexed)
    //
    // Caveat: comm (field 2) is in parentheses and may contain spaces.
    // So we find the last ')' and then split the rest.
    let s = fs::read_to_string("/proc/self/stat")?;
    let end = s
        .rfind(')')
        .ok_or_else(|| AppError::Internal("unexpected /proc/self/stat format".into()))?;

    let after = &s[end + 1..];
    let fields: Vec<&str> = after.split_whitespace().collect();

    // After ')', the first field is state (which is original field 3).
    // Therefore, original field 14 (utime) becomes index (14 - 3) = 11 in this slice (0-based).
    // original field 15 (stime) becomes index 12.
    if fields.len() <= 12 {
        return Err(AppError::Internal(
            "unexpected /proc/self/stat field count".into(),
        ));
    }

    let utime: u64 = fields[11]
        .parse()
        .map_err(|_| AppError::Internal("failed to parse utime from /proc/self/stat".into()))?;
    let stime: u64 = fields[12]
        .parse()
        .map_err(|_| AppError::Internal("failed to parse stime from /proc/self/stat".into()))?;

    Ok(utime.saturating_add(stime))
}
