//! One reading of a process's OS-level memory (`docs/collector-dotnet.md` §5), plus the
//! process start time every collector needs to build the shared target id
//! (`docs/architecture.md` §3.1).

use std::io;

/// What the OS reports for a process at one instant. Byte fields are exact; `limit` is
/// `None` when the process runs without a memory limit (no cgroup v2 limit, or Windows
/// without a job-object limit).
///
/// `threads` is `Some` when the platform reports it cheaply together with the memory
/// fields (Linux), `None` where it needs a separate expensive walk (Windows Toolhelp32
/// enumerates every thread in the system — ~35 ms with ~5k threads — so the host samples
/// it via [`thread_count`] on a slow cadence instead). `ProcessSeries::points` emits the
/// thread point only when present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessSample {
    /// Resident set / working set.
    pub rss: u64,
    /// Private bytes (not shareable with other processes).
    pub private: u64,
    /// Virtual size (Linux) / committed pagefile usage (Windows).
    pub virtual_memory: u64,
    /// Memory limit the process is under, when the OS reports one.
    pub limit: Option<u64>,
    /// Live thread count, when sampled this tick.
    pub threads: Option<u32>,
}

/// Samples `pid` once: the memory fields plus everything the OS reports together with them.
/// Measured at ~1 µs on Windows; the Linux reads are small procfs files. `NotFound`/
/// `PermissionDenied` means the process is gone or unreadable; the host stops sampling
/// that target.
pub fn sample(pid: u32) -> io::Result<ProcessSample> {
    imp::sample(pid)
}

/// The live thread count of `pid`. Expensive on Windows (a system-wide thread walk), so
/// the host calls this at a slow cadence (minutes) and merges it into the sample; on Linux
/// this is a cheap re-read and [`sample`] already fills `threads`.
pub fn thread_count(pid: u32) -> io::Result<u32> {
    imp::thread_count(pid)
}

/// The process start time in Unix seconds (target id component, `architecture.md` §3.1).
pub fn process_start_time(pid: u32) -> io::Result<u64> {
    imp::process_start_time(pid)
}

#[cfg(target_os = "linux")]
use crate::linux as imp;
#[cfg(windows)]
use crate::windows as imp;
