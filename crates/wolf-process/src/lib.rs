//! OS process memory collector (Linux `/proc` and cgroup, Windows process APIs).
//!
//! Process-level OS memory for any PID (`docs/collector-dotnet.md` §5): resident set,
//! private bytes, virtual size, the memory limit when one applies, and the thread count.
//! One-shot sampling; the host owns the cadence (1 s default, `wolf-host`, P1-8).
//!
//! ```text
//! OS (procfs / Win32) → sample(pid) → ProcessSample → ProcessSeries::points → storage
//! ```
//!
//! Also owns the process start time (`process_start_time`), which every collector takes
//! from the OS to build the same target id (`docs/architecture.md` §3.1).

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod parse;
mod sample;
mod series;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(windows)]
mod windows;

pub use sample::{ProcessSample, process_start_time, sample, thread_count};
pub use series::ProcessSeries;
