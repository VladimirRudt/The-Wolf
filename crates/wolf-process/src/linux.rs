//! Linux `/proc` + cgroup v2 implementation (`docs/collector-dotnet.md` §5).

use std::fs;
use std::io;
use std::path::Path;

use crate::parse::{
    parse_btime, parse_cgroup_v2_path, parse_memory_max, parse_smaps_rollup, parse_stat_starttime,
    parse_status,
};
use crate::sample::ProcessSample;

pub(crate) fn sample(pid: u32) -> io::Result<ProcessSample> {
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    let fields = parse_status(&status).ok_or_else(|| invalid_data("status fields"))?;
    let rollup = fs::read_to_string(format!("/proc/{pid}/smaps_rollup"))?;
    let private = parse_smaps_rollup(&rollup).ok_or_else(|| invalid_data("smaps_rollup"))?;
    Ok(ProcessSample {
        rss: fields.vm_rss,
        private,
        virtual_memory: fields.vm_size,
        limit: cgroup_limit(pid)?,
        threads: Some(fields.threads),
    })
}

pub(crate) fn thread_count(pid: u32) -> io::Result<u32> {
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    parse_status(&status)
        .map(|fields| fields.threads)
        .ok_or_else(|| invalid_data("status fields"))
}

pub(crate) fn process_start_time(pid: u32) -> io::Result<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let starttime_ticks =
        parse_stat_starttime(&stat).ok_or_else(|| invalid_data("stat starttime"))?;
    let proc_stat = fs::read_to_string("/proc/stat")?;
    let btime = parse_btime(&proc_stat).ok_or_else(|| invalid_data("btime"))?;
    // SAFETY: sysconf with _SC_CLK_TCK has no failure mode beyond -1, which we reject.
    let ticks_per_sec = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    if ticks_per_sec <= 0 {
        return Err(io::Error::other("sysconf(_SC_CLK_TCK) failed"));
    }
    Ok(btime + starttime_ticks / ticks_per_sec as u64)
}

/// The cgroup v2 `memory.max` of the process's cgroup; `Ok(None)` when the process is not
/// in a v2 hierarchy or the limit is `max` (unbounded).
fn cgroup_limit(pid: u32) -> io::Result<Option<u64>> {
    let cgroup = fs::read_to_string(format!("/proc/{pid}/cgroup"))?;
    let Some(path) = parse_cgroup_v2_path(&cgroup) else {
        return Ok(None);
    };
    let dir = Path::new("/sys/fs/cgroup").join(path.trim_start_matches('/'));
    read_limit(&dir.join("memory.max"))
}

fn read_limit(file: &Path) -> io::Result<Option<u64>> {
    match fs::read_to_string(file) {
        Ok(text) => Ok(parse_memory_max(&text)),
        // A cgroup path from /proc can be stale (or the hierarchy unmounted): no limit
        // rather than a failed sample.
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn invalid_data(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("unreadable {what}"))
}
