//! Pure parsers for Linux `/proc` and cgroup v2 files (`docs/collector-dotnet.md` §5).
//!
//! Everything here is platform-independent string parsing so the fixtures run in tests on
//! any OS; only the file reads in `linux.rs` are `cfg`-gated. Field semantics per `proc(5)`
//! and the cgroup v2 docs: `VmRSS`/`VmSize`/`Private_*` are kB, `Threads` is a count,
//! stat field 22 (`starttime`) is clock ticks since boot, `btime` is boot in Unix seconds,
//! and `memory.max` of `max` means no limit.

/// The `VmRSS`/`VmSize`/`Threads` fields of `/proc/<pid>/status`, memory in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StatusFields {
    pub(crate) vm_rss: u64,
    pub(crate) vm_size: u64,
    pub(crate) threads: u32,
}

/// Parses `/proc/<pid>/status`; `None` if a field is missing or not in kB.
pub(crate) fn parse_status(status: &str) -> Option<StatusFields> {
    let kb_field = |name: &str| -> Option<u64> {
        let line = status.lines().find(|line| line.starts_with(name))?;
        let mut parts = line[name.len()..].split_whitespace();
        let value: u64 = parts.next()?.parse().ok()?;
        (parts.next() == Some("kB")).then_some(value)
    };
    let rss = kb_field("VmRSS:")?;
    let size = kb_field("VmSize:")?;
    let threads = status
        .lines()
        .find(|line| line.starts_with("Threads:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some(StatusFields {
        vm_rss: rss * 1024,
        vm_size: size * 1024,
        threads,
    })
}

/// Private bytes from `/proc/<pid>/smaps_rollup` (`Private_Clean` + `Private_Dirty`, kB).
pub(crate) fn parse_smaps_rollup(rollup: &str) -> Option<u64> {
    let kb = |name: &str| -> Option<u64> {
        let line = rollup.lines().find(|line| line.starts_with(name))?;
        let mut parts = line[name.len()..].split_whitespace();
        let value: u64 = parts.next()?.parse().ok()?;
        (parts.next() == Some("kB")).then_some(value)
    };
    Some((kb("Private_Clean:")? + kb("Private_Dirty:")?) * 1024)
}

/// The cgroup v2 path of a process from `/proc/<pid>/cgroup` (the `0::` line; v1-only or
/// hybrid layouts without a unified entry yield `None`).
pub(crate) fn parse_cgroup_v2_path(cgroup: &str) -> Option<&str> {
    let line = cgroup.lines().find(|line| line.starts_with("0::"))?;
    Some(&line[3..])
}

/// Parses `memory.max`: `max` (or unparseable content) means no limit.
pub(crate) fn parse_memory_max(memory_max: &str) -> Option<u64> {
    match memory_max.trim() {
        "max" => None,
        bytes => bytes.parse().ok(),
    }
}

/// Boot time in Unix seconds from `/proc/stat` (`btime` line).
pub(crate) fn parse_btime(proc_stat: &str) -> Option<u64> {
    let line = proc_stat.lines().find(|line| line.starts_with("btime "))?;
    line["btime ".len()..].trim().parse().ok()
}

/// Start time in clock ticks since boot from `/proc/<pid>/stat` (field 22). The command
/// name in parentheses may contain spaces and parentheses itself, so the fields are counted
/// after its last `)`.
pub(crate) fn parse_stat_starttime(stat: &str) -> Option<u64> {
    let after_comm = &stat[stat.rfind(')')? + 1..];
    // Field 3 (state) is at index 0; field 22 is at index 19.
    after_comm.split_whitespace().nth(19)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATUS: &str = "\
Name:\tdotnet
Umask:\t0022
State:\tS (sleeping)
Pid:\t4812
VmPeak:\t 4194304 kB
VmSize:\t 2097152 kB
VmHWM:\t  512000 kB
VmRSS:\t  123456 kB
Threads:\t37
";

    #[test]
    fn status_fields_are_parsed_and_scaled_to_bytes() {
        let fields = parse_status(STATUS).expect("status");
        assert_eq!(fields.vm_rss, 123_456 * 1024);
        assert_eq!(fields.vm_size, 2_097_152 * 1024);
        assert_eq!(fields.threads, 37);
    }

    #[test]
    fn status_without_memory_fields_is_none() {
        assert_eq!(parse_status("Name:\tkthreadd\nThreads:\t1\n"), None);
        // A non-kB unit is not silently misread.
        assert_eq!(
            parse_status("VmRSS:\t123456 MB\nVmSize:\t1 kB\nThreads:\t2\n"),
            None
        );
    }

    const ROLLUP: &str = "\
00400000-7ffd3000 ---p 00000000 00:00 0                              [rollup]
Rss:\t               123456 kB
Pss:\t                54321 kB
Shared_Clean:\t        100 kB
Shared_Dirty:\t         20 kB
Private_Clean:\t       1000 kB
Private_Dirty:\t      23456 kB
";

    #[test]
    fn private_bytes_are_clean_plus_dirty() {
        assert_eq!(parse_smaps_rollup(ROLLUP), Some((1000 + 23_456) * 1024));
    }

    #[test]
    fn rollup_without_private_fields_is_none() {
        assert_eq!(parse_smaps_rollup("Rss:\t123 kB\n"), None);
    }

    #[test]
    fn cgroup_v2_path_is_the_unified_entry() {
        assert_eq!(
            parse_cgroup_v2_path("12:memory:/docker/abc\n0::/user.slice/app\n"),
            Some("/user.slice/app")
        );
        assert_eq!(parse_cgroup_v2_path("12:memory:/docker/abc\n"), None);
        // The root cgroup is a valid path.
        assert_eq!(parse_cgroup_v2_path("0::/\n"), Some("/"));
    }

    #[test]
    fn memory_max_max_means_no_limit() {
        assert_eq!(parse_memory_max("max\n"), None);
        assert_eq!(parse_memory_max("1073741824\n"), Some(1_073_741_824));
        assert_eq!(parse_memory_max("garbage\n"), None);
    }

    #[test]
    fn btime_is_parsed() {
        let stat = "cpu  1 2 3 4\nbtime 1760843645\n";
        assert_eq!(parse_btime(stat), Some(1_760_843_645));
        assert_eq!(parse_btime("cpu 1 2 3\n"), None);
    }

    #[test]
    fn starttime_survives_a_weird_command_name() {
        // comm with spaces and parentheses, as in "(my app (v2))".
        let mut fields = vec!["0"; 22];
        fields[0] = "4812";
        fields[1] = "(my app (v2))";
        fields[2] = "S";
        fields[21] = "123456789"; // field 22
        let stat = fields.join(" ");
        assert_eq!(parse_stat_starttime(&stat), Some(123_456_789));
        assert_eq!(parse_stat_starttime("4812 (x) S"), None);
    }
}
