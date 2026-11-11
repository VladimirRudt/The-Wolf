//! Windows implementation (`docs/collector-dotnet.md` §5): `GetProcessMemoryInfo` for
//! memory, Toolhelp32 for the thread count, `GetProcessTimes` for the start time. Plain
//! `windows-sys` FFI behind a handle guard; no async (D13).

use std::io;
use std::mem::{size_of, zeroed};

use windows_sys::Win32::Foundation::{CloseHandle, FILETIME, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
};
use windows_sys::Win32::System::ProcessStatus::{
    GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS, PROCESS_MEMORY_COUNTERS_EX,
};
use windows_sys::Win32::System::Threading::{
    GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
};

use crate::sample::ProcessSample;

/// Closes the handle on drop, so no `?` between open and use can leak it.
struct ProcessHandle(HANDLE);

impl ProcessHandle {
    fn open(pid: u32) -> io::Result<Self> {
        // SAFETY: no memory is touched; a null result is the documented failure.
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            Err(io::Error::last_os_error())
        } else {
            Ok(Self(handle))
        }
    }
}

impl Drop for ProcessHandle {
    fn drop(&mut self) {
        // SAFETY: the handle is open and owned by us; CloseHandle failure is not
        // actionable here.
        unsafe {
            CloseHandle(self.0);
        }
    }
}

pub(crate) fn sample(pid: u32) -> io::Result<ProcessSample> {
    let process = ProcessHandle::open(pid)?;
    // SAFETY: `counters` is a valid out-pointer of the size passed in; the handle is open.
    let counters = unsafe {
        let mut counters: PROCESS_MEMORY_COUNTERS_EX = zeroed();
        counters.cb = size_of::<PROCESS_MEMORY_COUNTERS_EX>() as u32;
        let ok = GetProcessMemoryInfo(
            process.0,
            &mut counters as *mut PROCESS_MEMORY_COUNTERS_EX as *mut PROCESS_MEMORY_COUNTERS,
            counters.cb,
        );
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        counters
    };
    Ok(ProcessSample {
        rss: counters.WorkingSetSize as u64,
        private: counters.PrivateUsage as u64,
        virtual_memory: counters.PagefileUsage as u64,
        // No job-object limit is reported yet (§5, "if any").
        limit: None,
        // A thread count needs the expensive Toolhelp32 walk (see `thread_count`); the
        // memory sample stays in the microsecond range without it.
        threads: None,
    })
}

pub(crate) fn thread_count(pid: u32) -> io::Result<u32> {
    // SAFETY: snapshot flags only; a failure is the INVALID_HANDLE_VALUE sentinel.
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    struct Snapshot(HANDLE);
    impl Drop for Snapshot {
        fn drop(&mut self) {
            // SAFETY: the snapshot handle is open and owned by us.
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
    let snapshot = Snapshot(snapshot);

    // SAFETY: `entry` is a valid out-pointer with dwSize set as Thread32First requires.
    let mut entry: THREADENTRY32 = unsafe { zeroed() };
    entry.dwSize = size_of::<THREADENTRY32>() as u32;
    let mut count = 0;
    // SAFETY: the snapshot is valid for the whole loop; Thread32Next returning 0 ends it
    // (ERROR_NO_MORE_FILES is the normal end, anything else just stops the walk).
    let mut ok = unsafe { Thread32First(snapshot.0, &mut entry) };
    while ok != 0 {
        if entry.th32OwnerProcessID == pid {
            count += 1;
        }
        // SAFETY: as above.
        ok = unsafe { Thread32Next(snapshot.0, &mut entry) };
    }
    Ok(count)
}

pub(crate) fn process_start_time(pid: u32) -> io::Result<u64> {
    let process = ProcessHandle::open(pid)?;
    // SAFETY: all four out-pointers are valid; the handle is open.
    let creation = unsafe {
        let mut creation: FILETIME = zeroed();
        let mut unused: FILETIME = zeroed();
        let ok = GetProcessTimes(
            process.0,
            &mut creation,
            &mut unused,
            &mut unused,
            &mut unused,
        );
        if ok == 0 {
            return Err(io::Error::last_os_error());
        }
        creation
    };
    // FILETIME counts 100 ns intervals since 1601-01-01 UTC.
    let intervals = (u64::from(creation.dwHighDateTime) << 32) | u64::from(creation.dwLowDateTime);
    const EPOCH_DIFF_SECS: u64 = 11_644_473_600; // 1601 → 1970
    Ok(intervals / 10_000_000 - EPOCH_DIFF_SECS)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Live smoke test against this test process: the API wiring works, values are sane.
    /// Exact cross-check against Task Manager is the manual step of P1-6.
    #[test]
    fn samples_its_own_process() {
        let pid = std::process::id();
        let sample = sample(pid).expect("sample own process");
        assert!(sample.rss > 0);
        assert!(sample.private > 0);
        assert!(sample.virtual_memory > 0);
        assert_eq!(sample.limit, None);
        // The memory sample stays cheap by not walking system threads.
        assert_eq!(sample.threads, None);
        assert!(thread_count(pid).expect("thread count") >= 1);

        let start = process_start_time(pid).expect("start time");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs();
        assert!(start > 1_577_836_800 && start <= now, "start {start}");
    }
}
