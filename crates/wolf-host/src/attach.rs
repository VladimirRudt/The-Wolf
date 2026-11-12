//! Attaching to one target (`docs/collector-dotnet.md` §2): find the endpoint, learn the
//! process, build the shared target id (start time from the OS, §3.1 of the architecture),
//! and the telemetry session configuration.

use wolf_contract::TargetId;
use wolf_dotnet::{EcSeries, GcSeries};
use wolf_ipc::{DiagnosticsClient, EventLevel, ProcessInfo, Provider, SessionConfig, discover};
use wolf_process::{ProcessSeries, process_start_time};

use crate::{Error, Result};

/// The catalogue's telemetry providers (`docs/collector-dotnet.md` §3): runtime GC events
/// at Informational, plus `System.Runtime` EventCounters every second.
pub fn telemetry_session_config() -> SessionConfig {
    const GC_KEYWORD: u64 = 0x1;
    SessionConfig::new(vec![
        Provider::new(
            "Microsoft-Windows-DotNETRuntime",
            GC_KEYWORD,
            EventLevel::Informational,
        ),
        Provider::new("System.Runtime", 0, EventLevel::Informational)
            .with_argument("EventCounterIntervalSec", "1"),
    ])
}

/// One attached target, before its session starts: everything the router and the host
/// need later.
#[derive(Debug)]
pub struct Attached {
    pub client: DiagnosticsClient,
    pub info: ProcessInfo,
    pub id: TargetId,
    pub gc: GcSeries,
    pub ec: EcSeries,
    pub process: ProcessSeries,
}

/// Finds pid's endpoint and learns the process. `handle` drives the request/response part
/// (ProcessInfo); session streaming is blocking and stays off the runtime (D13).
pub fn attach(pid: u32, handle: &tokio::runtime::Handle) -> Result<Attached> {
    let endpoint = discover()?
        .into_iter()
        .find(|endpoint| endpoint.pid == pid)
        .ok_or(Error::NoEndpoint(pid))?;
    let client = DiagnosticsClient::new(endpoint);
    let info = handle.block_on(client.process_info())?.clone();
    let id = target_id(pid)?;
    Ok(Attached {
        client,
        info,
        gc: GcSeries::new(id.clone()),
        ec: EcSeries::new(id.clone()),
        process: ProcessSeries::new(id.clone()),
        id,
    })
}

/// `pid:<pid>.<start>@<host>` with the start time from the OS, like every collector.
pub fn target_id(pid: u32) -> Result<TargetId> {
    Ok(TargetId::new(pid, process_start_time(pid)?, host_name())?)
}

/// This machine's name for target ids. Must be deterministic: every collector derives
/// target ids independently and they must agree (`docs/architecture.md` §3.1).
pub fn host_name() -> String {
    let raw = read_host_name().unwrap_or_else(|| "localhost".to_owned());
    let trimmed = raw.trim();
    // TargetId rejects empty hosts and '@'; a hostile hostname must not break attach.
    let cleaned: String = trimmed.chars().filter(|&c| c != '@').collect();
    if cleaned.is_empty() {
        "localhost".to_owned()
    } else {
        cleaned
    }
}

#[cfg(windows)]
fn read_host_name() -> Option<String> {
    std::env::var("COMPUTERNAME").ok()
}

#[cfg(target_os = "linux")]
fn read_host_name() -> Option<String> {
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .or_else(|| std::env::var("HOSTNAME").ok())
}

#[cfg(not(any(windows, target_os = "linux")))]
fn read_host_name() -> Option<String> {
    std::env::var("HOSTNAME").ok()
}
