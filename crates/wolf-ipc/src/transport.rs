use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

/// Diagnostic endpoint of one .NET process: a named pipe on Windows, a Unix domain socket elsewhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    pub pid: u32,
    pub path: PathBuf,
}

/// Lists the diagnostic endpoints of running .NET processes, sorted by pid.
///
/// Scans `\\.\pipe\` on Windows and `$TMPDIR` (or `/tmp`) elsewhere, as the runtime places
/// them there. Not covered yet: processes started with a different `TMPDIR`, other mount
/// namespaces (containers) and macOS app sandboxes.
pub fn discover() -> io::Result<Vec<Endpoint>> {
    let root = if cfg!(windows) {
        PathBuf::from(r"\\.\pipe\")
    } else {
        std::env::temp_dir()
    };

    let mut newest: BTreeMap<u32, (PathBuf, Option<SystemTime>)> = BTreeMap::new();
    for entry in std::fs::read_dir(root)? {
        let Ok(entry) = entry else { continue };
        let Some(pid) = entry.file_name().to_str().and_then(parse_endpoint_name) else {
            continue;
        };
        if !is_alive(pid) {
            continue;
        }
        // A reused pid can leave several sockets; the newest one wins, as in
        // Microsoft.Diagnostics.NETCore.Client.
        let modified = entry.metadata().and_then(|m| m.modified()).ok();
        if newest.get(&pid).is_none_or(|(_, seen)| modified > *seen) {
            newest.insert(pid, (entry.path(), modified));
        }
    }
    Ok(newest
        .into_iter()
        .map(|(pid, (path, _))| Endpoint { pid, path })
        .collect())
}

fn parse_endpoint_name(name: &str) -> Option<u32> {
    if cfg!(windows) {
        parse_pipe_name(name)
    } else {
        parse_socket_name(name)
    }
}

/// `dotnet-diagnostic-{pid}`
fn parse_pipe_name(name: &str) -> Option<u32> {
    let pid = name.strip_prefix("dotnet-diagnostic-")?;
    all_digits(pid).then(|| pid.parse().ok())?
}

/// `dotnet-diagnostic-{pid}-{disambiguation key}-socket`
fn parse_socket_name(name: &str) -> Option<u32> {
    let rest = name
        .strip_prefix("dotnet-diagnostic-")?
        .strip_suffix("-socket")?;
    let (pid, key) = rest.split_once('-')?;
    (all_digits(pid) && all_digits(key)).then(|| pid.parse().ok())?
}

fn all_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// Drops stale sockets left by dead processes where that is cheap to check. Elsewhere a stale
/// endpoint fails to connect.
fn is_alive(pid: u32) -> bool {
    !cfg!(target_os = "linux") || Path::new("/proc").join(pid.to_string()).exists()
}

#[cfg(unix)]
pub(crate) async fn connect(endpoint: &Endpoint) -> io::Result<tokio::net::UnixStream> {
    tokio::net::UnixStream::connect(&endpoint.path).await
}

#[cfg(windows)]
pub(crate) async fn connect(
    endpoint: &Endpoint,
) -> io::Result<tokio::net::windows::named_pipe::NamedPipeClient> {
    use tokio::net::windows::named_pipe::ClientOptions;
    // All pipe instances are in use: retry shortly. Callers bound the total time.
    const ERROR_PIPE_BUSY: i32 = 231;
    loop {
        match ClientOptions::new().open(&endpoint.path) {
            Err(e) if e.raw_os_error() == Some(ERROR_PIPE_BUSY) => {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            result => return result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pipe_names() {
        assert_eq!(parse_pipe_name("dotnet-diagnostic-4812"), Some(4812));
        for other in [
            "dotnet-diagnostic-",
            "dotnet-diagnostic-dsrouter-4812",
            "dotnet-diagnostic-+4812",
            "dotnet-diagnostic-4812-1-socket",
            "mojo.123",
        ] {
            assert_eq!(parse_pipe_name(other), None, "{other}");
        }
    }

    #[test]
    fn parses_socket_names() {
        assert_eq!(
            parse_socket_name("dotnet-diagnostic-4812-123456-socket"),
            Some(4812)
        );
        for other in [
            "dotnet-diagnostic-4812-socket",
            "dotnet-diagnostic-4812--socket",
            "dotnet-diagnostic-dsrouter-4812-1-socket",
            "dotnet-diagnostic-4812-1",
            "clr-debug-pipe-4812-1-in",
        ] {
            assert_eq!(parse_socket_name(other), None, "{other}");
        }
    }
}
