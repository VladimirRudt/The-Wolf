use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Identity of an observed process: `pid:<pid>.<start>@<host>`, e.g. `pid:4812.1760843645@srv-a`.
///
/// `<start>` is the process start time in Unix seconds, so a reused PID (e.g. after an app
/// restart) is a different target. Every collector must take it from the OS, so they all
/// produce the same id for the same process.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TargetId {
    pid: u32,
    start_time_s: u64,
    host: String,
}

impl TargetId {
    /// Creates a target id. `host` must be non-empty and must not contain `@`.
    pub fn new(
        pid: u32,
        start_time_s: u64,
        host: impl Into<String>,
    ) -> Result<Self, ParseTargetIdError> {
        let host = host.into();
        if host.is_empty() || host.contains('@') {
            return Err(ParseTargetIdError::InvalidHost(host));
        }
        Ok(Self {
            pid,
            start_time_s,
            host,
        })
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Process start time, Unix seconds (UTC).
    pub fn start_time_s(&self) -> u64 {
        self.start_time_s
    }

    pub fn host(&self) -> &str {
        &self.host
    }
}

impl fmt::Display for TargetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pid:{}.{}@{}", self.pid, self.start_time_s, self.host)
    }
}

impl FromStr for TargetId {
    type Err = ParseTargetIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let format_error = || ParseTargetIdError::Format(s.to_owned());
        let rest = s.strip_prefix("pid:").ok_or_else(format_error)?;
        let (process, host) = rest.split_once('@').ok_or_else(format_error)?;
        let (pid, start) = process.split_once('.').ok_or_else(format_error)?;
        let pid = pid
            .parse()
            .map_err(|_| ParseTargetIdError::InvalidPid(pid.to_owned()))?;
        let start = start
            .parse()
            .map_err(|_| ParseTargetIdError::InvalidStartTime(start.to_owned()))?;
        Self::new(pid, start, host)
    }
}

impl TryFrom<String> for TargetId {
    type Error = ParseTargetIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<TargetId> for String {
    fn from(value: TargetId) -> Self {
        value.to_string()
    }
}

/// Why a string is not a valid [`TargetId`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseTargetIdError {
    /// Not of the form `pid:<pid>.<start>@<host>`.
    Format(String),
    InvalidPid(String),
    InvalidStartTime(String),
    InvalidHost(String),
}

impl fmt::Display for ParseTargetIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Format(s) => write!(
                f,
                "target id {s:?} is not of the form pid:<pid>.<start>@<host>"
            ),
            Self::InvalidPid(s) => write!(f, "invalid pid {s:?} in target id"),
            Self::InvalidStartTime(s) => write!(f, "invalid start time {s:?} in target id"),
            Self::InvalidHost(s) => write!(f, "invalid host {s:?} in target id"),
        }
    }
}

impl std::error::Error for ParseTargetIdError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_and_displays() {
        let id: TargetId = "pid:4812.1760843645@srv-a".parse().expect("valid");
        assert_eq!(id.pid(), 4812);
        assert_eq!(id.start_time_s(), 1_760_843_645);
        assert_eq!(id.host(), "srv-a");
        assert_eq!(id.to_string(), "pid:4812.1760843645@srv-a");
    }

    #[test]
    fn serializes_as_string() {
        let id = TargetId::new(4812, 1_760_843_645, "srv-a").expect("valid");
        let json = serde_json::to_string(&id).expect("serialize");
        assert_eq!(json, r#""pid:4812.1760843645@srv-a""#);
        let back: TargetId = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, id);
    }

    #[test]
    fn reused_pid_is_a_different_target() {
        let before = TargetId::new(4812, 1_760_843_645, "srv-a").expect("valid");
        let after = TargetId::new(4812, 1_760_850_000, "srv-a").expect("valid");
        assert_ne!(before, after);
    }

    #[test]
    fn rejects_malformed() {
        for bad in [
            "",
            "4812.1@srv-a",
            "pid:4812@srv-a",
            "pid:4812.1",
            "pid:x.1@srv-a",
            "pid:-1.1@srv-a",
            "pid:1.x@srv-a",
            "pid:4812.1@",
            "pid:1.1@a@b",
        ] {
            assert!(
                bad.parse::<TargetId>().is_err(),
                "{bad:?} should be rejected"
            );
            assert!(
                serde_json::from_value::<TargetId>(bad.into()).is_err(),
                "{bad:?} should fail to deserialize"
            );
        }
    }
}
