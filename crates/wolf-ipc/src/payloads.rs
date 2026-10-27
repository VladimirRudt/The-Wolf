//! Payloads of IPC requests and responses: the data types and how they are encoded/parsed.
//! The envelope is in [`crate::messaging`]; how they travel is up to [`crate::DiagnosticsClient`].

use std::fmt;

use crate::messaging::{PayloadReader, PayloadWriter};
use crate::{CommandCode, IpcError};

// ---------------------------------------------------------------------------------------------
// ProcessInfo
// ---------------------------------------------------------------------------------------------

/// Answer to the ProcessInfo command family.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProcessInfo {
    /// Process id in the process's own PID namespace.
    pub process_id: u64,
    pub runtime_cookie: RuntimeCookie,
    /// Windows: `GetCommandLineW`. Elsewhere: full path of `argv[0]`, then the arguments.
    pub command_line: String,
    /// `Windows`, `Linux`, `macOS` or `Unknown`.
    pub os: String,
    /// `x86`, `x64`, `arm32`, `arm64` or `Unknown`.
    pub arch: String,
    /// Entry assembly name. ProcessInfo2 and later.
    pub entrypoint_assembly: Option<String>,
    /// CLR product version, e.g. `10.0.0+<commit>`. ProcessInfo2 and later.
    pub clr_product_version: Option<String>,
    /// Runtime identifier, e.g. `win-x64`. ProcessInfo3 with payload version 1 and later.
    pub runtime_identifier: Option<String>,
}

/// Which ProcessInfo command produced a payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessInfoVersion {
    V1,
    V2,
    V3,
}

impl ProcessInfoVersion {
    /// Newest first: the order to try them in.
    pub(crate) const ALL: [Self; 3] = [Self::V3, Self::V2, Self::V1];

    pub(crate) fn command(self) -> CommandCode {
        match self {
            Self::V1 => CommandCode::PROCESS_INFO,
            Self::V2 => CommandCode::PROCESS_INFO2,
            Self::V3 => CommandCode::PROCESS_INFO3,
        }
    }
}

impl ProcessInfo {
    pub(crate) fn parse(version: ProcessInfoVersion, payload: &[u8]) -> Result<Self, IpcError> {
        let mut reader = PayloadReader::new(payload);
        let payload_version = match version {
            ProcessInfoVersion::V3 => Some(reader.u32()?),
            ProcessInfoVersion::V1 | ProcessInfoVersion::V2 => None,
        };
        let process_id = reader.u64()?;
        let runtime_cookie = RuntimeCookie(reader.array()?);
        let command_line = reader.string()?;
        let os = reader.string()?;
        let arch = reader.string()?;
        let (entrypoint_assembly, clr_product_version) = match version {
            ProcessInfoVersion::V1 => (None, None),
            ProcessInfoVersion::V2 | ProcessInfoVersion::V3 => {
                (Some(reader.string()?), Some(reader.string()?))
            }
        };
        let runtime_identifier = match payload_version {
            Some(v) if v >= 1 => Some(reader.string()?),
            _ => None,
        };
        // Newer payload versions append fields; anything left over is ignored.
        Ok(Self {
            process_id,
            runtime_cookie,
            command_line,
            os,
            arch,
            entrypoint_assembly,
            clr_product_version,
            runtime_identifier,
        })
    }
}

/// Runtime instance GUID, unique across PID namespaces. Bytes are in .NET `Guid` layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RuntimeCookie(pub [u8; 16]);

impl fmt::Display for RuntimeCookie {
    /// Same text as .NET `Guid.ToString()`: the first three groups are little-endian.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let b = &self.0;
        write!(
            f,
            "{:08x}-{:04x}-{:04x}-{:02x}{:02x}-",
            u32::from_le_bytes([b[0], b[1], b[2], b[3]]),
            u16::from_le_bytes([b[4], b[5]]),
            u16::from_le_bytes([b[6], b[7]]),
            b[8],
            b[9]
        )?;
        b[10..].iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

// ---------------------------------------------------------------------------------------------
// EventPipe sessions (CollectTracing / StopTracing)
// ---------------------------------------------------------------------------------------------

/// `format` field of CollectTracing: 1 = NetTrace (0 would be the legacy NetPerf format).
const FORMAT_NETTRACE: u32 = 1;

/// Id of a running EventPipe session, assigned by the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SessionId(pub u64);

impl SessionId {
    pub(crate) fn parse(payload: &[u8]) -> Result<Self, IpcError> {
        Ok(Self(PayloadReader::new(payload).u64()?))
    }

    pub(crate) fn encode(self) -> Vec<u8> {
        PayloadWriter::default().u64(self.0).build()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// EventPipe/ETW event level. Same values as .NET `EventLevel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventLevel {
    LogAlways = 0,
    Critical = 1,
    Error = 2,
    Warning = 3,
    Informational = 4,
    Verbose = 5,
}

/// One event provider to enable in a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Provider {
    /// e.g. `Microsoft-Windows-DotNETRuntime`.
    pub name: String,
    pub keywords: u64,
    pub level: EventLevel,
    /// Provider arguments, e.g. `("EventCounterIntervalSec", "1")`.
    pub arguments: Vec<(String, String)>,
}

impl Provider {
    pub fn new(name: impl Into<String>, keywords: u64, level: EventLevel) -> Self {
        Self {
            name: name.into(),
            keywords,
            level,
            arguments: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_argument(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.arguments.push((key.into(), value.into()));
        self
    }

    /// `key=value;key=value`; a key or value containing `;` or `=` is quoted, as in
    /// `Microsoft.Diagnostics.NETCore.Client`.
    fn argument_string(&self) -> String {
        let escape = |s: &str| {
            if s.contains([';', '=']) {
                format!("\"{s}\"")
            } else {
                s.to_owned()
            }
        };
        self.arguments
            .iter()
            .map(|(k, v)| format!("{}={}", escape(k), escape(v)))
            .collect::<Vec<_>>()
            .join(";")
    }
}

/// Settings of an EventPipe session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionConfig {
    pub providers: Vec<Provider>,
    /// Runtime-side buffer for events not yet streamed; events are dropped when it is full.
    pub circular_buffer_mb: u32,
    /// Emit rundown (method/module metadata) when the session stops. Needed only to resolve
    /// stacks; telemetry sessions leave it off.
    pub request_rundown: bool,
    /// Record a stack for every event. Needed only for allocation sampling.
    pub request_stackwalk: bool,
}

impl SessionConfig {
    /// A telemetry-style session: no rundown, no stacks, 64 MB buffer.
    pub fn new(providers: Vec<Provider>) -> Self {
        Self {
            providers,
            circular_buffer_mb: 64,
            request_rundown: false,
            request_stackwalk: false,
        }
    }

    /// CollectTracing2 payload, plus the `requestStackwalk` flag for CollectTracing3.
    pub(crate) fn encode(&self, command: CommandCode) -> Vec<u8> {
        let mut writer = PayloadWriter::default()
            .u32(self.circular_buffer_mb)
            .u32(FORMAT_NETTRACE)
            .bool(self.request_rundown);
        if command == CommandCode::COLLECT_TRACING3 {
            writer = writer.bool(self.request_stackwalk);
        }
        writer = writer.count(self.providers.len());
        for provider in &self.providers {
            writer = writer
                .u64(provider.keywords)
                .u32(provider.level as u32)
                .string(&provider.name)
                .string(&provider.argument_string());
        }
        writer.build()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Message;

    // --- ProcessInfo -------------------------------------------------------------------------

    const COOKIE: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

    fn common(writer: PayloadWriter) -> PayloadWriter {
        writer
            .u64(4812)
            .bytes(&COOKIE)
            .string(r#""C:\Program Files\dotnet\dotnet.exe" App.dll"#)
            .string("Windows")
            .string("x64")
    }

    pub(crate) fn v2_payload() -> Vec<u8> {
        common(PayloadWriter::default())
            .string("App")
            .string("8.0.1+abc")
            .build()
    }

    /// ProcessInfo3 payload, version 1.
    pub(crate) fn v3_payload() -> Vec<u8> {
        common(PayloadWriter::default().u32(1))
            .string("App")
            .string("10.0.0")
            .string("win-x64")
            .build()
    }

    #[test]
    fn parses_process_info_v1() {
        let payload = common(PayloadWriter::default()).build();
        let info = ProcessInfo::parse(ProcessInfoVersion::V1, &payload).expect("parse");
        assert_eq!(info.process_id, 4812);
        assert_eq!(
            info.command_line,
            r#""C:\Program Files\dotnet\dotnet.exe" App.dll"#
        );
        assert_eq!(info.os, "Windows");
        assert_eq!(info.arch, "x64");
        assert_eq!(info.entrypoint_assembly, None);
        assert_eq!(info.clr_product_version, None);
    }

    #[test]
    fn parses_process_info_v2() {
        let info = ProcessInfo::parse(ProcessInfoVersion::V2, &v2_payload()).expect("parse");
        assert_eq!(info.entrypoint_assembly.as_deref(), Some("App"));
        assert_eq!(info.clr_product_version.as_deref(), Some("8.0.1+abc"));
        assert_eq!(info.runtime_identifier, None);
    }

    #[test]
    fn parses_process_info_v3_payload_versions() {
        let v3 = |payload_version: u32| {
            common(PayloadWriter::default().u32(payload_version))
                .string("App")
                .string("10.0.0")
                .string("win-x64")
                // A future version appends fields we don't know yet.
                .string("future field")
                .build()
        };

        let version0 = ProcessInfo::parse(ProcessInfoVersion::V3, &v3(0)).expect("v0");
        assert_eq!(version0.runtime_identifier, None);

        for payload_version in [1, 2] {
            let info =
                ProcessInfo::parse(ProcessInfoVersion::V3, &v3(payload_version)).expect("v1+");
            assert_eq!(info.runtime_identifier.as_deref(), Some("win-x64"));
            assert_eq!(info.clr_product_version.as_deref(), Some("10.0.0"));
        }
    }

    #[test]
    fn truncated_process_info_is_malformed() {
        let payload = v2_payload();
        for len in [0, 7, 8 + 16, payload.len() - 1] {
            assert!(
                matches!(
                    ProcessInfo::parse(ProcessInfoVersion::V2, &payload[..len]),
                    Err(IpcError::Malformed(_))
                ),
                "len {len}"
            );
        }
    }

    #[test]
    fn cookie_displays_like_dotnet_guid() {
        // new Guid(new byte[] { 0, 1, ..., 15 }).ToString()
        assert_eq!(
            RuntimeCookie(COOKIE).to_string(),
            "03020100-0504-0706-0809-0a0b0c0d0e0f"
        );
    }

    /// Golden file: a real ProcessInfo3 response from the SteadyAllocator sample (.NET 10,
    /// Windows x64), recorded with `examples/record_response.rs`.
    #[tokio::test]
    async fn parses_recorded_net10_windows_response() {
        let bytes = std::fs::read(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../testdata/ipc/processinfo3-net10-windows-x64.bin"
        ))
        .expect("golden file");
        let message = Message::read(&mut &bytes[..]).await.expect("message");
        assert_eq!(message.command, CommandCode::SERVER_OK);
        let payload = message.into_ok_payload().expect("ok");
        let info = ProcessInfo::parse(ProcessInfoVersion::V3, &payload).expect("parse");

        assert_eq!(info.os, "Windows");
        assert_eq!(info.arch, "x64");
        assert_eq!(info.entrypoint_assembly.as_deref(), Some("SteadyAllocator"));
        assert!(
            info.clr_product_version
                .as_deref()
                .is_some_and(|v| v.starts_with("10.")),
            "{:?}",
            info.clr_product_version
        );
        assert_eq!(info.runtime_identifier.as_deref(), Some("win-x64"));
        assert!(info.command_line.contains("SteadyAllocator"));
    }

    // --- Sessions ----------------------------------------------------------------------------

    pub(crate) fn gc_provider() -> Provider {
        Provider::new("P", 0x1, EventLevel::Informational)
    }

    #[test]
    fn encodes_collect_tracing3_payload() {
        let payload = SessionConfig::new(vec![gc_provider()]).encode(CommandCode::COLLECT_TRACING3);
        let expected: Vec<u8> = [
            &64u32.to_le_bytes()[..], // circularBufferMB
            &1u32.to_le_bytes(),      // format = NetTrace
            &[0],                     // requestRundown = false
            &[0],                     // requestStackwalk = false
            &1u32.to_le_bytes(),      // provider count
            &1u64.to_le_bytes(),      // keywords
            &4u32.to_le_bytes(),      // level = Informational
            &2u32.to_le_bytes(),      // name length incl. NUL
            &[b'P', 0, 0, 0],         // "P\0" in UTF-16LE
            &1u32.to_le_bytes(),      // arguments: empty string = just the NUL
            &[0, 0],
        ]
        .concat();
        assert_eq!(payload, expected);
    }

    #[test]
    fn collect_tracing2_has_no_stackwalk_flag() {
        let config = SessionConfig::new(vec![gc_provider()]);
        let v3 = config.encode(CommandCode::COLLECT_TRACING3);
        let v2 = config.encode(CommandCode::COLLECT_TRACING2);
        assert_eq!(v2.len() + 1, v3.len());
        assert_eq!(v2[..9], v3[..9]);
        assert_eq!(v2[9..], v3[10..]);
    }

    #[test]
    fn escapes_provider_arguments() {
        let provider = gc_provider()
            .with_argument("EventCounterIntervalSec", "1")
            .with_argument("Filter", "a=b;c");
        assert_eq!(
            provider.argument_string(),
            r#"EventCounterIntervalSec=1;Filter="a=b;c""#
        );
    }

    #[test]
    fn session_id_round_trips() {
        let id = SessionId(0x0102_0304_0506_0708);
        assert_eq!(SessionId::parse(&id.encode()).expect("parse"), id);
        assert!(SessionId::parse(&[1, 2, 3]).is_err());
    }
}
