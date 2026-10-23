use std::fmt;

use crate::IpcError;
use crate::protocol::PayloadReader;

/// Answer to the ProcessInfo command family.
#[derive(Debug, Clone, PartialEq, Eq)]
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

impl ProcessInfo {
    pub(crate) fn parse(version: ProcessInfoVersion, payload: &[u8]) -> Result<Self, IpcError> {
        let mut reader = PayloadReader::new(payload);
        let payload_version = match version {
            ProcessInfoVersion::V3 => Some(reader.u32()?),
            _ => None,
        };
        let process_id = reader.u64()?;
        let runtime_cookie = RuntimeCookie(reader.array()?);
        let command_line = reader.string()?;
        let os = reader.string()?;
        let arch = reader.string()?;
        let (entrypoint_assembly, clr_product_version) = match version {
            ProcessInfoVersion::V1 => (None, None),
            _ => (Some(reader.string()?), Some(reader.string()?)),
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

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::protocol::test_support::PayloadBuilder;
    use crate::{CommandCode, Message};

    const COOKIE: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];

    fn common(builder: PayloadBuilder) -> PayloadBuilder {
        builder
            .u64(4812)
            .bytes(&COOKIE)
            .string(r#""C:\Program Files\dotnet\dotnet.exe" App.dll"#)
            .string("Windows")
            .string("x64")
    }

    pub(crate) fn v2_payload() -> Vec<u8> {
        common(PayloadBuilder::default())
            .string("App")
            .string("8.0.1+abc")
            .build()
    }

    #[test]
    fn parses_v1() {
        let payload = common(PayloadBuilder::default()).build();
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
    fn parses_v2() {
        let info = ProcessInfo::parse(ProcessInfoVersion::V2, &v2_payload()).expect("parse");
        assert_eq!(info.entrypoint_assembly.as_deref(), Some("App"));
        assert_eq!(info.clr_product_version.as_deref(), Some("8.0.1+abc"));
        assert_eq!(info.runtime_identifier, None);
    }

    #[test]
    fn parses_v3_payload_versions() {
        let v3 = |payload_version: u32| {
            let builder = common(PayloadBuilder::default().u32(payload_version))
                .string("App")
                .string("10.0.0")
                .string("win-x64");
            // A future version appends fields we don't know yet.
            builder.string("future field").build()
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
    fn truncated_payload_is_malformed() {
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
}
