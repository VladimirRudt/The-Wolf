use std::{fmt, io};

use crate::CommandCode;

#[derive(Debug)]
pub enum IpcError {
    Io(io::Error),
    /// The runtime did not answer in time.
    Timeout,
    /// The response does not start with the protocol magic.
    BadMagic,
    /// The message is structurally invalid; the text says what was wrong.
    Malformed(&'static str),
    /// The payload does not fit the 16-bit message size.
    PayloadTooLarge(usize),
    /// The runtime answered with an error message.
    Server(ServerError),
    /// The runtime answered with something other than OK or Error.
    UnexpectedResponse(CommandCode),
}

impl fmt::Display for IpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "diagnostics IPC I/O error: {e}"),
            Self::Timeout => write!(f, "diagnostics IPC request timed out"),
            Self::BadMagic => write!(f, "diagnostics IPC message has an unknown magic"),
            Self::Malformed(what) => write!(f, "malformed diagnostics IPC message: {what}"),
            Self::PayloadTooLarge(len) => write!(f, "IPC payload of {len} bytes is too large"),
            Self::Server(e) => write!(f, "runtime returned error {e}"),
            Self::UnexpectedResponse(c) => {
                write!(f, "unexpected IPC response 0x{:02X}{:02X}", c.set, c.id)
            }
        }
    }
}

impl std::error::Error for IpcError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for IpcError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

/// HRESULT sent by the runtime in an error response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ServerError(pub u32);

impl ServerError {
    pub const BAD_ENCODING: Self = Self(0x8013_1384);
    pub const UNKNOWN_COMMAND: Self = Self(0x8013_1385);
    pub const UNKNOWN_MAGIC: Self = Self(0x8013_1386);
    pub const NOT_SUPPORTED: Self = Self(0x8013_1515);
    pub const FAIL: Self = Self(0x8000_4005);
    pub const NOT_YET_AVAILABLE: Self = Self(0x8013_135B);
    pub const RUNTIME_UNINITIALIZED: Self = Self(0x8013_1371);
    pub const INVALID_ARG: Self = Self(0x8007_0057);

    /// Name of a known diagnostics IPC error.
    pub fn name(self) -> Option<&'static str> {
        Some(match self {
            Self::BAD_ENCODING => "BAD_ENCODING",
            Self::UNKNOWN_COMMAND => "UNKNOWN_COMMAND",
            Self::UNKNOWN_MAGIC => "UNKNOWN_MAGIC",
            Self::NOT_SUPPORTED => "NOT_SUPPORTED",
            Self::FAIL => "FAIL",
            Self::NOT_YET_AVAILABLE => "NOT_YET_AVAILABLE",
            Self::RUNTIME_UNINITIALIZED => "RUNTIME_UNINITIALIZED",
            Self::INVALID_ARG => "INVALID_ARG",
            _ => return None,
        })
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:08X}", self.0)?;
        if let Some(name) = self.name() {
            write!(f, " ({name})")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_error_display() {
        assert_eq!(
            ServerError::UNKNOWN_COMMAND.to_string(),
            "0x80131385 (UNKNOWN_COMMAND)"
        );
        assert_eq!(ServerError(0x1234).to_string(), "0x00001234");
    }
}
