//! .NET diagnostics IPC client (Unix domain socket / Windows named pipe).
//!
//! Implements the Diagnostic IPC protocol: endpoint discovery, request/response messages and
//! the ProcessInfo command. Verified against `documentation/design-docs/ipc-protocol.md` and
//! `Microsoft.Diagnostics.NETCore.Client` (`DiagnosticsIpc/ProcessInfo.cs`, `IpcTransport.cs`,
//! `DiagnosticsClient.cs`) in `dotnet/diagnostics`. Where the doc's C struct and its field
//! list disagree (ProcessInfo field order), the field list and the client win.

mod client;
mod error;
mod process_info;
mod protocol;
mod transport;

pub use client::{process_info, request};
pub use error::{IpcError, ServerError};
pub use process_info::{ProcessInfo, RuntimeCookie};
pub use protocol::{CommandCode, Message, PROTOCOL_MAGIC};
pub use transport::{Endpoint, discover};
