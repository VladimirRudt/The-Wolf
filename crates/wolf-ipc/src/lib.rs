//! .NET diagnostics IPC client (Unix domain socket / Windows named pipe).
//!
//! - [`discover`] finds running .NET processes and their [`Endpoint`]s.
//! - [`DiagnosticsClient`] talks to one process: ProcessInfo and EventPipe sessions.
//!
//! Verified against `documentation/design-docs/ipc-protocol.md` and
//! `Microsoft.Diagnostics.NETCore.Client` (`DiagnosticsIpc/ProcessInfo.cs`, `IpcTransport.cs`,
//! `DiagnosticsClient.cs`, `EventPipeSession.cs`, `EventPipeSessionConfiguration.cs`) in
//! `dotnet/diagnostics`. Where the doc's C structs and its field lists disagree, the field
//! lists and the client win.
//!
//! This crate only opens sessions; the `nettrace` bytes are parsed by `wolf-nettrace`.

mod client;
mod discovery;
mod error;
mod messaging;
mod payloads;
#[cfg(test)]
mod test_server;

pub use client::{DiagnosticsClient, EventSession};
pub use discovery::{Endpoint, discover};
pub use error::{IpcError, ServerError};
pub use messaging::{CommandCode, Message, PROTOCOL_MAGIC};
pub use payloads::{EventLevel, ProcessInfo, Provider, RuntimeCookie, SessionConfig, SessionId};
