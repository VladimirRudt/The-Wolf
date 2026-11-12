//! Host errors. Per-target attach failures are reported and skipped by the caller; these
//! are the failures that stop the host itself.

use thiserror::Error;
use wolf_contract::ParseTargetIdError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("storage: {0}")]
    Storage(#[from] wolf_storage::Error),
    #[error("ipc: {0}")]
    Ipc(#[from] wolf_ipc::IpcError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("nettrace: {0}")]
    Parse(#[from] wolf_nettrace::ParseError),
    #[error("target id: {0}")]
    Contract(#[from] ParseTargetIdError),
    /// No diagnostic endpoint for this pid: not a .NET process, another user's process, or
    /// an endpoint directory we don't scan (`docs/collector-dotnet.md` §2).
    #[error("no diagnostic endpoint found for pid {0}")]
    NoEndpoint(u32),
    /// Every pid failed to attach; there is nothing to do.
    #[error("no targets could be attached")]
    NoTargets,
}

pub type Result<T> = std::result::Result<T, Error>;
