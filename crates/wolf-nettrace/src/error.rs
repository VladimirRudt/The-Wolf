/// A stream the parser cannot continue: the framing is broken or the format is not one we
/// read. Problems inside a block are not errors; the block is skipped and counted in
/// [`Stats`](crate::Stats).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ParseError {
    #[error("not a nettrace stream (bad magic)")]
    NotNettrace,
    #[error("unsupported nettrace version {0} (only 4 is supported)")]
    UnsupportedVersion(u32),
    #[error("malformed stream framing: {0}")]
    Malformed(&'static str),
    #[error("block of {0} bytes is larger than the parser buffer allows")]
    BlockTooLarge(usize),
}
