//! Parser for the EventPipe `.nettrace` stream format: NetTrace v4 framing with v5 metadata,
//! as written by every runtime from .NET 6 on (`docs/collector-dotnet.md` §3.1).
//!
//! The parser is incremental and allocation-free per event: the caller reads the stream
//! straight into the parser's fixed buffer and gets each event through a callback, with the
//! payload borrowed from that buffer.
//!
//! ```no_run
//! # fn read_chunk(_: &mut [u8]) -> std::io::Result<usize> { Ok(0) }
//! let mut parser = wolf_nettrace::NettraceParser::new();
//! loop {
//!     let n = read_chunk(parser.spare())?;
//!     if n == 0 {
//!         break;
//!     }
//!     parser.commit(n, |event| println!("{} {}", event.metadata.provider, event.metadata.event_id))?;
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

mod blocks;
mod cursor;
mod error;
mod parser;
mod sequence;
#[cfg(test)]
mod test_stream;
mod trace;

pub use blocks::{Event, EventMetadata};
pub use error::ParseError;
pub use parser::{BUFFER_SIZE, NettraceParser, Stats};
pub use trace::TraceInfo;
