//! The stream: a fixed buffer the caller reads into, and the framing around blocks (magic,
//! `Trace` object, block objects, end tag).

use std::collections::HashMap;
use std::fmt;

use crate::blocks::{self, Event, EventMetadata};
use crate::cursor::{ByteCursor, Malformed};
use crate::sequence::SequenceTracker;
use crate::{ParseError, TraceInfo};

/// Size of the parser's buffer, allocated once.
pub const BUFFER_SIZE: usize = 256 * 1024;
/// Largest block payload accepted. The runtime writes blocks of at most 100 KiB (`ep-file.c`);
/// the rest is headroom.
const MAX_BLOCK_SIZE: usize = 128 * 1024;
/// Longest object type name accepted; the longest known one is `MetadataBlock`.
const MAX_TYPE_NAME: usize = 64;
/// BeginObject, BeginObject, NullReference, version, min version, name length.
const TYPE_HEADER_SIZE: usize = 3 + 12;
/// Largest complete object: type header, name, EndObject, size, padding, payload, EndObject.
/// The unparsed tail is always smaller, so after compaction `spare()` has at least
/// `BUFFER_SIZE - MAX_OBJECT_SIZE` (~124 KiB) to read into.
const MAX_OBJECT_SIZE: usize = TYPE_HEADER_SIZE + MAX_TYPE_NAME + 1 + 4 + 3 + MAX_BLOCK_SIZE + 1;
const _: () = assert!(MAX_OBJECT_SIZE < BUFFER_SIZE);

const STREAM_MAGIC: &[u8] = b"Nettrace";
const SERIALIZATION_MAGIC: &[u8] = b"!FastSerialization.1";
/// The `Trace` object version every supported runtime writes.
const TRACE_VERSION: u32 = 4;
/// Highest block version this parser reads.
const BLOCK_VERSION: u32 = 2;

/// FastSerialization tags.
const NULL_REFERENCE: u8 = 1;
const BEGIN_PRIVATE_OBJECT: u8 = 5;
const END_OBJECT: u8 = 6;

/// Counters for self-telemetry.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Stats {
    /// Events passed to the callback.
    pub events: u64,
    /// Events the runtime dropped, from sequence-number gaps.
    pub dropped_events: u64,
    /// Events skipped because their metadata is unknown.
    pub unknown_events: u64,
    /// Blocks skipped: unknown type, unsupported version or encoding, or malformed contents.
    pub skipped_blocks: u64,
    /// Malformed metadata records; events that use them count as unknown.
    pub skipped_metadata: u64,
    /// Bytes after the end tag, ignored.
    pub trailing_bytes: u64,
    /// Why the last block or metadata record was skipped.
    pub last_skip_reason: Option<&'static str>,
}

/// Incremental `.nettrace` parser with a fixed buffer.
///
/// Read into [`spare`](Self::spare), then [`commit`](Self::commit) the bytes read. Chunks
/// may split the stream anywhere. After an error the stream is unusable: stop the session.
pub struct NettraceParser {
    buf: Box<[u8]>,
    /// First unparsed byte.
    start: usize,
    /// End of the bytes read.
    end: usize,
    /// Stream offset of `buf[0]`: blocks are padded to 4-byte stream offsets.
    origin: u64,
    stream: StreamState,
}

/// Everything except the buffer, so blocks can borrow from the buffer while this changes.
#[derive(Debug)]
struct StreamState {
    phase: Phase,
    metadata: HashMap<u32, EventMetadata>,
    sequences: SequenceTracker,
    stats: Stats,
}

#[derive(Debug)]
enum Phase {
    /// `Nettrace` magic and the FastSerialization header.
    Header,
    /// The `Trace` object.
    Trace,
    /// Block objects until the end tag.
    Blocks(TraceInfo),
    Finished,
}

impl Default for NettraceParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Leaves out the buffer: it is 256 KiB of raw payloads, including the target's command line.
impl fmt::Debug for NettraceParser {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NettraceParser")
            .field("start", &self.start)
            .field("end", &self.end)
            .field("origin", &self.origin)
            .field("phase", &self.stream.phase)
            .field("stats", &self.stream.stats)
            .finish_non_exhaustive()
    }
}

impl NettraceParser {
    /// Allocates the buffer ([`BUFFER_SIZE`]); nothing else is allocated per event.
    pub fn new() -> Self {
        Self {
            buf: vec![0; BUFFER_SIZE].into_boxed_slice(),
            start: 0,
            end: 0,
            origin: 0,
            stream: StreamState {
                phase: Phase::Header,
                metadata: HashMap::new(),
                sequences: SequenceTracker::default(),
                stats: Stats::default(),
            },
        }
    }

    /// Free space to read the next chunk into. Never clears memory: it resets the cursors when
    /// everything is parsed, and moves the unparsed tail to the front only when the space
    /// after it is smaller than one object.
    pub fn spare(&mut self) -> &mut [u8] {
        if self.start == self.end {
            self.origin += self.start as u64;
            self.start = 0;
            self.end = 0;
        } else if self.buf.len() - self.end < MAX_OBJECT_SIZE {
            self.buf.copy_within(self.start..self.end, 0);
            self.origin += self.start as u64;
            self.end -= self.start;
            self.start = 0;
        }
        &mut self.buf[self.end..]
    }

    /// Takes `n` bytes written into [`spare`](Self::spare), parses every complete object and
    /// passes each event to `on_event`. An incomplete object waits for the next chunk.
    ///
    /// # Errors
    /// [`ParseError`] if the stream is not nettrace, has an unsupported version, or its
    /// framing is broken. Bad block contents are not errors: see [`Stats`].
    ///
    /// # Panics
    /// If `n` is larger than the last `spare()` slice.
    pub fn commit(
        &mut self,
        n: usize,
        mut on_event: impl FnMut(Event<'_>),
    ) -> Result<(), ParseError> {
        assert!(
            n <= self.buf.len() - self.end,
            "committed more bytes than spare() returned"
        );
        self.end += n;
        while self.start < self.end {
            let data = &self.buf[self.start..self.end];
            let offset = self.origin + self.start as u64;
            match self.stream.step(data, offset, &mut on_event)? {
                Some(consumed) => self.start += consumed,
                None => break,
            }
        }
        Ok(())
    }

    /// True once the end tag was read: the stream ended cleanly, not cut off.
    pub fn is_finished(&self) -> bool {
        matches!(self.stream.phase, Phase::Finished)
    }

    /// Counters so far, for self-telemetry.
    pub fn stats(&self) -> Stats {
        self.stream.stats
    }
}

impl StreamState {
    /// Parses one unit from the front of `data`, which starts at stream `offset`. Returns the
    /// bytes consumed, or `None` if `data` doesn't hold a complete unit yet.
    fn step(
        &mut self,
        data: &[u8],
        offset: u64,
        on_event: &mut impl FnMut(Event<'_>),
    ) -> Result<Option<usize>, ParseError> {
        match &self.phase {
            Phase::Header => {
                let size = STREAM_MAGIC.len() + 4 + SERIALIZATION_MAGIC.len();
                let Some(header) = data.get(..size) else {
                    return Ok(None);
                };
                let (magic, rest) = header.split_at(STREAM_MAGIC.len());
                let (length, serialization) = rest.split_at(4);
                if magic != STREAM_MAGIC
                    || length != (SERIALIZATION_MAGIC.len() as u32).to_le_bytes()
                    || serialization != SERIALIZATION_MAGIC
                {
                    return Err(ParseError::NotNettrace);
                }
                self.phase = Phase::Trace;
                Ok(Some(size))
            }
            Phase::Trace => {
                let Some(header) = TypeHeader::parse(data)? else {
                    return Ok(None);
                };
                if header.name != b"Trace" {
                    return Err(ParseError::Malformed("expected the Trace object"));
                }
                if header.version != TRACE_VERSION {
                    return Err(ParseError::UnsupportedVersion(header.version));
                }
                let size = header.size + TraceInfo::SIZE + 1;
                let Some(object) = data.get(..size) else {
                    return Ok(None);
                };
                let fields = &object[header.size..size - 1];
                let trace = TraceInfo::parse(&mut ByteCursor::new(fields)).map_err(framing)?;
                expect_end_object(object)?;
                self.phase = Phase::Blocks(trace);
                Ok(Some(size))
            }
            Phase::Blocks(_) if data[0] == NULL_REFERENCE => {
                self.phase = Phase::Finished;
                Ok(Some(1))
            }
            Phase::Blocks(trace) => {
                let Some(header) = TypeHeader::parse(data)? else {
                    return Ok(None);
                };
                let Some(size_bytes) = data.get(header.size..header.size + 4) else {
                    return Ok(None);
                };
                let block_size = ByteCursor::new(size_bytes).u32().map_err(framing)? as usize;
                if block_size > MAX_BLOCK_SIZE {
                    return Err(ParseError::BlockTooLarge(block_size));
                }
                let after_size = header.size + 4;
                let padding = (4 - (offset + after_size as u64) % 4) % 4;
                let payload_start = after_size + padding as usize;
                let size = payload_start + block_size + 1;
                let Some(object) = data.get(..size) else {
                    return Ok(None);
                };
                expect_end_object(object)?;
                let payload = &object[payload_start..size - 1];

                let Self {
                    metadata,
                    sequences,
                    stats,
                    ..
                } = self;
                let parsed = if header.min_version > BLOCK_VERSION {
                    Err(Malformed("unsupported block version"))
                } else {
                    match header.name {
                        b"EventBlock" => blocks::event_block(
                            payload, trace, metadata, sequences, stats, on_event,
                        ),
                        b"MetadataBlock" => blocks::metadata_block(payload, metadata, stats),
                        b"SPBlock" => blocks::sequence_point_block(payload, sequences, stats),
                        // Stacks matter only with stackwalk on (allocation sampling, P3-5).
                        b"StackBlock" => Ok(()),
                        _ => Err(Malformed("unknown block type")),
                    }
                };
                if let Err(Malformed(reason)) = parsed {
                    stats.skipped_blocks += 1;
                    stats.last_skip_reason = Some(reason);
                }
                Ok(Some(size))
            }
            // Nothing follows the end tag; ignore anything that does.
            Phase::Finished => {
                self.stats.trailing_bytes += data.len() as u64;
                Ok(Some(data.len()))
            }
        }
    }
}

/// The type object that opens every object.
#[derive(Debug)]
struct TypeHeader<'a> {
    version: u32,
    min_version: u32,
    name: &'a [u8],
    /// Bytes from the object's BeginObject tag through the type's EndObject tag.
    size: usize,
}

impl<'a> TypeHeader<'a> {
    fn parse(data: &'a [u8]) -> Result<Option<Self>, ParseError> {
        // Check the tags as soon as they arrive, so garbage fails now, not after more reads.
        let tags = [BEGIN_PRIVATE_OBJECT, BEGIN_PRIVATE_OBJECT, NULL_REFERENCE];
        if !tags.starts_with(&data[..data.len().min(tags.len())]) {
            return Err(ParseError::Malformed("expected an object"));
        }
        let Some(fixed) = data.get(..TYPE_HEADER_SIZE) else {
            return Ok(None);
        };
        let mut cursor = ByteCursor::new(&fixed[3..]);
        let version = cursor.u32().map_err(framing)?;
        let min_version = cursor.u32().map_err(framing)?;
        let name_length = cursor.u32().map_err(framing)? as usize;
        if name_length > MAX_TYPE_NAME {
            return Err(ParseError::Malformed("type name too long"));
        }
        let size = TYPE_HEADER_SIZE + name_length + 1;
        let Some(header) = data.get(..size) else {
            return Ok(None);
        };
        expect_end_object(header)?;
        Ok(Some(Self {
            version,
            min_version,
            name: &header[TYPE_HEADER_SIZE..size - 1],
            size,
        }))
    }
}

fn expect_end_object(object: &[u8]) -> Result<(), ParseError> {
    match object.last() {
        Some(&END_OBJECT) => Ok(()),
        _ => Err(ParseError::Malformed("expected EndObject")),
    }
}

fn framing(error: Malformed) -> ParseError {
    ParseError::Malformed(error.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_stream::{
        StreamBuilder, TestEvent, event_block, metadata_block, metadata_record,
        sequence_point_block,
    };

    #[derive(Debug, PartialEq, Eq)]
    struct Seen {
        provider: String,
        event_id: u32,
        thread_id: u64,
        timestamp: i64,
        payload: Vec<u8>,
    }

    /// Feeds `stream` in reads of at most `chunk` bytes, like `read_chunk` would.
    fn parse(stream: &[u8], chunk: usize) -> (Result<Vec<Seen>, ParseError>, NettraceParser) {
        let mut parser = NettraceParser::new();
        let mut seen = Vec::new();
        let mut rest = stream;
        while !rest.is_empty() {
            let spare = parser.spare();
            let n = chunk.min(spare.len()).min(rest.len());
            spare[..n].copy_from_slice(&rest[..n]);
            rest = &rest[n..];
            let committed = parser.commit(n, |event| {
                seen.push(Seen {
                    provider: event.metadata.provider.clone(),
                    event_id: event.metadata.event_id,
                    thread_id: event.thread_id,
                    timestamp: event.timestamp,
                    payload: event.payload.to_vec(),
                });
            });
            if let Err(error) = committed {
                return (Err(error), parser);
            }
            assert!(parser.end - parser.start < MAX_OBJECT_SIZE);
        }
        (Ok(seen), parser)
    }

    fn gc_metadata() -> Vec<u8> {
        metadata_block(&[metadata_record(
            1,
            "Microsoft-Windows-DotNETRuntime",
            1,
            "",
            2,
        )])
    }

    fn event(metadata_id: u32, sequence: u32, timestamp: u64, payload: &[u8]) -> TestEvent {
        TestEvent {
            metadata_id,
            sequence,
            capture_thread: 0x2A,
            thread: 0x2B,
            timestamp,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn parses_a_stream_split_at_any_point() {
        let stream = StreamBuilder::new()
            .block("MetadataBlock", 2, &gc_metadata())
            .block(
                "EventBlock",
                2,
                &event_block(&[event(1, 1, 1000, &[1, 2, 3]), event(1, 2, 1010, &[4])]),
            )
            .block("StackBlock", 2, &[0; 8])
            .block("SPBlock", 2, &sequence_point_block(&[(0x2A, 2)]))
            .end();

        let (whole, parser) = parse(&stream, usize::MAX);
        let whole = whole.expect("parse");
        assert_eq!(whole.len(), 2);
        assert_eq!(whole[0].provider, "Microsoft-Windows-DotNETRuntime");
        assert_eq!((whole[0].event_id, whole[0].thread_id), (1, 0x2B));
        assert_eq!(
            (whole[1].timestamp, &whole[1].payload[..]),
            (1010, &[4][..])
        );
        assert!(parser.is_finished());
        assert_eq!(
            parser.stats(),
            Stats {
                events: 2,
                ..Stats::default()
            }
        );

        for chunk in [1, 2, 3, 5, 7, 64] {
            let (split, parser) = parse(&stream, chunk);
            assert_eq!(split.expect("parse"), whole, "chunk size {chunk}");
            assert!(parser.is_finished());
        }
    }

    #[test]
    fn event_carries_the_trace_clock() {
        let stream = StreamBuilder::new()
            .block("MetadataBlock", 2, &gc_metadata())
            .block("EventBlock", 2, &event_block(&[event(1, 1, 1025, &[])]))
            .end();
        let mut parser = NettraceParser::new();
        let spare = parser.spare();
        spare[..stream.len()].copy_from_slice(&stream);
        let mut times = Vec::new();
        parser
            .commit(stream.len(), |event| times.push(event.time_utc_ns()))
            .expect("parse");
        // The test trace syncs 2025-10-19T03:14:05.250Z to QPC 1000 at 10 MHz.
        assert_eq!(times, [1_760_843_645_250_002_500]);
    }

    #[test]
    fn counts_dropped_and_unknown_events() {
        let stream = StreamBuilder::new()
            .block("MetadataBlock", 2, &gc_metadata())
            .block(
                "EventBlock",
                2,
                &event_block(&[
                    event(1, 1, 10, &[]),
                    event(9, 2, 20, &[]),
                    event(1, 5, 30, &[]),
                ]),
            )
            .block("SPBlock", 2, &sequence_point_block(&[(0x2A, 8)]))
            .end();
        let (seen, parser) = parse(&stream, usize::MAX);
        assert_eq!(seen.expect("parse").len(), 2);
        assert_eq!(
            parser.stats(),
            Stats {
                events: 2,
                unknown_events: 1,
                // 3 and 4 missing between events; 6..=8 missing before the sequence point.
                dropped_events: 5,
                ..Stats::default()
            }
        );
    }

    #[test]
    fn skips_unknown_and_newer_blocks() {
        let stream = StreamBuilder::new()
            .block("MetadataBlock", 2, &gc_metadata())
            .block("ThreadBlock", 2, &[1, 2, 3])
            .block("EventBlock", 3, &[0xFF; 9])
            .block("EventBlock", 2, &[0xFF; 2])
            .block("EventBlock", 2, &event_block(&[event(1, 1, 10, &[7])]))
            .end();
        let (seen, parser) = parse(&stream, 3);
        assert_eq!(seen.expect("parse").len(), 1);
        assert_eq!(parser.stats().skipped_blocks, 3);
        assert_eq!(parser.stats().last_skip_reason, Some("truncated"));
        assert!(parser.is_finished());
    }

    #[test]
    fn counts_bad_metadata_records_and_trailing_bytes() {
        let mut stream = StreamBuilder::new()
            .block("MetadataBlock", 2, &metadata_block(&[vec![1, 0, 0]]))
            .end();
        stream.extend_from_slice(&[9, 9]);
        let (seen, parser) = parse(&stream, 4);
        assert!(seen.expect("parse").is_empty());
        let stats = parser.stats();
        assert_eq!((stats.skipped_metadata, stats.skipped_blocks), (1, 0));
        assert_eq!(stats.last_skip_reason, Some("truncated"));
        assert_eq!(stats.trailing_bytes, 2);
    }

    #[test]
    fn compacts_the_buffer_across_many_large_blocks() {
        let payload: Vec<u8> = (0..60_000u32).map(|i| i as u8).collect();
        let mut builder = StreamBuilder::new().block("MetadataBlock", 2, &gc_metadata());
        for i in 0..10u32 {
            let block = event_block(&[event(1, i + 1, u64::from(i) * 10, &payload)]);
            builder = builder.block("EventBlock", 2, &block);
        }
        let stream = builder.end();
        assert!(stream.len() > 2 * BUFFER_SIZE);

        for chunk in [1, 4093, 100_000, usize::MAX] {
            let (seen, parser) = parse(&stream, chunk);
            let seen = seen.expect("parse");
            assert_eq!(seen.len(), 10, "chunk size {chunk}");
            assert!(seen.iter().all(|event| event.payload == payload));
            assert!(parser.is_finished());
        }
    }

    #[test]
    fn a_cut_off_stream_is_not_finished() {
        let stream = StreamBuilder::new()
            .block("MetadataBlock", 2, &gc_metadata())
            .unfinished();
        let (seen, parser) = parse(&stream, 5);
        assert!(seen.expect("parse").is_empty());
        assert!(!parser.is_finished());
    }

    #[test]
    fn rejects_other_formats() {
        let (result, _) = parse(b"Microsoft-Windows-DotNETRuntime-not-a-trace", 7);
        assert_eq!(result, Err(ParseError::NotNettrace));

        let stream = StreamBuilder::with_trace_version(6).end();
        assert_eq!(parse(&stream, 1).0, Err(ParseError::UnsupportedVersion(6)));
    }

    #[test]
    fn rejects_an_oversized_block_before_reading_it() {
        let mut stream = StreamBuilder::new().unfinished();
        stream.extend_from_slice(&[5, 5, 1, 2, 0, 0, 0, 2, 0, 0, 0, 10, 0, 0, 0]);
        stream.extend_from_slice(b"EventBlock");
        stream.push(6);
        stream.extend_from_slice(&(1u32 << 20).to_le_bytes());
        assert_eq!(
            parse(&stream, usize::MAX).0,
            Err(ParseError::BlockTooLarge(1 << 20))
        );
    }

    #[test]
    fn rejects_broken_framing() {
        let mut stream = StreamBuilder::new()
            .block("MetadataBlock", 2, &gc_metadata())
            .unfinished();
        stream.push(9);
        assert_eq!(
            parse(&stream, usize::MAX).0,
            Err(ParseError::Malformed("expected an object"))
        );
    }
}
