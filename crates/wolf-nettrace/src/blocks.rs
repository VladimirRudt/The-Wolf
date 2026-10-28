//! Contents of the blocks: EventBlock, MetadataBlock and SPBlock payloads (NetTrace v5 spec,
//! checked against the runtime writer `ep-block.c` and TraceEvent's reader).

use std::collections::HashMap;
use std::fmt;

use crate::TraceInfo;
use crate::cursor::{ByteCursor, Malformed};
use crate::parser::Stats;
use crate::sequence::SequenceTracker;

/// Describes one kind of event. Sent once per session, before the first such event.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EventMetadata {
    /// e.g. `Microsoft-Windows-DotNETRuntime`.
    pub provider: String,
    /// Event id within the provider, e.g. 1 for `GCStart`.
    pub event_id: u32,
    /// Event name; empty for runtime events, set for EventSource events (`EventCounters`).
    pub name: String,
    /// Payload layout version, e.g. `GCStart_V2`.
    pub version: u32,
}

/// One event. Borrows from the parser and is valid only inside the callback.
#[derive(Clone, Copy)]
pub struct Event<'a> {
    pub metadata: &'a EventMetadata,
    /// The thread the event describes.
    pub thread_id: u64,
    /// QPC ticks; see [`Event::time_utc_ns`].
    pub timestamp: i64,
    /// Raw payload; its layout is given by `metadata` (provider, id, version).
    pub payload: &'a [u8],
    pub trace: &'a TraceInfo,
}

impl Event<'_> {
    /// Timestamp as UTC ns since the Unix epoch.
    pub fn time_utc_ns(&self) -> i64 {
        self.trace.to_utc_ns(self.timestamp)
    }
}

/// Shows the payload size, not its bytes: payloads can hold command lines and other secrets.
impl fmt::Debug for Event<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Event")
            .field("metadata", self.metadata)
            .field("thread_id", &self.thread_id)
            .field("timestamp", &self.timestamp)
            .field("payload_len", &self.payload.len())
            .finish_non_exhaustive()
    }
}

/// EventBlock header flag: event headers use the compressed encoding.
const HEADER_COMPRESSION: u16 = 1;

/// Compressed event header flags.
mod flags {
    pub(super) const METADATA_ID: u8 = 1 << 0;
    pub(super) const CAPTURE_THREAD_AND_SEQUENCE: u8 = 1 << 1;
    pub(super) const THREAD_ID: u8 = 1 << 2;
    pub(super) const STACK_ID: u8 = 1 << 3;
    pub(super) const ACTIVITY_ID: u8 = 1 << 4;
    pub(super) const RELATED_ACTIVITY_ID: u8 = 1 << 5;
    pub(super) const PAYLOAD_SIZE: u8 = 1 << 7;
}

/// The header fields we use. Each compressed header is a delta against the previous one in
/// the same block; the first is a delta against all zeros.
#[derive(Debug, Default, Clone, Copy)]
struct BlobHeader {
    metadata_id: u32,
    sequence_number: u32,
    capture_thread_id: u64,
    thread_id: u64,
    timestamp: i64,
    payload_size: u32,
}

impl BlobHeader {
    /// Reads the next compressed header, updating `self` in place.
    fn read_next(&mut self, cursor: &mut ByteCursor<'_>) -> Result<(), Malformed> {
        let flags = cursor.u8()?;
        if flags & flags::METADATA_ID != 0 {
            self.metadata_id = cursor.varint_u32()?;
        }
        if flags & flags::CAPTURE_THREAD_AND_SEQUENCE != 0 {
            self.sequence_number = self
                .sequence_number
                .wrapping_add(cursor.varint_u32()?)
                .wrapping_add(1);
            self.capture_thread_id = cursor.varint_u64()?;
            let _processor_number = cursor.varint_u32()?;
        } else if self.metadata_id != 0 {
            self.sequence_number = self.sequence_number.wrapping_add(1);
        }
        if flags & flags::THREAD_ID != 0 {
            self.thread_id = cursor.varint_u64()?;
        }
        if flags & flags::STACK_ID != 0 {
            let _stack_id = cursor.varint_u32()?;
        }
        // Delta-encoded; QPC values fit in i64, so the cast just reinterprets.
        self.timestamp = self.timestamp.wrapping_add(cursor.varint_u64()? as i64);
        if flags & flags::ACTIVITY_ID != 0 {
            cursor.bytes(16)?;
        }
        if flags & flags::RELATED_ACTIVITY_ID != 0 {
            cursor.bytes(16)?;
        }
        if flags & flags::PAYLOAD_SIZE != 0 {
            self.payload_size = cursor.varint_u32()?;
        }
        Ok(())
    }
}

/// Reads the EventBlock/MetadataBlock header and calls `on_blob` for every blob.
fn for_each_blob<'a>(
    payload: &'a [u8],
    mut on_blob: impl FnMut(&BlobHeader, &'a [u8]),
) -> Result<(), Malformed> {
    let mut cursor = ByteCursor::new(payload);
    let header_size = usize::from(cursor.u16()?);
    let block_flags = cursor.u16()?;
    // header size (2) + flags (2) + min and max timestamp (8 + 8).
    if header_size < 20 {
        return Err(Malformed("block header too small"));
    }
    cursor.bytes(header_size - 4)?;
    // Every runtime that writes NetTrace v4 compresses headers (`ep-block.c`).
    if block_flags & HEADER_COMPRESSION == 0 {
        return Err(Malformed("uncompressed event headers are not supported"));
    }
    let mut header = BlobHeader::default();
    while cursor.remaining() > 0 {
        header.read_next(&mut cursor)?;
        let blob = cursor.bytes(header.payload_size as usize)?;
        on_blob(&header, blob);
    }
    Ok(())
}

/// Emits the events of an EventBlock. Stops at the first malformed event: the events before
/// it are already emitted, the rest of the block is lost.
pub(crate) fn event_block<'a>(
    payload: &'a [u8],
    trace: &'a TraceInfo,
    metadata: &'a HashMap<u32, EventMetadata>,
    sequences: &mut SequenceTracker,
    stats: &mut Stats,
    on_event: &mut impl FnMut(Event<'_>),
) -> Result<(), Malformed> {
    for_each_blob(payload, |header, blob| {
        stats.dropped_events +=
            sequences.on_event(header.capture_thread_id, header.sequence_number);
        let Some(metadata) = metadata.get(&header.metadata_id) else {
            stats.unknown_events += 1;
            return;
        };
        stats.events += 1;
        on_event(Event {
            metadata,
            thread_id: header.thread_id,
            timestamp: header.timestamp,
            payload: blob,
            trace,
        });
    })
}

/// Adds the records of a MetadataBlock. A malformed record is skipped and counted; events
/// that refer to it are then counted as unknown.
pub(crate) fn metadata_block(
    payload: &[u8],
    metadata: &mut HashMap<u32, EventMetadata>,
    stats: &mut Stats,
) -> Result<(), Malformed> {
    for_each_blob(payload, |_, blob| match parse_metadata(blob) {
        Ok((id, record)) => {
            metadata.insert(id, record);
        }
        Err(Malformed(reason)) => {
            stats.skipped_metadata += 1;
            stats.last_skip_reason = Some(reason);
        }
    })
}

/// The fixed part of a metadata record. Field descriptions and tags that follow are not
/// read: payloads are decoded by known layouts (P1-4), not from the metadata.
fn parse_metadata(blob: &[u8]) -> Result<(u32, EventMetadata), Malformed> {
    let mut cursor = ByteCursor::new(blob);
    let id = cursor.u32()?;
    let provider = cursor.utf16z()?;
    let event_id = cursor.u32()?;
    let name = cursor.utf16z()?;
    let _keywords = cursor.u64()?;
    let version = cursor.u32()?;
    let _level = cursor.u32()?;
    Ok((
        id,
        EventMetadata {
            provider,
            event_id,
            name,
            version,
        },
    ))
}

/// Applies an SPBlock: the latest sequence number of every live thread.
pub(crate) fn sequence_point_block(
    payload: &[u8],
    sequences: &mut SequenceTracker,
    stats: &mut Stats,
) -> Result<(), Malformed> {
    let mut cursor = ByteCursor::new(payload);
    let _timestamp = cursor.i64()?;
    let count = cursor.u32()? as usize;
    // thread id (8) + sequence number (4)
    if count > cursor.remaining() / 12 {
        return Err(Malformed("sequence point thread count"));
    }
    let mut threads = Vec::with_capacity(count);
    for _ in 0..count {
        threads.push((cursor.u64()?, cursor.u32()?));
    }
    stats.dropped_events += sequences.on_sequence_point(&threads);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_stream::{event_block_payload, metadata_record};

    #[test]
    fn decodes_compressed_headers_with_deltas() {
        // Two events: the second repeats metadata, thread and size, so only its flags byte
        // and timestamp delta are written.
        let mut payload = event_block_payload();
        payload.extend_from_slice(&[
            0b1000_0111, // metadata id, capture thread + sequence, thread id, payload size
            3,           // metadata id
            4,           // sequence delta - 1 → 5
            0x2A,        // capture thread
            0,           // processor
            0x2B,        // thread
            100,         // timestamp delta
            2,           // payload size
            0xAA,
            0xBB,
        ]);
        payload.extend_from_slice(&[0, 7, 0xCC, 0xDD]);

        let mut seen = Vec::new();
        for_each_blob(&payload, |header, blob| {
            seen.push((*header, blob.to_vec()));
        })
        .expect("parse");

        let (first, first_payload) = &seen[0];
        assert_eq!(
            (
                first.metadata_id,
                first.sequence_number,
                first.capture_thread_id
            ),
            (3, 5, 0x2A)
        );
        assert_eq!((first.thread_id, first.timestamp), (0x2B, 100));
        assert_eq!(first_payload, &[0xAA, 0xBB]);
        let (second, second_payload) = &seen[1];
        assert_eq!(second.sequence_number, 6);
        assert_eq!((second.thread_id, second.timestamp), (0x2B, 107));
        assert_eq!(second_payload, &[0xCC, 0xDD]);
    }

    #[test]
    fn rejects_uncompressed_blocks() {
        let mut payload = event_block_payload();
        payload[2] = 0;
        assert_eq!(
            for_each_blob(&payload, |_, _| {}),
            Err(Malformed("uncompressed event headers are not supported"))
        );
    }

    #[test]
    fn rejects_payload_past_the_block_end() {
        let mut payload = event_block_payload();
        payload.extend_from_slice(&[flags::PAYLOAD_SIZE, 0, 50, 1, 2]);
        assert!(for_each_blob(&payload, |_, _| {}).is_err());
    }

    #[test]
    fn parses_metadata_record() {
        let (id, record) = parse_metadata(&metadata_record(
            3,
            "Microsoft-Windows-DotNETRuntime",
            1,
            "",
            2,
        ))
        .expect("parse");
        assert_eq!(id, 3);
        assert_eq!(record.provider, "Microsoft-Windows-DotNETRuntime");
        assert_eq!((record.event_id, record.version), (1, 2));
        assert_eq!(record.name, "");
    }
}
