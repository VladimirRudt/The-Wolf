//! Builds synthetic nettrace streams for unit tests.

use crate::trace::tests::trace_fields;

/// Appends `value` as a varint.
fn varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push((value as u8) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn utf16z(out: &mut Vec<u8>, text: &str) {
    for unit in text.encode_utf16().chain([0]) {
        out.extend_from_slice(&unit.to_le_bytes());
    }
}

/// A stream under construction: magic, header and Trace object, then blocks.
#[derive(Debug)]
pub(crate) struct StreamBuilder {
    bytes: Vec<u8>,
}

impl StreamBuilder {
    pub(crate) fn new() -> Self {
        Self::with_trace_version(4)
    }

    pub(crate) fn with_trace_version(version: u32) -> Self {
        let mut bytes = b"Nettrace".to_vec();
        bytes.extend_from_slice(&20u32.to_le_bytes());
        bytes.extend_from_slice(b"!FastSerialization.1");
        let mut builder = Self { bytes };
        builder.type_header("Trace", version, version);
        builder.bytes.extend_from_slice(&trace_fields());
        builder.bytes.push(6);
        builder
    }

    /// BeginObject, then the type object.
    fn type_header(&mut self, name: &str, version: u32, min_version: u32) {
        self.bytes.extend_from_slice(&[5, 5, 1]);
        self.bytes.extend_from_slice(&version.to_le_bytes());
        self.bytes.extend_from_slice(&min_version.to_le_bytes());
        self.bytes
            .extend_from_slice(&(name.len() as u32).to_le_bytes());
        self.bytes.extend_from_slice(name.as_bytes());
        self.bytes.push(6);
    }

    /// A block object: size, padding to a 4-byte stream offset, payload, EndObject.
    pub(crate) fn block(mut self, name: &str, min_version: u32, payload: &[u8]) -> Self {
        self.type_header(name, 2, min_version);
        self.bytes
            .extend_from_slice(&(payload.len() as u32).to_le_bytes());
        while self.bytes.len() % 4 != 0 {
            self.bytes.push(0);
        }
        self.bytes.extend_from_slice(payload);
        self.bytes.push(6);
        self
    }

    /// Ends the stream with the NullReference tag.
    pub(crate) fn end(mut self) -> Vec<u8> {
        self.bytes.push(1);
        self.bytes
    }

    /// The stream so far, without the end tag (a cut-off stream).
    pub(crate) fn unfinished(self) -> Vec<u8> {
        self.bytes
    }
}

/// EventBlock/MetadataBlock header: size 20, compressed, min and max timestamp 0.
pub(crate) fn event_block_payload() -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&20u16.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&[0; 16]);
    bytes
}

pub(crate) fn metadata_record(
    id: u32,
    provider: &str,
    event_id: u32,
    name: &str,
    version: u32,
) -> Vec<u8> {
    let mut bytes = id.to_le_bytes().to_vec();
    utf16z(&mut bytes, provider);
    bytes.extend_from_slice(&event_id.to_le_bytes());
    utf16z(&mut bytes, name);
    bytes.extend_from_slice(&0u64.to_le_bytes()); // keywords
    bytes.extend_from_slice(&version.to_le_bytes());
    bytes.extend_from_slice(&4u32.to_le_bytes()); // level
    bytes.extend_from_slice(&0u32.to_le_bytes()); // field count
    bytes
}

/// MetadataBlock payload: each record is a blob with metadata id 0.
pub(crate) fn metadata_block(records: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = event_block_payload();
    for record in records {
        bytes.push(1 << 7); // payload size only
        varint(&mut bytes, 0); // timestamp delta
        varint(&mut bytes, record.len() as u64);
        bytes.extend_from_slice(record);
    }
    bytes
}

#[derive(Debug, Clone)]
pub(crate) struct TestEvent {
    pub(crate) metadata_id: u32,
    pub(crate) sequence: u32,
    pub(crate) capture_thread: u64,
    pub(crate) thread: u64,
    pub(crate) timestamp: u64,
    pub(crate) payload: Vec<u8>,
}

/// EventBlock payload with every header field written explicitly.
pub(crate) fn event_block(events: &[TestEvent]) -> Vec<u8> {
    let mut bytes = event_block_payload();
    let (mut sequence, mut timestamp) = (0u32, 0u64);
    for event in events {
        bytes.push(0b1000_0111); // metadata id, capture thread + sequence, thread, payload size
        varint(&mut bytes, event.metadata_id.into());
        varint(
            &mut bytes,
            event.sequence.wrapping_sub(sequence).wrapping_sub(1).into(),
        );
        varint(&mut bytes, event.capture_thread);
        varint(&mut bytes, 0); // processor
        varint(&mut bytes, event.thread);
        varint(&mut bytes, event.timestamp - timestamp);
        varint(&mut bytes, event.payload.len() as u64);
        bytes.extend_from_slice(&event.payload);
        (sequence, timestamp) = (event.sequence, event.timestamp);
    }
    bytes
}

/// SPBlock payload.
pub(crate) fn sequence_point_block(threads: &[(u64, u32)]) -> Vec<u8> {
    let mut bytes = 0i64.to_le_bytes().to_vec();
    bytes.extend_from_slice(&(threads.len() as u32).to_le_bytes());
    for (thread, sequence) in threads {
        bytes.extend_from_slice(&thread.to_le_bytes());
        bytes.extend_from_slice(&sequence.to_le_bytes());
    }
    bytes
}
