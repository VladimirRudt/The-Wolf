//! Payloads of the `System.Runtime` EventCounters events (`docs/collector-dotnet.md` §3.3).
//!
//! Both payload layouts verified against `dotnet/runtime` (`EventCounter.cs`,
//! `IncrementingEventCounter.cs`, `CounterPayload.cs`, .NET 6–10) and the
//! `steady-gc-counters` recording decoded by TraceEvent: strings are null-terminated
//! UTF-16LE, numbers fixed-width little-endian, no per-field tags. Both kinds are written
//! by name (`EventSource.Write("EventCounters", ...)`), so the auto-assigned event id
//! differs per runtime version and is not used; the layouts are told apart by the
//! `CounterType` string plus an exact-consumption check. A payload that matches neither
//! (layout drift, corruption) is counted, never misread.
//!
//! Counter samples arrive several times a second per target, so decoding allocates
//! nothing per event: fields [`EcEvent`] keeps are written into buffers rented from an
//! [`EcStringPool`] (returned automatically on drop), and the rest are scanned past
//! in place.

use std::fmt;
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;
use wolf_nettrace::Event;

use crate::events_decoding::Decoded;

const EVENT_COUNTERS_PROVIDER: &str = "System.Runtime";
const EVENT_COUNTERS_NAME: &str = "EventCounters";

/// The most buffers kept for reuse. A burst beyond this returns its buffers to the
/// allocator instead of the pool, so a stuck consumer cannot grow memory without bound.
const POOL_CAPACITY: usize = 256;

/// How the counter reports its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CounterType {
    /// EventCounter/PollingCounter: statistics over the interval; the value is `mean`.
    Mean,
    /// IncrementingEventCounter/IncrementingPollingCounter: delta over the interval; the
    /// value is `increment / interval_sec`.
    Sum,
}

/// Reusable `String` buffers for decoded counter fields. Renting avoids an allocation
/// per field per event; buffers return to the pool automatically when the [`EcString`]
/// drops, on any thread. The pool is a bounded lock-free queue (crossbeam `ArrayQueue`):
/// one per collector is enough, and [`POOL_CAPACITY`] caps how much a slow consumer can
/// pin.
#[derive(Debug, Clone)]
pub struct EcStringPool {
    free: Arc<ArrayQueue<String>>,
}

impl EcStringPool {
    pub fn new() -> Self {
        Self {
            free: Arc::new(ArrayQueue::new(POOL_CAPACITY)),
        }
    }

    /// A buffer for one decoded field; allocates only while the pool is cold.
    pub(crate) fn rent(&self) -> EcString {
        let inner = self.free.pop().unwrap_or_default();
        EcString {
            inner: Some(inner),
            pool: Arc::clone(&self.free),
        }
    }
}

impl Default for EcStringPool {
    fn default() -> Self {
        Self::new()
    }
}

/// A `String` buffer rented from an [`EcStringPool`]; returns to the pool on drop.
/// Compares and displays as its text. `clone` allocates like a normal `String` and the
/// copy is pool-backed too.
#[derive(Clone)]
pub struct EcString {
    inner: Option<String>,
    pool: Arc<ArrayQueue<String>>,
}

impl EcString {
    /// The decoded text.
    pub fn as_str(&self) -> &str {
        self.inner.as_deref().unwrap_or_default()
    }

    /// Overwrites the text (test helper for building expected events).
    #[cfg(test)]
    pub(crate) fn set_str(&mut self, text: &str) {
        let inner = self.inner.get_or_insert_with(String::new);
        inner.clear();
        inner.push_str(text);
    }
}

impl Drop for EcString {
    fn drop(&mut self) {
        if let Some(buf) = self.inner.take() {
            // A full pool hands the buffer back to the allocator; the cap stands.
            let _ = self.pool.push(buf);
        }
    }
}

impl AsRef<str> for EcString {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq for EcString {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<&str> for EcString {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl Eq for EcString {}

impl fmt::Display for EcString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for EcString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("EcString").field(&self.as_str()).finish()
    }
}

/// One `System.Runtime` counter sample. The strings are rented from an
/// [`EcStringPool`] and return there when the event is dropped.
#[derive(Debug, Clone, PartialEq)]
pub struct EcEvent {
    /// The runtime's counter name, e.g. `gc-heap-size`.
    pub name: EcString,
    /// Which value field is set: `Mean` → `mean`, `Sum` → `increment`.
    pub counter_type: CounterType,
    /// The reporting interval as measured by the runtime (can jitter around 1 s).
    pub interval_sec: f32,
    /// CounterType::Mean: the interval mean.
    pub mean: Option<f64>,
    /// CounterType::Sum: the interval delta.
    pub increment: Option<f64>,
    /// Display units from the runtime, e.g. `MB`, `B`, `%` (empty if none). What the
    /// value is scaled in; see `RuntimeEventSource` (MB here is 1,000,000 bytes).
    pub display_units: EcString,
}

/// Decodes one parsed event; non-`System.Runtime`/non-`EventCounters` events are
/// [`Decoded::Other`]. `pool` provides the strings of the decoded sample.
pub fn decode_counter(event: &Event<'_>, pool: &EcStringPool) -> Decoded<EcEvent> {
    let metadata = event.metadata;
    if metadata.provider != EVENT_COUNTERS_PROVIDER || metadata.name != EVENT_COUNTERS_NAME {
        return Decoded::Other;
    }
    decode_payload(event.payload, pool)
}

fn decode_payload(payload: &[u8], pool: &EcStringPool) -> Decoded<EcEvent> {
    if let Some(ec) = parse(payload, CounterType::Mean, pool) {
        return Decoded::Value(ec);
    }
    if let Some(ec) = parse(payload, CounterType::Sum, pool) {
        return Decoded::Value(ec);
    }
    Decoded::Unreadable
}

/// Parses the payload as the layout of `counter_type`; `None` unless the `CounterType`
/// string matches and the cursor ends exactly at the payload end.
fn parse(payload: &[u8], counter_type: CounterType, pool: &EcStringPool) -> Option<EcEvent> {
    let expected_type_text = match counter_type {
        CounterType::Mean => "Mean",
        CounterType::Sum => "Sum",
    };
    let mut name = pool.rent();
    let mut display_units = pool.rent();
    let mut cursor = Cursor::new(payload);
    cursor.utf16z_into(&mut name)?;
    cursor.utf16z_skip()?; // display name
    let (mean, increment) = match counter_type {
        CounterType::Mean => {
            let mean = cursor.f64()?;
            let _std_dev = cursor.f64()?;
            let _count = cursor.u32()?;
            let _min = cursor.f64()?;
            let _max = cursor.f64()?;
            (Some(mean), None)
        }
        CounterType::Sum => {
            cursor.utf16z_skip()?; // display rate time scale
            let increment = cursor.f64()?;
            (None, Some(increment))
        }
    };
    let interval_sec = cursor.f32()?;
    // Field order around Metadata/Series differs between the two layouts.
    let type_matches = match counter_type {
        CounterType::Mean => {
            cursor.utf16z_skip()?; // series
            let matches = cursor.utf16z_is(expected_type_text)?;
            cursor.utf16z_skip()?; // metadata
            matches
        }
        CounterType::Sum => {
            cursor.utf16z_skip()?; // metadata
            cursor.utf16z_skip()?; // series
            cursor.utf16z_is(expected_type_text)?
        }
    };
    cursor.utf16z_into(&mut display_units)?;
    if !type_matches || !cursor.is_done() {
        return None;
    }
    Some(EcEvent {
        name,
        counter_type,
        interval_sec,
        mean,
        increment,
        display_units,
    })
}

/// Little-endian cursor; `None` past the end or at a broken string.
struct Cursor<'a> {
    payload: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, pos: 0 }
    }

    fn is_done(&self) -> bool {
        self.pos == self.payload.len()
    }

    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(n)?;
        let bytes = self.payload.get(self.pos..end)?;
        self.pos = end;
        Some(bytes)
    }

    fn u16(&mut self) -> Option<u16> {
        Some(u16::from_le_bytes(self.take(2)?.try_into().ok()?))
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn f32(&mut self) -> Option<f32> {
        Some(f32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn f64(&mut self) -> Option<f64> {
        Some(f64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    /// The content bytes of the next null-terminated UTF-16 field (without the
    /// terminator); the read position moves past it.
    fn utf16z_span(&mut self) -> Option<&'a [u8]> {
        let start = self.pos;
        while self.u16()? != 0 {}
        Some(&self.payload[start..self.pos - 2])
    }

    /// Reads a null-terminated UTF-16 field, decoding it into `out` (lossy on lone
    /// surrogates, like `String::from_utf16_lossy`, so a weird string never rejects the
    /// whole sample).
    fn utf16z_into(&mut self, out: &mut EcString) -> Option<()> {
        let span = self.utf16z_span()?;
        let units = span
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]));
        let inner = out.inner.get_or_insert_with(String::new);
        inner.clear();
        for decoded in char::decode_utf16(units) {
            inner.push(decoded.unwrap_or('\u{FFFD}'));
        }
        Some(())
    }

    /// Moves past a null-terminated UTF-16 field without allocating.
    fn utf16z_skip(&mut self) -> Option<()> {
        self.utf16z_span().map(|_| ())
    }

    /// Reads a null-terminated UTF-16 field and reports whether it equals `text`,
    /// without allocating.
    fn utf16z_is(&mut self, text: &str) -> Option<bool> {
        let span = self.utf16z_span()?;
        let mut units = span
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]));
        Some(text.encode_utf16().eq(&mut units))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).expect("hex"))
            .collect()
    }

    /// Payload hex from steady-gc-counters-net10-windows-x64.expected.txt (line 87).
    const GC_HEAP_SIZE: &str = "670063002d0068006500610070002d00730069007a006500000047004300200048006500610070002000530069007a006500000052b7b3af3cf0514000000000000000000100000052b7b3af3cf0514052b7b3af3cf051400389813f49006e00740065007200760061006c003d00310030003000300000004d00650061006e00000000004d0042000000";

    /// Payload hex from the same file (line 88).
    const GEN_0_GC_COUNT: &str = "670065006e002d0030002d00670063002d0063006f0075006e0074000000470065006e0020003000200047004300200043006f0075006e0074000000300030003a00300031003a0030003000000000000000000010400389813f000049006e00740065007200760061006c003d0031003000300030000000530075006d0000000000";

    /// Payload hex from the same file (line 92); empty DisplayUnits.
    const THREADPOOL_QUEUE_LENGTH: &str = "74006800720065006100640070006f006f006c002d00710075006500750065002d006c0065006e00670074006800000054006800720065006100640050006f006f006c0020005100750065007500650020004c0065006e0067007400680000000000000000000000000000000000000001000000000000000000000000000000000000000389813f49006e00740065007200760061006c003d00310030003000300000004d00650061006e00000000000000";

    /// Payload hex from the same file (line 86); a fractional MB mean.
    const WORKING_SET: &str = "77006f0072006b0069006e0067002d00730065007400000057006f0072006b0069006e00670020005300650074000000014eefe2fdb85d40000000000000000001000000014eefe2fdb85d40014eefe2fdb85d400389813f49006e00740065007200760061006c003d00310030003000300000004d00650061006e00000000004d0042000000";

    const INTERVAL_SEC: f32 = 1.0119938;

    #[test]
    fn decodes_mean_counters_from_the_steady_recording() {
        let pool = EcStringPool::new();
        let Decoded::Value(ec) = decode_payload(&hex(GC_HEAP_SIZE), &pool) else {
            panic!("not a counter");
        };
        assert_eq!(ec.name, "gc-heap-size");
        assert_eq!(ec.counter_type, CounterType::Mean);
        assert_eq!(ec.interval_sec, INTERVAL_SEC);
        assert!((ec.mean.expect("mean") - 71.753_704).abs() < 1e-9);
        assert_eq!(ec.increment, None);
        assert_eq!(ec.display_units, "MB");

        let Decoded::Value(ec) = decode_payload(&hex(THREADPOOL_QUEUE_LENGTH), &pool) else {
            panic!("not a counter");
        };
        assert_eq!(ec.name, "threadpool-queue-length");
        assert_eq!(ec.display_units, "");
        assert_eq!(ec.mean, Some(0.0));
    }

    #[test]
    fn decodes_incrementing_counters_from_the_steady_recording() {
        let pool = EcStringPool::new();
        let Decoded::Value(ec) = decode_payload(&hex(GEN_0_GC_COUNT), &pool) else {
            panic!("not a counter");
        };
        assert_eq!(ec.name, "gen-0-gc-count");
        assert_eq!(ec.counter_type, CounterType::Sum);
        assert_eq!(ec.interval_sec, INTERVAL_SEC);
        assert_eq!(ec.increment, Some(4.0));
        assert_eq!(ec.mean, None);
        assert_eq!(ec.display_units, "");
    }

    #[test]
    fn layouts_are_told_apart_not_misread() {
        let pool = EcStringPool::new();
        // A Sum payload does not parse as Mean and vice versa.
        assert_eq!(
            decode_payload(&hex(GEN_0_GC_COUNT), &pool),
            Decoded::Value(EcEvent {
                name: pooled(&pool, "gen-0-gc-count"),
                counter_type: CounterType::Sum,
                interval_sec: INTERVAL_SEC,
                mean: None,
                increment: Some(4.0),
                display_units: pooled(&pool, ""),
            })
        );
        // Swapped CounterType text rejects the layout.
        let mut wrong_type = hex(GC_HEAP_SIZE);
        let pos = GC_HEAP_SIZE.find("4d00650061006e00").expect("Mean text") / 2;
        wrong_type[pos] = b'X';
        assert_eq!(decode_payload(&wrong_type, &pool), Decoded::Unreadable);
    }

    #[test]
    fn trailing_garbage_and_truncation_are_unreadable() {
        let pool = EcStringPool::new();
        let mut trailing = hex(THREADPOOL_QUEUE_LENGTH);
        trailing.extend_from_slice(&[0xAB; 4]);
        assert_eq!(decode_payload(&trailing, &pool), Decoded::Unreadable);

        for cut in [0, 20, 60, 100] {
            assert_eq!(
                decode_payload(&hex(GC_HEAP_SIZE)[..cut], &pool),
                Decoded::Unreadable,
                "cut at {cut}"
            );
        }
    }

    #[test]
    fn fractional_mb_means_keep_full_precision() {
        // dotnet-counters shows "Working Set (MB) 118.890496"; the catalogue wants bytes.
        let pool = EcStringPool::new();
        let Decoded::Value(ec) = decode_payload(&hex(WORKING_SET), &pool) else {
            panic!("not a counter");
        };
        assert!((ec.mean.expect("mean") - 118.890_496).abs() < 1e-9);
        assert_eq!(ec.display_units, "MB");
    }

    #[test]
    fn strings_return_to_the_pool_and_are_reused() {
        let pool = EcStringPool::new();
        {
            let Decoded::Value(ec) = decode_payload(&hex(GC_HEAP_SIZE), &pool) else {
                panic!("not a counter");
            };
            assert_eq!(ec.name, "gc-heap-size");
        }
        // Both strings returned on drop…
        assert_eq!(pool.free.len(), 2, "both strings returned to the pool");

        // …and the next decode rents them again instead of allocating.
        let Decoded::Value(ec) = decode_payload(&hex(WORKING_SET), &pool) else {
            panic!("not a counter");
        };
        assert_eq!(ec.name, "working-set");
        assert_eq!(pool.free.len(), 0, "both buffers rented again");
    }

    #[test]
    fn a_full_pool_hands_surplus_buffers_to_the_allocator() {
        let pool = EcStringPool::new();
        let mut held = Vec::new();
        for _ in 0..POOL_CAPACITY / 2 {
            held.push(decode_payload(&hex(GC_HEAP_SIZE), &pool));
        }
        assert_eq!(pool.free.len(), 0, "all buffers rented");

        drop(held);
        assert_eq!(pool.free.len(), POOL_CAPACITY, "pool filled to its cap");

        // One buffer beyond the cap is not retained: back to the allocator.
        let one = pooled(&pool, "x"); // rent: cap - 1
        let clone = one.clone(); // a fresh buffer, never rented from the pool
        drop(one); // returns: cap
        drop(clone); // full: handed to the allocator
        assert_eq!(pool.free.len(), POOL_CAPACITY, "cap holds");
    }

    fn pooled(pool: &EcStringPool, text: &str) -> EcString {
        let mut s = pool.rent();
        s.set_str(text);
        s
    }
}
