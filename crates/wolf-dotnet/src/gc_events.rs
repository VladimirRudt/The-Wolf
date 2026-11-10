//! Payloads of the runtime GC events we use (`ClrEtwAll.man`; every runtime from .NET 6 on
//! fires the same versions: `gcevents.h`). Newer versions only append fields, so a longer
//! payload is fine; an older version or a short payload is skipped and counted.

use wolf_nettrace::Event;

use crate::events_decoding::Decoded;
use crate::gc::{GcKind, GcReason, HeapStats};

const RUNTIME_PROVIDER: &str = "Microsoft-Windows-DotNETRuntime";

/// `GCSuspendEEBegin.Reason` of a background GC's second pause, whose `Count` is its GC number.
pub(crate) const SUSPEND_FOR_GC_PREP: u32 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GcEvent {
    /// `GCSuspendEEBegin`: the app is being stopped.
    SuspendBegin {
        reason: u32,
        count: u32,
    },
    /// `GCSuspendEEEnd`: the app is stopped.
    SuspendEnd,
    Start {
        number: u32,
        generation: u32,
        reason: GcReason,
        kind: GcKind,
    },
    /// `GCGlobalHeapHistory`: how the GC was done. Carries no GC number; it comes right
    /// before the GC's `GCEnd` on the same thread.
    GlobalHeapHistory {
        compacting: bool,
    },
    End {
        number: u32,
    },
    HeapStats(HeapStats),
    /// `GCRestartEEEnd`: the app runs again.
    RestartEnd,
}

pub(crate) fn decode_gc_event(event: &Event<'_>) -> Decoded<GcEvent> {
    let metadata = event.metadata;
    if metadata.provider != RUNTIME_PROVIDER {
        return Decoded::Other;
    }
    decode_payload(metadata.event_id, metadata.version, event.payload)
}

fn decode_payload(event_id: u32, version: u32, payload: &[u8]) -> Decoded<GcEvent> {
    let payload_object = Payload(payload);
    let decoded = match event_id {
        // GCSuspendEEBegin_V1: Reason, Count.
        9 if version >= 1 => payload_object
            .u32(0)
            .zip(payload_object.u32(4))
            .map(|(reason, count)| GcEvent::SuspendBegin { reason, count }),
        8 => Some(GcEvent::SuspendEnd),
        // GCStart_V2: Count, Depth, Reason, Type, ClrInstanceID, ClientSequenceNumber.
        1 if version >= 2 => (|| {
            Some(GcEvent::Start {
                number: payload_object.u32(0)?,
                generation: payload_object.u32(4)?,
                reason: GcReason(payload_object.u32(8)?),
                kind: GcKind::from_raw(payload_object.u32(12)?),
            })
        })(),
        // GCGlobalHeapHistory_V4: GlobalMechanisms at 24 (bit 0 = concurrent, bit 1 =
        // compaction). Its `Count` (at 46) is the length of the `Times` array, not the GC number.
        205 if version >= 4 => {
            payload_object
                .u32(24)
                .map(|mechanisms| GcEvent::GlobalHeapHistory {
                    compacting: mechanisms & 0b10 != 0,
                })
        }
        // GCEnd_V1: Count, Depth.
        2 if version >= 1 => payload_object.u32(0).map(|number| GcEvent::End { number }),
        4 if version >= 2 => heap_stats(payload_object).map(GcEvent::HeapStats),
        3 => Some(GcEvent::RestartEnd),
        9 | 1 | 205 | 2 | 4 => None,
        _ => return Decoded::Other,
    };
    decoded.map_or(Decoded::Unreadable, |gc| Decoded::Value(gc))
}

/// `GCHeapStats_V2`: size and promoted bytes for gen0, gen1, gen2, LOH (`GenerationSize3`),
/// then finalization, pinned and handle counts, and POH (`GenerationSize4`) at the end.
fn heap_stats(p: Payload<'_>) -> Option<HeapStats> {
    Some(HeapStats {
        size: [p.u64(0)?, p.u64(16)?, p.u64(32)?, p.u64(48)?, p.u64(94)?],
        promoted: [p.u64(8)?, p.u64(24)?, p.u64(40)?, p.u64(56)?, p.u64(102)?],
        finalization_promoted_count: p.u64(72)?,
        pinned_objects: p.u32(80)?,
        gc_handles: p.u32(88)?,
    })
}

/// Little-endian fields at fixed offsets; `None` past the end.
#[derive(Clone, Copy)]
struct Payload<'a>(&'a [u8]);

impl Payload<'_> {
    fn u32(self, offset: usize) -> Option<u32> {
        let bytes = self.0.get(offset..offset + 4)?;
        Some(u32::from_le_bytes(bytes.try_into().ok()?))
    }

    fn u64(self, offset: usize) -> Option<u64> {
        let bytes = self.0.get(offset..offset + 8)?;
        Some(u64::from_le_bytes(bytes.try_into().ok()?))
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

    #[test]
    fn decodes_payloads_from_the_steady_recording() {
        // GC #40 of steady-gc-counters-net10-windows-x64 (payload hex from its .expected.txt).
        assert_eq!(
            decode_payload(9, 1, &hex("01000000270000000a00")),
            Decoded::Value(GcEvent::SuspendBegin {
                reason: 1, // SuspendForGC
                count: 39
            })
        );
        assert_eq!(
            decode_payload(1, 2, &hex("28000000010000000000000000000000")),
            Decoded::Value(GcEvent::Start {
                number: 40,
                generation: 1,
                reason: GcReason(0),
                kind: GcKind::Blocking
            })
        );
        assert_eq!(
            decode_payload(2, 1, &hex("28000000010000000a00")),
            Decoded::Value(GcEvent::End { number: 40 })
        );
        assert_eq!(
            decode_payload(3, 1, &hex("0a00")),
            Decoded::Value(GcEvent::RestartEnd)
        );
    }

    #[test]
    fn decodes_heap_stats_field_positions() {
        let mut payload = Vec::new();
        for value in 1..=10u64 {
            payload.extend_from_slice(&value.to_le_bytes()); // sizes, promoted, finalization
        }
        for value in [11u32, 12, 13] {
            payload.extend_from_slice(&value.to_le_bytes()); // pinned, sink blocks, handles
        }
        payload.extend_from_slice(&[0, 0]); // ClrInstanceID
        payload.extend_from_slice(&14u64.to_le_bytes()); // POH size
        payload.extend_from_slice(&15u64.to_le_bytes()); // POH promoted
        assert_eq!(payload.len(), 110);

        let Decoded::Value(GcEvent::HeapStats(stats)) = decode_payload(4, 2, &payload) else {
            panic!("not heap stats");
        };
        assert_eq!(stats.size, [1, 3, 5, 7, 14]);
        assert_eq!(stats.promoted, [2, 4, 6, 8, 15]);
        assert_eq!(stats.finalization_promoted_count, 10);
        assert_eq!((stats.pinned_objects, stats.gc_handles), (11, 13));
    }

    #[test]
    fn older_versions_and_short_payloads_are_unreadable() {
        assert_eq!(decode_payload(1, 1, &[0; 16]), Decoded::Unreadable);
        assert_eq!(decode_payload(4, 2, &[0; 100]), Decoded::Unreadable);
        assert_eq!(decode_payload(205, 4, &[0; 20]), Decoded::Unreadable);
        assert_eq!(decode_payload(39, 0, &[]), Decoded::Other);
    }

    #[test]
    fn longer_payloads_from_newer_versions_are_read() {
        let mut payload = hex("28000000010000000000000000000000");
        payload.extend_from_slice(&[0xFF; 20]);
        assert!(matches!(
            decode_payload(1, 3, &payload),
            Decoded::Value(GcEvent::Start { number: 40, .. })
        ));
    }
}
