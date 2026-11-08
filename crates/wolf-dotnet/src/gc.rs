//! What one completed GC looks like after correlation (`docs/collector-dotnet.md` §3.2).
//! Plain `Copy` values: they cross the session channel without allocating.

/// One GC: everything from its `GCStart`, `GCGlobalHeapHistory`, `GCEnd`, `GCHeapStats` and
/// its pauses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcCompleted {
    /// GC number (`GCStart.Count`), increasing per process.
    pub number: u32,
    /// Condemned generation: 0, 1 or 2.
    pub generation: u32,
    pub kind: GcKind,
    pub reason: GcReason,
    /// `GCStart` time, UTC ns.
    pub start_utc_ns: i64,
    /// `GCEnd` time, UTC ns.
    pub end_utc_ns: i64,
    /// Time the app was stopped for this GC: one pause, or two for a background GC.
    pub pause_ns: i64,
    /// `GCEnd − GCStart`; for a background GC much longer than the pause.
    pub duration_ns: i64,
    /// From `GCGlobalHeapHistory`; `None` if that event was missing.
    pub compacting: Option<bool>,
    pub heap: HeapStats,
}

/// `GCStart.Type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcKind {
    /// App stopped for the whole GC: every gen0/gen1, and full blocking gen2.
    Blocking,
    /// Gen2 with most work done while the app runs; two short pauses.
    Background,
    /// A gen0/gen1 GC that runs while a background GC is in progress.
    Foreground,
    /// A value newer than this collector.
    Unknown(u32),
}

impl GcKind {
    pub(crate) fn from_raw(raw: u32) -> Self {
        match raw {
            0 => Self::Blocking,
            1 => Self::Background,
            2 => Self::Foreground,
            other => Self::Unknown(other),
        }
    }

    pub fn raw(self) -> u32 {
        match self {
            Self::Blocking => 0,
            Self::Background => 1,
            Self::Foreground => 2,
            Self::Unknown(raw) => raw,
        }
    }

    /// Name used in `dotnet.gc` events; `None` for an unknown value.
    pub fn name(self) -> Option<&'static str> {
        match self {
            Self::Blocking => Some("blocking"),
            Self::Background => Some("background"),
            Self::Foreground => Some("foreground"),
            Self::Unknown(_) => None,
        }
    }
}

/// `GCStart.Reason`: why the GC was triggered (`gc_reason` in the runtime's `gc.h`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GcReason(pub u32);

impl GcReason {
    /// Names as in PerfView (TraceEvent `GCReason`) for 0–13, and from `gc.h` for the newer
    /// values; `None` for a value newer than this list.
    pub fn name(self) -> Option<&'static str> {
        const NAMES: [&str; 18] = [
            "AllocSmall",
            "Induced",
            "LowMemory",
            "Empty",
            "AllocLarge",
            "OutOfSpaceSOH",
            "OutOfSpaceLOH",
            "InducedNotForced",
            "Internal",
            "InducedLowMemory",
            "InducedCompacting",
            "LowMemoryHost",
            "PMFullGC",
            "LowMemoryHostBlocking",
            "BGCTuningSOH",
            "BGCTuningLOH",
            "BGCStepping",
            "InducedAggressive",
        ];
        NAMES.get(self.0 as usize).copied()
    }
}

/// `GCHeapStats`: the heap right after the GC. Sizes include fragmentation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HeapStats {
    /// Bytes per generation, in [`GENERATIONS`] order.
    pub size: [u64; 5],
    /// Bytes promoted out of each generation by this GC, in [`GENERATIONS`] order.
    pub promoted: [u64; 5],
    /// Objects kept alive only to be finalized.
    pub finalization_promoted_count: u64,
    pub pinned_objects: u32,
    pub gc_handles: u32,
}

/// Generation labels of the metric catalogue, in the order of [`HeapStats`] arrays.
pub const GENERATIONS: [&str; 5] = ["gen0", "gen1", "gen2", "loh", "poh"];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_match_the_runtime() {
        assert_eq!(GcReason(0).name(), Some("AllocSmall"));
        assert_eq!(GcReason(4).name(), Some("AllocLarge"));
        assert_eq!(GcReason(17).name(), Some("InducedAggressive"));
        assert_eq!(GcReason(18).name(), None);
        assert_eq!(GcKind::from_raw(1).name(), Some("background"));
        assert_eq!(GcKind::from_raw(9), GcKind::Unknown(9));
    }
}
