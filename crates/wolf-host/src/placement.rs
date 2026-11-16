//! Target → shard placement (D14). Fixed logical shards with a stable hash — Kafka-style
//! over-partitioning: the logical bucket of a target never changes, so adding physical
//! shards (or hosts, later) remaps whole buckets instead of reshuffling data. Old shard
//! files stay readable: the query layer unions everything it finds.

use wolf_contract::{Fnv1a, TargetId};

/// Fixed logical shard count. Big enough that a single host never needs to reshard;
/// small enough that idle buckets cost nothing (shard directories are created lazily, on
/// first target that lands in them).
pub const LOGICAL_SHARDS: u16 = 256;

/// Maps targets to this host's physical shards (pipelines).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placement {
    physical: u16,
}

impl Placement {
    /// `physical == 0` is clamped to 1: every host runs at least one pipeline.
    pub fn new(physical: u16) -> Self {
        Self {
            physical: physical.max(1),
        }
    }

    pub fn physical(self) -> u16 {
        self.physical
    }

    /// The physical shard owning `target`: `FNV-1a(target) % 256 % physical`. Stable
    /// across runs, so a restarted host places a re-attached target on the same shard as
    /// long as the shard count is unchanged.
    pub fn shard_of(&self, target: &TargetId) -> u16 {
        let mut hash = Fnv1a::new();
        hash.write(target.to_string().as_bytes());
        let logical = u16::try_from(hash.finish() % u64::from(LOGICAL_SHARDS)).unwrap_or(0);
        logical % self.physical
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(n: u32) -> TargetId {
        TargetId::new(4812 + n, 1_760_843_645, "srv-a").expect("target")
    }

    #[test]
    fn placement_is_stable_and_in_range() {
        for physical in [1, 2, 4, 7] {
            let placement = Placement::new(physical);
            for n in 0..500 {
                let shard = placement.shard_of(&target(n));
                assert!(shard < physical);
                assert_eq!(shard, placement.shard_of(&target(n)), "deterministic");
            }
        }
    }

    #[test]
    fn targets_spread_over_all_shards() {
        let placement = Placement::new(4);
        let mut used = [false; 4];
        for n in 0..500 {
            used[placement.shard_of(&target(n)) as usize] = true;
        }
        assert!(used.iter().all(|used| *used), "every shard sees targets");
    }

    #[test]
    fn zero_physical_means_one_shard() {
        let placement = Placement::new(0);
        assert_eq!(placement.physical(), 1);
        assert_eq!(placement.shard_of(&target(1)), 0);
    }
}
