use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::TargetId;
use crate::fnv::Fnv1a;

/// UTC wall-clock time in nanoseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(i64);

impl Timestamp {
    pub const fn from_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    pub const fn as_nanos(self) -> i64 {
        self.0
    }

    /// Current UTC time. Saturates instead of failing for clocks outside the i64 range.
    pub fn now() -> Self {
        let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(after) => i64::try_from(after.as_nanos()).unwrap_or(i64::MAX),
            Err(before) => i64::try_from(before.duration().as_nanos()).map_or(i64::MIN, |n| -n),
        };
        Self(nanos)
    }
}

/// Unit of a series' values. Unknown units are kept as [`Unit::Other`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Unit {
    #[serde(rename = "bytes")]
    Bytes,
    #[serde(rename = "bytes/s")]
    BytesPerSecond,
    #[serde(rename = "percent")]
    Percent,
    #[serde(rename = "count")]
    Count,
    #[serde(rename = "ms")]
    Milliseconds,
    #[serde(untagged)]
    Other(String),
}

/// Stable id of a series: a hash of target, metric and labels (see [`SeriesDescriptor`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SeriesId(pub u64);

/// What a series measures. Sent once per series; [`Point`]s refer to it by [`SeriesId`].
///
/// The id is computed from target, metric and labels with a fixed hash (FNV-1a 64), so
/// every collector derives the same id independently and it is stable across releases.
/// The unit is not part of the identity.
///
/// Serialize-only: nothing reads descriptors from JSON yet. A deserializer must recompute
/// the id rather than trust the input.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SeriesDescriptor {
    id: SeriesId,
    target: TargetId,
    /// Metric name from the catalogue (`docs/collector-dotnet.md` §4), e.g. `dotnet.gc.heap.size`.
    metric: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    labels: BTreeMap<String, String>,
    unit: Unit,
}

impl SeriesDescriptor {
    pub fn new(
        target: TargetId,
        metric: impl Into<String>,
        labels: BTreeMap<String, String>,
        unit: Unit,
    ) -> Self {
        let metric = metric.into();
        let id = series_id(&target, &metric, &labels);
        Self {
            id,
            target,
            metric,
            labels,
            unit,
        }
    }

    pub fn id(&self) -> SeriesId {
        self.id
    }

    pub fn target(&self) -> &TargetId {
        &self.target
    }

    pub fn metric(&self) -> &str {
        &self.metric
    }

    pub fn labels(&self) -> &BTreeMap<String, String> {
        &self.labels
    }

    pub fn unit(&self) -> &Unit {
        &self.unit
    }
}

/// FNV-1a 64 over target, metric and sorted labels, each terminated by 0xFF
/// (never present in UTF-8), so `{"ab": "c"}` and `{"a": "bc"}` hash differently.
fn series_id(target: &TargetId, metric: &str, labels: &BTreeMap<String, String>) -> SeriesId {
    let mut hash = Fnv1a::new();
    let mut field = |bytes: &[u8]| {
        hash.write(bytes);
        hash.write(&[0xFF]);
    };
    field(target.to_string().as_bytes());
    field(metric.as_bytes());
    for (key, value) in labels {
        field(key.as_bytes());
        field(value.as_bytes());
    }
    SeriesId(hash.finish())
}

/// One reading of a series (`docs/architecture.md` §3.2). Plain numbers: no allocations.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Point {
    pub series: SeriesId,
    #[serde(rename = "ts_utc_ns")]
    pub ts: Timestamp,
    pub value: f64,
}

/// Something that happened, with structured details (`docs/architecture.md` §3.3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    #[serde(rename = "ts_utc_ns")]
    pub ts: Timestamp,
    pub target: TargetId,
    /// Event kind from the catalogue, e.g. `dotnet.gc`, `target.attached`.
    pub kind: String,
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub data: Map<String, Value>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn target() -> TargetId {
        "pid:4812.1760843645@srv-a".parse().expect("valid target")
    }

    fn gen2_heap_size() -> SeriesDescriptor {
        SeriesDescriptor::new(
            target(),
            "dotnet.gc.heap.size",
            BTreeMap::from([("generation".into(), "gen2".into())]),
            Unit::Bytes,
        )
    }

    #[test]
    fn series_id_is_stable_across_releases() {
        // Golden value: if this changes, stored series ids no longer match. Don't update it
        // without a migration plan.
        assert_eq!(gen2_heap_size().id(), SeriesId(14_822_194_904_077_002_167));
    }

    #[test]
    fn series_id_depends_on_identity_only() {
        let base = gen2_heap_size();
        let other_unit = SeriesDescriptor::new(
            target(),
            "dotnet.gc.heap.size",
            base.labels().clone(),
            Unit::Count,
        );
        assert_eq!(other_unit.id(), base.id());

        let other_label = SeriesDescriptor::new(
            target(),
            "dotnet.gc.heap.size",
            BTreeMap::from([("generation".into(), "gen1".into())]),
            Unit::Bytes,
        );
        assert_ne!(other_label.id(), base.id());
    }

    #[test]
    fn series_id_separates_fields() {
        let label = |k: &str, v: &str| BTreeMap::from([(k.to_owned(), v.to_owned())]);
        let a = SeriesDescriptor::new(target(), "m", label("ab", "c"), Unit::Count);
        let b = SeriesDescriptor::new(target(), "m", label("a", "bc"), Unit::Count);
        assert_ne!(a.id(), b.id());
    }

    #[test]
    fn descriptor_serializes_with_id() {
        let descriptor = gen2_heap_size();
        let value = serde_json::to_value(&descriptor).expect("serialize");
        assert_eq!(
            value,
            json!({
                "id": descriptor.id().0,
                "target": "pid:4812.1760843645@srv-a",
                "metric": "dotnet.gc.heap.size",
                "labels": { "generation": "gen2" },
                "unit": "bytes"
            })
        );
    }

    #[test]
    fn descriptor_without_labels_omits_them() {
        let descriptor =
            SeriesDescriptor::new(target(), "process.threads", BTreeMap::new(), Unit::Count);
        let value = serde_json::to_value(&descriptor).expect("serialize");
        assert!(value.get("labels").is_none());
    }

    #[test]
    fn point_is_three_numbers() {
        let point = Point {
            series: SeriesId(42),
            ts: Timestamp::from_nanos(1_760_843_645_123_456_789),
            value: 157_286_400.0,
        };
        let value = json!({ "series": 42, "ts_utc_ns": 1_760_843_645_123_456_789_i64, "value": 157_286_400.0 });
        assert_eq!(serde_json::to_value(point).expect("serialize"), value);
        assert_eq!(
            serde_json::from_value::<Point>(value).expect("deserialize"),
            point
        );
        assert_eq!(std::mem::size_of::<Point>(), 24);
    }

    #[test]
    fn event_matches_architecture_example() {
        let value = json!({
            "ts_utc_ns": 1_760_843_645_000_000_000_i64,
            "target": "pid:4812.1760843645@srv-a",
            "kind": "dotnet.gc",
            "data": { "generation": 2, "reason": "AllocSmall", "type": "background",
                      "pause_ms": 7.9, "compacting": false }
        });
        let event: Event = serde_json::from_value(value.clone()).expect("deserialize");
        assert_eq!(event.kind, "dotnet.gc");
        assert_eq!(event.data["pause_ms"], json!(7.9));
        assert_eq!(serde_json::to_value(&event).expect("serialize"), value);
    }

    #[test]
    fn units_round_trip_including_unknown() {
        for (unit, text) in [
            (Unit::Bytes, "bytes"),
            (Unit::BytesPerSecond, "bytes/s"),
            (Unit::Percent, "percent"),
            (Unit::Count, "count"),
            (Unit::Milliseconds, "ms"),
            (Unit::Other("furlongs".into()), "furlongs"),
        ] {
            assert_eq!(serde_json::to_value(&unit).expect("serialize"), json!(text));
            assert_eq!(
                serde_json::from_value::<Unit>(json!(text)).expect("deserialize"),
                unit
            );
        }
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let value = json!({
            "ts_utc_ns": 1, "target": "pid:1.1@h", "kind": "dotnet.gc", "added_in_v2": true
        });
        let event: Event = serde_json::from_value(value).expect("deserialize");
        assert!(event.data.is_empty());
    }

    #[test]
    fn now_is_after_2020() {
        assert!(Timestamp::now().as_nanos() > 1_577_836_800_000_000_000);
    }
}
