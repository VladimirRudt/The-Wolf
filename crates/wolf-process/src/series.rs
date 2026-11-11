//! Maps [`ProcessSample`]s to the contract points of the process metric catalogue
//! (`docs/collector-dotnet.md` §5). The only place those names are written. Mirrors
//! `wolf_dotnet::EcSeries`: built once per target at attach, then one call per sample.

use wolf_contract::{Point, SeriesDescriptor, SeriesId, TargetId, Timestamp, Unit};

use crate::sample::ProcessSample;

const RSS: &str = "process.memory.rss";
const PRIVATE: &str = "process.memory.private";
const VIRTUAL: &str = "process.memory.virtual";
const LIMIT: &str = "process.memory.limit";
const THREADS: &str = "process.memory.threads";

/// The five process series of one target, created once at attach.
#[derive(Debug, Clone)]
pub struct ProcessSeries {
    descriptors: Vec<SeriesDescriptor>,
    ids: ProcessSeriesIds,
}

#[derive(Debug, Clone, Copy)]
struct ProcessSeriesIds {
    rss: SeriesId,
    private: SeriesId,
    virtual_memory: SeriesId,
    limit: SeriesId,
    threads: SeriesId,
}

impl ProcessSeries {
    pub fn new(target: TargetId) -> Self {
        let descriptor = |metric: &str, unit: Unit| {
            SeriesDescriptor::new(target.clone(), metric, Default::default(), unit)
        };
        let descriptors = [
            descriptor(RSS, Unit::Bytes),
            descriptor(PRIVATE, Unit::Bytes),
            descriptor(VIRTUAL, Unit::Bytes),
            descriptor(LIMIT, Unit::Bytes),
            descriptor(THREADS, Unit::Count),
        ];
        let ids = ProcessSeriesIds {
            rss: descriptors[0].id(),
            private: descriptors[1].id(),
            virtual_memory: descriptors[2].id(),
            limit: descriptors[3].id(),
            threads: descriptors[4].id(),
        };
        Self {
            descriptors: descriptors.into(),
            ids,
        }
    }

    /// Descriptors to register with storage before the first point.
    pub fn descriptors(&self) -> &[SeriesDescriptor] {
        &self.descriptors
    }

    /// The catalogue points for one sample: rss/private/virtual always, plus the limit
    /// and thread count when this tick has them (Windows fills threads only via
    /// [`crate::thread_count`]).
    pub fn points(&self, ts: Timestamp, sample: &ProcessSample) -> Vec<Point> {
        let point = |series, value| Point {
            series,
            ts,
            value: value as f64,
        };
        let mut points = vec![
            point(self.ids.rss, sample.rss),
            point(self.ids.private, sample.private),
            point(self.ids.virtual_memory, sample.virtual_memory),
        ];
        if let Some(limit) = sample.limit {
            points.push(point(self.ids.limit, limit));
        }
        if let Some(threads) = sample.threads {
            points.push(point(self.ids.threads, u64::from(threads)));
        }
        points
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> TargetId {
        "pid:4812.1760843645@srv-a".parse().expect("target")
    }

    #[test]
    fn the_five_catalogue_series_are_declared() {
        let series = ProcessSeries::new(target());
        let declared: Vec<_> = series
            .descriptors()
            .iter()
            .map(|d| (d.metric(), d.unit()))
            .collect();
        assert_eq!(
            declared,
            [
                (RSS, &Unit::Bytes),
                (PRIVATE, &Unit::Bytes),
                (VIRTUAL, &Unit::Bytes),
                (LIMIT, &Unit::Bytes),
                (THREADS, &Unit::Count),
            ]
        );
    }

    #[test]
    fn samples_become_points_limit_and_threads_only_when_present() {
        let series = ProcessSeries::new(target());
        let ts = Timestamp::from_nanos(1_760_843_645_000_000_000);
        let sample = ProcessSample {
            rss: 100,
            private: 80,
            virtual_memory: 200,
            limit: None,
            threads: None,
        };

        let points = series.points(ts, &sample);
        assert_eq!(points.len(), 3, "only the memory fields");
        assert_eq!(points[0].value, 100.0);
        assert_eq!(points[0].series, series.descriptors()[0].id());

        let full = ProcessSample {
            limit: Some(1_073_741_824),
            threads: Some(37),
            ..sample
        };
        let points = series.points(ts, &full);
        assert_eq!(points.len(), 5);
        assert_eq!(points[3].value, 1_073_741_824.0);
        assert_eq!(points[3].series, series.descriptors()[3].id());
        assert_eq!(points[4].value, 37.0);
        assert_eq!(points[4].series, series.descriptors()[4].id());
        assert!(points.iter().all(|p| p.ts == ts));
    }
}
