use wolf_nettrace::Event;

const EVENT_COUNTER_PROVIDE: &str = "System.Runtime";

/// A value from the EventCounters payload dictionary. Values can be
/// numbers or strings depending on the key.
#[derive(Debug, Clone, PartialEq)]
pub enum PayloadValue {
    Float(f64),
    Int(i64),
    Str(String),
}

/// How the counter reports its value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CounterType {
    /// EventCounter: aggregates over the interval (mean/min/max/...).
    Mean,
    /// Incrementing counter: delta over the interval.
    Sum,
    /// Polling counter reported as a rate.
    Rate,
}

#[derive(Debug, Copy)]
pub struct EcEvent {
    pub name: String,
    pub display_name: Option<String>,
    pub display_units: Option<String>,
    pub counter_type: CounterType,
    pub interval_sec: f64,

    // Aggregates (EventCounter / mean type)
    pub mean: Option<f64>,
    pub std_dev: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub count: Option<u64>,

    // Incrementing counters
    pub increment: Option<f64>,

    pub metadata: Option<String>,
}

pub(crate) fn decode_gc_event(event: &Event<'_>) -> Decoded<EcEvent> {
    let metadata = event.metadata;
    if metadata.provider != EVENT_COUNTER_PROVIDE {
        return Decoded::Other;
    }
    decode_payload(metadata.event_id, metadata.version, event.payload)
}
