//! The decode result shared by the event families (`gc_events`, `ec_events`): a parsed
//! stream event is either one we use, another event, or one of ours we can't read.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decoded<TValue> {
    /// Some other event; not counted.
    Other,
    Value(TValue),
    /// An event we use, in a version or size we can't read.
    Unreadable,
}
