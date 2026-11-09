
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Decoded<TValue> {
    /// Some other event; not counted.
    Other,
    Value(TValue),
    /// A GC event we use, in a version or size we can't read.
    Unreadable,
}
