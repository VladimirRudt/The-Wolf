//! The message type every producer actor sends to the router, tagged with the target slot.

use wolf_contract::Timestamp;
use wolf_process::ProcessSample;

use crate::session::SessionItem;

/// One mailbox message: everything the router consumes.
#[derive(Debug)]
pub enum Input {
    /// From a session reader actor.
    Session(SessionItem),
    /// From the process-sampler actor: one OS memory reading of a target.
    Sample {
        ts: Timestamp,
        sample: ProcessSample,
    },
}

/// Mailbox payload: the target slot and the message. Moved by value through the ring —
/// no allocation (`EcEvent`'s strings are pool-rented and return on drop).
pub type Message = (u32, Input);
