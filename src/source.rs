//! Ingress: where events come from, and how they are acknowledged.
//!
//! ```text
//! data Envelope a = Envelope { event :: Event, ack :: a }
//!
//! class Ack a where
//!   ack :: a -> IO (Either AckError ())        -- consumes the handle
//!
//! class Source s where
//!   type Ack s
//!   next :: s -> IO (Either SourceError (Maybe (Envelope (Ack s))))
//! ```
//!
//! The bus acknowledges an envelope only after the event's final ledger row
//! (`Settled`, or `Failed` plus its dead-letter row) has been written. An
//! envelope dropped before that, for example by a crash, is never
//! acknowledged, so the source may redeliver it: the delivery guarantee is
//! at least once. `ack` takes the handle by value, so acknowledging twice
//! does not compile.
//!
//! This module holds the traits only. There are no broker implementations
//! in this crate; [`NoAck`] serves plain streams that need no acknowledgement.

use async_trait::async_trait;

use crate::event::Event;

/// Why an acknowledgement could not be delivered to the source.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AckError {
    /// The source could not be reached.
    #[error("source unavailable for ack: {reason}")]
    Unavailable {
        /// Detail from the source.
        reason: String,
    },
    /// The source no longer recognises this delivery, for example after a
    /// rebalance or a timeout; it may already have redelivered the event.
    #[error("ack rejected: {reason}")]
    Rejected {
        /// Detail from the source.
        reason: String,
    },
}

/// Why a source could not produce its next envelope.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SourceError {
    /// The source could not be reached. Reconnecting is the source's job;
    /// surfacing this stops the run.
    #[error("source unavailable: {reason}")]
    Unavailable {
        /// Detail from the source.
        reason: String,
    },
    /// The source produced something that is not an event.
    #[error("source delivered a malformed event: {reason}")]
    Malformed {
        /// What was wrong.
        reason: String,
    },
}

/// A consume-once acknowledgement handle.
#[async_trait]
pub trait Ack: Send {
    /// Tells the source the event has been fully handled.
    async fn ack(self) -> Result<(), AckError>;
}

/// The acknowledgement for sources that need none.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoAck;

#[async_trait]
impl Ack for NoAck {
    async fn ack(self) -> Result<(), AckError> {
        Ok(())
    }
}

/// An event with the handle that acknowledges it.
#[derive(Debug)]
pub struct Envelope<A> {
    event: Event,
    ack: A,
}

impl<A: Ack> Envelope<A> {
    /// Pairs an event with its acknowledgement.
    pub fn new(event: Event, ack: A) -> Self {
        Envelope { event, ack }
    }

    /// The event.
    pub fn event(&self) -> &Event {
        &self.event
    }

    /// Splits the envelope. The caller now owns the obligation to ack.
    pub fn into_parts(self) -> (Event, A) {
        (self.event, self.ack)
    }
}

impl Envelope<NoAck> {
    /// An event that needs no acknowledgement.
    pub fn unacked(event: Event) -> Self {
        Envelope { event, ack: NoAck }
    }
}

/// A pull-based, fallible supplier of envelopes.
///
/// `next` returns `Ok(None)` when the source is exhausted. Implementations
/// own reconnection and redelivery; the bus only pulls and acks.
#[async_trait]
pub trait Source: Send {
    /// The acknowledgement handle this source hands out.
    type Ack: Ack;

    /// The next envelope, or `None` at the end.
    async fn next(&mut self) -> Result<Option<Envelope<Self::Ack>>, SourceError>;
}
