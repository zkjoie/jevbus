//! Deliveries and the subscriber streams that carry them.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::channel::mpsc;
use futures::Stream;

use crate::event::Event;
use crate::ledger::JudgeFailure;
use crate::lifecycle::Attempt;
use crate::routing::Verdict;

/// An event handed to one subscription, with the verdict that sent it there.
///
/// The event is shared: one published event fans out to every subscriber that
/// matched it, so the `Arc` is the fan-out itself, not a borrow-checker escape.
#[derive(Debug, Clone)]
pub struct Delivery {
    event: Arc<Event>,
    verdict: Verdict,
}

impl Delivery {
    pub(crate) fn new(event: Arc<Event>, verdict: Verdict) -> Self {
        Delivery { event, verdict }
    }

    /// The delivered event.
    pub fn event(&self) -> &Event {
        &self.event
    }

    /// Why it was delivered here.
    pub fn verdict(&self) -> &Verdict {
        &self.verdict
    }

    /// Splits the delivery into its parts.
    pub fn into_parts(self) -> (Arc<Event>, Verdict) {
        (self.event, self.verdict)
    }
}

/// An event the bus gave up on, with the reason.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    event: Arc<Event>,
    attempts: Attempt,
    cause: JudgeFailure,
}

impl DeadLetter {
    pub(crate) fn new(event: Arc<Event>, attempts: Attempt, cause: JudgeFailure) -> Self {
        DeadLetter {
            event,
            attempts,
            cause,
        }
    }

    /// The event that failed.
    pub fn event(&self) -> &Event {
        &self.event
    }

    /// Judge attempts consumed before giving up.
    pub fn attempts(&self) -> Attempt {
        self.attempts
    }

    /// The final cause.
    pub fn cause(&self) -> &JudgeFailure {
        &self.cause
    }

    /// Splits the dead letter into its parts, for replay.
    pub fn into_parts(self) -> (Arc<Event>, Attempt, JudgeFailure) {
        (self.event, self.attempts, self.cause)
    }
}

/// The consuming end of a bounded channel from the bus.
///
/// Dropping the receiver unsubscribes; later items are recorded in the
/// ledger as [`crate::Outcome::Unsubscribed`]. The channel is bounded, so a
/// slow consumer applies backpressure to the bus and, through it, to the
/// input event stream.
#[derive(Debug)]
pub struct Receiver<T> {
    inner: mpsc::Receiver<T>,
}

impl<T> Stream for Receiver<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// The consuming end of one subscription: a stream of deliveries.
pub type Subscriber = Receiver<Delivery>;

/// The stream of events the bus gave up on.
pub type DeadLetters = Receiver<DeadLetter>;

/// Bus-side handle for one receiver.
pub(crate) type Sender<T> = mpsc::Sender<T>;

/// Creates a bounded channel.
///
/// `capacity` is the number of items that may wait unread before the bus
/// blocks on this receiver.
pub(crate) fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let (sender, inner) = mpsc::channel(capacity);
    (sender, Receiver { inner })
}
