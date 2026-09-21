//! Chaining one bus into another, in process, with acknowledgements that
//! travel back up the chain.
//!
//! ```text
//! link :: Capacity -> (LinkSink, LinkSource)
//!
//! instance Sink Delivery LinkSink   -- upstream: publish waits for the downstream ack
//! instance Source LinkSource        -- downstream: next yields Envelope LinkAck
//! instance Ack LinkAck              -- resolves the upstream publish
//! ```
//!
//! The upstream bus registers the [`LinkSink`] with [`Bus::subscribe_to`];
//! the downstream bus runs the [`LinkSource`] with [`Bus::run_source`]. An
//! upstream `publish` completes only when the downstream has acknowledged
//! the event, which happens after the downstream's final ledger row. The
//! upstream therefore settles and acks its own source only once the whole
//! chain has handled the event: at-least-once holds end to end.
//!
//! If the downstream drops the event without acknowledging, the upstream
//! sees [`SinkError::Unavailable`] and records [`Outcome::SinkFailed`]; if
//! the downstream has gone away entirely, [`SinkError::Closed`] and
//! [`Outcome::Unsubscribed`]. Nothing is silent.
//!
//! Across processes, carry the serialised [`Delivery`] over any transport and
//! rebuild the same shape on the other side; the transport is not part of
//! this crate.
//!
//! [`Bus::subscribe_to`]: crate::Bus::subscribe_to
//! [`Bus::run_source`]: crate::Bus::run_source
//! [`Outcome::SinkFailed`]: crate::Outcome::SinkFailed
//! [`Outcome::Unsubscribed`]: crate::Outcome::Unsubscribed

use async_trait::async_trait;
use futures::channel::{mpsc, oneshot};
use futures::lock::Mutex;
use futures::{SinkExt, StreamExt};

use crate::delivery::Delivery;
use crate::event::Event;
use crate::sink::{Sink, SinkError};
use crate::source::{Ack, AckError, Envelope, Source, SourceError};

/// One event in flight across the link, with the channel that resolves the
/// upstream publish.
struct Handoff {
    event: Event,
    done: oneshot::Sender<Result<(), AckError>>,
}

/// The upstream end.
#[derive(Debug)]
pub struct LinkSink {
    sender: Mutex<mpsc::Sender<Handoff>>,
}

/// The downstream end.
#[derive(Debug)]
pub struct LinkSource {
    receiver: mpsc::Receiver<Handoff>,
}

/// The downstream's acknowledgement handle; consuming it releases the
/// upstream.
#[derive(Debug)]
pub struct LinkAck {
    done: oneshot::Sender<Result<(), AckError>>,
}

impl std::fmt::Debug for Handoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handoff")
            .field("event", &self.event)
            .finish_non_exhaustive()
    }
}

/// Creates a link that holds at most `capacity` unacknowledged events.
pub fn link(capacity: usize) -> (LinkSink, LinkSource) {
    let (sender, receiver) = mpsc::channel(capacity);
    (
        LinkSink {
            sender: Mutex::new(sender),
        },
        LinkSource { receiver },
    )
}

#[async_trait]
impl Sink<Delivery> for LinkSink {
    async fn publish(&self, delivery: Delivery) -> Result<(), SinkError> {
        let (done, released) = oneshot::channel();
        let handoff = Handoff {
            event: delivery.into_event(),
            done,
        };
        {
            let mut sender = self.sender.lock().await;
            sender
                .send(handoff)
                .await
                .map_err(|_disconnected| SinkError::Closed)?;
        }
        match released.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(ack_error)) => Err(SinkError::Unavailable {
                reason: format!("downstream ack failed: {ack_error}"),
            }),
            Err(_cancelled) => Err(SinkError::Unavailable {
                reason: "downstream dropped the event without acknowledging".to_owned(),
            }),
        }
    }
}

#[async_trait]
impl Source for LinkSource {
    type Ack = LinkAck;

    async fn next(&mut self) -> Result<Option<Envelope<LinkAck>>, SourceError> {
        Ok(self
            .receiver
            .next()
            .await
            .map(|handoff| Envelope::new(handoff.event, LinkAck { done: handoff.done })))
    }
}

#[async_trait]
impl Ack for LinkAck {
    async fn ack(self) -> Result<(), AckError> {
        self.done.send(Ok(())).map_err(|_gone| AckError::Rejected {
            reason: "upstream is no longer waiting for this event".to_owned(),
        })
    }
}
