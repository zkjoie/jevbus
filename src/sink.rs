//! Egress: where deliveries and dead letters go.
//!
//! ```text
//! class Sink t s where
//!   publish :: s -> t -> IO (Either SinkError ())
//! ```
//!
//! A [`Sink`] is anything that accepts one item at a time and reports
//! whether it took it. The in-process [`Subscriber`](crate::Subscriber)
//! streams are sinks over bounded channels; a broker topic would be another
//! implementation. This crate ships only the trait and the channel.

use async_trait::async_trait;
use futures::lock::Mutex;
use futures::SinkExt;

use crate::delivery::Sender;

/// Why a sink did not take an item.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SinkError {
    /// Nobody is listening any more; the consumer has gone away for good.
    #[error("sink closed")]
    Closed,
    /// The sink could not be reached right now.
    #[error("sink unavailable: {reason}")]
    Unavailable {
        /// Detail from the sink.
        reason: String,
    },
}

/// A destination for items of type `T`.
///
/// `publish` may wait: a bounded sink applies backpressure to the bus and,
/// through it, to the source.
#[async_trait]
pub trait Sink<T: Send + 'static>: Send + Sync {
    /// Offers `item`. `Ok` means the sink took it.
    async fn publish(&self, item: T) -> Result<(), SinkError>;
}

#[async_trait]
impl<T: Send + 'static, S: Sink<T> + ?Sized> Sink<T> for std::sync::Arc<S> {
    async fn publish(&self, item: T) -> Result<(), SinkError> {
        (**self).publish(item).await
    }
}

#[async_trait]
impl<T: Send + 'static, S: Sink<T> + ?Sized> Sink<T> for Box<S> {
    async fn publish(&self, item: T) -> Result<(), SinkError> {
        (**self).publish(item).await
    }
}

#[async_trait]
impl<T: Send + 'static, S: Sink<T> + ?Sized> Sink<T> for &S {
    async fn publish(&self, item: T) -> Result<(), SinkError> {
        (**self).publish(item).await
    }
}

/// The in-process sink: a bounded channel whose receiver is a
/// [`Receiver`](crate::delivery::Receiver) stream.
#[derive(Debug)]
pub struct ChannelSink<T> {
    // An async mutex: `send` needs `&mut` and may wait on backpressure, and
    // publishes to one route are sequential anyway.
    sender: Mutex<Sender<T>>,
}

impl<T> ChannelSink<T> {
    pub(crate) fn new(sender: Sender<T>) -> Self {
        ChannelSink {
            sender: Mutex::new(sender),
        }
    }
}

#[async_trait]
impl<T: Send + 'static> Sink<T> for ChannelSink<T> {
    async fn publish(&self, item: T) -> Result<(), SinkError> {
        let mut sender = self.sender.lock().await;
        sender
            .send(item)
            .await
            .map_err(|_disconnected| SinkError::Closed)
    }
}
