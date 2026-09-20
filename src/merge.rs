//! Merging several events into one before they reach the bus.
//!
//! [`windowed`] is a stream adapter. It groups events by the key a
//! [`Combiner`] assigns, keeps each group open for [`WindowPolicy::span`] or
//! until it holds [`WindowPolicy::max_parts`] events, then asks the combiner
//! to fold the group into one event. Combining is asynchronous, so a combiner
//! may consult external state. Events without a key pass through untouched.
//! When the input ends every open group is flushed in key order.
//!
//! Invariant: no event is lost. Every input event leaves the stage either as
//! itself, inside a merged event's [`Event::parts`], or inside a
//! [`MergeFailure`].

use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::{self, BoxFuture, Either};
use futures::stream::{self, Fuse, FuturesUnordered};
use futures::{Stream, StreamExt};

use crate::event::{Event, EventId, Payload};
use crate::time::Sleeper;

/// Why a group could not be combined.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CombineError {
    /// The combiner was given no parts. The stage never does this.
    #[error("nothing to combine")]
    Empty,
    /// The combiner declined the group.
    #[error("combiner rejected the group: {reason}")]
    Rejected {
        /// Why.
        reason: String,
    },
}

/// Decides which events belong together and how they fold into one.
#[async_trait]
pub trait Combiner: Send + Sync {
    /// The grouping key.
    type Key: Ord + Clone + Send + Sync + 'static;

    /// The group `event` belongs to, or `None` to pass it through unmerged.
    fn key(&self, event: &Event) -> Option<Self::Key>;

    /// Folds a group into one event.
    ///
    /// Pre: `parts` is non-empty and in arrival order.
    /// Post: the result's [`Event::parts`] should list `parts`; use
    /// [`Event::merged`].
    async fn combine(&self, parts: &[Event]) -> Result<Event, CombineError>;
}

/// A group the stage gave up on, with the events it held.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeFailure {
    /// The events, in arrival order. Nothing is lost.
    pub parts: Vec<Event>,
    /// Why.
    pub cause: CombineError,
}

/// When a group is flushed.
///
/// `Copy` law: plain configuration values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowPolicy {
    /// How long a group stays open after its first event.
    pub span: Duration,
    /// Flush as soon as a group holds this many events.
    pub max_parts: NonZeroUsize,
}

/// Concatenates payloads of events that share a string key.
///
/// The merged id is `<first id>+<count>` and the payload joins the parts
/// with newlines. This is the simplest useful combiner and the reference
/// implementation for tests.
#[derive(Debug, Clone)]
pub struct ConcatByKey<F> {
    key: F,
}

impl<F> ConcatByKey<F>
where
    F: Fn(&Event) -> Option<String> + Send + Sync,
{
    /// Groups by `key`.
    pub fn new(key: F) -> Self {
        ConcatByKey { key }
    }
}

#[async_trait]
impl<F> Combiner for ConcatByKey<F>
where
    F: Fn(&Event) -> Option<String> + Send + Sync,
{
    type Key = String;

    fn key(&self, event: &Event) -> Option<String> {
        (self.key)(event)
    }

    async fn combine(&self, parts: &[Event]) -> Result<Event, CombineError> {
        let first = parts.first().ok_or(CombineError::Empty)?;
        let id = EventId::new(format!("{}+{}", first.id(), parts.len())).map_err(|_| {
            CombineError::Rejected {
                reason: "empty id".to_owned(),
            }
        })?;
        let text = parts
            .iter()
            .map(|part| part.payload().as_str())
            .collect::<Vec<_>>()
            .join("\n");
        Ok(Event::merged(id, Payload::new(text), parts))
    }
}

/// Runs the merge stage over `input`.
///
/// The output preserves arrival order among pass-through events and flushes
/// groups when they become due; a slow `combine` applies backpressure to the
/// input.
pub fn windowed<I, C, S>(
    input: I,
    combiner: C,
    sleeper: S,
    policy: WindowPolicy,
) -> impl Stream<Item = Result<Event, MergeFailure>>
where
    I: Stream<Item = Event> + Send,
    C: Combiner,
    S: Sleeper,
{
    let stage = Stage {
        input: Box::pin(input).fuse(),
        groups: BTreeMap::new(),
        timers: FuturesUnordered::new(),
        ready: VecDeque::new(),
        generation: 0,
        closed: false,
        combiner,
        sleeper,
        policy,
    };
    stream::unfold(stage, |mut stage| async move {
        let item = stage.next_item().await;
        item.map(|item| (item, stage))
    })
}

/// One open group.
struct Group {
    parts: Vec<Event>,
    /// Distinguishes this group from an earlier one with the same key whose
    /// timer may still be pending.
    generation: u64,
}

/// The stage's state between yielded items.
struct Stage<I, C: Combiner, S> {
    input: Fuse<Pin<Box<I>>>,
    groups: BTreeMap<C::Key, Group>,
    timers: FuturesUnordered<BoxFuture<'static, (C::Key, u64)>>,
    ready: VecDeque<Result<Event, MergeFailure>>,
    generation: u64,
    closed: bool,
    combiner: C,
    sleeper: S,
    policy: WindowPolicy,
}

/// What woke the stage.
enum Wake<K> {
    Arrived(Event),
    InputEnded,
    Due(K, u64),
}

impl<I, C, S> Stage<I, C, S>
where
    I: Stream<Item = Event>,
    C: Combiner,
    S: Sleeper,
{
    async fn next_item(&mut self) -> Option<Result<Event, MergeFailure>> {
        loop {
            if let Some(item) = self.ready.pop_front() {
                return Some(item);
            }
            if self.closed {
                match self.groups.keys().next().cloned() {
                    Some(key) => self.flush(&key).await,
                    None => return None,
                }
                continue;
            }
            match self.wake().await {
                Wake::Arrived(event) => self.admit(event).await,
                Wake::InputEnded => self.closed = true,
                Wake::Due(key, generation) => {
                    if self.group_is(&key, generation) {
                        self.flush(&key).await;
                    }
                }
            }
        }
    }

    /// Waits for the next input event or the next due timer.
    async fn wake(&mut self) -> Wake<C::Key> {
        if self.timers.is_empty() {
            return self
                .input
                .next()
                .await
                .map_or(Wake::InputEnded, Wake::Arrived);
        }
        match future::select(self.input.next(), self.timers.next()).await {
            Either::Left((Some(event), _)) => Wake::Arrived(event),
            Either::Left((None, _)) => Wake::InputEnded,
            Either::Right((Some((key, generation)), _)) => Wake::Due(key, generation),
            // `timers` was non-empty, so this branch means every timer was
            // already consumed; wait for input instead.
            Either::Right((None, _)) => self
                .input
                .next()
                .await
                .map_or(Wake::InputEnded, Wake::Arrived),
        }
    }

    /// Places an event in its group, opening one if needed, and flushes the
    /// group if it is now full.
    async fn admit(&mut self, event: Event) {
        let Some(key) = self.combiner.key(&event) else {
            self.ready.push_back(Ok(event));
            return;
        };
        let full = {
            let group = self.groups.entry(key.clone()).or_insert_with(|| {
                self.generation = self.generation.wrapping_add(1);
                Group {
                    parts: Vec::new(),
                    generation: self.generation,
                }
            });
            if group.parts.is_empty() {
                let sleep = self.sleeper.sleep(self.policy.span);
                let tag = (key.clone(), group.generation);
                // The timer owns its tag and its sleep; nothing borrows the stage.
                self.timers.push(Box::pin(async move {
                    sleep.await;
                    tag
                }));
            }
            group.parts.push(event);
            group.parts.len() >= self.policy.max_parts.get()
        };
        if full {
            self.flush(&key).await;
        }
    }

    /// Semantic predicate: the open group under `key` is the one a timer
    /// was started for.
    fn group_is(&self, key: &C::Key, generation: u64) -> bool {
        self.groups
            .get(key)
            .is_some_and(|group| group.generation == generation)
    }

    /// Removes the group and queues its combined event, or the failure.
    async fn flush(&mut self, key: &C::Key) {
        let Some(group) = self.groups.remove(key) else {
            return;
        };
        let item = match self.combiner.combine(&group.parts).await {
            Ok(event) => Ok(event),
            Err(cause) => Err(MergeFailure {
                parts: group.parts,
                cause,
            }),
        };
        self.ready.push_back(item);
    }
}

#[cfg(all(test, feature = "tokio"))]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use futures::channel::mpsc;
    use futures::SinkExt;
    use tokio::sync::Mutex;

    use super::*;
    use crate::time::TokioSleeper;

    fn event(id: &str, text: &str) -> Event {
        Event::new(EventId::new(id).unwrap(), Payload::new(text))
    }

    /// Key is the text before the first ':'; no ':' means pass-through.
    fn by_prefix() -> ConcatByKey<impl Fn(&Event) -> Option<String> + Send + Sync> {
        ConcatByKey::new(|event: &Event| {
            event
                .payload()
                .as_str()
                .split_once(':')
                .map(|(k, _)| k.to_owned())
        })
    }

    fn policy(span_ms: u64, max_parts: usize) -> WindowPolicy {
        WindowPolicy {
            span: Duration::from_millis(span_ms),
            max_parts: NonZeroUsize::new(max_parts).unwrap(),
        }
    }

    fn ids(items: &[Result<Event, MergeFailure>]) -> Vec<String> {
        items
            .iter()
            .map(|item| match item {
                Ok(event) => event.id().to_string(),
                Err(failure) => format!("failure({})", failure.parts.len()),
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn groups_by_key_and_flushes_everything_when_input_ends() {
        let input = stream::iter([
            event("1", "u1:hello"),
            event("2", "u2:hi"),
            event("3", "u1:again"),
            event("4", "no key"),
        ]);
        let out: Vec<_> = windowed(input, by_prefix(), TokioSleeper, policy(1_000, 10))
            .collect()
            .await;
        assert_eq!(ids(&out), ["4", "1+2", "2+1"]);
        let Some(Ok(merged)) = out.get(1) else {
            return assert!(out.len() == 3);
        };
        assert_eq!(merged.payload().as_str(), "u1:hello\nu1:again");
        assert_eq!(merged.parts().len(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_full_group_flushes_at_once_without_waiting_for_the_window() {
        let (mut tx, rx) = mpsc::channel(8);
        let out = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&out);
        let stage = tokio::spawn(async move {
            let mut stream =
                std::pin::pin!(windowed(rx, by_prefix(), TokioSleeper, policy(1_000, 2)));
            while let Some(item) = stream.next().await {
                sink.lock().await.push(item);
            }
        });
        tx.send(event("1", "u1:a")).await.unwrap();
        tx.send(event("2", "u1:b")).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(ids(&out.lock().await), ["1+2"]);
        drop(tx);
        stage.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_stale_timer_does_not_flush_a_newer_group_with_the_same_key() {
        let (mut tx, rx) = mpsc::channel(8);
        let out = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&out);
        let stage = tokio::spawn(async move {
            let mut stream =
                std::pin::pin!(windowed(rx, by_prefix(), TokioSleeper, policy(1_000, 2)));
            while let Some(item) = stream.next().await {
                sink.lock().await.push(item);
            }
        });
        // t=0: group 1 opens (timer at t=1000) and fills at once.
        tx.send(event("1", "u1:a")).await.unwrap();
        tx.send(event("2", "u1:b")).await.unwrap();
        // t=500: group 2 opens under the same key (timer at t=1500).
        tokio::time::sleep(Duration::from_millis(500)).await;
        tx.send(event("3", "u1:c")).await.unwrap();
        // t=1100: group 1's timer has fired; group 2 must still be open.
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_eq!(ids(&out.lock().await), ["1+2"]);
        // t=1600: group 2's own timer flushes it.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(ids(&out.lock().await), ["1+2", "3+1"]);
        drop(tx);
        stage.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_rejected_group_is_returned_intact_as_a_failure() {
        struct Refusing;
        #[async_trait]
        impl Combiner for Refusing {
            type Key = ();
            fn key(&self, _: &Event) -> Option<()> {
                Some(())
            }
            async fn combine(&self, _: &[Event]) -> Result<Event, CombineError> {
                Err(CombineError::Rejected {
                    reason: "never".into(),
                })
            }
        }
        let input = stream::iter([event("1", "a"), event("2", "b")]);
        let out: Vec<_> = windowed(input, Refusing, TokioSleeper, policy(10, 10))
            .collect()
            .await;
        assert_eq!(ids(&out), ["failure(2)"]);
    }
}
