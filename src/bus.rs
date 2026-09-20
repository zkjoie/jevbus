//! The bus: events in through a stream or a [`Source`], deliveries out
//! through [`Sink`]s.

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use futures::{future, stream, Stream, StreamExt};

use crate::breaker::{BreakerPolicy, Shared, Transient};
use crate::delivery::{self, DeadLetter, DeadLetters, Delivery, Subscriber};
use crate::judge::{Judge, JudgeError};
use crate::ledger::{Entry, Ledger, LedgerError, Outcome, Record};
use crate::lifecycle::{Failed, Judged, Recorded, RetryPolicy, Tracked};
use crate::question::QuestionSet;
use crate::routing::{self, Policy, Verdict};
use crate::sink::{ChannelSink, Sink, SinkError};
use crate::source::{Ack, Envelope, Source, SourceError};
use crate::subscription::{Disposition, Subscription, SubscriptionId};
use crate::time::{self, Sleeper};

/// Tunables for a [`Bus`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BusConfig {
    /// How subscriptions compete for an event.
    pub policy: Policy,
    /// How many judge requests may be in flight at once. Output order is the
    /// input order regardless of this value.
    pub concurrency: NonZeroUsize,
    /// Deliveries that may wait unread per in-process subscriber before the
    /// bus blocks.
    pub channel_capacity: usize,
    /// Retry, backoff and timeout for judge calls.
    pub retry: RetryPolicy,
    /// When consecutive failures pause all judge calls.
    pub breaker: BreakerPolicy,
}

impl Default for BusConfig {
    fn default() -> Self {
        BusConfig {
            policy: Policy::FanOut,
            concurrency: NonZeroUsize::MIN.saturating_add(7),
            channel_capacity: 64,
            retry: RetryPolicy::default(),
            breaker: BreakerPolicy::default(),
        }
    }
}

/// A subscription could not be registered.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SubscribeError {
    /// A subscription with this id is already registered.
    #[error("subscription {id} already exists")]
    Duplicate {
        /// The clashing id.
        id: SubscriptionId,
    },
    /// A dead-letter sink was already registered.
    #[error("dead-letter sink already registered")]
    DeadLettersTaken,
}

/// The bus stopped before the input ended.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RunError {
    /// A record could not be written. See [`Ledger`].
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    /// The source failed. See [`SourceError`].
    #[error(transparent)]
    Source(#[from] SourceError),
    /// A judge call panicked while holding the breaker lock.
    #[error("breaker lock poisoned")]
    BreakerPoisoned,
}

/// Where one subscription's deliveries go.
type DeliverySink = Box<dyn Sink<Delivery>>;

struct Route {
    subscription: Subscription,
    deliver: DeliverySink,
    review: Option<DeliverySink>,
}

/// A streaming event bus.
///
/// Register subscriptions, then call one of the `run` methods with the
/// input. The bus consumes itself so that no subscription can be added
/// mid-run; the question set is fixed for the run's lifetime.
pub struct Bus<J, L, S> {
    judge: J,
    ledger: L,
    sleeper: S,
    config: BusConfig,
    routes: Vec<Route>,
    dead_letters: Option<Box<dyn Sink<DeadLetter>>>,
}

impl<J: Judge, L: Ledger, S: Sleeper> Bus<J, L, S> {
    /// Creates a bus with no subscriptions.
    pub fn new(judge: J, ledger: L, sleeper: S, config: BusConfig) -> Self {
        Bus {
            judge,
            ledger,
            sleeper,
            config,
            routes: Vec::new(),
            dead_letters: None,
        }
    }

    /// Registers a subscription and returns its in-process delivery stream.
    ///
    /// Events that fall in the review band are recorded as
    /// [`Outcome::Unreviewed`] and not delivered.
    pub fn subscribe(&mut self, subscription: Subscription) -> Result<Subscriber, SubscribeError> {
        let (sender, subscriber) = delivery::channel(self.config.channel_capacity);
        self.subscribe_to(subscription, ChannelSink::new(sender))?;
        Ok(subscriber)
    }

    /// Registers a subscription with a separate in-process review stream.
    ///
    /// Returns `(deliveries, reviews)`.
    pub fn subscribe_with_review(
        &mut self,
        subscription: Subscription,
    ) -> Result<(Subscriber, Subscriber), SubscribeError> {
        let (deliver, subscriber) = delivery::channel(self.config.channel_capacity);
        let (review, reviewer) = delivery::channel(self.config.channel_capacity);
        self.subscribe_with_review_to(
            subscription,
            ChannelSink::new(deliver),
            ChannelSink::new(review),
        )?;
        Ok((subscriber, reviewer))
    }

    /// Registers a subscription whose deliveries go to `sink`.
    pub fn subscribe_to(
        &mut self,
        subscription: Subscription,
        sink: impl Sink<Delivery> + 'static,
    ) -> Result<(), SubscribeError> {
        self.check_unique(subscription.id())?;
        self.routes.push(Route {
            subscription,
            deliver: Box::new(sink),
            review: None,
        });
        Ok(())
    }

    /// Registers a subscription whose deliveries go to `sink` and whose
    /// review-band events go to `reviewer`.
    pub fn subscribe_with_review_to(
        &mut self,
        subscription: Subscription,
        sink: impl Sink<Delivery> + 'static,
        reviewer: impl Sink<Delivery> + 'static,
    ) -> Result<(), SubscribeError> {
        self.check_unique(subscription.id())?;
        self.routes.push(Route {
            subscription,
            deliver: Box::new(sink),
            review: Some(Box::new(reviewer)),
        });
        Ok(())
    }

    /// Takes the in-process stream of events the bus gives up on. May be
    /// called once, and not together with [`Bus::dead_letters_to`].
    ///
    /// Without a dead-letter sink, failed events are still recorded in the
    /// ledger as [`Outcome::Unhandled`].
    pub fn dead_letters(&mut self) -> Result<DeadLetters, SubscribeError> {
        let (sender, receiver) = delivery::channel(self.config.channel_capacity);
        self.dead_letters_to(ChannelSink::new(sender))?;
        Ok(receiver)
    }

    /// Sends events the bus gives up on to `sink`. May be called once.
    pub fn dead_letters_to(
        &mut self,
        sink: impl Sink<DeadLetter> + 'static,
    ) -> Result<(), SubscribeError> {
        if self.dead_letters.is_some() {
            return Err(SubscribeError::DeadLettersTaken);
        }
        self.dead_letters = Some(Box::new(sink));
        Ok(())
    }

    /// The registered subscriptions in registration order.
    pub fn subscriptions(&self) -> impl Iterator<Item = &Subscription> {
        self.routes.iter().map(|route| &route.subscription)
    }

    fn check_unique(&self, id: &SubscriptionId) -> Result<(), SubscribeError> {
        if self.subscriptions().any(|sub| sub.id() == id) {
            Err(SubscribeError::Duplicate { id: id.clone() })
        } else {
            Ok(())
        }
    }

    /// Drives a stream of events that need no acknowledgement.
    ///
    /// See [`Bus::run_acked`] for the guarantees.
    pub async fn run<E>(self, events: E) -> Result<(), RunError>
    where
        E: Stream<Item = crate::event::Event>,
    {
        self.run_acked(events.map(Envelope::unacked)).await
    }

    /// Pulls from a [`Source`] until it is exhausted or fails.
    pub async fn run_source<R: Source>(self, mut source: R) -> Result<(), RunError> {
        let envelopes = stream::unfold((&mut source, false), |(source, done)| async move {
            if done {
                return None;
            }
            match source.next().await {
                Ok(Some(envelope)) => Some((Ok(envelope), (source, false))),
                Ok(None) => None,
                Err(error) => Some((Err(error), (source, true))),
            }
        });
        self.run_fallible(envelopes).await
    }

    /// Drives a stream of envelopes, acknowledging each after its final
    /// ledger row.
    ///
    /// With no subscriptions the bus returns at once without reading the
    /// input. Judge failures are retried per [`RetryPolicy`], gated by the
    /// breaker, and finally dead-lettered; they never stop the run. Only a
    /// ledger failure, a source failure or a poisoned breaker lock does.
    /// Events are acknowledged in input order; an envelope the run never
    /// reaches is never acknowledged.
    pub async fn run_acked<A, E>(self, envelopes: E) -> Result<(), RunError>
    where
        A: Ack,
        E: Stream<Item = Envelope<A>>,
    {
        self.run_fallible(envelopes.map(Ok)).await
    }

    async fn run_fallible<A, E>(self, envelopes: E) -> Result<(), RunError>
    where
        A: Ack,
        E: Stream<Item = Result<Envelope<A>, SourceError>>,
    {
        let Bus {
            judge,
            ledger,
            sleeper,
            config,
            mut routes,
            mut dead_letters,
        } = self;
        let questions = routing::plan(config.policy, routes.iter().map(|r| &r.subscription));
        if questions.is_empty() {
            return Ok(());
        }
        let breaker = Mutex::new(Shared::new());
        let cx = JudgeContext {
            judge: &judge,
            ledger: &ledger,
            sleeper: &sleeper,
            config: &config,
            questions: &questions,
            breaker: &breaker,
        };
        let judged = envelopes
            .map(|envelope| async {
                let (event, ack) = envelope?.into_parts();
                let phase = judge_resiliently(&cx, event).await?;
                Ok::<_, RunError>((phase, ack))
            })
            .buffered(config.concurrency.get());
        futures::pin_mut!(judged);
        while let Some(next) = judged.next().await {
            let (phase, ack) = next?;
            let event = Arc::clone(match &phase {
                Ok(judged) => judged.event(),
                Err(failed) => failed.event(),
            });
            dispatch(
                &ledger,
                config.policy,
                &mut routes,
                &mut dead_letters,
                phase,
            )
            .await?;
            ledger
                .record(Record {
                    event: event.id().clone(),
                    entry: Entry::Acknowledged {
                        result: ack.ack().await,
                    },
                })
                .await?;
        }
        Ok(())
    }
}

/// Everything a judge attempt needs, borrowed for the run.
struct JudgeContext<'a, J, L, S> {
    judge: &'a J,
    ledger: &'a L,
    sleeper: &'a S,
    config: &'a BusConfig,
    questions: &'a QuestionSet,
    breaker: &'a Mutex<Shared>,
}

/// `Either (Tracked Failed) (Tracked Judged)`: where the judge phase leaves
/// an event.
type JudgePhase = Result<Tracked<Judged>, Tracked<Failed>>;

/// Walks one event through `Judging`/`Retrying` until `Judged` or `Failed`.
///
/// The phases are types (see [`crate::lifecycle`]); this function only
/// supplies the effects between transitions: sleeping, calling the judge and
/// recording.
async fn judge_resiliently<J: Judge, L: Ledger, S: Sleeper>(
    cx: &JudgeContext<'_, J, L, S>,
    event: crate::event::Event,
) -> Result<JudgePhase, RunError> {
    let event = Arc::new(event);
    if event.is_composite() {
        cx.ledger
            .record(Record {
                event: event.id().clone(),
                entry: Entry::Composed {
                    parts: event.parts().to_vec(),
                },
            })
            .await?;
    }
    let mut judging = Tracked::begin(event);
    loop {
        if let Some(cooldown) = gate(cx.breaker)? {
            cx.sleeper.sleep(cooldown).await;
        }
        record(cx.ledger, &judging).await?;
        let call = cx.judge.judge(judging.event().payload(), cx.questions);
        let result = time::with_timeout(cx.sleeper, cx.config.retry.timeout, call)
            .await
            .unwrap_or(Err(JudgeError::TimedOut {
                after: cx.config.retry.timeout,
            }));
        note(cx.breaker, &cx.config.breaker, &result)?;
        match judging.conclude(&cx.config.retry, result) {
            Ok(Ok(judged)) => {
                record(cx.ledger, &judged).await?;
                return Ok(Ok(judged));
            }
            Ok(Err(retrying)) => {
                record(cx.ledger, &retrying).await?;
                cx.sleeper.sleep(retrying.delay()).await;
                judging = retrying.resume();
            }
            Err(failed) => return Ok(Err(failed)),
        }
    }
}

/// Consults the breaker before an attempt. Returns the cooldown to wait, if
/// any; see [`Shared::admit`].
fn gate(breaker: &Mutex<Shared>) -> Result<Option<std::time::Duration>, RunError> {
    let mut slot = breaker.lock().map_err(|_| RunError::BreakerPoisoned)?;
    let (next, wait) = slot.admit();
    *slot = next;
    Ok(wait)
}

/// Feeds an attempt's result back into the breaker. Only transient failures
/// count against it: a permanent error says nothing about availability.
fn note<T>(
    breaker: &Mutex<Shared>,
    policy: &BreakerPolicy,
    result: &Result<T, JudgeError>,
) -> Result<(), RunError> {
    let outcome = match result {
        Ok(_) => Ok(()),
        Err(cause) if cause.is_transient() => Err(Transient),
        Err(_) => return Ok(()),
    };
    let mut slot = breaker.lock().map_err(|_| RunError::BreakerPoisoned)?;
    *slot = slot.observe(policy, outcome);
    Ok(())
}

async fn record<L: Ledger, P: Recorded>(ledger: &L, tracked: &Tracked<P>) -> Result<(), RunError> {
    ledger
        .record(tracked.record())
        .await
        .map_err(RunError::from)
}

async fn dispatch<L: Ledger>(
    ledger: &L,
    policy: Policy,
    routes: &mut [Route],
    dead_letters: &mut Option<Box<dyn Sink<DeadLetter>>>,
    phase: JudgePhase,
) -> Result<(), RunError> {
    let routed = match phase
        .and_then(|judged| judged.decide(policy, routes.iter().map(|r| &r.subscription)))
    {
        Ok(routed) => routed,
        Err(failed) => return dead_letter(ledger, dead_letters, failed).await,
    };
    let event = Arc::clone(routed.event());
    // `decide` guarantees one verdict per route, in route order.
    let sends = routes
        .iter()
        .zip(routed.verdicts())
        .map(|(route, verdict)| send(route, Arc::clone(&event), verdict));
    let outcomes = future::join_all(sends).await;
    for (verdict, outcome) in outcomes {
        ledger
            .record(Record {
                event: event.id().clone(),
                entry: Entry::Routed {
                    subscription: verdict.subscription.clone(),
                    probability: verdict.probability,
                    disposition: verdict.disposition,
                    outcome,
                },
            })
            .await?;
    }
    record(ledger, &routed.settle()).await
}

async fn dead_letter<L: Ledger>(
    ledger: &L,
    sink: &mut Option<Box<dyn Sink<DeadLetter>>>,
    failed: Tracked<Failed>,
) -> Result<(), RunError> {
    record(ledger, &failed).await?;
    let (event, attempts, cause) = failed.into_parts();
    let outcome = match sink {
        None => Outcome::Unhandled,
        Some(sink) => outcome_of(
            sink.publish(DeadLetter::new(Arc::clone(&event), attempts, cause))
                .await,
            Outcome::Delivered,
        ),
    };
    ledger
        .record(Record {
            event: event.id().clone(),
            entry: Entry::DeadLettered { outcome },
        })
        .await
        .map_err(RunError::from)
}

async fn send<'v>(
    route: &Route,
    event: Arc<crate::event::Event>,
    verdict: &'v Verdict,
) -> (&'v Verdict, Outcome) {
    let outcome = match verdict.disposition {
        Disposition::Drop => Outcome::Dropped,
        Disposition::Deliver => {
            offer(route.deliver.as_ref(), &event, verdict, Outcome::Delivered).await
        }
        Disposition::Review => match route.review.as_deref() {
            Some(reviewer) => offer(reviewer, &event, verdict, Outcome::Reviewed).await,
            None => Outcome::Unreviewed,
        },
    };
    (verdict, outcome)
}

async fn offer(
    sink: &dyn Sink<Delivery>,
    event: &Arc<crate::event::Event>,
    verdict: &Verdict,
    on_success: Outcome,
) -> Outcome {
    outcome_of(
        sink.publish(Delivery::new(Arc::clone(event), verdict.clone()))
            .await,
        on_success,
    )
}

/// Maps a sink's answer to the ledger's vocabulary.
fn outcome_of(result: Result<(), SinkError>, on_success: Outcome) -> Outcome {
    match result {
        Ok(()) => on_success,
        Err(SinkError::Closed) => Outcome::Unsubscribed,
        Err(SinkError::Unavailable { .. }) => Outcome::SinkFailed,
    }
}
