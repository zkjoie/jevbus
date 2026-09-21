//! `jevbus`: a streaming event bus whose routing is decided by a probabilistic judge.
//!
//! # Model
//!
//! An [`Event`] carries a [`Payload`]. A [`Subscription`] is a plain-language
//! description of the events a consumer wants, plus a [`Thresholds`] policy.
//! The [`Bus`] consumes a `Stream<Item = Event>`. For every event it asks a
//! [`Judge`] once, receives one [`Probability`] per subscription, and turns
//! each into a [`Disposition`]: deliver, review, or drop. Deliveries flow out
//! on per-subscription [`Subscriber`] streams; every outcome, including drops,
//! is written to a [`Ledger`].
//!
//! Judge calls for consecutive events are pipelined with bounded concurrency,
//! and output order equals input order.
//!
//! # Blueprints and chaining
//!
//! A bus is data plus handles. The data half is a [`Blueprint`]: the
//! configuration, the subscriptions and which of them have review or
//! dead-letter egress. It serialises with `serde`, and
//! [`Bus::from_blueprint`] rebuilds a bus from it. Two buses chain through a
//! [`link()`]: the upstream subscription's [`Sink`] hands each delivery to the
//! downstream bus's [`Source`], and the upstream `publish` completes only
//! when the downstream has acknowledged, so at-least-once holds end to end.
//! [`Delivery`] and [`Event`] serialise too, which is the wire shape for a
//! link across processes; the transport is not part of this crate.
//!
//! # Ingress and egress
//!
//! Events enter through a `Stream<Item = Event>`, a `Stream<Item =
//! Envelope<A>>` carrying an [`Ack`] handle, or a pull-based [`Source`].
//! They leave through a [`Sink`]: the in-process [`Subscriber`] channel, or
//! any implementation of the trait. The bus acknowledges an envelope only
//! after its final ledger row, so delivery is at least once. This crate
//! ships the traits and the in-process channel; broker implementations live
//! outside it.
//!
//! # Merging events
//!
//! [`merge::windowed`] is an asynchronous stage placed before the bus: it
//! groups events by a key chosen by a [`Combiner`], holds each group for a
//! time window or until it is full, and asks the combiner to fold the group
//! into one [`Event`] whose [`Event::parts`] records its lineage. The bus
//! writes that lineage to the ledger as a `Composed` row.
//!
//! # Caching answers
//!
//! [`Cached`] wraps any [`Judge`] and consults a [`Cache`] before calling it.
//! The key is a digest of the payload and the question set, so a changed
//! subscription never hits a stale answer. [`MemoryLru`] is the in-process
//! backend; the [`Cache`] trait admits external stores such as memcached.
//! Entries expire after a [`Ttl`], one day by default.
//!
//! # Event state and an unavailable judge
//!
//! Every event walks the state machine in [`lifecycle`], whose phases are
//! types: `Tracked<Judging>`, `Tracked<Retrying>`, and so on, with
//! transitions that consume one phase and return the next, or a nested
//! `Either` (`Result`) where the outcome depends on the judge. Transient
//! failures are retried with exponential backoff, permanent ones or an
//! exhausted budget send the event to a [`DeadLetters`] stream, and each
//! transition is written to the ledger. A [`breaker::Breaker`] shared by all
//! in-flight calls opens after consecutive failures so that new events wait
//! out the outage instead of burning their retry budgets. Time is an injected
//! effect, [`Sleeper`].
//!
//! # Layers
//!
//! * Pure core: [`routing`] maps subscriptions to a [`QuestionSet`] and an
//!   [`AnswerSet`] back to [`Verdict`]s. No IO, no time, no channels.
//! * Boundaries: [`Judge`] and [`Ledger`] are traits; consumers are streams.
//!   The [`Bus`] is generic over the judge and the ledger.
//! * Protocol: [`jev::SystemOne`] is Jev's request/response protocol as a
//!   trait, and [`jev::JevJudge`] makes any implementation a [`Judge`].
//!   [`jev::http`] is the HTTPS implementation and the only network code.

pub mod blueprint;
pub mod breaker;
pub mod bus;
pub mod cache;
pub mod delivery;
pub mod event;
pub mod judge;
pub mod ledger;
pub mod lifecycle;
pub mod link;
pub mod merge;
pub mod probability;
pub mod question;
pub mod routing;
pub mod sink;
pub mod source;
pub mod subscription;
pub mod time;

pub mod jev;

pub use blueprint::{Blueprint, Handles, Role, RouteSpec};
pub use breaker::{Breaker, BreakerPolicy, Shared};
pub use bus::{Bus, BusConfig, RunError, SubscribeError};
pub use cache::{Cache, CacheError, CacheKey, CacheStats, Cached, MemoryLru, Ttl};
pub use delivery::{DeadLetter, DeadLetters, Delivery, Receiver, Subscriber};
pub use event::{EmptyId, Event, EventId, Payload};
pub use judge::{Judge, JudgeError};
pub use ledger::{Entry, JudgeFailure, Ledger, LedgerError, MemoryLedger, Outcome, Record};
pub use lifecycle::{Attempt, Attempts, EventState, Recorded, RetryPolicy, Tracked};
pub use link::{link, LinkAck, LinkSink, LinkSource};
pub use merge::{CombineError, Combiner, ConcatByKey, MergeFailure, WindowPolicy};
pub use probability::{Probability, ProbabilityError};
pub use question::{Answer, AnswerSet, Question, QuestionName, QuestionSet};
pub use routing::{Policy, RoutingError, Verdict};
pub use sink::{Sink, SinkError};
pub use source::{Ack, AckError, Envelope, NoAck, Source, SourceError};
pub use subscription::{Disposition, Subscription, SubscriptionId, ThresholdError, Thresholds};
pub use time::{Clock, Sleeper, SystemClock};
#[cfg(feature = "tokio")]
pub use time::{TokioClock, TokioSleeper};
