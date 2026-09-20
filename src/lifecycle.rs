//! The per-event state machine, encoded in types.
//!
//! In Haskell terms:
//!
//! ```text
//! data Tracked p   = Tracked { event :: Event, phase :: p }
//! data Judging     = Judging  Attempt
//! data Retrying    = Retrying Attempt Delay JudgeError
//! data Judged      = Judged   Attempt AnswerSet
//! data Routed      = Routed   Attempt [Verdict]
//! data Failed      = Failed   Attempt JudgeFailure
//! data Settled     = Settled  Attempt
//!
//! conclude :: RetryPolicy -> Either JudgeError AnswerSet -> Tracked Judging
//!          -> Either (Tracked Failed) (Either (Tracked Retrying) (Tracked Judged))
//! resume   :: Tracked Retrying -> Tracked Judging
//! decide   :: Policy -> [Subscription] -> Tracked Judged
//!          -> Either (Tracked Failed) (Tracked Routed)
//! settle   :: Tracked Routed -> Tracked Settled
//! ```
//!
//! Each phase is a distinct type, so `settle` on a retrying event or `resume`
//! on a settled one does not compile. Where the next phase depends on a
//! runtime result the transition returns a nested `Either`, spelled with
//! Rust's `Result`; `Either a (Either b c)` is the three-way sum.
//!
//! The ledger stores a data view of each phase, [`EventState`]; the machine
//! itself never inspects that view.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use crate::event::Event;
use crate::judge::JudgeError;
use crate::ledger::{Entry, JudgeFailure, Record};
use crate::question::AnswerSet;
use crate::routing::{self, Policy, Verdict};
use crate::subscription::Subscription;

/// A 1-based attempt counter.
///
/// `Copy` law: a scalar with no identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Attempt(NonZeroU32);

impl Attempt {
    /// The first attempt.
    pub const FIRST: Attempt = Attempt(NonZeroU32::MIN);

    /// The following attempt. Saturates at `u32::MAX`.
    pub fn next(self) -> Attempt {
        Attempt(self.0.saturating_add(1))
    }

    /// The attempt number, starting at 1.
    pub fn get(self) -> u32 {
        self.0.get()
    }
}

/// How many attempts an event may consume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Attempts {
    /// At most this many.
    Bounded(NonZeroU32),
    /// Keep retrying transient failures until one succeeds. Backpressure,
    /// not loss, is the answer to a long outage.
    Unbounded,
}

impl Attempts {
    /// Semantic predicate: after `done` attempts, another one is allowed.
    pub fn allows_another(self, done: Attempt) -> bool {
        match self {
            Attempts::Bounded(max) => done.get() < max.get(),
            Attempts::Unbounded => true,
        }
    }
}

/// Retry and timeout policy for judge calls.
///
/// `Copy` law: plain configuration values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Attempt budget per event.
    pub attempts: Attempts,
    /// Delay before the second attempt.
    pub base_delay: Duration,
    /// Upper bound on any delay.
    pub max_delay: Duration,
    /// Growth factor between consecutive delays.
    pub multiplier: u32,
    /// Limit on one judge call.
    pub timeout: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy {
            attempts: Attempts::Bounded(NonZeroU32::MIN.saturating_add(4)),
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(10),
            multiplier: 2,
            timeout: Duration::from_secs(5),
        }
    }
}

impl RetryPolicy {
    /// Delay before the attempt following `done`.
    ///
    /// Law: `delay(a) <= delay(a.next())` and `delay(a) <= max_delay`.
    pub fn delay_after(&self, done: Attempt) -> Duration {
        let exponent = done.get().saturating_sub(1);
        let factor = self.multiplier.checked_pow(exponent);
        let grown = factor.map_or(self.max_delay, |f| self.base_delay.saturating_mul(f));
        grown.min(self.max_delay)
    }
}

/// The data view of a phase, as written to the ledger.
#[derive(Debug, Clone, PartialEq)]
pub enum EventState {
    /// A judge call is in flight.
    Judging {
        /// Which attempt.
        attempt: Attempt,
    },
    /// The last attempt failed transiently; waiting to try again.
    Retrying {
        /// The attempt that failed.
        attempt: Attempt,
        /// How long the bus waits before the next attempt.
        delay: Duration,
        /// Why the attempt failed.
        cause: JudgeError,
    },
    /// Answers were obtained; verdicts follow.
    Judged {
        /// Attempts consumed.
        attempts: Attempt,
    },
    /// No verdicts will be reached. The event goes to the dead-letter stream.
    Failed {
        /// Attempts consumed.
        attempts: Attempt,
        /// The final cause.
        cause: JudgeFailure,
    },
    /// Every verdict has been acted on and recorded.
    Settled,
}

/// An event together with the phase it is in.
#[derive(Debug)]
pub struct Tracked<P> {
    event: Arc<Event>,
    phase: P,
}

/// A judge call is in flight.
#[derive(Debug)]
pub struct Judging {
    attempt: Attempt,
}

/// Waiting to try again.
#[derive(Debug)]
pub struct Retrying {
    attempt: Attempt,
    delay: Duration,
    cause: JudgeError,
}

/// Answers obtained.
#[derive(Debug)]
pub struct Judged {
    attempts: Attempt,
    answers: AnswerSet,
}

/// Verdicts reached; deliveries pending.
#[derive(Debug)]
pub struct Routed {
    attempts: Attempt,
    verdicts: Vec<Verdict>,
}

/// Given up.
#[derive(Debug)]
pub struct Failed {
    attempts: Attempt,
    cause: JudgeFailure,
}

/// Done.
#[derive(Debug)]
pub struct Settled {
    attempts: Attempt,
}

/// A phase the ledger records.
///
/// `Routed` is deliberately not one: its content is written as one row per
/// subscription by the bus.
pub trait Recorded {
    /// The data view.
    fn state(&self) -> EventState;
}

impl Recorded for Judging {
    fn state(&self) -> EventState {
        EventState::Judging {
            attempt: self.attempt,
        }
    }
}

impl Recorded for Retrying {
    fn state(&self) -> EventState {
        EventState::Retrying {
            attempt: self.attempt,
            delay: self.delay,
            cause: self.cause.clone(),
        }
    }
}

impl Recorded for Judged {
    fn state(&self) -> EventState {
        EventState::Judged {
            attempts: self.attempts,
        }
    }
}

impl Recorded for Failed {
    fn state(&self) -> EventState {
        EventState::Failed {
            attempts: self.attempts,
            cause: self.cause.clone(),
        }
    }
}

impl Recorded for Settled {
    fn state(&self) -> EventState {
        EventState::Settled
    }
}

/// `Either (Tracked Failed) (Either (Tracked Retrying) (Tracked Judged))`.
pub type Concluded = Result<Result<Tracked<Judged>, Tracked<Retrying>>, Tracked<Failed>>;

/// `Either (Tracked Failed) (Tracked Routed)`.
pub type Decided = Result<Tracked<Routed>, Tracked<Failed>>;

impl<P> Tracked<P> {
    /// The event.
    pub fn event(&self) -> &Arc<Event> {
        &self.event
    }

    /// The phase's witness data.
    pub fn phase(&self) -> &P {
        &self.phase
    }

    fn into<Q>(self, phase: Q) -> Tracked<Q> {
        Tracked {
            event: self.event,
            phase,
        }
    }
}

impl<P: Recorded> Tracked<P> {
    /// The ledger row for the current phase.
    pub fn record(&self) -> Record {
        Record {
            event: self.event.id().clone(),
            entry: Entry::Lifecycle(self.phase.state()),
        }
    }
}

impl Tracked<Judging> {
    /// Enters the machine at the first attempt.
    pub fn begin(event: Arc<Event>) -> Self {
        Tracked {
            event,
            phase: Judging {
                attempt: Attempt::FIRST,
            },
        }
    }

    /// The attempt in flight.
    pub fn attempt(&self) -> Attempt {
        self.phase.attempt
    }

    /// Consumes the attempt's result.
    ///
    /// Post: `Ok(Err(retrying))` only if the error is transient and the
    /// policy allows another attempt.
    pub fn conclude(
        self,
        policy: &RetryPolicy,
        result: Result<AnswerSet, JudgeError>,
    ) -> Concluded {
        let attempt = self.phase.attempt;
        match result {
            Ok(answers) => Ok(Ok(self.into(Judged {
                attempts: attempt,
                answers,
            }))),
            Err(cause) if cause.is_transient() && policy.attempts.allows_another(attempt) => {
                Ok(Err(self.into(Retrying {
                    attempt,
                    delay: policy.delay_after(attempt),
                    cause,
                })))
            }
            Err(cause) => Err(self.into(Failed {
                attempts: attempt,
                cause: JudgeFailure::Judge(cause),
            })),
        }
    }
}

impl Tracked<Retrying> {
    /// How long to wait before resuming.
    pub fn delay(&self) -> Duration {
        self.phase.delay
    }

    /// Starts the next attempt.
    pub fn resume(self) -> Tracked<Judging> {
        let attempt = self.phase.attempt.next();
        self.into(Judging { attempt })
    }
}

impl Tracked<Judged> {
    /// The answers.
    pub fn answers(&self) -> &AnswerSet {
        &self.phase.answers
    }

    /// Turns answers into verdicts, or fails if they do not fit the plan.
    pub fn decide<'a>(
        self,
        policy: Policy,
        subscriptions: impl IntoIterator<Item = &'a Subscription>,
    ) -> Decided {
        let attempts = self.phase.attempts;
        match routing::decide(policy, subscriptions, &self.phase.answers) {
            Ok(verdicts) => Ok(self.into(Routed { attempts, verdicts })),
            Err(cause) => Err(self.into(Failed {
                attempts,
                cause: JudgeFailure::Routing(cause),
            })),
        }
    }
}

impl Tracked<Routed> {
    /// One verdict per subscription, in registration order.
    pub fn verdicts(&self) -> &[Verdict] {
        &self.phase.verdicts
    }

    /// All deliveries have been offered and recorded.
    pub fn settle(self) -> Tracked<Settled> {
        let attempts = self.phase.attempts;
        self.into(Settled { attempts })
    }
}

impl Tracked<Failed> {
    /// Attempts consumed.
    pub fn attempts(&self) -> Attempt {
        self.phase.attempts
    }

    /// The final cause.
    pub fn cause(&self) -> &JudgeFailure {
        &self.phase.cause
    }

    /// Leaves the machine with what a dead letter needs.
    pub fn into_parts(self) -> (Arc<Event>, Attempt, JudgeFailure) {
        (self.event, self.phase.attempts, self.phase.cause)
    }
}

impl Tracked<Settled> {
    /// Attempts consumed.
    pub fn attempts(&self) -> Attempt {
        self.phase.attempts
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;
    use crate::event::{EventId, Payload};
    use proptest::prelude::*;

    fn policy(max: u32) -> RetryPolicy {
        RetryPolicy {
            attempts: NonZeroU32::new(max).map_or(Attempts::Unbounded, Attempts::Bounded),
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(350),
            multiplier: 2,
            timeout: Duration::from_secs(1),
        }
    }

    fn transient() -> JudgeError {
        JudgeError::Unavailable {
            reason: "down".into(),
        }
    }

    fn permanent() -> JudgeError {
        JudgeError::Rejected {
            status: 401,
            message: "bad key".into(),
        }
    }

    fn begin() -> Tracked<Judging> {
        let id = EventId::new("e").unwrap_or_else(|_| begin().event.id().clone());
        Tracked::begin(Arc::new(Event::new(id, Payload::new("x"))))
    }

    #[test]
    fn delay_grows_geometrically_and_is_capped() {
        let p = policy(9);
        let a1 = Attempt::FIRST;
        let a2 = a1.next();
        let a3 = a2.next();
        assert_eq!(p.delay_after(a1), Duration::from_millis(100));
        assert_eq!(p.delay_after(a2), Duration::from_millis(200));
        assert_eq!(p.delay_after(a3), Duration::from_millis(350));
    }

    #[test]
    fn conclude_retries_transient_errors_while_attempts_remain_then_fails() {
        let p = policy(3);
        // Judging(1) -> Retrying(1) -> Judging(2) -> Retrying(2) -> Judging(3) -> Failed(3)
        let Ok(Err(r1)) = begin().conclude(&p, Err(transient())) else {
            panic!("first transient failure must retry");
        };
        assert_eq!(r1.delay(), Duration::from_millis(100));
        let Ok(Err(r2)) = r1.resume().conclude(&p, Err(transient())) else {
            panic!("second transient failure must retry");
        };
        assert_eq!(r2.delay(), Duration::from_millis(200));
        let Err(failed) = r2.resume().conclude(&p, Err(transient())) else {
            panic!("third failure exhausts the budget");
        };
        assert_eq!(failed.attempts().get(), 3);
    }

    #[test]
    fn conclude_fails_permanent_errors_at_once() {
        assert!(begin().conclude(&policy(9), Err(permanent())).is_err());
    }

    #[test]
    fn conclude_with_answers_is_judged_and_records_the_attempt_count() {
        let Ok(Ok(judged)) = begin().conclude(&policy(9), Ok(AnswerSet::new())) else {
            panic!("answers must judge");
        };
        assert_eq!(
            judged.phase().state(),
            EventState::Judged {
                attempts: Attempt::FIRST
            }
        );
    }

    #[test]
    fn unbounded_attempts_always_allow_another() {
        let p = policy(0);
        let far = (0..100).fold(begin(), |j, _| match j.conclude(&p, Err(transient())) {
            Ok(Err(r)) => r.resume(),
            _ => begin(),
        });
        assert_eq!(far.attempt().get(), 101);
    }

    proptest! {
        // Law: delay is monotone in the attempt and never exceeds the cap.
        #[test]
        fn delay_is_monotone_and_bounded(steps in 0u32..40, base in 1u64..1000, cap in 1u64..5000, mult in 1u32..5) {
            let p = RetryPolicy {
                attempts: Attempts::Unbounded,
                base_delay: Duration::from_millis(base),
                max_delay: Duration::from_millis(cap),
                multiplier: mult,
                timeout: Duration::from_secs(1),
            };
            let a = (0..steps).fold(Attempt::FIRST, |a, _| a.next());
            prop_assert!(p.delay_after(a) <= p.delay_after(a.next()));
            prop_assert!(p.delay_after(a) <= p.max_delay);
        }
    }
}
