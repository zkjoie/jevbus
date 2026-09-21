//! A circuit breaker shared by every in-flight judge call, encoded in types.
//!
//! ```text
//! data Breaker s = Breaker s
//! data Closed    = Closed   Failures
//! data Open      = Open     Cooldown
//! data HalfOpen  = HalfOpen Cooldown
//!
//! succeed :: Breaker Closed   -> Breaker Closed
//! fail    :: Policy -> Breaker Closed   -> Either (Breaker Open) (Breaker Closed)
//! probe   :: Breaker Open     -> Breaker HalfOpen
//! succeed :: Breaker HalfOpen -> Breaker Closed
//! fail    :: Policy -> Breaker HalfOpen -> Breaker Open
//!
//! newtype Shared = Shared (Either (Either Closed Open) HalfOpen)
//! ```
//!
//! Only an open breaker has a cooldown to wait and only an open breaker can
//! be probed; asking a closed one is a type error. [`Shared`] is the storage
//! boundary: a mutex slot must hold "whichever state", so it is the sum of
//! the three, and its methods dispatch to the typed transitions.

use std::num::NonZeroU32;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// When to open, and for how long.
///
/// `Copy` law: plain configuration values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BreakerPolicy {
    /// Consecutive failures that open the breaker.
    pub failure_threshold: NonZeroU32,
    /// Cooldown after the breaker first opens.
    pub cooldown: Duration,
    /// Upper bound on the cooldown as it doubles.
    pub max_cooldown: Duration,
}

impl Default for BreakerPolicy {
    fn default() -> Self {
        BreakerPolicy {
            failure_threshold: NonZeroU32::MIN.saturating_add(4),
            cooldown: Duration::from_secs(1),
            max_cooldown: Duration::from_secs(30),
        }
    }
}

/// A breaker in state `S`.
///
/// `Copy` law: every state is a scalar witness with no identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Breaker<S> {
    state: S,
}

/// Calls flow; counting consecutive failures.
///
/// Invariant: `failures < policy.failure_threshold`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed {
    failures: u32,
}

/// Calls wait out the cooldown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Open {
    cooldown: Duration,
}

/// A probe is in flight; its result decides the next state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HalfOpen {
    cooldown: Duration,
}

/// A transient failure was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transient;

impl Default for Breaker<Closed> {
    fn default() -> Self {
        Breaker {
            state: Closed { failures: 0 },
        }
    }
}

impl Breaker<Closed> {
    /// A fresh, closed breaker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Consecutive failures so far.
    pub fn failures(self) -> u32 {
        self.state.failures
    }

    /// A call succeeded. Post: `failures == 0`.
    pub fn succeed(self) -> Breaker<Closed> {
        Breaker::default()
    }

    /// A call failed transiently.
    ///
    /// Post: `Err(open)` exactly when the threshold is reached, which is
    /// what preserves the `Closed` invariant.
    pub fn fail(self, policy: &BreakerPolicy) -> Result<Breaker<Closed>, Breaker<Open>> {
        let failures = self.state.failures.saturating_add(1);
        if failures >= policy.failure_threshold.get() {
            Err(Breaker {
                state: Open {
                    cooldown: policy.cooldown.min(policy.max_cooldown),
                },
            })
        } else {
            Ok(Breaker {
                state: Closed { failures },
            })
        }
    }
}

impl Breaker<Open> {
    /// How long callers wait.
    pub fn cooldown(self) -> Duration {
        self.state.cooldown
    }

    /// A caller has committed to waiting out the cooldown and probing.
    pub fn probe(self) -> Breaker<HalfOpen> {
        Breaker {
            state: HalfOpen {
                cooldown: self.state.cooldown,
            },
        }
    }
}

impl Breaker<HalfOpen> {
    /// The probe succeeded.
    pub fn succeed(self) -> Breaker<Closed> {
        Breaker::default()
    }

    /// The probe failed: reopen with a doubled, capped cooldown.
    pub fn fail(self, policy: &BreakerPolicy) -> Breaker<Open> {
        Breaker {
            state: Open {
                cooldown: self
                    .state
                    .cooldown
                    .saturating_mul(2)
                    .min(policy.max_cooldown),
            },
        }
    }
}

/// `Either (Either Closed Open) HalfOpen`: whichever state the shared slot
/// currently holds.
pub type AnyState = Result<Result<Breaker<Closed>, Breaker<Open>>, Breaker<HalfOpen>>;

/// The storage boundary: the one place that holds a breaker of unknown state.
///
/// `Copy` law: inherits from the states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shared(AnyState);

impl Default for Shared {
    fn default() -> Self {
        Shared(Ok(Ok(Breaker::new())))
    }
}

impl Shared {
    /// A closed breaker.
    pub fn new() -> Self {
        Self::default()
    }

    /// The current state.
    pub fn state(self) -> AnyState {
        self.0
    }

    /// A caller is about to attempt. Returns the cooldown it must first
    /// wait, if any; an open breaker becomes half-open so that this caller's
    /// attempt is the probe.
    pub fn admit(self) -> (Shared, Option<Duration>) {
        match self.0 {
            Ok(Err(open)) => (Shared(Err(open.probe())), Some(open.cooldown())),
            closed_or_half => (Shared(closed_or_half), None),
        }
    }

    /// Feeds back an attempt's outcome: `Ok(())` for success, `Err(Transient)`
    /// for a transient failure. Permanent failures say nothing about
    /// availability and must not be reported here.
    ///
    /// A failure reported while the breaker is open (a concurrent caller
    /// that started before it opened) leaves it open.
    pub fn observe(self, policy: &BreakerPolicy, outcome: Result<(), Transient>) -> Shared {
        Shared(match (self.0, outcome) {
            (Ok(Ok(closed)), Ok(())) => Ok(Ok(closed.succeed())),
            (Ok(Ok(closed)), Err(Transient)) => Ok(closed.fail(policy)),
            (Ok(Err(_open)), Ok(())) => Ok(Ok(Breaker::new())),
            (Ok(Err(open)), Err(Transient)) => Ok(Err(open)),
            (Err(half), Ok(())) => Ok(Ok(half.succeed())),
            (Err(half), Err(Transient)) => Ok(Err(half.fail(policy))),
        })
    }

    /// Semantic predicate: calls are currently blocked.
    pub fn is_open(self) -> bool {
        matches!(self.0, Ok(Err(_)))
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;

    fn policy() -> BreakerPolicy {
        BreakerPolicy {
            failure_threshold: NonZeroU32::MIN.saturating_add(1),
            cooldown: Duration::from_secs(1),
            max_cooldown: Duration::from_secs(3),
        }
    }

    #[test]
    fn closed_opens_exactly_at_the_failure_threshold() {
        let p = policy();
        let Ok(still_closed) = Breaker::new().fail(&p) else {
            panic!("one failure below threshold stays closed");
        };
        assert_eq!(still_closed.failures(), 1);
        let Err(open) = still_closed.fail(&p) else {
            panic!("second failure reaches the threshold");
        };
        assert_eq!(open.cooldown(), Duration::from_secs(1));
    }

    #[test]
    fn half_open_failure_doubles_the_cooldown_up_to_the_cap() {
        let p = policy();
        let Err(open) = Breaker::new().fail(&p).and_then(|c| c.fail(&p)) else {
            panic!("two failures open the breaker");
        };
        let open = open.probe().fail(&p);
        assert_eq!(open.cooldown(), Duration::from_secs(2));
        let open = open.probe().fail(&p);
        assert_eq!(open.cooldown(), Duration::from_secs(3));
        let open = open.probe().fail(&p);
        assert_eq!(open.cooldown(), Duration::from_secs(3));
    }

    #[test]
    fn half_open_success_closes_with_a_clean_count() {
        let p = policy();
        let Err(open) = Breaker::new().fail(&p).and_then(|c| c.fail(&p)) else {
            panic!("two failures open the breaker");
        };
        assert_eq!(open.probe().succeed().failures(), 0);
    }

    #[test]
    fn shared_admit_returns_the_cooldown_once_and_makes_the_caller_the_probe() {
        let p = policy();
        let shared = Shared::new()
            .observe(&p, Err(Transient))
            .observe(&p, Err(Transient));
        assert!(shared.is_open());
        let (shared, wait) = shared.admit();
        assert_eq!(wait, Some(Duration::from_secs(1)));
        assert!(!shared.is_open());
        let (_, wait_again) = shared.admit();
        assert_eq!(wait_again, None);
    }

    #[test]
    fn shared_success_from_any_state_yields_closed() {
        let p = policy();
        let open = Shared::new()
            .observe(&p, Err(Transient))
            .observe(&p, Err(Transient));
        assert_eq!(open.observe(&p, Ok(())), Shared::new());
        let (half, _) = open.admit();
        assert_eq!(half.observe(&p, Ok(())), Shared::new());
        assert_eq!(
            Shared::new()
                .observe(&p, Err(Transient))
                .observe(&p, Ok(())),
            Shared::new()
        );
    }
}
