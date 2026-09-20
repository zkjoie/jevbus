//! The time boundary: sleeping and timeouts as an injected effect.
//!
//! The bus never names a runtime. Anything that can sleep is a [`Sleeper`];
//! [`TokioSleeper`] is provided behind the `tokio` feature.

use std::future::Future;
use std::time::{Duration, Instant};

use futures::future::{self, BoxFuture, Either};

/// A source of delays.
///
/// The returned future owns everything it needs, so timers can outlive the
/// borrow of the sleeper that made them.
pub trait Sleeper: Send + Sync {
    /// Resolves after at least `duration`.
    fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()>;
}

impl<T: Sleeper + ?Sized> Sleeper for &T {
    fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()> {
        (**self).sleep(duration)
    }
}

/// Runs `work` for at most `limit`.
///
/// Post: `Some(output)` if `work` finished first, `None` if the limit elapsed
/// first. `work` is dropped, and thereby cancelled, on `None`.
pub async fn with_timeout<S, F>(sleeper: &S, limit: Duration, work: F) -> Option<F::Output>
where
    S: Sleeper + ?Sized,
    F: Future,
{
    futures::pin_mut!(work);
    match future::select(work, sleeper.sleep(limit)).await {
        Either::Left((output, _sleep)) => Some(output),
        Either::Right(((), _work)) => None,
    }
}

/// A source of the current monotonic time.
///
/// Injected so that expiry can be tested on a paused clock.
pub trait Clock: Send + Sync {
    /// The current instant.
    fn now(&self) -> Instant;
}

impl<T: Clock + ?Sized> Clock for &T {
    fn now(&self) -> Instant {
        (**self).now()
    }
}

/// The operating system's monotonic clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// A [`Clock`] that follows tokio's timer, including a paused one in tests.
#[cfg(feature = "tokio")]
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioClock;

#[cfg(feature = "tokio")]
impl Clock for TokioClock {
    fn now(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }
}

/// A [`Sleeper`] backed by tokio's timer.
#[cfg(feature = "tokio")]
#[derive(Debug, Clone, Copy, Default)]
pub struct TokioSleeper;

#[cfg(feature = "tokio")]
impl Sleeper for TokioSleeper {
    fn sleep(&self, duration: Duration) -> BoxFuture<'static, ()> {
        Box::pin(tokio::time::sleep(duration))
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn with_timeout_yields_the_output_when_work_finishes_first() {
        let out = with_timeout(&TokioSleeper, Duration::from_secs(5), async { 7 }).await;
        assert_eq!(out, Some(7));
    }

    #[tokio::test(start_paused = true)]
    async fn with_timeout_yields_none_when_the_limit_elapses_first() {
        let slow = tokio::time::sleep(Duration::from_secs(10));
        let out = with_timeout(&TokioSleeper, Duration::from_secs(1), slow).await;
        assert_eq!(out, None);
    }
}
