//! Integration: event state under judge failure, retry, timeout, breaker and
//! dead-lettering. Runs on paused tokio time, so backoff costs nothing.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;

use async_trait::async_trait;
use futures::{stream, StreamExt};
use jevbus::{
    Answer, AnswerSet, Attempt, Attempts, BreakerPolicy, Bus, BusConfig, Entry, Event, EventId,
    EventState, Judge, JudgeError, JudgeFailure, MemoryLedger, Outcome, Payload, Policy,
    Probability, QuestionSet, RetryPolicy, Subscription, SubscriptionId, Thresholds, TokioSleeper,
};

/// A judge whose behaviour per call is scripted up front.
enum Script {
    Ok,
    Transient,
    Permanent,
    Hang,
}

struct Scripted {
    script: Mutex<Vec<Script>>,
    calls: AtomicUsize,
}

impl Scripted {
    fn new(script: Vec<Script>) -> Self {
        Scripted {
            script: Mutex::new(script),
            calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl Judge for Scripted {
    async fn judge(&self, _: &Payload, questions: &QuestionSet) -> Result<AnswerSet, JudgeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let step = {
            let mut script = self.script.lock().unwrap();
            if script.is_empty() {
                Script::Ok
            } else {
                script.remove(0)
            }
        };
        match step {
            Script::Ok => Ok(questions
                .iter()
                .map(|(name, _)| {
                    (
                        name.clone(),
                        Answer::Noul {
                            probability: Probability::new(0.9).unwrap(),
                        },
                    )
                })
                .collect()),
            Script::Transient => Err(JudgeError::Rejected {
                status: 503,
                message: "overloaded".into(),
            }),
            Script::Permanent => Err(JudgeError::Rejected {
                status: 401,
                message: "bad key".into(),
            }),
            Script::Hang => {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Err(JudgeError::Unavailable {
                    reason: "unreachable".into(),
                })
            }
        }
    }
}

fn sub() -> Subscription {
    Subscription::new(
        SubscriptionId::new("a").unwrap(),
        "anything",
        Thresholds::deliver_only(Probability::new(0.5).unwrap()),
    )
}

fn event(id: &str) -> Event {
    Event::new(EventId::new(id).unwrap(), Payload::new(id))
}

fn config(max_attempts: u32, breaker_threshold: u32) -> BusConfig {
    BusConfig {
        policy: Policy::FanOut,
        concurrency: NonZeroUsize::new(2).unwrap(),
        channel_capacity: 4,
        retry: RetryPolicy {
            attempts: Attempts::Bounded(NonZeroU32::new(max_attempts).unwrap()),
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
            multiplier: 2,
            timeout: Duration::from_secs(1),
        },
        breaker: BreakerPolicy {
            failure_threshold: NonZeroU32::new(breaker_threshold).unwrap(),
            cooldown: Duration::from_secs(10),
            max_cooldown: Duration::from_secs(60),
        },
    }
}

fn states(ledger: &MemoryLedger, id: &str) -> Vec<String> {
    ledger
        .records()
        .unwrap()
        .iter()
        .filter(|r| r.event.as_str() == id)
        .map(|r| match &r.entry {
            Entry::Lifecycle(EventState::Judging { attempt }) => {
                format!("judging{}", attempt.get())
            }
            Entry::Lifecycle(EventState::Retrying { attempt, .. }) => {
                format!("retrying{}", attempt.get())
            }
            Entry::Lifecycle(EventState::Judged { .. }) => "judged".into(),
            Entry::Lifecycle(EventState::Failed { attempts, .. }) => {
                format!("failed{}", attempts.get())
            }
            Entry::Lifecycle(EventState::Settled) => "settled".into(),
            Entry::Routed { outcome, .. } => format!("routed:{outcome:?}").to_lowercase(),
            Entry::DeadLettered { outcome } => format!("dead:{outcome:?}").to_lowercase(),
            Entry::Composed { parts } => format!("composed{}", parts.len()),
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn transient_failures_are_retried_with_backoff_and_the_event_is_delivered() {
    let judge = Scripted::new(vec![Script::Transient, Script::Transient, Script::Ok]);
    let ledger = MemoryLedger::new();
    let mut bus = Bus::new(&judge, &ledger, TokioSleeper, config(5, 100));
    let a = bus.subscribe(sub()).unwrap();

    let started = Instant::now();
    let (run, got) = tokio::join!(bus.run(stream::iter([event("e1")])), a.collect::<Vec<_>>());
    run.unwrap();

    assert_eq!(got.len(), 1);
    assert_eq!(judge.calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        states(&ledger, "e1"),
        [
            "judging1",
            "retrying1",
            "judging2",
            "retrying2",
            "judging3",
            "judged",
            "routed:delivered",
            "settled"
        ]
    );
    // 100ms + 200ms of backoff elapsed on the paused clock.
    assert!(started.elapsed() >= Duration::from_millis(300));
}

#[tokio::test(start_paused = true)]
async fn exhausted_attempts_dead_letter_the_event_with_its_attempt_count() {
    let judge = Scripted::new(vec![
        Script::Transient,
        Script::Transient,
        Script::Transient,
    ]);
    let ledger = MemoryLedger::new();
    let mut bus = Bus::new(&judge, &ledger, TokioSleeper, config(3, 100));
    let a = bus.subscribe(sub()).unwrap();
    let dead = bus.dead_letters().unwrap();

    let (run, got, dead) = tokio::join!(
        bus.run(stream::iter([event("e1")])),
        a.collect::<Vec<_>>(),
        dead.collect::<Vec<_>>()
    );
    run.unwrap();

    assert!(got.is_empty());
    let Some(letter) = dead.first() else {
        return assert_eq!(dead.len(), 1);
    };
    assert_eq!(letter.attempts(), Attempt::FIRST.next().next());
    assert!(matches!(
        letter.cause(),
        JudgeFailure::Judge(JudgeError::Rejected { status: 503, .. })
    ));
    assert_eq!(
        states(&ledger, "e1"),
        [
            "judging1",
            "retrying1",
            "judging2",
            "retrying2",
            "judging3",
            "failed3",
            "dead:delivered"
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn permanent_failure_dead_letters_at_once_without_retry() {
    let judge = Scripted::new(vec![Script::Permanent]);
    let ledger = MemoryLedger::new();
    let mut bus = Bus::new(&judge, &ledger, TokioSleeper, config(5, 100));
    let a = bus.subscribe(sub()).unwrap();

    let (run, _) = tokio::join!(bus.run(stream::iter([event("e1")])), a.collect::<Vec<_>>());
    run.unwrap();

    assert_eq!(judge.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        states(&ledger, "e1"),
        ["judging1", "failed1", "dead:unhandled"]
    );
}

#[tokio::test(start_paused = true)]
async fn a_hanging_judge_times_out_and_is_retried() {
    let judge = Scripted::new(vec![Script::Hang, Script::Ok]);
    let ledger = MemoryLedger::new();
    let mut bus = Bus::new(&judge, &ledger, TokioSleeper, config(5, 100));
    let a = bus.subscribe(sub()).unwrap();

    let (run, got) = tokio::join!(bus.run(stream::iter([event("e1")])), a.collect::<Vec<_>>());
    run.unwrap();

    assert_eq!(got.len(), 1);
    let retry_cause = ledger
        .records()
        .unwrap()
        .into_iter()
        .find_map(|r| match r.entry {
            Entry::Lifecycle(EventState::Retrying { cause, .. }) => Some(cause),
            _ => None,
        });
    assert!(matches!(retry_cause, Some(JudgeError::TimedOut { .. })));
}

#[tokio::test(start_paused = true)]
async fn open_breaker_makes_later_events_wait_instead_of_spending_attempts() {
    // Threshold 2: the first event's two transient failures open the breaker
    // with a 10s cooldown. The second event must wait it out, then succeed on
    // its first attempt rather than failing into retries of its own.
    let judge = Scripted::new(vec![
        Script::Transient,
        Script::Transient,
        Script::Ok,
        Script::Ok,
    ]);
    let ledger = MemoryLedger::new();
    let mut cfg = config(5, 2);
    cfg.concurrency = NonZeroUsize::new(1).unwrap();
    let mut bus = Bus::new(&judge, &ledger, TokioSleeper, cfg);
    let a = bus.subscribe(sub()).unwrap();

    let started = Instant::now();
    let (run, got) = tokio::join!(
        bus.run(stream::iter([event("e1"), event("e2")])),
        a.collect::<Vec<_>>()
    );
    run.unwrap();

    assert_eq!(got.len(), 2);
    assert_eq!(judge.calls.load(Ordering::SeqCst), 4);
    assert_eq!(
        states(&ledger, "e2"),
        ["judging1", "judged", "routed:delivered", "settled"]
    );
    // e1: 100ms backoff, then 10s cooldown before attempt 3 (the probe).
    assert!(started.elapsed() >= Duration::from_secs(10));
}

#[tokio::test(start_paused = true)]
async fn dead_letter_stream_can_only_be_taken_once() {
    let judge = Scripted::new(vec![]);
    let mut bus = Bus::new(&judge, MemoryLedger::new(), TokioSleeper, config(1, 1));
    assert!(bus.dead_letters().is_ok());
    assert!(bus.dead_letters().is_err());
}

#[test]
fn unhandled_outcome_is_distinct_from_unsubscribed() {
    assert_ne!(Outcome::Unhandled, Outcome::Unsubscribed);
}
