//! Integration: an event stream in, subscriber streams out, ledger complete.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use futures::{stream, StreamExt};
use jevbus::routing::exclusive_name;
use jevbus::{
    Answer, AnswerSet, Bus, BusConfig, Disposition, Entry, Event, EventId, EventState, Judge,
    JudgeError, MemoryLedger, Outcome, Payload, Policy, Probability, QuestionSet, Subscription,
    SubscriptionId, Thresholds, TokioSleeper,
};

/// A judge scripted by payload text. Under `FanOut` it answers each question
/// from a per-subscription table; under `Exclusive` it answers the single
/// choice question from the same table.
struct Scripted {
    table: BTreeMap<&'static str, BTreeMap<&'static str, f64>>,
    calls: AtomicUsize,
    delay_ms: u64,
}

#[async_trait]
impl Judge for Scripted {
    async fn judge(
        &self,
        payload: &Payload,
        questions: &QuestionSet,
    ) -> Result<AnswerSet, JudgeError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.delay_ms > 0 {
            // Later events finish first when concurrency > 1; order must survive.
            let jitter = (payload.as_str().len() as u64 % 3) * self.delay_ms;
            tokio::time::sleep(Duration::from_millis(jitter)).await;
        }
        let row = self
            .table
            .get(payload.as_str())
            .cloned()
            .unwrap_or_default();
        Ok(questions
            .iter()
            .map(|(name, question)| {
                let answer = match question {
                    jevbus::Question::Noul { .. } => Answer::Noul {
                        probability: p(*row.get(name.as_str()).unwrap_or(&0.0)),
                    },
                    jevbus::Question::Choice { criteria, .. } => Answer::Choice {
                        choice: String::new(),
                        probabilities: criteria
                            .keys()
                            .map(|k| (k.clone(), p(*row.get(k.as_str()).unwrap_or(&0.0))))
                            .collect(),
                        confidence: None,
                    },
                    jevbus::Question::Score { levels, .. } => Answer::Score {
                        level: levels.first().cloned().unwrap_or_default(),
                        score: 0.0,
                        distribution: BTreeMap::new(),
                        confidence: None,
                    },
                };
                (name.clone(), answer)
            })
            .collect())
    }
}

fn p(v: f64) -> Probability {
    Probability::new(v).unwrap()
}

fn sub(id: &str) -> Subscription {
    Subscription::new(
        SubscriptionId::new(id).unwrap(),
        format!("about {id}"),
        Thresholds::new(p(0.7), p(0.4)).unwrap(),
    )
}

fn event(text: &str) -> Event {
    Event::new(
        EventId::new(format!("id:{text}")).unwrap(),
        Payload::new(text),
    )
}

fn scripted(delay_ms: u64) -> Scripted {
    Scripted {
        table: BTreeMap::from([
            ("e1", BTreeMap::from([("a", 0.9), ("b", 0.1)])),
            ("e22", BTreeMap::from([("a", 0.2), ("b", 0.95)])),
            ("e333", BTreeMap::from([("a", 0.5), ("b", 0.8)])),
        ]),
        calls: AtomicUsize::new(0),
        delay_ms,
    }
}

fn config(policy: Policy, concurrency: usize) -> BusConfig {
    BusConfig {
        policy,
        concurrency: NonZeroUsize::new(concurrency).unwrap(),
        channel_capacity: 4,
        ..BusConfig::default()
    }
}

#[tokio::test]
async fn fan_out_routes_each_event_to_a_or_b_and_preserves_input_order() {
    let mut bus = Bus::new(
        scripted(10),
        MemoryLedger::new(),
        TokioSleeper,
        config(Policy::FanOut, 4),
    );
    let a = bus.subscribe(sub("a")).unwrap();
    let (b, b_review) = bus.subscribe_with_review(sub("b")).unwrap();

    let (run, got_a, got_b, got_b_review) = tokio::join!(
        bus.run(stream::iter([event("e1"), event("e22"), event("e333")])),
        a.collect::<Vec<_>>(),
        b.collect::<Vec<_>>(),
        b_review.collect::<Vec<_>>()
    );
    run.unwrap();

    let ids = |d: &[jevbus::Delivery]| {
        d.iter()
            .map(|x| x.event().id().as_str().to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&got_a), ["id:e1"]);
    assert_eq!(ids(&got_b), ["id:e22", "id:e333"]);
    assert!(got_b_review.is_empty());
    assert!(got_a
        .iter()
        .all(|d| d.verdict().disposition == Disposition::Deliver));
}

#[tokio::test]
async fn ledger_records_every_subscription_for_every_event_including_drops_and_unreviewed() {
    let ledger = MemoryLedger::new();
    let mut bus = Bus::new(
        scripted(0),
        &ledger,
        TokioSleeper,
        config(Policy::FanOut, 1),
    );
    let a = bus.subscribe(sub("a")).unwrap();
    let b = bus.subscribe(sub("b")).unwrap();
    drop(b); // b unsubscribes before the run starts

    let (run, _) = tokio::join!(
        bus.run(stream::iter([event("e1"), event("e333")])),
        a.collect::<Vec<_>>()
    );
    run.unwrap();

    let records = ledger.records().unwrap();
    let outcomes: Vec<(String, Outcome)> = records
        .iter()
        .filter_map(|r| match &r.entry {
            Entry::Routed {
                subscription,
                outcome,
                ..
            } => Some((subscription.as_str().to_owned(), *outcome)),
            _ => None,
        })
        .collect();
    assert_eq!(outcomes.len(), 4, "2 events x 2 subscriptions");
    assert_eq!(
        outcomes,
        [
            ("a".to_owned(), Outcome::Delivered),
            ("b".to_owned(), Outcome::Dropped),
            ("a".to_owned(), Outcome::Unreviewed),
            ("b".to_owned(), Outcome::Unsubscribed),
        ]
    );
}

#[tokio::test]
async fn exclusive_policy_delivers_to_exactly_one_of_a_or_b() {
    let ledger = MemoryLedger::new();
    let mut bus = Bus::new(
        scripted(0),
        &ledger,
        TokioSleeper,
        config(Policy::Exclusive, 2),
    );
    let a = bus.subscribe(sub("a")).unwrap();
    let b = bus.subscribe(sub("b")).unwrap();

    let (run, got_a, got_b) = tokio::join!(
        bus.run(stream::iter([event("e1"), event("e22"), event("e333")])),
        a.collect::<Vec<_>>(),
        b.collect::<Vec<_>>()
    );
    run.unwrap();

    assert_eq!(got_a.len(), 1);
    assert_eq!(got_b.len(), 2);
    let per_event_deliveries = ledger
        .records()
        .unwrap()
        .iter()
        .filter(|r| {
            matches!(
                r.entry,
                Entry::Routed {
                    outcome: Outcome::Delivered,
                    ..
                }
            )
        })
        .count();
    assert_eq!(per_event_deliveries, 3);
}

#[tokio::test]
async fn judge_failure_is_recorded_per_event_and_does_not_stop_the_run() {
    struct Failing;
    #[async_trait]
    impl Judge for Failing {
        async fn judge(&self, _: &Payload, _: &QuestionSet) -> Result<AnswerSet, JudgeError> {
            // Permanent: a bad key is not fixed by retrying.
            Err(JudgeError::Rejected {
                status: 401,
                message: "bad key".into(),
            })
        }
    }
    let ledger = MemoryLedger::new();
    let mut bus = Bus::new(Failing, &ledger, TokioSleeper, config(Policy::FanOut, 1));
    let a = bus.subscribe(sub("a")).unwrap();
    let (run, got) = tokio::join!(
        bus.run(stream::iter([event("e1"), event("e22")])),
        a.collect::<Vec<_>>()
    );
    run.unwrap();
    assert!(got.is_empty());
    let failed = ledger
        .records()
        .unwrap()
        .iter()
        .filter(|r| matches!(r.entry, Entry::Lifecycle(EventState::Failed { .. })))
        .count();
    assert_eq!(failed, 2);
}

#[tokio::test]
async fn no_subscriptions_means_no_judge_calls() {
    let judge = scripted(0);
    let bus = Bus::new(
        &judge,
        MemoryLedger::new(),
        TokioSleeper,
        BusConfig::default(),
    );
    bus.run(stream::iter([event("e1")])).await.unwrap();
    assert_eq!(judge.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn one_judge_call_per_event_regardless_of_subscription_count() {
    let judge = scripted(0);
    let mut bus = Bus::new(
        &judge,
        MemoryLedger::new(),
        TokioSleeper,
        config(Policy::FanOut, 3),
    );
    let a = bus.subscribe(sub("a")).unwrap();
    let b = bus.subscribe(sub("b")).unwrap();
    let (run, _, _) = tokio::join!(
        bus.run(stream::iter([event("e1"), event("e22"), event("e333")])),
        a.collect::<Vec<_>>(),
        b.collect::<Vec<_>>()
    );
    run.unwrap();
    assert_eq!(judge.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn a_composite_event_records_its_lineage_before_judgment() {
    let ledger = MemoryLedger::new();
    let mut bus = Bus::new(
        scripted(0),
        &ledger,
        TokioSleeper,
        config(Policy::FanOut, 1),
    );
    let a = bus.subscribe(sub("a")).unwrap();
    let parts = [event("e1"), event("e22")];
    let merged = Event::merged(EventId::new("m").unwrap(), Payload::new("e1"), &parts);

    let (run, got) = tokio::join!(bus.run(stream::iter([merged])), a.collect::<Vec<_>>());
    run.unwrap();

    assert_eq!(got.len(), 1);
    let first = ledger.records().unwrap().into_iter().next().unwrap();
    assert_eq!(first.event.as_str(), "m");
    let lineage = match first.entry {
        Entry::Composed { parts } => parts
            .iter()
            .map(|p| p.as_str().to_owned())
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    };
    assert_eq!(lineage, ["id:e1", "id:e22"]);
}

#[test]
fn exclusive_question_name_is_stable() {
    assert_eq!(exclusive_name().as_str(), "route");
}
