//! Integration: a bus as data, and two buses chained with acks flowing back.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::stream;
use jevbus::{
    link, Ack, AckError, Answer, AnswerSet, Blueprint, Bus, BusConfig, Delivery, Entry, Envelope,
    Event, EventId, Judge, JudgeError, MemoryLedger, Outcome, Payload, Probability, QuestionSet,
    Sink, SinkError, Source, SourceError, Subscription, SubscriptionId, Thresholds, TokioSleeper,
};

/// Answers every noul with 0.9.
struct Certain;

#[async_trait]
impl Judge for Certain {
    async fn judge(&self, _: &Payload, questions: &QuestionSet) -> Result<AnswerSet, JudgeError> {
        Ok(questions
            .iter()
            .map(|(name, _)| {
                (
                    name.clone(),
                    Answer::Noul {
                        probability: Probability::new(0.9).unwrap(),
                    },
                )
            })
            .collect())
    }
}

fn sub(id: &str) -> Subscription {
    Subscription::new(
        SubscriptionId::new(id).unwrap(),
        format!("about {id}"),
        Thresholds::new(
            Probability::new(0.7).unwrap(),
            Probability::new(0.4).unwrap(),
        )
        .unwrap(),
    )
    .with_examples(["one example"])
}

fn event(id: &str) -> Event {
    Event::new(EventId::new(id).unwrap(), Payload::new(id))
}

struct Collecting(Mutex<Vec<Delivery>>);

#[async_trait]
impl Sink<Delivery> for Collecting {
    async fn publish(&self, item: Delivery) -> Result<(), SinkError> {
        self.0.lock().unwrap().push(item);
        Ok(())
    }
}

/// A source whose acks are logged with a timestamp-free sequence.
struct Logged {
    queue: VecDeque<Event>,
    log: Arc<Mutex<Vec<String>>>,
}

struct LogAck {
    id: String,
    log: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Ack for LogAck {
    async fn ack(self) -> Result<(), AckError> {
        self.log.lock().unwrap().push(format!("ack:{}", self.id));
        Ok(())
    }
}

#[async_trait]
impl Source for Logged {
    type Ack = LogAck;
    async fn next(&mut self) -> Result<Option<Envelope<LogAck>>, SourceError> {
        Ok(self.queue.pop_front().map(|event| {
            let id = event.id().as_str().to_owned();
            Envelope::new(
                event,
                LogAck {
                    id,
                    log: Arc::clone(&self.log),
                },
            )
        }))
    }
}

#[test]
fn blueprint_round_trips_through_json_and_rebuilds_the_same_topology() {
    let mut bus = Bus::new(
        Certain,
        MemoryLedger::new(),
        TokioSleeper,
        BusConfig::default(),
    );
    let _a = bus.subscribe(sub("a")).unwrap();
    let _b = bus.subscribe_with_review(sub("b")).unwrap();
    let _dead = bus.dead_letters().unwrap();

    let blueprint = bus.blueprint();
    let json = serde_json::to_string_pretty(&blueprint).unwrap();
    let back: Blueprint = serde_json::from_str(&json).unwrap();
    assert_eq!(back, blueprint);

    let (rebuilt, handles) =
        Bus::from_blueprint(back, Certain, MemoryLedger::new(), TokioSleeper).unwrap();
    assert_eq!(rebuilt.blueprint(), blueprint);
    assert_eq!(handles.subscribers.len(), 2);
    assert_eq!(handles.reviewers.len(), 1);
    assert!(handles.dead_letters.is_some());
}

#[test]
fn blueprint_deserialisation_enforces_the_threshold_invariant() {
    let bad = r#"{"config":{"policy":"fan_out","concurrency":1,"channel_capacity":1,
        "retry":{"attempts":{"bounded":1},"base_delay":{"secs":1,"nanos":0},"max_delay":{"secs":1,"nanos":0},
                 "multiplier":2,"timeout":{"secs":1,"nanos":0}},
        "breaker":{"failure_threshold":1,"cooldown":{"secs":1,"nanos":0},"max_cooldown":{"secs":1,"nanos":0}}},
        "routes":[{"subscription":{"id":"a","description":"x","thresholds":{"deliver":0.2,"review":0.9}}}]}"#;
    let result: Result<Blueprint, _> = serde_json::from_str(bad);
    assert!(result.is_err(), "review above deliver must be rejected");
}

#[tokio::test]
async fn two_buses_chain_and_the_upstream_acks_only_after_the_downstream_has() {
    // Upstream A: source with an ack log -> subscription "pass" -> link.
    // Downstream B: link -> subscription "b" -> collecting sink.
    let log = Arc::new(Mutex::new(Vec::new()));
    let (sink_ab, source_b) = link(4);

    let ledger_a = MemoryLedger::new();
    let mut a = Bus::new(Certain, &ledger_a, TokioSleeper, BusConfig::default());
    a.subscribe_to(sub("pass"), sink_ab).unwrap();

    let ledger_b = MemoryLedger::new();
    let collected = Arc::new(Collecting(Mutex::new(Vec::new())));
    let mut b = Bus::new(Certain, &ledger_b, TokioSleeper, BusConfig::default());
    b.subscribe_to(sub("b"), Arc::clone(&collected)).unwrap();

    let source_a = Logged {
        queue: [event("e1"), event("e2")].into_iter().collect(),
        log: Arc::clone(&log),
    };
    let (ran_a, ran_b) = tokio::join!(a.run_source(source_a), b.run_source(source_b));
    ran_a.unwrap();
    ran_b.unwrap();

    let got: Vec<String> = collected
        .0
        .lock()
        .unwrap()
        .iter()
        .map(|d| d.event().id().to_string())
        .collect();
    assert_eq!(got, ["e1", "e2"]);
    assert_eq!(*log.lock().unwrap(), ["ack:e1", "ack:e2"]);

    // B settled e1 before A acked it: B's Settled row exists and A's Routed
    // row for e1 says Delivered, which the link only reports after B's ack.
    let a_outcomes: Vec<Outcome> = ledger_a
        .records()
        .unwrap()
        .into_iter()
        .filter_map(|r| match r.entry {
            Entry::Routed { outcome, .. } => Some(outcome),
            _ => None,
        })
        .collect();
    assert_eq!(a_outcomes, [Outcome::Delivered, Outcome::Delivered]);
    let b_settled = ledger_b
        .records()
        .unwrap()
        .iter()
        .filter(|r| matches!(r.entry, Entry::Lifecycle(jevbus::EventState::Settled)))
        .count();
    assert_eq!(b_settled, 2);
}

#[tokio::test]
async fn a_downstream_that_went_away_is_recorded_as_unsubscribed_upstream() {
    let (sink_ab, source_b) = link(4);
    drop(source_b);
    let ledger = MemoryLedger::new();
    let mut a = Bus::new(Certain, &ledger, TokioSleeper, BusConfig::default());
    a.subscribe_to(sub("pass"), sink_ab).unwrap();

    a.run(stream::iter([event("e1")])).await.unwrap();

    let outcomes: Vec<Outcome> = ledger
        .records()
        .unwrap()
        .into_iter()
        .filter_map(|r| match r.entry {
            Entry::Routed { outcome, .. } => Some(outcome),
            _ => None,
        })
        .collect();
    assert_eq!(outcomes, [Outcome::Unsubscribed]);
}

#[tokio::test]
async fn a_downstream_that_drops_an_event_unacked_is_recorded_as_sink_failed() {
    let (sink_ab, mut source_b) = link(4);
    let ledger = MemoryLedger::new();
    let mut a = Bus::new(Certain, &ledger, TokioSleeper, BusConfig::default());
    a.subscribe_to(sub("pass"), sink_ab).unwrap();

    let downstream = async {
        // Take the envelope and drop it without acking.
        let envelope = source_b.next().await.unwrap();
        drop(envelope);
    };
    let (ran, ()) = tokio::join!(a.run(stream::iter([event("e1")])), downstream);
    ran.unwrap();

    let outcomes: Vec<Outcome> = ledger
        .records()
        .unwrap()
        .into_iter()
        .filter_map(|r| match r.entry {
            Entry::Routed { outcome, .. } => Some(outcome),
            _ => None,
        })
        .collect();
    assert_eq!(outcomes, [Outcome::SinkFailed]);
}

#[test]
fn delivery_serialises_as_event_and_verdict() {
    let delivery: Delivery = serde_json::from_str(
        r#"{"event":{"id":"e1","payload":"hello"},
            "verdict":{"subscription":"a","probability":0.9,"disposition":"deliver"}}"#,
    )
    .unwrap();
    assert_eq!(delivery.event().id().as_str(), "e1");
    let json = serde_json::to_value(&delivery).unwrap();
    assert_eq!(
        json.pointer("/verdict/disposition"),
        Some(&serde_json::json!("deliver"))
    );
}
