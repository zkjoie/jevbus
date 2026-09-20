//! Integration: a pull-based source with acknowledgements, and sinks other
//! than the in-process channel.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use jevbus::{
    Ack, AckError, Answer, AnswerSet, Bus, BusConfig, DeadLetter, Delivery, Entry, Envelope, Event,
    EventId, Judge, JudgeError, MemoryLedger, Outcome, Payload, Probability, QuestionSet, Sink,
    SinkError, Source, SourceError, Subscription, SubscriptionId, Thresholds, TokioSleeper,
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

/// Answers nothing, permanently.
struct Refusing;

#[async_trait]
impl Judge for Refusing {
    async fn judge(&self, _: &Payload, _: &QuestionSet) -> Result<AnswerSet, JudgeError> {
        Err(JudgeError::Rejected {
            status: 401,
            message: "no".into(),
        })
    }
}

/// An ack handle that appends the event id to a shared log.
struct LoggedAck {
    id: String,
    log: Arc<Mutex<Vec<String>>>,
    fail: bool,
}

#[async_trait]
impl Ack for LoggedAck {
    async fn ack(self) -> Result<(), AckError> {
        if self.fail {
            return Err(AckError::Rejected {
                reason: "late".into(),
            });
        }
        self.log.lock().unwrap().push(self.id);
        Ok(())
    }
}

/// A source over a queue; optionally fails after the queue drains.
struct QueueSource {
    queue: VecDeque<Event>,
    acks: Arc<Mutex<Vec<String>>>,
    fail_at_end: bool,
    fail_ack_for: Option<String>,
}

#[async_trait]
impl Source for QueueSource {
    type Ack = LoggedAck;

    async fn next(&mut self) -> Result<Option<Envelope<LoggedAck>>, SourceError> {
        match self.queue.pop_front() {
            Some(event) => {
                let id = event.id().as_str().to_owned();
                let fail = self.fail_ack_for.as_deref() == Some(id.as_str());
                Ok(Some(Envelope::new(
                    event,
                    LoggedAck {
                        id,
                        log: Arc::clone(&self.acks),
                        fail,
                    },
                )))
            }
            None if self.fail_at_end => Err(SourceError::Unavailable {
                reason: "broker gone".into(),
            }),
            None => Ok(None),
        }
    }
}

/// A sink that collects what it is given.
struct Collecting<T>(Mutex<Vec<T>>);

impl<T> Collecting<T> {
    fn new() -> Self {
        Collecting(Mutex::new(Vec::new()))
    }
}

#[async_trait]
impl<T: Send + 'static> Sink<T> for Collecting<T> {
    async fn publish(&self, item: T) -> Result<(), SinkError> {
        self.0.lock().unwrap().push(item);
        Ok(())
    }
}

/// A sink that is down.
struct Down;

#[async_trait]
impl<T: Send + 'static> Sink<T> for Down {
    async fn publish(&self, _: T) -> Result<(), SinkError> {
        Err(SinkError::Unavailable {
            reason: "down".into(),
        })
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

fn source(ids: &[&str]) -> (QueueSource, Arc<Mutex<Vec<String>>>) {
    let acks = Arc::new(Mutex::new(Vec::new()));
    let source = QueueSource {
        queue: ids.iter().map(|id| event(id)).collect(),
        acks: Arc::clone(&acks),
        fail_at_end: false,
        fail_ack_for: None,
    };
    (source, acks)
}

fn ack_results(ledger: &MemoryLedger) -> Vec<(String, bool)> {
    ledger
        .records()
        .unwrap()
        .into_iter()
        .filter_map(|r| match r.entry {
            Entry::Acknowledged { result } => Some((r.event.as_str().to_owned(), result.is_ok())),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_source_is_acknowledged_in_order_after_each_event_settles() {
    let ledger = MemoryLedger::new();
    let sink = Arc::new(Collecting::<Delivery>::new());
    let mut bus = Bus::new(Certain, &ledger, TokioSleeper, BusConfig::default());
    bus.subscribe_to(sub(), Arc::clone(&sink)).unwrap();
    let (source, acks) = source(&["e1", "e2", "e3"]);

    bus.run_source(source).await.unwrap();

    assert_eq!(*acks.lock().unwrap(), ["e1", "e2", "e3"]);
    assert_eq!(sink.0.lock().unwrap().len(), 3);
    // Every ack row comes after that event's Settled row.
    let rows: Vec<String> = ledger
        .records()
        .unwrap()
        .iter()
        .filter(|r| r.event.as_str() == "e2")
        .map(|r| match &r.entry {
            Entry::Acknowledged { .. } => "ack".to_owned(),
            Entry::Lifecycle(state) => format!("{state:?}")
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_owned(),
            other => format!("{other:?}")
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_owned(),
        })
        .collect();
    assert_eq!(rows.last().map(String::as_str), Some("ack"));
}

#[tokio::test]
async fn a_dead_lettered_event_is_still_acknowledged_and_goes_to_the_dead_letter_sink() {
    let ledger = MemoryLedger::new();
    let dead = Arc::new(Collecting::<DeadLetter>::new());
    let mut bus = Bus::new(Refusing, &ledger, TokioSleeper, BusConfig::default());
    bus.subscribe_to(sub(), Collecting::<Delivery>::new())
        .unwrap();
    bus.dead_letters_to(Arc::clone(&dead)).unwrap();
    let (source, acks) = source(&["e1"]);

    bus.run_source(source).await.unwrap();

    assert_eq!(dead.0.lock().unwrap().len(), 1);
    assert_eq!(*acks.lock().unwrap(), ["e1"]);
}

#[tokio::test]
async fn an_ack_the_source_rejects_is_recorded_not_fatal() {
    let ledger = MemoryLedger::new();
    let mut bus = Bus::new(Certain, &ledger, TokioSleeper, BusConfig::default());
    bus.subscribe_to(sub(), Collecting::<Delivery>::new())
        .unwrap();
    let (mut source, _) = source(&["e1", "e2"]);
    source.fail_ack_for = Some("e1".into());

    bus.run_source(source).await.unwrap();

    assert_eq!(
        ack_results(&ledger),
        [("e1".to_owned(), false), ("e2".to_owned(), true)]
    );
}

#[tokio::test]
async fn a_failing_source_stops_the_run_after_acknowledging_what_it_delivered() {
    let ledger = MemoryLedger::new();
    let mut bus = Bus::new(Certain, &ledger, TokioSleeper, BusConfig::default());
    bus.subscribe_to(sub(), Collecting::<Delivery>::new())
        .unwrap();
    let (mut source, acks) = source(&["e1"]);
    source.fail_at_end = true;

    let result = bus.run_source(source).await;

    assert!(matches!(
        result,
        Err(jevbus::RunError::Source(SourceError::Unavailable { .. }))
    ));
    assert_eq!(*acks.lock().unwrap(), ["e1"]);
}

#[tokio::test]
async fn a_sink_that_is_down_is_recorded_as_sink_failed() {
    let ledger = MemoryLedger::new();
    let mut bus = Bus::new(Certain, &ledger, TokioSleeper, BusConfig::default());
    bus.subscribe_to(sub(), Down).unwrap();

    bus.run(futures::stream::iter([event("e1")])).await.unwrap();

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

#[tokio::test]
async fn dead_letter_sink_can_only_be_registered_once() {
    let mut bus = Bus::new(
        Certain,
        MemoryLedger::new(),
        TokioSleeper,
        BusConfig::default(),
    );
    assert!(bus.dead_letters_to(Collecting::<DeadLetter>::new()).is_ok());
    assert!(bus.dead_letters().is_err());
}
