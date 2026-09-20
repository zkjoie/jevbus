//! Streams a few support messages through Jev and prints where each lands.
//!
//! Run with `cargo run --example stream`. The key is read from `JEV_KEY`,
//! loaded from a `.env` file in the working directory when present.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::process::ExitCode;

use futures::{stream, StreamExt};
use jevbus::jev::JevJudge;
use jevbus::{
    Bus, BusConfig, Event, EventId, MemoryLedger, Payload, Probability, Subscription,
    SubscriptionId, Thresholds, TokioSleeper,
};

fn subscription(id: &str, description: &str) -> Option<Subscription> {
    let thresholds =
        Thresholds::new(Probability::new(0.7).ok()?, Probability::new(0.4).ok()?).ok()?;
    Some(Subscription::new(
        SubscriptionId::new(id).ok()?,
        description,
        thresholds,
    ))
}

fn events() -> Vec<Event> {
    [
        ("m1", "My card was charged twice for the same order."),
        (
            "m2",
            "The deploy failed and customers are seeing 500s right now!",
        ),
        ("m3", "How do I change the avatar on my profile?"),
    ]
    .into_iter()
    .filter_map(|(id, text)| Some(Event::new(EventId::new(id).ok()?, Payload::new(text))))
    .collect()
}

#[tokio::main]
async fn main() -> ExitCode {
    // A missing .env is not an error; the variable may already be exported.
    let _ = dotenvy::dotenv();
    let judge = match JevJudge::from_env() {
        Ok(judge) => judge,
        Err(e) => {
            eprintln!("cannot build judge: {e}");
            return ExitCode::FAILURE;
        }
    };
    let ledger = MemoryLedger::new();
    let mut bus = Bus::new(judge, &ledger, TokioSleeper, BusConfig::default());

    let Some(billing) = subscription("billing", "a billing, payment or refund problem") else {
        return ExitCode::FAILURE;
    };
    let Some(incident) = subscription(
        "incident",
        "a production outage or urgent technical failure",
    ) else {
        return ExitCode::FAILURE;
    };
    let (Ok(mut a), Ok(mut b)) = (bus.subscribe(billing), bus.subscribe(incident)) else {
        return ExitCode::FAILURE;
    };

    let consumer_a = tokio::spawn(async move {
        while let Some(d) = a.next().await {
            println!(
                "[A billing]  {} p={}",
                d.event().id(),
                d.verdict().probability
            );
        }
    });
    let consumer_b = tokio::spawn(async move {
        while let Some(d) = b.next().await {
            println!(
                "[B incident] {} p={}",
                d.event().id(),
                d.verdict().probability
            );
        }
    });

    let run = bus.run(stream::iter(events())).await;
    let _ = tokio::join!(consumer_a, consumer_b);
    if let Err(e) = run {
        eprintln!("bus stopped: {e}");
        return ExitCode::FAILURE;
    }
    match ledger.records() {
        Ok(records) => {
            for r in records {
                println!("ledger: {} {:?}", r.event, r.entry);
            }
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("ledger: {e}");
            ExitCode::FAILURE
        }
    }
}
