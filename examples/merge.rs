//! Merges bursts of messages from the same user before judging them.
//!
//! Three messages from `u1` arrive within one window and reach the judge as
//! a single event; `u2`'s lone message goes through on its own. The ledger
//! shows the lineage of the merged event.
//!
//! Run with `cargo run --example merge`. The key is read from `JEV_KEY`,
//! loaded from a `.env` file in the working directory when present.

#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::num::NonZeroUsize;
use std::process::ExitCode;
use std::time::Duration;

use futures::{stream, StreamExt};
use jevbus::jev::JevJudge;
use jevbus::merge::{windowed, ConcatByKey, WindowPolicy};
use jevbus::{
    Bus, BusConfig, Entry, Event, EventId, MemoryLedger, Payload, Probability, Subscription,
    SubscriptionId, Thresholds, TokioSleeper,
};

/// Messages are `user: text`; the user is the merge key.
fn user_of(event: &Event) -> Option<String> {
    event
        .payload()
        .as_str()
        .split_once(": ")
        .map(|(user, _)| user.to_owned())
}

fn events() -> Vec<Event> {
    [
        ("m1", "u1: hi, I was charged twice"),
        ("m2", "u1: order 4412"),
        ("m3", "u2: how do I change my avatar?"),
        ("m4", "u1: can you refund the second charge?"),
    ]
    .into_iter()
    .filter_map(|(id, text)| Some(Event::new(EventId::new(id).ok()?, Payload::new(text))))
    .collect()
}

fn subscription(id: &str, description: &str) -> Option<Subscription> {
    let thresholds =
        Thresholds::new(Probability::new(0.7).ok()?, Probability::new(0.4).ok()?).ok()?;
    Some(Subscription::new(
        SubscriptionId::new(id).ok()?,
        description,
        thresholds,
    ))
}

#[tokio::main]
async fn main() -> ExitCode {
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
    let Ok(mut billing) = bus.subscribe(billing) else {
        return ExitCode::FAILURE;
    };
    let consumer = tokio::spawn(async move {
        while let Some(d) = billing.next().await {
            println!(
                "[billing] {} (from {} parts) p={}",
                d.event().id(),
                d.event().parts().len(),
                d.verdict().probability
            );
        }
    });

    let Some(max_parts) = NonZeroUsize::new(10) else {
        return ExitCode::FAILURE;
    };
    let merged = windowed(
        stream::iter(events()),
        ConcatByKey::new(user_of),
        TokioSleeper,
        WindowPolicy {
            span: Duration::from_secs(2),
            max_parts,
        },
    )
    .filter_map(|item| async move {
        match item {
            Ok(event) => Some(event),
            Err(failure) => {
                eprintln!(
                    "merge failed for {} events: {}",
                    failure.parts.len(),
                    failure.cause
                );
                None
            }
        }
    });

    let run = bus.run(merged).await;
    let _ = consumer.await;
    if let Err(e) = run {
        eprintln!("bus stopped: {e}");
        return ExitCode::FAILURE;
    }
    match ledger.records() {
        Ok(records) => {
            for r in records
                .iter()
                .filter(|r| matches!(r.entry, Entry::Composed { .. }))
            {
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
