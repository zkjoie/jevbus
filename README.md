# jevbus

A streaming event bus whose routing, subscription and consumption are decided
by a probabilistic judge. The reference judge is TypeSafe AI's **Jev**
(System One) model: send it a payload and a set of typed questions, get back
calibrated probabilities instead of prose.

```
EventStream ──▶ Bus ──▶ Judge (one request per event, one question per subscription)
                 │
                 ├──▶ Subscriber A   (Stream<Delivery>)
                 ├──▶ Subscriber B   (Stream<Delivery>)
                 ├──▶ Reviewer B     (Stream<Delivery>, optional review band)
                 └──▶ Ledger         (every outcome, including drops)
```

## Model

| Concept        | Type                | Invariant / law                                   |
|----------------|---------------------|---------------------------------------------------|
| Event          | `Event`             | id non-empty; payload opaque text                 |
| Subscription   | `Subscription`      | plain-language description + examples + policy    |
| Policy         | `Thresholds`        | `review <= deliver`; `dispose` is monotone        |
| Judgment       | `Probability`       | in `[0, 1]`, never NaN                            |
| Decision       | `Disposition`       | `Drop < Review < Deliver`                         |
| Competition    | `Policy`            | `FanOut` (many may match) or `Exclusive` (argmax) |

Thresholds are policy, not model: changing them never needs a new judgment.

## Layers

* **Pure core** `routing`: `plan(subs) -> QuestionSet` and
  `decide(subs, answers) -> Vec<Verdict>`. No IO. Replayable from a recorded
  answer set.
* **Boundaries**: `Judge` and `Ledger` are traits; consumers are plain
  `futures::Stream`s fed by bounded channels, so a slow consumer applies
  backpressure all the way back to the input stream.
* **Adapter** `jev` (feature, on by default): `JevJudge` over HTTPS. The only
  module that does network IO.

## Streaming semantics

* One judge request per event regardless of subscription count.
* Judge calls for consecutive events are pipelined (`BusConfig::concurrency`)
  and outputs stay in input order.
* A judge failure is recorded for that event and the run continues. A ledger
  failure stops the run: an unaudited delivery is worse than none.
* Dropping a `Subscriber` unsubscribes; later matches are recorded as
  `Outcome::Unsubscribed`.

## Merging events

`merge::windowed` is an asynchronous stage placed in front of the bus:

```rust
use jevbus::merge::{windowed, ConcatByKey, WindowPolicy};

let by_user = ConcatByKey::new(|e: &Event| user_of(e));   // None => pass through
let merged = windowed(raw_events, by_user, TokioSleeper, WindowPolicy {
    span: Duration::from_secs(5),
    max_parts: NonZeroUsize::new(10)?,
})
.filter_map(|item| async { item.ok() });                  // or route MergeFailure elsewhere

bus.run(merged).await?;
```

* A `Combiner` assigns each event a group key and folds a group into one
  event with `async fn combine`, so merging may consult external state.
* A group flushes when its window elapses, when it reaches `max_parts`, or
  when the input ends. A timer from an earlier group with the same key never
  flushes a later one.
* The merged event's `Event::parts` lists its constituents; the bus records
  them as a `Composed` ledger row, so verdicts and dead letters trace back to
  the original events.
* Nothing is lost: a group the combiner rejects comes out as a
  `MergeFailure` carrying every event it held.

## Caching answers

```rust
use jevbus::{Cached, MemoryLru, SystemClock, Ttl};

let cache = MemoryLru::new(NonZeroUsize::new(10_000)?, SystemClock);
let judge = Cached::new(JevJudge::from_env()?, cache, Ttl::default());   // 24 h
let mut bus = Bus::new(judge, ledger, TokioSleeper, BusConfig::default());
```

* `Cached<J, C>` is a `Judge`; the bus does not know it is there.
* The key is a SHA-256 digest of the payload and the question set. The
  question set is fixed for a run and changes with the subscriptions, so an
  edited subscription never hits a stale answer.
* Only successful answer sets are cached. Errors go through retry and the
  breaker as usual.
* A failing cache degrades to a miss; faults are counted in `CacheStats`
  alongside hits and misses, never swallowed silently.
* `Cache` is a trait: implement `get` and `put` over memcached, Redis or
  anything else; `AnswerSet` serialises with `serde`.

## Event state and an unavailable judge

Each event walks a state machine (`lifecycle`) whose phases are **types**,
Haskell-style: `Tracked<Judging>`, `Tracked<Retrying>`, `Tracked<Judged>`,
`Tracked<Routed>`, `Tracked<Failed>`, `Tracked<Settled>`. A transition
consumes one phase and returns the next; where the outcome depends on the
judge it returns a nested `Either`, written with `Result`:

```text
conclude :: Tracked Judging -> Either (Tracked Failed) (Either (Tracked Retrying) (Tracked Judged))
decide   :: Tracked Judged  -> Either (Tracked Failed) (Tracked Routed)
```

Calling `settle` on a retrying event does not compile. The ledger stores a
data view (`EventState`) of every transition:

```
Judging(n) ──ok──▶ Judged ──dispatched──▶ Settled
    │
    ├──transient error, budget left──▶ Retrying(n, delay) ──slept──▶ Judging(n+1)
    │
    └──permanent error, or budget spent──▶ Failed(n, cause) ──▶ dead-letter stream
```

* **Transient vs permanent** is a predicate on `JudgeError`: connection
  failures, timeouts, 408/429 and 5xx are transient; other 4xx and malformed
  replies are not and dead-letter at once.
* **Retry** is exponential backoff with a cap (`RetryPolicy`), bounded or
  unbounded attempts, and a per-call timeout.
* **Breaker** (`breaker::Breaker<Closed | Open | HalfOpen>`) is typed the
  same way: only `Breaker<Open>` has a cooldown or can be probed, and
  `Breaker<Closed>::fail` returns `Either (Breaker Open) (Breaker Closed)`.
  The mutex slot that all in-flight calls share holds
  `Either (Either Closed Open) HalfOpen`. It is shared by all in-flight calls. After
  `failure_threshold` consecutive transient failures it opens, and every
  later event waits out the cooldown before its first attempt instead of
  burning its own retry budget. One success closes it; a failed probe doubles
  the cooldown.
* **Dead letters** are a stream (`Bus::dead_letters`) carrying the event, the
  attempt count and the final cause, for replay or alerting. If nobody takes
  the stream, failures are still recorded as `Outcome::Unhandled`.
* **Time** is injected through the `Sleeper` trait; `TokioSleeper` is the
  provided implementation, and tests run on a paused clock.

## Usage

```rust
use futures::{stream, StreamExt};
use jevbus::jev::JevJudge;
use jevbus::{Bus, BusConfig, MemoryLedger, Probability, Subscription, SubscriptionId, Thresholds, TokioSleeper};

let judge = JevJudge::from_env()?;                    // reads JEV_KEY
let mut bus = Bus::new(judge, MemoryLedger::new(), TokioSleeper, BusConfig::default());

let thresholds = Thresholds::new(Probability::new(0.7)?, Probability::new(0.4)?)?;
let mut a = bus.subscribe(Subscription::new(
    SubscriptionId::new("billing")?, "a billing or refund problem", thresholds,
))?;
let mut b = bus.subscribe(Subscription::new(
    SubscriptionId::new("incident")?, "a production outage", thresholds,
))?;

tokio::spawn(async move { while let Some(d) = a.next().await { /* consume A */ } });
tokio::spawn(async move { while let Some(d) = b.next().await { /* consume B */ } });

bus.run(stream::iter(events)).await?;
```

Run the example against the live API (the key is read from `.env` or the environment):

```bash
cargo run --example stream
```

## Development

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

The crate denies `missing_docs`, `unsafe_code`, and clippy's `unwrap_used`,
`expect_used`, `panic`, `todo`, `unimplemented`, `unreachable` and
`indexing_slicing` on production code.

## Wire format

Verified against live `jev-1.13.0` responses on 2026-09-21:

| Question | Request                                   | Answer                                                        |
|----------|-------------------------------------------|---------------------------------------------------------------|
| noul     | `{type, instructions}`                    | `{type, noul}`                                                |
| choice   | `{type, instructions, criteria: {k: v}}`  | `{type, choice, confidence, probabilities: {k: p}}`           |
| score    | `{type, instructions, criteria: [level]}` | `{type, score, confidence, legend: {i: level}, probabilities}`|

`jev::wire::decode` re-keys score distributions by level name and validates
every probability; a bad value surfaces as `JudgeError::MalformedAnswer`
naming the question.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you explicitly state
otherwise, any contribution intentionally submitted for inclusion in this
crate by you, as defined in the Apache-2.0 license, shall be dual licensed as
above, without any additional terms or conditions.
