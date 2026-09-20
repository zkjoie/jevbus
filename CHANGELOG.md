# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the crate follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `merge` example: bursts of messages from the same user are merged before
  judgment, and the ledger shows the lineage of the merged event.

## [0.1.0] - 2026-09-21

### Added

- Streaming `Bus`: `Stream<Event>` in, per-subscription `Subscriber` streams
  out, one judge request per event, pipelined with input order preserved.
- `Judge` and `Ledger` traits; `MemoryLedger`.
- Pure routing core with `FanOut` and `Exclusive` policies.
- Typestate event lifecycle (`Tracked<Judging | Retrying | Judged | Routed |
  Failed | Settled>`) with retry, backoff, timeout and a `DeadLetters` stream.
- Typestate circuit breaker (`Breaker<Closed | Open | HalfOpen>`) shared by
  in-flight calls.
- Asynchronous merge stage `merge::windowed` with the `Combiner` trait and
  event lineage (`Event::parts`, `Composed` ledger rows).
- `Sleeper` time boundary with `TokioSleeper`.
- `jev` feature: `JevJudge` over TypeSafe AI's System One HTTP API, wire
  format verified against `jev-1.13.0`.
