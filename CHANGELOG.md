# Changelog

All notable changes to this crate are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the crate follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- Jev's protocol is a trait: `jev::SystemOne` (`ask :: Request -> Either
  JudgeError Response`). `JevJudge<P>` is generic over it, so a gateway,
  another vendor speaking the same protocol, or a test double plugs in
  without the network stack. The HTTPS implementation moved to
  `jev::http::HttpSystemOne`; `JevJudge::from_env()` and `::new(key)` are
  unchanged. The `jev` feature now gates only the HTTPS code.

### Added

- Answer cache: `Cached<J, C>` judge decorator, `Cache` trait for external
  backends, in-process `MemoryLru`, `Ttl` (default 24 h), `CacheKey` digest of
  payload and question set, hit/miss/fault `CacheStats`.
- `Clock` time boundary with `SystemClock` and `TokioClock`; `Answer`,
  `AnswerSet` and `QuestionName` are now `serde` (de)serializable.
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
