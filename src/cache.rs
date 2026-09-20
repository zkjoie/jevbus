//! Caching judge answers.
//!
//! ```text
//! data CacheKey = CacheKey Digest              -- sha256 (payload, questions)
//! newtype Ttl   = Ttl Duration                  -- > 0
//!
//! class Cache c where
//!   get :: c -> CacheKey -> IO (Either CacheError (Maybe AnswerSet))
//!   put :: c -> CacheKey -> AnswerSet -> Ttl -> IO (Either CacheError ())
//!
//! data Cached j c = Cached { judge :: j, cache :: c, ttl :: Ttl }
//! instance (Judge j, Cache c) => Judge (Cached j c)
//! ```
//!
//! `Cached` is a decorator: the bus sees a [`Judge`]. A cache fault is never
//! a judgment failure; it degrades to a miss and is counted in
//! [`CacheStats`]. Only successful answer sets are stored.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::event::Payload;
use crate::judge::{Judge, JudgeError};
use crate::question::{AnswerSet, Question, QuestionSet};
use crate::time::Clock;

/// Identifies one (payload, question set) pair.
///
/// `Copy` law: a 32-byte value with no identity.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CacheKey([u8; 32]);

impl CacheKey {
    /// The digest of `payload` and `questions`.
    ///
    /// Law: equal inputs give equal keys; the encoding is length-prefixed so
    /// that no two distinct inputs share a byte string.
    pub fn of(payload: &Payload, questions: &QuestionSet) -> CacheKey {
        let mut hasher = Sha256::new();
        feed(&mut hasher, payload.as_str());
        for (name, question) in questions.iter() {
            feed(&mut hasher, name.as_str());
            feed_question(&mut hasher, question);
        }
        CacheKey(hasher.finalize().into())
    }

    /// The raw digest.
    pub fn bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lower-case hex, for backends that key by string.
    pub fn hex(&self) -> String {
        self.0.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

impl fmt::Debug for CacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CacheKey({})", self.hex())
    }
}

fn feed(hasher: &mut Sha256, text: &str) {
    hasher.update((text.len() as u64).to_le_bytes());
    hasher.update(text.as_bytes());
}

fn feed_question(hasher: &mut Sha256, question: &Question) {
    match question {
        Question::Noul { instructions } => {
            feed(hasher, "noul");
            feed(hasher, instructions);
        }
        Question::Choice {
            instructions,
            criteria,
        } => {
            feed(hasher, "choice");
            feed(hasher, instructions);
            for (option, description) in criteria {
                feed(hasher, option);
                feed(hasher, description);
            }
        }
        Question::Score {
            instructions,
            levels,
        } => {
            feed(hasher, "score");
            feed(hasher, instructions);
            for level in levels {
                feed(hasher, level);
            }
        }
    }
}

/// How long a cached answer stays valid.
///
/// Invariant: non-zero.
/// `Copy` law: a scalar with no identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Ttl(Duration);

/// A zero time-to-live.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("ttl must be greater than zero")]
pub struct ZeroTtl;

impl Ttl {
    /// One day.
    pub const DAY: Ttl = Ttl(Duration::from_secs(24 * 60 * 60));

    /// Validates that `duration` is non-zero.
    pub fn new(duration: Duration) -> Result<Ttl, ZeroTtl> {
        if duration.is_zero() {
            Err(ZeroTtl)
        } else {
            Ok(Ttl(duration))
        }
    }

    /// The duration.
    pub fn duration(self) -> Duration {
        self.0
    }
}

impl Default for Ttl {
    /// 24 hours.
    fn default() -> Self {
        Ttl::DAY
    }
}

/// Why a cache operation failed. The decorator treats any of these as a miss.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CacheError {
    /// The backend could not be reached.
    #[error("cache unavailable: {reason}")]
    Unavailable {
        /// Backend detail.
        reason: String,
    },
    /// The backend returned something that is not an answer set.
    #[error("cache entry corrupt: {reason}")]
    Corrupt {
        /// What was wrong.
        reason: String,
    },
    /// A previous writer panicked while holding the cache lock.
    #[error("cache lock poisoned")]
    Poisoned,
}

/// A store of answer sets keyed by [`CacheKey`].
///
/// Implementations own expiry: `put` receives the [`Ttl`] and `get` must not
/// return an expired entry.
#[async_trait]
pub trait Cache: Send + Sync {
    /// The unexpired answers under `key`, if any.
    async fn get(&self, key: &CacheKey) -> Result<Option<AnswerSet>, CacheError>;

    /// Stores `answers` under `key` for `ttl`.
    async fn put(&self, key: CacheKey, answers: AnswerSet, ttl: Ttl) -> Result<(), CacheError>;
}

#[async_trait]
impl<T: Cache + ?Sized> Cache for &T {
    async fn get(&self, key: &CacheKey) -> Result<Option<AnswerSet>, CacheError> {
        (**self).get(key).await
    }

    async fn put(&self, key: CacheKey, answers: AnswerSet, ttl: Ttl) -> Result<(), CacheError> {
        (**self).put(key, answers, ttl).await
    }
}

/// Counters kept by [`Cached`].
///
/// `Copy` law: a snapshot of three numbers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CacheStats {
    /// Answers served from the cache.
    pub hits: u64,
    /// Answers obtained from the judge.
    pub misses: u64,
    /// Cache operations that failed and were treated as misses.
    pub faults: u64,
}

#[derive(Debug, Default)]
struct Counters {
    hits: AtomicU64,
    misses: AtomicU64,
    faults: AtomicU64,
}

impl Counters {
    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            faults: self.faults.load(Ordering::Relaxed),
        }
    }
}

/// A [`Judge`] that consults a [`Cache`] before its inner judge.
#[derive(Debug)]
pub struct Cached<J, C> {
    inner: J,
    cache: C,
    ttl: Ttl,
    counters: Counters,
}

impl<J, C> Cached<J, C> {
    /// Wraps `inner`, storing its successful answers in `cache` for `ttl`.
    pub fn new(inner: J, cache: C, ttl: Ttl) -> Self {
        Cached {
            inner,
            cache,
            ttl,
            counters: Counters::default(),
        }
    }

    /// Hits, misses and faults so far.
    pub fn stats(&self) -> CacheStats {
        self.counters.snapshot()
    }

    /// The wrapped judge.
    pub fn inner(&self) -> &J {
        &self.inner
    }
}

#[async_trait]
impl<J: Judge, C: Cache> Judge for Cached<J, C> {
    async fn judge(
        &self,
        payload: &Payload,
        questions: &QuestionSet,
    ) -> Result<AnswerSet, JudgeError> {
        let key = CacheKey::of(payload, questions);
        match self.cache.get(&key).await {
            Ok(Some(answers)) => {
                Counters::bump(&self.counters.hits);
                return Ok(answers);
            }
            Ok(None) => Counters::bump(&self.counters.misses),
            Err(_fault) => Counters::bump(&self.counters.faults),
        }
        let answers = self.inner.judge(payload, questions).await?;
        if self
            .cache
            .put(key, answers.clone(), self.ttl)
            .await
            .is_err()
        {
            Counters::bump(&self.counters.faults);
        }
        Ok(answers)
    }
}

/// An in-process, bounded, least-recently-used cache with per-entry expiry.
///
/// The mutex is the named boundary for shared mutation; it is never held
/// across an await.
#[derive(Debug)]
pub struct MemoryLru<K> {
    entries: Mutex<lru::LruCache<CacheKey, Entry>>,
    clock: K,
}

#[derive(Debug, Clone)]
struct Entry {
    answers: AnswerSet,
    expires_at: Instant,
}

impl Entry {
    /// Semantic predicate: the entry is still valid at `now`.
    fn is_live_at(&self, now: Instant) -> bool {
        now < self.expires_at
    }
}

impl<K: Clock> MemoryLru<K> {
    /// A cache holding at most `capacity` entries.
    pub fn new(capacity: NonZeroUsize, clock: K) -> Self {
        MemoryLru {
            entries: Mutex::new(lru::LruCache::new(capacity)),
            clock,
        }
    }

    /// Entries currently held, expired ones included until they are touched.
    pub fn len(&self) -> Result<usize, CacheError> {
        self.entries
            .lock()
            .map(|entries| entries.len())
            .map_err(|_| CacheError::Poisoned)
    }

    /// Semantic predicate: nothing is held.
    pub fn is_empty(&self) -> Result<bool, CacheError> {
        self.len().map(|len| len == 0)
    }
}

#[async_trait]
impl<K: Clock> Cache for MemoryLru<K> {
    async fn get(&self, key: &CacheKey) -> Result<Option<AnswerSet>, CacheError> {
        let now = self.clock.now();
        let mut entries = self.entries.lock().map_err(|_| CacheError::Poisoned)?;
        match entries.get(key) {
            Some(entry) if entry.is_live_at(now) => Ok(Some(entry.answers.clone())),
            Some(_expired) => {
                entries.pop(key);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn put(&self, key: CacheKey, answers: AnswerSet, ttl: Ttl) -> Result<(), CacheError> {
        let expires_at = self.clock.now() + ttl.duration();
        let mut entries = self.entries.lock().map_err(|_| CacheError::Poisoned)?;
        entries.put(
            key,
            Entry {
                answers,
                expires_at,
            },
        );
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::probability::Probability;
    use crate::question::{Answer, QuestionName};
    use std::sync::atomic::AtomicUsize;

    fn questions(text: &str) -> QuestionSet {
        QuestionSet::from_iter([(
            QuestionName::new("q").unwrap(),
            Question::Noul {
                instructions: text.into(),
            },
        )])
    }

    fn answers() -> AnswerSet {
        AnswerSet::from_iter([(
            QuestionName::new("q").unwrap(),
            Answer::Noul {
                probability: Probability::ONE,
            },
        )])
    }

    #[test]
    fn key_is_deterministic_and_sensitive_to_payload_and_questions() {
        let p = Payload::new("hello");
        let q = questions("is it a greeting");
        assert_eq!(CacheKey::of(&p, &q), CacheKey::of(&p, &q));
        assert_ne!(
            CacheKey::of(&p, &q),
            CacheKey::of(&Payload::new("hellp"), &q)
        );
        assert_ne!(
            CacheKey::of(&p, &q),
            CacheKey::of(&p, &questions("is it a farewell"))
        );
        assert_eq!(CacheKey::of(&p, &q).hex().len(), 64);
    }

    #[test]
    fn key_encoding_is_length_prefixed_so_boundaries_cannot_shift() {
        // "ab" + "c" vs "a" + "bc" would collide under plain concatenation.
        let left = CacheKey::of(&Payload::new("ab"), &questions("c"));
        let right = CacheKey::of(&Payload::new("a"), &questions("bc"));
        assert_ne!(left, right);
    }

    #[test]
    fn ttl_defaults_to_one_day_and_rejects_zero() {
        assert_eq!(Ttl::default().duration(), Duration::from_secs(86_400));
        assert_eq!(Ttl::new(Duration::ZERO), Err(ZeroTtl));
        assert!(Ttl::new(Duration::from_secs(1)).is_ok());
    }

    /// A clock the test moves by hand.
    struct ManualClock(Mutex<Instant>);

    impl ManualClock {
        fn new() -> Self {
            ManualClock(Mutex::new(Instant::now()))
        }

        fn advance(&self, by: Duration) {
            let mut now = self.0.lock().unwrap();
            *now += by;
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> Instant {
            *self.0.lock().unwrap()
        }
    }

    /// Counts calls and answers every noul with certainty.
    struct Counting(AtomicUsize);

    #[async_trait]
    impl Judge for Counting {
        async fn judge(
            &self,
            _: &Payload,
            questions: &QuestionSet,
        ) -> Result<AnswerSet, JudgeError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(questions
                .iter()
                .map(|(name, _)| {
                    (
                        name.clone(),
                        Answer::Noul {
                            probability: Probability::ONE,
                        },
                    )
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn second_identical_request_is_served_from_the_cache() {
        let clock = ManualClock::new();
        let cached = Cached::new(
            Counting(AtomicUsize::new(0)),
            MemoryLru::new(NonZeroUsize::new(8).unwrap(), &clock),
            Ttl::default(),
        );
        let p = Payload::new("x");
        let q = questions("q");
        assert_eq!(cached.judge(&p, &q).await, Ok(answers()));
        assert_eq!(cached.judge(&p, &q).await, Ok(answers()));
        assert_eq!(cached.inner().0.load(Ordering::SeqCst), 1);
        assert_eq!(
            cached.stats(),
            CacheStats {
                hits: 1,
                misses: 1,
                faults: 0
            }
        );
    }

    #[tokio::test]
    async fn an_expired_entry_is_judged_again() {
        let clock = ManualClock::new();
        let cached = Cached::new(
            Counting(AtomicUsize::new(0)),
            MemoryLru::new(NonZeroUsize::new(8).unwrap(), &clock),
            Ttl::new(Duration::from_secs(60)).unwrap(),
        );
        let p = Payload::new("x");
        let q = questions("q");
        let _ = cached.judge(&p, &q).await;
        clock.advance(Duration::from_secs(59));
        let _ = cached.judge(&p, &q).await;
        assert_eq!(
            cached.inner().0.load(Ordering::SeqCst),
            1,
            "still live at 59s"
        );
        clock.advance(Duration::from_secs(1));
        let _ = cached.judge(&p, &q).await;
        assert_eq!(cached.inner().0.load(Ordering::SeqCst), 2, "expired at 60s");
    }

    #[tokio::test]
    async fn lru_evicts_the_least_recently_used_entry_at_capacity() {
        let clock = ManualClock::new();
        let lru = MemoryLru::new(NonZeroUsize::new(2).unwrap(), &clock);
        let q = questions("q");
        let (a, b, c) = (
            CacheKey::of(&Payload::new("a"), &q),
            CacheKey::of(&Payload::new("b"), &q),
            CacheKey::of(&Payload::new("c"), &q),
        );
        lru.put(a, answers(), Ttl::default()).await.unwrap();
        lru.put(b, answers(), Ttl::default()).await.unwrap();
        let _ = lru.get(&a).await; // a is now the most recent
        lru.put(c, answers(), Ttl::default()).await.unwrap();
        assert_eq!(lru.len(), Ok(2));
        assert!(lru.get(&a).await.unwrap().is_some());
        assert!(
            lru.get(&b).await.unwrap().is_none(),
            "b was least recently used"
        );
        assert!(lru.get(&c).await.unwrap().is_some());
    }

    /// A backend that always fails.
    struct Broken;

    #[async_trait]
    impl Cache for Broken {
        async fn get(&self, _: &CacheKey) -> Result<Option<AnswerSet>, CacheError> {
            Err(CacheError::Unavailable {
                reason: "down".into(),
            })
        }
        async fn put(&self, _: CacheKey, _: AnswerSet, _: Ttl) -> Result<(), CacheError> {
            Err(CacheError::Unavailable {
                reason: "down".into(),
            })
        }
    }

    #[tokio::test]
    async fn a_broken_cache_degrades_to_misses_and_counts_faults() {
        let cached = Cached::new(Counting(AtomicUsize::new(0)), Broken, Ttl::default());
        let p = Payload::new("x");
        let q = questions("q");
        assert_eq!(cached.judge(&p, &q).await, Ok(answers()));
        assert_eq!(cached.judge(&p, &q).await, Ok(answers()));
        assert_eq!(cached.inner().0.load(Ordering::SeqCst), 2);
        assert_eq!(
            cached.stats().faults,
            4,
            "one get and one put fault per call"
        );
    }

    #[tokio::test]
    async fn judge_errors_are_not_cached() {
        struct Failing;
        #[async_trait]
        impl Judge for Failing {
            async fn judge(&self, _: &Payload, _: &QuestionSet) -> Result<AnswerSet, JudgeError> {
                Err(JudgeError::Unavailable {
                    reason: "down".into(),
                })
            }
        }
        let clock = ManualClock::new();
        let lru = MemoryLru::new(NonZeroUsize::new(8).unwrap(), &clock);
        let cached = Cached::new(Failing, &lru, Ttl::default());
        assert!(cached
            .judge(&Payload::new("x"), &questions("q"))
            .await
            .is_err());
        assert_eq!(lru.is_empty(), Ok(true));
    }
}
