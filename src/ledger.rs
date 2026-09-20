//! The audit boundary: every routing outcome is recorded, including drops.

use std::sync::Mutex;

use async_trait::async_trait;

use crate::event::EventId;
use crate::judge::JudgeError;
use crate::lifecycle::EventState;
use crate::probability::Probability;
use crate::routing::RoutingError;
use crate::subscription::{Disposition, SubscriptionId};

/// What happened after a disposition was chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Handed to the subscriber stream.
    Delivered,
    /// Handed to the review stream.
    Reviewed,
    /// Below the review threshold; nothing sent.
    Dropped,
    /// The disposition was `Review` but the subscription has no reviewer.
    Unreviewed,
    /// The subscriber (or reviewer) stream had been dropped.
    Unsubscribed,
    /// The event failed and no dead-letter stream was taken.
    Unhandled,
}

/// Why an event received no verdicts at all.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JudgeFailure {
    /// The judge failed.
    #[error(transparent)]
    Judge(#[from] JudgeError),
    /// The judge answered, but the answers did not fit the plan.
    #[error(transparent)]
    Routing(#[from] RoutingError),
}

/// One ledger line.
#[derive(Debug, Clone, PartialEq)]
pub enum Entry {
    /// A verdict was reached for one subscription.
    Routed {
        /// The subscription.
        subscription: SubscriptionId,
        /// The judge's probability.
        probability: Probability,
        /// The policy's disposition.
        disposition: Disposition,
        /// What the bus then did.
        outcome: Outcome,
    },
    /// The event was produced by merging these parts.
    Composed {
        /// Direct constituents, in order.
        parts: Vec<EventId>,
    },
    /// The event moved to a new lifecycle state.
    Lifecycle(EventState),
    /// The event was offered to the dead-letter stream.
    DeadLettered {
        /// Whether anyone took it.
        outcome: Outcome,
    },
}

/// A recorded fact about one event.
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    /// The event.
    pub event: EventId,
    /// What happened.
    pub entry: Entry,
}

/// Why a record could not be written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LedgerError {
    /// The store could not be reached.
    #[error("ledger unavailable: {reason}")]
    Unavailable {
        /// Detail from the store.
        reason: String,
    },
    /// A previous writer panicked while holding the ledger lock.
    #[error("ledger lock poisoned")]
    Poisoned,
}

/// Something that durably records [`Record`]s.
///
/// The bus stops when a record cannot be written: an unaudited delivery is
/// treated as worse than no delivery.
#[async_trait]
pub trait Ledger: Send + Sync {
    /// Appends `record`.
    async fn record(&self, record: Record) -> Result<(), LedgerError>;
}

/// An in-memory ledger for tests and single-process use.
///
/// The mutex is the named effect boundary for shared mutation; the bus itself
/// holds no shared state.
#[derive(Debug, Default)]
pub struct MemoryLedger {
    records: Mutex<Vec<Record>>,
}

impl MemoryLedger {
    /// An empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// A snapshot of every record in write order.
    pub fn records(&self) -> Result<Vec<Record>, LedgerError> {
        self.records
            .lock()
            .map(|guard| guard.clone())
            .map_err(|_| LedgerError::Poisoned)
    }
}

#[async_trait]
impl Ledger for MemoryLedger {
    async fn record(&self, record: Record) -> Result<(), LedgerError> {
        self.records
            .lock()
            .map(|mut guard| guard.push(record))
            .map_err(|_| LedgerError::Poisoned)
    }
}

// A shared reference to a ledger is a ledger: the bus may borrow one that the
// caller reads after the run.
#[async_trait]
impl<T: Ledger + ?Sized> Ledger for &T {
    async fn record(&self, record: Record) -> Result<(), LedgerError> {
        (**self).record(record).await
    }
}
