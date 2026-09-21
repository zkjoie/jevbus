//! The data half of a bus.
//!
//! ```text
//! data Blueprint = Blueprint { config :: BusConfig, routes :: [RouteSpec], deadLetters :: Bool }
//! data RouteSpec = RouteSpec { subscription :: Subscription, review :: Bool }
//!
//! blueprint     :: Bus -> Blueprint
//! fromBlueprint :: Blueprint -> Judge -> Ledger -> Sleeper -> (Bus, Handles)
//! ```
//!
//! A [`Blueprint`] holds everything about a bus that is a value: policy,
//! retry and breaker settings, the subscriptions, and which routes have a
//! review or dead-letter egress. It leaves out the handles: the judge, the
//! ledger, the timer and the sinks, which are effects and are supplied when
//! the bus is rebuilt. A blueprint serialises with `serde`, so a topology can
//! be stored, versioned, sent to another process and rebuilt there.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::bus::BusConfig;
use crate::delivery::{DeadLetters, Subscriber};
use crate::subscription::{Subscription, SubscriptionId};

/// One subscription and whether it has a review egress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteSpec {
    /// The subscription.
    pub subscription: Subscription,
    /// Whether review-band events have somewhere to go.
    #[serde(default)]
    pub review: bool,
}

/// Everything about a bus that is a value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Blueprint {
    /// Policy, concurrency, retry and breaker settings.
    pub config: BusConfig,
    /// Routes in registration order.
    #[serde(default)]
    pub routes: Vec<RouteSpec>,
    /// Whether failed events have somewhere to go.
    #[serde(default)]
    pub dead_letters: bool,
}

/// Which egress of a route a sink is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Events at or above the deliver threshold.
    Deliver,
    /// Events in the review band.
    Review,
}

/// The in-process streams created by [`crate::Bus::from_blueprint`].
#[derive(Debug, Default)]
pub struct Handles {
    /// Delivery stream per subscription.
    pub subscribers: BTreeMap<SubscriptionId, Subscriber>,
    /// Review stream per subscription that asked for one.
    pub reviewers: BTreeMap<SubscriptionId, Subscriber>,
    /// The dead-letter stream, if the blueprint had one.
    pub dead_letters: Option<DeadLetters>,
}
