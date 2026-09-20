//! Calibrated probabilities as a validated value type.

use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

/// A probability in the closed interval `[0, 1]`.
///
/// Invariant: `0.0 <= value <= 1.0` and `value` is not NaN. The constructor
/// [`Probability::new`] is the only way to obtain one, so every value in the
/// program satisfies the invariant.
///
/// `Copy` law: a probability is an identity-free scalar. Duplicating it has
/// no cost and no semantic effect.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Probability(f64);

/// A value that is not a probability.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("{0} is not a probability: expected a number in [0, 1]")]
pub struct ProbabilityError(f64);

impl Probability {
    /// The impossible event.
    pub const ZERO: Probability = Probability(0.0);
    /// The certain event.
    pub const ONE: Probability = Probability(1.0);

    /// Validates `value` against the interval invariant.
    ///
    /// Post: `Ok(p)` implies `p.value() == value` and `0.0 <= value <= 1.0`.
    pub fn new(value: f64) -> Result<Self, ProbabilityError> {
        if (0.0..=1.0).contains(&value) {
            Ok(Probability(value))
        } else {
            Err(ProbabilityError(value))
        }
    }

    /// The raw value, guaranteed to lie in `[0, 1]`.
    pub fn value(self) -> f64 {
        self.0
    }

    /// Semantic predicate: `self` reaches or exceeds `threshold`.
    pub fn at_least(self, threshold: Probability) -> bool {
        self.0 >= threshold.0
    }
}

// The invariant excludes NaN, so `total_cmp` agrees with `PartialOrd` and the
// order is total.
impl Eq for Probability {}

impl PartialOrd for Probability {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Probability {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl<'de> Deserialize<'de> for Probability {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = f64::deserialize(deserializer)?;
        Probability::new(raw).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for Probability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:.4}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_accepts_the_closed_unit_interval() {
        assert!(Probability::new(0.0).is_ok());
        assert!(Probability::new(0.5).is_ok());
        assert!(Probability::new(1.0).is_ok());
    }

    #[test]
    fn new_rejects_values_outside_the_interval_and_nan() {
        assert_eq!(Probability::new(-0.1), Err(ProbabilityError(-0.1)));
        assert_eq!(Probability::new(1.1), Err(ProbabilityError(1.1)));
        assert!(Probability::new(f64::NAN).is_err());
        assert!(Probability::new(f64::INFINITY).is_err());
    }

    #[test]
    fn at_least_is_reflexive_and_respects_order() {
        assert!(Probability::ONE.at_least(Probability::ONE));
        assert!(Probability::ONE.at_least(Probability::ZERO));
        assert!(!Probability::ZERO.at_least(Probability::ONE));
    }

    #[test]
    fn deserialize_enforces_the_invariant() {
        let ok: Result<Probability, _> = serde_json::from_str("0.25");
        assert_eq!(ok.ok(), Probability::new(0.25).ok());
        let bad: Result<Probability, _> = serde_json::from_str("1.5");
        assert!(bad.is_err());
    }
}
