//! Subscriptions: what a consumer wants, and the policy that decides delivery.

use std::fmt;

use crate::event::EmptyId;
use crate::probability::Probability;
use crate::question::{Question, QuestionName};

/// Identity of a subscription. Doubles as the judge question name.
///
/// Invariant: non-empty, carried by the inner [`QuestionName`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriptionId(QuestionName);

impl SubscriptionId {
    /// Validates that `id` is non-empty.
    pub fn new(id: impl Into<String>) -> Result<Self, EmptyId> {
        QuestionName::new(id).map(SubscriptionId)
    }

    /// The identifier text.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// The question name that carries this subscription's judgment.
    pub fn question_name(&self) -> &QuestionName {
        &self.0
    }
}

impl fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0.as_str())
    }
}

/// What the bus does with an event for one subscription.
///
/// Ordered from least to most engaged: `Drop < Review < Deliver`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Disposition {
    /// Below both thresholds. Recorded, not delivered.
    Drop,
    /// Between the review and deliver thresholds. Sent to the reviewer.
    Review,
    /// At or above the deliver threshold. Sent to the consumer.
    Deliver,
}

/// Delivery policy. Thresholds are policy, not model: changing them never
/// requires a new judgment.
///
/// Invariant: `review <= deliver`.
///
/// `Copy` law: two probabilities, identity-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Thresholds {
    deliver: Probability,
    review: Probability,
}

/// The review threshold exceeded the deliver threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("review threshold {review} exceeds deliver threshold {deliver}")]
pub struct ThresholdError {
    deliver: Probability,
    review: Probability,
}

impl Thresholds {
    /// Validates `review <= deliver`.
    pub fn new(deliver: Probability, review: Probability) -> Result<Self, ThresholdError> {
        if deliver.at_least(review) {
            Ok(Thresholds { deliver, review })
        } else {
            Err(ThresholdError { deliver, review })
        }
    }

    /// A policy with no review band: deliver at or above `deliver`, else drop.
    pub fn deliver_only(deliver: Probability) -> Self {
        Thresholds {
            deliver,
            review: deliver,
        }
    }

    /// The deliver threshold.
    pub fn deliver(self) -> Probability {
        self.deliver
    }

    /// The review threshold.
    pub fn review(self) -> Probability {
        self.review
    }

    /// Maps a probability to a disposition.
    ///
    /// Law (monotone): `p <= q` implies `dispose(p) <= dispose(q)`.
    pub fn dispose(self, probability: Probability) -> Disposition {
        if probability.at_least(self.deliver) {
            Disposition::Deliver
        } else if probability.at_least(self.review) {
            Disposition::Review
        } else {
            Disposition::Drop
        }
    }
}

/// A plain-language subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    id: SubscriptionId,
    description: String,
    examples: Vec<String>,
    counter_examples: Vec<String>,
    thresholds: Thresholds,
}

impl Subscription {
    /// Creates a subscription with no examples.
    pub fn new(id: SubscriptionId, description: impl Into<String>, thresholds: Thresholds) -> Self {
        Subscription {
            id,
            description: description.into(),
            examples: Vec::new(),
            counter_examples: Vec::new(),
            thresholds,
        }
    }

    /// Adds payloads that should match.
    pub fn with_examples<I, S>(mut self, examples: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.examples.extend(examples.into_iter().map(Into::into));
        self
    }

    /// Adds payloads that should not match.
    pub fn with_counter_examples<I, S>(mut self, examples: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.counter_examples
            .extend(examples.into_iter().map(Into::into));
        self
    }

    /// The identity.
    pub fn id(&self) -> &SubscriptionId {
        &self.id
    }

    /// The plain-language description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// The delivery policy.
    pub fn thresholds(&self) -> Thresholds {
        self.thresholds
    }

    /// Renders the subscription as the instructions of a yes/no question.
    ///
    /// Pure: same subscription, same text.
    pub fn instructions(&self) -> String {
        let mut text = format!("The message matches: {}", self.description);
        push_list(&mut text, "Examples that match", &self.examples);
        push_list(
            &mut text,
            "Examples that do NOT match",
            &self.counter_examples,
        );
        text
    }

    /// The yes/no question that judges this subscription in isolation.
    pub fn question(&self) -> Question {
        Question::Noul {
            instructions: self.instructions(),
        }
    }
}

fn push_list(text: &mut String, heading: &str, items: &[String]) {
    if items.is_empty() {
        return;
    }
    text.push('\n');
    text.push_str(heading);
    text.push(':');
    for item in items {
        text.push_str("\n- ");
        text.push_str(item);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn p(v: f64) -> Probability {
        Probability::new(v).unwrap_or(Probability::ZERO)
    }

    #[test]
    fn thresholds_reject_review_above_deliver() {
        assert!(Thresholds::new(p(0.7), p(0.4)).is_ok());
        assert!(Thresholds::new(p(0.5), p(0.5)).is_ok());
        assert!(Thresholds::new(p(0.4), p(0.7)).is_err());
    }

    #[test]
    fn dispose_maps_each_band_to_its_disposition() {
        let t = Thresholds::new(p(0.7), p(0.4)).unwrap_or(Thresholds::deliver_only(p(0.7)));
        let table = [
            (0.0, Disposition::Drop),
            (0.39, Disposition::Drop),
            (0.4, Disposition::Review),
            (0.69, Disposition::Review),
            (0.7, Disposition::Deliver),
            (1.0, Disposition::Deliver),
        ];
        for (value, expected) in table {
            assert_eq!(t.dispose(p(value)), expected, "p = {value}");
        }
    }

    #[test]
    fn instructions_include_description_and_both_example_lists() {
        let Ok(id) = SubscriptionId::new("a") else {
            return;
        };
        let sub = Subscription::new(id, "billing problems", Thresholds::deliver_only(p(0.5)))
            .with_examples(["my card was charged twice"])
            .with_counter_examples(["how do I change my avatar"]);
        let text = sub.instructions();
        assert!(text.contains("billing problems"));
        assert!(text.contains("- my card was charged twice"));
        assert!(text.contains("do NOT match:\n- how do I change my avatar"));
    }

    proptest! {
        // Law: dispose is monotone in the probability.
        #[test]
        fn dispose_is_monotone(a in 0.0f64..=1.0, b in 0.0f64..=1.0, d in 0.0f64..=1.0, r in 0.0f64..=1.0) {
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            let (deliver, review) = if d >= r { (d, r) } else { (r, d) };
            let t = Thresholds::new(p(deliver), p(review)).unwrap_or(Thresholds::deliver_only(p(deliver)));
            prop_assert!(t.dispose(p(lo)) <= t.dispose(p(hi)));
        }
    }
}
