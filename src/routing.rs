//! Pure core: subscriptions become questions, answers become verdicts.
//!
//! Nothing here performs IO. Both functions are deterministic in their inputs,
//! which makes routing replayable from a recorded answer set.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::probability::Probability;
use crate::question::{Answer, AnswerSet, Question, QuestionName, QuestionSet};
use crate::subscription::{Disposition, Subscription, SubscriptionId};

/// How subscriptions compete for an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Policy {
    /// One yes/no question per subscription. Any number may match.
    #[default]
    FanOut,
    /// One choice question over all subscriptions. At most one is delivered:
    /// the option with the highest probability, if it clears its thresholds.
    Exclusive,
}

/// Name of the single question asked under [`Policy::Exclusive`].
pub const EXCLUSIVE_QUESTION: &str = "route";

/// The routing decision for one subscription and one event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Verdict {
    /// The subscription judged.
    pub subscription: SubscriptionId,
    /// The judge's probability that the event matches.
    pub probability: Probability,
    /// The policy's decision for that probability.
    pub disposition: Disposition,
}

/// The kind of answer a policy asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnswerKind {
    /// A yes/no probability.
    Noul,
    /// One option among several.
    Choice,
}

impl std::fmt::Display for AnswerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            AnswerKind::Noul => "noul",
            AnswerKind::Choice => "choice",
        })
    }
}

/// Why an answer set could not be turned into verdicts.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RoutingError {
    /// The judge did not answer a question it was asked.
    #[error("no answer to question {question}")]
    MissingAnswer {
        /// The unanswered question.
        question: QuestionName,
    },
    /// The judge answered with the wrong kind of answer.
    #[error("answer to {question} is not a {expected} answer")]
    WrongAnswerKind {
        /// The question.
        question: QuestionName,
        /// The kind the policy asked for.
        expected: AnswerKind,
    },
    /// An exclusive answer omitted one subscription's option.
    #[error("exclusive answer has no probability for {subscription}")]
    MissingOption {
        /// The subscription without a probability.
        subscription: SubscriptionId,
    },
}

/// Builds the question set for one judge request.
///
/// Post: under `FanOut`, one `Noul` per subscription, named by its id. Under
/// `Exclusive`, one `Choice` named [`EXCLUSIVE_QUESTION`] whose options are
/// the subscription ids, or an empty set when there are no subscriptions.
pub fn plan<'a>(
    policy: Policy,
    subscriptions: impl IntoIterator<Item = &'a Subscription>,
) -> QuestionSet {
    match policy {
        Policy::FanOut => subscriptions
            .into_iter()
            .map(|sub| (sub.id().question_name().clone(), sub.question()))
            .collect(),
        Policy::Exclusive => {
            let criteria: BTreeMap<String, String> = subscriptions
                .into_iter()
                .map(|sub| (sub.id().as_str().to_owned(), sub.instructions()))
                .collect();
            if criteria.is_empty() {
                return QuestionSet::new();
            }
            let question = Question::Choice {
                instructions: "Which subscription does the message belong to?".to_owned(),
                criteria,
            };
            QuestionSet::from_iter([(exclusive_name(), question)])
        }
    }
}

/// Turns answers into one verdict per subscription.
///
/// Post: `Ok(v)` implies `v.len()` equals the number of subscriptions and
/// `v[i].subscription == subscriptions[i].id()`, in iteration order.
pub fn decide<'a>(
    policy: Policy,
    subscriptions: impl IntoIterator<Item = &'a Subscription>,
    answers: &AnswerSet,
) -> Result<Vec<Verdict>, RoutingError> {
    match policy {
        Policy::FanOut => subscriptions
            .into_iter()
            .map(|sub| fan_out_verdict(sub, answers))
            .collect(),
        Policy::Exclusive => exclusive_verdicts(subscriptions, answers),
    }
}

fn fan_out_verdict(sub: &Subscription, answers: &AnswerSet) -> Result<Verdict, RoutingError> {
    let question = sub.id().question_name();
    match answers.get(question) {
        Some(Answer::Noul { probability }) => Ok(verdict(
            sub,
            *probability,
            sub.thresholds().dispose(*probability),
        )),
        Some(_) => Err(RoutingError::WrongAnswerKind {
            question: question.clone(),
            expected: AnswerKind::Noul,
        }),
        None => Err(RoutingError::MissingAnswer {
            question: question.clone(),
        }),
    }
}

fn exclusive_verdicts<'a>(
    subscriptions: impl IntoIterator<Item = &'a Subscription>,
    answers: &AnswerSet,
) -> Result<Vec<Verdict>, RoutingError> {
    let subscriptions: Vec<&Subscription> = subscriptions.into_iter().collect();
    if subscriptions.is_empty() {
        return Ok(Vec::new());
    }
    let question = exclusive_name();
    let probabilities = match answers.get(&question) {
        Some(Answer::Choice { probabilities, .. }) => probabilities,
        Some(_) => {
            return Err(RoutingError::WrongAnswerKind {
                question,
                expected: AnswerKind::Choice,
            })
        }
        None => return Err(RoutingError::MissingAnswer { question }),
    };
    let scored: Vec<(&Subscription, Probability)> = subscriptions
        .iter()
        .map(|sub| {
            probabilities
                .get(sub.id().as_str())
                .map(|p| (*sub, *p))
                .ok_or_else(|| RoutingError::MissingOption {
                    subscription: sub.id().clone(),
                })
        })
        .collect::<Result<_, _>>()?;
    // The winner is the first subscription, in registration order, whose
    // probability is maximal. Ties resolve to the earliest registration.
    let winner = scored
        .iter()
        .map(|(_, p)| *p)
        .max()
        .unwrap_or(Probability::ZERO);
    let mut winner_taken = false;
    Ok(scored
        .into_iter()
        .map(|(sub, p)| {
            let is_winner = !winner_taken && p == winner;
            winner_taken |= is_winner;
            let disposition = if is_winner {
                sub.thresholds().dispose(p)
            } else {
                Disposition::Drop
            };
            verdict(sub, p, disposition)
        })
        .collect())
}

fn verdict(sub: &Subscription, probability: Probability, disposition: Disposition) -> Verdict {
    Verdict {
        subscription: sub.id().clone(),
        probability,
        disposition,
    }
}

/// The name of the exclusive question as a validated [`QuestionName`].
pub fn exclusive_name() -> QuestionName {
    QuestionName::literal(EXCLUSIVE_QUESTION)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::subscription::Thresholds;

    fn p(v: f64) -> Probability {
        Probability::new(v).unwrap_or(Probability::ZERO)
    }

    #[test]
    fn exclusive_question_literal_is_non_empty() {
        assert!(QuestionName::new(EXCLUSIVE_QUESTION).is_ok());
    }

    fn sub(id: &'static str) -> Subscription {
        Subscription::new(
            SubscriptionId::new(id).unwrap(),
            format!("about {id}"),
            Thresholds::new(p(0.7), p(0.4)).unwrap_or(Thresholds::deliver_only(p(0.7))),
        )
    }

    fn noul(id: &'static str, v: f64) -> (QuestionName, Answer) {
        (
            sub(id).id().question_name().clone(),
            Answer::Noul { probability: p(v) },
        )
    }

    #[test]
    fn fan_out_plan_asks_one_noul_per_subscription_named_by_id() {
        let subs = [sub("a"), sub("b")];
        let set = plan(Policy::FanOut, &subs);
        assert_eq!(set.len(), 2);
        let names: Vec<&str> = set.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["a", "b"]);
        assert!(set.iter().all(|(_, q)| matches!(q, Question::Noul { .. })));
    }

    #[test]
    fn fan_out_decide_maps_each_probability_through_the_thresholds() {
        let subs = [sub("a"), sub("b"), sub("c")];
        let answers = AnswerSet::from_iter([noul("a", 0.9), noul("b", 0.5), noul("c", 0.1)]);
        let verdicts = decide(Policy::FanOut, &subs, &answers).unwrap_or_default();
        let dispositions: Vec<Disposition> = verdicts.iter().map(|v| v.disposition).collect();
        assert_eq!(
            dispositions,
            [Disposition::Deliver, Disposition::Review, Disposition::Drop]
        );
    }

    #[test]
    fn fan_out_decide_reports_missing_and_mistyped_answers() {
        let subs = [sub("a")];
        assert!(matches!(
            decide(Policy::FanOut, &subs, &AnswerSet::new()),
            Err(RoutingError::MissingAnswer { .. })
        ));
        let wrong = AnswerSet::from_iter([(
            sub("a").id().question_name().clone(),
            Answer::Choice {
                choice: "x".into(),
                probabilities: BTreeMap::new(),
                confidence: None,
            },
        )]);
        assert!(matches!(
            decide(Policy::FanOut, &subs, &wrong),
            Err(RoutingError::WrongAnswerKind { .. })
        ));
    }

    #[test]
    fn exclusive_plan_asks_one_choice_over_all_subscriptions() {
        let subs = [sub("a"), sub("b")];
        let set = plan(Policy::Exclusive, &subs);
        assert_eq!(set.len(), 1);
        let option_count = match set.iter().next() {
            Some((name, Question::Choice { criteria, .. }))
                if name.as_str() == EXCLUSIVE_QUESTION =>
            {
                criteria.len()
            }
            _ => 0,
        };
        assert_eq!(
            option_count, 2,
            "one choice question with one option per subscription"
        );
        assert!(plan(Policy::Exclusive, &[]).is_empty());
    }

    #[test]
    fn exclusive_decide_delivers_only_the_argmax_and_drops_the_rest() {
        let subs = [sub("a"), sub("b")];
        let answers = AnswerSet::from_iter([(
            exclusive_name(),
            Answer::Choice {
                choice: "b".into(),
                probabilities: BTreeMap::from([("a".into(), p(0.3)), ("b".into(), p(0.8))]),
                confidence: None,
            },
        )]);
        let verdicts = decide(Policy::Exclusive, &subs, &answers).unwrap_or_default();
        let dispositions: Vec<Disposition> = verdicts.iter().map(|v| v.disposition).collect();
        assert_eq!(dispositions, [Disposition::Drop, Disposition::Deliver]);
    }

    #[test]
    fn exclusive_decide_sends_a_low_confidence_winner_to_review_or_drop() {
        let subs = [sub("a"), sub("b")];
        let answers = AnswerSet::from_iter([(
            exclusive_name(),
            Answer::Choice {
                choice: "a".into(),
                probabilities: BTreeMap::from([("a".into(), p(0.5)), ("b".into(), p(0.2))]),
                confidence: None,
            },
        )]);
        let verdicts = decide(Policy::Exclusive, &subs, &answers).unwrap_or_default();
        let dispositions: Vec<Disposition> = verdicts.iter().map(|v| v.disposition).collect();
        assert_eq!(dispositions, [Disposition::Review, Disposition::Drop]);
    }
}
