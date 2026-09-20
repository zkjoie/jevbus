//! Jev's JSON wire format and its conversion to the crate vocabulary.
//!
//! Verified against live responses from `jev-1.13.0` on 2026-09-21. A field
//! outside a probability's range surfaces as [`JudgeError::MalformedAnswer`]
//! naming the question rather than a silent misroute.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::event::Payload;
use crate::judge::JudgeError;
use crate::probability::Probability;
use crate::question::{Answer, AnswerSet, Question, QuestionName, QuestionSet};

/// A System One request body.
#[derive(Debug, Serialize)]
pub struct Request<'a> {
    model: &'a str,
    state: &'a str,
    questions: BTreeMap<&'a str, WireQuestion<'a>>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum WireQuestion<'a> {
    Noul {
        instructions: &'a str,
    },
    Choice {
        instructions: &'a str,
        criteria: &'a BTreeMap<String, String>,
    },
    Score {
        instructions: &'a str,
        // Jev takes the ordered level names under `criteria`, as a list.
        criteria: &'a [String],
    },
}

/// A System One response body.
#[derive(Debug, Deserialize)]
pub struct Response {
    /// The concrete model version that answered, when reported.
    #[serde(default)]
    pub model: Option<String>,
    /// Answers keyed by question name.
    pub answers: BTreeMap<String, WireAnswer>,
}

/// One answer as it appears on the wire.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum WireAnswer {
    /// Yes/no probability.
    Noul {
        /// The probability that the proposition holds.
        noul: f64,
    },
    /// One option among several.
    Choice {
        /// The chosen option.
        choice: String,
        /// Probability per option.
        probabilities: BTreeMap<String, f64>,
        /// Confidence in the choice.
        #[serde(default)]
        confidence: Option<f64>,
    },
    /// A rating on an ordered scale.
    Score {
        /// Expected level index as a real number.
        score: f64,
        /// Confidence in the most likely level.
        #[serde(default)]
        confidence: Option<f64>,
        /// Level index (as a decimal string) to level name.
        legend: BTreeMap<String, String>,
        /// Probability per level index (as a decimal string).
        probabilities: BTreeMap<String, f64>,
    },
}

/// Encodes a request. Pure.
pub fn encode<'a>(model: &'a str, payload: &'a Payload, questions: &'a QuestionSet) -> Request<'a> {
    Request {
        model,
        state: payload.as_str(),
        questions: questions
            .iter()
            .map(|(name, question)| (name.as_str(), encode_question(question)))
            .collect(),
    }
}

fn encode_question(question: &Question) -> WireQuestion<'_> {
    match question {
        Question::Noul { instructions } => WireQuestion::Noul { instructions },
        Question::Choice {
            instructions,
            criteria,
        } => WireQuestion::Choice {
            instructions,
            criteria,
        },
        Question::Score {
            instructions,
            levels,
        } => WireQuestion::Score {
            instructions,
            criteria: levels,
        },
    }
}

/// Decodes a response into the crate vocabulary, validating every probability.
///
/// Post: every key of `response.answers` appears in the result, or the
/// function fails naming the offending question.
pub fn decode(response: Response) -> Result<AnswerSet, JudgeError> {
    response
        .answers
        .into_iter()
        .map(|(name, answer)| {
            let question = QuestionName::new(name).map_err(|_| JudgeError::MalformedReply {
                detail: "empty question name".to_owned(),
            })?;
            let answer = decode_answer(&question, answer)?;
            Ok((question, answer))
        })
        .collect()
}

fn decode_answer(question: &QuestionName, answer: WireAnswer) -> Result<Answer, JudgeError> {
    let malformed = |detail: String| JudgeError::MalformedAnswer {
        question: question.clone(),
        detail,
    };
    match answer {
        WireAnswer::Noul { noul } => Ok(Answer::Noul {
            probability: Probability::new(noul).map_err(|e| malformed(e.to_string()))?,
        }),
        WireAnswer::Choice {
            choice,
            probabilities,
            confidence,
        } => Ok(Answer::Choice {
            choice,
            probabilities: decode_map(probabilities).map_err(|e| malformed(e.to_string()))?,
            confidence: confidence
                .map(Probability::new)
                .transpose()
                .map_err(|e| malformed(e.to_string()))?,
        }),
        WireAnswer::Score {
            score,
            confidence,
            legend,
            probabilities,
        } => {
            let distribution =
                decode_score_distribution(&legend, probabilities).map_err(malformed)?;
            let level = most_likely_level(&distribution)
                .ok_or_else(|| malformed("empty distribution".to_owned()))?;
            Ok(Answer::Score {
                level,
                score,
                distribution,
                confidence: confidence
                    .map(Probability::new)
                    .transpose()
                    .map_err(|e| malformed(e.to_string()))?,
            })
        }
    }
}

/// Re-keys an index-keyed distribution by level name using the legend.
///
/// Post: every index in `probabilities` has a legend entry, or the missing
/// index is named in the error.
fn decode_score_distribution(
    legend: &BTreeMap<String, String>,
    probabilities: BTreeMap<String, f64>,
) -> Result<BTreeMap<String, Probability>, String> {
    probabilities
        .into_iter()
        .map(|(index, value)| {
            let name = legend
                .get(&index)
                .cloned()
                .ok_or_else(|| format!("legend has no level for index {index}"))?;
            let probability = Probability::new(value).map_err(|e| e.to_string())?;
            Ok((name, probability))
        })
        .collect()
}

/// The level with the highest probability; ties resolve to the first by name.
fn most_likely_level(distribution: &BTreeMap<String, Probability>) -> Option<String> {
    distribution
        .iter()
        .max_by(|(name_a, p_a), (name_b, p_b)| p_a.cmp(p_b).then_with(|| name_b.cmp(name_a)))
        .map(|(name, _)| name.clone())
}

fn decode_map(
    raw: BTreeMap<String, f64>,
) -> Result<BTreeMap<String, Probability>, crate::probability::ProbabilityError> {
    raw.into_iter()
        .map(|(k, v)| Probability::new(v).map(|p| (k, p)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn encode_matches_the_documented_request_shape() {
        let payload = Payload::new("card charged twice");
        let Ok(name) = QuestionName::new("billing") else {
            return;
        };
        let questions = QuestionSet::from_iter([(
            name,
            Question::Noul {
                instructions: "The message is about billing".into(),
            },
        )]);
        let encoded =
            serde_json::to_value(encode("jev-latest", &payload, &questions)).unwrap_or_default();
        assert_eq!(
            encoded,
            json!({
                "model": "jev-latest",
                "state": "card charged twice",
                "questions": {
                    "billing": { "type": "noul", "instructions": "The message is about billing" }
                }
            })
        );
    }

    #[test]
    fn decode_accepts_noul_and_choice_answers() {
        let body = json!({
            "model": "jev-1.13.0",
            "answers": {
                "billing": { "type": "noul", "noul": 0.93 },
                "route": { "type": "choice", "choice": "a",
                           "probabilities": { "a": 0.8, "b": 0.2 }, "confidence": 0.9 }
            }
        });
        let response: Response = serde_json::from_value(body).unwrap_or_else(|_| Response {
            model: None,
            answers: BTreeMap::new(),
        });
        let decoded = decode(response).unwrap_or_default();
        let Ok(billing) = QuestionName::new("billing") else {
            return;
        };
        assert!(matches!(decoded.get(&billing), Some(Answer::Noul { .. })));
        let Ok(route) = QuestionName::new("route") else {
            return;
        };
        assert!(
            matches!(decoded.get(&route), Some(Answer::Choice { choice, .. }) if choice == "a")
        );
    }

    #[test]
    fn encode_sends_score_levels_under_criteria_as_a_list() {
        let payload = Payload::new("x");
        let Ok(name) = QuestionName::new("sev") else {
            return;
        };
        let questions = QuestionSet::from_iter([(
            name,
            Question::Score {
                instructions: "How severe?".into(),
                levels: vec!["low".into(), "high".into()],
            },
        )]);
        let encoded =
            serde_json::to_value(encode("jev-latest", &payload, &questions)).unwrap_or_default();
        assert_eq!(
            encoded.pointer("/questions/sev/criteria"),
            Some(&json!(["low", "high"]))
        );
    }

    #[test]
    fn decode_rekeys_a_score_distribution_by_legend_and_picks_the_argmax_level() {
        // Verbatim shape of a jev-1.13.0 response.
        let body = json!({
            "model": "jev-1.13.0",
            "answers": {
                "sev": { "type": "score", "score": 1.99, "confidence": 0.99,
                         "legend": { "0": "low", "1": "medium", "2": "high" },
                         "probabilities": { "0": 0.0, "1": 0.01, "2": 0.99 } }
            },
            "usage": { "input_tokens": 307, "output_tokens": 17 }
        });
        let decoded = serde_json::from_value::<Response>(body).ok().map(decode);
        let Ok(sev) = QuestionName::new("sev") else {
            return;
        };
        let level = match decoded {
            Some(Ok(set)) => match set.get(&sev) {
                Some(Answer::Score {
                    level,
                    distribution,
                    ..
                }) if distribution.len() == 3 => level.clone(),
                _ => String::new(),
            },
            _ => String::new(),
        };
        assert_eq!(level, "high");
    }

    #[test]
    fn decode_rejects_out_of_range_probabilities_by_name() {
        let body = json!({ "answers": { "billing": { "type": "noul", "noul": 1.7 } } });
        let decoded = serde_json::from_value::<Response>(body).ok().map(decode);
        assert!(matches!(
            decoded,
            Some(Err(JudgeError::MalformedAnswer { question, .. })) if question.as_str() == "billing"
        ));
    }
}
