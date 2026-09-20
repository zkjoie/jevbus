//! The judge protocol: typed questions about a payload and typed answers.
//!
//! This is the vocabulary shared by every [`crate::Judge`]. It mirrors the
//! three question kinds of TypeSafe AI's System One API without depending on
//! its wire format.

use std::collections::BTreeMap;
use std::fmt;

use crate::event::EmptyId;
use crate::probability::Probability;

/// Name of a question within a [`QuestionSet`].
///
/// Invariant: non-empty.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QuestionName(String);

impl QuestionName {
    /// Validates that `name` is non-empty.
    pub fn new(name: impl Into<String>) -> Result<Self, EmptyId> {
        let name = name.into();
        if name.is_empty() {
            Err(EmptyId)
        } else {
            Ok(QuestionName(name))
        }
    }

    /// Crate-private constructor for compile-time literals.
    ///
    /// Pre: `name` is non-empty. Each call site is a literal witnessed by a
    /// unit test next to it, so the invariant holds without a runtime check.
    pub(crate) fn literal(name: &'static str) -> Self {
        QuestionName(name.to_owned())
    }

    /// The name text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for QuestionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A typed question the judge answers about a payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Question {
    /// A yes/no proposition. The answer is the probability that it holds.
    Noul {
        /// The proposition, phrased so that "yes" means it holds.
        instructions: String,
    },
    /// One option among several. The answer is a probability per option.
    Choice {
        /// What is being chosen.
        instructions: String,
        /// Option name to option description.
        criteria: BTreeMap<String, String>,
    },
    /// A rating on an ordered scale. The answer is a distribution over levels.
    Score {
        /// What is being rated.
        instructions: String,
        /// Level names from lowest to highest.
        levels: Vec<String>,
    },
}

/// A set of named questions asked in one judge request.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuestionSet(BTreeMap<QuestionName, Question>);

impl QuestionSet {
    /// An empty set.
    pub fn new() -> Self {
        QuestionSet(BTreeMap::new())
    }

    /// Adds or replaces a question.
    pub fn insert(&mut self, name: QuestionName, question: Question) {
        self.0.insert(name, question);
    }

    /// Iterates the questions in name order.
    pub fn iter(&self) -> impl Iterator<Item = (&QuestionName, &Question)> {
        self.0.iter()
    }

    /// Number of questions.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Semantic predicate: no questions to ask.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl FromIterator<(QuestionName, Question)> for QuestionSet {
    fn from_iter<I: IntoIterator<Item = (QuestionName, Question)>>(iter: I) -> Self {
        QuestionSet(iter.into_iter().collect())
    }
}

/// A judge's answer to one [`Question`].
#[derive(Debug, Clone, PartialEq)]
pub enum Answer {
    /// Answer to [`Question::Noul`].
    Noul {
        /// Probability that the proposition holds.
        probability: Probability,
    },
    /// Answer to [`Question::Choice`].
    Choice {
        /// The option with the highest probability.
        choice: String,
        /// Probability per option.
        probabilities: BTreeMap<String, Probability>,
        /// The judge's confidence in `choice`, when reported.
        confidence: Option<Probability>,
    },
    /// Answer to [`Question::Score`].
    Score {
        /// The most likely level, by name.
        level: String,
        /// Expected level index as a real number in `[0, levels - 1]`.
        score: f64,
        /// Probability per level name.
        distribution: BTreeMap<String, Probability>,
        /// The judge's confidence in `level`, when reported.
        confidence: Option<Probability>,
    },
}

/// Answers keyed by question name.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AnswerSet(BTreeMap<QuestionName, Answer>);

impl AnswerSet {
    /// An empty set.
    pub fn new() -> Self {
        AnswerSet(BTreeMap::new())
    }

    /// Adds or replaces an answer.
    pub fn insert(&mut self, name: QuestionName, answer: Answer) {
        self.0.insert(name, answer);
    }

    /// Looks up an answer by name.
    pub fn get(&self, name: &QuestionName) -> Option<&Answer> {
        self.0.get(name)
    }
}

impl FromIterator<(QuestionName, Answer)> for AnswerSet {
    fn from_iter<I: IntoIterator<Item = (QuestionName, Answer)>>(iter: I) -> Self {
        AnswerSet(iter.into_iter().collect())
    }
}
